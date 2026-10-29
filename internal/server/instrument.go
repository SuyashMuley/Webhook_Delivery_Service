package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/telemetry"
)

// RequestInfo is filled in by handlers further down the chain (for example the
// authenticated tenant) so the access log and span written by Instrument can
// include it.
type RequestInfo struct {
	TenantID string
}

type infoKey struct{}

// SetTenant records the authenticated tenant for the access log and trace.
func SetTenant(ctx context.Context, tenantID string) {
	if ri, ok := ctx.Value(infoKey{}).(*RequestInfo); ok {
		ri.TenantID = tenantID
	}
}

// statusWriter records the response status and size.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Instrument wraps h with, in one place: a request ID, a server span (resuming
// an incoming W3C trace context), request metrics keyed by the matched route
// pattern (never the raw path, to keep cardinality bounded) and one structured
// access-log line carrying the request, tenant and trace IDs. m may be nil.
func Instrument(log *slog.Logger, m *metrics.Metrics, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" || len(reqID) > 64 {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-Id", reqID)

		info := &RequestInfo{}
		ctx := context.WithValue(r.Context(), infoKey{}, info)
		ctx = telemetry.ExtractHeader(ctx, propagation.HeaderCarrier(r.Header))
		ctx, span := telemetry.Tracer().Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		r = r.WithContext(ctx)

		sw := &statusWriter{ResponseWriter: w}
		h.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}

		// r.Pattern is set by the ServeMux that routed the request, e.g. "POST /v1/events".
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		dur := time.Since(start)

		span.SetName(route)
		span.SetAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", sw.status),
			attribute.String("request.id", reqID),
		)
		if info.TenantID != "" {
			span.SetAttributes(attribute.String("tenant.id", info.TenantID))
		}
		if sw.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(sw.status))
		}

		if m != nil {
			m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(sw.status)).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur.Seconds())
		}

		// Probes would drown everything else, so they log at debug.
		level := slog.LevelInfo
		if strings.HasSuffix(route, "/healthz") || strings.HasSuffix(route, "/readyz") {
			level = slog.LevelDebug
		} else if sw.status >= 500 {
			level = slog.LevelError
		}
		traceID, _ := telemetry.IDs(ctx)
		log.Log(ctx, level, "request", "method", r.Method, "route", route, "status", sw.status,
			"duration_ms", dur.Milliseconds(), "bytes", sw.bytes, "request_id", reqID,
			"tenant_id", info.TenantID, "trace_id", traceID)
	})
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
