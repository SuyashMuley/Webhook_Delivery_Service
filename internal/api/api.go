// Package api implements the tenant-facing HTTP API (/v1/...).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/ratelimit"
	"webhookdelivery/internal/server"
	"webhookdelivery/internal/store"
)

type Options struct {
	AllowInsecureURLs bool // permit http:// endpoint URLs (dev only)
	MaxAttempts       int  // attempts per delivery before it is dead-lettered

	Quota   *ratelimit.Quota // per-tenant ingest rate and backlog limits; nil disables them
	Metrics *metrics.Metrics // optional
	// RequestTimeout bounds every request, database calls included. Without it a
	// database that stops answering would hang requests for as long as the outage
	// lasts; with it producers get a prompt 503 and retry. Default 5s.
	RequestTimeout time.Duration
	// Wake is called after an event that created deliveries has committed, to wake
	// idle workers (see store.Notifier). Optional.
	Wake func()
}

type API struct {
	store *store.Store
	log   *slog.Logger
	opts  Options
}

func New(s *store.Store, log *slog.Logger, opts Options) *API {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 8
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 5 * time.Second
	}
	return &API{store: s, log: log, opts: opts}
}

// Register mounts all /v1 routes, each behind API-key authentication.
func (a *API) Register(mux *http.ServeMux) {
	mux.Handle("POST /v1/endpoints", a.authenticate(a.createEndpoint))
	mux.Handle("GET /v1/endpoints", a.authenticate(a.listEndpoints))
	mux.Handle("GET /v1/endpoints/{id}", a.authenticate(a.getEndpoint))
	mux.Handle("PATCH /v1/endpoints/{id}", a.authenticate(a.updateEndpoint))
	mux.Handle("DELETE /v1/endpoints/{id}", a.authenticate(a.deleteEndpoint))

	mux.Handle("GET /v1/endpoints/{id}/secrets", a.authenticate(a.listSecrets))
	mux.Handle("POST /v1/endpoints/{id}/secrets/rotate", a.authenticate(a.rotateSecret))
	mux.Handle("DELETE /v1/endpoints/{id}/secrets/{secretId}", a.authenticate(a.revokeSecret))

	mux.Handle("POST /v1/events", a.authenticate(a.createEvent))
	mux.Handle("GET /v1/events/{id}/deliveries", a.authenticate(a.listEventDeliveries))

	mux.Handle("GET /v1/deliveries/{id}", a.authenticate(a.getDelivery))
	mux.Handle("POST /v1/deliveries/{id}/replay", a.authenticate(a.replayDelivery))
	mux.Handle("GET /v1/dlq", a.authenticate(a.listDLQ))
	mux.Handle("POST /v1/dlq/replay", a.authenticate(a.replayDLQ))
}

type tenantKey struct{}

func (a *API) authenticate(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), a.opts.RequestTimeout)
		defer cancel()
		r = r.WithContext(ctx)

		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			a.writeError(w, http.StatusUnauthorized, "unauthorized", "missing or malformed Authorization header; expected 'Bearer <api key>'")
			return
		}
		tenantID, err := a.store.AuthenticateKey(r.Context(), auth.HashKey(token))
		switch {
		case errors.Is(err, store.ErrNotFound):
			a.writeError(w, http.StatusUnauthorized, "unauthorized", "invalid API key")
			return
		case err != nil:
			a.internalError(w, r, err)
			return
		}
		server.SetTenant(r.Context(), tenantID)
		next(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenantID)))
	})
}

func tenantID(r *http.Request) string {
	return r.Context().Value(tenantKey{}).(string)
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (a *API) writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

func (a *API) internalError(w http.ResponseWriter, r *http.Request, err error) {
	if unavailable(err) {
		// The database is not answering: tell the producer to back off and retry
		// (with its Idempotency-Key) instead of reporting a bug.
		a.log.Warn("database unavailable", "method", r.Method, "path", r.URL.Path, "error", err)
		w.Header().Set("Retry-After", "2")
		a.writeError(w, http.StatusServiceUnavailable, "unavailable", "the service is temporarily unavailable; retry shortly")
		return
	}
	a.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	a.writeError(w, http.StatusInternalServerError, "internal", "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decode reads a size-limited, strict JSON body into dst. On failure it writes
// the error response and returns false.
func (a *API) decode(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.More() {
		err = errors.New("unexpected data after JSON body")
	}
	if err == nil {
		return true
	}
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		a.writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body too large")
	case errors.Is(err, io.EOF):
		a.writeError(w, http.StatusBadRequest, "invalid_request", "request body is empty")
	default:
		a.writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON: "+err.Error())
	}
	return false
}

// unavailable reports whether err means the database could not be reached or did
// not answer in time, as opposed to a bug or a bad request: our own deadline
// expiring, a network error, a dropped connection, or Postgres shutting down.
func unavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// class 08: connection exception; 57P01-03: admin shutdown, crash shutdown, cannot connect now
		return strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "57P0")
	}
	return false
}
