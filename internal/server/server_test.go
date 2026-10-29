package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"webhookdelivery/internal/metrics"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func get(t *testing.T, p Pinger, drain *Drain, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	NewMux(p, drain).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	return rec.Code
}

func TestHealthz(t *testing.T) {
	// Liveness must not depend on the database.
	if got := get(t, fakePinger{err: errors.New("down")}, nil, "/healthz"); got != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", got)
	}
}

func TestReadyz(t *testing.T) {
	if got := get(t, fakePinger{}, nil, "/readyz"); got != http.StatusOK {
		t.Fatalf("readyz healthy = %d, want 200", got)
	}
	if got := get(t, fakePinger{err: errors.New("down")}, nil, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readyz unhealthy = %d, want 503", got)
	}
}

func TestReadyzFailsWhileDrainingButLivenessDoesNot(t *testing.T) {
	var d Drain
	if got := get(t, fakePinger{}, &d, "/readyz"); got != http.StatusOK {
		t.Fatalf("before drain readyz = %d", got)
	}
	d.Start()
	if got := get(t, fakePinger{}, &d, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("while draining readyz = %d, want 503", got)
	}
	if got := get(t, fakePinger{}, &d, "/healthz"); got != http.StatusOK {
		t.Fatalf("a draining process is still alive: healthz = %d", got)
	}
}

func TestRunDrainsBeforeClosingTheListener(t *testing.T) {
	var d Drain
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	go func() {
		done <- Run(ctx, log, "127.0.0.1:0", http.NewServeMux(), time.Second, WithDrain(&d, 150*time.Millisecond))
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()

	deadline := time.Now().Add(time.Second)
	for !d.Active() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !d.Active() {
		t.Fatal("shutdown did not start draining")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 140*time.Millisecond {
		t.Fatalf("listener closed after %v; the drain delay (150ms) was skipped", took)
	}
}

func TestInstrumentRecordsMetricsLogsAndRequestID(t *testing.T) {
	m := metrics.New()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/things/{id}", func(w http.ResponseWriter, r *http.Request) {
		SetTenant(r.Context(), "tenant-123")
		w.WriteHeader(http.StatusTeapot)
	})
	h := Instrument(log, m, mux)

	for _, path := range []string{"/v1/things/1", "/v1/things/2", "/v1/things/3"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		if rec.Header().Get("X-Request-Id") == "" {
			t.Fatal("no X-Request-Id on the response")
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/nope/"+strings.Repeat("x", 20), nil))

	// Three different paths, one route pattern: cardinality stays bounded.
	if got := testutil.ToFloat64(m.HTTPRequests.WithLabelValues("GET", "GET /v1/things/{id}", "418")); got != 3 {
		t.Errorf("requests for the route pattern = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.HTTPRequests.WithLabelValues("GET", "unmatched", "404")); got != 1 {
		t.Errorf("unmatched requests = %v, want 1 (not one series per path)", got)
	}
	log1 := buf.String()
	for _, want := range []string{`"tenant_id":"tenant-123"`, `"route":"GET /v1/things/{id}"`, `"status":418`, `"request_id"`} {
		if !strings.Contains(log1, want) {
			t.Errorf("access log missing %s:\n%s", want, log1)
		}
	}

	// A caller-supplied request id is propagated.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/things/9", nil)
	req.Header.Set("X-Request-Id", "caller-id-1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-Id") != "caller-id-1" {
		t.Errorf("request id = %q, want the caller's", rec.Header().Get("X-Request-Id"))
	}
}
