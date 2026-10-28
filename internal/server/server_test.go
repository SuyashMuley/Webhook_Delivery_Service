package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func get(t *testing.T, p Pinger, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	NewMux(p).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestHealthz(t *testing.T) {
	// Liveness must not depend on the database.
	if got := get(t, fakePinger{err: errors.New("down")}, "/healthz"); got != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", got)
	}
}

func TestReadyz(t *testing.T) {
	if got := get(t, fakePinger{}, "/readyz"); got != http.StatusOK {
		t.Fatalf("readyz healthy = %d, want 200", got)
	}
	if got := get(t, fakePinger{err: errors.New("down")}, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readyz unhealthy = %d, want 503", got)
	}
}
