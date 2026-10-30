package api_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"webhookdelivery/internal/api"
	"webhookdelivery/internal/harness/chaosproxy"
	"webhookdelivery/internal/store"
	tu "webhookdelivery/internal/testutil"
)

// apiBehindProxy runs an API whose database connections go through a fault proxy.
func apiBehindProxy(t *testing.T, timeout time.Duration) (srv *httptest.Server, key string, proxy *chaosproxy.Proxy) {
	t.Helper()
	owner := tu.NewStore(t)
	_, key = tu.NewTenant(t, owner, "acme")

	u, err := url.Parse(owner.Pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	if proxy, err = chaosproxy.New("127.0.0.1:0", net.JoinHostPort(u.Hostname(), u.Port())); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	u.Host = proxy.Addr()

	apiStore, err := store.Connect(context.Background(), u.String(), store.WithRole(store.RoleAPI))
	if err != nil {
		t.Fatal(err)
	}
	apiStore.Sealer = owner.Sealer
	t.Cleanup(apiStore.Close)

	mux := http.NewServeMux()
	api.New(apiStore, slog.New(slog.NewTextHandler(io.Discard, nil)), api.Options{
		AllowInsecureURLs: true, MaxAttempts: 8, RequestTimeout: timeout,
	}).Register(mux)
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, key, proxy
}

func listEndpoints(t *testing.T, base, key string) (int, string, time.Duration) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", base+"/v1/endpoints", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	start := time.Now()
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, resp.Header.Get("Retry-After"), time.Since(start)
}

// Found by the chaos run: with the database unreachable (silently, as in a
// dropped route) every ingest request hung for the whole outage. Producers need a
// fast 503 with Retry-After so they can back off and retry with their
// idempotency key.
func TestDatabaseOutageGivesAPrompt503WithRetryAfter(t *testing.T) {
	srv, key, proxy := apiBehindProxy(t, 700*time.Millisecond)
	if status, _, _ := listEndpoints(t, srv.URL, key); status != http.StatusOK {
		t.Fatalf("healthy request = %d", status)
	}

	proxy.SetMode(chaosproxy.Stall) // silent partition: connections hang rather than fail
	status, retryAfter, took := listEndpoints(t, srv.URL, key)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("stalled database: status = %d, want 503", status)
	}
	if n, err := strconv.Atoi(retryAfter); err != nil || n < 1 {
		t.Fatalf("Retry-After = %q, want a whole number of seconds", retryAfter)
	}
	if took > 3*time.Second {
		t.Fatalf("the request hung for %v; it must give up at its deadline (700ms)", took)
	}

	proxy.SetMode(chaosproxy.Reset) // loud partition: connections are refused/reset
	if status, _, took := listEndpoints(t, srv.URL, key); status != http.StatusServiceUnavailable || took > 3*time.Second {
		t.Fatalf("reset database: status=%d after %v, want a prompt 503", status, took)
	}

	proxy.SetMode(chaosproxy.Pass) // and the API recovers by itself
	deadline := time.Now().Add(10 * time.Second)
	for {
		if status, _, _ := listEndpoints(t, srv.URL, key); status == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the API did not recover after the partition healed")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
