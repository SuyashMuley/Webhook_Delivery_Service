package api_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"webhookdelivery/internal/api"
	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/ratelimit"
	"webhookdelivery/internal/store"
	tu "webhookdelivery/internal/testutil"
)

// replica starts another API server on the same database, like a second pod.
func replica(t *testing.T, owner *store.Store, m *metrics.Metrics, qc ratelimit.Config) *httptest.Server {
	t.Helper()
	apiStore := tu.WithRole(t, owner, store.RoleAPI)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	api.New(apiStore, log, api.Options{
		AllowInsecureURLs: true, MaxAttempts: 8, Metrics: m,
		Quota: ratelimit.New(apiStore, m, log, qc),
	}).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// result is the part of a response the tests look at; the body is already closed.
type result struct {
	StatusCode int
	Header     http.Header
}

func postEvent(t *testing.T, base, key, body string) result {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), "POST", base+"/v1/events", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return result{StatusCode: resp.StatusCode, Header: resp.Header}
}

const okEvent = `{"type":"a.b","payload":{"n":1}}`

func TestIngestIsRateLimitedWithRetryAfter(t *testing.T) {
	st := tu.NewStore(t)
	tenant, key := tu.NewTenant(t, st, "acme")
	if _, err := st.Pool.Exec(context.Background(), `UPDATE tenants SET events_per_sec = 5 WHERE id = $1`, tenant); err != nil {
		t.Fatal(err)
	}
	m := metrics.New()
	srv := replica(t, st, m, ratelimit.Config{})

	// Malformed requests are rejected before the quota, so they do not spend tokens.
	for range 40 {
		if resp := postEvent(t, srv.URL, key, `{"type":"bad type","payload":{}}`); resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid event = %d, want 400", resp.StatusCode)
		}
	}

	accepted, limited := 0, 0
	var retryAfter string
	for range 40 {
		resp := postEvent(t, srv.URL, key, okEvent)
		switch resp.StatusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusTooManyRequests:
			limited++
			retryAfter = resp.Header.Get("Retry-After")
		default:
			t.Fatalf("unexpected status %d", resp.StatusCode)
		}
	}
	// 5/s with a 2s burst: about 10 get through a fast flood, the rest are refused.
	if accepted < 8 || accepted > 14 || limited < 26 {
		t.Fatalf("accepted %d, limited %d of a 40-event flood at 5/s (burst ~10)", accepted, limited)
	}
	if n, err := strconv.Atoi(retryAfter); err != nil || n < 1 {
		t.Fatalf("Retry-After = %q, want a whole number of seconds >= 1", retryAfter)
	}
	if got := testutil.ToFloat64(m.IngestRejected.WithLabelValues("rate_limited")); got != float64(limited) {
		t.Fatalf("rate_limited metric = %v, want %d", got, limited)
	}
	if got := testutil.ToFloat64(m.EventsIngested.WithLabelValues("accepted")); got != float64(accepted) {
		t.Fatalf("accepted metric = %v, want %d", got, accepted)
	}

	// Once the bucket refills, ingest resumes.
	time.Sleep(1200 * time.Millisecond)
	if resp := postEvent(t, srv.URL, key, okEvent); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("after waiting, ingest = %d, want 202", resp.StatusCode)
	}
}

// The limit is per tenant across the whole fleet, not per replica.
func TestRateLimitIsSharedAcrossAPIReplicas(t *testing.T) {
	st := tu.NewStore(t)
	tenant, key := tu.NewTenant(t, st, "acme")
	if _, err := st.Pool.Exec(context.Background(), `UPDATE tenants SET events_per_sec = 20 WHERE id = $1`, tenant); err != nil {
		t.Fatal(err)
	}
	replicas := []*httptest.Server{
		replica(t, st, metrics.New(), ratelimit.Config{}),
		replica(t, st, metrics.New(), ratelimit.Config{}),
		replica(t, st, metrics.New(), ratelimit.Config{}),
	}

	var accepted atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := range 12 { // 12 concurrent producers spread over 3 replicas
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 40 {
				if postEvent(t, replicas[i%3].URL, key, okEvent).StatusCode == http.StatusAccepted {
					accepted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	// A shared bucket allows the 40-token burst plus 20/s for as long as the test ran.
	// Per-replica buckets would allow roughly three times that.
	limit := 40 + 20*elapsed
	t.Logf("accepted %d events in %.2fs across 3 replicas (shared 20/s limit allows ~%.0f; three independent buckets would allow ~%.0f)",
		accepted.Load(), elapsed, limit, 3*limit)
	if float64(accepted.Load()) > limit+15 {
		t.Fatalf("accepted %d events in %.2fs across 3 replicas; a shared 20/s limit allows about %.0f: the limit is not shared", accepted.Load(), elapsed, limit)
	}
	if accepted.Load() < 20 {
		t.Fatalf("accepted only %d events: the limiter is refusing far too much", accepted.Load())
	}
}

func TestBacklogQuotaRefusesIngestUntilTheQueueDrains(t *testing.T) {
	st := tu.NewStore(t)
	tenant, key := tu.NewTenant(t, st, "acme")
	if _, err := st.Pool.Exec(context.Background(), `UPDATE tenants SET max_pending = 3 WHERE id = $1`, tenant); err != nil {
		t.Fatal(err)
	}
	m := metrics.New()
	srv := replica(t, st, m, ratelimit.Config{BacklogTTL: 20 * time.Millisecond})
	e := &env{t: t, srv: srv, st: st, tenant: tenant, key: key}
	e.mustCreateEndpoint("*")

	for i := range 3 {
		if resp := postEvent(t, srv.URL, key, okEvent); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("event %d under the quota = %d, want 202", i, resp.StatusCode)
		}
		time.Sleep(30 * time.Millisecond) // let the cached backlog count refresh
	}
	resp := postEvent(t, srv.URL, key, okEvent)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("event over the backlog quota = %d, want 429", resp.StatusCode)
	}
	if ra, _ := strconv.Atoi(resp.Header.Get("Retry-After")); ra < 5 {
		t.Fatalf("Retry-After = %q, want a real back-off (>= 5s) for a full backlog", resp.Header.Get("Retry-After"))
	}
	if got := testutil.ToFloat64(m.IngestRejected.WithLabelValues("backlog_full")); got != 1 {
		t.Fatalf("backlog_full metric = %v, want 1", got)
	}

	// The queue drains (deliveries finish), so ingest is accepted again.
	if _, err := st.Pool.Exec(context.Background(), `UPDATE deliveries SET status = 'succeeded'`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if resp := postEvent(t, srv.URL, key, okEvent); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("after the backlog drained, ingest = %d, want 202", resp.StatusCode)
	}
}

func TestQuotaOfOneTenantDoesNotAffectAnother(t *testing.T) {
	st := tu.NewStore(t)
	noisy, noisyKey := tu.NewTenant(t, st, "noisy")
	_, quietKey := tu.NewTenant(t, st, "quiet")
	if _, err := st.Pool.Exec(context.Background(), `UPDATE tenants SET events_per_sec = 2 WHERE id = $1`, noisy); err != nil {
		t.Fatal(err)
	}
	srv := replica(t, st, metrics.New(), ratelimit.Config{})
	for range 30 {
		postEvent(t, srv.URL, noisyKey, okEvent)
	}
	if resp := postEvent(t, srv.URL, noisyKey, okEvent); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("noisy tenant not throttled: %d", resp.StatusCode)
	}
	for range 20 {
		if resp := postEvent(t, srv.URL, quietKey, okEvent); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("the quiet tenant was throttled because of the noisy one: %d", resp.StatusCode)
		}
	}
}
