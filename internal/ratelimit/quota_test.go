package ratelimit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/store"
)

// fakeBucket is a token bucket with an injectable clock, standing in for the
// Postgres function.
type fakeBucket struct {
	mu       sync.Mutex
	now      time.Time
	rate     int
	tokens   float64
	updated  time.Time
	leases   int // how many times the shared bucket was touched
	maxPend  int
	backlog  int
	backlogQ int // backlog queries
	err      error
}

func newBucket(rate int) *fakeBucket {
	now := time.Unix(1_700_000_000, 0)
	return &fakeBucket{now: now, rate: rate, tokens: float64(rate) * 2, updated: now, maxPend: 1000}
}

func (b *fakeBucket) advance(d time.Duration) { b.mu.Lock(); b.now = b.now.Add(d); b.mu.Unlock() }
func (b *fakeBucket) clock() time.Time        { b.mu.Lock(); defer b.mu.Unlock(); return b.now }

func (b *fakeBucket) TakeIngestTokens(_ context.Context, _ string, want int) (int, int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return 0, 0, b.err
	}
	b.leases++
	avail := min(float64(b.rate)*2, b.tokens+b.now.Sub(b.updated).Seconds()*float64(b.rate))
	grant := min(want, int(avail))
	b.tokens, b.updated = avail-float64(grant), b.now
	return grant, b.rate, nil
}

func (b *fakeBucket) TenantLimits(context.Context, string) (store.TenantLimits, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return store.TenantLimits{}, b.err
	}
	return store.TenantLimits{EventsPerSec: b.rate, MaxPending: b.maxPend}, nil
}

func (b *fakeBucket) PendingBacklog(context.Context, string, int) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.backlogQ++
	return b.backlog, b.err
}

func quota(b *fakeBucket) (*Quota, *metrics.Metrics) {
	m := metrics.New()
	return New(b, m, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Now: b.clock}), m
}

func TestRateIsEnforcedAndRecovers(t *testing.T) {
	b := newBucket(100) // 100/s, burst 200
	q, m := quota(b)
	ctx := context.Background()

	// A flood: only the burst gets through.
	allowed := 0
	for range 1000 {
		if q.Allow(ctx, "t1").Allowed {
			allowed++
		}
	}
	if allowed < 150 || allowed > 200 {
		t.Fatalf("allowed %d of a 1000-event flood, want about the 200-event burst", allowed)
	}
	d := q.Allow(ctx, "t1")
	if d.Allowed || d.Reason != ReasonRateLimited || d.RetryAfter < time.Second {
		t.Fatalf("flooded decision = %+v, want rate_limited with Retry-After >= 1s", d)
	}
	if got := testutil.ToFloat64(m.IngestRejected.WithLabelValues(ReasonRateLimited)); got < 800 {
		t.Fatalf("rejections counted = %v, want ~800", got)
	}

	// A second later the bucket has refilled by one second's worth.
	b.advance(time.Second)
	recovered := 0
	for range 1000 {
		if q.Allow(ctx, "t1").Allowed {
			recovered++
		}
	}
	if recovered < 70 || recovered > 100 {
		t.Fatalf("after 1s allowed %d, want about the 100/s rate", recovered)
	}
}

// The reason for leasing: the shared bucket is touched once per lease, not once per event.
func TestDatabaseIsNotHitPerEvent(t *testing.T) {
	b := newBucket(1000)
	q, _ := quota(b)
	n := 0
	for range 150 { // well inside the burst, so every event is allowed
		if q.Allow(context.Background(), "t1").Allowed {
			n++
		}
	}
	if n != 150 {
		t.Fatalf("allowed %d, want 150", n)
	}
	if b.leases > 5 {
		t.Fatalf("the shared bucket was touched %d times for 150 events; leasing should batch these", b.leases)
	}
}

func TestStaleLeaseIsDiscarded(t *testing.T) {
	b := newBucket(100)
	q, _ := quota(b)
	ctx := context.Background()
	q.Allow(ctx, "t1") // leases ~10 tokens
	before := b.leases
	b.advance(2 * time.Second) // lease expired; unused tokens must not be hoarded
	q.Allow(ctx, "t1")
	if b.leases != before+1 {
		t.Fatalf("a stale lease was reused (leases %d -> %d)", before, b.leases)
	}
}

func TestTenantsAreIndependent(t *testing.T) {
	b := newBucket(10) // burst 20
	q, _ := quota(b)
	ctx := context.Background()
	for range 100 {
		q.Allow(ctx, "noisy")
	}
	if q.Allow(ctx, "noisy").Allowed {
		t.Fatal("noisy tenant should be throttled")
	}
	// The fake shares one bucket across tenants, so give the quiet tenant its own state:
	// what is being checked here is that local state is keyed per tenant.
	if q.state("quiet") == q.state("noisy") {
		t.Fatal("tenants share local quota state")
	}
}

func TestBacklogQuotaRefusesAndCachesTheCount(t *testing.T) {
	b := newBucket(1000)
	b.maxPend, b.backlog = 500, 500
	q, m := quota(b)
	ctx := context.Background()

	d := q.Allow(ctx, "t1")
	if d.Allowed || d.Reason != ReasonBacklogFull || d.RetryAfter < 5*time.Second {
		t.Fatalf("decision = %+v, want backlog_full with a back-off", d)
	}
	for range 50 {
		q.Allow(ctx, "t1")
	}
	if b.backlogQ != 1 {
		t.Fatalf("backlog counted %d times for 51 requests; the count should be cached", b.backlogQ)
	}
	if b.leases != 0 {
		t.Fatalf("tokens were spent on events refused for backlog (%d leases)", b.leases)
	}
	if got := testutil.ToFloat64(m.IngestRejected.WithLabelValues(ReasonBacklogFull)); got != 51 {
		t.Fatalf("backlog rejections = %v, want 51", got)
	}

	// Once the backlog drains and the cached count expires, ingest resumes.
	b.backlog = 10
	b.advance(3 * time.Second)
	if !q.Allow(ctx, "t1").Allowed {
		t.Fatal("ingest did not resume after the backlog drained")
	}
}

func TestQuotaFailsOpenWhenTheDatabaseErrors(t *testing.T) {
	b := newBucket(10)
	b.err = errors.New("connection refused")
	q, m := quota(b)
	for range 20 {
		if !q.Allow(context.Background(), "t1").Allowed {
			t.Fatal("a failing quota check must not block ingest")
		}
	}
	if testutil.ToFloat64(m.QuotaCheckErrors) == 0 {
		t.Fatal("fail-open events were not counted")
	}
}
