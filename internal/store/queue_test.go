package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

func seed(t *testing.T, st *store.Store, events int) string {
	t.Helper()
	return seedTenant(t, st, "acme", events)
}

// seedTenant creates a tenant with one endpoint subscribed to everything and
// `events` due deliveries.
func seedTenant(t *testing.T, st *store.Store, name string, events int) string {
	t.Helper()
	ctx := context.Background()
	tenant, _ := testutil.NewTenant(t, st, name)
	if _, err := st.CreateEndpoint(ctx, store.Endpoint{
		TenantID: tenant, URL: "http://127.0.0.1:1/", EventTypes: []string{"*"},
	}); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		if _, err := st.IngestEvent(ctx, tenant, "t.e", json.RawMessage(`{}`), fmt.Sprint("k", i), 8); err != nil {
			t.Fatal(err)
		}
	}
	return tenant
}

// The core distributed guarantee: however many workers poll concurrently,
// each delivery is leased to exactly one of them.
func TestClaimIsExclusiveAcrossConcurrentWorkers(t *testing.T) {
	st := testutil.NewStore(t)
	const total = 300
	seed(t, st, total)
	// This test never completes deliveries; lift the in-flight cap so it
	// measures exclusivity, not the cap (see TestClaimRespectsTenantMaxInflight).
	if _, err := st.Pool.Exec(context.Background(), `UPDATE tenants SET max_inflight = 1000000`); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	owner := map[string]string{} // delivery id -> worker that claimed it
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := fmt.Sprint("worker-", w)
			for {
				got, err := st.ClaimDeliveries(context.Background(), worker, 7, 7, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, d := range got {
					if prev, dup := owner[d.ID]; dup {
						t.Errorf("delivery %s claimed by both %s and %s", d.ID, prev, worker)
					}
					owner[d.ID] = worker
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(owner) != total {
		t.Fatalf("claimed %d distinct deliveries, want %d", len(owner), total)
	}
	var inFlight, badAttempts int
	_ = st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM deliveries WHERE status = 'in_flight'`).Scan(&inFlight)
	_ = st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM deliveries WHERE attempt_count <> 1`).Scan(&badAttempts)
	if inFlight != total || badAttempts != 0 {
		t.Fatalf("in_flight=%d (want %d), rows with attempt_count<>1: %d", inFlight, total, badAttempts)
	}
}

func TestClaimSkipsDeliveriesNotYetDue(t *testing.T) {
	st := testutil.NewStore(t)
	seed(t, st, 2)
	if _, err := st.Pool.Exec(context.Background(),
		`UPDATE deliveries SET next_attempt_at = now() + interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	got, err := st.ClaimDeliveries(context.Background(), "w", 10, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("claimed %d not-yet-due deliveries (err %v)", len(got), err)
	}
}

func TestFinishRequiresLease(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	seed(t, st, 1)

	got, err := st.ClaimDeliveries(ctx, "worker-a", 1, 1, time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	attempt := store.Attempt{StartedAt: time.Now(), Duration: time.Millisecond, StatusCode: 200}

	if err := st.CompleteDelivery(ctx, "worker-b", got[0], attempt); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("complete by non-owner: got %v, want ErrLeaseLost", err)
	}
	if err := st.CompleteDelivery(ctx, "worker-a", got[0], attempt); err != nil {
		t.Fatalf("complete by owner: %v", err)
	}
	// A second completion (e.g. a duplicate/stale worker) must not apply.
	if err := st.CompleteDelivery(ctx, "worker-a", got[0], attempt); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("double complete: got %v, want ErrLeaseLost", err)
	}
}

func claimedByTenant(ds []store.ClaimedDelivery) map[string]int {
	m := map[string]int{}
	for _, d := range ds {
		m[d.TenantID]++
	}
	return m
}

// One tenant with a huge backlog must not starve small tenants.
func TestClaimIsFairAcrossTenants(t *testing.T) {
	st := testutil.NewStore(t)
	big := seedTenant(t, st, "big", 100)
	small1 := seedTenant(t, st, "small1", 3)
	small2 := seedTenant(t, st, "small2", 3)

	got, err := st.ClaimDeliveries(context.Background(), "w", 20, 5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	by := claimedByTenant(got)
	if by[big] != 5 || by[small1] != 3 || by[small2] != 3 {
		t.Fatalf("claimed per tenant = big:%d small1:%d small2:%d, want 5/3/3", by[big], by[small1], by[small2])
	}
}

func TestClaimRespectsTenantMaxInflight(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	tenant := seedTenant(t, st, "capped", 10)
	if _, err := st.Pool.Exec(ctx, `UPDATE tenants SET max_inflight = 3`); err != nil {
		t.Fatal(err)
	}

	first, err := st.ClaimDeliveries(ctx, "w", 10, 10, time.Minute)
	if err != nil || len(first) != 3 {
		t.Fatalf("first claim returned %d (err %v), want 3 (the tenant's cap)", len(first), err)
	}
	if more, _ := st.ClaimDeliveries(ctx, "w", 10, 10, time.Minute); len(more) != 0 {
		t.Fatalf("claimed %d more while at the in-flight cap", len(more))
	}

	attempt := store.Attempt{StartedAt: time.Now(), StatusCode: 200}
	if err := st.CompleteDelivery(ctx, "w", first[0], attempt); err != nil {
		t.Fatal(err)
	}
	next, err := st.ClaimDeliveries(ctx, "w", 10, 10, time.Minute)
	if err != nil || len(next) != 1 || next[0].TenantID != tenant {
		t.Fatalf("after one completion claimed %d, want exactly 1 freed slot (err %v)", len(next), err)
	}
}

func TestClaimSkipsPausedEndpointsAndSuspendedTenants(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	seedTenant(t, st, "paused-ep", 2)
	suspended := seedTenant(t, st, "suspended", 2)
	if _, err := st.Pool.Exec(ctx, `UPDATE endpoints SET status = 'paused'
		WHERE tenant_id NOT IN (SELECT id FROM tenants WHERE name = 'suspended')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool.Exec(ctx, `UPDATE tenants SET status = 'suspended' WHERE id = $1`, suspended); err != nil {
		t.Fatal(err)
	}
	got, err := st.ClaimDeliveries(ctx, "w", 10, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("claimed %d deliveries that should be held (err %v)", len(got), err)
	}

	// Resuming the endpoint releases its held deliveries.
	if _, err := st.Pool.Exec(ctx, `UPDATE endpoints SET status = 'active'`); err != nil {
		t.Fatal(err)
	}
	got, err = st.ClaimDeliveries(ctx, "w", 10, 10, time.Minute)
	if err != nil || len(got) != 2 {
		t.Fatalf("after resume claimed %d, want 2 (suspended tenant stays held)", len(got))
	}
}

func TestReclaimExpired(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	seed(t, st, 4)

	// Two deliveries are leased briefly (expired by the time we reclaim), one is
	// leased for long (must be left alone), and one is left pending.
	short, _ := st.ClaimDeliveries(ctx, "crashed", 2, 10, time.Millisecond)
	long, _ := st.ClaimDeliveries(ctx, "healthy", 1, 10, time.Minute)
	if len(short) != 2 || len(long) != 1 {
		t.Fatalf("setup claims: %d short, %d long", len(short), len(long))
	}
	// One of the crashed deliveries has already used all its attempts.
	if _, err := st.Pool.Exec(ctx, `UPDATE deliveries SET attempt_count = max_attempts WHERE id = $1`, short[0].ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	requeued, dead, err := st.ReclaimExpired(ctx, 100)
	if err != nil || requeued != 1 || dead != 1 {
		t.Fatalf("reclaim = requeued %d, dead %d (err %v), want 1 and 1", requeued, dead, err)
	}

	var status, reason string
	if err := st.Pool.QueryRow(ctx, `SELECT status, dead_reason FROM deliveries WHERE id = $1`, short[0].ID).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "dead" || reason != store.DeadLeaseExpired {
		t.Fatalf("exhausted delivery: status=%q reason=%q, want dead/lease_expired", status, reason)
	}
	var longStatus string
	_ = st.Pool.QueryRow(ctx, `SELECT status FROM deliveries WHERE id = $1`, long[0].ID).Scan(&longStatus)
	if longStatus != "in_flight" {
		t.Fatalf("a live lease was reclaimed: status=%q", longStatus)
	}
	var lost int
	_ = st.Pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE error LIKE 'lease expired%'`).Scan(&lost)
	if lost != 2 {
		t.Fatalf("recorded %d lost attempts, want 2", lost)
	}

	// A stale worker coming back after the reclaim must not overwrite anything.
	err = st.CompleteDelivery(ctx, "crashed", short[1], store.Attempt{StartedAt: time.Now(), StatusCode: 200})
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale worker completion: got %v, want ErrLeaseLost", err)
	}
}
