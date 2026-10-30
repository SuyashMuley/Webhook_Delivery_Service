package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"webhookdelivery/internal/store"
	"webhookdelivery/internal/telemetry"
	"webhookdelivery/internal/testutil"
)

func TestQueueStatsSeparatesLatencyFromDeliberatelyHeldWork(t *testing.T) {
	st := testutil.NewStore(t)
	seedTenant(t, st, "healthy", 3)
	seedTenant(t, st, "paused", 2)
	seedTenant(t, st, "tripped", 2)
	seedTenant(t, st, "slowcustomer", 3)
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)

	// healthy: 3 due, the oldest due 120s ago, plus one retry scheduled for later and one dead.
	exec(t, st, `UPDATE deliveries SET next_attempt_at = now() - interval '120 seconds'
	               WHERE id = (SELECT d.id FROM deliveries d JOIN tenants t ON t.id = d.tenant_id WHERE t.name = 'healthy' ORDER BY d.id LIMIT 1)`)
	exec(t, st, `UPDATE deliveries SET next_attempt_at = now() + interval '1 hour'
	               WHERE id = (SELECT d.id FROM deliveries d JOIN tenants t ON t.id = d.tenant_id WHERE t.name = 'healthy' ORDER BY d.id DESC LIMIT 1)`)
	// Work held for a paused endpoint or an open circuit is much older, but must not count as latency.
	exec(t, st, `UPDATE endpoints SET status = 'paused' WHERE tenant_id = (SELECT id FROM tenants WHERE name = 'paused')`)
	exec(t, st, `UPDATE endpoints SET circuit_open_until = now() + interval '1 hour' WHERE tenant_id = (SELECT id FROM tenants WHERE name = 'tripped')`)
	exec(t, st, `UPDATE deliveries SET next_attempt_at = now() - interval '5 hours' WHERE tenant_id IN (SELECT id FROM tenants WHERE name IN ('paused', 'tripped'))`)
	exec(t, st, `UPDATE deliveries SET status = 'dead', dead_at = now(), dead_reason = 'max_attempts'
	               WHERE id = (SELECT d.id FROM deliveries d JOIN tenants t ON t.id = d.tenant_id WHERE t.name = 'paused' LIMIT 1)`)
	// A customer whose endpoint is slow: it is at its concurrency cap (1 in flight), and the
	// rest of its work has been waiting for hours. That is the customer's backlog, not ours.
	exec(t, st, `UPDATE endpoints SET max_concurrency = 1 WHERE tenant_id = (SELECT id FROM tenants WHERE name = 'slowcustomer')`)
	exec(t, st, `UPDATE deliveries SET next_attempt_at = now() - interval '4 hours' WHERE tenant_id = (SELECT id FROM tenants WHERE name = 'slowcustomer')`)
	exec(t, st, `UPDATE deliveries SET status = 'in_flight', locked_by = 'w', lease_expires_at = now() + interval '1 minute'
	               WHERE id = (SELECT d.id FROM deliveries d JOIN tenants t ON t.id = d.tenant_id WHERE t.name = 'slowcustomer' LIMIT 1)`)

	got, err := st.QueueStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.OldestDueAge < 115*time.Second || got.OldestDueAge > 140*time.Second {
		t.Errorf("oldest due age = %v, want ~120s: work held for paused/open-circuit endpoints (5h) and work behind a slow endpoint's own concurrency cap (4h) must be excluded", got.OldestDueAge)
	}
	// Of the 7 due deliveries only healthy's 2 could be taken by an extra worker: paused endpoints and
	// open circuits will not be claimed, and slowcustomer's endpoint is at its concurrency cap.
	if got.DueClaimable != 2 {
		t.Errorf("claimable due = %d, want 2 (work behind paused endpoints, open circuits and a saturated endpoint cannot be helped by more workers)", got.DueClaimable)
	}
	// Of the 7 due deliveries only healthy's 2 could be taken by an extra worker: paused endpoints and
	// open circuits will not be claimed, and slowcustomer's endpoint is at its concurrency cap.
	if got.DueClaimable != 2 {
		t.Errorf("claimable due = %d, want 2 (work behind paused endpoints, open circuits and a saturated endpoint cannot be helped by more workers)", got.DueClaimable)
	}
	if got.Scheduled != 1 {
		t.Errorf("scheduled = %d, want 1", got.Scheduled)
	}
	if got.Due != 7 { // healthy 2, paused 1, tripped 2, slowcustomer 2 (3 minus the one in flight)
		t.Errorf("due = %d, want 7", got.Due)
	}
	if got.DeadByReason["max_attempts"] != 1 {
		t.Errorf("dead by reason = %v", got.DeadByReason)
	}
	if got.InFlight != 1 {
		t.Errorf("in flight = %d, want 1", got.InFlight)
	}
	if got.PausedEndpoints != 1 || got.CircuitOpenEndpoints != 1 || got.DisabledEndpoints != 0 {
		t.Errorf("endpoint states: paused=%d open=%d disabled=%d", got.PausedEndpoints, got.CircuitOpenEndpoints, got.DisabledEndpoints)
	}
}

func TestPurgeRemovesOnlyOldFinishedWork(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	tenant := seedTenant(t, st, "acme", 6) // 6 events, 6 deliveries
	ids := func(n int) []string {
		rows, err := st.Pool.Query(ctx, `SELECT id::text FROM deliveries ORDER BY id LIMIT $1`, n)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			_ = rows.Scan(&id)
			out = append(out, id)
		}
		return out
	}
	d := ids(6)
	// d[0] old succeeded, d[1] recent succeeded, d[2] old dead, d[3] recent dead, d[4],d[5] still pending.
	exec(t, st, `UPDATE deliveries SET status = 'succeeded', updated_at = now() - interval '10 days' WHERE id = $1`, d[0])
	exec(t, st, `UPDATE deliveries SET status = 'succeeded', updated_at = now() - interval '1 hour'  WHERE id = $1`, d[1])
	exec(t, st, `UPDATE deliveries SET status = 'dead', dead_at = now() - interval '40 days', dead_reason = 'max_attempts' WHERE id = $1`, d[2])
	exec(t, st, `UPDATE deliveries SET status = 'dead', dead_at = now() - interval '2 days',  dead_reason = 'max_attempts' WHERE id = $1`, d[3])
	exec(t, st, `INSERT INTO attempts (delivery_id, attempt_no) VALUES ($1, 1), ($2, 1)`, d[0], d[1])
	exec(t, st, `INSERT INTO delivery_replays (delivery_id, tenant_id, prior_attempt_count) VALUES ($1, $2, 3)`, d[0], tenant)
	// Every event is 20 days old; one extra old event has no deliveries at all.
	exec(t, st, `UPDATE events SET created_at = now() - interval '20 days'`)
	exec(t, st, `INSERT INTO events (tenant_id, type, payload, created_at) VALUES ($1, 'orphan', '{}', now() - interval '20 days')`, tenant)

	res, err := st.Purge(ctx, 7*24*time.Hour, 30*24*time.Hour, 7*24*time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Succeeded != 1 || res.Dead != 1 {
		t.Fatalf("purged %+v, want exactly 1 old succeeded and 1 old dead delivery", res)
	}
	if n := scalar[int](t, st, `SELECT count(*) FROM deliveries`); n != 4 {
		t.Fatalf("%d deliveries remain, want 4 (recent succeeded, recent dead, 2 pending)", n)
	}
	// The old delivery's attempt and replay audit rows went with it; the others stayed.
	if n := scalar[int](t, st, `SELECT count(*) FROM attempts`); n != 1 {
		t.Errorf("%d attempts remain, want 1", n)
	}
	if n := scalar[int](t, st, `SELECT count(*) FROM delivery_replays`); n != 0 {
		t.Errorf("%d replay audit rows remain, want 0", n)
	}
	// Events are removed only once nothing refers to them: the orphan and the 2 events of the purged deliveries.
	if res.Events != 3 {
		t.Errorf("purged %d events, want 3 (the orphan + the two whose deliveries were purged)", res.Events)
	}
	if n := scalar[int](t, st, `SELECT count(*) FROM events`); n != 4 {
		t.Errorf("%d events remain, want 4 (those still referenced by a delivery)", n)
	}

	// Disabled categories (age 0) delete nothing, and the batch limit is honored.
	res, err = st.Purge(ctx, 0, 0, 0, 100)
	if err != nil || res != (store.PurgeResult{}) {
		t.Fatalf("purge with every category disabled = %+v, %v", res, err)
	}
	exec(t, st, `UPDATE deliveries SET status = 'succeeded', updated_at = now() - interval '10 days'`)
	if res, _ = st.Purge(ctx, 7*24*time.Hour, 0, 0, 2); res.Succeeded != 2 {
		t.Fatalf("batch limit: purged %d in one pass, want 2", res.Succeeded)
	}
}

func TestReleaseWorkerLeasesUndoesTheStartedAttempt(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	seed(t, st, 4)
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)
	exec(t, st, `UPDATE endpoints SET max_concurrency = 1000`)

	mine, err := st.ClaimDeliveries(ctx, "worker-a", 3, 3, time.Minute)
	if err != nil || len(mine) != 3 {
		t.Fatalf("claim: %d %v", len(mine), err)
	}
	other, _ := st.ClaimDeliveries(ctx, "worker-b", 1, 1, time.Minute)
	if len(other) != 1 {
		t.Fatal("worker-b claim failed")
	}

	n, err := st.ReleaseWorkerLeases(ctx, "worker-a")
	if err != nil || n != 3 {
		t.Fatalf("released %d (err %v), want 3", n, err)
	}
	if got := scalar[int](t, st, `SELECT count(*) FROM deliveries WHERE status = 'pending' AND attempt_count = 0 AND locked_by IS NULL AND next_attempt_at <= now()`); got != 3 {
		t.Fatalf("%d deliveries are immediately claimable with a fresh attempt budget, want 3", got)
	}
	if got := scalar[int](t, st, `SELECT count(*) FROM deliveries WHERE status = 'in_flight' AND locked_by = 'worker-b'`); got != 1 {
		t.Fatalf("another worker's lease was released")
	}
	if got := scalar[int](t, st, `SELECT count(*) FROM attempts`); got != 0 {
		t.Fatalf("release wrote %d attempt rows; the attempt never finished", got)
	}
}

func TestTraceContextIsStoredOnDeliveriesAndReturnedByClaim(t *testing.T) {
	prev := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev); _ = tp.Shutdown(context.Background()) })

	st := testutil.NewStore(t)
	tenant := seedTenant(t, st, "acme", 0)
	ctx, span := telemetry.Tracer().Start(context.Background(), "request")
	if _, err := st.IngestEvent(ctx, tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}
	span.End()
	wantTrace, _ := telemetry.IDs(ctx)

	got := claim(t, st, 1)
	if len(got) != 1 || !strings.Contains(got[0].TraceContext, wantTrace) {
		t.Fatalf("claimed trace context %q does not belong to trace %s", got[0].TraceContext, wantTrace)
	}

	// With no incoming trace, ingest starts a new root trace of its own, so every
	// delivery is traceable; it is a different trace from the first request's.
	if _, err := st.IngestEvent(context.Background(), tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}
	exec(t, st, `UPDATE tenants SET max_inflight = 100`)
	var fresh int
	for _, d := range claim(t, st, 5) {
		if d.TraceContext == "" {
			t.Fatal("a delivery created without an incoming trace has no trace context")
		}
		if !strings.Contains(d.TraceContext, wantTrace) {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d deliveries started a new trace, want 1", fresh)
	}
}

func TestIngestQuotaFunctionsAreTenantScoped(t *testing.T) {
	owner := testutil.NewStore(t)
	a := seedTenant(t, owner, "a", 0)
	b := seedTenant(t, owner, "b", 0)
	exec(t, owner, `UPDATE tenants SET events_per_sec = 10, max_pending = 77`)
	apiStore := testutil.WithRole(t, owner, store.RoleAPI)
	ctx := context.Background()

	l, err := apiStore.TenantLimits(ctx, a)
	if err != nil || l.EventsPerSec != 10 || l.MaxPending != 77 {
		t.Fatalf("TenantLimits = %+v, %v", l, err)
	}

	// The bucket starts full (burst = 2s of tokens) and is per tenant.
	got, perSec, err := apiStore.TakeIngestTokens(ctx, a, 1000)
	if err != nil || got != 20 || perSec != 10 {
		t.Fatalf("first take = %d (rate %d, err %v), want the burst of 20", got, perSec, err)
	}
	if got, _, _ := apiStore.TakeIngestTokens(ctx, a, 5); got != 0 {
		t.Fatalf("tenant A took %d more tokens from an empty bucket", got)
	}
	if got, _, _ := apiStore.TakeIngestTokens(ctx, b, 5); got != 5 {
		t.Fatalf("tenant A draining its bucket affected tenant B (B got %d)", got)
	}

	// The bucket refills with time.
	time.Sleep(600 * time.Millisecond)
	if got, _, _ := apiStore.TakeIngestTokens(ctx, a, 100); got < 4 || got > 8 {
		t.Fatalf("after 600ms at 10/s the bucket gave %d tokens, want ~6", got)
	}

	// The definer function acts only on the transaction's tenant: with no tenant
	// context it refuses, and it takes no tenant argument that could be forged.
	if _, err := apiStore.Pool.Exec(ctx, `SELECT * FROM take_ingest_tokens(5)`); err == nil || !strings.Contains(err.Error(), "no tenant context") {
		t.Fatalf("take_ingest_tokens without a tenant context: got %v", err)
	}
}

func TestPendingBacklogIsBounded(t *testing.T) {
	owner := testutil.NewStore(t)
	tenant := seedTenant(t, owner, "acme", 30)
	seedTenant(t, owner, "other", 50)
	apiStore := testutil.WithRole(t, owner, store.RoleAPI)

	if n, err := apiStore.PendingBacklog(context.Background(), tenant, 1000); err != nil || n != 30 {
		t.Fatalf("backlog = %d (err %v), want this tenant's 30 only", n, err)
	}
	if n, _ := apiStore.PendingBacklog(context.Background(), tenant, 10); n != 10 {
		t.Fatalf("backlog with cap 10 = %d; the count must stop at the cap", n)
	}
}

// The worker role has only SELECT and DELETE on events. Running purge as the table owner
// (as the other test does) hides any statement that needs more, such as SELECT ... FOR UPDATE.
func TestPurgeWorksUnderTheWorkerRole(t *testing.T) {
	owner := testutil.NewStore(t)
	ctx := context.Background()
	seedTenant(t, owner, "acme", 3)
	exec(t, owner, `UPDATE deliveries SET status = 'succeeded', updated_at = now() - interval '10 days'`)
	exec(t, owner, `UPDATE events SET created_at = now() - interval '20 days'`)

	worker := testutil.WithRole(t, owner, store.RoleWorker)
	res, err := worker.Purge(ctx, 7*24*time.Hour, 30*24*time.Hour, 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("purge as the worker role: %v", err)
	}
	if res.Succeeded != 3 || res.Events != 3 {
		t.Fatalf("purged %+v, want 3 deliveries and 3 events", res)
	}
}
