package store_test

import (
	"context"
	"testing"
	"time"

	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

func exec(t *testing.T, st *store.Store, q string, args ...any) {
	t.Helper()
	if _, err := st.Pool.Exec(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func scalar[T any](t *testing.T, st *store.Store, q string, args ...any) (v T) {
	t.Helper()
	if err := st.Pool.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func fail(t *testing.T, st *store.Store, d store.ClaimedDelivery, code int) {
	t.Helper()
	a := store.Attempt{StartedAt: time.Now(), StatusCode: code}
	if err := st.FailDelivery(context.Background(), "w", d, a, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func claim(t *testing.T, st *store.Store, n int) []store.ClaimedDelivery {
	t.Helper()
	got, err := st.ClaimDeliveries(context.Background(), "w", n, n, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestClaimRespectsEndpointConcurrency(t *testing.T) {
	st := testutil.NewStore(t)
	seed(t, st, 10)
	exec(t, st, `UPDATE endpoints SET max_concurrency = 2`)
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)

	first := claim(t, st, 10)
	if len(first) != 2 {
		t.Fatalf("claimed %d, want the endpoint's cap of 2", len(first))
	}
	if more := claim(t, st, 10); len(more) != 0 {
		t.Fatalf("claimed %d more while the endpoint was at its cap", len(more))
	}
	if err := st.CompleteDelivery(context.Background(), "w", first[0], store.Attempt{StartedAt: time.Now(), StatusCode: 200}); err != nil {
		t.Fatal(err)
	}
	if next := claim(t, st, 10); len(next) != 1 {
		t.Fatalf("after one completion claimed %d, want exactly the 1 freed slot", len(next))
	}
}

// A dead endpoint must not be able to use up capacity that belongs to a healthy one.
func TestSlowEndpointCannotConsumeSlotsOfHealthyOne(t *testing.T) {
	st := testutil.NewStore(t)
	tenant := seedTenant(t, st, "acme", 20) // endpoint #1 gets 20 deliveries
	healthy, err := st.CreateEndpoint(context.Background(), store.Endpoint{TenantID: tenant, URL: "http://127.0.0.1:2/", EventTypes: []string{"other"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := st.IngestEvent(context.Background(), tenant, "other", []byte(`{}`), "o"+string(rune('a'+i)), 8); err != nil {
			t.Fatal(err)
		}
	}
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)
	exec(t, st, `UPDATE endpoints SET max_concurrency = 3 WHERE id <> $1`, healthy.ID)

	// Nothing ever completes (the first endpoint is a black hole). Over many
	// polls the black hole may hold at most its 3 slots, and the healthy
	// endpoint must still get all of its work.
	byEndpoint := map[string]int{}
	for range 10 {
		for _, d := range claim(t, st, 50) {
			byEndpoint[d.EndpointID]++
		}
	}
	if byEndpoint[healthy.ID] != 5 {
		t.Fatalf("healthy endpoint got %d of its 5 deliveries", byEndpoint[healthy.ID])
	}
	for id, n := range byEndpoint {
		if id != healthy.ID && n != 3 {
			t.Fatalf("black-hole endpoint holds %d slots, want its cap of 3", n)
		}
	}
}

func TestCircuitBreakerLifecycle(t *testing.T) {
	st := testutil.NewStore(t)
	st.Breaker = store.BreakerPolicy{Threshold: 3, BaseOpen: time.Hour, MaxOpen: 24 * time.Hour}
	seed(t, st, 12)
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)

	// Three real failures through the normal outcome path open the circuit.
	for i := 1; i <= 3; i++ {
		got := claim(t, st, 1)
		if len(got) != 1 {
			t.Fatalf("failure %d: claimed %d", i, len(got))
		}
		fail(t, st, got[0], 500)
	}
	if open := scalar[bool](t, st, `SELECT circuit_open_until > now() FROM endpoints`); !open {
		t.Fatal("circuit did not open after 3 consecutive failures")
	}
	if got := claim(t, st, 10); len(got) != 0 {
		t.Fatalf("claimed %d deliveries from an endpoint with an open circuit", len(got))
	}
	// Deliveries waited; they did not burn attempts while the circuit was open.
	if n := scalar[int](t, st, `SELECT count(*) FROM deliveries WHERE status = 'pending' AND attempt_count = 0`); n != 9 {
		t.Fatalf("%d untouched deliveries, want 9", n)
	}

	// Window passes: half-open allows exactly one probe.
	exec(t, st, `UPDATE endpoints SET circuit_open_until = now() - interval '1 second'`)
	probe := claim(t, st, 10)
	if len(probe) != 1 {
		t.Fatalf("half-open endpoint let %d deliveries through, want a single probe", len(probe))
	}
	if more := claim(t, st, 10); len(more) != 0 {
		t.Fatalf("a second delivery was claimed while the probe is in flight")
	}

	// Probe fails: the circuit re-opens, for longer.
	fail(t, st, probe[0], 503)
	if trips := scalar[int](t, st, `SELECT circuit_trips FROM endpoints`); trips != 2 {
		t.Fatalf("circuit_trips = %d after a failed probe, want 2", trips)
	}
	if secs := scalar[float64](t, st, `SELECT extract(epoch FROM circuit_open_until - now()) FROM endpoints`); secs < 3600*1.9 {
		t.Fatalf("re-opened for only %.0fs; the window should have doubled to ~2h", secs)
	}

	// Next probe succeeds: the circuit closes and traffic flows normally again.
	exec(t, st, `UPDATE endpoints SET circuit_open_until = now() - interval '1 second'`)
	probe = claim(t, st, 10)
	if len(probe) != 1 {
		t.Fatalf("second probe: claimed %d", len(probe))
	}
	if err := st.CompleteDelivery(context.Background(), "w", probe[0], store.Attempt{StartedAt: time.Now(), StatusCode: 200}); err != nil {
		t.Fatal(err)
	}
	if f, trips, open := scalar[int](t, st, `SELECT consecutive_failures FROM endpoints`),
		scalar[int](t, st, `SELECT circuit_trips FROM endpoints`),
		scalar[bool](t, st, `SELECT circuit_open_until IS NOT NULL OR failing_since IS NOT NULL FROM endpoints`); f != 0 || trips != 0 || open {
		t.Fatalf("breaker not reset after success: failures=%d trips=%d stillMarked=%v", f, trips, open)
	}
	if got := claim(t, st, 10); len(got) != 5 { // back to the endpoint's normal cap
		t.Fatalf("after recovery claimed %d, want the normal cap of 5", len(got))
	}
}

func TestOnlyEndpointFaultsCountTowardTheBreaker(t *testing.T) {
	st := testutil.NewStore(t)
	st.Breaker = store.BreakerPolicy{Threshold: 2, BaseOpen: time.Hour, MaxOpen: time.Hour}
	seed(t, st, 6)
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)
	ctx := context.Background()

	d := claim(t, st, 1)[0]
	fail(t, st, d, 429) // rate limited: alive, just busy
	d = claim(t, st, 1)[0]
	if err := st.FailDelivery(ctx, "w", d, store.Attempt{StartedAt: time.Now(), Error: "cannot decrypt", Local: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if f := scalar[int](t, st, `SELECT consecutive_failures FROM endpoints`); f != 0 {
		t.Fatalf("429 and local failures counted toward the breaker: consecutive_failures=%d", f)
	}
	d = claim(t, st, 1)[0]
	fail(t, st, d, 500)
	if f := scalar[int](t, st, `SELECT consecutive_failures FROM endpoints`); f != 1 {
		t.Fatalf("a 500 should count once, got %d", f)
	}
}

func TestAutoDisableDeadLettersWaitingDeliveriesAndCanBeReversed(t *testing.T) {
	st := testutil.NewStore(t)
	st.Breaker = store.BreakerPolicy{Threshold: 100, BaseOpen: time.Minute, MaxOpen: time.Hour, DisableAfter: time.Hour}
	tenant := seed(t, st, 5)
	exec(t, st, `UPDATE tenants SET max_inflight = 1000`)
	exec(t, st, `UPDATE endpoints SET failing_since = now() - interval '2 hours', consecutive_failures = 50`)

	d := claim(t, st, 1)[0]
	fail(t, st, d, 500) // the failure that tips it over

	if status := scalar[string](t, st, `SELECT status FROM endpoints`); status != "disabled" {
		t.Fatalf("endpoint status = %q, want disabled", status)
	}
	if reason := scalar[string](t, st, `SELECT disabled_reason FROM endpoints`); reason == "" {
		t.Fatal("no disabled_reason recorded")
	}
	// The 4 deliveries still waiting, and the one that just failed, are in the DLQ.
	if n := scalar[int](t, st, `SELECT count(*) FROM deliveries WHERE status = 'dead' AND dead_reason = $1`, store.DeadEndpointDisabled); n != 5 {
		t.Fatalf("%d deliveries dead-lettered as endpoint_disabled, want 5", n)
	}
	if got := claim(t, st, 10); len(got) != 0 {
		t.Fatal("a disabled endpoint was still claimed")
	}

	// The tenant fixes the endpoint and re-enables it: health resets.
	active := "active"
	id := scalar[string](t, st, `SELECT id::text FROM endpoints`)
	ep, err := st.UpdateEndpoint(context.Background(), tenant, id, store.EndpointPatch{Status: &active})
	if err != nil {
		t.Fatal(err)
	}
	if ep.Status != "active" || ep.ConsecutiveFailures != 0 || ep.FailingSince != nil || ep.DisabledAt != nil || ep.DisabledReason != nil {
		t.Fatalf("re-enabled endpoint still carries failure state: %+v", ep)
	}
	// ...and can replay what was dead-lettered.
	if n, err := st.ReplayDead(context.Background(), tenant, store.DeadFilter{Reason: store.DeadEndpointDisabled}, 100, 0); err != nil || n != 5 {
		t.Fatalf("replayed %d (err %v), want 5", n, err)
	}
}
