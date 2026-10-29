package invariants_test

import (
	"context"
	"strings"
	"testing"

	"webhookdelivery/internal/invariants"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

type obs map[invariants.Key]int

func (o obs) Count(ev, ep string) int { return o[invariants.Key{Event: ev, Endpoint: ep}] }
func (o obs) Each(f func(invariants.Key, int)) {
	for k, n := range o {
		f(k, n)
	}
}

// world builds 3 events x 2 endpoints, all delivered and received: the healthy baseline.
func world(t *testing.T) (*store.Store, []string, obs) {
	t.Helper()
	st := testutil.NewStore(t)
	ctx := context.Background()
	tenant, _ := testutil.NewTenant(t, st, "acme")
	for range 2 {
		if _, err := st.CreateEndpoint(ctx, store.Endpoint{TenantID: tenant, URL: "http://127.0.0.1:1/", EventTypes: []string{"*"}}); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	for range 3 {
		res, err := st.IngestEvent(ctx, tenant, "a.b", []byte(`{}`), "", 8)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.EventID)
	}
	if _, err := st.Pool.Exec(ctx, `UPDATE deliveries SET status = 'succeeded', attempt_count = 1`); err != nil {
		t.Fatal(err)
	}
	seen := obs{}
	rows, _ := st.Pool.Query(ctx, `SELECT event_id::text, endpoint_id::text FROM deliveries`)
	for rows.Next() {
		var ev, ep string
		_ = rows.Scan(&ev, &ep)
		seen[invariants.Key{Event: ev, Endpoint: ep}] = 1
	}
	rows.Close()
	return st, ids, seen
}

func check(t *testing.T, st *store.Store, ids []string, seen obs) invariants.Report {
	t.Helper()
	rep, err := invariants.Check(context.Background(), st.Pool, invariants.Expected{EventIDs: ids, Fanout: 2, Received: seen})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func violated(rep invariants.Report, name string) bool {
	for _, v := range rep.Violations {
		if v.Invariant == name {
			return true
		}
	}
	return false
}

func TestHealthyWorldPasses(t *testing.T) {
	st, ids, seen := world(t)
	if rep := check(t, st, ids, seen); !rep.OK() || rep.Deliveries != 6 || rep.ByStatus["succeeded"] != 6 {
		t.Fatalf("a healthy world failed: %s", rep)
	}
}

// Each invariant must actually be able to fail: a checker that never complains proves nothing.
func TestEachInvariantDetectsItsViolation(t *testing.T) {
	cases := map[string]struct {
		mutate func(t *testing.T, st *store.Store, ids *[]string, seen obs)
		want   string
	}{
		"durable": {want: "durable", mutate: func(t *testing.T, st *store.Store, ids *[]string, _ obs) {
			*ids = append(*ids, "00000000-0000-0000-0000-000000000001") // the client was told this was accepted
		}},
		"fan-out": {want: "fan-out", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `DELETE FROM deliveries WHERE id = (SELECT id FROM deliveries LIMIT 1)`)
		}},
		"terminal-pending": {want: "terminal", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `UPDATE deliveries SET status = 'pending' WHERE id = (SELECT id FROM deliveries LIMIT 1)`)
		}},
		"terminal-in-flight": {want: "terminal", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `UPDATE deliveries SET status = 'in_flight', locked_by = 'w', lease_expires_at = now() + interval '1 minute' WHERE id = (SELECT id FROM deliveries LIMIT 1)`)
		}},
		"no-strays": {want: "no-strays", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `UPDATE deliveries SET locked_by = 'ghost' WHERE id = (SELECT id FROM deliveries LIMIT 1)`)
		}},
		"no-loss": {want: "no-loss", mutate: func(t *testing.T, st *store.Store, _ *[]string, seen obs) {
			for k := range seen { // the endpoint never got it, yet the delivery says succeeded
				delete(seen, k)
				return
			}
		}},
		"explained": {want: "explained", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `UPDATE deliveries SET status = 'dead' WHERE id = (SELECT id FROM deliveries LIMIT 1)`)
		}},
		"bounded": {want: "bounded", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `UPDATE deliveries SET attempt_count = max_attempts + 1 WHERE id = (SELECT id FROM deliveries LIMIT 1)`)
		}},
		"bounded-rows": {want: "bounded-rows", mutate: func(t *testing.T, st *store.Store, _ *[]string, _ obs) {
			execSQL(t, st, `INSERT INTO attempts (delivery_id, attempt_no) SELECT id, 1 FROM deliveries LIMIT 1`) // attempt_count is 1
			execSQL(t, st, `INSERT INTO attempts (delivery_id, attempt_no) SELECT id, 2 FROM deliveries ORDER BY id LIMIT 1`)
			execSQL(t, st, `UPDATE deliveries SET attempt_count = 0`)
		}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			st, ids, seen := world(t)
			c.mutate(t, st, &ids, seen)
			rep := check(t, st, ids, seen)
			if !violated(rep, c.want) {
				t.Fatalf("expected a %q violation, got: %s", c.want, rep)
			}
		})
	}
}

func TestDuplicatesAreCountedButAreNotViolations(t *testing.T) {
	st, ids, seen := world(t)
	for k := range seen {
		seen[k] = 3 // at-least-once: redelivered after a crash
		break
	}
	rep := check(t, st, ids, seen)
	if !rep.OK() || rep.Duplicates != 1 || rep.MaxReceives != 3 {
		t.Fatalf("duplicates misreported: %s", rep)
	}
	if !strings.Contains(rep.String(), "duplicates=1") {
		t.Fatalf("report text omits duplicates: %s", rep)
	}
}

func execSQL(t *testing.T, st *store.Store, q string) {
	t.Helper()
	if _, err := st.Pool.Exec(context.Background(), q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}
