package api_test

import (
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

// newDead creates an event, then marks its delivery to epID as dead.
func (e *env) newDead(epID, reason string, at time.Time) string {
	e.t.Helper()
	_, ev := e.do(e.key, "POST", "/v1/events", map[string]any{"type": "x.y", "payload": map[string]any{}})
	_, ds := e.do(e.key, "GET", "/v1/events/"+ev["id"].(string)+"/deliveries", nil)
	for _, d := range ds["data"].([]any) {
		m := d.(map[string]any)
		if m["endpoint_id"] != epID {
			continue
		}
		id := m["id"].(string)
		if _, err := e.st.Pool.Exec(context.Background(),
			`UPDATE deliveries SET status = 'dead', attempt_count = 8, dead_at = $2, dead_reason = $3, last_error = 'boom'
			  WHERE id = $1`, id, at, reason); err != nil {
			e.t.Fatal(err)
		}
		return id
	}
	e.t.Fatalf("no delivery for endpoint %s", epID)
	return ""
}

func ids(t *testing.T, resp map[string]any) []string {
	t.Helper()
	var out []string
	for _, row := range resp["data"].([]any) {
		out = append(out, row.(map[string]any)["delivery_id"].(string))
	}
	return out
}

func TestDLQListFiltersAndPagination(t *testing.T) {
	e := newEnv(t, true)
	epA, epB := e.mustCreateEndpoint("*"), e.mustCreateEndpoint("*")
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	d1 := e.newDead(epA, store.DeadMaxAttempts, base)
	d2 := e.newDead(epA, store.DeadEndpointGone, base.Add(time.Minute))
	d3 := e.newDead(epB, store.DeadMaxAttempts, base.Add(2*time.Minute))

	list := func(query string) map[string]any {
		status, out := e.do(e.key, "GET", "/v1/dlq"+query, nil)
		if status != http.StatusOK {
			t.Fatalf("GET /v1/dlq%s = %d %v", query, status, out)
		}
		return out
	}
	check := func(query string, want ...string) {
		t.Helper()
		if got := ids(t, list(query)); !slices.Equal(got, want) {
			t.Errorf("GET /v1/dlq%s = %v, want %v", query, got, want)
		}
	}

	check("", d3, d2, d1) // newest first
	check("?reason=endpoint_gone", d2)
	check("?endpoint_id="+epB, d3)
	check("?endpoint_id="+epA+"&reason=max_attempts", d1)
	check("?since="+base.Add(90*time.Second).UTC().Format(time.RFC3339), d3)

	page1 := list("?limit=2")
	next, _ := page1["next_after"].(string)
	if !slices.Equal(ids(t, page1), []string{d3, d2}) || next == "" {
		t.Fatalf("page1 = %v next=%q", ids(t, page1), next)
	}
	page2 := list("?limit=2&after=" + next)
	if !slices.Equal(ids(t, page2), []string{d1}) || page2["next_after"] != nil {
		t.Fatalf("page2 = %v next=%v", ids(t, page2), page2["next_after"])
	}

	for _, bad := range []string{"?reason=nope", "?endpoint_id=x", "?since=yesterday", "?limit=0", "?limit=101", "?after=garbage"} {
		if status, _ := e.do(e.key, "GET", "/v1/dlq"+bad, nil); status != http.StatusBadRequest {
			t.Errorf("GET /v1/dlq%s = %d, want 400", bad, status)
		}
	}

	_, keyB := testutil.NewTenant(t, e.st, "globex")
	if _, out := e.do(keyB, "GET", "/v1/dlq", nil); len(out["data"].([]any)) != 0 {
		t.Errorf("another tenant sees this tenant's DLQ: %v", out)
	}
}

func TestReplaySingleDelivery(t *testing.T) {
	e := newEnv(t, true)
	ep := e.mustCreateEndpoint("*")
	dead := e.newDead(ep, store.DeadMaxAttempts, time.Now())
	_, keyB := testutil.NewTenant(t, e.st, "globex")

	if status, _ := e.do(keyB, "POST", "/v1/deliveries/"+dead+"/replay", nil); status != http.StatusNotFound {
		t.Fatalf("other tenant replay = %d, want 404", status)
	}
	if status, _ := e.do(e.key, "POST", "/v1/deliveries/00000000-0000-0000-0000-000000000000/replay", nil); status != http.StatusNotFound {
		t.Fatalf("unknown delivery replay = %d, want 404", status)
	}

	status, out := e.do(e.key, "POST", "/v1/deliveries/"+dead+"/replay", nil)
	if status != http.StatusAccepted || out["replayed"] != float64(1) {
		t.Fatalf("replay = %d %v", status, out)
	}
	_, d := e.do(e.key, "GET", "/v1/deliveries/"+dead, nil)
	if d["status"] != "pending" || d["attempt_count"] != float64(0) || d["dead_reason"] != nil {
		t.Fatalf("replayed delivery = %v", d)
	}
	var audits int
	_ = e.st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM delivery_replays WHERE delivery_id = $1`, dead).Scan(&audits)
	if audits != 1 {
		t.Fatalf("audit rows = %d, want 1", audits)
	}

	if status, _ = e.do(e.key, "POST", "/v1/deliveries/"+dead+"/replay", nil); status != http.StatusConflict {
		t.Fatalf("replaying a pending delivery = %d, want 409", status)
	}
}

func TestBulkReplayIsRateLimitedAndFiltered(t *testing.T) {
	e := newEnv(t, true)
	epA, epB := e.mustCreateEndpoint("*"), e.mustCreateEndpoint("*")
	base := time.Now().Add(-time.Hour)
	d1 := e.newDead(epA, store.DeadMaxAttempts, base)
	d2 := e.newDead(epA, store.DeadMaxAttempts, base.Add(time.Minute))
	d3 := e.newDead(epA, store.DeadMaxAttempts, base.Add(2*time.Minute))
	other := e.newDead(epB, store.DeadMaxAttempts, base)

	status, out := e.do(e.key, "POST", "/v1/dlq/replay", map[string]any{"endpoint_id": epA, "rate_per_second": 2})
	if status != http.StatusAccepted || out["replayed"] != float64(3) {
		t.Fatalf("bulk replay = %d %v", status, out)
	}

	// Due times are spread 0.5s apart (2/s), oldest dead delivery first.
	var gap1, gap2 float64
	q := `SELECT extract(epoch FROM (SELECT next_attempt_at FROM deliveries WHERE id = $2) -
	                              (SELECT next_attempt_at FROM deliveries WHERE id = $1))`
	_ = e.st.Pool.QueryRow(context.Background(), q, d1, d2).Scan(&gap1)
	_ = e.st.Pool.QueryRow(context.Background(), q, d2, d3).Scan(&gap2)
	if gap1 < 0.4 || gap1 > 0.6 || gap2 < 0.4 || gap2 > 0.6 {
		t.Fatalf("replay spacing = %.2fs, %.2fs; want ~0.5s each", gap1, gap2)
	}

	// The other endpoint's dead delivery was outside the filter.
	_, d := e.do(e.key, "GET", "/v1/deliveries/"+other, nil)
	if d["status"] != "dead" {
		t.Fatalf("delivery outside the filter was replayed: %v", d["status"])
	}

	for name, body := range map[string]any{
		"limit too big": map[string]any{"limit": 5000},
		"zero rate":     map[string]any{"rate_per_second": 0},
		"huge rate":     map[string]any{"rate_per_second": 5000},
		"bad reason":    map[string]any{"reason": "nope"},
		"bad endpoint":  map[string]any{"endpoint_id": "x"},
	} {
		if status, _ := e.do(e.key, "POST", "/v1/dlq/replay", body); status != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, status)
		}
	}
	// Nothing left to replay for A: replayed count is 0, not an error.
	if status, out = e.do(e.key, "POST", "/v1/dlq/replay", map[string]any{"endpoint_id": epA}); status != http.StatusAccepted || out["replayed"] != float64(0) {
		t.Fatalf("empty replay = %d %v", status, out)
	}
}

func TestGetDeliveryShowsAttemptHistory(t *testing.T) {
	e := newEnv(t, true)
	ep := e.mustCreateEndpoint("*")
	id := e.newDead(ep, store.DeadMaxAttempts, time.Now())
	if _, err := e.st.Pool.Exec(context.Background(),
		`INSERT INTO attempts (delivery_id, attempt_no, duration_ms, status_code, response_body_snippet)
		 VALUES ($1, 1, 12, 503, 'upstream down'), ($1, 2, 9, 500, 'oops')`, id); err != nil {
		t.Fatal(err)
	}
	status, d := e.do(e.key, "GET", "/v1/deliveries/"+id, nil)
	atts, _ := d["attempts"].([]any)
	if status != http.StatusOK || len(atts) != 2 || atts[0].(map[string]any)["status_code"] != float64(503) {
		t.Fatalf("delivery detail = %d %v", status, d)
	}
	if d["dead_reason"] != store.DeadMaxAttempts || d["event_type"] != "x.y" {
		t.Fatalf("delivery detail = %v", d)
	}
}
