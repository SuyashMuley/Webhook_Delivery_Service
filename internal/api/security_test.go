package api_test

import (
	"net/http"
	"testing"

	"webhookdelivery/internal/testutil"
)

func TestSecretRotationAPI(t *testing.T) {
	e := newEnv(t, true)
	status, ep := e.do(e.key, "POST", "/v1/endpoints", map[string]any{"url": "http://example.test/hook"})
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, ep)
	}
	id, first := ep["id"].(string), ep["secret"].(string)
	base := "/v1/endpoints/" + id + "/secrets"

	_, list := e.do(e.key, "GET", base, nil)
	if len(list["data"].([]any)) != 1 {
		t.Fatalf("new endpoint should have 1 secret, got %v", list)
	}

	status, rot := e.do(e.key, "POST", base+"/rotate", map[string]any{"grace_period": "2h"})
	if status != http.StatusCreated {
		t.Fatalf("rotate = %d %v", status, rot)
	}
	second := rot["secret"].(string)
	if second == first || second == "" {
		t.Fatalf("rotation returned %q (old %q)", second, first)
	}
	all := rot["secrets"].([]any)
	if len(all) != 2 {
		t.Fatalf("after rotation there should be 2 secrets, got %v", all)
	}
	var oldID string
	expiring := 0
	for _, s := range all {
		m := s.(map[string]any)
		if m["expires_at"] != nil {
			expiring++
			oldID = m["id"].(string)
		}
		if _, leaked := m["value"]; leaked {
			t.Fatal("secret value leaked in the listing")
		}
	}
	if expiring != 1 {
		t.Fatalf("exactly the old secret should expire, got %d expiring", expiring)
	}

	// Rotating with an explicit secret, a hard cut (grace 0s), and bad input.
	custom := "whsec_Q1VTVE9NQ1VTVE9NQ1VTVE9NQ1VTVE9NQ1VTVE9NQ1U="
	if status, out := e.do(e.key, "POST", base+"/rotate", map[string]any{"secret": custom, "grace_period": "0s"}); status != http.StatusCreated || out["secret"] != custom || len(out["secrets"].([]any)) != 1 {
		t.Fatalf("hard rotate with a chosen secret = %d %v", status, out)
	}
	for name, body := range map[string]any{
		"negative grace": map[string]any{"grace_period": "-1h"},
		"huge grace":     map[string]any{"grace_period": "8760h"},
		"not a duration": map[string]any{"grace_period": "soon"},
		"bad secret":     map[string]any{"secret": "plaintext"},
	} {
		if status, _ := e.do(e.key, "POST", base+"/rotate", body); status != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, status)
		}
	}

	// Revoke: unknown is 404, the last secret is protected.
	if status, _ := e.do(e.key, "DELETE", base+"/"+oldID, nil); status != http.StatusNotFound {
		t.Errorf("revoking an already-removed secret = %d, want 404", status)
	}
	_, cur := e.do(e.key, "GET", base, nil)
	lastID := cur["data"].([]any)[0].(map[string]any)["id"].(string)
	if status, out := e.do(e.key, "DELETE", base+"/"+lastID, nil); status != http.StatusConflict {
		t.Errorf("revoking the last secret = %d %v, want 409", status, out)
	}

	// Another tenant can see none of this.
	_, keyB := testutil.NewTenant(t, e.st, "globex")
	for _, c := range []struct{ method, path string }{
		{"GET", base}, {"POST", base + "/rotate"}, {"DELETE", base + "/" + lastID},
	} {
		if status, _ := e.do(keyB, c.method, c.path, nil); status != http.StatusNotFound {
			t.Errorf("tenant B %s %s = %d, want 404", c.method, c.path, status)
		}
	}
}

func TestRotationIsBoundedByLiveSecretLimit(t *testing.T) {
	e := newEnv(t, true)
	id := e.mustCreateEndpoint("*")
	var last int
	for range 8 {
		last, _ = e.do(e.key, "POST", "/v1/endpoints/"+id+"/secrets/rotate", map[string]any{"grace_period": "24h"})
		if last == http.StatusConflict {
			return
		}
	}
	t.Fatalf("rotation never hit the live-secret limit (last status %d)", last)
}

// In production-like mode (no insecure URLs) internal destinations are rejected
// at registration for fast feedback. The sender enforces the same at connect time.
func TestEndpointURLsMustBePublicInProduction(t *testing.T) {
	e := newEnv(t, false)
	for _, u := range []string{
		"https://127.0.0.1/hook",
		"https://localhost/hook",
		"https://10.1.2.3/hook",
		"https://192.168.0.10:8443/hook",
		"https://169.254.169.254/latest/meta-data/",
		"https://[::1]/hook",
		"https://db.internal/hook",
	} {
		if status, out := e.do(e.key, "POST", "/v1/endpoints", map[string]any{"url": u}); status != http.StatusBadRequest {
			t.Errorf("POST url=%s: status = %d %v, want 400", u, status, out)
		}
	}
	if status, out := e.do(e.key, "POST", "/v1/endpoints", map[string]any{"url": "https://example.com/hook"}); status != http.StatusCreated {
		t.Errorf("public https url rejected: %d %v", status, out)
	}
	// Also enforced on update.
	id := e.mustCreateEndpointProd()
	if status, _ := e.do(e.key, "PATCH", "/v1/endpoints/"+id, map[string]any{"url": "https://127.0.0.1/x"}); status != http.StatusBadRequest {
		t.Errorf("PATCH to an internal url = %d, want 400", status)
	}
}

func (e *env) mustCreateEndpointProd() string {
	e.t.Helper()
	status, ep := e.do(e.key, "POST", "/v1/endpoints", map[string]any{"url": "https://example.org/hook"})
	if status != http.StatusCreated {
		e.t.Fatalf("create = %d %v", status, ep)
	}
	return ep["id"].(string)
}

func TestEndpointShowsHealthAndReenableClearsIt(t *testing.T) {
	e := newEnv(t, true)
	id := e.mustCreateEndpoint("*")

	_, ep := e.do(e.key, "GET", "/v1/endpoints/"+id, nil)
	h, ok := ep["health"].(map[string]any)
	if !ok || h["consecutive_failures"] != float64(0) || h["circuit_open_until"] != nil {
		t.Fatalf("new endpoint health = %v", ep["health"])
	}

	if _, err := e.st.Pool.Exec(t.Context(),
		`UPDATE endpoints SET status = 'disabled', disabled_at = now(), disabled_reason = 'failing continuously',
		        consecutive_failures = 40, failing_since = now() - interval '3 days',
		        circuit_open_until = now() + interval '1 hour', circuit_trips = 4`); err != nil {
		t.Fatal(err)
	}
	_, ep = e.do(e.key, "GET", "/v1/endpoints/"+id, nil)
	h = ep["health"].(map[string]any)
	if ep["status"] != "disabled" || ep["disabled_reason"] != "failing continuously" || h["consecutive_failures"] != float64(40) || h["circuit_open_until"] == nil {
		t.Fatalf("disabled endpoint view = %v", ep)
	}

	status, ep := e.do(e.key, "PATCH", "/v1/endpoints/"+id, map[string]any{"status": "active"})
	h = ep["health"].(map[string]any)
	if status != http.StatusOK || ep["status"] != "active" || ep["disabled_reason"] != nil ||
		h["consecutive_failures"] != float64(0) || h["circuit_open_until"] != nil || h["failing_since"] != nil {
		t.Fatalf("re-enable did not clear failure state: %d %v", status, ep)
	}
}

func TestEndpointConcurrencyIsConfigurable(t *testing.T) {
	e := newEnv(t, true)
	status, ep := e.do(e.key, "POST", "/v1/endpoints", map[string]any{"url": "http://example.test/a"})
	if status != http.StatusCreated || ep["max_concurrency"] != float64(5) {
		t.Fatalf("default max_concurrency: %d %v", status, ep["max_concurrency"])
	}
	status, ep = e.do(e.key, "POST", "/v1/endpoints", map[string]any{"url": "http://example.test/b", "max_concurrency": 20})
	if status != http.StatusCreated || ep["max_concurrency"] != float64(20) {
		t.Fatalf("create with max_concurrency: %d %v", status, ep)
	}
	id := ep["id"].(string)
	if status, ep = e.do(e.key, "PATCH", "/v1/endpoints/"+id, map[string]any{"max_concurrency": 2}); status != http.StatusOK || ep["max_concurrency"] != float64(2) {
		t.Fatalf("patch max_concurrency: %d %v", status, ep)
	}
	for _, bad := range []int{0, -1, 101, 100000} {
		if status, _ := e.do(e.key, "PATCH", "/v1/endpoints/"+id, map[string]any{"max_concurrency": bad}); status != http.StatusBadRequest {
			t.Errorf("max_concurrency=%d: status = %d, want 400", bad, status)
		}
	}
	// Other fields are untouched by a concurrency-only patch.
	if _, got := e.do(e.key, "GET", "/v1/endpoints/"+id, nil); got["url"] != "http://example.test/b" || got["status"] != "active" {
		t.Fatalf("concurrency patch changed other fields: %v", got)
	}
}
