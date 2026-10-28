package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"webhookdelivery/internal/api"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

type env struct {
	t      *testing.T
	srv    *httptest.Server
	st     *store.Store
	tenant string
	key    string
}

func newEnv(t *testing.T, allowInsecure bool) *env {
	t.Helper()
	st := testutil.NewStore(t)
	tenant, key := testutil.NewTenant(t, st, "acme")
	mux := http.NewServeMux()
	api.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), allowInsecure).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, st: st, tenant: tenant, key: key}
}

// do sends a request and decodes the JSON response (nil body on 204).
func (e *env) do(key, method, path string, body any) (int, map[string]any) {
	e.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			e.t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if raw, _ := io.ReadAll(resp.Body); len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			e.t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return resp.StatusCode, out
}

func (e *env) mustCreateEndpoint(types ...string) string {
	e.t.Helper()
	status, ep := e.do(e.key, "POST", "/v1/endpoints", map[string]any{
		"url": "http://127.0.0.1:1/hook", "event_types": types,
	})
	if status != http.StatusCreated {
		e.t.Fatalf("create endpoint: %d %v", status, ep)
	}
	return ep["id"].(string)
}

func TestEndpointCRUD(t *testing.T) {
	e := newEnv(t, true)

	status, ep := e.do(e.key, "POST", "/v1/endpoints", map[string]any{
		"url": "http://example.test/hook", "description": "orders", "event_types": []string{"invoice.paid"},
	})
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, ep)
	}
	if s, _ := ep["secret"].(string); !strings.HasPrefix(s, "whsec_") {
		t.Fatalf("create should return a generated secret, got %v", ep["secret"])
	}
	id := ep["id"].(string)

	status, got := e.do(e.key, "GET", "/v1/endpoints/"+id, nil)
	if status != http.StatusOK || got["description"] != "orders" {
		t.Fatalf("get = %d %v", status, got)
	}
	if _, leaked := got["secret"]; leaked {
		t.Fatal("GET must not return the secret")
	}

	status, got = e.do(e.key, "PATCH", "/v1/endpoints/"+id, map[string]any{"status": "paused"})
	if status != http.StatusOK || got["status"] != "paused" || got["description"] != "orders" {
		t.Fatalf("patch = %d %v", status, got)
	}

	status, list := e.do(e.key, "GET", "/v1/endpoints", nil)
	if status != http.StatusOK || len(list["data"].([]any)) != 1 {
		t.Fatalf("list = %d %v", status, list)
	}

	if status, _ = e.do(e.key, "DELETE", "/v1/endpoints/"+id, nil); status != http.StatusNoContent {
		t.Fatalf("delete = %d", status)
	}
	if status, _ = e.do(e.key, "GET", "/v1/endpoints/"+id, nil); status != http.StatusNotFound {
		t.Fatalf("get after delete = %d", status)
	}
	if status, _ = e.do(e.key, "GET", "/v1/endpoints/not-a-uuid", nil); status != http.StatusNotFound {
		t.Fatalf("get bad id = %d", status)
	}
}

func TestEndpointPagination(t *testing.T) {
	e := newEnv(t, true)
	for range 3 {
		e.mustCreateEndpoint("*")
	}
	_, page1 := e.do(e.key, "GET", "/v1/endpoints?limit=2", nil)
	next, _ := page1["next_after"].(string)
	if len(page1["data"].([]any)) != 2 || next == "" {
		t.Fatalf("page1 = %v", page1)
	}
	_, page2 := e.do(e.key, "GET", "/v1/endpoints?limit=2&after="+next, nil)
	if len(page2["data"].([]any)) != 1 || page2["next_after"] != nil {
		t.Fatalf("page2 = %v", page2)
	}
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t, true)

	for name, key := range map[string]string{"missing": "", "invalid": "whk_nope"} {
		if status, _ := e.do(key, "GET", "/v1/endpoints", nil); status != http.StatusUnauthorized {
			t.Errorf("%s key: status = %d, want 401", name, status)
		}
	}

	if _, err := e.st.Pool.Exec(context.Background(), `UPDATE tenants SET status = 'suspended'`); err != nil {
		t.Fatal(err)
	}
	if status, _ := e.do(e.key, "GET", "/v1/endpoints", nil); status != http.StatusUnauthorized {
		t.Errorf("suspended tenant: status = %d, want 401", status)
	}
}

func TestTenantIsolation(t *testing.T) {
	e := newEnv(t, true)
	_, keyB := testutil.NewTenant(t, e.st, "globex")

	epID := e.mustCreateEndpoint("*")
	_, ev := e.do(e.key, "POST", "/v1/events", map[string]any{"type": "a.b", "payload": map[string]any{}})
	evID := ev["id"].(string)

	checks := []struct{ method, path string }{
		{"GET", "/v1/endpoints/" + epID},
		{"PATCH", "/v1/endpoints/" + epID},
		{"DELETE", "/v1/endpoints/" + epID},
		{"GET", "/v1/events/" + evID + "/deliveries"},
	}
	for _, c := range checks {
		var body any
		if c.method == "PATCH" {
			body = map[string]any{"status": "paused"}
		}
		if status, _ := e.do(keyB, c.method, c.path, body); status != http.StatusNotFound {
			t.Errorf("tenant B %s %s = %d, want 404", c.method, c.path, status)
		}
	}
	if _, list := e.do(keyB, "GET", "/v1/endpoints", nil); len(list["data"].([]any)) != 0 {
		t.Errorf("tenant B sees tenant A endpoints: %v", list)
	}
	// Tenant A's endpoint must be untouched by B's attempted PATCH/DELETE.
	if status, got := e.do(e.key, "GET", "/v1/endpoints/"+epID, nil); status != 200 || got["status"] != "active" {
		t.Errorf("endpoint was modified by another tenant: %d %v", status, got)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, false) // production-like: https only

	bad := map[string]any{
		"http url":          map[string]any{"url": "http://example.com/hook"},
		"missing url":       map[string]any{"description": "x"},
		"relative url":      map[string]any{"url": "/hook"},
		"url credentials":   map[string]any{"url": "https://user:pw@example.com/"}, //nolint:gosec // deliberately invalid input
		"bad event type":    map[string]any{"url": "https://example.com/", "event_types": []string{"has space"}},
		"bad secret":        map[string]any{"url": "https://example.com/", "secret": "plaintext"},
		"unknown field":     map[string]any{"url": "https://example.com/", "nope": 1},
		"malformed json":    `{"url":`,
		"trailing garbage":  `{"url":"https://example.com/"} {}`,
		"empty body":        "",
		"non-object params": `[]`,
	}
	for name, body := range bad {
		if status, _ := e.do(e.key, "POST", "/v1/endpoints", body); status != http.StatusBadRequest {
			t.Errorf("endpoint %s: status = %d, want 400", name, status)
		}
	}

	badEvents := map[string]any{
		"missing type":    map[string]any{"payload": map[string]any{}},
		"invalid type":    map[string]any{"type": "a b", "payload": map[string]any{}},
		"wildcard type":   map[string]any{"type": "*", "payload": map[string]any{}},
		"missing payload": map[string]any{"type": "a.b"},
		"null payload":    `{"type":"a.b","payload":null}`,
	}
	for name, body := range badEvents {
		if status, _ := e.do(e.key, "POST", "/v1/events", body); status != http.StatusBadRequest {
			t.Errorf("event %s: status = %d, want 400", name, status)
		}
	}

	huge := map[string]any{"type": "a.b", "payload": map[string]any{"blob": strings.Repeat("x", 300<<10)}}
	if status, _ := e.do(e.key, "POST", "/v1/events", huge); status != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized event: status = %d, want 413", status)
	}
}

func TestEventFanoutAndIdempotency(t *testing.T) {
	e := newEnv(t, true)
	e.mustCreateEndpoint("invoice.paid")           // matches
	e.mustCreateEndpoint("*")                      // matches
	e.mustCreateEndpoint("user.created")           // wrong type
	paused := e.mustCreateEndpoint("invoice.paid") // matches but paused
	if status, _ := e.do(e.key, "PATCH", "/v1/endpoints/"+paused, map[string]any{"status": "paused"}); status != 200 {
		t.Fatal("pause failed")
	}

	post := func(idem string) (int, map[string]any) {
		raw, _ := json.Marshal(map[string]any{"type": "invoice.paid", "payload": map[string]any{"amount": 42}})
		req, _ := http.NewRequestWithContext(context.Background(), "POST", e.srv.URL+"/v1/events", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+e.key)
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	status, first := post("order-123")
	if status != http.StatusAccepted || first["deliveries"] != float64(2) || first["duplicate"] != false {
		t.Fatalf("first = %d %v", status, first)
	}
	status, again := post("order-123")
	if status != http.StatusOK || again["duplicate"] != true || again["id"] != first["id"] || again["deliveries"] != float64(2) {
		t.Fatalf("duplicate = %d %v", status, again)
	}

	var events, deliveries int
	_ = e.st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM events`).Scan(&events)
	_ = e.st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM deliveries`).Scan(&deliveries)
	if events != 1 || deliveries != 2 {
		t.Fatalf("idempotent replay wrote rows: events=%d deliveries=%d", events, deliveries)
	}

	// Without a key every request is a new event.
	if _, second := post(""); second["id"] == first["id"] {
		t.Fatal("request without Idempotency-Key was deduplicated")
	}

	status, ds := e.do(e.key, "GET", "/v1/events/"+first["id"].(string)+"/deliveries", nil)
	if status != http.StatusOK || len(ds["data"].([]any)) != 2 {
		t.Fatalf("deliveries = %d %v", status, ds)
	}
}
