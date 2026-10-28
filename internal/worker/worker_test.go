package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"webhookdelivery/internal/api"
	"webhookdelivery/internal/deliver"
	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
	"webhookdelivery/internal/worker"
)

// receiver is a fake customer endpoint that verifies signatures like a real one would.
type receiver struct {
	srv *httptest.Server

	mu     sync.Mutex
	hits   []hit
	status int    // response code to return
	secret []byte // signing key the receiver verifies with
}

type hit struct {
	webhookID string
	body      []byte
	verifyErr error
}

func newReceiver(t *testing.T, status int) *receiver {
	r := &receiver{status: status}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		defer r.mu.Unlock()
		err := sign.Verify(req.Header.Get("webhook-signature"), [][]byte{r.secret}, req.Header.Get("webhook-id"),
			req.Header.Get("webhook-timestamp"), body, 5*time.Minute, time.Now())
		r.hits = append(r.hits, hit{webhookID: req.Header.Get("webhook-id"), body: body, verifyErr: err})
		w.WriteHeader(r.status)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) setSecret(s []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secret = s
}

func (r *receiver) snapshot() []hit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hit(nil), r.hits...)
}

type system struct {
	t   *testing.T
	st  *store.Store
	api *httptest.Server
	key string
}

// newSystem wires the real API and a real worker to a fresh database.
func newSystem(t *testing.T) *system {
	t.Helper()
	st := testutil.NewStore(t)
	_, key := testutil.NewTenant(t, st, "acme")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mux := http.NewServeMux()
	api.New(st, log, true).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	w := worker.New(st, deliver.NewSender(5*time.Second), log, worker.Config{
		ID: "test-worker", BatchSize: 10, Concurrency: 10,
		PollInterval: 20 * time.Millisecond, Lease: time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	return &system{t: t, st: st, api: srv, key: key}
}

func (s *system) post(path string, body any) map[string]any {
	s.t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.api.URL+path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+s.key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode >= 300 {
		s.t.Fatalf("POST %s = %d %v", path, resp.StatusCode, out)
	}
	return out
}

// addEndpoint registers r as an endpoint and teaches it the generated secret.
func (s *system) addEndpoint(r *receiver, eventTypes ...string) {
	s.t.Helper()
	ep := s.post("/v1/endpoints", map[string]any{"url": r.srv.URL, "event_types": eventTypes})
	key, err := sign.ParseSecret(ep["secret"].(string))
	if err != nil {
		s.t.Fatal(err)
	}
	r.setSecret(key)
}

func (s *system) waitFor(cond func() bool, what string) {
	s.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.t.Fatalf("timed out waiting for %s", what)
}

func (s *system) count(query string) (n int) {
	_ = s.st.Pool.QueryRow(context.Background(), query).Scan(&n)
	return n
}

// Phase 1 exit criterion: one event is delivered exactly once to each of three
// subscribed endpoints, with a verifiable signature, and not to others.
func TestEventDeliveredOnceToEachSubscribedEndpoint(t *testing.T) {
	s := newSystem(t)
	subscribed := []*receiver{newReceiver(t, 200), newReceiver(t, 200), newReceiver(t, 200)}
	other := newReceiver(t, 200)
	for _, r := range subscribed {
		s.addEndpoint(r, "order.created")
	}
	s.addEndpoint(other, "user.created")

	ev := s.post("/v1/events", map[string]any{"type": "order.created", "payload": map[string]any{"order": 7}})
	if ev["deliveries"] != float64(3) {
		t.Fatalf("fan-out = %v, want 3", ev["deliveries"])
	}

	s.waitFor(func() bool { return s.count(`SELECT count(*) FROM deliveries WHERE status = 'succeeded'`) == 3 }, "3 successful deliveries")
	time.Sleep(200 * time.Millisecond) // give any erroneous duplicate send time to show up

	for i, r := range subscribed {
		hits := r.snapshot()
		if len(hits) != 1 {
			t.Fatalf("receiver %d got %d deliveries, want exactly 1", i, len(hits))
		}
		if hits[0].verifyErr != nil {
			t.Errorf("receiver %d: signature did not verify: %v", i, hits[0].verifyErr)
		}
		if hits[0].webhookID != ev["id"] {
			t.Errorf("receiver %d: webhook-id %q, want event id %q", i, hits[0].webhookID, ev["id"])
		}
		var env struct {
			ID, Type string
			Data     struct{ Order int }
		}
		if err := json.Unmarshal(hits[0].body, &env); err != nil || env.Type != "order.created" || env.Data.Order != 7 {
			t.Errorf("receiver %d: unexpected body %s (%v)", i, hits[0].body, err)
		}
	}
	if n := len(other.snapshot()); n != 0 {
		t.Errorf("unsubscribed endpoint received %d deliveries", n)
	}
	if n := s.count(`SELECT count(*) FROM attempts WHERE status_code = 200`); n != 3 {
		t.Errorf("recorded %d successful attempts, want 3", n)
	}
	if n := s.count(`SELECT count(*) FROM deliveries WHERE attempt_count <> 1`); n != 0 {
		t.Errorf("%d deliveries were attempted more than once", n)
	}
}

func TestFailedDeliveryIsRecordedAndRescheduled(t *testing.T) {
	s := newSystem(t)
	r := newReceiver(t, http.StatusInternalServerError)
	s.addEndpoint(r, "*")
	s.post("/v1/events", map[string]any{"type": "a.b", "payload": map[string]any{}})

	s.waitFor(func() bool { return s.count(`SELECT count(*) FROM attempts`) == 1 }, "failed attempt recorded")

	var status string
	var code int
	var delay float64
	err := s.st.Pool.QueryRow(context.Background(),
		`SELECT status, last_status_code, extract(epoch FROM next_attempt_at - now()) FROM deliveries`).
		Scan(&status, &code, &delay)
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" || code != 500 || delay < 20 {
		t.Fatalf("status=%s code=%d retry in %.0fs; want pending, 500, ~30s", status, code, delay)
	}
	if n := len(r.snapshot()); n != 1 {
		t.Fatalf("receiver hit %d times, want 1 (retry is 30s away)", n)
	}
}
