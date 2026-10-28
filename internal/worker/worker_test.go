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
	"webhookdelivery/internal/retry"
	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
	"webhookdelivery/internal/worker"
)

// response is what a fake receiver answers with.
type response struct {
	status  int
	headers map[string]string
}

func ok() func(int) response { return func(int) response { return response{status: 200} } }

func always(status int) func(int) response {
	return func(int) response { return response{status: status} }
}

// receiver is a fake customer endpoint that verifies signatures like a real
// one would. behave receives the 1-based number of the request.
type receiver struct {
	srv *httptest.Server

	mu     sync.Mutex
	hits   []hit
	behave func(n int) response
	secret []byte
}

type hit struct {
	at        time.Time
	webhookID string
	body      []byte
	verifyErr error
}

func newReceiver(t *testing.T, behave func(n int) response) *receiver {
	r := &receiver{behave: behave}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		defer r.mu.Unlock()
		err := sign.Verify(req.Header.Get("webhook-signature"), [][]byte{r.secret}, req.Header.Get("webhook-id"),
			req.Header.Get("webhook-timestamp"), body, 5*time.Minute, time.Now())
		r.hits = append(r.hits, hit{at: time.Now(), webhookID: req.Header.Get("webhook-id"), body: body, verifyErr: err})
		resp := r.behave(len(r.hits))
		for k, v := range resp.headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(resp.status)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) setSecret(s []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secret = s
}

func (r *receiver) setBehavior(f func(n int) response) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.behave = f
}

func (r *receiver) snapshot() []hit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hit(nil), r.hits...)
}

type opts struct {
	schedule    []time.Duration
	maxAttempts int
	lease       time.Duration
}

type system struct {
	t   *testing.T
	st  *store.Store
	api *httptest.Server
	key string
	o   opts
}

// newSystem wires the real API to a fresh database. Workers are started
// explicitly with startWorker so tests can set up state first.
func newSystem(t *testing.T, o opts) *system {
	t.Helper()
	if o.schedule == nil {
		o.schedule = []time.Duration{20 * time.Millisecond}
	}
	if o.maxAttempts == 0 {
		o.maxAttempts = 3
	}
	if o.lease == 0 {
		o.lease = time.Minute
	}
	st := testutil.NewStore(t)
	_, key := testutil.NewTenant(t, st, "acme")

	mux := http.NewServeMux()
	api.New(st, discard(), api.Options{AllowInsecureURLs: true, MaxAttempts: o.maxAttempts}).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &system{t: t, st: st, api: srv, key: key, o: o}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func (s *system) startWorker() {
	s.t.Helper()
	w := worker.New(s.st, deliver.NewSender(5*time.Second), discard(), worker.Config{
		ID: "test-worker", BatchSize: 10, TenantSlice: 10, Concurrency: 10,
		PollInterval: 20 * time.Millisecond, Lease: s.o.lease, ReaperInterval: 50 * time.Millisecond,
		Retry: retry.Policy{Schedule: s.o.schedule, MaxRetryAfter: time.Hour},
	})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = w.Run(ctx) }()
	go func() { defer wg.Done(); _ = w.RunReaper(ctx) }()
	s.t.Cleanup(func() { cancel(); wg.Wait() })
}

func (s *system) call(method, path string, body any) map[string]any {
	s.t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, s.api.URL+path, rd)
	req.Header.Set("Authorization", "Bearer "+s.key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode >= 300 {
		s.t.Fatalf("%s %s = %d %v", method, path, resp.StatusCode, out)
	}
	return out
}

func (s *system) post(path string, body any) map[string]any { return s.call("POST", path, body) }

// addEndpoint registers r as an endpoint and teaches it the generated secret.
func (s *system) addEndpoint(r *receiver, eventTypes ...string) string {
	s.t.Helper()
	ep := s.post("/v1/endpoints", map[string]any{"url": r.srv.URL, "event_types": eventTypes})
	key, err := sign.ParseSecret(ep["secret"].(string))
	if err != nil {
		s.t.Fatal(err)
	}
	r.setSecret(key)
	return ep["id"].(string)
}

func (s *system) event(typ string) map[string]any {
	return s.post("/v1/events", map[string]any{"type": typ, "payload": map[string]any{"n": 1}})
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

func (s *system) count(query string, args ...any) (n int) {
	_ = s.st.Pool.QueryRow(context.Background(), query, args...).Scan(&n)
	return n
}

func (s *system) statusCount(status string) int {
	return s.count(`SELECT count(*) FROM deliveries WHERE status = $1`, status)
}

func (s *system) scalar(dst any, query string, args ...any) {
	s.t.Helper()
	if err := s.st.Pool.QueryRow(context.Background(), query, args...).Scan(dst); err != nil {
		s.t.Fatalf("%s: %v", query, err)
	}
}

// Phase 1 exit criterion: one event is delivered exactly once to each of three
// subscribed endpoints, with a verifiable signature, and not to others.
func TestEventDeliveredOnceToEachSubscribedEndpoint(t *testing.T) {
	s := newSystem(t, opts{})
	subscribed := []*receiver{newReceiver(t, ok()), newReceiver(t, ok()), newReceiver(t, ok())}
	other := newReceiver(t, ok())
	for _, r := range subscribed {
		s.addEndpoint(r, "order.created")
	}
	s.addEndpoint(other, "user.created")
	s.startWorker()

	ev := s.post("/v1/events", map[string]any{"type": "order.created", "payload": map[string]any{"order": 7}})
	if ev["deliveries"] != float64(3) {
		t.Fatalf("fan-out = %v, want 3", ev["deliveries"])
	}

	s.waitFor(func() bool { return s.statusCount("succeeded") == 3 }, "3 successful deliveries")
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
	if n := s.count(`SELECT count(*) FROM deliveries WHERE attempt_count <> 1`); n != 0 {
		t.Errorf("%d deliveries were attempted more than once", n)
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 5})
	r := newReceiver(t, func(n int) response {
		if n <= 2 {
			return response{status: 500}
		}
		return response{status: 200}
	})
	s.addEndpoint(r, "*")
	s.startWorker()
	ev := s.event("a.b")

	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "delivery to succeed on the 3rd attempt")

	var codes []int32
	s.scalar(&codes, `SELECT array_agg(status_code ORDER BY id) FROM attempts`)
	if len(codes) != 3 || codes[0] != 500 || codes[1] != 500 || codes[2] != 200 {
		t.Fatalf("attempt status codes = %v, want [500 500 200]", codes)
	}
	for i, h := range r.snapshot() {
		if h.webhookID != ev["id"] {
			t.Errorf("attempt %d: webhook-id changed across retries: %q", i+1, h.webhookID)
		}
		if h.verifyErr != nil {
			t.Errorf("attempt %d: signature did not verify: %v", i+1, h.verifyErr)
		}
	}
	if n := s.count(`SELECT attempt_count FROM deliveries`); n != 3 {
		t.Errorf("attempt_count = %d, want 3", n)
	}
}

func TestExhaustedDeliveryGoesToDLQAndCanBeReplayed(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 3})
	r := newReceiver(t, always(500))
	s.addEndpoint(r, "*")
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool { return s.statusCount("dead") == 1 }, "delivery to be dead-lettered")
	time.Sleep(200 * time.Millisecond) // a dead delivery must not be retried again
	if n := len(r.snapshot()); n != 3 {
		t.Fatalf("receiver was hit %d times, want exactly max_attempts=3", n)
	}
	var reason string
	s.scalar(&reason, `SELECT dead_reason FROM deliveries`)
	if reason != store.DeadMaxAttempts {
		t.Fatalf("dead_reason = %q", reason)
	}

	dlq := s.call("GET", "/v1/dlq", nil)["data"].([]any)
	if len(dlq) != 1 {
		t.Fatalf("DLQ has %d entries, want 1", len(dlq))
	}
	id := dlq[0].(map[string]any)["delivery_id"].(string)

	// The customer fixes their endpoint, then replays the failed delivery.
	r.setBehavior(ok())
	s.post("/v1/deliveries/"+id+"/replay", nil)
	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "replayed delivery to succeed")

	if n := len(r.snapshot()); n != 4 {
		t.Errorf("receiver hits = %d, want 4 (3 failures + 1 replay)", n)
	}
	if n := s.count(`SELECT attempt_count FROM deliveries`); n != 1 {
		t.Errorf("attempt_count after replay = %d, want a fresh budget (1)", n)
	}
	if n := s.count(`SELECT count(*) FROM delivery_replays`); n != 1 {
		t.Errorf("replay audit rows = %d, want 1", n)
	}
	// History is preserved across the replay.
	detail := s.call("GET", "/v1/deliveries/"+id, nil)
	if got := len(detail["attempts"].([]any)); got != 4 {
		t.Errorf("attempt history has %d entries, want 4", got)
	}
}

func TestGoneEndpointIsDeadLetteredAndPaused(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 5})
	r := newReceiver(t, always(http.StatusGone))
	epID := s.addEndpoint(r, "*")
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool { return s.statusCount("dead") == 1 }, "410 to dead-letter the delivery")
	time.Sleep(150 * time.Millisecond)

	var reason, epStatus string
	s.scalar(&reason, `SELECT dead_reason FROM deliveries`)
	s.scalar(&epStatus, `SELECT status FROM endpoints WHERE id = $1`, epID)
	if reason != store.DeadEndpointGone || epStatus != "paused" {
		t.Fatalf("dead_reason=%q endpoint status=%q, want endpoint_gone/paused", reason, epStatus)
	}
	if n := len(r.snapshot()); n != 1 {
		t.Fatalf("receiver hit %d times, want 1 (410 is not retried)", n)
	}
	if ev := s.event("a.b"); ev["deliveries"] != float64(0) {
		t.Fatalf("paused endpoint still receives new events: %v", ev)
	}
}

func TestRetryAfterIsHonoredOnlyFor429And503(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 5}) // backoff is only 20ms, so any long wait comes from Retry-After
	r := newReceiver(t, func(n int) response {
		switch n {
		case 1: // Retry-After on a 500 is ignored
			return response{status: 500, headers: map[string]string{"Retry-After": "100"}}
		case 2:
			return response{status: 429, headers: map[string]string{"Retry-After": "2"}}
		}
		return response{status: 200}
	})
	s.addEndpoint(r, "*")
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "delivery to succeed")
	hits := r.snapshot()
	if len(hits) != 3 {
		t.Fatalf("hits = %d, want 3", len(hits))
	}
	if gap := hits[1].at.Sub(hits[0].at); gap > time.Second {
		t.Errorf("retry after a 500 waited %v; its Retry-After should have been ignored", gap)
	}
	if gap := hits[2].at.Sub(hits[1].at); gap < 1900*time.Millisecond {
		t.Errorf("retry after 429 Retry-After: 2 came after %v, want >= 2s", gap)
	}
}

// Equivalent to kill -9 on a worker mid-delivery: the delivery is leased but
// never completed. The reaper must notice the expired lease and requeue it.
func TestDeliveryOfCrashedWorkerIsRecovered(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 5})
	r := newReceiver(t, ok())
	s.addEndpoint(r, "*")
	s.event("a.b")

	claimed, err := s.st.ClaimDeliveries(context.Background(), "crashed-worker", 10, 10, 200*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("simulated crash claim: %v %v", claimed, err)
	}
	// ...and the crashed worker is never heard from again.

	s.startWorker()
	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "delivery to be recovered and delivered")

	if n := len(r.snapshot()); n != 1 {
		t.Errorf("receiver hits = %d, want 1", n)
	}
	var nos []int32
	var firstErr string
	s.scalar(&nos, `SELECT array_agg(attempt_no ORDER BY id) FROM attempts`)
	s.scalar(&firstErr, `SELECT error FROM attempts ORDER BY id LIMIT 1`)
	if len(nos) != 2 || nos[0] != 1 || nos[1] != 2 {
		t.Errorf("attempt numbers = %v, want [1 2] (the lost attempt counts)", nos)
	}
	if firstErr == "" {
		t.Error("the lost attempt was not recorded with an error")
	}
}

func TestSlowReceiverDoesNotBlockOthers(t *testing.T) {
	s := newSystem(t, opts{})
	release := make(chan struct{})
	slow := newReceiver(t, ok())
	slow.srv.Config.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release })
	fast := newReceiver(t, ok())
	s.addEndpoint(slow, "*")
	s.addEndpoint(fast, "*")
	t.Cleanup(func() { close(release) })
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool { return len(fast.snapshot()) == 1 && s.statusCount("succeeded") == 1 },
		"the fast endpoint to be delivered while the slow one hangs")
	if s.statusCount("in_flight") != 1 {
		t.Fatalf("expected the slow delivery to still be in flight")
	}
}
