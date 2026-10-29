package worker_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"webhookdelivery/internal/store"
	tu "webhookdelivery/internal/testutil"
	"webhookdelivery/internal/worker"
)

func TestShutdownAbortsSlowDeliveriesAndHandsThemBack(t *testing.T) {
	s := newSystem(t, opts{drain: 300 * time.Millisecond, sendTimeout: 30 * time.Second})
	hole := newBlackHole(t) // the request hangs until the receiver "comes back"
	s.addEndpoint(hole, "*")
	stop := s.startWorkerAs("worker-old", nil)
	s.event("a.b")

	s.waitFor(func() bool { return hole.active.Load() == 1 }, "the delivery to be in flight")
	began := time.Now()
	stop() // SIGTERM: stop claiming, drain for 300ms, abort, release
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("shutdown took %v; it must be bounded by the drain timeout", took)
	}

	// The unfinished delivery is back in the queue, due now, with its attempt refunded...
	var status string
	var attempts int
	var lockedBy *string
	s.scalar(&status, `SELECT status FROM deliveries`)
	s.scalar(&attempts, `SELECT attempt_count FROM deliveries`)
	s.scalar(&lockedBy, `SELECT locked_by FROM deliveries`)
	if status != "pending" || attempts != 0 || lockedBy != nil {
		t.Fatalf("after shutdown: status=%s attempt_count=%d locked_by=%v; want pending, 0, unlocked", status, attempts, lockedBy)
	}
	if n := s.count(`SELECT count(*) FROM attempts`); n != 0 {
		t.Fatalf("shutdown recorded %d attempts for a request it aborted", n)
	}
	if got := testutil.ToFloat64(s.o.metrics.Deliveries.WithLabelValues("released")); got != 1 {
		t.Fatalf("released counter = %v, want 1", got)
	}
	if got := testutil.ToFloat64(s.o.metrics.Inflight); got != 0 {
		t.Fatalf("inflight gauge = %v after shutdown, want 0", got)
	}

	// ...so a replacement worker delivers it at once, without waiting for any lease to expire.
	hole.open()
	s.startWorkerAs("worker-new", nil)
	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "the replacement worker to deliver it")
	if n := s.count(`SELECT attempt_count FROM deliveries`); n != 1 {
		t.Fatalf("attempt_count = %d, want 1: the aborted attempt must not count", n)
	}
}

func TestShutdownLetsFastDeliveriesFinish(t *testing.T) {
	s := newSystem(t, opts{drain: 5 * time.Second})
	r := newReceiver(t, ok())
	r.delay = 300 * time.Millisecond
	s.addEndpoint(r, "*")
	stop := s.startWorkerAs("w", nil)
	s.event("a.b")

	s.waitFor(func() bool { return s.count(`SELECT count(*) FROM deliveries WHERE status = 'in_flight'`) == 1 }, "delivery in flight")
	stop() // must wait for the 300ms request instead of aborting it

	if s.statusCount("succeeded") != 1 || s.count(`SELECT count(*) FROM attempts WHERE status_code = 200`) != 1 {
		t.Fatal("an in-flight delivery that could finish within the drain timeout was not allowed to")
	}
	if got := testutil.ToFloat64(s.o.metrics.Deliveries.WithLabelValues("released")); got != 0 {
		t.Fatalf("released = %v, want 0", got)
	}
}

func TestDeliveryMetrics(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 2, schedule: []time.Duration{time.Hour}}) // one retry, then dead
	good := newReceiver(t, ok())
	bad := newReceiver(t, always(500))
	s.addEndpoint(good, "*")
	s.addEndpoint(bad, "*")
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool {
		return s.statusCount("succeeded") == 1 && s.count(`SELECT count(*) FROM attempts WHERE status_code = 500`) == 1
	}, "one success and one failed attempt")
	m := s.o.metrics
	if got := testutil.ToFloat64(m.Deliveries.WithLabelValues("succeeded")); got != 1 {
		t.Errorf("succeeded = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Deliveries.WithLabelValues("retry")); got != 1 {
		t.Errorf("retry = %v, want 1", got)
	}
	// Both deliveries' first attempts were timed from enqueue; the platform's own delay is tiny here.
	if n := testutil.CollectAndCount(m.FirstAttemptDelay); n != 1 {
		t.Errorf("first-attempt-delay histogram not exported")
	}
	for _, class := range []string{"success", "http_5xx"} {
		if n := histogramCount(t, m.Registry, "webhook_attempt_duration_seconds", "result", class); n == 0 {
			t.Errorf("no attempt duration observed for class %s", class)
		}
	}
	// Idle again: nothing in flight.
	s.waitFor(func() bool { return testutil.ToFloat64(m.Inflight) == 0 }, "inflight to return to 0")
}

func TestStatsLoopPublishesQueueState(t *testing.T) {
	s := newSystem(t, opts{})
	r := newReceiver(t, ok())
	s.addEndpoint(r, "*")
	for range 3 {
		s.event("a.b")
	}
	s.exec(`UPDATE deliveries SET next_attempt_at = now() - interval '90 seconds' WHERE id = (SELECT id FROM deliveries ORDER BY id LIMIT 1)`)
	s.exec(`UPDATE deliveries SET status = 'dead', dead_at = now(), dead_reason = 'max_attempts' WHERE id = (SELECT id FROM deliveries ORDER BY id DESC LIMIT 1)`)

	ws := tu.WithRole(t, s.st, store.RoleWorker)
	w := worker.New(ws, nil, discard(), worker.Config{ID: "stats", Metrics: s.o.metrics, StatsInterval: 30 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.RunStats(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	m := s.o.metrics
	// A sampling pass sets its gauges one after another and the last-success
	// timestamp at the end, so wait for that before reading the others.
	s.waitFor(func() bool { return testutil.ToFloat64(m.StatsLastSuccess) > 0 }, "a complete queue-state sample")
	if got := testutil.ToFloat64(m.QueueDepth.WithLabelValues("due")); got != 2 {
		t.Errorf("queue_depth{due} = %v, want 2", got)
	}
	if age := testutil.ToFloat64(m.OldestPendingAge); age < 85 || age > 110 {
		t.Errorf("oldest pending age = %.0fs, want ~90s", age)
	}
	if got := testutil.ToFloat64(m.DLQDepth.WithLabelValues("max_attempts")); got != 1 {
		t.Errorf("dlq_depth{max_attempts} = %v, want 1", got)
	}
	if ts := testutil.ToFloat64(m.StatsLastSuccess); ts < float64(time.Now().Add(-time.Minute).Unix()) {
		t.Errorf("stats last-success timestamp not set: %v", ts)
	}
}

func TestRetentionJobPurgesOldFinishedWork(t *testing.T) {
	s := newSystem(t, opts{})
	r := newReceiver(t, ok())
	s.addEndpoint(r, "*")
	for range 4 {
		s.event("a.b")
	}
	s.exec(`UPDATE deliveries SET status = 'succeeded', updated_at = now() - interval '10 days'
	          WHERE id IN (SELECT id FROM deliveries ORDER BY id LIMIT 2)`)
	s.exec(`UPDATE deliveries SET status = 'succeeded', updated_at = now() WHERE id IN (SELECT id FROM deliveries WHERE status = 'pending' ORDER BY id LIMIT 1)`)

	ws := tu.WithRole(t, s.st, store.RoleWorker)
	w := worker.New(ws, nil, discard(), worker.Config{
		ID: "purge", Metrics: s.o.metrics, PurgeInterval: 30 * time.Millisecond, PurgeBatch: 1,
		RetainSucceeded: 7 * 24 * time.Hour, RetainDead: 30 * 24 * time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.RunPurge(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// Batch size 1 means the job has to loop to drain two old rows: that is what a backlog looks like.
	s.waitFor(func() bool { return s.count(`SELECT count(*) FROM deliveries`) == 2 }, "old deliveries to be purged")
	if got := testutil.ToFloat64(s.o.metrics.Purged.WithLabelValues("deliveries_succeeded")); got != 2 {
		t.Errorf("purged counter = %v, want 2", got)
	}
	if n := s.count(`SELECT count(*) FROM deliveries WHERE status = 'succeeded'`); n != 1 {
		t.Errorf("the recent succeeded delivery was purged (succeeded rows = %d)", n)
	}
}

// One trace follows an event from the producer's HTTP request through Postgres
// into the worker's delivery attempt, even though they run in different
// goroutines and, in production, different processes hours apart.
func TestTraceFollowsAnEventFromRequestToDelivery(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev); _ = tp.Shutdown(context.Background()) })

	s := newSystem(t, opts{instrument: true})
	r := newReceiver(t, ok())
	s.addEndpoint(r, "*")
	s.startWorker()

	// The producer is itself traced: it sends a W3C traceparent with its request.
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.api.URL+"/v1/events",
		strings.NewReader(`{"type":"a.b","payload":{}}`))
	req.Header.Set("Authorization", "Bearer "+s.key)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "delivery")
	_ = tp.ForceFlush(context.Background())

	var sawRequest, sawIngest, sawAttempt bool
	for _, sp := range rec.Ended() {
		if sp.SpanContext().TraceID().String() != traceID {
			continue
		}
		switch sp.Name() {
		case "POST /v1/events":
			sawRequest = true
		case "store.ingest_event":
			sawIngest = true
		case "delivery.attempt":
			sawAttempt = true
		}
	}
	if !sawRequest || !sawIngest || !sawAttempt {
		var names []string
		for _, sp := range rec.Ended() {
			names = append(names, sp.Name()+"@"+sp.SpanContext().TraceID().String()[:8])
		}
		t.Fatalf("trace %s is missing spans (request=%v ingest=%v attempt=%v); recorded: %v",
			traceID[:8], sawRequest, sawIngest, sawAttempt, names)
	}
}

// histogramCount returns how many observations a histogram series has, found by label.
func histogramCount(t *testing.T, reg *prometheus.Registry, name, label, value string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

// With a long poll interval, only the LISTEN/NOTIFY wake-up can make delivery fast.
// The same scenario without it must wait for the poll, which shows the test measures the wake-up.
func TestNewWorkWakesIdleWorkersImmediately(t *testing.T) {
	deliveryTime := func(listen bool) time.Duration {
		s := newSystem(t, opts{})
		r := newReceiver(t, ok())
		s.addEndpoint(r, "*")
		s.startWorkerAs("w", func(c *worker.Config) {
			c.PollInterval = 3 * time.Second
			c.Listen = listen
		})
		time.Sleep(700 * time.Millisecond) // the worker finds the queue empty and goes idle (and, if listening, attaches)
		start := time.Now()
		s.event("a.b")
		s.waitForWithin(15*time.Second, func() bool { return s.statusCount("succeeded") == 1 }, "delivery")
		return time.Since(start)
	}

	woken := deliveryTime(true)
	polled := deliveryTime(false)
	t.Logf("idle worker, 3s poll interval: first delivery after %v with LISTEN/NOTIFY, %v without", woken.Round(time.Millisecond), polled.Round(time.Millisecond))
	if woken > 500*time.Millisecond {
		t.Errorf("with LISTEN/NOTIFY an idle worker took %v to pick up new work; it should be nearly immediate", woken)
	}
	if polled < 1200*time.Millisecond {
		t.Errorf("without notifications delivery took only %v, so this test is not distinguishing the wake-up from polling", polled)
	}
}
