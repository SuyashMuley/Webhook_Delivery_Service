package worker_test

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"webhookdelivery/internal/harness/invariants"
	"webhookdelivery/internal/store"
	tu "webhookdelivery/internal/testutil"
)

// recvObs adapts the test receivers to invariants.Observations.
type recvObs struct{ byEndpoint map[string]*receiver }

func (o recvObs) Count(event, endpoint string) int {
	if r, ok := o.byEndpoint[endpoint]; ok {
		return r.ackCount(event)
	}
	return 0
}

func (o recvObs) Each(f func(invariants.Key, int)) {
	for ep, r := range o.byEndpoint {
		r.mu.Lock()
		for ev, n := range r.acked {
			f(invariants.Key{Event: ev, Endpoint: ep}, n)
		}
		r.mu.Unlock()
	}
}

// The in-process counterpart of cmd/loadtest, small enough to run on every
// commit: receivers that fail and throttle, workers that crash (abandon leased
// work) and are replaced, and the invariants checked at the end.
func TestInvariantsHoldUnderFailuresAndCrashes(t *testing.T) {
	const events, fanout = 250, 3
	s := newSystem(t, opts{
		maxAttempts: 8,
		schedule:    []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond},
		lease:       400 * time.Millisecond,
		concurrency: 20,
		drain:       500 * time.Millisecond,
	})

	// Receivers that misbehave: 25% 500s and 10% 429s with a short Retry-After.
	byEndpoint := map[string]*receiver{}
	for range fanout {
		r := newReceiver(t, func(int) response {
			switch x := rand.Float64(); { //nolint:gosec // fault injection
			case x < 0.25:
				return response{status: 500}
			case x < 0.35:
				return response{status: 429, headers: map[string]string{"Retry-After": "1"}}
			}
			return response{status: 200}
		})
		byEndpoint[s.addEndpoint(r, "*")] = r
	}

	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { stop(); wg.Wait() })

	// Real workers, replaced every ~300ms: a graceful stop hands back in-flight work.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			stopWorker := s.startWorkerAs("w-"+strconv.Itoa(i), nil)
			select {
			case <-ctx.Done():
			case <-time.After(300 * time.Millisecond):
			}
			stopWorker()
		}
	}()

	// The crash monkey: claims work under a throwaway identity and never reports back,
	// leaving exactly what `kill -9` leaves behind: leases that must expire and be reclaimed.
	monkey := tuStore(t, s)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			_, _ = monkey.ClaimDeliveries(ctx, "crashed-"+strconv.Itoa(i), 5, 5, 400*time.Millisecond)
			time.Sleep(150 * time.Millisecond)
		}
	}()

	var ids []string
	for range events {
		ids = append(ids, s.event("chaos.test")["id"].(string))
		time.Sleep(4 * time.Millisecond)
	}

	// Stop the chaos and let the system settle: only workers and the reaper remain.
	stop()
	wg.Wait()
	s.startWorker()
	s.waitForWithin(60*time.Second, func() bool {
		return s.count(`SELECT count(*) FROM deliveries WHERE status IN ('pending','in_flight')`) == 0
	}, "the queue to drain")

	// The test is only meaningful if the faults really happened.
	m := s.o.metrics
	if n := testutil.ToFloat64(m.LeaseReclaims.WithLabelValues("requeued")); n == 0 {
		t.Fatal("the crash monkey never caused a lease reclaim: crash recovery was not exercised")
	}
	if n := testutil.ToFloat64(m.Deliveries.WithLabelValues("retry")); n < 20 {
		t.Fatalf("only %v retries: the receivers did not fail enough to exercise retrying", n)
	}

	rep, err := invariants.Check(context.Background(), s.st.Pool, invariants.Expected{
		EventIDs: ids, Fanout: fanout, Received: recvObs{byEndpoint},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", rep)
	if !rep.OK() {
		t.Fatalf("invariants violated:\n%s", rep)
	}
	if rep.Deliveries != events*fanout {
		t.Fatalf("%d deliveries, want %d", rep.Deliveries, events*fanout)
	}
	if rep.ByStatus["succeeded"] < events*fanout*9/10 {
		t.Fatalf("only %d of %d deliveries succeeded; the faults are not severe enough to justify losing that many", rep.ByStatus["succeeded"], events*fanout)
	}
}

// tuStore returns a worker-role store for the crash monkey.
func tuStore(t *testing.T, s *system) *store.Store {
	t.Helper()
	ws := tu.WithRole(t, s.st, store.RoleWorker)
	ws.Breaker = s.o.breaker
	return ws
}
