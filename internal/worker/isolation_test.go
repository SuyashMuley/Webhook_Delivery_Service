package worker_test

import (
	"strings"
	"testing"
	"time"

	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
)

// Phase 3 exit criterion: one black-hole endpoint must not slow deliveries to
// healthy endpoints. The same workload runs twice, with the black hole limited
// to 5 concurrent deliveries (the default) and effectively unlimited.
func TestBlackHoleEndpointDoesNotSlowHealthyEndpoints(t *testing.T) {
	const events = 50

	run := func(t *testing.T, holeCap int) (healthyDone time.Duration, holeMax int32) {
		s := newSystem(t, opts{
			maxAttempts: 3,
			concurrency: 10, // the worker's total capacity
			sendTimeout: time.Second,
			// Keep the circuit breaker out of this measurement: it would also
			// stop the black hole after a few timeouts, hiding the cap's effect.
			breaker: store.BreakerPolicy{Threshold: 100000, BaseOpen: time.Minute, MaxOpen: time.Minute},
		})
		hole := newBlackHole(t)
		healthy := newReceiver(t, ok())
		holeID := s.addEndpoint(hole, "*")
		s.addEndpoint(healthy, "*")
		s.exec(`UPDATE endpoints SET max_concurrency = $1 WHERE id = $2`, holeCap, holeID)
		for range events {
			s.event("load.test") // fans out to both: `events` deliveries each
		}

		start := time.Now()
		s.startWorker()
		// The uncapped run is slow on purpose (the black hole holds every worker slot until
		// its timeouts), so give it generous room; only the measured time is asserted on.
		s.waitForWithin(60*time.Second, func() bool { return len(healthy.snapshot()) >= events }, "all healthy deliveries")
		return time.Since(start), hole.maxActive.Load()
	}

	cappedTime, cappedMax := run(t, 5)
	uncappedTime, uncappedMax := run(t, 1000)
	t.Logf("healthy endpoint, %d deliveries, worker capacity 10, black hole times out after 1s:", events)
	t.Logf("  with the per-endpoint cap (5): done in %v (black hole held at most %d slots)", cappedTime.Round(time.Millisecond), cappedMax)
	t.Logf("  without the cap:               done in %v (black hole held up to %d slots)", uncappedTime.Round(time.Millisecond), uncappedMax)

	if cappedMax > 5 {
		t.Errorf("black hole held %d concurrent deliveries, cap is 5", cappedMax)
	}
	if uncappedMax <= 5 {
		t.Errorf("the uncapped run never exceeded 5 concurrent black-hole deliveries (%d): the scenario does not exercise the cap", uncappedMax)
	}
	// The healthy endpoint is fast no matter what the black hole does...
	if cappedTime > 3*time.Second {
		t.Errorf("with the cap, healthy deliveries took %v; the black hole slowed them down", cappedTime)
	}
	// ...and without the cap the black hole demonstrably hurts it.
	if uncappedTime < 2*cappedTime {
		t.Errorf("without the cap healthy deliveries took %v vs %v with it: no clear benefit", uncappedTime, cappedTime)
	}
}

func TestCircuitBreakerPausesAttemptsThenRecovers(t *testing.T) {
	s := newSystem(t, opts{
		maxAttempts: 50, // never reach the DLQ in this test
		schedule:    []time.Duration{10 * time.Millisecond},
		breaker:     store.BreakerPolicy{Threshold: 3, BaseOpen: 400 * time.Millisecond, MaxOpen: 400 * time.Millisecond},
	})
	r := newReceiver(t, always(500))
	s.addEndpoint(r, "*")
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool { return len(r.snapshot()) >= 3 }, "3 failed attempts")
	third := r.snapshot()[2].at
	s.waitFor(func() bool { return len(r.snapshot()) >= 4 }, "the half-open probe")
	if gap := r.snapshot()[3].at.Sub(third); gap < 350*time.Millisecond {
		t.Fatalf("the next attempt after the circuit opened came after %v; it should wait out the 400ms open window", gap)
	}
	if n := len(r.snapshot()); n != 4 {
		t.Fatalf("%d attempts so far; a half-open circuit allows a single probe", n)
	}

	// The receiver recovers: the next probe succeeds and everything flows again.
	r.setBehavior(ok())
	s.waitFor(func() bool { return s.statusCount("succeeded") == 1 }, "delivery to succeed after recovery")

	var failures int
	var openUntil *time.Time
	s.scalar(&failures, `SELECT consecutive_failures FROM endpoints`)
	s.scalar(&openUntil, `SELECT circuit_open_until FROM endpoints`)
	if failures != 0 || openUntil != nil {
		t.Fatalf("circuit not closed after recovery: failures=%d open_until=%v", failures, openUntil)
	}
}

func TestRotationSignsWithBothSecretsDuringGrace(t *testing.T) {
	const s1 = "whsec_QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE="
	const s2 = "whsec_QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
	k1, _ := sign.ParseSecret(s1)
	k2, _ := sign.ParseSecret(s2)

	s := newSystem(t, opts{})
	r := newReceiver(t, ok())
	ep := s.post("/v1/endpoints", map[string]any{"url": r.srv.URL, "secret": s1})
	id := ep["id"].(string)
	s.startWorker()

	verify := func(h hit, key []byte) error {
		return sign.Verify(h.signature, [][]byte{key}, h.webhookID, h.timestamp, h.body, 5*time.Minute, time.Now())
	}
	deliver := func(n int) hit {
		s.event("a.b")
		s.waitFor(func() bool { return len(r.snapshot()) >= n }, "delivery")
		return r.snapshot()[n-1]
	}

	h := deliver(1)
	if len(strings.Fields(h.signature)) != 1 || verify(h, k1) != nil {
		t.Fatalf("before rotation: want one signature valid under the first secret, got %q", h.signature)
	}

	// Rotate with a grace period: deliveries now carry a signature per secret, so a
	// receiver that has only been updated to the new secret (or not yet) still verifies.
	s.post("/v1/endpoints/"+id+"/secrets/rotate", map[string]any{"secret": s2, "grace_period": "1h"})
	h = deliver(2)
	if len(strings.Fields(h.signature)) != 2 || verify(h, k1) != nil || verify(h, k2) != nil {
		t.Fatalf("during grace: want signatures valid under both secrets, got %q", h.signature)
	}

	// Revoke the old secret: only the new one signs.
	var oldID string
	for _, sec := range s.call("GET", "/v1/endpoints/"+id+"/secrets", nil)["data"].([]any) {
		if m := sec.(map[string]any); m["expires_at"] != nil {
			oldID = m["id"].(string)
		}
	}
	s.call("DELETE", "/v1/endpoints/"+id+"/secrets/"+oldID, nil)
	h = deliver(3)
	if len(strings.Fields(h.signature)) != 1 || verify(h, k2) != nil || verify(h, k1) == nil {
		t.Fatalf("after revoking the old secret: want only the new secret to verify, got %q", h.signature)
	}
}

// A decryption problem is ours, not the endpoint's: nothing must be sent
// unsigned, the attempt is recorded with a clear error, and the endpoint's
// circuit breaker is left alone.
func TestUndecryptableSecretsFailLocallyWithoutHurtingTheEndpoint(t *testing.T) {
	s := newSystem(t, opts{maxAttempts: 2, schedule: []time.Duration{10 * time.Millisecond}})
	r := newReceiver(t, ok())
	s.addEndpoint(r, "*")
	s.exec(`UPDATE endpoints SET secrets = jsonb_set(secrets, '{0,sealed}', '"v1.dev.AAAA.BBBB"')`)
	s.startWorker()
	s.event("a.b")

	s.waitFor(func() bool { return s.statusCount("dead") == 1 }, "delivery to exhaust its attempts")
	if n := len(r.snapshot()); n != 0 {
		t.Fatalf("receiver got %d requests; nothing may be sent when the secrets cannot be decrypted", n)
	}
	var msg string
	var failures int
	s.scalar(&msg, `SELECT error FROM attempts ORDER BY id LIMIT 1`)
	s.scalar(&failures, `SELECT consecutive_failures FROM endpoints`)
	if !strings.Contains(msg, "cannot decrypt") {
		t.Errorf("attempt error = %q, want it to say the secrets cannot be decrypted", msg)
	}
	if failures != 0 {
		t.Errorf("endpoint consecutive_failures = %d; local failures must not count against it", failures)
	}
}
