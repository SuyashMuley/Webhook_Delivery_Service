package store

import (
	"testing"
	"time"
)

func TestBreakerOpensAtThresholdAndBacksOff(t *testing.T) {
	p := BreakerPolicy{Threshold: 3, BaseOpen: 30 * time.Second, MaxOpen: 100 * time.Second}
	now := time.Unix(1_700_000_000, 0)
	h := endpointHealth{Status: "active"}

	for i := 1; i <= 2; i++ {
		h, _ = p.onFailure(h, now)
		if h.OpenUntil != nil {
			t.Fatalf("circuit opened after %d failures, threshold is 3", i)
		}
	}
	h, _ = p.onFailure(h, now)
	if h.OpenUntil == nil || !h.OpenUntil.Equal(now.Add(30*time.Second)) || h.Trips != 1 {
		t.Fatalf("after 3 failures: open until %v, trips %d; want now+30s, 1", h.OpenUntil, h.Trips)
	}

	// In-flight stragglers failing while the circuit is open must not extend it.
	opened := *h.OpenUntil
	for range 5 {
		h, _ = p.onFailure(h, now.Add(time.Second))
	}
	if !h.OpenUntil.Equal(opened) || h.Trips != 1 {
		t.Fatalf("stragglers re-tripped the circuit: until %v trips %d", h.OpenUntil, h.Trips)
	}

	// The half-open probe fails after the window: re-open for twice as long, then capped.
	wantWindows := []time.Duration{60 * time.Second, 100 * time.Second, 100 * time.Second}
	at := opened.Add(time.Second)
	for i, want := range wantWindows {
		h, _ = p.onFailure(h, at)
		if got := h.OpenUntil.Sub(at); got != want {
			t.Fatalf("re-open %d: window %v, want %v", i+1, got, want)
		}
		at = h.OpenUntil.Add(time.Second)
	}
}

func TestBreakerAutoDisablesAfterContinuousFailure(t *testing.T) {
	p := BreakerPolicy{Threshold: 2, BaseOpen: time.Minute, MaxOpen: time.Hour, DisableAfter: 24 * time.Hour}
	start := time.Unix(1_700_000_000, 0)

	h, disabled := p.onFailure(endpointHealth{Status: "active"}, start)
	if disabled || h.FailingSince == nil || !h.FailingSince.Equal(start) {
		t.Fatalf("first failure: disabled=%v failing_since=%v", disabled, h.FailingSince)
	}
	h, disabled = p.onFailure(h, start.Add(23*time.Hour))
	if disabled {
		t.Fatal("disabled before the failure streak reached DisableAfter")
	}
	h, disabled = p.onFailure(h, start.Add(24*time.Hour))
	if !disabled || h.Status != "disabled" {
		t.Fatalf("after 24h of failures: disabled=%v status=%q", disabled, h.Status)
	}

	// A paused endpoint is the tenant's choice; the breaker must not override it.
	paused := endpointHealth{Status: "paused", FailingSince: &start}
	if h, disabled = p.onFailure(paused, start.Add(48*time.Hour)); disabled || h.Status != "paused" {
		t.Fatalf("paused endpoint was disabled: %+v", h)
	}
	// And DisableAfter = 0 means never.
	never := BreakerPolicy{Threshold: 2, BaseOpen: time.Minute, MaxOpen: time.Hour}
	if _, disabled = never.onFailure(endpointHealth{Status: "active", FailingSince: &start}, start.Add(1000*time.Hour)); disabled {
		t.Fatal("auto-disable fired although disabled by policy")
	}
}
