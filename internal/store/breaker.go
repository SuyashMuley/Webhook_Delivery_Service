package store

import "time"

// BreakerPolicy configures the per-endpoint circuit breaker.
//
// After Threshold consecutive failed attempts the circuit opens: the endpoint
// is not attempted again until the open window passes (BaseOpen, doubling each
// time the circuit re-opens, up to MaxOpen). Deliveries just wait meanwhile and
// do not use up their attempts. When the window passes the circuit is
// half-open: a single probe delivery is allowed; if it succeeds the endpoint
// is healthy again, if it fails the circuit re-opens for longer.
//
// If an endpoint fails continuously for DisableAfter it is disabled and its
// waiting deliveries are dead-lettered.
type BreakerPolicy struct {
	Threshold    int
	BaseOpen     time.Duration
	MaxOpen      time.Duration
	DisableAfter time.Duration // 0 disables auto-disable
}

func DefaultBreaker() BreakerPolicy {
	return BreakerPolicy{Threshold: 20, BaseOpen: 30 * time.Second, MaxOpen: time.Hour, DisableAfter: 72 * time.Hour}
}

// endpointHealth is the part of an endpoint row the breaker reads and writes.
type endpointHealth struct {
	Status       string
	Failures     int
	Trips        int
	FailingSince *time.Time
	OpenUntil    *time.Time
}

// onFailure returns the endpoint's health after one more failed attempt, and
// whether that failure caused it to be auto-disabled.
func (p BreakerPolicy) onFailure(h endpointHealth, now time.Time) (next endpointHealth, disabled bool) {
	h.Failures++
	if h.FailingSince == nil {
		t := now
		h.FailingSince = &t
	}

	// Attempts that were already in flight when the circuit opened may still
	// fail; they count as failures but must not extend the open window.
	open := h.OpenUntil != nil && h.OpenUntil.After(now)
	if h.Failures >= p.Threshold && !open {
		window := p.BaseOpen
		for i := 0; i < h.Trips && window < p.MaxOpen; i++ {
			window *= 2
		}
		window = min(window, p.MaxOpen)
		until := now.Add(window)
		h.OpenUntil = &until
		h.Trips++
	}

	if p.DisableAfter > 0 && h.Status == "active" && now.Sub(*h.FailingSince) >= p.DisableAfter {
		h.Status = "disabled"
		disabled = true
	}
	return h, disabled
}
