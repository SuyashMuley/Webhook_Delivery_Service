// Package retry decides how long to wait before re-sending a failed delivery.
package retry

import (
	"math/rand/v2"
	"time"
)

// DefaultSchedule is the nominal wait after the 1st, 2nd, ... failed attempt.
// With the default of 8 attempts the last retry happens about a day after the
// first try. If there are more attempts than entries, the last entry repeats.
var DefaultSchedule = []time.Duration{
	30 * time.Second,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	5 * time.Hour,
	8 * time.Hour,
	8 * time.Hour,
}

type Policy struct {
	// Schedule holds the nominal delay after each failed attempt.
	Schedule []time.Duration
	// MaxRetryAfter bounds how far a receiver's Retry-After can push a retry out.
	MaxRetryAfter time.Duration
}

func DefaultPolicy() Policy {
	return Policy{Schedule: DefaultSchedule, MaxRetryAfter: 6 * time.Hour}
}

// Delay returns how long to wait after attempt number failedAttempt (1-based)
// failed.
//
// It uses "equal jitter": the nominal delay n becomes a value between n/2 and
// n. Unlike full jitter (0..n) it can never collapse a long backoff into an
// immediate retry, while still spreading retries so a recovering receiver is
// not hit by everything at once. jitter must be in [0, 1); see Jitter.
//
// If the receiver sent a Retry-After, the wait is at least that long (capped
// at MaxRetryAfter), so we never retry sooner than the receiver asked.
func (p Policy) Delay(failedAttempt int, retryAfter time.Duration, jitter float64) time.Duration {
	nominal := time.Minute // only reachable with an empty schedule, which config rejects
	if n := len(p.Schedule); n > 0 {
		nominal = p.Schedule[min(max(failedAttempt, 1), n)-1]
	}
	half := nominal / 2
	d := half + time.Duration(jitter*float64(half))

	if retryAfter > 0 {
		d = max(d, min(retryAfter, p.MaxRetryAfter))
	}
	return d
}

// Jitter returns a random value in [0, 1) for Policy.Delay.
func Jitter() float64 {
	return rand.Float64() //nolint:gosec // retry spreading needs no cryptographic randomness
}
