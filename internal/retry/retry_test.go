package retry

import (
	"testing"
	"time"
)

func TestDelayFollowsScheduleWithEqualJitter(t *testing.T) {
	p := Policy{Schedule: []time.Duration{time.Minute, time.Hour}, MaxRetryAfter: 6 * time.Hour}
	cases := []struct {
		attempt int
		nominal time.Duration
	}{
		{1, time.Minute},
		{2, time.Hour},
		{3, time.Hour}, // beyond the schedule: last entry repeats
		{99, time.Hour},
		{0, time.Minute}, // defensive: treated as the first failure
	}
	for _, c := range cases {
		lo, hi := p.Delay(c.attempt, 0, 0), p.Delay(c.attempt, 0, 0.999999)
		if lo != c.nominal/2 {
			t.Errorf("attempt %d: min delay %v, want %v", c.attempt, lo, c.nominal/2)
		}
		if hi < lo || hi > c.nominal {
			t.Errorf("attempt %d: max delay %v outside [%v, %v]", c.attempt, hi, lo, c.nominal)
		}
	}
}

func TestDelayHonorsRetryAfter(t *testing.T) {
	p := Policy{Schedule: []time.Duration{time.Minute}, MaxRetryAfter: time.Hour}

	if got := p.Delay(1, 10*time.Minute, 0); got != 10*time.Minute {
		t.Errorf("Retry-After longer than backoff: got %v, want 10m", got)
	}
	if got := p.Delay(1, time.Second, 0); got != 30*time.Second {
		t.Errorf("Retry-After shorter than backoff must not shorten it: got %v", got)
	}
	if got := p.Delay(1, 48*time.Hour, 0); got != time.Hour {
		t.Errorf("Retry-After must be capped at MaxRetryAfter: got %v", got)
	}
}

func TestDefaultScheduleSpansAboutADay(t *testing.T) {
	var total time.Duration
	for _, d := range DefaultSchedule {
		total += d
	}
	if total < 20*time.Hour || total > 26*time.Hour {
		t.Fatalf("default schedule totals %v, want roughly 24h", total)
	}
}
