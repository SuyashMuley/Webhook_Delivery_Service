// Package loadtest holds the pieces of the load and chaos tool: an embedded
// receiver with fault injection, an open-loop event generator, child-process
// management, and fault scenarios.
package loadtest

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Samples collects durations and reports exact percentiles.
type Samples struct {
	mu sync.Mutex
	v  []time.Duration
}

func (s *Samples) Add(d time.Duration) {
	s.mu.Lock()
	s.v = append(s.v, d)
	s.mu.Unlock()
}

type Summary struct {
	N                            int
	Min, P50, P90, P95, P99, Max time.Duration
	P999, Mean                   time.Duration
}

func (s *Samples) Summary() Summary {
	s.mu.Lock()
	v := slices.Clone(s.v)
	s.mu.Unlock()
	if len(v) == 0 {
		return Summary{}
	}
	slices.Sort(v)
	var sum time.Duration
	for _, d := range v {
		sum += d
	}
	q := func(p float64) time.Duration {
		i := int(float64(len(v))*p+0.5) - 1
		return v[min(max(i, 0), len(v)-1)]
	}
	return Summary{N: len(v), Min: v[0], P50: q(.50), P90: q(.90), P95: q(.95), P99: q(.99), P999: q(.999),
		Max: v[len(v)-1], Mean: sum / time.Duration(len(v))}
}

func (s Summary) String() string {
	if s.N == 0 {
		return "no samples"
	}
	return fmt.Sprintf("n=%d p50=%v p90=%v p95=%v p99=%v p99.9=%v max=%v", s.N,
		round(s.P50), round(s.P90), round(s.P95), round(s.P99), round(s.P999), round(s.Max))
}

func round(d time.Duration) time.Duration {
	switch {
	case d >= 10*time.Second:
		return d.Round(100 * time.Millisecond)
	case d >= time.Second:
		return d.Round(10 * time.Millisecond)
	case d >= 10*time.Millisecond:
		return d.Round(time.Millisecond)
	default:
		return d.Round(10 * time.Microsecond)
	}
}
