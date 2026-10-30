package loadtest

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"webhookdelivery/internal/invariants"
)

// Latencies are measured by the receiver's own clock, independent of the
// system's metrics.
type Latencies struct {
	// FromRequest: from when the producer sent the request to the first time the
	// endpoint acknowledged the event. Includes the API's own ingest time. This is
	// the conservative number.
	FromRequest Samples
	// FromAck: from when the producer got its 202 to first receipt, floored at 0
	// (a fast system can deliver before the producer even sees the 202).
	FromAck Samples
	// Undelivered counts (event, endpoint) pairs the endpoint never acknowledged.
	Undelivered int
	Delivered   int
	// PerSecond is first-receipts per wall-clock second, for throughput.
	PerSecond map[int64]int
}

// MeasureLatencies compares the producer's timestamps with the receiver's.
func MeasureLatencies(gen *GenResult, tenants []Tenant, recv *Receiver) *Latencies {
	l := &Latencies{PerSecond: map[int64]int{}}
	for _, ev := range gen.Events {
		for _, ep := range tenants[ev.Tenant].Endpoints {
			first, ok := recv.FirstSeen(ev.ID, ep)
			if !ok {
				l.Undelivered++
				continue
			}
			l.Delivered++
			l.FromRequest.Add(first.Sub(ev.Start))
			l.FromAck.Add(max(0, first.Sub(ev.Ack)))
			l.PerSecond[first.Unix()]++
		}
	}
	return l
}

// Throughput summarizes first-receipts per second over the steady part of the run.
func (l *Latencies) Throughput(start time.Time, duration time.Duration) (mean, peak float64) {
	from, to := start.Add(5*time.Second).Unix(), start.Add(duration-2*time.Second).Unix()
	var sum, n float64
	for s, c := range l.PerSecond {
		peak = max(peak, float64(c))
		if s >= from && s <= to {
			sum += float64(c)
		}
	}
	if to > from {
		n = float64(to - from + 1)
		mean = sum / n
	}
	return mean, peak
}

type Report struct {
	Scenario string
	Config   map[string]any
	Started  time.Time
	Duration time.Duration

	Gen struct {
		Scheduled, Late, Accepted, GaveUp, Retries, Throttled, ServerErr, NetErr int64
		Ack                                                                      Summary
	}
	Deliveries struct {
		Delivered, Undelivered int
		MeanPerSec, PeakPerSec float64
		FromRequest, FromAck   Summary
	}
	Server          ServerMetrics
	ReceiverReqs    int64
	ReceiverFaulted int64
	BadSignatures   int64
	Timeline        []string
	DrainSeconds    float64
	Drained         bool
	KilledOnStop    []string
	Invariants      invariants.Report
	Verdict         string
	Notes           []string
}

// Pass reports whether the run is a success: the invariants held and nothing was
// signed wrongly. In a fault-free run the generator must also have kept to its
// schedule, otherwise the numbers say nothing about the system. During a
// deliberate outage it is expected to fall behind (the producer cannot send into
// a system that is down), which is reported but is not a failure.
func (r *Report) Pass() bool {
	if !r.Invariants.OK() || r.BadSignatures != 0 {
		return false
	}
	return r.Scenario != "none" || r.Gen.Late == 0
}

func (r *Report) Print(w io.Writer) {
	p := func(f string, a ...any) { _, _ = fmt.Fprintf(w, f+"\n", a...) }
	p("")
	p("=================================================================")
	p(" LOAD / CHAOS REPORT   scenario=%s", r.Scenario)
	p("=================================================================")
	keys := make([]string, 0, len(r.Config))
	for k := range r.Config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var cfg []string
	for _, k := range keys {
		cfg = append(cfg, fmt.Sprintf("%s=%v", k, r.Config[k]))
	}
	p(" config: %s", strings.Join(cfg, " "))
	p("")
	p("PRODUCER (events)")
	p("  scheduled %d   accepted %d   gave up %d   late(generator saturated) %d", r.Gen.Scheduled, r.Gen.Accepted, r.Gen.GaveUp, r.Gen.Late)
	p("  retries %d   429 %d   5xx %d   network errors %d", r.Gen.Retries, r.Gen.Throttled, r.Gen.ServerErr, r.Gen.NetErr)
	p("  ingest latency: %s", r.Gen.Ack)
	p("")
	p("DELIVERIES (measured by the receiver's clock)")
	p("  delivered %d   never acknowledged %d", r.Deliveries.Delivered, r.Deliveries.Undelivered)
	p("  throughput: mean %.0f/s over the steady window, peak %.0f/s", r.Deliveries.MeanPerSec, r.Deliveries.PeakPerSec)
	p("  request -> first receipt: %s", r.Deliveries.FromRequest)
	p("  202 ack -> first receipt: %s", r.Deliveries.FromAck)
	p("  receiver saw %d requests, %d injected faults, %d bad signatures", r.ReceiverReqs, r.ReceiverFaulted, r.BadSignatures)
	p("")
	p("SERVER-SIDE (workers' own metrics, %d process(es) scraped)", r.Server.Scraped)
	p("  enqueue -> first attempt: %s", r.Server.FirstAttempt)
	p("  outcomes: %v   lease reclaims: %.0f   claim errors: %.0f   quota check failures: %.0f",
		r.Server.Outcomes, r.Server.LeaseReclaims, r.Server.ClaimErrors, r.Server.QuotaCheckFails)
	if len(r.Timeline) > 0 {
		p("")
		p("FAULT TIMELINE")
		for _, l := range r.Timeline {
			p("  %s", l)
		}
	}
	p("")
	p("DRAIN: %s in %.0fs.   Shutdown: %s", map[bool]string{true: "drained", false: "DID NOT DRAIN"}[r.Drained], r.DrainSeconds,
		map[bool]string{true: "all processes exited gracefully", false: fmt.Sprintf("had to kill %v", r.KilledOnStop)}[len(r.KilledOnStop) == 0])
	p("")
	p("INVARIANTS")
	p("  %s", strings.ReplaceAll(r.Invariants.String(), "\n", "\n  "))
	for _, n := range r.Notes {
		p("NOTE: %s", n)
	}
	p("")
	p("RESULT: %s", r.Verdict)
}

func (r *Report) WriteJSON(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// Window is the latency of deliveries first received within one slice of the run.
type Window struct {
	Start    time.Duration
	N        int
	P50, P99 time.Duration
}

// WindowedLatency buckets request-to-first-receipt latency by when the delivery was
// first received, so drift over a long run (a slow leak, growing tables) is visible
// instead of being averaged away.
func WindowedLatency(gen *GenResult, tenants []Tenant, recv *Receiver, start time.Time, window time.Duration) []Window {
	buckets := map[int]*Samples{}
	for _, ev := range gen.Events {
		for _, ep := range tenants[ev.Tenant].Endpoints {
			first, ok := recv.FirstSeen(ev.ID, ep)
			if !ok {
				continue
			}
			i := int(first.Sub(start) / window)
			if i < 0 {
				i = 0
			}
			if buckets[i] == nil {
				buckets[i] = &Samples{}
			}
			buckets[i].Add(first.Sub(ev.Start))
		}
	}
	var out []Window
	for i := 0; i < len(buckets)+1000 && len(out) < len(buckets); i++ {
		if b, ok := buckets[i]; ok {
			sum := b.Summary()
			out = append(out, Window{Start: time.Duration(i) * window, N: sum.N, P50: sum.P50, P99: sum.P99})
		}
	}
	return out
}
