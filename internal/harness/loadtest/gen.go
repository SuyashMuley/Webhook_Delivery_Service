package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tenant is a load-test tenant with its API key and endpoint ids.
type Tenant struct {
	ID        string
	Key       string
	Endpoints []string
}

// EventRec is one event the API acknowledged.
type EventRec struct {
	ID     string
	Tenant int
	Start  time.Time // when the (first) request for it was sent
	Ack    time.Time // when the 2xx arrived
}

type GenConfig struct {
	APIs     []string // base URLs, used round-robin
	Tenants  []Tenant
	Rate     float64       // events per second, open loop
	Duration time.Duration // how long to generate
	Senders  int           // concurrent in-flight requests allowed
	// RetryFor is how long a producer keeps retrying one event (same Idempotency-Key)
	// through errors and throttling before giving up.
	RetryFor time.Duration
	RunID    string
}

// GenResult is what the generator observed from the producer's side.
type GenResult struct {
	Scheduled int64 // events the schedule called for
	Late      int64 // scheduled but not sent because every sender was busy (generator saturated)
	Accepted  int64
	GaveUp    int64
	Retries   int64
	Throttled int64 // 429 responses
	ServerErr int64 // 5xx responses
	NetErr    int64 // connection-level failures

	Ack    Samples // latency of the successful request, from the first attempt's start
	Events []EventRec
	mu     sync.Mutex
}

// Generate sends events at a fixed arrival rate regardless of how fast the
// system answers (open loop). A closed-loop generator slows down when the
// system does and so hides exactly the latency we want to measure.
func Generate(ctx context.Context, cfg GenConfig) *GenResult {
	res := &GenResult{}
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: 1024, MaxIdleConnsPerHost: 512, IdleConnTimeout: 30 * time.Second,
		},
	}
	type job struct {
		n  int64
		at time.Time
	}
	jobs := make(chan job, cfg.Senders)
	var wg sync.WaitGroup
	for range cfg.Senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				sendEvent(ctx, client, cfg, res, j.n)
			}
		}()
	}

	interval := time.Duration(float64(time.Second) / cfg.Rate)
	start := time.Now()
	end := start.Add(cfg.Duration)
	for i := int64(0); ctx.Err() == nil; i++ {
		at := start.Add(time.Duration(i) * interval)
		if at.After(end) {
			break
		}
		if d := time.Until(at); d > 0 {
			time.Sleep(d)
		}
		atomic.AddInt64(&res.Scheduled, 1)
		select {
		case jobs <- job{n: i, at: at}:
		default:
			atomic.AddInt64(&res.Late, 1)
		}
	}
	close(jobs)
	wg.Wait()
	return res
}

func sendEvent(ctx context.Context, client *http.Client, cfg GenConfig, res *GenResult, n int64) {
	tenantIdx := int(n % int64(len(cfg.Tenants)))
	tn := cfg.Tenants[tenantIdx]
	body, _ := json.Marshal(map[string]any{
		"type":    "load.test",
		"payload": map[string]any{"n": n, "pad": strings.Repeat("x", 200)},
	})
	idem := fmt.Sprintf("lt-%s-%d", cfg.RunID, n)
	started := time.Now()
	deadline := started.Add(cfg.RetryFor)
	backoff := 100 * time.Millisecond

	for attempt := 0; ; attempt++ {
		base := cfg.APIs[(int(n)+attempt)%len(cfg.APIs)] // a retry tries another API instance
		req, _ := http.NewRequestWithContext(ctx, "POST", base+"/v1/events", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tn.Key)
		req.Header.Set("Idempotency-Key", idem)
		resp, err := client.Do(req)

		var retryAfter time.Duration
		switch {
		case err != nil:
			atomic.AddInt64(&res.NetErr, 1)
		default:
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
			_ = resp.Body.Close()
			switch {
			case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK:
				var out struct {
					ID string `json:"id"`
				}
				if json.Unmarshal(b, &out) == nil && out.ID != "" {
					ack := time.Now()
					res.Ack.Add(ack.Sub(started))
					atomic.AddInt64(&res.Accepted, 1)
					res.mu.Lock()
					res.Events = append(res.Events, EventRec{ID: out.ID, Tenant: tenantIdx, Start: started, Ack: ack})
					res.mu.Unlock()
					return
				}
			case resp.StatusCode == http.StatusTooManyRequests:
				atomic.AddInt64(&res.Throttled, 1)
				if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
					retryAfter = time.Duration(s) * time.Second
				}
			case resp.StatusCode >= 500:
				atomic.AddInt64(&res.ServerErr, 1)
			default: // a 4xx other than 429 will not get better by retrying
				atomic.AddInt64(&res.GaveUp, 1)
				return
			}
		}

		if time.Now().Add(backoff).After(deadline) || ctx.Err() != nil {
			atomic.AddInt64(&res.GaveUp, 1)
			return
		}
		atomic.AddInt64(&res.Retries, 1)
		time.Sleep(max(backoff, retryAfter))
		backoff = min(backoff*2, 2*time.Second)
	}
}
