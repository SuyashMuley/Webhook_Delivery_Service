// Package worker runs the delivery loop: claim due deliveries, send them, record the outcome.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"webhookdelivery/internal/deliver"
	"webhookdelivery/internal/retry"
	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
)

// reclaimBatch bounds how many expired leases one reaper pass handles.
const reclaimBatch = 500

type Config struct {
	ID             string // identifies this worker in deliveries.locked_by
	BatchSize      int
	TenantSlice    int // max deliveries claimed from one tenant per poll
	Concurrency    int
	PollInterval   time.Duration
	Lease          time.Duration
	ReaperInterval time.Duration
	Retry          retry.Policy
}

type Worker struct {
	store  *store.Store
	sender *deliver.Sender
	log    *slog.Logger
	cfg    Config
}

func New(s *store.Store, sender *deliver.Sender, log *slog.Logger, cfg Config) *Worker {
	return &Worker{store: s, sender: sender, log: log.With("worker_id", cfg.ID), cfg: cfg}
}

// envelope is the JSON body sent to receivers.
type envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// Run claims and delivers until ctx is cancelled, then waits for in-flight
// deliveries to finish before returning.
func (w *Worker) Run(ctx context.Context) error {
	slots := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	for ctx.Err() == nil {
		free := cap(slots) - len(slots)
		if free == 0 {
			sleep(ctx, 10*time.Millisecond)
			continue
		}

		claimed, err := w.store.ClaimDeliveries(ctx, w.cfg.ID, min(w.cfg.BatchSize, free), w.cfg.TenantSlice, w.cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("claim deliveries", "error", err)
				sleep(ctx, time.Second)
			}
			continue
		}

		for _, d := range claimed {
			slots <- struct{}{} // never blocks: we only claimed as many as there were free slots
			wg.Add(1)
			go func() {
				defer func() { <-slots; wg.Done() }()
				w.process(d)
			}()
		}
		if len(claimed) == 0 {
			sleep(ctx, w.cfg.PollInterval)
		}
	}
	return nil
}

func (w *Worker) process(d store.ClaimedDelivery) {
	log := w.log.With("delivery_id", d.ID, "event_id", d.EventID, "endpoint_id", d.EndpointID,
		"tenant_id", d.TenantID, "attempt", d.Attempt)

	body, err := json.Marshal(envelope{ID: d.EventID, Type: d.EventType, CreatedAt: d.EventCreatedAt, Data: d.Payload})
	if err != nil {
		log.Error("marshal envelope", "error", err)
		return // lease expires and the reaper (Phase 2) will recover it
	}

	started := time.Now()
	var res deliver.Result
	local := false
	switch secrets := signingKeys(log, d.Secrets, started); {
	case d.SecretsErr != nil:
		// Not the endpoint's fault, and sending unsigned would be worse than not
		// sending: fail the attempt so it is retried (and visible) later.
		log.Error("cannot decrypt signing secrets", "error", d.SecretsErr)
		res, local = deliver.Result{Err: errors.New("cannot decrypt the endpoint's signing secrets")}, true
	case len(secrets) == 0:
		res, local = deliver.Result{Err: errors.New("endpoint has no live signing secret")}, true
	default:
		res = w.sender.Send(context.Background(), deliver.Request{
			URL:       d.URL,
			ID:        d.EventID,
			Timestamp: started,
			Body:      body,
			Secrets:   secrets,
		})
	}

	attempt := store.Attempt{
		StartedAt:   started,
		Duration:    res.Duration,
		StatusCode:  res.StatusCode,
		BodySnippet: res.BodySnippet,
		Local:       local,
	}
	if res.Err != nil {
		attempt.Error = res.Err.Error()
	}

	// Recording the outcome must survive shutdown, so it is not tied to the loop's context.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		verdict string
		delay   time.Duration
	)
	switch {
	case res.Success():
		verdict = "succeeded"
		err = w.store.CompleteDelivery(ctx, w.cfg.ID, d, attempt)
	case res.StatusCode == http.StatusGone:
		// 410 is the receiver telling us the endpoint no longer exists: stop
		// sending, dead-letter this delivery and pause the endpoint.
		verdict = "dead:" + store.DeadEndpointGone
		err = w.store.KillDelivery(ctx, w.cfg.ID, d, attempt, store.DeadEndpointGone)
	case d.Attempt >= d.MaxAttempts:
		verdict = "dead:" + store.DeadMaxAttempts
		err = w.store.KillDelivery(ctx, w.cfg.ID, d, attempt, store.DeadMaxAttempts)
	default:
		// Retry-After only means something on 429 and 503.
		var retryAfter time.Duration
		if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusServiceUnavailable {
			retryAfter = res.RetryAfter
		}
		delay = w.cfg.Retry.Delay(d.Attempt, retryAfter, retry.Jitter())
		verdict = "retry"
		err = w.store.FailDelivery(ctx, w.cfg.ID, d, attempt, time.Now().Add(delay))
	}
	if errors.Is(err, store.ErrLeaseLost) {
		// The lease expired and the delivery was reclaimed while we were sending.
		// Whoever holds it now owns the outcome; ours must not overwrite it.
		log.Warn("lease lost before the outcome could be recorded", "status", res.StatusCode)
		return
	}
	if err != nil {
		log.Error("record delivery outcome", "error", err)
		return
	}
	log.Info("delivery attempt", "verdict", verdict, "status", res.StatusCode,
		"duration_ms", res.Duration.Milliseconds(), "retry_in", delay.Round(time.Second))
}

// RunReaper periodically returns deliveries whose lease expired (their worker
// crashed or stalled) to the queue, or to the DLQ once their attempts are used
// up. Every worker runs one; the queries are safe to run concurrently.
func (w *Worker) RunReaper(ctx context.Context) error {
	t := time.NewTicker(w.cfg.ReaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for ctx.Err() == nil {
			requeued, dead, err := w.store.ReclaimExpired(ctx, reclaimBatch)
			if err != nil {
				if ctx.Err() == nil {
					w.log.Error("reclaim expired leases", "error", err)
				}
				break
			}
			if requeued+dead > 0 {
				w.log.Warn("reclaimed expired leases", "requeued", requeued, "dead_lettered", dead)
			}
			if requeued+dead < reclaimBatch {
				break
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// signingKeys returns the key bytes of every secret that may sign at time now:
// during a rotation the old and new secrets both sign, so receivers can verify
// with either.
func signingKeys(log *slog.Logger, secrets []store.Secret, now time.Time) [][]byte {
	var keys [][]byte
	for _, s := range secrets {
		if !s.Live(now) {
			continue
		}
		key, err := sign.ParseSecret(s.Value)
		if err != nil {
			log.Warn("skipping unparseable secret", "secret_id", s.ID, "error", err)
			continue
		}
		keys = append(keys, key)
	}
	return keys
}
