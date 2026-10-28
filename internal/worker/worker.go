// Package worker runs the delivery loop: claim due deliveries, send them, record the outcome.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"webhookdelivery/internal/deliver"
	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
)

// interimRetryDelay is a fixed placeholder until Phase 2 replaces it with
// exponential backoff, attempt limits and the dead letter queue.
const interimRetryDelay = 30 * time.Second

type Config struct {
	ID           string // identifies this worker in deliveries.locked_by
	BatchSize    int
	Concurrency  int
	PollInterval time.Duration
	Lease        time.Duration
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

		claimed, err := w.store.ClaimDeliveries(ctx, w.cfg.ID, min(w.cfg.BatchSize, free), w.cfg.Lease)
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

	var secrets [][]byte
	for _, s := range d.Secrets {
		if key, err := sign.ParseSecret(s.Value); err == nil {
			secrets = append(secrets, key)
		} else {
			log.Warn("skipping unparseable secret", "secret_id", s.ID, "error", err)
		}
	}

	started := time.Now()
	res := w.sender.Send(context.Background(), deliver.Request{
		URL:       d.URL,
		ID:        d.EventID,
		Timestamp: started,
		Body:      body,
		Secrets:   secrets,
	})

	attempt := store.Attempt{
		StartedAt:   started,
		Duration:    res.Duration,
		StatusCode:  res.StatusCode,
		BodySnippet: res.BodySnippet,
	}
	if res.Err != nil {
		attempt.Error = res.Err.Error()
	}

	// Recording the outcome must survive shutdown, so it is not tied to the loop's context.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if res.Success() {
		err = w.store.CompleteDelivery(ctx, w.cfg.ID, d, attempt)
	} else {
		err = w.store.FailDelivery(ctx, w.cfg.ID, d, attempt, time.Now().Add(interimRetryDelay))
	}
	if err != nil {
		log.Error("record delivery outcome", "error", err)
		return
	}
	log.Info("delivery attempt", "success", res.Success(), "status", res.StatusCode,
		"duration_ms", res.Duration.Milliseconds())
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
