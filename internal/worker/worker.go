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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhookdelivery/internal/deliver"
	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/retry"
	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/telemetry"
)

// reclaimBatch bounds how many expired leases one reaper pass handles.
const reclaimBatch = 500

type Config struct {
	ID             string // identifies this worker in deliveries.locked_by
	BatchSize      int
	TenantSlice    int // max deliveries claimed from one tenant per poll
	Concurrency    int
	PollInterval   time.Duration // how long an idle worker sleeps; the fallback when notifications are off or missed
	Listen         bool          // wake idle workers instantly via Postgres LISTEN/NOTIFY when work arrives
	Lease          time.Duration
	ReaperInterval time.Duration
	Retry          retry.Policy

	// DrainTimeout is how long shutdown waits for in-flight deliveries to finish
	// before aborting them and handing their leases back. Default 20s.
	DrainTimeout time.Duration

	Metrics *metrics.Metrics // optional

	// Queue-state sampling for metrics. Default 15s.
	StatsInterval time.Duration

	// Retention. A zero age disables that category; a zero PurgeInterval disables the job.
	PurgeInterval   time.Duration
	PurgeBatch      int // rows per category per statement (default 1000)
	RetainSucceeded time.Duration
	RetainDead      time.Duration
	RetainEvents    time.Duration
}

type Worker struct {
	store  *store.Store
	sender *deliver.Sender
	log    *slog.Logger
	cfg    Config
	m      *metrics.Metrics

	// sendCtx bounds every outbound request. It is cancelled only when shutdown
	// has waited DrainTimeout and must abort what is still running.
	sendCtx context.Context
}

func New(s *store.Store, sender *deliver.Sender, log *slog.Logger, cfg Config) *Worker {
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 20 * time.Second
	}
	if cfg.StatsInterval <= 0 {
		cfg.StatsInterval = 15 * time.Second
	}
	if cfg.PurgeBatch <= 0 {
		cfg.PurgeBatch = 1000
	}
	m := cfg.Metrics
	if m == nil {
		m = metrics.New() // keeps the code below free of nil checks
	}
	return &Worker{store: s, sender: sender, log: log.With("worker_id", cfg.ID), cfg: cfg, m: m, sendCtx: context.Background()}
}

// envelope is the JSON body sent to receivers.
type envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// Run claims and delivers until ctx is cancelled. It then stops claiming, waits
// up to DrainTimeout for in-flight deliveries to finish, aborts any that are
// still running and hands their leases straight back to the queue so another
// worker retries them immediately rather than after the lease expires.
func (w *Worker) Run(ctx context.Context) error {
	sendCtx, abortSends := context.WithCancel(context.Background())
	defer abortSends()
	w.sendCtx = sendCtx

	slots := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup

	// wake has room for one pending signal: bursts of notifications coalesce into a
	// single early poll, and a busy worker (which does not sleep) ignores them.
	wake := make(chan struct{}, 1)
	if w.cfg.Listen {
		lctx, stopListening := context.WithCancel(ctx)
		defer stopListening()
		go w.store.Listen(lctx, store.NotifyChannel,
			func() {
				select {
				case wake <- struct{}{}:
				default:
				}
			},
			func(err error) {
				w.log.Warn("listening for new work failed; falling back to polling until it reconnects", "error", err)
			})
	}

	for ctx.Err() == nil {
		free := cap(slots) - len(slots)
		if free == 0 {
			sleep(ctx, 10*time.Millisecond)
			continue
		}

		claimed, err := w.store.ClaimDeliveries(ctx, w.cfg.ID, min(w.cfg.BatchSize, free), w.cfg.TenantSlice, w.cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				w.m.ClaimErrors.Inc()
				w.log.Error("claim deliveries", "error", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		w.m.ClaimBatchSize.Observe(float64(len(claimed)))

		for _, d := range claimed {
			slots <- struct{}{} // never blocks: we only claimed as many as there were free slots
			wg.Add(1)
			w.m.Inflight.Inc()
			go func() {
				defer func() { <-slots; w.m.Inflight.Dec(); wg.Done() }()
				w.process(d)
			}()
		}
		if len(claimed) == 0 {
			select { // idle: sleep until woken by new work, the poll interval, or shutdown
			case <-wake:
			case <-time.After(w.cfg.PollInterval):
			case <-ctx.Done():
			}
		}
	}

	w.drain(&wg, abortSends)
	return nil
}

func (w *Worker) drain(wg *sync.WaitGroup, abortSends context.CancelFunc) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(w.cfg.DrainTimeout):
		w.log.Warn("drain timeout reached; aborting in-flight deliveries", "timeout", w.cfg.DrainTimeout)
		abortSends()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			w.log.Error("in-flight deliveries did not stop after being aborted")
		}
	}

	// Whatever is still leased to us was started but not finished: give it back.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := w.store.ReleaseWorkerLeases(ctx, w.cfg.ID)
	if err != nil {
		w.log.Error("release leases on shutdown (they will expire instead)", "error", err)
		return
	}
	if n > 0 {
		w.m.Deliveries.WithLabelValues(metrics.OutcomeReleased).Add(float64(n))
		w.log.Info("released unfinished deliveries back to the queue", "count", n)
	}
}

func (w *Worker) process(d store.ClaimedDelivery) {
	// Continue the trace of the request that created this delivery. It may have
	// run in another process hours ago; without stored context this is a new root.
	ctx, span := telemetry.Tracer().Start(telemetry.Extract(context.Background(), d.TraceContext),
		"delivery.attempt", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	span.SetAttributes(
		attribute.String("delivery.id", d.ID), attribute.String("event.id", d.EventID),
		attribute.String("endpoint.id", d.EndpointID), attribute.String("tenant.id", d.TenantID),
		attribute.Int("delivery.attempt", d.Attempt))
	traceID, _ := telemetry.IDs(ctx)

	log := w.log.With("delivery_id", d.ID, "event_id", d.EventID, "endpoint_id", d.EndpointID,
		"tenant_id", d.TenantID, "attempt", d.Attempt, "trace_id", traceID)

	body, err := json.Marshal(envelope{ID: d.EventID, Type: d.EventType, CreatedAt: d.EventCreatedAt, Data: d.Payload})
	if err != nil {
		log.Error("marshal envelope", "error", err)
		span.SetStatus(codes.Error, "marshal envelope")
		return // lease expires and the reaper will recover it
	}

	started := time.Now()
	if d.Attempt == 1 {
		w.m.FirstAttemptDelay.Observe(started.Sub(d.CreatedAt).Seconds())
	}

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
		res = w.sender.Send(w.sendCtx, deliver.Request{
			URL:       d.URL,
			ID:        d.EventID,
			Timestamp: started,
			Body:      body,
			Secrets:   secrets,
		})
	}

	// Shutdown aborted the request mid-flight. The outcome is unknown and not
	// the endpoint's fault, so record nothing: drain hands the lease back and
	// another worker retries immediately.
	if w.sendCtx.Err() != nil && !res.Success() {
		log.Info("delivery aborted by shutdown; its lease will be released")
		span.AddEvent("aborted by shutdown")
		return
	}

	w.m.AttemptDuration.WithLabelValues(resultClass(res, local)).Observe(res.Duration.Seconds())
	span.SetAttributes(attribute.Int("http.response.status_code", res.StatusCode))

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
	rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		outcome string
		delay   time.Duration
	)
	switch {
	case res.Success():
		outcome = metrics.OutcomeSucceeded
		err = w.store.CompleteDelivery(rctx, w.cfg.ID, d, attempt)
	case res.StatusCode == http.StatusGone:
		// 410 is the receiver telling us the endpoint no longer exists: stop
		// sending, dead-letter this delivery and pause the endpoint.
		outcome = metrics.OutcomeDeadEndpointGone
		err = w.store.KillDelivery(rctx, w.cfg.ID, d, attempt, store.DeadEndpointGone)
	case d.Attempt >= d.MaxAttempts:
		outcome = metrics.OutcomeDeadMaxAttempts
		err = w.store.KillDelivery(rctx, w.cfg.ID, d, attempt, store.DeadMaxAttempts)
	default:
		// Retry-After only means something on 429 and 503.
		var retryAfter time.Duration
		if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusServiceUnavailable {
			retryAfter = res.RetryAfter
		}
		delay = w.cfg.Retry.Delay(d.Attempt, retryAfter, retry.Jitter())
		outcome = metrics.OutcomeRetry
		err = w.store.FailDelivery(rctx, w.cfg.ID, d, attempt, time.Now().Add(delay))
	}
	if errors.Is(err, store.ErrLeaseLost) {
		// The lease expired and the delivery was reclaimed while we were sending.
		// Whoever holds it now owns the outcome; ours must not overwrite it.
		w.m.Deliveries.WithLabelValues(metrics.OutcomeLeaseLost).Inc()
		log.Warn("lease lost before the outcome could be recorded", "status", res.StatusCode)
		span.SetStatus(codes.Error, "lease lost")
		return
	}
	if err != nil {
		log.Error("record delivery outcome", "error", err)
		span.SetStatus(codes.Error, "record outcome")
		return
	}

	w.m.Deliveries.WithLabelValues(outcome).Inc()
	span.SetAttributes(attribute.String("delivery.outcome", outcome))
	if outcome != metrics.OutcomeSucceeded {
		span.SetStatus(codes.Error, outcome)
	}
	log.Info("delivery attempt", "outcome", outcome, "status", res.StatusCode,
		"duration_ms", res.Duration.Milliseconds(), "retry_in", delay.Round(time.Second))
}

// resultClass buckets an attempt for the duration histogram without unbounded labels.
func resultClass(res deliver.Result, local bool) string {
	switch {
	case local:
		return "local_error"
	case res.Err != nil:
		return "network_error"
	case res.StatusCode >= 200 && res.StatusCode < 300:
		return "success"
	case res.StatusCode == http.StatusTooManyRequests:
		return "http_429"
	case res.StatusCode >= 500:
		return "http_5xx"
	case res.StatusCode >= 400:
		return "http_4xx"
	default:
		return "http_other"
	}
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
				w.m.LeaseReclaims.WithLabelValues("requeued").Add(float64(requeued))
				w.m.LeaseReclaims.WithLabelValues("dead").Add(float64(dead))
				w.log.Warn("reclaimed expired leases", "requeued", requeued, "dead_lettered", dead)
			}
			if requeued+dead < reclaimBatch {
				break
			}
		}
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

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
