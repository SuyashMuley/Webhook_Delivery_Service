// Package ratelimit enforces per-tenant ingest quotas: an events-per-second
// rate and a cap on the backlog of undelivered work.
//
// The rate limit is a token bucket stored in Postgres so that every API
// replica shares one limit. To keep the database off the hot path a replica
// does not touch the bucket per event: it leases a small batch of tokens (about
// 100ms worth) and serves requests from that lease until it runs out or goes
// stale. The bucket therefore sees a handful of writes per tenant per second
// regardless of traffic, at the price of some slack: unused leased tokens are
// forfeited, and the tenant can briefly exceed its rate by about one lease per
// replica.
//
// Both checks fail open: if the database cannot answer, ingest is allowed and
// QuotaCheckErrors is incremented. Refusing all traffic because the limiter is
// down would turn a quota feature into an outage.
package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/store"
)

// Source is the shared state the quotas are computed from (store.Store implements it).
type Source interface {
	TakeIngestTokens(ctx context.Context, tenantID string, want int) (granted, perSec int, err error)
	TenantLimits(ctx context.Context, tenantID string) (store.TenantLimits, error)
	PendingBacklog(ctx context.Context, tenantID string, cap int) (int, error)
}

// Reasons for refusing an event.
const (
	ReasonRateLimited = "rate_limited"
	ReasonBacklogFull = "backlog_full"
)

// Decision is the result of checking one event against a tenant's quotas.
type Decision struct {
	Allowed    bool
	Reason     string        // set when !Allowed
	RetryAfter time.Duration // how long the caller should wait before retrying
}

type Config struct {
	LeaseTTL    time.Duration // how long leased tokens stay usable (default 500ms)
	LimitsTTL   time.Duration // how long a tenant's quota settings are cached (default 30s)
	BacklogTTL  time.Duration // how long a backlog count is reused (default 2s)
	DefaultRate int           // assumed rate before the first lease reveals the real one
	Now         func() time.Time
}

func (c *Config) defaults() {
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 500 * time.Millisecond
	}
	if c.LimitsTTL <= 0 {
		c.LimitsTTL = 30 * time.Second
	}
	if c.BacklogTTL <= 0 {
		c.BacklogTTL = 2 * time.Second
	}
	if c.DefaultRate <= 0 {
		c.DefaultRate = 100
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

type Quota struct {
	src Source
	m   *metrics.Metrics
	log *slog.Logger
	cfg Config

	mu      sync.Mutex
	tenants map[string]*tenantState
}

// tenantState is one tenant's local view. Its own mutex serializes that
// tenant's database round trips, so a burst of requests for one tenant
// produces one lease, not one per request, while other tenants are unaffected.
type tenantState struct {
	mu sync.Mutex

	tokens      int
	tokensUntil time.Time
	perSec      int

	limits      store.TenantLimits
	limitsUntil time.Time

	backlog      int
	backlogUntil time.Time
}

func New(src Source, m *metrics.Metrics, log *slog.Logger, cfg Config) *Quota {
	cfg.defaults()
	return &Quota{src: src, m: m, log: log, cfg: cfg, tenants: map[string]*tenantState{}}
}

func (q *Quota) state(tenantID string) *tenantState {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tenants) > 10000 { // idle tenants must not accumulate forever
		now := q.cfg.Now()
		for id, st := range q.tenants {
			if now.After(st.limitsUntil) && now.After(st.backlogUntil) && now.After(st.tokensUntil) {
				delete(q.tenants, id)
			}
		}
	}
	st, ok := q.tenants[tenantID]
	if !ok {
		st = &tenantState{perSec: q.cfg.DefaultRate}
		q.tenants[tenantID] = st
	}
	return st
}

// Allow checks one event for tenantID against its quotas and, if allowed,
// consumes a token. It also records the rejection metric.
func (q *Quota) Allow(ctx context.Context, tenantID string) Decision {
	st := q.state(tenantID)
	st.mu.Lock()
	defer st.mu.Unlock()
	now := q.cfg.Now()

	// Backlog first: no point spending a token on an event we will refuse.
	if d, refused := q.checkBacklog(ctx, st, tenantID, now); refused {
		q.reject(tenantID, d.Reason)
		return d
	}

	if now.After(st.tokensUntil) {
		st.tokens = 0
	}
	if st.tokens == 0 {
		want := min(max(st.perSec/10, 1), 100) // ~100ms worth of tokens
		granted, perSec, err := q.src.TakeIngestTokens(ctx, tenantID, want)
		if err != nil {
			q.failOpen("take ingest tokens", tenantID, err)
			return Decision{Allowed: true}
		}
		if perSec > 0 {
			st.perSec = perSec
		}
		st.tokens, st.tokensUntil = granted, now.Add(q.cfg.LeaseTTL)
	}
	if st.tokens == 0 {
		d := Decision{Reason: ReasonRateLimited, RetryAfter: retryAfterForRate(st.perSec)}
		q.reject(tenantID, d.Reason)
		return d
	}
	st.tokens--
	return Decision{Allowed: true}
}

func (q *Quota) checkBacklog(ctx context.Context, st *tenantState, tenantID string, now time.Time) (Decision, bool) {
	if now.After(st.limitsUntil) {
		l, err := q.src.TenantLimits(ctx, tenantID)
		if err != nil {
			q.failOpen("load tenant limits", tenantID, err)
			return Decision{}, false
		}
		st.limits, st.limitsUntil = l, now.Add(q.cfg.LimitsTTL)
	}
	if st.limits.MaxPending <= 0 {
		return Decision{}, false
	}
	if now.After(st.backlogUntil) {
		n, err := q.src.PendingBacklog(ctx, tenantID, st.limits.MaxPending)
		if err != nil {
			q.failOpen("count backlog", tenantID, err)
			return Decision{}, false
		}
		st.backlog, st.backlogUntil = n, now.Add(q.cfg.BacklogTTL)
	}
	if st.backlog >= st.limits.MaxPending {
		// Retry once the cached count will have been refreshed, with a floor so
		// well-behaved producers back off instead of hammering.
		return Decision{Reason: ReasonBacklogFull, RetryAfter: max(q.cfg.BacklogTTL, 5*time.Second)}, true
	}
	return Decision{}, false
}

func (q *Quota) reject(tenantID, reason string) {
	if q.m != nil {
		q.m.IngestRejected.WithLabelValues(reason).Inc()
	}
	q.log.Debug("ingest rejected by quota", "tenant_id", tenantID, "reason", reason)
}

func (q *Quota) failOpen(what, tenantID string, err error) {
	if q.m != nil {
		q.m.QuotaCheckErrors.Inc()
	}
	q.log.Warn("quota check failed; allowing the request", "check", what, "tenant_id", tenantID, "error", err)
}

// retryAfterForRate is how long until one more token is available, never less
// than a second (Retry-After has whole-second granularity).
func retryAfterForRate(perSec int) time.Duration {
	if perSec <= 0 {
		return time.Second
	}
	return max(time.Second, time.Second/time.Duration(perSec))
}
