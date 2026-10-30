package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// QueueStats samples the state of the queue for metrics. It needs the worker
// role (or owner): it reads across tenants.
func (s *Store) QueueStats(ctx context.Context) (QueueStats, error) {
	var st QueueStats
	var oldest float64
	err := s.Pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()),
		  (SELECT count(*) FROM deliveries WHERE status = 'pending' AND next_attempt_at >  now()),
		  (SELECT count(*) FROM deliveries WHERE status = 'in_flight'),
		  (SELECT count(*) FROM endpoints WHERE status = 'active' AND circuit_open_until > now()),
		  (SELECT count(*) FROM endpoints WHERE status = 'paused'),
		  (SELECT count(*) FROM endpoints WHERE status = 'disabled'),
		  COALESCE((
		    -- oldest due delivery among endpoints that are claimable right now
		    SELECT extract(epoch FROM now() - min(q.m)) FROM (
		      SELECT (SELECT dd.next_attempt_at FROM deliveries dd
		               WHERE dd.endpoint_id = e.id AND dd.status = 'pending' AND dd.next_attempt_at <= now()
		               ORDER BY dd.next_attempt_at LIMIT 1) AS m
		        FROM endpoints e JOIN tenants t ON t.id = e.tenant_id
		       WHERE e.status = 'active' AND t.status = 'active'
		         AND (e.circuit_open_until IS NULL OR e.circuit_open_until <= now())
		         -- Work waiting because its own endpoint (or tenant) is at its concurrency cap is
		         -- waiting on the customer, not on us: a slow endpoint must not look like a slow platform.
		         AND (SELECT count(*) FROM deliveries f WHERE f.endpoint_id = e.id AND f.status = 'in_flight') < e.max_concurrency
		         AND (SELECT count(*) FROM deliveries f WHERE f.tenant_id = t.id AND f.status = 'in_flight') < t.max_inflight
		    ) q), 0)`).
		Scan(&st.Due, &st.Scheduled, &st.InFlight, &st.CircuitOpenEndpoints, &st.PausedEndpoints, &st.DisabledEndpoints, &oldest)
	if err != nil {
		return st, err
	}
	st.OldestDueAge = time.Duration(oldest * float64(time.Second))

	if err := s.Pool.QueryRow(ctx, `
		WITH ep_in AS (SELECT endpoint_id, count(*) AS n FROM deliveries WHERE status = 'in_flight' GROUP BY 1),
		     t_in  AS (SELECT tenant_id,   count(*) AS n FROM deliveries WHERE status = 'in_flight' GROUP BY 1),
		     eligible AS (
		       SELECT e.id
		         FROM endpoints e
		         JOIN tenants t ON t.id = e.tenant_id
		         LEFT JOIN ep_in ON ep_in.endpoint_id = e.id
		         LEFT JOIN t_in  ON t_in.tenant_id   = t.id
		        WHERE e.status = 'active' AND t.status = 'active'
		          AND (e.circuit_open_until IS NULL OR e.circuit_open_until <= now())
		          AND COALESCE(ep_in.n, 0) < e.max_concurrency
		          AND COALESCE(t_in.n, 0)  < t.max_inflight)
		SELECT COALESCE(sum((SELECT count(*) FROM deliveries d
		                      WHERE d.endpoint_id = eligible.id AND d.status = 'pending' AND d.next_attempt_at <= now())), 0)
		  FROM eligible`).Scan(&st.DueClaimable); err != nil {
		return st, err
	}

	rows, err := s.Pool.Query(ctx,
		`SELECT COALESCE(dead_reason, 'unknown'), count(*) FROM deliveries WHERE status = 'dead' GROUP BY 1`)
	if err != nil {
		return st, err
	}
	st.DeadByReason = map[string]int{}
	for rows.Next() {
		var reason string
		var n int
		if err := rows.Scan(&reason, &n); err != nil {
			return st, err
		}
		st.DeadByReason[reason] = n
	}
	return st, rows.Err()
}

// Purge deletes finished work older than the given ages, at most limit rows per
// category, and returns how many it removed. Successful deliveries are removed
// after succeededAge, dead-lettered ones after deadAge, and an event once it is
// older than eventAge and no delivery refers to it any more. Deleting a
// delivery removes its attempts and replay audit rows with it. Batches use
// SKIP LOCKED, so several workers can run it concurrently.
//
// Note the idempotency window: an Idempotency-Key is forgotten when its event
// is purged.
func (s *Store) Purge(ctx context.Context, succeededAge, deadAge, eventAge time.Duration, limit int) (PurgeResult, error) {
	var r PurgeResult
	var err error
	if succeededAge > 0 {
		r.Succeeded, err = s.deleteBatch(ctx,
			`WITH doomed AS (
			    SELECT id FROM deliveries WHERE status = 'succeeded' AND updated_at < now() - make_interval(secs => $1)
			     ORDER BY updated_at LIMIT $2 FOR UPDATE SKIP LOCKED)
			 DELETE FROM deliveries d USING doomed WHERE d.id = doomed.id`, succeededAge.Seconds(), limit)
		if err != nil {
			return r, err
		}
	}
	if deadAge > 0 {
		r.Dead, err = s.deleteBatch(ctx,
			`WITH doomed AS (
			    SELECT id FROM deliveries WHERE status = 'dead' AND dead_at < now() - make_interval(secs => $1)
			     ORDER BY dead_at LIMIT $2 FOR UPDATE SKIP LOCKED)
			 DELETE FROM deliveries d USING doomed WHERE d.id = doomed.id`, deadAge.Seconds(), limit)
		if err != nil {
			return r, err
		}
	}
	if eventAge > 0 {
		// No FOR UPDATE here: it needs UPDATE on events, which the worker role (rightly) lacks.
		// Two purgers racing for a row just means one deletes it and the other deletes nothing.
		r.Events, err = s.deleteBatch(ctx,
			`WITH doomed AS (
			    SELECT e.id FROM events e
			     WHERE e.created_at < now() - make_interval(secs => $1)
			       AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_id = e.id)
			     ORDER BY e.created_at LIMIT $2)
			 DELETE FROM events e USING doomed WHERE e.id = doomed.id`, eventAge.Seconds(), limit)
	}
	return r, err
}

func (s *Store) deleteBatch(ctx context.Context, q string, age float64, limit int) (int, error) {
	tag, err := s.Pool.Exec(ctx, q, age, limit)
	return int(tag.RowsAffected()), err
}

// TenantLimits returns the tenant's ingest quotas.
func (s *Store) TenantLimits(ctx context.Context, tenantID string) (TenantLimits, error) {
	var l TenantLimits
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT events_per_sec, max_pending FROM tenant_limits()`).Scan(&l.EventsPerSec, &l.MaxPending)
	})
	return l, err
}

// TakeIngestTokens takes up to want tokens from the tenant's shared ingest
// bucket and reports how many it got and the tenant's configured rate. Callers
// lease several tokens at once so the database is not hit once per event.
func (s *Store) TakeIngestTokens(ctx context.Context, tenantID string, want int) (granted, perSec int, err error) {
	err = s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT granted, per_sec FROM take_ingest_tokens($1)`, want).Scan(&granted, &perSec)
	})
	return granted, perSec, err
}

// PendingBacklog counts the tenant's pending deliveries, but looks at no more
// than cap of them: the answer only needs to say whether the backlog has
// reached cap, and bounding the scan keeps the check cheap.
func (s *Store) PendingBacklog(ctx context.Context, tenantID string, cap int) (n int, err error) {
	err = s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT count(*) FROM (SELECT 1 FROM deliveries WHERE tenant_id = $1 AND status = 'pending' LIMIT $2) q`,
			tenantID, cap).Scan(&n)
	})
	return n, err
}

// SetTenantLimits changes a tenant's quotas (admin use; runs as the owner).
func (s *Store) SetTenantLimits(ctx context.Context, tenantID string, eventsPerSec, maxPending, maxInflight int) error {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE tenants SET events_per_sec = $2, max_pending = $3, max_inflight = $4, updated_at = now() WHERE id = $1`,
		tenantID, eventsPerSec, maxPending, maxInflight)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
