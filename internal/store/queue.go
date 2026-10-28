package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// claimSQL leases up to $1 due deliveries to worker $2 for $3 seconds, taking
// at most $4 per tenant.
//
// Fairness: tenants are visited in random order and each contributes a bounded
// slice, so one tenant with a huge backlog cannot starve the others. A tenant
// is also never given more than its max_inflight in total (a soft limit:
// concurrent claimers may overshoot it by up to workers x slice).
//
// Concurrency: FOR UPDATE SKIP LOCKED sits inside the per-tenant LATERAL
// subquery, so rows are locked only as they are produced and concurrent
// workers pick disjoint rows without blocking. Only deliveries of active
// endpoints are claimable; deliveries for paused endpoints wait.
//
// The attempt counter is bumped at claim time so a crashed worker's attempt
// still counts toward max_attempts.
const claimSQL = `
WITH due AS (
    SELECT d.id
      FROM (SELECT id, max_inflight FROM tenants WHERE status = 'active' ORDER BY random()) t
     CROSS JOIN LATERAL (
           SELECT d.id
             FROM deliveries d
             JOIN endpoints e ON e.id = d.endpoint_id AND e.status = 'active'
            WHERE d.tenant_id = t.id AND d.status = 'pending' AND d.next_attempt_at <= now()
            ORDER BY d.next_attempt_at
            LIMIT GREATEST(0, LEAST($4::int, t.max_inflight
                  - (SELECT count(*) FROM deliveries x WHERE x.tenant_id = t.id AND x.status = 'in_flight')))
              FOR UPDATE OF d SKIP LOCKED
          ) d
     LIMIT $1
), claimed AS (
    UPDATE deliveries d
       SET status = 'in_flight',
           locked_by = $2,
           lease_expires_at = now() + make_interval(secs => $3),
           attempt_count = d.attempt_count + 1,
           updated_at = now()
      FROM due
     WHERE d.id = due.id
 RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempt_count, d.max_attempts
)
SELECT c.id::text, c.tenant_id::text, c.event_id::text, c.endpoint_id::text, c.attempt_count, c.max_attempts,
       ev.type, ev.payload, ev.created_at, ep.url, ep.secrets
  FROM claimed c
  JOIN events ev ON ev.id = c.event_id
  JOIN endpoints ep ON ep.id = c.endpoint_id`

// ClaimDeliveries atomically leases up to limit due deliveries to workerID,
// taking at most perTenant from any one tenant.
func (s *Store) ClaimDeliveries(ctx context.Context, workerID string, limit, perTenant int, lease time.Duration) ([]ClaimedDelivery, error) {
	rows, err := s.Pool.Query(ctx, claimSQL, limit, workerID, lease.Seconds(), perTenant)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ClaimedDelivery, error) {
		var d ClaimedDelivery
		err := r.Scan(&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.Attempt, &d.MaxAttempts,
			&d.EventType, &d.Payload, &d.EventCreatedAt, &d.URL, &d.Secrets)
		return d, err
	})
}

// CompleteDelivery records a successful attempt and marks the delivery succeeded.
func (s *Store) CompleteDelivery(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt) error {
	return s.finish(ctx, workerID, d, a, outcome{status: "succeeded"})
}

// FailDelivery records a failed attempt and puts the delivery back to pending,
// due again at retryAt.
func (s *Store) FailDelivery(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt, retryAt time.Time) error {
	return s.finish(ctx, workerID, d, a, outcome{status: "pending", retryAt: &retryAt})
}

// KillDelivery records the final attempt and moves the delivery to the DLQ
// with one of the Dead* reasons. For DeadEndpointGone it also pauses the
// endpoint so nothing else is sent to it until the tenant resumes it.
func (s *Store) KillDelivery(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt, reason string) error {
	return s.finish(ctx, workerID, d, a, outcome{status: "dead", deadReason: reason})
}

type outcome struct {
	status     string
	retryAt    *time.Time
	deadReason string
}

// finish writes the attempt and the delivery's new state atomically. It only
// applies if workerID still holds the lease, so a stale worker can never
// overwrite the outcome of a delivery that was reclaimed and re-sent.
func (s *Store) finish(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt, o outcome) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE deliveries
			    SET status = $3::text,
			        next_attempt_at = COALESCE($4, next_attempt_at),
			        last_status_code = $5,
			        last_error = $6,
			        dead_at = CASE WHEN $3::text = 'dead' THEN now() END,
			        dead_reason = $7,
			        lease_expires_at = NULL,
			        locked_by = NULL,
			        updated_at = now()
			  WHERE id = $1 AND status = 'in_flight' AND locked_by = $2`,
			d.ID, workerID, o.status, o.retryAt, nullInt(a.StatusCode), nullStr(a.Error), nullStr(o.deadReason))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		if _, err = tx.Exec(ctx,
			`INSERT INTO attempts (delivery_id, attempt_no, started_at, duration_ms,
			                       status_code, error, response_body_snippet)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			d.ID, d.Attempt, a.StartedAt, a.Duration.Milliseconds(),
			nullInt(a.StatusCode), nullStr(a.Error), nullStr(a.BodySnippet)); err != nil {
			return err
		}
		if o.deadReason == DeadEndpointGone {
			_, err = tx.Exec(ctx,
				`UPDATE endpoints SET status = 'paused', updated_at = now()
				  WHERE id = $1 AND status = 'active'`, d.EndpointID)
		}
		return err
	})
}

const reclaimSQL = `
WITH expired AS (
    SELECT id FROM deliveries
     WHERE status = 'in_flight' AND lease_expires_at < now()
     ORDER BY lease_expires_at
     LIMIT $1
       FOR UPDATE SKIP LOCKED
), reclaimed AS (
    UPDATE deliveries d
       SET status = CASE WHEN d.attempt_count >= d.max_attempts THEN 'dead' ELSE 'pending' END,
           next_attempt_at = now(),
           dead_at = CASE WHEN d.attempt_count >= d.max_attempts THEN now() END,
           dead_reason = CASE WHEN d.attempt_count >= d.max_attempts THEN 'lease_expired' END,
           last_error = 'lease expired before the attempt finished (worker crashed or stalled)',
           lease_expires_at = NULL,
           locked_by = NULL,
           updated_at = now()
      FROM expired
     WHERE d.id = expired.id
 RETURNING d.id, d.attempt_count, d.status
), logged AS (
    INSERT INTO attempts (delivery_id, attempt_no, error)
    SELECT id, attempt_count, 'lease expired before the attempt finished (worker crashed or stalled)'
      FROM reclaimed
)
SELECT count(*) FILTER (WHERE status = 'pending'), count(*) FILTER (WHERE status = 'dead')
  FROM reclaimed`

// ReclaimExpired returns deliveries whose lease ran out (the worker crashed or
// stalled) to the queue. A delivery that has already used all its attempts goes
// to the DLQ instead, so a payload that crashes workers cannot loop forever.
// It is safe to call from many workers at once.
func (s *Store) ReclaimExpired(ctx context.Context, limit int) (requeued, dead int, err error) {
	err = s.Pool.QueryRow(ctx, reclaimSQL, limit).Scan(&requeued, &dead)
	return requeued, dead, err
}

func nullInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}
