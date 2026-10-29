package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// claimSQL leases up to $1 due deliveries to worker $2 for $3 seconds, taking
// at most $4 per tenant. $5 is the circuit breaker threshold.
//
// Isolation, from the outside in:
//   - Tenants are visited in random order and each contributes a bounded slice,
//     so one tenant's backlog cannot starve the others. A tenant never has more
//     than its max_inflight in flight (a soft limit: concurrent claimers can
//     overshoot by up to workers x slice).
//   - Within a tenant, endpoints are visited in random order and each may only
//     fill the slots left under its own max_concurrency, so one slow or dead
//     endpoint cannot absorb the workers' capacity.
//   - Endpoints whose circuit is open, and endpoints that are paused or
//     disabled, are skipped entirely: their deliveries wait. An endpoint at or
//     over the breaker threshold whose open window has passed is half-open and
//     gets a concurrency of 1, a single probe.
//
// FOR UPDATE SKIP LOCKED sits in the innermost subquery, so rows are locked only
// as they are produced and concurrent workers pick disjoint rows without
// blocking each other. The attempt counter is bumped at claim time so a crashed
// worker's attempt still counts toward max_attempts.
const claimSQL = `
WITH due AS (
    SELECT x.id
      FROM (SELECT id, max_inflight FROM tenants WHERE status = 'active' ORDER BY random()) t
     CROSS JOIN LATERAL (
           SELECT m.id
             FROM (SELECT e.id, e.max_concurrency, e.consecutive_failures
                     FROM endpoints e
                    WHERE e.tenant_id = t.id AND e.status = 'active'
                      AND (e.circuit_open_until IS NULL OR e.circuit_open_until <= now())
                    ORDER BY random()) e
            CROSS JOIN LATERAL (
                  SELECT dd.id
                    FROM deliveries dd
                   WHERE dd.endpoint_id = e.id AND dd.status = 'pending' AND dd.next_attempt_at <= now()
                   ORDER BY dd.next_attempt_at
                   LIMIT GREATEST(0,
                         (CASE WHEN e.consecutive_failures >= $5 THEN 1 ELSE e.max_concurrency END)
                         - (SELECT count(*) FROM deliveries f WHERE f.endpoint_id = e.id AND f.status = 'in_flight'))
                     FOR UPDATE OF dd SKIP LOCKED
                 ) m
            LIMIT GREATEST(0, LEAST($4::int, t.max_inflight
                  - (SELECT count(*) FROM deliveries f WHERE f.tenant_id = t.id AND f.status = 'in_flight')))
          ) x
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
 RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempt_count, d.max_attempts, d.created_at, d.trace_context
)
SELECT c.id::text, c.tenant_id::text, c.event_id::text, c.endpoint_id::text, c.attempt_count, c.max_attempts,
       ev.type, ev.payload, ev.created_at, ep.url, ep.secrets, c.created_at, c.trace_context
  FROM claimed c
  JOIN events ev ON ev.id = c.event_id
  JOIN endpoints ep ON ep.id = c.endpoint_id`

// ClaimDeliveries atomically leases up to limit due deliveries to workerID,
// taking at most perTenant from any one tenant. Signing secrets are decrypted;
// if an endpoint's secrets cannot be, the delivery is still returned with
// SecretsErr set so the worker can fail the attempt instead of losing the lease.
func (s *Store) ClaimDeliveries(ctx context.Context, workerID string, limit, perTenant int, lease time.Duration) ([]ClaimedDelivery, error) {
	rows, err := s.Pool.Query(ctx, claimSQL, limit, workerID, lease.Seconds(), perTenant, s.Breaker.Threshold)
	if err != nil {
		return nil, err
	}
	claimed, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (ClaimedDelivery, error) {
		var d ClaimedDelivery
		var trace *string
		err := r.Scan(&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.Attempt, &d.MaxAttempts,
			&d.EventType, &d.Payload, &d.EventCreatedAt, &d.URL, &d.Secrets, &d.CreatedAt, &trace)
		if trace != nil {
			d.TraceContext = *trace
		}
		return d, err
	})
	if err != nil {
		return nil, err
	}
	for i := range claimed {
		d := &claimed[i]
		d.Secrets, d.SecretsErr = s.openSecrets(d.EndpointID, d.Secrets)
	}
	return claimed, nil
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

// finish writes the attempt, the delivery's new state and the endpoint's
// health atomically. It only applies if workerID still holds the lease, so a
// stale worker can never overwrite the outcome of a delivery that was
// reclaimed and re-sent.
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

		switch {
		case o.status == "succeeded":
			return s.recordEndpointSuccess(ctx, tx, d.EndpointID)
		case o.deadReason == DeadEndpointGone:
			_, err = tx.Exec(ctx,
				`UPDATE endpoints SET status = 'paused', updated_at = now()
				  WHERE id = $1 AND status = 'active'`, d.EndpointID)
			return err
		case a.StatusCode == 429 || a.Local:
			// 429: the receiver answered and asked us to slow down. It is alive,
			// so this is backpressure (handled by Retry-After), not endpoint
			// failure. Local: the fault is ours, not the endpoint's.
			return nil
		default:
			return s.recordEndpointFailure(ctx, tx, d.EndpointID, time.Now())
		}
	})
}

// recordEndpointSuccess closes the circuit and clears the failure streak. It
// only writes when there is something to clear, so a healthy endpoint's row is
// not rewritten on every delivery.
func (s *Store) recordEndpointSuccess(ctx context.Context, tx pgx.Tx, endpointID string) error {
	_, err := tx.Exec(ctx,
		`UPDATE endpoints
		    SET consecutive_failures = 0, circuit_trips = 0, circuit_open_until = NULL, failing_since = NULL
		  WHERE id = $1
		    AND (consecutive_failures > 0 OR circuit_trips > 0 OR circuit_open_until IS NOT NULL OR failing_since IS NOT NULL)`,
		endpointID)
	return err
}

// recordEndpointFailure advances the endpoint's circuit breaker and, if the
// endpoint has been failing for longer than the policy allows, disables it and
// dead-letters the deliveries still waiting for it.
func (s *Store) recordEndpointFailure(ctx context.Context, tx pgx.Tx, endpointID string, now time.Time) error {
	var h endpointHealth
	err := tx.QueryRow(ctx,
		`SELECT status, consecutive_failures, circuit_trips, failing_since, circuit_open_until
		   FROM endpoints WHERE id = $1 FOR UPDATE`, endpointID).
		Scan(&h.Status, &h.Failures, &h.Trips, &h.FailingSince, &h.OpenUntil)
	if err != nil {
		return fmt.Errorf("load endpoint health: %w", err)
	}

	next, disabled := s.Breaker.onFailure(h, now)
	var reason *string
	if disabled {
		r := fmt.Sprintf("failing continuously since %s", next.FailingSince.UTC().Format(time.RFC3339))
		reason = &r
	}
	if _, err := tx.Exec(ctx,
		`UPDATE endpoints
		    SET status = $2, consecutive_failures = $3, circuit_trips = $4,
		        failing_since = $5, circuit_open_until = $6,
		        disabled_at = CASE WHEN $7::text IS NOT NULL THEN now() ELSE disabled_at END,
		        disabled_reason = COALESCE($7, disabled_reason),
		        updated_at = now()
		  WHERE id = $1`,
		endpointID, next.Status, next.Failures, next.Trips, next.FailingSince, next.OpenUntil, reason); err != nil {
		return err
	}

	if disabled {
		_, err = tx.Exec(ctx,
			`UPDATE deliveries
			    SET status = 'dead', dead_at = now(), dead_reason = $2, updated_at = now()
			  WHERE endpoint_id = $1 AND status = 'pending'`, endpointID, DeadEndpointDisabled)
	}
	return err
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

// ReleaseWorkerLeases hands every delivery still leased to workerID straight
// back to the queue, undoing the attempt it had started (the attempt counter is
// decremented, no attempt row is written). A worker calls it when it is
// shutting down and could not finish its in-flight work in time, so that work
// is retried immediately by another worker instead of waiting for the lease to
// expire. The request may already have reached the receiver, so, as always,
// delivery is at-least-once.
func (s *Store) ReleaseWorkerLeases(ctx context.Context, workerID string) (int, error) {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE deliveries
		    SET status = 'pending', attempt_count = GREATEST(attempt_count - 1, 0),
		        next_attempt_at = now(), lease_expires_at = NULL, locked_by = NULL, updated_at = now()
		  WHERE locked_by = $1 AND status = 'in_flight'`, workerID)
	return int(tag.RowsAffected()), err
}
