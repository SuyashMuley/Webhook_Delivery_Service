package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// claimSQL leases up to $1 due deliveries to worker $2 for $3 seconds.
// FOR UPDATE SKIP LOCKED makes concurrent workers pick disjoint rows without
// blocking each other. The attempt counter is bumped at claim time so a
// crashed worker's attempt still counts.
const claimSQL = `
WITH due AS (
    SELECT id FROM deliveries
     WHERE status = 'pending' AND next_attempt_at <= now()
     ORDER BY next_attempt_at
     LIMIT $1
       FOR UPDATE SKIP LOCKED
), claimed AS (
    UPDATE deliveries d
       SET status = 'in_flight',
           locked_by = $2,
           lease_expires_at = now() + make_interval(secs => $3),
           attempt_count = d.attempt_count + 1,
           updated_at = now()
      FROM due
     WHERE d.id = due.id
 RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempt_count
)
SELECT c.id::text, c.tenant_id::text, c.event_id::text, c.endpoint_id::text, c.attempt_count,
       ev.type, ev.payload, ev.created_at, ep.url, ep.secrets
  FROM claimed c
  JOIN events ev ON ev.id = c.event_id
  JOIN endpoints ep ON ep.id = c.endpoint_id`

// ClaimDeliveries atomically leases up to limit due deliveries to workerID.
func (s *Store) ClaimDeliveries(ctx context.Context, workerID string, limit int, lease time.Duration) ([]ClaimedDelivery, error) {
	rows, err := s.Pool.Query(ctx, claimSQL, limit, workerID, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ClaimedDelivery, error) {
		var d ClaimedDelivery
		err := r.Scan(&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.Attempt,
			&d.EventType, &d.Payload, &d.EventCreatedAt, &d.URL, &d.Secrets)
		return d, err
	})
}

// CompleteDelivery records a successful attempt and marks the delivery succeeded.
func (s *Store) CompleteDelivery(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt) error {
	return s.finish(ctx, workerID, d, a, "succeeded", nil)
}

// FailDelivery records a failed attempt and puts the delivery back to pending,
// due again at retryAt.
func (s *Store) FailDelivery(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt, retryAt time.Time) error {
	return s.finish(ctx, workerID, d, a, "pending", &retryAt)
}

// finish writes the attempt and the delivery's new state atomically. It only
// applies if workerID still holds the lease, so a stale worker can never
// overwrite the outcome of a delivery that was reclaimed and re-sent.
func (s *Store) finish(ctx context.Context, workerID string, d ClaimedDelivery, a Attempt, status string, retryAt *time.Time) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE deliveries
			    SET status = $3,
			        next_attempt_at = COALESCE($4, next_attempt_at),
			        last_status_code = $5,
			        last_error = $6,
			        lease_expires_at = NULL,
			        locked_by = NULL,
			        updated_at = now()
			  WHERE id = $1 AND status = 'in_flight' AND locked_by = $2`,
			d.ID, workerID, status, retryAt, nullInt(a.StatusCode), nullStr(a.Error))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO attempts (delivery_id, attempt_no, started_at, duration_ms,
			                       status_code, error, response_body_snippet)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			d.ID, d.Attempt, a.StartedAt, a.Duration.Milliseconds(),
			nullInt(a.StatusCode), nullStr(a.Error), nullStr(a.BodySnippet))
		return err
	})
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
