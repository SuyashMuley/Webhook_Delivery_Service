package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ListDead returns up to limit DLQ entries for a tenant, newest first, starting
// after cursor (nil means from the start).
func (s *Store) ListDead(ctx context.Context, tenantID string, f DeadFilter, cursor *DeadCursor, limit int) ([]DeadDelivery, error) {
	var curAt, curID any
	if cursor != nil {
		curAt, curID = cursor.DeadAt, cursor.ID
	}
	var out []DeadDelivery
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT d.id::text, d.event_id::text, ev.type, d.endpoint_id::text, d.dead_reason,
		        d.attempt_count, d.last_status_code, d.last_error, d.dead_at, d.created_at
		   FROM deliveries d JOIN events ev ON ev.id = d.event_id
		  WHERE d.tenant_id = $1 AND d.status = 'dead'
		    AND ($2::uuid IS NULL OR d.endpoint_id = $2::uuid)
		    AND ($3::text IS NULL OR d.dead_reason = $3::text)
		    AND ($4::timestamptz IS NULL OR d.dead_at >= $4::timestamptz)
		    AND ($5::timestamptz IS NULL OR (d.dead_at, d.id) < ($5::timestamptz, $6::uuid))
		  ORDER BY d.dead_at DESC, d.id DESC
		  LIMIT $7`,
			tenantID, nullStr(f.EndpointID), nullStr(f.Reason), f.Since, curAt, curID, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (DeadDelivery, error) {
			var d DeadDelivery
			err := r.Scan(&d.DeliveryID, &d.EventID, &d.EventType, &d.EndpointID, &d.Reason,
				&d.AttemptCount, &d.LastStatusCode, &d.LastError, &d.DeadAt, &d.CreatedAt)
			return d, err
		})
		return err
	})
	return out, err
}

// GetDelivery returns one of the tenant's deliveries with its attempt history.
func (s *Store) GetDelivery(ctx context.Context, tenantID, id string) (DeliveryDetail, error) {
	var d DeliveryDetail
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx,
			`SELECT d.id::text, d.endpoint_id::text, d.status, d.attempt_count, d.last_status_code,
			        d.last_error, d.next_attempt_at, d.created_at, d.dead_at, d.dead_reason,
			        d.event_id::text, ev.type, d.max_attempts
			   FROM deliveries d JOIN events ev ON ev.id = d.event_id
			  WHERE d.id = $1 AND d.tenant_id = $2`, id, tenantID).
			Scan(&d.ID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.LastStatusCode,
				&d.LastError, &d.NextAttemptAt, &d.CreatedAt, &d.DeadAt, &d.DeadReason,
				&d.EventID, &d.EventType, &d.MaxAttempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		rows, err := tx.Query(ctx,
			`SELECT attempt_no, started_at, duration_ms, status_code, error, response_body_snippet
			   FROM attempts WHERE delivery_id = $1 ORDER BY id`, id)
		if err != nil {
			return err
		}
		d.Attempts, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (AttemptRecord, error) {
			var a AttemptRecord
			err := r.Scan(&a.AttemptNo, &a.StartedAt, &a.DurationMS, &a.StatusCode, &a.Error, &a.ResponseBodySnippet)
			return a, err
		})
		return err
	})
	if d.Attempts == nil {
		d.Attempts = []AttemptRecord{}
	}
	return d, err
}

const replaySQL = `
WITH picked AS (
    SELECT id, attempt_count, dead_reason, dead_at FROM deliveries
     WHERE tenant_id = $1 AND status = 'dead'
       AND ($2::uuid IS NULL OR id = $2::uuid)
       AND ($3::uuid IS NULL OR endpoint_id = $3::uuid)
       AND ($4::text IS NULL OR dead_reason = $4::text)
       AND ($5::timestamptz IS NULL OR dead_at >= $5::timestamptz)
     ORDER BY dead_at, id
     LIMIT $6
       FOR UPDATE
), numbered AS (
    SELECT id, attempt_count, dead_reason, row_number() OVER (ORDER BY dead_at, id) AS rn FROM picked
), replayed AS (
    UPDATE deliveries d
       SET status = 'pending',
           attempt_count = 0,
           -- Stagger due times so a bulk replay trickles out at $7 deliveries/sec
           -- instead of hitting the receiver all at once.
           next_attempt_at = now() + make_interval(secs =>
               CASE WHEN $7::float8 > 0 THEN (n.rn - 1)::float8 / $7::float8 ELSE 0 END),
           dead_at = NULL, dead_reason = NULL, last_error = NULL, last_status_code = NULL,
           updated_at = now()
      FROM numbered n
     WHERE d.id = n.id
 RETURNING d.id
), audit AS (
    INSERT INTO delivery_replays (delivery_id, tenant_id, prior_dead_reason, prior_attempt_count)
    SELECT id, $1::uuid, dead_reason, attempt_count FROM numbered
)
SELECT count(*) FROM replayed`

// ReplayDead moves dead deliveries matching f back to the queue (at most limit
// of them) with a fresh attempt budget, spreading their due times over time so
// they are sent at about ratePerSec per second (0 means all at once). Each
// replay is recorded in delivery_replays. It returns how many were replayed.
func (s *Store) ReplayDead(ctx context.Context, tenantID string, f DeadFilter, limit int, ratePerSec float64) (int, error) {
	var n int
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, replaySQL, tenantID, nullStr(f.DeliveryID), nullStr(f.EndpointID),
			nullStr(f.Reason), f.Since, limit, ratePerSec).Scan(&n)
	})
	return n, err
}

// ReplayDelivery replays one dead delivery. It returns ErrNotFound if the
// tenant has no such delivery and ErrConflict if it is not in the DLQ.
func (s *Store) ReplayDelivery(ctx context.Context, tenantID, id string) error {
	n, err := s.ReplayDead(ctx, tenantID, DeadFilter{DeliveryID: id}, 1, 0)
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	var exists bool
	if err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM deliveries WHERE id = $1 AND tenant_id = $2)`,
			id, tenantID).Scan(&exists)
	}); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return ErrConflict
}
