package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// IngestEvent stores an event and fans it out to every active endpoint of the
// tenant subscribed to its type, all in one transaction. If idemKey is set and
// the tenant already ingested that key, nothing is written and the original
// event is returned with Duplicate = true.
func (s *Store) IngestEvent(ctx context.Context, tenantID, eventType string, payload json.RawMessage, idemKey string) (IngestResult, error) {
	var res IngestResult
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var key *string
		if idemKey != "" {
			key = &idemKey
		}
		err := tx.QueryRow(ctx,
			`INSERT INTO events (tenant_id, type, payload, idempotency_key)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
			 RETURNING id::text`,
			tenantID, eventType, payload, key).Scan(&res.EventID)

		if errors.Is(err, pgx.ErrNoRows) {
			res.Duplicate = true
			if err := tx.QueryRow(ctx,
				`SELECT id::text FROM events WHERE tenant_id = $1 AND idempotency_key = $2`,
				tenantID, idemKey).Scan(&res.EventID); err != nil {
				return err
			}
			return tx.QueryRow(ctx,
				`SELECT count(*) FROM deliveries WHERE event_id = $1`, res.EventID).Scan(&res.Deliveries)
		}
		if err != nil {
			return err
		}

		tag, err := tx.Exec(ctx,
			`INSERT INTO deliveries (tenant_id, event_id, endpoint_id)
			 SELECT $1::uuid, $2::uuid, id FROM endpoints
			  WHERE tenant_id = $1::uuid AND status = 'active'
			    AND ('*' = ANY(event_types) OR $3 = ANY(event_types))`,
			tenantID, res.EventID, eventType)
		res.Deliveries = int(tag.RowsAffected())
		return err
	})
	return res, err
}

// ListEventDeliveries returns the deliveries created for one of the tenant's
// events, or ErrNotFound if the event does not exist for that tenant.
func (s *Store) ListEventDeliveries(ctx context.Context, tenantID, eventID string) ([]DeliverySummary, error) {
	var exists bool
	if err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM events WHERE id = $1 AND tenant_id = $2)`,
		eventID, tenantID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}

	rows, err := s.Pool.Query(ctx,
		`SELECT id::text, endpoint_id::text, status, attempt_count, last_status_code,
		        last_error, next_attempt_at, created_at, dead_at
		   FROM deliveries WHERE event_id = $1 AND tenant_id = $2
		  ORDER BY created_at, id`, eventID, tenantID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (DeliverySummary, error) {
		var d DeliverySummary
		err := r.Scan(&d.ID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.LastStatusCode,
			&d.LastError, &d.NextAttemptAt, &d.CreatedAt, &d.DeadAt)
		return d, err
	})
}
