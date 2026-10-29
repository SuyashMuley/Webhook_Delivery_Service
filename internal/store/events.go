package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"webhookdelivery/internal/telemetry"
)

// IngestEvent stores an event and fans it out to every active endpoint of the
// tenant subscribed to its type, all in one transaction. If idemKey is set and
// the tenant already ingested that key, nothing is written and the original
// event is returned with Duplicate = true. Each delivery may be attempted at
// most maxAttempts times before it is sent to the DLQ.
func (s *Store) IngestEvent(ctx context.Context, tenantID, eventType string, payload json.RawMessage, idemKey string, maxAttempts int) (IngestResult, error) {
	// The ingest span's context is stored on every delivery so the worker can
	// continue this trace when it sends them, possibly much later.
	ctx, span := telemetry.Tracer().Start(ctx, "store.ingest_event")
	defer span.End()
	traceparent := telemetry.Inject(ctx)

	var res IngestResult
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
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
			`INSERT INTO deliveries (tenant_id, event_id, endpoint_id, max_attempts, trace_context)
			 SELECT $1::uuid, $2::uuid, id, $4::int, NULLIF($5::text, '') FROM endpoints
			  WHERE tenant_id = $1::uuid AND status = 'active'
			    AND ('*' = ANY(event_types) OR $3 = ANY(event_types))`,
			tenantID, res.EventID, eventType, maxAttempts, traceparent)
		res.Deliveries = int(tag.RowsAffected())
		// Deliberately no NOTIFY here: a transaction that issues one takes a
		// database-wide lock for the duration of its commit (including the WAL
		// flush), which serializes every ingest commit. See Notifier.
		return err
	})
	return res, err
}

// ListEventDeliveries returns the deliveries created for one of the tenant's
// events, or ErrNotFound if the event does not exist for that tenant.
func (s *Store) ListEventDeliveries(ctx context.Context, tenantID, eventID string) ([]DeliverySummary, error) {
	var out []DeliverySummary
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM events WHERE id = $1 AND tenant_id = $2)`,
			eventID, tenantID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}

		rows, err := tx.Query(ctx,
			`SELECT id::text, endpoint_id::text, status, attempt_count, last_status_code,
			        last_error, next_attempt_at, created_at, dead_at, dead_reason
			   FROM deliveries WHERE event_id = $1 AND tenant_id = $2
			  ORDER BY created_at, id`, eventID, tenantID)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (DeliverySummary, error) {
			var d DeliverySummary
			err := r.Scan(&d.ID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.LastStatusCode,
				&d.LastError, &d.NextAttemptAt, &d.CreatedAt, &d.DeadAt, &d.DeadReason)
			return d, err
		})
		return err
	})
	return out, err
}
