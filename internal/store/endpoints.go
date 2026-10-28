package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

const endpointCols = `id::text, tenant_id::text, url, description, event_types, secrets,
	status, max_concurrency, created_at, updated_at`

func scanEndpoint(row pgx.Row) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.TenantID, &e.URL, &e.Description, &e.EventTypes, &e.Secrets,
		&e.Status, &e.MaxConcurrency, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, ErrNotFound
	}
	return e, err
}

func (s *Store) CreateEndpoint(ctx context.Context, e Endpoint) (Endpoint, error) {
	// nil slices would be written as SQL NULL, which the NOT NULL columns reject.
	if e.Secrets == nil {
		e.Secrets = []Secret{}
	}
	if len(e.EventTypes) == 0 {
		e.EventTypes = []string{"*"}
	}
	return scanEndpoint(s.Pool.QueryRow(ctx,
		`INSERT INTO endpoints (tenant_id, url, description, event_types, secrets)
		 VALUES ($1, $2, $3, $4, $5) RETURNING `+endpointCols,
		e.TenantID, e.URL, e.Description, e.EventTypes, e.Secrets))
}

func (s *Store) GetEndpoint(ctx context.Context, tenantID, id string) (Endpoint, error) {
	return scanEndpoint(s.Pool.QueryRow(ctx,
		`SELECT `+endpointCols+` FROM endpoints WHERE id = $1 AND tenant_id = $2`, id, tenantID))
}

// ListEndpoints returns up to limit endpoints ordered by creation time, starting
// after the endpoint with id `after` (keyset pagination; empty means from the start).
func (s *Store) ListEndpoints(ctx context.Context, tenantID, after string, limit int) ([]Endpoint, error) {
	var afterArg any
	if after != "" {
		afterArg = after
	}
	rows, err := s.Pool.Query(ctx,
		`SELECT `+endpointCols+` FROM endpoints
		  WHERE tenant_id = $1
		    AND ($2::uuid IS NULL OR (created_at, id) >
		         (SELECT created_at, id FROM endpoints WHERE id = $2::uuid AND tenant_id = $1))
		  ORDER BY created_at, id
		  LIMIT $3`, tenantID, afterArg, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Endpoint, error) { return scanEndpoint(r) })
}

func (s *Store) UpdateEndpoint(ctx context.Context, tenantID, id string, p EndpointPatch) (Endpoint, error) {
	return scanEndpoint(s.Pool.QueryRow(ctx,
		`UPDATE endpoints SET
		    url         = COALESCE($3, url),
		    description = COALESCE($4, description),
		    event_types = COALESCE($5, event_types),
		    status      = COALESCE($6, status),
		    updated_at  = now()
		  WHERE id = $1 AND tenant_id = $2
		  RETURNING `+endpointCols,
		id, tenantID, p.URL, p.Description, p.EventTypes, p.Status))
}

func (s *Store) DeleteEndpoint(ctx context.Context, tenantID, id string) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM endpoints WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
