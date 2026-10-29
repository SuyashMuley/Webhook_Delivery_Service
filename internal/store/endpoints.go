package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DefaultEndpointConcurrency is how many deliveries may be in flight to one
// endpoint at once unless the tenant changes it (matches the column default).
const DefaultEndpointConcurrency = 5

const endpointCols = `id::text, tenant_id::text, url, description, event_types, secrets,
	status, max_concurrency, created_at, updated_at,
	consecutive_failures, circuit_open_until, failing_since, disabled_at, disabled_reason`

func scanEndpoint(row pgx.Row) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.TenantID, &e.URL, &e.Description, &e.EventTypes, &e.Secrets,
		&e.Status, &e.MaxConcurrency, &e.CreatedAt, &e.UpdatedAt,
		&e.ConsecutiveFailures, &e.CircuitOpenUntil, &e.FailingSince, &e.DisabledAt, &e.DisabledReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, ErrNotFound
	}
	return e, err
}

// secretAAD ties a sealed secret to the endpoint and secret it belongs to.
func secretAAD(endpointID, secretID string) string { return endpointID + "/" + secretID }

func (s *Store) seal(plaintext, endpointID, secretID string) (string, error) {
	if s.Sealer == nil {
		return "", errNoSealer
	}
	return s.Sealer.Seal(plaintext, secretAAD(endpointID, secretID))
}

// openSecrets decrypts an endpoint's stored secrets. Secrets written before
// encryption existed (Legacy) are passed through so an upgrade does not break
// delivery; `admin encrypt-secrets` migrates them.
func (s *Store) openSecrets(endpointID string, stored []Secret) ([]Secret, error) {
	out := make([]Secret, len(stored))
	for i, sec := range stored {
		sec.Value = sec.Legacy
		if sec.Sealed != "" {
			if s.Sealer == nil {
				return nil, errNoSealer
			}
			v, err := s.Sealer.Open(sec.Sealed, secretAAD(endpointID, sec.ID))
			if err != nil {
				return nil, fmt.Errorf("secret %s: %w", sec.ID, err)
			}
			sec.Value = v
		}
		out[i] = sec
	}
	return out, nil
}

// CreateEndpoint stores a new endpoint. Each of e.Secrets must carry its
// plaintext in Value; it is sealed before being written and never stored.
func (s *Store) CreateEndpoint(ctx context.Context, e Endpoint) (Endpoint, error) {
	e.ID = uuid.NewString()
	sealed := make([]Secret, len(e.Secrets)) // non-nil so it is written as [] rather than NULL
	for i, sec := range e.Secrets {
		blob, err := s.seal(sec.Value, e.ID, sec.ID)
		if err != nil {
			return Endpoint{}, err
		}
		sealed[i] = Secret{ID: sec.ID, Sealed: blob, CreatedAt: sec.CreatedAt, ExpiresAt: sec.ExpiresAt}
	}
	if len(e.EventTypes) == 0 {
		e.EventTypes = []string{"*"}
	}
	if e.MaxConcurrency < 1 {
		e.MaxConcurrency = DefaultEndpointConcurrency
	}

	var out Endpoint
	err := s.inTenant(ctx, e.TenantID, func(tx pgx.Tx) (err error) {
		out, err = scanEndpoint(tx.QueryRow(ctx,
			`INSERT INTO endpoints (id, tenant_id, url, description, event_types, secrets, max_concurrency)
			 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING `+endpointCols,
			e.ID, e.TenantID, e.URL, e.Description, e.EventTypes, sealed, e.MaxConcurrency))
		return err
	})
	return out, err
}

func (s *Store) GetEndpoint(ctx context.Context, tenantID, id string) (Endpoint, error) {
	var out Endpoint
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) (err error) {
		out, err = scanEndpoint(tx.QueryRow(ctx,
			`SELECT `+endpointCols+` FROM endpoints WHERE id = $1 AND tenant_id = $2`, id, tenantID))
		return err
	})
	return out, err
}

// ListEndpoints returns up to limit endpoints ordered by creation time, starting
// after the endpoint with id `after` (keyset pagination; empty means from the start).
func (s *Store) ListEndpoints(ctx context.Context, tenantID, after string, limit int) ([]Endpoint, error) {
	var afterArg any
	if after != "" {
		afterArg = after
	}
	var out []Endpoint
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT `+endpointCols+` FROM endpoints
			  WHERE tenant_id = $1
			    AND ($2::uuid IS NULL OR (created_at, id) >
			         (SELECT created_at, id FROM endpoints WHERE id = $2::uuid AND tenant_id = $1))
			  ORDER BY created_at, id
			  LIMIT $3`, tenantID, afterArg, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Endpoint, error) { return scanEndpoint(r) })
		return err
	})
	return out, err
}

// UpdateEndpoint applies p. Setting the status to "active" also clears the
// circuit breaker and any auto-disable, so it is how a tenant re-enables an
// endpoint (or manually resets a tripped circuit) after fixing it.
func (s *Store) UpdateEndpoint(ctx context.Context, tenantID, id string, p EndpointPatch) (Endpoint, error) {
	var out Endpoint
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) (err error) {
		out, err = scanEndpoint(tx.QueryRow(ctx,
			`UPDATE endpoints SET
			    url         = COALESCE($3, url),
			    description = COALESCE($4, description),
			    event_types = COALESCE($5, event_types),
			    status      = COALESCE($6, status),
			    max_concurrency = COALESCE($7, max_concurrency),
			    consecutive_failures = CASE WHEN $6::text = 'active' THEN 0    ELSE consecutive_failures END,
			    circuit_trips        = CASE WHEN $6::text = 'active' THEN 0    ELSE circuit_trips END,
			    circuit_open_until   = CASE WHEN $6::text = 'active' THEN NULL ELSE circuit_open_until END,
			    failing_since        = CASE WHEN $6::text = 'active' THEN NULL ELSE failing_since END,
			    disabled_at          = CASE WHEN $6::text = 'active' THEN NULL ELSE disabled_at END,
			    disabled_reason      = CASE WHEN $6::text = 'active' THEN NULL ELSE disabled_reason END,
			    updated_at  = now()
			  WHERE id = $1 AND tenant_id = $2
			  RETURNING `+endpointCols,
			id, tenantID, p.URL, p.Description, p.EventTypes, p.Status, p.MaxConcurrency))
		return err
	})
	return out, err
}

func (s *Store) DeleteEndpoint(ctx context.Context, tenantID, id string) error {
	return s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM endpoints WHERE id = $1 AND tenant_id = $2`, id, tenantID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}
