package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateTenant(ctx context.Context, name string) (Tenant, error) {
	t := Tenant{Name: name}
	err := s.Pool.QueryRow(ctx,
		`INSERT INTO tenants (name) VALUES ($1) RETURNING id::text`, name).Scan(&t.ID)
	return t, err
}

func (s *Store) CreateAPIKey(ctx context.Context, tenantID, prefix string, hash []byte, scopes []string) (string, error) {
	if scopes == nil {
		scopes = []string{}
	}
	var id string
	err := s.Pool.QueryRow(ctx,
		`INSERT INTO api_keys (tenant_id, key_prefix, key_hash, scopes)
		 VALUES ($1, $2, $3, $4) RETURNING id::text`,
		tenantID, prefix, hash, scopes).Scan(&id)
	return id, err
}

// AuthenticateKey resolves an API key hash to its tenant. Revoked keys and
// suspended tenants resolve to ErrNotFound.
func (s *Store) AuthenticateKey(ctx context.Context, hash []byte) (tenantID string, err error) {
	err = s.Pool.QueryRow(ctx,
		`SELECT k.tenant_id::text
		   FROM api_keys k JOIN tenants t ON t.id = k.tenant_id
		  WHERE k.key_hash = $1 AND k.revoked_at IS NULL AND t.status = 'active'`,
		hash).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return tenantID, err
}
