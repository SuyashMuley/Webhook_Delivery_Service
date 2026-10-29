package store

import (
	"context"
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
// suspended tenants resolve to ErrNotFound. It calls a SECURITY DEFINER
// function because the API database role may not read api_keys directly.
func (s *Store) AuthenticateKey(ctx context.Context, hash []byte) (tenantID string, err error) {
	var id *string
	if err = s.Pool.QueryRow(ctx, `SELECT authenticate_api_key($1)::text`, hash).Scan(&id); err != nil {
		return "", err
	}
	if id == nil {
		return "", ErrNotFound
	}
	return *id, nil
}

// RevokeAPIKey revokes the key with the given prefix (admin use).
func (s *Store) RevokeAPIKey(ctx context.Context, prefix string) error {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = now() WHERE key_prefix = $1 AND revoked_at IS NULL`, prefix)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
