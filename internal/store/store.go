// Package store owns the Postgres connection pool and all queries.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"webhookdelivery/internal/secretbox"
)

type Store struct {
	Pool *pgxpool.Pool

	// Sealer encrypts and decrypts endpoint signing secrets. Operations that
	// touch secrets fail if it is not set.
	Sealer secretbox.Sealer

	// Breaker is the circuit breaker / auto-disable policy applied by workers.
	Breaker BreakerPolicy
}

// Option customizes Connect.
type Option func(*pgxpool.Config)

// Roles a connection can run as. They are created by migration 00004 and
// carry the row-level-security policies that isolate tenants.
const (
	RoleAPI    = "webhooks_api"
	RoleWorker = "webhooks_worker"
)

// WithRole makes every connection in the pool run as the given database role
// (SET ROLE), so that the role's privileges and row-level security apply.
// The login user must be allowed to assume the role. In production prefer a
// login user that only has the restricted role, so the session cannot undo it.
func WithRole(role string) Option {
	return func(c *pgxpool.Config) {
		c.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
			if err != nil {
				return fmt.Errorf("set role %s: %w", role, err)
			}
			return nil
		}
	}
}

func Connect(ctx context.Context, databaseURL string, opts ...Option) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	for _, o := range opts {
		o(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{Pool: pool, Breaker: DefaultBreaker()}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) Close() { s.Pool.Close() }

// inTenant runs fn in a transaction whose app.tenant_id setting is tenantID,
// which is what the row-level-security policies of the API role key on. Every
// tenant-facing query goes through it, so a query that forgets its own
// "WHERE tenant_id = ..." still cannot see another tenant's rows.
func (s *Store) inTenant(ctx context.Context, tenantID string, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
			return err
		}
		return fn(tx)
	})
}
