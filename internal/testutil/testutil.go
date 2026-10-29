// Package testutil provides helpers for integration tests that need Postgres.
//
// Tests are skipped when DATABASE_URL is unset. Each test gets its own freshly
// migrated database, so tests are isolated from each other and from any
// worker that might otherwise claim another test's deliveries.
package testutil

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/secretbox"
	"webhookdelivery/internal/store"
	"webhookdelivery/migrations"
)

func NewStore(t testing.TB) *store.Store {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "test_" + hex.EncodeToString(suffix)

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect to %s: %v", base, err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create test database: %v", err)
	}
	_ = admin.Close(ctx)

	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	testURL := u.String()

	migrate(t, testURL)

	st, err := store.Connect(ctx, testURL)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	st.Sealer = secretbox.DevKeyring()
	t.Cleanup(func() {
		st.Close()
		admin, err := pgx.Connect(ctx, base)
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer func() { _ = admin.Close(ctx) }()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Logf("drop test database %s: %v", name, err)
		}
	})
	return st
}

// WithRole opens a second store on the same test database whose connections
// run as the given restricted role (store.RoleAPI or store.RoleWorker). The
// store returned by NewStore is the table owner, which bypasses row-level
// security; use it for setup and assertions, and this one for the code under
// test so grants and policies are exercised for real.
func WithRole(t testing.TB, owner *store.Store, role string) *store.Store {
	t.Helper()
	st, err := store.Connect(context.Background(), owner.Pool.Config().ConnString(), store.WithRole(role))
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	st.Sealer = owner.Sealer
	st.Breaker = owner.Breaker
	t.Cleanup(st.Close)
	return st
}

func migrate(t testing.TB, dbURL string) {
	t.Helper()
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Up(context.Background()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

// NewTenant creates a tenant with one API key and returns the tenant id and the plaintext key.
func NewTenant(t testing.TB, st *store.Store, name string) (tenantID, apiKey string) {
	t.Helper()
	ctx := context.Background()
	tenant, err := st.CreateTenant(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	key, prefix, err := auth.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateAPIKey(ctx, tenant.ID, prefix, auth.HashKey(key), nil); err != nil {
		t.Fatal(err)
	}
	return tenant.ID, key
}
