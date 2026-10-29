package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

// inTenantTx runs fn as the API role in a transaction scoped to tenant, using
// raw SQL, so the assertions below depend on row-level security and not on any
// WHERE clause in the store's own queries.
func inTenantTx(t *testing.T, api *store.Store, tenant string, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := api.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if tenant != "" {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenant); err != nil {
			t.Fatal(err)
		}
	}
	fn(tx)
}

func count(t *testing.T, tx pgx.Tx, table string) (n int) {
	t.Helper()
	if err := tx.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestRowLevelSecurityIsolatesTenants(t *testing.T) {
	owner := testutil.NewStore(t)
	a := seedTenant(t, owner, "a", 2)
	b := seedTenant(t, owner, "b", 3)
	apiStore := testutil.WithRole(t, owner, store.RoleAPI)
	// give tenant A's deliveries some attempt history
	exec(t, owner, `INSERT INTO attempts (delivery_id, attempt_no) SELECT id, 1 FROM deliveries`)

	// No tenant context: fail closed.
	inTenantTx(t, apiStore, "", func(tx pgx.Tx) {
		for _, table := range []string{"endpoints", "events", "deliveries", "attempts"} {
			if n := count(t, tx, table); n != 0 {
				t.Errorf("without a tenant context the API role sees %d rows in %s, want 0", n, table)
			}
		}
	})

	// With tenant A's context, a query with no WHERE clause sees only A.
	inTenantTx(t, apiStore, a, func(tx pgx.Tx) {
		want := map[string]int{"endpoints": 1, "events": 2, "deliveries": 2, "attempts": 2}
		for table, n := range want {
			if got := count(t, tx, table); got != n {
				t.Errorf("tenant A sees %d rows in %s, want %d", got, table, n)
			}
		}
		// Cannot write as another tenant...
		_, err := tx.Exec(context.Background(),
			`INSERT INTO endpoints (tenant_id, url) VALUES ($1, 'https://evil.example/')`, b)
		if err == nil || !strings.Contains(err.Error(), "row-level security") {
			t.Errorf("inserting a row for another tenant: got %v, want an RLS violation", err)
		}
	})
	inTenantTx(t, apiStore, a, func(tx pgx.Tx) {
		// ...nor touch another tenant's rows.
		tag, err := tx.Exec(context.Background(), `UPDATE endpoints SET url = 'https://evil.example/' WHERE tenant_id = $1`, b)
		if err != nil || tag.RowsAffected() != 0 {
			t.Errorf("updating another tenant's endpoint affected %d rows (err %v)", tag.RowsAffected(), err)
		}
		tag, err = tx.Exec(context.Background(), `DELETE FROM endpoints WHERE tenant_id = $1`, b)
		if err != nil || tag.RowsAffected() != 0 {
			t.Errorf("deleting another tenant's endpoint affected %d rows (err %v)", tag.RowsAffected(), err)
		}
	})
	if n := scalar[int](t, owner, `SELECT count(*) FROM endpoints WHERE url LIKE '%evil%'`); n != 0 {
		t.Fatalf("%d endpoints were modified across tenants", n)
	}

	// The API role has no direct access to tenants or API keys.
	for _, table := range []string{"tenants", "api_keys"} {
		inTenantTx(t, apiStore, a, func(tx pgx.Tx) { // a fresh tx each time: the error aborts it
			if _, err := tx.Exec(context.Background(), `SELECT * FROM `+table); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("API role reading %s: got %v, want permission denied", table, err)
			}
		})
	}
}

func TestAPIKeyLookupThroughDefinerFunction(t *testing.T) {
	owner := testutil.NewStore(t)
	tenant, key := testutil.NewTenant(t, owner, "acme")
	apiStore := testutil.WithRole(t, owner, store.RoleAPI)

	got, err := apiStore.AuthenticateKey(context.Background(), auth.HashKey(key))
	if err != nil || got != tenant {
		t.Fatalf("AuthenticateKey = %q, %v; want %q", got, err, tenant)
	}
	if _, err := apiStore.AuthenticateKey(context.Background(), auth.HashKey("whk_bogus")); err != store.ErrNotFound { //nolint:errorlint // sentinel
		t.Fatalf("unknown key: got %v, want ErrNotFound", err)
	}

	prefix := scalar[string](t, owner, `SELECT key_prefix FROM api_keys`)
	if err := owner.RevokeAPIKey(context.Background(), prefix); err != nil {
		t.Fatal(err)
	}
	if _, err := apiStore.AuthenticateKey(context.Background(), auth.HashKey(key)); err != store.ErrNotFound { //nolint:errorlint // sentinel
		t.Fatalf("revoked key still authenticates: %v", err)
	}
}

func TestWorkerRoleSeesAllTenantsButHasLimitedPrivileges(t *testing.T) {
	owner := testutil.NewStore(t)
	seedTenant(t, owner, "a", 2)
	seedTenant(t, owner, "b", 3)
	worker := testutil.WithRole(t, owner, store.RoleWorker)
	ctx := context.Background()

	if n := scalar[int](t, worker, `SELECT count(*) FROM deliveries`); n != 5 {
		t.Fatalf("worker sees %d deliveries, want all 5", n)
	}
	for name, q := range map[string]string{
		"delete endpoints": `DELETE FROM endpoints`,
		"insert events":    `INSERT INTO events (tenant_id, type, payload) SELECT id, 'x', '{}' FROM tenants LIMIT 1`,
		"read api keys":    `SELECT * FROM api_keys`,
		"alter tenants":    `UPDATE tenants SET status = 'suspended'`,
	} {
		if _, err := worker.Pool.Exec(ctx, q); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("worker role %s: got %v, want permission denied", name, err)
		}
	}

	// ...while the operations delivery needs work.
	if got := claim(t, worker, 3); len(got) == 0 {
		t.Fatal("worker role could not claim deliveries")
	}
}
