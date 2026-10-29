package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"webhookdelivery/internal/secretbox"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

const (
	plainA = "whsec_QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE="
	plainB = "whsec_QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
)

func newEndpoint(t *testing.T, st *store.Store, tenant, secret string) store.Endpoint {
	t.Helper()
	ep, err := st.CreateEndpoint(context.Background(), store.Endpoint{
		TenantID: tenant, URL: "http://127.0.0.1:1/", EventTypes: []string{"*"},
		Secrets: []store.Secret{{ID: "sec_1", Value: secret, CreatedAt: time.Now()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

func TestSecretsAreEncryptedAtRestAndDecryptedOnClaim(t *testing.T) {
	st := testutil.NewStore(t)
	tenant, _ := testutil.NewTenant(t, st, "acme")
	ep := newEndpoint(t, st, tenant, plainA)
	if _, err := st.IngestEvent(context.Background(), tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}

	raw := scalar[string](t, st, `SELECT secrets::text FROM endpoints WHERE id = $1`, ep.ID)
	if strings.Contains(raw, "QUFBQUFB") || strings.Contains(raw, "whsec_") {
		t.Fatalf("plaintext secret is stored in the database: %s", raw)
	}
	if !strings.Contains(raw, `"sealed"`) {
		t.Fatalf("secret is not stored sealed: %s", raw)
	}

	got := claim(t, st, 1)
	if len(got) != 1 || got[0].SecretsErr != nil || len(got[0].Secrets) != 1 || got[0].Secrets[0].Value != plainA {
		t.Fatalf("claim did not return the decrypted secret: %+v", got)
	}
}

// A sealed secret copied onto a different endpoint must not decrypt: the
// ciphertext is bound to the endpoint and secret it was created for.
func TestSealedSecretIsBoundToItsEndpoint(t *testing.T) {
	st := testutil.NewStore(t)
	tenant, _ := testutil.NewTenant(t, st, "acme")
	a := newEndpoint(t, st, tenant, plainA)
	b := newEndpoint(t, st, tenant, plainB)
	exec(t, st, `UPDATE endpoints SET secrets = (SELECT secrets FROM endpoints WHERE id = $1) WHERE id = $2`, a.ID, b.ID)
	if _, err := st.IngestEvent(context.Background(), tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}
	exec(t, st, `UPDATE tenants SET max_inflight = 100`)

	bad := 0
	for _, d := range claim(t, st, 10) {
		if d.EndpointID == b.ID {
			if d.SecretsErr == nil || len(d.Secrets) != 0 {
				t.Fatalf("endpoint B decrypted a secret that was sealed for endpoint A")
			}
			bad++
		} else if d.SecretsErr != nil {
			t.Fatalf("endpoint A could not decrypt its own secret: %v", d.SecretsErr)
		}
	}
	if bad != 1 {
		t.Fatalf("expected endpoint B's delivery to be claimed with a secrets error, got %d", bad)
	}
}

func TestSecretRotationGraceAndLimits(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	tenant, _ := testutil.NewTenant(t, st, "acme")
	ep := newEndpoint(t, st, tenant, plainA)

	// Rotate with a grace period: both secrets are live, the old one expires.
	created, all, err := st.RotateSecret(ctx, tenant, ep.ID, plainB, time.Hour)
	if err != nil || len(all) != 2 {
		t.Fatalf("rotate: %v %v", all, err)
	}
	var old, fresh store.SecretInfo
	for _, s := range all {
		if s.ID == created.ID {
			fresh = s
		} else {
			old = s
		}
	}
	if fresh.ExpiresAt != nil || old.ExpiresAt == nil || time.Until(*old.ExpiresAt) < 59*time.Minute {
		t.Fatalf("grace not applied: old=%+v new=%+v", old, fresh)
	}
	if _, err := st.CreateEndpoint(ctx, store.Endpoint{TenantID: tenant, URL: "http://127.0.0.1:1/"}); err != nil {
		t.Fatal(err)
	}

	// The worker's view: both secrets decrypt and are live.
	if _, err := st.IngestEvent(ctx, tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}
	for _, d := range claim(t, st, 5) {
		if d.EndpointID != ep.ID {
			continue
		}
		vals := map[string]bool{}
		for _, s := range d.Secrets {
			if s.Live(time.Now()) {
				vals[s.Value] = true
			}
		}
		if !vals[plainA] || !vals[plainB] || len(vals) != 2 {
			t.Fatalf("during grace both secrets must sign, got %v", vals)
		}
	}

	// Revoking the old one leaves the new; revoking the last is refused.
	if err := st.RevokeSecret(ctx, tenant, ep.ID, old.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.RevokeSecret(ctx, tenant, ep.ID, fresh.ID); !errors.Is(err, store.ErrLastSecret) {
		t.Fatalf("revoking the last secret: got %v, want ErrLastSecret", err)
	}
	if err := st.RevokeSecret(ctx, tenant, ep.ID, "sec_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoking an unknown secret: got %v", err)
	}

	// grace = 0 retires the previous secret at once.
	_, all, err = st.RotateSecret(ctx, tenant, ep.ID, plainA, 0)
	if err != nil || len(all) != 1 {
		t.Fatalf("hard rotate left %d secrets (err %v), want 1", len(all), err)
	}

	// Rotating repeatedly inside the grace window is bounded.
	var lastErr error
	for range 8 {
		_, _, lastErr = st.RotateSecret(ctx, tenant, ep.ID, plainB, time.Hour)
		if lastErr != nil {
			break
		}
	}
	if !errors.Is(lastErr, store.ErrTooManySecrets) {
		t.Fatalf("unbounded rotation: got %v, want ErrTooManySecrets", lastErr)
	}
}

func TestResealMigratesLegacyAndRotatesKeys(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	tenant, _ := testutil.NewTenant(t, st, "acme")

	// An endpoint written before encryption existed: plaintext "value".
	id := scalar[string](t, st, `
		INSERT INTO endpoints (tenant_id, url, secrets)
		VALUES ($1, 'http://127.0.0.1:1/', jsonb_build_array(jsonb_build_object('id','sec_old','value',$2::text,'created_at',now())))
		RETURNING id::text`, tenant, plainA)
	if _, err := st.IngestEvent(ctx, tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}
	// Legacy secrets keep working until migrated.
	if got := claim(t, st, 1); len(got) != 1 || got[0].SecretsErr != nil || got[0].Secrets[0].Value != plainA {
		t.Fatalf("legacy plaintext secret not usable: %+v", got)
	}

	oldRing := secretbox.DevKeyring()
	n, err := st.ResealSecrets(ctx, oldRing.ActiveKeyID())
	if err != nil || n != 1 {
		t.Fatalf("migrate legacy: resealed %d (err %v), want 1", n, err)
	}
	if raw := scalar[string](t, st, `SELECT secrets::text FROM endpoints WHERE id = $1`, id); strings.Contains(raw, "QUFBQUFB") {
		t.Fatalf("plaintext still stored after migration: %s", raw)
	}
	if n, _ := st.ResealSecrets(ctx, oldRing.ActiveKeyID()); n != 0 {
		t.Fatalf("second run resealed %d, want 0 (idempotent)", n)
	}

	// Key rotation: a new active key, the dev key kept for reading.
	newKey, _ := secretbox.GenerateKey("k2")
	spec := newKey + ",dev:" + devKeyB64()
	ring, err := secretbox.ParseKeyring(spec)
	if err != nil {
		t.Fatal(err)
	}
	st.Sealer = ring
	if n, err := st.ResealSecrets(ctx, "k2"); err != nil || n != 1 {
		t.Fatalf("key rotation: resealed %d (err %v), want 1", n, err)
	}
	raw := scalar[string](t, st, `SELECT secrets->0->>'sealed' FROM endpoints WHERE id = $1`, id)
	if secretbox.KeyID(raw) != "k2" {
		t.Fatalf("secret still sealed under %q after rotation", secretbox.KeyID(raw))
	}
	// And it still decrypts with the new ring.
	exec(t, st, `UPDATE deliveries SET status='pending', locked_by=NULL, lease_expires_at=NULL`)
	if got := claim(t, st, 1); len(got) != 1 || got[0].Secrets[0].Value != plainA {
		t.Fatalf("secret unreadable after key rotation: %+v", got)
	}
}

// devKeyB64 is the development key in base64, derived exactly as
// secretbox.DevKeyring does, so a test can keep it in a rotated ring.
func devKeyB64() string {
	k := sha256.Sum256([]byte("webhookdelivery-insecure-dev-key"))
	return base64.StdEncoding.EncodeToString(k[:])
}
