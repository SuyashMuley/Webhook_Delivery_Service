package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"webhookdelivery/internal/secretbox"
)

// maxLiveSecrets bounds how many secrets can sign at once (the current one plus
// those still inside a rotation grace period). Each live secret adds a
// signature to every delivery's header.
const maxLiveSecrets = 5

func infos(secrets []Secret) []SecretInfo {
	out := make([]SecretInfo, len(secrets))
	for i, sec := range secrets {
		out[i] = SecretInfo{ID: sec.ID, CreatedAt: sec.CreatedAt, ExpiresAt: sec.ExpiresAt}
	}
	return out
}

func (s *Store) lockSecrets(ctx context.Context, tx pgx.Tx, tenantID, endpointID string) ([]Secret, error) {
	var secrets []Secret
	err := tx.QueryRow(ctx,
		`SELECT secrets FROM endpoints WHERE id = $1 AND tenant_id = $2 FOR UPDATE`,
		endpointID, tenantID).Scan(&secrets)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return secrets, err
}

func saveSecrets(ctx context.Context, tx pgx.Tx, endpointID string, secrets []Secret) error {
	if secrets == nil {
		secrets = []Secret{}
	}
	_, err := tx.Exec(ctx, `UPDATE endpoints SET secrets = $2, updated_at = now() WHERE id = $1`, endpointID, secrets)
	return err
}

// ListSecrets describes an endpoint's signing secrets (never their values).
func (s *Store) ListSecrets(ctx context.Context, tenantID, endpointID string) ([]SecretInfo, error) {
	var out []SecretInfo
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		secrets, err := s.lockSecrets(ctx, tx, tenantID, endpointID)
		out = infos(secrets)
		return err
	})
	return out, err
}

// RotateSecret adds newSecret (plaintext) as the endpoint's signing secret. The
// previous secrets keep signing for `grace` so receivers can switch over
// without dropping deliveries: during that time every delivery carries a
// signature for each live secret and a receiver can verify with either. With
// grace = 0 every previous secret stops immediately, including ones still in
// an earlier grace period: that is the hard rotation to use after a compromise.
func (s *Store) RotateSecret(ctx context.Context, tenantID, endpointID, newSecret string, grace time.Duration) (SecretInfo, []SecretInfo, error) {
	var created SecretInfo
	var all []SecretInfo
	err := s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		current, err := s.lockSecrets(ctx, tx, tenantID, endpointID)
		if err != nil {
			return err
		}
		now := time.Now().UTC()

		var kept []Secret
		for _, sec := range current {
			if grace <= 0 || !sec.Live(now) {
				continue // hard rotation retires everything; expired secrets are pruned
			}
			if sec.ExpiresAt == nil { // the outgoing current secret starts its grace period
				exp := now.Add(grace)
				sec.ExpiresAt = &exp
			}
			kept = append(kept, sec) // earlier outgoing secrets keep their original expiry
		}
		if len(kept) >= maxLiveSecrets {
			return ErrTooManySecrets
		}

		id := "sec_" + uuid.NewString()
		blob, err := s.seal(newSecret, endpointID, id)
		if err != nil {
			return err
		}
		fresh := Secret{ID: id, Sealed: blob, CreatedAt: now}
		kept = append(kept, fresh)
		if err := saveSecrets(ctx, tx, endpointID, kept); err != nil {
			return err
		}
		created = SecretInfo{ID: id, CreatedAt: now}
		all = infos(kept)
		return nil
	})
	return created, all, err
}

// RevokeSecret removes one secret immediately. The last live secret cannot be
// revoked, since the endpoint would then be unsignable.
func (s *Store) RevokeSecret(ctx context.Context, tenantID, endpointID, secretID string) error {
	return s.inTenant(ctx, tenantID, func(tx pgx.Tx) error {
		current, err := s.lockSecrets(ctx, tx, tenantID, endpointID)
		if err != nil {
			return err
		}
		now := time.Now()
		var kept []Secret
		found, live := false, 0
		for _, sec := range current {
			if sec.ID == secretID {
				found = true
				continue
			}
			if sec.Live(now) {
				live++
			}
			kept = append(kept, sec)
		}
		if !found {
			return ErrNotFound
		}
		if live == 0 {
			return ErrLastSecret
		}
		return saveSecrets(ctx, tx, endpointID, kept)
	})
}

// ResealSecrets makes sure every stored secret is encrypted under the key with
// id activeKeyID: plaintext secrets from before encryption existed are
// encrypted, and secrets sealed under an older key are re-sealed. It returns
// how many secrets were rewritten. It runs across all tenants, so it needs the
// table owner, not the API role.
func (s *Store) ResealSecrets(ctx context.Context, activeKeyID string) (int, error) {
	rewritten := 0
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text, secrets FROM endpoints FOR UPDATE`)
		if err != nil {
			return err
		}
		type row struct {
			id      string
			secrets []Secret
		}
		var all []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.secrets); err != nil {
				return err
			}
			all = append(all, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, r := range all {
			changed := false
			for i, sec := range r.secrets {
				if sec.Sealed != "" && secretbox.KeyID(sec.Sealed) == activeKeyID {
					continue
				}
				plain := sec.Legacy
				if sec.Sealed != "" {
					if s.Sealer == nil {
						return errNoSealer
					}
					if plain, err = s.Sealer.Open(sec.Sealed, secretAAD(r.id, sec.ID)); err != nil {
						return err
					}
				}
				blob, err := s.seal(plain, r.id, sec.ID)
				if err != nil {
					return err
				}
				r.secrets[i].Sealed, r.secrets[i].Legacy = blob, ""
				changed = true
				rewritten++
			}
			if changed {
				if err := saveSecrets(ctx, tx, r.id, r.secrets); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return rewritten, err
}
