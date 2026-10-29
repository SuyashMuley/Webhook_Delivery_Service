// Command admin bootstraps and maintains the system (there is no self-service signup).
//
// Usage:
//
//	admin create-tenant <name>        prints the new tenant id
//	admin create-key <tenant-id>      prints a new API key (shown once)
//	admin revoke-key <key-prefix>     revokes the key with that prefix (e.g. whk_1a2b3c4d)
//	admin gen-secret-key <key-id>     prints a new key-encryption key for SECRET_ENCRYPTION_KEYS
//	admin set-limits <tenant-id> <events-per-sec> <max-pending> <max-inflight>
//	                                  tenant quotas: ingest rate, undelivered backlog, concurrent deliveries
//	admin encrypt-secrets             encrypts legacy plaintext secrets and re-seals secrets
//	                                  under the active key (run after a key rotation)
//
// Commands that touch the database need DATABASE_URL and run as the login user
// (the table owner), not as one of the restricted roles.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/config"
	"webhookdelivery/internal/secretbox"
	"webhookdelivery/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "admin:", err)
		os.Exit(1)
	}
}

var errUsage = errors.New("usage: admin create-tenant <name> | create-key <tenant-id> | revoke-key <key-prefix> | gen-secret-key <key-id> | set-limits <tenant-id> <events/s> <max-pending> <max-inflight> | encrypt-secrets")

func run(args []string) error {
	if len(args) == 0 {
		return errUsage
	}
	ctx := context.Background()

	if args[0] == "gen-secret-key" {
		if len(args) != 2 {
			return errUsage
		}
		key, err := secretbox.GenerateKey(args[1])
		if err != nil {
			return err
		}
		fmt.Println(key)
		return nil
	}

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is required")
	}
	db, err := store.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer db.Close()

	switch args[0] {
	case "create-tenant":
		if len(args) != 2 {
			return errUsage
		}
		t, err := db.CreateTenant(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Println(t.ID)
	case "create-key":
		if len(args) != 2 {
			return errUsage
		}
		key, prefix, err := auth.GenerateKey()
		if err != nil {
			return err
		}
		if _, err := db.CreateAPIKey(ctx, args[1], prefix, auth.HashKey(key), nil); err != nil {
			return err
		}
		fmt.Println(key)
	case "revoke-key":
		if len(args) != 2 {
			return errUsage
		}
		return db.RevokeAPIKey(ctx, args[1])
	case "set-limits":
		if len(args) != 5 {
			return errUsage
		}
		var n [3]int
		for i := range n {
			v, err := strconv.Atoi(args[2+i])
			if err != nil || v < 1 {
				return fmt.Errorf("limits must be positive integers, got %q", args[2+i])
			}
			n[i] = v
		}
		return db.SetTenantLimits(ctx, args[1], n[0], n[1], n[2])
	case "encrypt-secrets":
		// Reuse the services' own key configuration. DATABASE_URL is already
		// set; the other required settings are not needed here.
		cfg, err := config.Load("", "")
		if err != nil {
			return err
		}
		ring, err := cfg.Keyring()
		if err != nil {
			return err
		}
		db.Sealer = ring
		n, err := db.ResealSecrets(ctx, ring.ActiveKeyID())
		if err != nil {
			return err
		}
		fmt.Printf("re-encrypted %d secret(s) under key %q\n", n, ring.ActiveKeyID())
	default:
		return errUsage
	}
	return nil
}
