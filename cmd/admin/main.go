// Command admin bootstraps tenants and API keys (there is no self-service signup).
//
// Usage:
//
//	admin create-tenant <name>        prints the new tenant id
//	admin create-key <tenant-id>      prints a new API key (shown once)
package main

import (
	"context"
	"fmt"
	"os"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "admin:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: admin create-tenant <name> | admin create-key <tenant-id>")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer db.Close()

	switch args[0] {
	case "create-tenant":
		t, err := db.CreateTenant(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Println(t.ID)
	case "create-key":
		key, prefix, err := auth.GenerateKey()
		if err != nil {
			return err
		}
		if _, err := db.CreateAPIKey(ctx, args[1], prefix, auth.HashKey(key), nil); err != nil {
			return err
		}
		fmt.Println(key)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}
