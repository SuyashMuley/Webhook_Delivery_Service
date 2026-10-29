package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"webhookdelivery/internal/api"
	"webhookdelivery/internal/config"
	"webhookdelivery/internal/logging"
	"webhookdelivery/internal/server"
	"webhookdelivery/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(":8080", store.RoleAPI)
	if err != nil {
		return err
	}
	log := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat, "api")

	ring, err := cfg.Keyring()
	if err != nil {
		return err
	}
	if cfg.SecretKeys == "" {
		log.Warn("SECRET_ENCRYPTION_KEYS is not set: using the public development key; endpoint secrets are NOT protected")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var opts []store.Option
	if cfg.DBRole != "" {
		opts = append(opts, store.WithRole(cfg.DBRole))
	}
	db, err := store.Connect(ctx, cfg.DatabaseURL, opts...)
	if err != nil {
		return err
	}
	defer db.Close()
	db.Sealer = ring

	mux := server.NewMux(db)
	// Plain-http and internal endpoint URLs are convenient locally and forbidden in production.
	api.New(db, log, api.Options{AllowInsecureURLs: cfg.AllowPrivateTargets, MaxAttempts: cfg.MaxAttempts}).Register(mux)

	return server.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
