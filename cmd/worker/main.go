package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"webhookdelivery/internal/config"
	"webhookdelivery/internal/logging"
	"webhookdelivery/internal/server"
	"webhookdelivery/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(":8081")
	if err != nil {
		return err
	}
	log := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat, "worker")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	// The claim/deliver loop arrives in Phase 1; for now the worker only
	// exposes health endpoints so it can be deployed and probed.
	log.Info("worker started; delivery loop not implemented yet")
	return server.Run(ctx, log, cfg.HTTPAddr, server.NewMux(db), cfg.ShutdownTimeout)
}
