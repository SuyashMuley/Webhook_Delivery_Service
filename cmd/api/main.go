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
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(":8080")
	if err != nil {
		return err
	}
	log := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat, "api")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	return server.Run(ctx, log, cfg.HTTPAddr, server.NewMux(db), cfg.ShutdownTimeout)
}
