package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"webhookdelivery/internal/config"
	"webhookdelivery/internal/deliver"
	"webhookdelivery/internal/logging"
	"webhookdelivery/internal/retry"
	"webhookdelivery/internal/server"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/worker"
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

	id, err := workerID()
	if err != nil {
		return err
	}
	w := worker.New(db, deliver.NewSender(cfg.DeliveryTimeout), log, worker.Config{
		ID:             id,
		BatchSize:      cfg.WorkerBatchSize,
		TenantSlice:    cfg.TenantSlice,
		Concurrency:    cfg.WorkerConcurrency,
		PollInterval:   cfg.PollInterval,
		Lease:          cfg.LeaseDuration,
		ReaperInterval: cfg.ReaperInterval,
		Retry:          retry.Policy{Schedule: cfg.RetrySchedule, MaxRetryAfter: cfg.RetryAfterMax},
	})

	// The health server and the delivery loop live and die together: if either
	// stops, the other is cancelled.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return server.Run(gctx, log, cfg.HTTPAddr, server.NewMux(db), cfg.ShutdownTimeout)
	})
	g.Go(func() error {
		log.Info("worker started", "worker_id", id)
		return w.Run(gctx)
	})
	g.Go(func() error { return w.RunReaper(gctx) })
	return g.Wait()
}

// workerID is unique per process so lease ownership is unambiguous.
func workerID() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return host + "-" + hex.EncodeToString(b), nil
}
