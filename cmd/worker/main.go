package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"webhookdelivery/internal/config"
	"webhookdelivery/internal/deliver"
	"webhookdelivery/internal/logging"
	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/retry"
	"webhookdelivery/internal/server"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/telemetry"
	"webhookdelivery/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(":8081", store.RoleWorker)
	if err != nil {
		return err
	}
	log := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat, "worker")

	ring, err := cfg.Keyring()
	if err != nil {
		return err
	}
	if cfg.SecretKeys == "" {
		log.Warn("SECRET_ENCRYPTION_KEYS is not set: using the public development key; endpoint secrets are NOT protected")
	}
	if cfg.AllowPrivateTargets {
		log.Warn("private/loopback delivery targets and plain http are allowed (development setting)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	flush, err := telemetry.Setup(ctx, telemetry.Config{Service: "worker", Endpoint: cfg.OTLPEndpoint, SampleRatio: cfg.TraceSampleRatio})
	if err != nil {
		return err
	}
	defer func() {
		fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = flush(fctx)
	}()

	var opts []store.Option
	if cfg.DBRole != "" {
		opts = append(opts, store.WithRole(cfg.DBRole))
	}
	id, err := workerID()
	if err != nil {
		return err
	}

	m := metrics.New()
	drain := &server.Drain{}
	gate := server.NewGate() // alive immediately, ready once the database is reachable

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return server.Run(gctx, log, cfg.HTTPAddr, gate, cfg.ShutdownTimeout, server.WithDrain(drain, cfg.DrainDelay))
	})
	if cfg.MetricsAddr != "" {
		g.Go(func() error {
			mm := http.NewServeMux()
			mm.Handle("GET /metrics", m.Handler())
			return server.Run(gctx, log, cfg.MetricsAddr, mm, 5*time.Second)
		})
	}
	g.Go(func() error {
		db, err := store.ConnectWithRetry(gctx, cfg.DatabaseURL, 60*time.Second,
			func(err error) { log.Warn("waiting for the database", "error", err) }, opts...)
		if err != nil {
			return fmt.Errorf("database: %w", err)
		}
		defer db.Close()
		db.Sealer = ring
		db.Breaker = store.BreakerPolicy{
			Threshold:    cfg.BreakerThreshold,
			BaseOpen:     cfg.BreakerBaseOpen,
			MaxOpen:      cfg.BreakerMaxOpen,
			DisableAfter: cfg.AutoDisableAfter,
		}
		w := worker.New(db, deliver.NewSender(cfg.DeliveryTimeout, cfg.AllowPrivateTargets), log, worker.Config{
			Metrics:         m,
			DrainTimeout:    cfg.WorkerDrainTimeout,
			StatsInterval:   cfg.StatsInterval,
			PurgeInterval:   cfg.PurgeInterval,
			RetainSucceeded: cfg.RetainSucceeded,
			RetainDead:      cfg.RetainDead,
			RetainEvents:    cfg.RetainEvents,
			ID:              id,
			BatchSize:       cfg.WorkerBatchSize,
			TenantSlice:     cfg.TenantSlice,
			Concurrency:     cfg.WorkerConcurrency,
			PollInterval:    cfg.PollInterval,
			Listen:          cfg.WorkerListen,
			Lease:           cfg.LeaseDuration,
			ReaperInterval:  cfg.ReaperInterval,
			Retry:           retry.Policy{Schedule: cfg.RetrySchedule, MaxRetryAfter: cfg.RetryAfterMax},
		})
		gate.Open(server.NewMux(db, drain))
		log.Info("worker started", "worker_id", id)

		// The delivery loops share a fate: if one stops, the others are cancelled. The pool
		// is closed only after all of them have finished.
		loops, lctx := errgroup.WithContext(gctx)
		loops.Go(func() error { return w.Run(lctx) })
		loops.Go(func() error { return w.RunReaper(lctx) })
		loops.Go(func() error { return w.RunStats(lctx) })
		loops.Go(func() error { return w.RunPurge(lctx) })
		return loops.Wait()
	})
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
