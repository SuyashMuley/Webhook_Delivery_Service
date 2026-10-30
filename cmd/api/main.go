package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"webhookdelivery/internal/api"
	"webhookdelivery/internal/config"
	"webhookdelivery/internal/logging"
	"webhookdelivery/internal/metrics"
	"webhookdelivery/internal/ratelimit"
	"webhookdelivery/internal/server"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/telemetry"
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

	flush, err := telemetry.Setup(ctx, telemetry.Config{Service: "api", Endpoint: cfg.OTLPEndpoint, SampleRatio: cfg.TraceSampleRatio})
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

	m := metrics.New()
	drain := &server.Drain{}
	// The HTTP server starts at once behind a Gate, before the database is reachable:
	// liveness answers immediately (so Kubernetes does not kill a process that is merely
	// waiting) and readiness stays red until the app is open.
	gate := server.NewGate()

	g, gctx := errgroup.WithContext(ctx)
	serverDone := make(chan struct{})
	g.Go(func() error {
		defer close(serverDone)
		return server.Run(gctx, log, cfg.HTTPAddr, gate, cfg.ShutdownTimeout, server.WithDrain(drain, cfg.DrainDelay))
	})
	// The /metrics server is separate from the public one so the ingress never exposes it.
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
		// Keep the pool until the server has finished draining its in-flight requests.
		defer func() { <-serverDone; db.Close() }()
		db.Sealer = ring

		notifier := db.NewNotifier(10 * time.Millisecond)
		go notifier.Run(gctx)
		mux := server.NewMux(db, drain)
		// Plain-http and internal endpoint URLs are convenient locally and forbidden in production.
		api.New(db, log, api.Options{
			AllowInsecureURLs: cfg.AllowPrivateTargets,
			MaxAttempts:       cfg.MaxAttempts,
			RequestTimeout:    cfg.APIRequestTimeout,
			Quota:             ratelimit.New(db, m, log, ratelimit.Config{}),
			Metrics:           m,
			Wake:              notifier.Notify,
		}).Register(mux)
		gate.Open(server.Instrument(log, m, mux))
		log.Info("ready")
		<-gctx.Done()
		return nil
	})
	return g.Wait()
}
