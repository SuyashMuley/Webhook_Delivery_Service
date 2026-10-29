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
	db, err := store.ConnectWithRetry(ctx, cfg.DatabaseURL, 60*time.Second, opts...)
	if err != nil {
		return err
	}
	defer db.Close()
	db.Sealer = ring

	m := metrics.New()
	notifier := db.NewNotifier(10 * time.Millisecond)
	go notifier.Run(ctx)
	drain := &server.Drain{}
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

	// The /metrics server is separate from the public one so the ingress never exposes it.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return server.Run(gctx, log, cfg.HTTPAddr, server.Instrument(log, m, mux), cfg.ShutdownTimeout,
			server.WithDrain(drain, cfg.DrainDelay))
	})
	if cfg.MetricsAddr != "" {
		g.Go(func() error {
			mm := http.NewServeMux()
			mm.Handle("GET /metrics", m.Handler())
			return server.Run(gctx, log, cfg.MetricsAddr, mm, 5*time.Second)
		})
	}
	return g.Wait()
}
