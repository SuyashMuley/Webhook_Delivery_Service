// Package server provides the HTTP server shell shared by the api and worker binaries.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Pinger reports whether a dependency is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Drain flips a service into "shutting down": readiness starts failing so load
// balancers and Kubernetes stop sending new traffic before the listener closes.
type Drain struct{ v atomic.Bool }

func (d *Drain) Start()       { d.v.Store(true) }
func (d *Drain) Active() bool { return d != nil && d.v.Load() }

// NewMux returns a mux with the standard health endpoints:
//   - /healthz: liveness, always 200 while the process runs
//   - /readyz:  readiness, 200 only if the database answers and we are not draining
//
// drain may be nil.
func NewMux(db Pinger, drain *Drain) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if drain.Active() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}

// RunOption customizes Run.
type RunOption func(*runConfig)

type runConfig struct {
	drain      *Drain
	drainDelay time.Duration
}

// WithDrain makes Run mark d as draining as soon as shutdown starts, then wait
// delay before closing the listener. The delay gives the load balancer time to
// notice the failing readiness check and stop routing here, so in-flight and
// newly arriving requests are not cut off.
func WithDrain(d *Drain, delay time.Duration) RunOption {
	return func(c *runConfig) { c.drain, c.drainDelay = d, delay }
}

// Run serves h on addr until ctx is cancelled, then shuts down gracefully
// within shutdownTimeout.
func Run(ctx context.Context, log *slog.Logger, addr string, h http.Handler, shutdownTimeout time.Duration, opts ...RunOption) error {
	var rc runConfig
	for _, o := range opts {
		o(&rc)
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    32 << 10,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	if rc.drain != nil {
		rc.drain.Start()
		log.Info("draining: readiness now failing", "delay", rc.drainDelay)
		time.Sleep(rc.drainDelay)
	}
	log.Info("shutting down http server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
