package worker

import (
	"context"
	"time"
)

// RunStats samples the queue every StatsInterval and publishes it as gauges.
// Every worker runs it, so dashboards and alerts aggregate with max(), not sum().
func (w *Worker) RunStats(ctx context.Context) error {
	w.sampleStats(ctx)
	t := time.NewTicker(w.cfg.StatsInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.sampleStats(ctx)
		}
	}
}

func (w *Worker) sampleStats(ctx context.Context) {
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	st, err := w.store.QueueStats(sctx)
	if err != nil {
		if ctx.Err() == nil {
			w.m.StatsErrors.Inc()
			w.log.Warn("sample queue stats", "error", err)
		}
		return
	}
	w.m.QueueDepth.WithLabelValues("due").Set(float64(st.Due))
	w.m.QueueDepth.WithLabelValues("scheduled").Set(float64(st.Scheduled))
	w.m.QueueDepth.WithLabelValues("in_flight").Set(float64(st.InFlight))
	w.m.OldestPendingAge.Set(st.OldestDueAge.Seconds())
	w.m.DLQDepth.Reset() // drop reasons that no longer have entries
	for reason, n := range st.DeadByReason {
		w.m.DLQDepth.WithLabelValues(reason).Set(float64(n))
	}
	w.m.Endpoints.WithLabelValues("circuit_open").Set(float64(st.CircuitOpenEndpoints))
	w.m.Endpoints.WithLabelValues("paused").Set(float64(st.PausedEndpoints))
	w.m.Endpoints.WithLabelValues("disabled").Set(float64(st.DisabledEndpoints))
	w.m.StatsLastSuccess.SetToCurrentTime()
}

// purgeBudget bounds how long one retention pass may run.
const purgeBudget = 30 * time.Second

// RunPurge removes old finished work every PurgeInterval, in bounded batches so
// it never holds long locks or floods the database. It does nothing if
// PurgeInterval is zero. Every worker can run it; batches use SKIP LOCKED.
func (w *Worker) RunPurge(ctx context.Context) error {
	if w.cfg.PurgeInterval <= 0 {
		return nil
	}
	t := time.NewTicker(w.cfg.PurgeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.purgeOnce(ctx)
		}
	}
}

func (w *Worker) purgeOnce(ctx context.Context) {
	deadline := time.Now().Add(purgeBudget)
	var total struct{ succeeded, dead, events int }
	for ctx.Err() == nil && time.Now().Before(deadline) {
		r, err := w.store.Purge(ctx, w.cfg.RetainSucceeded, w.cfg.RetainDead, w.cfg.RetainEvents, w.cfg.PurgeBatch)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("retention purge", "error", err)
			}
			break
		}
		total.succeeded += r.Succeeded
		total.dead += r.Dead
		total.events += r.Events
		w.m.Purged.WithLabelValues("deliveries_succeeded").Add(float64(r.Succeeded))
		w.m.Purged.WithLabelValues("deliveries_dead").Add(float64(r.Dead))
		w.m.Purged.WithLabelValues("events").Add(float64(r.Events))
		if r.Succeeded < w.cfg.PurgeBatch && r.Dead < w.cfg.PurgeBatch && r.Events < w.cfg.PurgeBatch {
			break // caught up
		}
	}
	if total.succeeded+total.dead+total.events > 0 {
		w.log.Info("retention purge", "deliveries_succeeded", total.succeeded, "deliveries_dead", total.dead, "events", total.events)
	}
}
