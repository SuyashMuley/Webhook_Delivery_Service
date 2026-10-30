package loadtest

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Sample is one reading of the system's resource use during a long run.
type Sample struct {
	At             time.Duration
	RSSMB          map[string]float64 // by process name
	DBMB           float64
	DeliveriesMB   float64
	DeliveriesLive int64
	DeliveriesDead int64 // dead tuples: bloat waiting for vacuum
	AttemptsMB     float64
	EventsMB       float64
	Autovacuums    int64 // on deliveries
	Pending        int64
}

// Soak samples resource use at a fixed interval until ctx ends.
type Soak struct {
	mu      sync.Mutex
	Samples []Sample
}

// Run samples every interval; logf is called with a one-line summary of each sample.
func (s *Soak) Run(ctx context.Context, start time.Time, interval time.Duration, pool *pgxpool.Pool, procs []*Proc, logf func(string, ...any)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		smp := Sample{At: time.Since(start).Round(time.Second), RSSMB: map[string]float64{}}
		for _, p := range procs {
			if pid := p.Pid(); pid != 0 {
				if out, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output(); err == nil {
					kb, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
					smp.RSSMB[p.Name] = kb / 1024
				}
			}
		}
		const mb = 1 << 20
		var dbBytes int64
		_ = pool.QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&dbBytes)
		smp.DBMB = float64(dbBytes) / mb
		rows, err := pool.Query(ctx, `SELECT relname, pg_total_relation_size(relid), n_live_tup, n_dead_tup, autovacuum_count
		                                FROM pg_stat_user_tables WHERE relname IN ('deliveries','events','attempts')`)
		if err == nil {
			for rows.Next() {
				var name string
				var size, live, dead, av int64
				if rows.Scan(&name, &size, &live, &dead, &av) == nil {
					switch name {
					case "deliveries":
						smp.DeliveriesMB, smp.DeliveriesLive, smp.DeliveriesDead, smp.Autovacuums = float64(size)/mb, live, dead, av
					case "attempts":
						smp.AttemptsMB = float64(size) / mb
					case "events":
						smp.EventsMB = float64(size) / mb
					}
				}
			}
			rows.Close()
		}
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE status='pending'`).Scan(&smp.Pending)
		s.mu.Lock()
		s.Samples = append(s.Samples, smp)
		s.mu.Unlock()
		logf("soak t+%-6v db=%.0fMB deliveries=%.0fMB(live %d, dead tuples %d, autovacuums %d) pending=%d  %s",
			smp.At, smp.DBMB, smp.DeliveriesMB, smp.DeliveriesLive, smp.DeliveriesDead, smp.Autovacuums, smp.Pending, rssLine(smp.RSSMB))
	}
}

func rssLine(m map[string]float64) string {
	var parts []string
	for _, n := range []string{"api-0", "api-1", "worker-0", "worker-1", "worker-2"} {
		if v, ok := m[n]; ok {
			parts = append(parts, fmt.Sprintf("%s=%.0fMB", n, v))
		}
	}
	return "rss: " + strings.Join(parts, " ")
}

// Trend compares the steady part of the run: the sample at warmup versus the last one.
func (s *Soak) Trend(warmup time.Duration) (first, last Sample, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, smp := range s.Samples {
		if smp.At >= warmup {
			first, ok = smp, true
			break
		}
	}
	if !ok || len(s.Samples) == 0 {
		return first, last, false
	}
	return first, s.Samples[len(s.Samples)-1], true
}
