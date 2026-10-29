// Command loadtest drives the system with realistic load, optionally while
// injecting faults, then proves that nothing was lost.
//
// It creates a throwaway database, starts real api and worker processes (so
// `kill -9` is a genuine process crash), runs an embedded receiver that can
// misbehave, sends events at a fixed rate, and finally checks the database
// invariants (see internal/invariants).
//
//	make bin
//	go run ./cmd/loadtest -scenario none -rate 400 -fanout 5 -duration 60s
//	go run ./cmd/loadtest -scenario mixed -duration 120s
//
// It needs Postgres (DATABASE_URL, as the table owner) and the built binaries
// (-bin-dir, default ./bin). Scenarios db-restart and mixed also need Docker.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"webhookdelivery/internal/chaosproxy"
	"webhookdelivery/internal/invariants"
	"webhookdelivery/internal/loadtest"
	"webhookdelivery/internal/store"
	"webhookdelivery/migrations"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		dbURL       = flag.String("db", os.Getenv("DATABASE_URL"), "Postgres URL of an existing server (table owner)")
		binDir      = flag.String("bin-dir", "bin", "directory with the built api and worker binaries")
		outDir      = flag.String("out", "", "directory for logs and the JSON report (default loadtest-out/<timestamp>)")
		scenario    = flag.String("scenario", "none", "fault scenario: "+strings.Join(loadtest.Scenarios, ", "))
		duration    = flag.Duration("duration", 60*time.Second, "how long to generate load")
		rate        = flag.Float64("rate", 400, "events per second (deliveries per second = rate x fanout)")
		fanout      = flag.Int("fanout", 5, "endpoints per tenant, i.e. deliveries per event")
		tenants     = flag.Int("tenants", 20, "number of tenants")
		apis        = flag.Int("apis", 2, "api processes")
		workers     = flag.Int("workers", 3, "worker processes")
		concurrency = flag.Int("worker-concurrency", 200, "WORKER_CONCURRENCY per worker process")
		senders     = flag.Int("senders", 512, "concurrent producer requests allowed")
		drainWait   = flag.Duration("drain-timeout", 3*time.Minute, "how long to wait for the queue to drain after the load stops")
		pgContainer = flag.String("pg-container", "", "Postgres container for docker-based faults (default: docker compose's postgres)")
		keep        = flag.Bool("keep", false, "keep the test database afterwards")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logf := func(f string, a ...any) {
		fmt.Fprintf(os.Stderr, "%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(f, a...))
	}

	if *dbURL == "" {
		logf("DATABASE_URL (or -db) is required")
		return 2
	}
	plan, err := loadtest.Plan(*scenario, *duration)
	if err != nil {
		logf("%v", err)
		return 2
	}
	out := *outDir
	if out == "" {
		out = filepath.Join("loadtest-out", time.Now().Format("20060102-150405")+"-"+*scenario)
	}
	if err := os.MkdirAll(out, 0o750); err != nil {
		logf("%v", err)
		return 2
	}
	for _, b := range []string{"api", "worker"} {
		if _, err := os.Stat(filepath.Join(*binDir, b)); err != nil {
			logf("missing %s: run `make bin` first", filepath.Join(*binDir, b))
			return 2
		}
	}

	// ---- a private database, so runs never interfere with development data
	runID := randHex(4)
	dbName := "lt_" + runID
	ownerURL, err := createDatabase(ctx, *dbURL, dbName)
	if err != nil {
		logf("create database: %v", err)
		return 1
	}
	if !*keep {
		defer dropDatabase(*dbURL, dbName)
	}
	owner, err := store.Connect(ctx, ownerURL)
	if err != nil {
		logf("connect: %v", err)
		return 1
	}
	defer owner.Close()

	// ---- optional partition proxy between the services and Postgres
	sutURL := ownerURL
	var proxy *chaosproxy.Proxy
	if loadtest.NeedsProxy(*scenario) {
		u, _ := url.Parse(ownerURL)
		if proxy, err = chaosproxy.New("127.0.0.1:0", u.Host); err != nil {
			logf("proxy: %v", err)
			return 1
		}
		defer proxy.Close()
		u.Host = proxy.Addr()
		sutURL = u.String()
	}

	recv, err := loadtest.NewReceiver()
	if err != nil {
		logf("receiver: %v", err)
		return 1
	}
	defer recv.Close()

	pg := *pgContainer
	if loadtest.NeedsDocker(*scenario) && pg == "" {
		b, err := exec.CommandContext(ctx, "docker", "compose", "ps", "-q", "postgres").Output()
		pg = strings.TrimSpace(string(b))
		if err != nil || pg == "" {
			logf("scenario %s needs the Postgres container: start it with `docker compose up -d postgres` or pass -pg-container", *scenario)
			return 2
		}
	}

	// ---- start the system under test
	fleet := buildFleet(*binDir, out, sutURL, *apis, *workers, *concurrency)
	for _, p := range fleet.All() {
		if err := p.Start(); err != nil {
			logf("start %s: %v", p.Name, err)
			return 1
		}
	}
	defer fleet.StopAll(10 * time.Second)
	for _, p := range fleet.All() {
		if err := p.WaitReady(ctx, 30*time.Second); err != nil {
			logf("%v", err)
			return 1
		}
	}
	logf("started %d api + %d worker processes against %s (logs in %s)", *apis, *workers, dbName, out)

	tn, err := loadtest.SetupTenants(ctx, owner, fleet.APIURLs[0], recv, *tenants, *fanout)
	if err != nil {
		logf("setup: %v", err)
		return 1
	}
	logf("created %d tenants x %d endpoints; offering %.0f events/s = %.0f deliveries/s for %v; scenario=%s",
		*tenants, *fanout, *rate, *rate*float64(*fanout), *duration, *scenario)

	// ---- run
	env := &loadtest.Env{Fleet: fleet, Proxy: proxy, Recv: recv, PGContainer: pg, Logf: logf}
	start := time.Now()
	faultsDone := make(chan struct{})
	go func() { defer close(faultsDone); loadtest.RunTimeline(ctx, env, plan, start) }()

	genFinished := make(chan struct{})
	var generated *loadtest.GenResult
	go func() {
		defer close(genFinished)
		generated = loadtest.Generate(ctx, loadtest.GenConfig{
			APIs: fleet.APIURLs, Tenants: tn, Rate: *rate, Duration: *duration, Senders: *senders,
			RetryFor: 45 * time.Second, RunID: runID,
		})
	}()
	progress(ctx, logf, owner, recv, start, genFinished)
	<-genFinished
	gen := generated
	loadEnd := time.Since(start)

	<-faultsDone // let any in-flight fault finish and heal
	recv.SetFault(loadtest.Fault{})
	if proxy != nil {
		proxy.SetMode(chaosproxy.Pass)
	}

	// ---- drain
	logf("load finished after %v; waiting for the queue to drain...", loadEnd.Round(time.Second))
	drainStart := time.Now()
	drained := waitDrain(ctx, owner, *drainWait)
	drainSecs := time.Since(drainStart).Seconds()
	logf("drain %s after %.0fs", map[bool]string{true: "complete", false: "TIMED OUT"}[drained], drainSecs)

	server := loadtest.ScrapeWorkers(ctx, fleet.WorkerMetrics)
	killed := fleet.StopAll(30 * time.Second)

	// ---- verdict
	var ids []string
	for _, e := range gen.Events {
		ids = append(ids, e.ID)
	}
	inv, err := invariants.Check(ctx, owner.Pool, invariants.Expected{EventIDs: ids, Fanout: *fanout, Received: recv})
	if err != nil {
		logf("invariant check failed to run: %v", err)
		return 1
	}
	lat := loadtest.MeasureLatencies(gen, tn, recv)
	mean, peak := lat.Throughput(start, *duration)

	rep := &loadtest.Report{Scenario: *scenario, Started: start, Duration: *duration, Timeline: env.Timeline(),
		Config: map[string]any{"rate": *rate, "fanout": *fanout, "tenants": *tenants, "apis": *apis, "workers": *workers,
			"worker-concurrency": *concurrency, "duration": duration.String(), "cores": numCPU()},
		Server: server, Invariants: inv, Drained: drained, DrainSeconds: drainSecs, KilledOnStop: killed,
		ReceiverReqs: recv.Requests.Load(), ReceiverFaulted: recv.Faulted.Load(), BadSignatures: recv.BadSignature.Load()}
	rep.Gen.Scheduled, rep.Gen.Late, rep.Gen.Accepted = gen.Scheduled, gen.Late, gen.Accepted
	rep.Gen.GaveUp, rep.Gen.Retries, rep.Gen.Throttled = gen.GaveUp, gen.Retries, gen.Throttled
	rep.Gen.ServerErr, rep.Gen.NetErr, rep.Gen.Ack = gen.ServerErr, gen.NetErr, gen.Ack.Summary()
	rep.Deliveries.Delivered, rep.Deliveries.Undelivered = lat.Delivered, lat.Undelivered
	rep.Deliveries.MeanPerSec, rep.Deliveries.PeakPerSec = mean, peak
	rep.Deliveries.FromRequest, rep.Deliveries.FromAck = lat.FromRequest.Summary(), lat.FromAck.Summary()
	if gen.GaveUp > 0 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("%d events were never acknowledged (the producer gave up after 45s of retries); they are excluded from the invariants, as a real producer would alert on them", gen.GaveUp))
	}
	if gen.Late > 0 && *scenario == "none" {
		rep.Notes = append(rep.Notes, "the generator could not keep to its schedule, so offered load was lower than requested and latency numbers are not meaningful: raise -senders or lower -rate")
	} else if gen.Late > 0 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("%d scheduled events were never sent because every producer connection was stuck while the system was unreachable (load shed at the producer, as a real client would queue or drop); they are not part of the invariants", gen.Late))
	}
	if !drained {
		rep.Notes = append(rep.Notes, "the queue did not drain within the timeout")
	}
	rep.Verdict = "PASS"
	if !rep.Pass() || !drained {
		rep.Verdict = "FAIL"
	}
	rep.Print(os.Stdout)
	if err := rep.WriteJSON(filepath.Join(out, "report.json")); err != nil {
		logf("write report: %v", err)
	}
	if rep.Verdict != "PASS" {
		return 1
	}
	return 0
}

func buildFleet(binDir, out, dbURL string, apis, workers, concurrency int) *loadtest.Fleet {
	common := []string{
		"DATABASE_URL=" + dbURL, "APP_ENV=dev", "LOG_LEVEL=warn", "LOG_FORMAT=json",
		"MAX_ATTEMPTS=8", "RETRY_SCHEDULE=1s,2s,3s,5s,5s,5s,5s",
		"DELIVERY_TIMEOUT=3s", "LEASE_DURATION=10s", "REAPER_INTERVAL=2s",
		fmt.Sprintf("WORKER_CONCURRENCY=%d", concurrency), "WORKER_BATCH_SIZE=50", "WORKER_TENANT_SLICE=10",
		"BREAKER_BASE_OPEN=2s", "BREAKER_MAX_OPEN=5s",
		"PURGE_INTERVAL=0s", "STATS_INTERVAL=5s", "SHUTDOWN_DRAIN_DELAY=200ms", "WORKER_DRAIN_TIMEOUT=5s",
	}
	f := &loadtest.Fleet{}
	for i := range apis {
		p := &loadtest.Proc{
			Name: fmt.Sprintf("api-%d", i), Bin: filepath.Join(binDir, "api"), LogPath: filepath.Join(out, fmt.Sprintf("api-%d.log", i)),
			Env:   append(append([]string{}, common...), fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", 18080+i), fmt.Sprintf("METRICS_ADDR=127.0.0.1:%d", 19090+i)),
			Ready: fmt.Sprintf("http://127.0.0.1:%d/readyz", 18080+i),
		}
		f.APIs = append(f.APIs, p)
		f.APIURLs = append(f.APIURLs, fmt.Sprintf("http://127.0.0.1:%d", 18080+i))
	}
	for i := range workers {
		p := &loadtest.Proc{
			Name: fmt.Sprintf("worker-%d", i), Bin: filepath.Join(binDir, "worker"), LogPath: filepath.Join(out, fmt.Sprintf("worker-%d.log", i)),
			Env:   append(append([]string{}, common...), fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", 18180+i), fmt.Sprintf("METRICS_ADDR=127.0.0.1:%d", 19190+i)),
			Ready: fmt.Sprintf("http://127.0.0.1:%d/readyz", 18180+i),
		}
		f.Workers = append(f.Workers, p)
		f.WorkerMetrics = append(f.WorkerMetrics, fmt.Sprintf("http://127.0.0.1:%d/metrics", 19190+i))
	}
	return f
}

// progress logs the state of the system every 5 seconds until the generator is done.
func progress(ctx context.Context, logf func(string, ...any), owner *store.Store, recv *loadtest.Receiver, start time.Time, finished <-chan struct{}) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	var lastAcked int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case <-t.C:
			var pending, inflight int
			_ = owner.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='pending'), count(*) FILTER (WHERE status='in_flight') FROM deliveries`).Scan(&pending, &inflight)
			acked := recv.Acked.Load()
			logf("t+%-4v acked/s=%-5.0f pending=%-6d in_flight=%-4d receiver_reqs=%d", time.Since(start).Round(time.Second),
				float64(acked-lastAcked)/5, pending, inflight, recv.Requests.Load())
			lastAcked = acked
		}
	}
}

func waitDrain(ctx context.Context, owner *store.Store, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		var n int
		if err := owner.Pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE status IN ('pending','in_flight')`).Scan(&n); err == nil && n == 0 {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func createDatabase(ctx context.Context, base, name string) (string, error) {
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		return "", err
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", err
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		return "", err
	}
	if _, err := p.Up(ctx); err != nil {
		return "", err
	}
	return u.String(), nil
}

func dropDatabase(base, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		return
	}
	defer func() { _ = admin.Close(ctx) }()
	_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func numCPU() int { return runtime.NumCPU() }
