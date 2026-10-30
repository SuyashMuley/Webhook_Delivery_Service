package store_test

import (
	"context"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"webhookdelivery/internal/chaosproxy"
	"webhookdelivery/internal/store"
	"webhookdelivery/internal/testutil"
)

func listen(t *testing.T, st *store.Store) (*atomic.Int64, context.CancelFunc) {
	t.Helper()
	var n atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		st.Listen(ctx, store.NotifyChannel, func() { n.Add(1) }, nil)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return &n, cancel
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestNotifierWakesListenersAndCoalescesBursts(t *testing.T) {
	st := testutil.NewStore(t)
	n, _ := listen(t, st)
	waitFor(t, "the listener to attach", func() bool { return n.Load() >= 1 }) // the initial catch-up call
	before := n.Load()

	notifier := st.NewNotifier(50 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); notifier.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// One commit: the listener hears about it promptly (window + slack).
	start := time.Now()
	notifier.Notify()
	waitFor(t, "a notification", func() bool { return n.Load() > before })
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("wake-up took %v", took)
	}

	// A burst of 1,000 commits inside a few windows is announced a handful of times, not 1,000.
	mark := n.Load()
	for range 1000 {
		notifier.Notify()
	}
	time.Sleep(300 * time.Millisecond)
	if got := n.Load() - mark; got < 1 || got > 8 {
		t.Fatalf("1,000 Notify calls produced %d notifications; want a few (coalesced), at least 1", got)
	}
}

// Ingest itself must never issue NOTIFY: that is what serialized commits.
func TestIngestDoesNotNotifyInsideItsTransaction(t *testing.T) {
	st := testutil.NewStore(t)
	tenant := seedTenant(t, st, "acme", 0)
	n, _ := listen(t, st)
	waitFor(t, "the listener to attach", func() bool { return n.Load() >= 1 })
	before := n.Load()
	if _, err := st.IngestEvent(context.Background(), tenant, "a.b", []byte(`{}`), "", 8); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := n.Load() - before; got != 0 {
		t.Fatalf("IngestEvent notified %d time(s) inside its transaction; that serializes commits (use Notifier)", got)
	}
}

// If the listener's connection dies (Postgres restart, partition, failover), it
// must reconnect and carry on, and ask for a catch-up check because notifications
// sent while it was away are gone.
func TestListenerReconnectsAfterItsConnectionIsKilled(t *testing.T) {
	st := testutil.NewStore(t)
	n, _ := listen(t, st)
	waitFor(t, "the listener to attach", func() bool { return n.Load() >= 1 })

	// Kill every other session that is LISTENing.
	exec(t, st, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	              WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'LISTEN%'`)
	// After reconnecting it reports the catch-up.
	before := n.Load()
	waitFor(t, "the listener to reconnect", func() bool { return n.Load() > before })

	before = n.Load()
	notifier := st.NewNotifier(5 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); notifier.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	notifier.Notify()
	waitFor(t, "a notification after reconnecting", func() bool { return n.Load() > before })
}

// A brief database outage while a service starts (a rolling restart during a
// failover, a partition) must not crash it: found by the chaos run, where a
// restarted worker exited because its single connection attempt failed.
func TestConnectWithRetryWaitsForTheDatabase(t *testing.T) {
	owner := testutil.NewStore(t)
	u, _ := url.Parse(owner.Pool.Config().ConnString())
	proxy, err := chaosproxy.New("127.0.0.1:0", net.JoinHostPort(u.Hostname(), u.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	u.Host = proxy.Addr()

	proxy.SetMode(chaosproxy.Reset) // the database is unreachable
	go func() { time.Sleep(1200 * time.Millisecond); proxy.SetMode(chaosproxy.Pass) }()
	start := time.Now()
	st, err := store.ConnectWithRetry(context.Background(), u.String(), 15*time.Second, nil)
	if err != nil {
		t.Fatalf("did not wait out a short outage: %v", err)
	}
	defer st.Close()
	if time.Since(start) < time.Second {
		t.Fatalf("connected after %v, before the database was reachable?", time.Since(start))
	}

	// A bad URL is a configuration error, not an outage: fail at once instead of waiting.
	start = time.Now()
	if _, err := store.ConnectWithRetry(context.Background(), "postgres://%zz", 15*time.Second, nil); err == nil || time.Since(start) > time.Second {
		t.Fatalf("a malformed URL should fail immediately, got %v after %v", err, time.Since(start))
	}

	// And it does give up when the database never comes back.
	proxy.SetMode(chaosproxy.Reset)
	start = time.Now()
	if _, err := store.ConnectWithRetry(context.Background(), u.String(), 1500*time.Millisecond, nil); err == nil {
		t.Fatal("connected to a database that is down")
	}
	if took := time.Since(start); took < time.Second || took > 6*time.Second {
		t.Fatalf("gave up after %v, want about the 1.5s budget", took)
	}
}
