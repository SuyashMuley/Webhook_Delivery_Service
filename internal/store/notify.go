package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// NotifyChannel is the Postgres channel on which ingest announces new work.
// Idle workers LISTEN on it, so a delivery is picked up within milliseconds
// instead of waiting for the next poll. Polling remains as the fallback, so a
// missed notification only costs latency, never correctness.
//
// Note: LISTEN needs a session-level connection, so it does not work through a
// transaction-pooling proxy such as PgBouncer in transaction mode; workers then
// simply fall back to polling (set WORKER_LISTEN=false to skip the attempt).
const NotifyChannel = "webhook_deliveries"

// Listen calls onNotify whenever something is announced on channel, until ctx
// ends. It holds one dedicated connection (outside the pool) and reconnects
// with backoff if it drops, calling onNotify after each reconnect because
// notifications sent while disconnected are lost. onError is told about
// connection problems (may be nil).
func (s *Store) Listen(ctx context.Context, channel string, onNotify func(), onError func(error)) {
	connString := s.Pool.Config().ConnString()
	backoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		err := s.listenOnce(ctx, connString, channel, onNotify)
		if ctx.Err() != nil {
			return
		}
		if onError != nil && err != nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

func (s *Store) listenOnce(ctx context.Context, connString, channel string, onNotify func()) error {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return err
	}
	onNotify() // anything announced while we were not listening was missed: check now
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		onNotify()
	}
}

// Notifier announces new work on NotifyChannel without costing ingest any
// throughput.
//
// Why not just NOTIFY inside the ingest transaction? Postgres serializes the
// commit of every transaction that issued a NOTIFY behind a database-wide lock,
// held through the WAL flush. Found by the load test: it capped ingest at about
// 600 events/s (one commit fsync at a time) on an otherwise idle machine.
//
// Instead the API calls Notify after its transaction has committed, and a
// single goroutine turns those calls into at most one NOTIFY per window, issued
// in its own transaction. That transaction writes nothing, so it needs no WAL
// flush and holds the lock only for microseconds. The price is up to one window
// (a few milliseconds) of extra wake-up latency, and notifications are lost if
// the process dies between commit and notify, which costs latency only, since
// workers also poll.
type Notifier struct {
	s      *Store
	window time.Duration
	kick   chan struct{}
}

func (s *Store) NewNotifier(window time.Duration) *Notifier {
	return &Notifier{s: s, window: window, kick: make(chan struct{}, 1)}
}

// Notify records that new work was committed. It never blocks.
func (n *Notifier) Notify() {
	select {
	case n.kick <- struct{}{}:
	default: // one is already pending; it will cover this too
	}
}

// Run sends coalesced notifications until ctx ends.
func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.kick:
		}
		// Let a burst of commits accumulate so they share one notification.
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.window):
		}
		select { // anything kicked during the window is covered by the notification about to be sent
		case <-n.kick:
		default:
		}
		_, _ = n.s.Pool.Exec(ctx, `SELECT pg_notify($1, '')`, NotifyChannel)
	}
}
