// Package invariants checks, against the database, the properties the system
// promises regardless of crashes, partitions and misbehaving receivers.
//
// It is used by the load/chaos tool and by the in-process chaos tests, so a
// property that is only checked in one place cannot quietly drift.
//
// The properties, for the set of events a client was told were accepted:
//
//	durable     every accepted event exists
//	fan-out     every event has exactly one delivery per subscribed endpoint
//	terminal    once the system has drained, every delivery is succeeded or dead:
//	            none pending, none in flight
//	no strays   no lease is left held or expired
//	no loss     a delivery marked succeeded was really received by the endpoint
//	explained   every dead delivery has a reason and a time
//	bounded     attempts never exceed max_attempts, and attempt rows never exceed the counter
//
// Delivery is at-least-once, so duplicates are counted and reported but are not
// violations.
package invariants

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Key identifies one delivery from the receiver's point of view.
type Key struct{ Event, Endpoint string }

// Observations is what the receiving side saw.
type Observations interface {
	// Count returns how many valid requests the endpoint received for the event.
	Count(event, endpoint string) int
	// Each calls f for every (event, endpoint) that was received at least once.
	Each(f func(k Key, n int))
}

// Expected describes what the client believes it successfully did.
type Expected struct {
	EventIDs []string // events acknowledged with 2xx
	Fanout   int      // endpoints each event is subscribed to
	Received Observations
}

type Violation struct {
	Invariant string
	Count     int
	Examples  []string // a few concrete rows, to make the failure debuggable
}

type Report struct {
	Events       int
	Deliveries   int
	ByStatus     map[string]int
	DeadByReason map[string]int
	Duplicates   int // deliveries received more than once (expected after crashes; not a violation)
	MaxReceives  int
	Violations   []Violation
}

// OK reports whether every invariant held.
func (r Report) OK() bool { return len(r.Violations) == 0 }

func (r Report) String() string {
	s := fmt.Sprintf("events=%d deliveries=%d status=%v dead_by_reason=%v duplicates=%d max_receives=%d",
		r.Events, r.Deliveries, r.ByStatus, r.DeadByReason, r.Duplicates, r.MaxReceives)
	for _, v := range r.Violations {
		s += fmt.Sprintf("\n  VIOLATION %-9s x%d  e.g. %v", v.Invariant, v.Count, v.Examples)
	}
	return s
}

const maxExamples = 5

// Check evaluates the invariants. pool must connect as the table owner (it reads
// across tenants). Call it after the system has drained.
func Check(ctx context.Context, pool *pgxpool.Pool, exp Expected) (Report, error) {
	rep := Report{Events: len(exp.EventIDs), ByStatus: map[string]int{}, DeadByReason: map[string]int{}}
	add := func(name string, count int, examples []string) {
		if count > 0 {
			if len(examples) > maxExamples {
				examples = examples[:maxExamples]
			}
			rep.Violations = append(rep.Violations, Violation{Invariant: name, Count: count, Examples: examples})
		}
	}
	ids := exp.EventIDs

	// durable
	var present int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE id = ANY($1::uuid[])`, ids).Scan(&present); err != nil {
		return rep, err
	}
	if present != len(ids) {
		missing, err := queryStrings(ctx, pool, `
			SELECT x.id::text FROM unnest($1::uuid[]) x(id)
			 WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = x.id) LIMIT 5`, ids)
		if err != nil {
			return rep, err
		}
		add("durable", len(ids)-present, missing)
	}

	// fan-out
	rows, err := pool.Query(ctx, `
		SELECT x.id::text, COALESCE(d.c, 0)
		  FROM unnest($1::uuid[]) x(id)
		  JOIN events e ON e.id = x.id
		  LEFT JOIN (SELECT event_id, count(*) c FROM deliveries WHERE event_id = ANY($1::uuid[]) GROUP BY 1) d
		         ON d.event_id = x.id
		 WHERE COALESCE(d.c, 0) <> $2`, ids, exp.Fanout)
	if err != nil {
		return rep, err
	}
	var bad []string
	nBad := 0
	for rows.Next() {
		var id string
		var c int
		if err := rows.Scan(&id, &c); err != nil {
			return rep, err
		}
		nBad++
		bad = append(bad, fmt.Sprintf("event %s has %d deliveries, want %d", id, c, exp.Fanout))
	}
	rows.Close()
	add("fan-out", nBad, bad)

	// status census, terminal, strays
	rows, err = pool.Query(ctx, `SELECT status, count(*) FROM deliveries WHERE event_id = ANY($1::uuid[]) GROUP BY 1`, ids)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return rep, err
		}
		rep.ByStatus[st] = n
		rep.Deliveries += n
	}
	rows.Close()
	if n := rep.ByStatus["pending"] + rep.ByStatus["in_flight"]; n > 0 {
		ex, err := queryStrings(ctx, pool, `
			SELECT id::text || ' ' || status || ' attempts=' || attempt_count || ' due=' || next_attempt_at
			  FROM deliveries WHERE event_id = ANY($1::uuid[]) AND status IN ('pending','in_flight') LIMIT 5`, ids)
		if err != nil {
			return rep, err
		}
		add("terminal", n, ex)
	}
	var strays int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM deliveries
		 WHERE event_id = ANY($1::uuid[])
		   AND ((status IN ('succeeded','dead') AND (locked_by IS NOT NULL OR lease_expires_at IS NOT NULL))
		     OR (status = 'in_flight' AND lease_expires_at < now()))`, ids).Scan(&strays); err != nil {
		return rep, err
	}
	add("no-strays", strays, nil)

	// no loss: succeeded => received
	rows, err = pool.Query(ctx, `
		SELECT event_id::text, endpoint_id::text FROM deliveries
		 WHERE event_id = ANY($1::uuid[]) AND status = 'succeeded'`, ids)
	if err != nil {
		return rep, err
	}
	var lost []string
	nLost := 0
	for rows.Next() {
		var ev, ep string
		if err := rows.Scan(&ev, &ep); err != nil {
			return rep, err
		}
		if exp.Received.Count(ev, ep) == 0 {
			nLost++
			lost = append(lost, "event "+ev+" endpoint "+ep+" marked succeeded but never received")
		}
	}
	rows.Close()
	add("no-loss", nLost, lost)

	// explained dead, DLQ census
	rows, err = pool.Query(ctx, `SELECT COALESCE(dead_reason, '<none>'), count(*) FROM deliveries WHERE event_id = ANY($1::uuid[]) AND status = 'dead' GROUP BY 1`, ids)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var r string
		var n int
		if err := rows.Scan(&r, &n); err != nil {
			return rep, err
		}
		rep.DeadByReason[r] = n
	}
	rows.Close()
	var unexplained int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE event_id = ANY($1::uuid[]) AND status = 'dead' AND (dead_reason IS NULL OR dead_at IS NULL)`, ids).Scan(&unexplained); err != nil {
		return rep, err
	}
	add("explained", unexplained, nil)

	// bounded
	var over, rowsOver int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE event_id = ANY($1::uuid[]) AND attempt_count > max_attempts`, ids).Scan(&over); err != nil {
		return rep, err
	}
	add("bounded", over, nil)
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM deliveries d
		 WHERE d.event_id = ANY($1::uuid[])
		   AND (SELECT count(*) FROM attempts a WHERE a.delivery_id = d.id) > d.attempt_count`, ids).Scan(&rowsOver); err != nil {
		return rep, err
	}
	add("bounded-rows", rowsOver, nil)

	// duplicates (informational)
	exp.Received.Each(func(_ Key, n int) {
		if n > 1 {
			rep.Duplicates++
		}
		rep.MaxReceives = max(rep.MaxReceives, n)
	})

	sort.Slice(rep.Violations, func(i, j int) bool { return rep.Violations[i].Invariant < rep.Violations[j].Invariant })
	return rep, nil
}

func queryStrings(ctx context.Context, pool *pgxpool.Pool, q string, args ...any) ([]string, error) {
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
