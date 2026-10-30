package loadtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"webhookdelivery/internal/harness/chaosproxy"
)

// Env is everything a fault can act on.
type Env struct {
	Fleet       *Fleet
	Proxy       *chaosproxy.Proxy // between the services and Postgres; nil if the scenario has no partitions
	Recv        *Receiver
	PGContainer string // Docker container running Postgres
	Logf        func(format string, args ...any)

	mu     sync.Mutex
	events []string
}

// Note records something that happened, for the report timeline.
func (e *Env) Note(at time.Duration, format string, args ...any) {
	line := fmt.Sprintf("t+%-6v %s", at.Round(100*time.Millisecond), fmt.Sprintf(format, args...))
	e.mu.Lock()
	e.events = append(e.events, line)
	e.mu.Unlock()
	if e.Logf != nil {
		e.Logf("%s", line)
	}
}

func (e *Env) Timeline() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.events)
}

// Action is something that happens At a moment after the load starts.
type Action struct {
	At   time.Duration
	Name string
	Run  func(ctx context.Context, e *Env, since func() time.Duration) error
}

// NeedsProxy reports whether a scenario partitions the database network.
func NeedsProxy(scenario string) bool {
	return scenario == "db-partition" || scenario == "mixed"
}

// NeedsDocker reports whether a scenario controls the Postgres container.
func NeedsDocker(scenario string) bool {
	return scenario == "db-restart" || scenario == "mixed"
}

// Scenarios lists the available fault scenarios.
var Scenarios = []string{"none", "kill-workers", "kill-api", "db-restart", "db-partition", "receiver-faults", "mixed"}

// Plan returns the actions of a scenario for a load phase of length d.
func Plan(name string, d time.Duration) ([]Action, error) {
	frac := func(f float64) time.Duration { return time.Duration(float64(d) * f) }
	switch name {
	case "none":
		return nil, nil
	case "kill-workers":
		return killLoop("worker", frac(0.15), frac(0.15), frac(0.80)), nil
	case "kill-api":
		return killLoop("api", frac(0.25), frac(0.25), frac(0.80)), nil
	case "db-restart":
		return dbRestart(frac(0.30), frac(0.60)), nil
	case "db-partition":
		return dbPartition(frac(0.25), 10*time.Second, frac(0.60), 8*time.Second), nil
	case "receiver-faults":
		return receiverFaults(d), nil
	case "mixed":
		var all []Action
		all = append(all, killLoop("worker", frac(0.12), frac(0.22), frac(0.80))...)
		all = append(all, killLoop("api", frac(0.30), frac(0.40), frac(0.80))...)
		all = append(all, dbPartition(frac(0.20), 8*time.Second, frac(0.55), 6*time.Second)...)
		all = append(all, dbRestart(frac(0.70), frac(0.90))...)
		all = append(all, receiverFaults(d)...)
		return all, nil
	}
	return nil, fmt.Errorf("unknown scenario %q (have: %s)", name, strings.Join(Scenarios, ", "))
}

// RunTimeline executes actions at their scheduled times. Actions run concurrently
// (a slow restart must not delay the next fault).
func RunTimeline(ctx context.Context, e *Env, actions []Action, start time.Time) {
	slices.SortFunc(actions, func(a, b Action) int { return int(a.At - b.At) })
	var wg sync.WaitGroup
	since := func() time.Duration { return time.Since(start) }
	for _, a := range actions {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-time.After(time.Until(start.Add(a.At))):
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Run(ctx, e, since); err != nil {
				e.Note(since(), "FAULT %s failed: %v", a.Name, err)
			}
		}()
	}
	wg.Wait()
}

// killLoop `kill -9`s a random process of the given kind every `every`, from
// `first` until `until`, restarting each victim two seconds later. Only one
// victim is down at a time, as in a rolling failure.
func killLoop(kind string, first, every, until time.Duration) []Action {
	var out []Action
	for at := first; at < until; at += every {
		out = append(out, Action{At: at, Name: "kill " + kind, Run: func(ctx context.Context, e *Env, since func() time.Duration) error {
			procs := e.Fleet.Workers
			if kind == "api" {
				procs = e.Fleet.APIs
			}
			var alive []*Proc
			for _, p := range procs {
				if p.Alive() {
					alive = append(alive, p)
				}
			}
			if len(alive) < 2 {
				return nil // never take the last one down: the point is to lose capacity, not everything
			}
			victim := alive[rand.IntN(len(alive))] //nolint:gosec // chaos choice, not security
			victim.Kill()
			e.Note(since(), "kill -9 %s", victim.Name)
			time.Sleep(2 * time.Second)
			// A supervisor (systemd, Kubernetes) keeps restarting a crashed process, so retry.
			var err error
			for try := 0; try < 5; try++ {
				if err = victim.Start(); err != nil {
					return err
				}
				if err = victim.WaitReady(ctx, 40*time.Second); err == nil {
					break
				}
				victim.Kill()
				time.Sleep(time.Second)
			}
			if err != nil {
				return err
			}
			e.Note(since(), "restarted %s", victim.Name)
			return nil
		}})
	}
	return out
}

func dbPartition(stallAt, stallFor, resetAt, resetFor time.Duration) []Action {
	hold := func(mode chaosproxy.Mode, d time.Duration) func(context.Context, *Env, func() time.Duration) error {
		return func(_ context.Context, e *Env, since func() time.Duration) error {
			if e.Proxy == nil {
				return fmt.Errorf("no proxy: the services are not connected through one")
			}
			e.Proxy.SetMode(mode)
			e.Note(since(), "DB network partition begins (%s) for %v", mode, d)
			time.Sleep(d)
			e.Proxy.SetMode(chaosproxy.Pass)
			e.Note(since(), "DB network partition healed")
			return nil
		}
	}
	return []Action{
		{At: stallAt, Name: "db stall", Run: hold(chaosproxy.Stall, stallFor)},
		{At: resetAt, Name: "db reset", Run: hold(chaosproxy.Reset, resetFor)},
	}
}

func dbRestart(restartAt, crashAt time.Duration) []Action {
	return []Action{
		{At: restartAt, Name: "postgres restart", Run: func(ctx context.Context, e *Env, since func() time.Duration) error {
			e.Note(since(), "docker restart postgres (clean shutdown)")
			if err := docker(ctx, "restart", e.PGContainer); err != nil {
				return err
			}
			if err := waitPG(ctx, e.PGContainer); err != nil {
				return err
			}
			e.Note(since(), "postgres back")
			return nil
		}},
		{At: crashAt, Name: "postgres crash", Run: func(ctx context.Context, e *Env, since func() time.Duration) error {
			e.Note(since(), "docker kill -s KILL postgres (crash; recovery replays the WAL)")
			if err := docker(ctx, "kill", "-s", "KILL", e.PGContainer); err != nil {
				return err
			}
			time.Sleep(time.Second)
			if err := docker(ctx, "start", e.PGContainer); err != nil {
				return err
			}
			if err := waitPG(ctx, e.PGContainer); err != nil {
				return err
			}
			e.Note(since(), "postgres recovered")
			return nil
		}},
	}
}

func receiverFaults(d time.Duration) []Action {
	frac := func(f float64) time.Duration { return time.Duration(float64(d) * f) }
	set := func(at time.Duration, f Fault) Action {
		return Action{At: at, Name: "receiver " + f.String(), Run: func(_ context.Context, e *Env, since func() time.Duration) error {
			e.Recv.SetFault(f)
			e.Note(since(), "receiver now: %s", f)
			return nil
		}}
	}
	return []Action{
		set(frac(0.10), Fault{Kind: FaultStatus, Prob: 0.30}),
		set(frac(0.20), Fault{Kind: FaultSlow, Prob: 0.30, Delay: 8 * time.Second}),
		set(frac(0.30), Fault{Kind: FaultGarbage, Prob: 0.30}),
		set(frac(0.40), Fault{Kind: FaultReset, Prob: 0.30}),
		set(frac(0.50), Fault{Kind: FaultBigBody, Prob: 0.30}),
		set(frac(0.60), Fault{Kind: FaultBlackhole, Prob: 0.20}),
		set(frac(0.72), Fault{Kind: FaultStatus, Prob: 0.90, Status: 503}), // near-total outage
		set(frac(0.82), Fault{}),                                           // healed
	}
}

func docker(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput() //nolint:gosec // fixed binary; the arguments are built by this package, not user input
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func waitPG(ctx context.Context, container string) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if err := docker(ctx, "exec", container, "pg_isready", "-U", "webhooks", "-q"); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("postgres did not come back within 60s")
}
