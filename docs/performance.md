# Performance and resilience results

What was measured, how, what it showed, the bugs it found, and what it does **not** show.

## TL;DR

- **Target met with headroom.** 2,000 deliveries/s sustained for 60s with a p99 of **18 ms** from request to first receipt (24 ms by the workers' own histogram), zero retries, zero failures. 4,000 deliveries/s sustained with p99 319 ms.
- **Nothing was lost in any of the ten runs** (four load levels and six fault scenarios), including repeated `kill -9` of workers and API servers, database network partitions, a Postgres restart and a `SIGKILL` crash with WAL recovery, a receiver that fails in six different ways, and all of it at once. Every accepted event ended with one delivery per endpoint, each `succeeded` (none stuck, none lost).
- **Delivery is at-least-once, and the duplicates are measured**, not hand-waved: 0 in clean runs, 13 after five worker crashes, ~2,300 when receivers answer after the sender's timeout.
- The load test found **seven real defects** in the system as built through Phase 4 (listed below), the worst of which capped throughput at ~650 deliveries/s. All are fixed, with regression tests.

## Method

`cmd/loadtest` (run with `make loadtest SCENARIO=...`):

1. Creates a **throwaway database** and migrates it, so runs never touch development data.
2. Starts real **`api` and `worker` processes**. A `kill -9` is therefore a genuine process crash: no signal handler, no drain, no lease release.
3. Runs an embedded **receiver** standing in for every customer endpoint. It verifies each webhook signature, records exactly which events it acknowledged with a 2xx, and can misbehave on command.
4. Sends events at a **fixed arrival rate** (open loop). A closed-loop generator slows down when the system does and so hides exactly the latency being measured. If the generator itself cannot keep its schedule it says so ("late") and a fault-free run is failed, because its numbers would be meaningless.
5. Sends producer-style requests: on a network error, 5xx or 429 it retries with the **same `Idempotency-Key`**, against another API instance.
6. After the load stops it **waits for the queue to drain**, scrapes the workers' own metrics, stops every process with `SIGTERM` (checking they exit gracefully), then runs the **invariant checker** against the database.

Latency is measured two independent ways: by the receiver's clock (from when the producer sent the request to the first time the endpoint acknowledged the event) and by the workers' `webhook_first_attempt_delay_seconds` histogram. They agree.

### The invariants (`internal/harness/invariants`)

For the set of events the producer was told were accepted:

| Invariant | Meaning |
|---|---|
| durable | every accepted event exists |
| fan-out | each event has exactly one delivery per subscribed endpoint |
| terminal | after draining, every delivery is `succeeded` or `dead`; none `pending`, none `in_flight` |
| no-strays | no lease is left held or expired, and finished deliveries hold no lease |
| no-loss | every delivery marked `succeeded` was really acknowledged by the endpoint |
| explained | every `dead` delivery has a reason and a time |
| bounded | `attempt_count <= max_attempts`, and attempt rows never exceed the counter |

Each invariant has a test that deliberately breaks it and checks the checker notices (a checker that never complains proves nothing). The same checker backs `TestInvariantsHoldUnderFailuresAndCrashes`, an in-process chaos test that runs on every `go test` and was itself mutation-tested: with the lease reaper disabled it fails.

### Environment (read this before trusting any number)

One MacBook, 10 cores, 24 GB. **Everything shared those cores**: the load generator, the receiver, 2-3 API processes, 3-4 workers, and Postgres 17 running in Docker's Linux VM (10 CPUs, 8 GB). Traffic is loopback, plain HTTP, ~300-byte payloads, 20-40 tenants, five endpoints each. Each run is a single execution of 45-150 seconds. The numbers below are about *this system's behaviour*, not a capacity guarantee for production hardware.

Service settings for the runs: 8 attempts with a `1s,2s,3s,5s,5s,5s,5s` retry schedule, 3s delivery timeout, 10s lease, `WORKER_CONCURRENCY=200`, 20 concurrent deliveries per endpoint.

## Results

### Throughput and latency (no faults)

| Offered | Sustained | Ingest p99 | Request to first receipt p50 / p99 | Workers' own p99 | Result |
|---|---|---|---|---|---|
| 200 deliveries/s | 200/s | 4.8 ms | 16 ms / 19 ms | 25 ms | all 9,005 delivered, 0 retries |
| **2,000/s** (target) | **2,000/s** | 8.3 ms | 6 ms / **18 ms** | 24 ms | all 120,005 delivered, 0 retries, 0 failures |
| 4,000/s | 4,000/s | 31 ms | 22 ms / 319 ms | 353 ms | all 240,005 delivered, 0 retries |
| 6,000/s | **3,300/s** | 56 ms | 16 s / 32 s | 47 s | saturated: backlog built, then drained in 29 s; nothing lost |

Producer-visible ingest latency stayed low at every level (p99 under 60 ms) because the API accepts into Postgres and the backlog, not the delivery side, absorbs overload. **Above roughly 4,000 deliveries/s on this machine the system degrades gracefully**: the queue grows, latency rises, and it all drains afterwards with every invariant intact.

I did **not** profile the ~4,000/s ceiling after the fixes below, so I can say it is where the delivery side saturates on this shared box, but not which resource is the limit. An earlier profile (before the ingest fix) showed Postgres at ~370% CPU inside the Docker VM, so database CPU is the likely candidate, unconfirmed.

### Fault scenarios (2,000 deliveries/s offered, except where noted)

| Scenario | What was done | Delivered | Violations | Duplicates (max receives) | Notes |
|---|---|---|---|---|---|
| `kill-workers` | 5 x `kill -9` of a random worker, each replaced 2s later | 150,005 / 150,005 | none | 13 (2) | worst latency 10 s = the lease length (work held by a dead worker waits for its lease) |
| `kill-api` | 3 x `kill -9` of an API process | 150,005 / 150,005 | none | 0 | producer retried 814 failed requests with idempotency keys: no duplicate events |
| `db-partition` | 10 s **silent** stall, then 8 s **reset**, between services and Postgres | 123,845 / 123,845 | none | 27 (2) | producers got fast `503 + Retry-After` (4,492 retries); 5,232 scheduled events were never sent because every producer connection was busy waiting for the outage |
| `db-restart` | clean Postgres restart, then `SIGKILL` crash and WAL recovery | 178,755 / 178,755 | none | 45 (2) | 45 lease reclaims, matching the duplicates |
| `receiver-faults` | 30% 500s, 30% 8 s hangs, 30% garbage (non-HTTP bytes), 30% connection resets, 30% 5 MB bodies, 20% black holes, then 90% 503s | 200,005 / 200,005 | none | 2,365 (3) | 52,247 retries, none dead-lettered; duplicates come from "slow" receivers that processed the request but answered after the 3 s timeout |
| **`mixed`** | all of the above overlapping: 6 `kill -9`s, 2 partitions, Postgres restart and crash, every receiver fault | 280,170 / 280,170 | none | 2,326 (3) | 405 lease reclaims; drained 24 s after the load ended |

"Duplicates" counts deliveries an endpoint acknowledged more than once. That is the at-least-once guarantee working as documented: a crash or a timeout leaves the outcome unknown, so the delivery is retried. Receivers must deduplicate on `webhook-id`.

Latency under faults is high **by design** (retries, timeouts, backoff): in `mixed` the median first receipt is 19 s. The claim being tested is correctness under failure, not latency under failure.

### Soak: 30 minutes at 1,000 deliveries/s with retention on

`./bin/loadtest -scenario none -rate 200 -fanout 5 -duration 30m -purge -sample 30s`. Retention ran every 20 s with 2-minute windows, so the purge cycled about 90 times. 360,001 events, 1,800,005 deliveries: all accepted, all delivered, 0 retries, 0 duplicates, 0 unacknowledged.

| | Start (t+5m) | End (t+30m) |
|---|---|---|
| API RSS | 27-28 MB | 29-31 MB (+5 to +10%) |
| Worker RSS | 33 MB | 37-38 MB (+13 to +15%) |
| Database size | 130 MB | 138 MB |
| `deliveries` live rows / dead tuples | 134k / 102k | 140k / 96k (autovacuum ran 25 more times) |

Latency by 5-minute window: p50 6.9, 7.0, 7.2, 8.1, 8.3, 7.2 ms; p99 17, 18, 23, 71 ms, **2.06 s**, 21 ms.

- **Bloat is bounded**: the table size and live rows stay flat, so purge plus autovacuum keep up.
- **Memory still crept up** by a few MB. 30 minutes cannot tell a slow leak from a cache that has not finished warming; this is unresolved. A longer run with heap profiles would settle it.
- **One unexplained latency event**: around t+23m a backlog of about 1,400 deliveries built for roughly 10 s (workers' throughput dipped to 750/s), giving the 2 s p99 in that window; it drained by itself. I did not identify the cause (checkpoint, autovacuum and GC are all candidates).
- The invariant checker only examines events younger than the retention window when `-purge` is set, because older ones are deleted on purpose. The 4-minute validation run passed all invariants.
- **Bug found by this run**: the retention purge failed every cycle under the real `webhooks_worker` role (see defect 7).

## Defects found by this phase

All present in the system as it stood after Phase 4, all fixed, each with a regression test.

1. **Connection churn capped throughput at ~650 deliveries/s and dead-lettered healthy deliveries.** The sender's HTTP transport kept 4 idle connections per host. At high concurrency to one host every delivery dialed a new connection and abandoned it to `TIME_WAIT`, exhausting ephemeral ports (`can't assign requested address` on 21,222 attempts; 857 deliveries ran out of attempts with **no faults injected**). It would also have paid a TLS handshake per delivery in production. Now 256 idle per host. (`TestConcurrentDeliveriesToOneHostReuseConnections`)
2. **Idle-poll latency.** An idle worker slept up to the 500 ms poll interval, giving a p50 of ~170-240 ms and p99 of ~600 ms even at modest load. Idle workers now `LISTEN` and are woken by the API: p99 fell to ~20 ms. (`TestNewWorkWakesIdleWorkersImmediately`: 32 ms with, 2.3 s without)
3. **`NOTIFY` inside the ingest transaction capped ingest at ~600 events/s.** Introduced by my own fix for #2. Postgres serializes the commit of any transaction that issued a `NOTIFY` behind a database-wide lock held through the WAL flush, so commits ran one fsync at a time (26 sessions waiting on `Lock:object`). The notification now happens after commit, coalesced to one per 10 ms. Ingest was then clean at 1,600 events/s. (`TestIngestDoesNotNotifyInsideItsTransaction`)
4. **The worker binary never enabled the wake-up.** A scripted edit silently matched nothing. In-process tests could not see it; the real-process load test did, because latency did not improve.
5. **A database stall hung every API request for the whole outage** (ingest p99 of ~10 s). Requests now have a deadline (`API_REQUEST_TIMEOUT`, 5 s) and an unreachable database maps to `503` with `Retry-After`. (`TestDatabaseOutageGivesAPrompt503WithRetryAfter`)
6. **A service restarted during a database blip crashed instead of waiting**, and stayed down. Services now retry the initial connection for up to 60 s; a malformed URL still fails immediately. (`TestConnectWithRetryWaitsForTheDatabase`)
7. **Retention never purged events in production.** The events purge used `SELECT ... FOR UPDATE SKIP LOCKED`, which needs `UPDATE` on `events`; the worker role only has `SELECT` and `DELETE` (events are immutable). The error also aborted the purge loop, so deliveries were purged one batch per cycle, far slower than they arrived (1.29M live rows and 854 MB after 25 minutes instead of about 130k). Tests passed because they ran as the table owner. Now no row lock is used; `TestPurgeWorksUnderTheWorkerRole` fails on the old query. Found by the soak, not by any earlier test.

Smaller finds in the test tooling itself, noted because they would have invalidated results: the fault proxy consumed a byte of the data stream while polling for a closed client; the Prometheus text parser panics without an explicit validation scheme; several of my own tests were flaky or wrong (racy gauge reads, `LIMIT 1` without `ORDER BY`, a timeout too short for the deliberately slow case).

## What this does not show

Be careful not to over-read the results.

- **Hardware.** One shared laptop. Absolute throughput will differ; relative behaviour (graceful degradation, no loss) is the finding.
- **No TLS and a trivially fast receiver on loopback.** Real endpoints add handshake cost, latency and variance. Connection reuse (finding 1) matters more, not less, there.
- **Short runs, single samples.** 45-150 s each, one run per scenario, so no confidence intervals. The fault and throughput runs had retention off. One 30-minute soak (above) covers bloat, autovacuum and purge, but not hours-long behaviour, and its memory trend is inconclusive.
- **Small scale in the dimensions that matter for fairness:** 20-40 tenants and 100-200 endpoints, not thousands. The claim query's cost grows with tenants and endpoints probed per claim (see `docs/plan.md`); that scaling was benchmarked in isolation but not in these end-to-end runs.
- **Single Postgres on a local Docker volume.** Not tested: replica failover, replication lag, a full disk, a slow disk, or a managed service's behaviour. The "crash" is `SIGKILL` of the container; a real power loss with different fsync semantics is not simulated.
- **Network faults are only between the services and Postgres** (plus an abusive receiver). Not tested: partitions between API and workers (they do not talk to each other), between a worker and the internet specifically, DNS failures, or clock skew between hosts.
- **Fault choices were mine.** Chaos finds what it is pointed at. The invariant checker is the stronger guarantee than the scenario list.
- **PgBouncer-style transaction pooling** is not tested; `LISTEN` does not work through it (set `WORKER_LISTEN=false`).
- **The producer sheds load during outages** (the `late` count): events it could not send because every connection was busy are not covered by the invariants. That models a producer that drops or queues; a real producer should alert on it.

## Reproducing

```sh
make bin
make loadtest                          # 2,000 deliveries/s for 60 s, no faults
make loadtest SCENARIO=kill-workers    # none | kill-workers | kill-api | db-restart | db-partition | receiver-faults | mixed
./bin/loadtest -scenario none -rate 800 -fanout 5 -tenants 40 -duration 60s -workers 4 -apis 3   # 4,000/s
```

Each run writes `report.json` and every process's log to `loadtest-out/<timestamp>-<scenario>/`. `db-restart` and `mixed` need Docker (they control the Postgres container); the others need only a reachable Postgres. A short version (steady load, worker kills, receiver faults) runs in CI as `loadtest-smoke`.
