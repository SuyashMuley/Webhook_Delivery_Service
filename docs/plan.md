# Webhook Delivery System: Design & Plan

Status: **Decisions agreed (see section 13). Phases 0-5 complete.**

Context: portfolio project built to production-grade standards for security, observability, scalability, and multi-tenancy.

## 1. Goals

- Accept events from internal producers and deliver them as HTTP POSTs to customer-registered endpoints.
- **At-least-once** delivery. Durable: an accepted event is never lost, even if workers or nodes crash.
- Automatic retries with exponential backoff and jitter.
- **Dead letter queue (DLQ)** for deliveries that exhaust retries, with inspection and manual/bulk replay.
- Horizontally scalable: any number of stateless API and worker nodes.
- Isolation: one slow or failing endpoint must not degrade delivery to others.
- Secure by default: signed payloads, SSRF protection, secret rotation.
- Observable: metrics, structured logs, per-attempt history.

### Non-goals (v1)

- Exactly-once delivery (impossible over HTTP; we provide idempotency keys so receivers can dedupe).
- Strict ordering per endpoint (documented as unordered; see 9).
- Multi-region active/active.
- Customer-facing UI (API only; a small admin CLI is enough).

## 2. Proposed Tech Stack

| Concern | Choice | Why |
|---|---|---|
| Language | **Go** | Cheap concurrency for thousands of in-flight HTTP calls, single static binaries, strong stdlib `net/http`. |
| Source of truth + queue | **PostgreSQL** | Durable, transactional, and `SELECT ... FOR UPDATE SKIP LOCKED` gives a correct distributed work queue without a second system. Avoids the dual-write problem (event stored but not enqueued). |
| Delayed retries | `next_attempt_at` column + index | A retry is just an UPDATE. No separate delay queue needed. |
| DLQ | `status = 'dead'` rows in the same table | Queryable, replayable with one UPDATE, keeps full attempt history. |
| Rate limit / circuit breaker state | Postgres columns on `endpoints` (v1); Redis later if needed | Keeps v1 to one stateful dependency. |
| Metrics / tracing | Prometheus + OpenTelemetry | Standard. |
| Local dev | Docker Compose (Postgres, api, worker, a flaky mock receiver) | One command to run everything. |
| Migrations | `goose` | Simple SQL migrations. |
| DB access | `pgx` + `sqlc` | Type-safe SQL, no ORM magic around locking queries. |

### Why not Kafka / RabbitMQ / SQS / Redis as the queue?

| Option | Verdict |
|---|---|
| Kafka | Retries with per-message delay are awkward (retry topics per delay tier, partition blocking). Heavy ops. Reconsider only past ~10k+ deliveries/sec. |
| RabbitMQ / SQS | Delay and DLQ are built in, but we still need Postgres for history, so we'd have two systems and a dual-write problem (needs an outbox anyway). |
| Redis (sorted set / BullMQ) | Fast, but durability is weaker, and state must be reconciled with Postgres. |
| **Postgres queue** | One system, transactional enqueue, easy DLQ/replay/inspection. Comfortably handles low thousands of deliveries/sec. **Chosen for v1.** |

The worker's dequeue is behind a small `Queue` interface so the backend can be swapped later if throughput demands it.

## 3. Architecture

```
 Producers                                   Customer endpoints
    │ POST /v1/events                               ▲
    ▼                                               │ HTTPS POST (signed)
┌─────────┐   tx: insert event +      ┌──────────────┴─────────┐
│  API    │──────deliveries──────────▶│       Workers          │
│ (N pods)│                           │ (N pods, stateless)    │
└────┬────┘                           │  claim → send → record │
     │ endpoint/subscription CRUD     └───────────┬────────────┘
     ▼                                            │ SKIP LOCKED claim,
┌─────────────────────────────────────────────────▼───────────┐
│                       PostgreSQL                            │
│ endpoints · events · deliveries(queue+DLQ) · attempts       │
└─────────────────────────────────────────────────────────────┘
     ▲
     │ reaper (in worker): reclaim expired leases; purge old data
```

### Components

1. **API service** (stateless)
   - Endpoint and subscription CRUD.
   - `POST /v1/events`: validates, de-dupes on `(tenant, idempotency_key)`, and in **one transaction** inserts the event plus one `deliveries` row per matching endpoint (fan-out on write). Returns `202`.
   - DLQ and delivery inspection/replay APIs.
2. **Worker service** (stateless, scale horizontally)
   - Poll loop claims a batch of due deliveries, sends them with bounded concurrency, records the attempt, and updates state.
   - Reaper goroutine returns deliveries with expired leases to `pending`.
3. **PostgreSQL**: the only stateful component (use managed HA Postgres with a replica in production).

## 4. Data Model

```sql
tenants(
  id uuid pk, name, status,           -- active | suspended
  max_inflight int, events_per_sec int, max_pending int  -- fairness / quota limits
)

api_keys(
  id uuid pk, tenant_id, key_prefix, key_hash,  -- hash only; plaintext shown once
  scopes text[], created_at, last_used_at, revoked_at
)

endpoints(
  id uuid pk, tenant_id, url, description,
  event_types text[],                 -- subscription filter ('*' allowed)
  secrets jsonb,                      -- [{id, value, created_at, expires_at}] supports rotation
  status text,                        -- active | paused | disabled
  max_concurrency int default 5,      -- per-endpoint in-flight cap
  consecutive_failures int default 0, -- circuit breaker
  circuit_open_until timestamptz,
  created_at, updated_at
)

events(
  id uuid pk, tenant_id, type, payload jsonb,
  idempotency_key text, created_at,
  unique(tenant_id, idempotency_key)
)

deliveries(                           -- one per (event, endpoint); this IS the queue + DLQ
  id uuid pk, event_id, endpoint_id,
  status text,                        -- pending | in_flight | succeeded | dead
  attempt_count int default 0,
  max_attempts int,
  next_attempt_at timestamptz,        -- when it becomes due
  lease_expires_at timestamptz,       -- set while in_flight
  locked_by text,
  last_status_code int, last_error text,
  dead_at timestamptz, dead_reason text,
  created_at, updated_at
)
-- partial index: (next_attempt_at) WHERE status = 'pending'
-- partial index: (lease_expires_at) WHERE status = 'in_flight'
-- index: (endpoint_id, status)

attempts(                             -- append-only history
  id, delivery_id, attempt_no, started_at, duration_ms,
  status_code, error, response_body_snippet (<=1KB)
)
```

## 5. Delivery Lifecycle

```
            ┌───────────── retry (backoff) ──────────────┐
            ▼                                             │
 [pending] ──claim──▶ [in_flight] ──2xx──▶ [succeeded]    │
    ▲                      │                              │
    │ lease expired        ├──fail & attempts left────────┘
    └──(reaper)────────────┤
                           └──fail & attempts exhausted──▶ [dead] (DLQ)
                                                              │
                                              replay API ─────┘ → [pending]
```

### Claiming (the core distributed primitive)

The implemented query (see `internal/store/queue.go`) claims per tenant, so one tenant's backlog cannot starve the rest:

```sql
WITH due AS (
  SELECT d.id
    FROM (SELECT id, max_inflight FROM tenants WHERE status='active' ORDER BY random()) t
   CROSS JOIN LATERAL (
         SELECT d.id FROM deliveries d
           JOIN endpoints e ON e.id = d.endpoint_id AND e.status = 'active'
          WHERE d.tenant_id = t.id AND d.status = 'pending' AND d.next_attempt_at <= now()
          ORDER BY d.next_attempt_at
          LIMIT GREATEST(0, LEAST($slice, t.max_inflight - <tenant's in-flight count>))
            FOR UPDATE OF d SKIP LOCKED) d
   LIMIT $batch
)
UPDATE deliveries d SET status='in_flight', locked_by=$worker,
       lease_expires_at = now() + $lease, attempt_count = attempt_count + 1
  FROM due WHERE d.id = due.id RETURNING ...;
```

- `SKIP LOCKED` sits *inside* the per-tenant lateral, so rows are locked only as they are produced (verified with `EXPLAIN ANALYZE`: `LockRows` below `Limit`, exactly the returned rows locked). This replaces the earlier two-step idea (window function, then lock), which can't lock under a window and makes concurrent workers collide on the same candidates.
- Tenants are visited in random order, each contributing at most `$slice` (default 5) rows, so small tenants are served every poll. A worker re-polls immediately while it keeps finding work, so a single busy tenant still drains quickly.
- `max_inflight` is a **soft** limit: concurrent claimers can overshoot it by up to workers x slice.
- Phase 3 extends the shape to tenants -> endpoints -> deliveries: inside a tenant, endpoints are visited in random order and each may only fill the slots left under its own `max_concurrency` (default 5; a half-open endpoint gets 1). Endpoints that are paused, disabled or have an open circuit are skipped, so their deliveries wait (and are not scanned). The earlier per-tenant index is replaced by `deliveries_endpoint_due_idx (endpoint_id, next_attempt_at) WHERE pending`.
- Measured with `BenchmarkClaimParallel` (50 tenants x 5 endpoints, 150k due deliveries, one tenant owning two thirds): every claim returns the full 20 rows, 0.86ms per claim single-threaded and ~0.22ms effective with 8 parallel claimers (about 90k deliveries/s of claim throughput on a laptop Postgres). `EXPLAIN ANALYZE` on 499k deliveries: index scan on `deliveries_endpoint_due_idx`, `LockRows` locking exactly the returned rows, ~1ms.
- Known characteristics: (a) cost grows with the number of *tenants and endpoints* probed per claim, not with deliveries; thousands of tenants is fine, hundreds of thousands would need a tenant-queue table. (b) When a single tenant has all the work a claim returns only its slice (5 rows) and the worker re-polls immediately: fairness is paid for with more round trips.

Multiple workers never get the same row. Crashed worker: the lease expires and the reaper returns the row to `pending` (or to the DLQ with `lease_expired` if all attempts are spent, so a payload that crashes workers cannot loop forever). The lost attempt is counted and logged in `attempts`, since the request may have reached the receiver. This is why the guarantee is *at-least-once*. A stale worker that finishes after its lease was reclaimed cannot overwrite the new outcome (`locked_by` guard).

### Retry policy (defaults, configurable)

- An explicit schedule of nominal delays after each failed attempt: `30s, 5m, 30m, 2h, 5h, 8h, 8h` (`RETRY_SCHEDULE`), 8 attempts in total (`MAX_ATTEMPTS`), spanning at most about 24h.
- **Equal jitter**: each delay is uniform in `[nominal/2, nominal]`. The original draft used full jitter (`rand(0, min(cap, base*2^n))`) with base 30s, but that arithmetic only spans ~2h and full jitter can collapse a backoff to near-zero; the schedule above actually delivers the intended ~24h.
- `Retry-After` is honored on 429 and 503 only: the wait becomes at least that long, capped at `RETRY_AFTER_MAX` (6h). It is ignored on other statuses.
- Attempts are exhausted when `attempt_count >= max_attempts` (stored per delivery at creation); the delivery then moves to the DLQ with reason `max_attempts`.
- Per-tenant/endpoint overrides are not implemented; settings are global.

### Response classification

| Result | Action |
|---|---|
| 2xx | success |
| 410 Gone | pause the endpoint, send the delivery to DLQ with `endpoint_gone` |
| 3xx | not followed; treated as failure |
| 429 / 5xx / 408 / network error / timeout | retry |
| Other 4xx | retry (many receivers return transient 4xx during deploys), counts toward the circuit breaker |

Timeouts: 10s connect+response total, response body read capped at 64KB (we only store a 1KB snippet).

### Circuit breaker and isolation

Implemented as described in `architecture.md` section 6. Defaults: open after 20 consecutive failures (`BREAKER_THRESHOLD`), first open window 30s doubling up to 1h (`BREAKER_BASE_OPEN`, `BREAKER_MAX_OPEN`), auto-disable after 72h of continuous failure (`AUTO_DISABLE_AFTER`). Notes:
- 429s and local (our-side) failures do not count against the endpoint.
- Failures of attempts that were already in flight when the circuit opened do not extend the window.
- "Notify the tenant" on auto-disable is limited to exposing `status`, `disabled_at`, `disabled_reason` and a `health` block on the endpoint in the API; there is no outbound notification channel yet (candidate for Phase 4: a tenant-level webhook for operational events).

### Multi-tenancy and fairness

- Every table carries `tenant_id`; every query is tenant-scoped (enforced in the store layer, plus Postgres row-level security as defense in depth).
- **Noisy-neighbor protection**: a tenant that floods events must not delay others.
  - Ingest: per-tenant token-bucket rate limit and `max_pending` backlog quota (429 when exceeded).
  - Delivery: per-tenant `max_inflight` cap, and claim batches take at most K due rows per tenant (round-robin across tenants with due work). Implemented as a two-step claim (pick candidates with `row_number() OVER (PARTITION BY tenant_id)`, then lock them with `SKIP LOCKED`); exact SQL gets prototyped and benchmarked in Phase 2.
- Metrics and logs are labeled by tenant (bounded cardinality: top-N tenants plus an "other" bucket).
- Suspending a tenant pauses its deliveries without deleting data.

## 6. Dead Letter Queue

- A delivery enters the DLQ when attempts are exhausted or on a permanent condition (`endpoint_gone`, `endpoint_disabled`, `payload_too_large`).
- Stored in place (`status='dead'`, `dead_reason`, `dead_at`) with its complete attempt history.
- API:
  - `GET /v1/dlq?endpoint_id=&reason=&since=`
  - `POST /v1/deliveries/{id}/replay`
  - `POST /v1/dlq/replay` (bulk by filter; rate-limited so replays don't swamp the endpoint)
- Retention: 30 days (configurable), then purged. Metric and alert on DLQ growth rate.
- Replay resets `attempt_count`, sets `status='pending'`, and links an audit record.

## 7. Security

Implemented in Phase 3; the full description is in `architecture.md` sections 6 and 7. In short: HMAC signing with multi-secret rotation, envelope-encrypted secrets at rest (AES-256-GCM, KEK keyring, AAD-bound), SSRF protection enforced at connect time on the resolved IP, production-only https, hashed API keys, strict request limits, and Postgres row-level security with separate API and worker roles.

## 8. Scalability & Reliability

- API and workers scale independently behind a load balancer / HPA.
- Workers pull in batches, so throughput scales with worker count until Postgres becomes the limit (index-only claim on the partial index; expect low thousands/sec on modest hardware).
- Fan-out on write means big fan-outs are one insert batch; for huge fan-outs (>1000 endpoints) chunk the inserts.
- Retention jobs delete old `succeeded` deliveries/attempts in batches (or use time-partitioned tables) to keep the hot table small.
- Graceful shutdown: stop claiming, finish in-flight within a deadline, release leases of anything unfinished.
- Postgres failure: API returns 503 (producers retry using their idempotency key); workers back off. No data loss because nothing is acknowledged before commit.
- Backpressure: API rejects with 429 when pending-backlog per tenant exceeds a threshold.

## 9. Known Trade-offs (please confirm)

1. **At-least-once, not exactly-once.** Receivers must dedupe on `webhook-id`.
2. **No ordering guarantee.** Retries reorder events. If ordering matters, we would add a per-endpoint (or per-key) FIFO mode later, which reduces throughput.
3. **Postgres as queue** caps scale at roughly low-thousands deliveries/sec per cluster; the `Queue` interface leaves an escape hatch.
4. **Fan-out on write** costs storage but makes per-endpoint state, retries, and DLQ trivially independent.

## 10. Observability

- Metrics (as implemented, with the final names and semantics, in `architecture.md` section 8): `webhook_deliveries_total{outcome}`, `webhook_attempt_duration_seconds`, `webhook_first_attempt_delay_seconds`, `webhook_queue_depth`, `webhook_oldest_pending_age_seconds` (the key SLO signal), `webhook_dlq_depth`, `webhook_inflight_deliveries`, `webhook_endpoints{state}`, `webhook_lease_reclaims_total`, plus API and quota metrics.
- Structured logs with `delivery_id`, `event_id`, `endpoint_id`, `attempt`.
- Traces: API ingest to worker attempt (trace context stored on the delivery row).
- Alerts: oldest pending > 5 min, DLQ growth, reaper reclaim spike (signals crashing workers).

## 11. Implementation Plan

Each phase ends in something runnable and tested.

### Phase 0: Scaffold (small) — DONE
- [x] Repo layout: `cmd/{api,worker,migrate}`, `internal/{store,config,logging,server}`, `migrations/`, `docs/`, `Dockerfile`, `docker-compose.yml`, `Makefile`.
  - Packages `queue`, `deliver`, `sign`, `ssrf` are created in the phases that implement them.
- [x] Postgres + goose migrations for the 6 tables; config; logging; health endpoints (`/healthz`, `/readyz`).
- [x] GitHub Actions CI written: vet, migration up/down/up, race tests, lint, `govulncheck`, container builds.
  - CI is green on GitHub (after bumping Go to 1.27.3 for stdlib vulnerability fixes and fixing two lint issues).
  - Integration tests (testcontainers) start in Phase 1; Phase 0 has unit tests only.
- [x] Verified locally: build, vet, tests, Compose stack healthy, migration round-trip, graceful shutdown.

### Phase 1: Core happy path — DONE (not yet pushed/CI-verified)
- [x] Endpoint CRUD (keyset pagination, pause/resume); `POST /v1/events` with transactional fan-out and `Idempotency-Key`; `GET /v1/events/{id}/deliveries`. Spec in `api/openapi.yaml`.
- [x] Worker: claim with SKIP LOCKED, send HTTP, record attempt, mark `succeeded`. Lease-guarded completion so a stale worker can't overwrite a reclaimed delivery.
- [x] Standard Webhooks HMAC signing (tested against the spec's vector); mock receiver in compose that verifies signatures.
- [x] Minimum auth pulled forward from Phase 3: API-key auth (SHA-256 hashed), `cmd/admin` to create tenants and keys, tenant-scoped queries.
- [x] Integration tests against real Postgres (a fresh database per test, via `DATABASE_URL`) instead of testcontainers: claim exclusivity under 8 concurrent workers, tenant isolation, idempotency, fan-out, end-to-end delivery.
- [x] **Exit met**: event posted → delivered exactly once to 3 endpoints with verified signatures (integration test and live Docker run).
- Deviations: hand-written `pgx` queries instead of `sqlc`. Failed attempts use a **fixed 30s retry placeholder** (no max attempts, no DLQ) until Phase 2.
- Deferred: fair per-tenant claiming and its benchmark move to Phase 2 with the rest of the claim-query work.

### Phase 2: Retries and DLQ — DONE
- [x] Retry policy: configurable schedule with equal jitter, `Retry-After` on 429/503, response classification (410 → DLQ + pause endpoint), max attempts → `dead`.
- [x] Lease reaper for crashed workers (requeues, or dead-letters with `lease_expired` once attempts are spent); lost attempts are recorded.
- [x] DLQ APIs: `GET /v1/dlq` (filters: endpoint, reason, since; keyset pagination), `GET /v1/deliveries/{id}` (with attempt history), `POST /v1/deliveries/{id}/replay`, `POST /v1/dlq/replay` (bulk, rate-limited by staggering due times). Replays reset the attempt budget and are audited in `delivery_replays`.
- [x] Fair per-tenant claiming with `max_inflight`, and benchmark (see "Claiming" above). Paused endpoints hold their deliveries.
- [x] **Exit met**:
  - Integration tests: retries until success, exhaustion → DLQ → replay → delivered, 410 handling, Retry-After, crashed-worker recovery, tenant fairness, in-flight cap, reaper.
  - Live Docker run: worker `kill -9`'d mid-delivery, the delivery was recovered after lease expiry and delivered on attempt 2 (receiver saw it twice: at-least-once).
- Deviation: per-endpoint concurrency cap stays in Phase 3 with the circuit breaker (it needs endpoint-level state alongside the breaker).
- Gap closed in Phase 4: workers now drain and hand back unfinished leases on shutdown.

### Phase 3: Isolation & protection — DONE
- [x] Per-endpoint concurrency cap (in the claim query), circuit breaker with half-open single probe, auto-disable that dead-letters waiting deliveries (`endpoint_disabled`); re-enabling via `PATCH status=active` resets health.
- [x] SSRF guard enforced in the dialer on the resolved IP (covers DNS rebinding), https-only at send time in production, registration-time host checks, no redirects/proxies.
- [x] Secret rotation API (grace period with dual signatures, hard rotation, revoke, live-secret cap).
- [x] Encrypted secrets at rest (envelope encryption, keyring with key rotation, AAD-bound), `admin encrypt-secrets` for legacy plaintext and re-sealing, `admin gen-secret-key`.
- [x] Row-level security with `webhooks_api` / `webhooks_worker` roles; API key lookup via a SECURITY DEFINER function; API and worker integration tests run under the restricted roles.
- [x] Request limits: added a request-header cap to the existing payload/response/timeout limits. `admin revoke-key`.
- [x] **Exit met** (`TestBlackHoleEndpointDoesNotSlowHealthyEndpoints`, repeatable): 50 healthy deliveries next to a black-hole endpoint (1s timeout, worker capacity 10) finish in ~130-150ms with the cap versus 4-7s without it, where the black hole holds all 10 worker slots.
- Deviations / notes:
  - A hard rotation (`grace_period: 0s`) retires *every* earlier secret, including ones still in a previous grace window, because that is what an operator wants after a compromise (found by a test).
  - The breaker deliberately ignores 429 and local failures.
  - `max_concurrency` (default 5) is settable per endpoint on create/PATCH, bounded to 1-100; the tenant's `max_inflight` still applies on top.
  - Role switching via `SET ROLE` is a convenience for Compose/dev; production should use login users that only hold the restricted role (documented in `architecture.md`).
  - Migration 00004 creates cluster-wide roles, so the migrating user needs `CREATEROLE`.

### Phase 4: Observability & ops — DONE
- [x] Prometheus metrics (API, worker, queue state), separate metrics port; structured access logs with request and trace IDs.
- [x] OpenTelemetry traces: one trace from the producer's request through the ingest transaction to every delivery attempt, via trace context stored on the delivery row. Verified in Jaeger on the live stack and by a test.
- [x] Grafana dashboard (25 panels) and 23 Prometheus rules (SLO recording rules, multi-burn-rate error-budget alerts, latency, DLQ, reclaim, availability), all provisioned in Compose (`make up-obs`). Alert rules are unit-tested with `promtool` (`make test-alerts`, also in CI); every dashboard query was run against live Prometheus.
- [x] Kubernetes: kustomize base in `deploy/k8s` (hardened Deployments, PDBs, HPAs on queue depth/age/CPU, default-deny NetworkPolicies with a private-range-blocking worker egress, migration Job). Schema-validated against Kubernetes 1.30 in CI (`make validate-k8s`). Later applied to a live kind cluster (Kubernetes 1.37, Calico): see `docs/kubernetes-verification.md`.
- [x] Tenant quotas: shared-across-replicas ingest rate limit (token bucket in Postgres with leased batches) and backlog quota, both returning 429 + `Retry-After` and failing open; `admin set-limits`.
- [x] Retention/purge job; graceful shutdown (readiness drain, bounded drain, lease release); readiness-aware rolling updates.
- [x] **Exit checks** (live Docker run): 350 events × 5 endpoints flowed through with metrics, dashboard queries and a connected Jaeger trace; a 60-request flood against a 5/s tenant got 12 accepted and 48 `429`s; stopping a worker with 5 deliveries in flight aborted and released all 5 in 3s.
- Deviations and notes:
  - **No per-tenant metric labels** (the earlier design said "top-N plus other"). That needs a grouped scan of the whole backlog on every scrape; tenant detail is in logs and traces instead.
  - **`oldest_pending_age` excludes customer-side waiting** (paused endpoints, open circuits, endpoints or tenants at their concurrency caps). The first version counted a deliberately slow customer endpoint as platform latency and would have paged; found in the live run and fixed, with a test.
  - Rate limiting is accurate across replicas without Redis by leasing token batches from Postgres, at the price of slack of about one lease per replica.
  - Retention adds three indexes (a little write cost); partitioning remains the scale-out path.
  - A bug in my first alert rules was caught by the rule tests: `count(up == 1) == 0` never fires when targets exist but are down.
  - Fixed a flaky tamper test in `secretbox` (flipping a base64 padding bit changes nothing).

### Phase 5: Verification at scale — DONE
Full methodology, results, caveats and limits are in `docs/performance.md`.
- [x] Load tool (`cmd/loadtest`): real `api`/`worker` processes, open-loop generator, fault-injecting receiver, throwaway database per run, latency measured independently by the receiver's clock and by the workers' histogram.
- [x] **Target met**: 2,000 deliveries/s sustained with p99 18 ms request-to-first-receipt (24 ms by the workers' own histogram); 4,000/s sustained with p99 319 ms. Above ~4k/s on this shared 10-core machine it degrades gracefully (backlog builds and drains, nothing lost).
- [x] Chaos, against real processes: `kill -9` of workers and APIs, database silent-stall and reset partitions (TCP fault proxy), Postgres clean restart and `SIGKILL` crash recovery, a receiver that fails with 5xx, hangs, resets, non-HTTP garbage, 5 MB bodies and black holes, and all of it at once.
- [x] Invariant checker (`internal/harness/invariants`, 7 properties, each tested to be able to fail): **held in all 10 runs**, including `mixed` (280,170 deliveries, 6 process kills, 2 partitions, a database restart and crash). Duplicates are counted: 0 clean, 13 after five worker crashes, ~2,300 with slow receivers.
- [x] In-process version in `go test` (`TestInvariantsHoldUnderFailuresAndCrashes`, mutation-tested against a disabled reaper) and a short real-process smoke run in CI (`loadtest-smoke`).
- **Six defects found and fixed** (all with regression tests): per-delivery connection churn (capped throughput at ~650/s and dead-lettered healthy deliveries); idle-poll latency (now `LISTEN/NOTIFY`, p99 ~600 ms to ~20 ms); `NOTIFY` inside the ingest transaction serializing commits (capped ingest at ~600/s; now issued after commit, coalesced); a worker flag that silently never reached the binary; API requests hanging for the length of a database stall (now a 5 s deadline and `503 + Retry-After`); services crashing instead of waiting out a database blip at startup (now retry for 60 s).
- CI is green for Phases 4-5 on a clean runner, including the `ops` job (alert rules, Kubernetes schema validation) and the `loadtest-smoke` job (real processes, worker kills, receiver faults, invariants).
- Not covered (see `docs/performance.md`): real hardware, TLS, long soak runs, thousands of tenants end to end, replica failover and storage faults.

### Later / optional
- Redis for shared rate limiting; per-endpoint ordered mode; event replay by time range; customer portal UI; swapping queue backend (Kafka/SQS) behind the interface.

## 12. Testing Strategy

- Unit: backoff math, classification, signing (against Standard Webhooks test vectors), SSRF IP filter.
- Integration (testcontainers Postgres): claim exclusivity under concurrency, lease reclaim, idempotent ingest, DLQ replay.
- End-to-end in Docker Compose with a configurable flaky receiver.
- Property/invariant tests under chaos (Phase 5).

## 13. Open Questions for You

## 13. Decisions

1. **Stack: Go + PostgreSQL.** Go's concurrency model and static binaries suit a high-fan-out HTTP sender; Python would work but needs more effort to reach the same throughput and concurrency control.
2. **Purpose: portfolio project, built to production standards.** All phases (security, observability, load/chaos testing) are in scope, plus CI and Kubernetes manifests.
3. **Multi-tenant: yes**, with API keys, quotas, and fairness (see section 5).
4. **Scale: Postgres queue, not Kafka.** Rationale: Kafka handles per-message delayed retries poorly (retry-topic tiers, partition head-of-line blocking) and would still need Postgres for history. A well-built Postgres queue shows deeper engineering and is the right tool up to low-thousands/sec. The load test in Phase 5 will publish measured numbers, and the `Queue` interface plus a documented scale-out path (partitioned tables, read replicas for queries, sharding by tenant) covers growth beyond that.
5. **Retry policy**: retry every non-2xx except 410 (confirmed).
6. **Ordering**: unordered, documented (default).
