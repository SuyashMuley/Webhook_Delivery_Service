# Architecture

A multi-tenant, horizontally scalable webhook delivery service with at-least-once delivery, exponential-backoff retries, and a dead letter queue (DLQ). See `plan.md` (same folder) for the build plan and open trade-offs.

## 1. System Context

```mermaid
flowchart LR
    P[Producer services] -- "POST /v1/events (API key)" --> API
    T[Tenant admins / CLI] -- "endpoint, DLQ, replay APIs" --> API
    subgraph WDS[Webhook Delivery System]
        API[API service]
        W[Worker service]
        DB[(PostgreSQL)]
        API --> DB
        W <--> DB
    end
    W -- "HTTPS POST, HMAC-signed" --> R[Customer endpoints]
    W -. metrics / traces .-> O[Prometheus · Grafana · OTel]
    API -. metrics / traces .-> O
```

## 2. Components

| Component | Responsibility | State | Scales by |
|---|---|---|---|
| **API service** (`cmd/api`) | Auth, tenant quotas, endpoint/subscription CRUD, event ingest with fan-out, DLQ inspection and replay | Stateless | More replicas behind a load balancer |
| **Worker service** (`cmd/worker`) | Claim due deliveries, send HTTP, record attempts, schedule retries, move to DLQ, reclaim expired leases, run retention jobs | Stateless | More replicas (HPA on oldest-pending age) |
| **PostgreSQL** | Source of truth **and** the queue (`deliveries` table); attempt history; DLQ | Stateful | Vertical, then partitioning, read replicas, tenant sharding |
| **Observability stack** | Prometheus metrics, Grafana dashboards, OpenTelemetry traces, structured logs | External | Standard |

Internal packages (`internal/`): `store` (sqlc-generated queries, tenant scoping), `queue` (claim/complete/retry interface), `deliver` (HTTP sender, classification, backoff), `sign` (HMAC), `ssrf` (IP filtering dialer), `auth`, `ratelimit`, `config`, `telemetry`.

## 3. Key Design Decision: Postgres as the Queue

Deliveries are rows. The queue, the retry scheduler, and the DLQ are all views of one table.

- **Transactional enqueue**: event and delivery rows commit together, so there is no dual-write gap between "stored" and "queued".
- **Distributed claiming**: `FOR UPDATE SKIP LOCKED` lets any number of workers pull disjoint batches without coordination.
- **Delayed retry**: a retry is `UPDATE ... SET next_attempt_at = ...`; no separate delay queue.
- **DLQ**: `status = 'dead'`, queryable with SQL and replayable with one update; attempt history is preserved.
- **Escape hatch**: all queue operations go through a `Queue` interface, so the backend can be swapped (e.g. Kafka/SQS) if throughput outgrows Postgres.

Kafka was rejected for v1: per-message delayed retries need retry-topic tiers and suffer partition head-of-line blocking, and Postgres would still be required for history and DLQ queries.

## 4. Data Model

```mermaid
erDiagram
    TENANTS ||--o{ API_KEYS : has
    TENANTS ||--o{ ENDPOINTS : owns
    TENANTS ||--o{ EVENTS : produces
    EVENTS ||--o{ DELIVERIES : "fans out to"
    ENDPOINTS ||--o{ DELIVERIES : receives
    DELIVERIES ||--o{ ATTEMPTS : records

    TENANTS { uuid id PK; text status; int max_inflight; int events_per_sec; int max_pending }
    API_KEYS { uuid id PK; uuid tenant_id FK; text key_hash; text[] scopes; timestamptz revoked_at }
    ENDPOINTS { uuid id PK; uuid tenant_id FK; text url; text[] event_types; jsonb secrets_sealed; text status; int max_concurrency; int consecutive_failures; int circuit_trips; timestamptz circuit_open_until; timestamptz failing_since; text disabled_reason }
    EVENTS { uuid id PK; uuid tenant_id FK; text type; jsonb payload; text idempotency_key }
    DELIVERIES { uuid id PK; uuid event_id FK; uuid endpoint_id FK; text status; int attempt_count; timestamptz next_attempt_at; timestamptz lease_expires_at; text dead_reason }
    ATTEMPTS { uuid id PK; uuid delivery_id FK; int attempt_no; int status_code; int duration_ms; text error }
```

Hot-path indexes: partial `(next_attempt_at) WHERE status='pending'`, partial `(lease_expires_at) WHERE status='in_flight'`, `(endpoint_id, status)`, unique `(tenant_id, idempotency_key)`.

## 5. Data Flows

### 5.1 Ingest

```mermaid
sequenceDiagram
    participant P as Producer
    participant A as API
    participant D as Postgres
    P->>A: POST /v1/events (API key, idempotency key)
    A->>A: authenticate, rate-limit, backlog quota
    A->>D: BEGIN
    A->>D: INSERT event (ON CONFLICT idempotency_key → return existing)
    A->>D: INSERT deliveries for each matching active endpoint
    A->>D: COMMIT
    A-->>P: 202 Accepted {event_id}
```

Nothing is acknowledged before commit. If Postgres is down the API returns 503 and the producer retries safely using the idempotency key.

### 5.2 Delivery attempt

```mermaid
sequenceDiagram
    participant W as Worker
    participant D as Postgres
    participant E as Customer endpoint
    loop poll
        W->>D: claim batch (SKIP LOCKED; fair per tenant; per-endpoint concurrency cap; skips paused/disabled endpoints and open circuits) → in_flight + lease
        par for each delivery (bounded concurrency)
            W->>W: decrypt secrets, sign with every live secret
            W->>E: POST (SSRF-checked dial, 10s timeout, no redirects)
            E-->>W: status / error
            W->>D: INSERT attempt; UPDATE delivery; update endpoint health (breaker)
        end
    end
```

Outcome handling after each attempt:

| Result | Delivery update |
|---|---|
| 2xx | `succeeded` |
| 410 | `dead` (`endpoint_gone`); pause endpoint |
| Other failure, attempts left | `pending`, `next_attempt_at = now + scheduled delay with equal jitter` (at least `Retry-After` on 429/503) |
| Other failure, exhausted | `dead` (`max_attempts`) → DLQ |

### 5.3 State machine

```mermaid
stateDiagram-v2
    [*] --> pending: created at ingest
    pending --> in_flight: claimed (lease)
    in_flight --> succeeded: 2xx
    in_flight --> pending: failure + attempts left\n(or lease expired → reaper)
    in_flight --> dead: exhausted / permanent
    dead --> pending: replay
    succeeded --> [*]
```

### 5.4 Failure recovery

- **Worker crash**: lease expires; reaper returns the row to `pending`. The request may already have reached the receiver, hence *at-least-once* (receivers dedupe on `webhook-id`).
- **Worker shutdown**: stop claiming, drain in-flight within a deadline, release remaining leases.
- **Postgres outage**: API returns 503; workers back off; no data loss.
- **Dead endpoint**: a 410 pauses the endpoint immediately. Circuit breaker and auto-disable for endpoints that merely fail arrive in Phase 3.

## 6. Isolation and Fairness

| Risk | Control | Status |
|---|---|---|
| Tenant floods events | Per-tenant token bucket at ingest; `max_pending` quota (429) | Phase 4 |
| Tenant dominates workers | Per-tenant `max_inflight` (soft); each claim takes at most a slice per tenant, tenants visited in random order | Done |
| Slow or dead endpoint hogs worker slots | Per-endpoint `max_concurrency` enforced in the claim query (default 5) | Done |
| Endpoint that keeps failing | Circuit breaker: opens after N consecutive failures, exponential open window, single half-open probe; auto-disable after a long failure streak | Done |
| Cross-tenant data access | `tenant_id` on every table and every query, **plus** Postgres row-level security (below) | Done |

**Circuit breaker.** State lives on the endpoint row (`consecutive_failures`, `circuit_trips`, `circuit_open_until`, `failing_since`). Only endpoint faults count: 429 (the receiver is alive, just busy) and failures on our side (undecryptable secrets) do not. While the circuit is open the endpoint is skipped by the claim query, so its deliveries simply wait and do not burn their attempts. When the window passes the endpoint is half-open: concurrency is forced to 1 so a single probe goes out. A successful delivery closes the circuit and resets everything; a failed probe re-opens it for twice as long, up to a cap. If an endpoint fails continuously for `AUTO_DISABLE_AFTER` (default 72h) it is disabled and its waiting deliveries are dead-lettered as `endpoint_disabled`, where they can be replayed after the tenant fixes the endpoint and sets `status: active` (which also resets the breaker).

**Row-level security.** Two NOLOGIN group roles carry the privileges, and the API and worker connections run as them (`SET ROLE` on every pooled connection):

| Role | Sees | Can do |
|---|---|---|
| `webhooks_api` | only the tenant in the transaction-local `app.tenant_id` (no tenant set: no rows) | CRUD endpoints, insert events, replay deliveries; **no access to `tenants` or `api_keys`** |
| `webhooks_worker` | all tenants (it delivers for everyone) | claim and update deliveries/endpoints, insert attempts; cannot delete endpoints, insert events or read keys |

Every tenant-facing store method runs in a transaction that first sets `app.tenant_id`, so a query that forgets its own `WHERE tenant_id = ...` returns nothing instead of leaking. API-key lookup happens before the tenant is known, so it goes through a `SECURITY DEFINER` function (`authenticate_api_key`) that the API role may execute. The table owner (migrations, `admin` CLI) is exempt. In production, connect with a login user that holds only the restricted role (or `DB_ROLE=none`), since a session that merely `SET ROLE`d can `RESET ROLE`.

## 7. Security Architecture

- **AuthN**: per-tenant API keys; only SHA-256 hashes stored; prefix shown for identification; revocable (`admin revoke-key`). Lookup goes through a definer function so the API role never reads the key table.
- **Payload integrity**: HMAC-SHA256 over `{id}.{timestamp}.{body}` (Standard Webhooks headers); timestamp enables replay rejection.
- **Secret rotation**: an endpoint holds several secrets. Rotating (`POST /v1/endpoints/{id}/secrets/rotate`) adds a new one and keeps the old one signing for a grace period (default 24h, max 7d), and while both are live every delivery carries a signature for each, so a receiver can switch at any point in that window with no failed deliveries. `grace_period: "0s"` is the hard rotation for a compromise: it retires every previous secret immediately. At most 5 live secrets; the last one cannot be revoked.
- **Secrets at rest**: envelope encryption (`internal/secretbox`). Each secret is AES-256-GCM encrypted under its own random data key, which is wrapped by a key-encryption key from a keyring; the ciphertext is bound (AAD) to its endpoint and secret id, so a sealed secret copied onto another endpoint will not open. Keys come from `SECRET_ENCRYPTION_KEYS` (`id:base64`, first is active, older ones kept for reading); `admin encrypt-secrets` migrates plaintext secrets and re-seals after a key rotation. The `Sealer` interface is where a cloud KMS plugs in. Production refuses to start without keys; development falls back to a public dev key and logs a warning.
- **SSRF** (`internal/ssrf`, `internal/deliver`): the check runs in the dialer's `Control` hook on the IP actually being connected to, after DNS resolution, so it cannot be bypassed by a hostname that resolves (or later rebinds) to an internal address. Blocked: loopback, RFC 1918, link-local including the 169.254.169.254 metadata service, CGNAT, multicast/reserved, IPv6 ULA/link-local, IPv4-mapped forms, NAT64 and 6to4. Production also refuses plain `http` at send time, never follows redirects, and ignores proxy environment variables. Registration rejects obviously internal hosts for fast feedback, but that is advisory. `ALLOW_PRIVATE_TARGETS` (default on only in dev) lifts the restrictions.
- **Limits**: 256KB event payload, 16KB request bodies on management endpoints, 32KB request headers, 64KB response read, 10s delivery timeout, API server read/write/idle timeouts.
- **Container/runtime**: non-root distroless images, `govulncheck` in CI. Kubernetes NetworkPolicy and read-only filesystems arrive with the deployment manifests in Phase 4.

## 8. Observability

- **Metrics** (Prometheus): `deliveries_total{status}`, `attempt_duration_seconds`, `queue_depth`, `oldest_pending_age_seconds` (primary SLO signal), `dlq_depth`, `inflight`, `circuit_open_endpoints`, `lease_reclaims_total`. Tenant label limited to top-N plus `other`.
- **Tracing** (OTel): ingest → delivery attempt linked via trace context stored on the delivery row.
- **Logging**: structured JSON with `tenant_id`, `event_id`, `delivery_id`, `endpoint_id`, `attempt`.
- **Alerts**: oldest pending > 5 min, DLQ growth rate, reaper reclaim spike, error-budget burn on delivery success rate.

## 9. Deployment Topology

```mermaid
flowchart TB
    LB[Load balancer / Ingress] --> API1[api pod] & API2[api pod]
    subgraph K8s
        API1 & API2
        W1[worker pod] & W2[worker pod] & W3[worker pod]
        HPA[HPA: oldest_pending_age, CPU]
    end
    API1 & API2 & W1 & W2 & W3 --> PGP[(Postgres primary)]
    PGP --> PGR[(Read replica: DLQ/history queries)]
    W1 & W2 & W3 -- egress via NetworkPolicy --> NET[Internet]
```

Local development: Docker Compose with Postgres, api, worker, a configurable flaky mock receiver, Prometheus, and Grafana.

## 10. Scalability Path

1. **Single Postgres** (v1): low thousands of deliveries/sec; measured in the Phase 5 load test.
2. **Read offload**: DLQ/history/inspection queries to replicas.
3. **Partitioning**: time-partition `attempts` and finished `deliveries`; drop partitions instead of bulk deletes.
4. **Tenant sharding**: route tenants to separate Postgres clusters (shard map in a small control-plane table).
5. **Queue swap**: replace the Postgres `Queue` implementation with Kafka/SQS behind the same interface if sustained throughput exceeds what sharded Postgres handles economically.

## 11. Guarantees and Non-Guarantees

| Guarantee | Notes |
|---|---|
| No accepted event is lost | Durable commit before `202` |
| At-least-once delivery | Duplicates possible; receivers dedupe on `webhook-id` |
| Bounded retries, then DLQ | 8 attempts over up to ~24h by default (`30s, 5m, 30m, 2h, 5h, 8h, 8h` with equal jitter) |
| Tenant isolation | Data and capacity |
| **Not guaranteed**: ordering | Retries reorder events; documented |
| **Not guaranteed**: exactly-once | Impossible over HTTP |
