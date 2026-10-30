# Webhook Delivery

Multi-tenant webhook delivery service with retries and a dead letter queue. See [docs/architecture.md](docs/architecture.md) for the design and [docs/plan.md](docs/plan.md) for the build plan.

**Status:** Phases 0-5 complete: events fan out to subscribed endpoints and are delivered signed, with scheduled retries, a dead letter queue with replay, crash recovery, per-endpoint isolation (concurrency caps and a circuit breaker), SSRF protection, encrypted and rotatable secrets, and database-enforced tenant isolation. Phase 4 adds metrics, tracing, dashboards and alerts, Kubernetes manifests, tenant quotas, retention and graceful shutdown. Phase 5 adds a load and chaos tool with a database invariant checker (see [docs/performance.md](docs/performance.md)).

## Run locally

```sh
make up                      # postgres, migrations, api :8080, worker :8081, mockreceiver :9000
export DATABASE_URL='postgres://webhooks:webhooks@localhost:5432/webhooks?sslmode=disable'

TENANT=$(go run ./cmd/admin create-tenant acme)
KEY=$(go run ./cmd/admin create-key "$TENANT")        # shown once

# The mock receiver verifies signatures with this dev secret.
curl -s localhost:8080/v1/endpoints -H "Authorization: Bearer $KEY" -d '{
  "url": "http://mockreceiver:9000/hook/a", "event_types": ["order.created"],
  "secret": "whsec_ZGV2LW9ubHktcmVjZWl2ZXItc2VjcmV0LTAxMjM0NTY="}'

curl -s localhost:8080/v1/events -H "Authorization: Bearer $KEY" -H 'Idempotency-Key: demo-1' \
  -d '{"type": "order.created", "payload": {"order_id": 42}}'

docker compose logs mockreceiver     # shows verified=true
make down
```

### Try failures, the DLQ and crash recovery

The mock receiver's behavior is chosen by hook name: `/hook/fail*` returns 500, `/hook/gone*` 410, `/hook/limited*` 429 + `Retry-After`, `/hook/slow*` answers after 6s. Start the stack with a fast retry policy:

```sh
MAX_ATTEMPTS=3 RETRY_SCHEDULE=1s,2s DELIVERY_TIMEOUT=8s LEASE_DURATION=10s REAPER_INTERVAL=2s make up
# register endpoints at http://mockreceiver:9000/hook/fail1 and /hook/slow1, publish an event, then:
docker compose kill -s KILL worker && docker compose start worker   # crash mid-delivery; it is recovered after the lease expires

curl -s localhost:8080/v1/dlq -H "Authorization: Bearer $KEY"                        # dead deliveries
curl -s localhost:8080/v1/deliveries/$ID -H "Authorization: Bearer $KEY"             # attempt history
curl -s -X POST localhost:8080/v1/deliveries/$ID/replay -H "Authorization: Bearer $KEY"
curl -s localhost:8080/v1/dlq/replay -H "Authorization: Bearer $KEY" -d '{"reason":"max_attempts","rate_per_second":5}'
```

### Load and chaos testing

```sh
make loadtest                      # 400 events/s x 5 endpoints = 2,000 deliveries/s for 60s
make loadtest SCENARIO=mixed       # ...while crashing workers and APIs, partitioning and crashing Postgres, and abusing the receiver
./bin/loadtest -h                  # rate, fan-out, tenants, process counts, durations
```

`cmd/loadtest` creates a throwaway database, runs real `api` and `worker` processes (so `kill -9` is a genuine crash), sends events at a fixed arrival rate, and afterwards checks the database invariants: every accepted event exists with one delivery per endpoint, every delivery is `succeeded` or `dead` (none stuck), nothing marked succeeded was lost, and no lease is left behind. Scenarios: `none`, `kill-workers`, `kill-api`, `db-restart`, `db-partition`, `receiver-faults`, `mixed`. Results, methodology and the bugs this found are in [docs/performance.md](docs/performance.md). A short version runs in CI; `TestInvariantsHoldUnderFailuresAndCrashes` is the in-process equivalent that runs with `go test`.

### Observability, quotas and operations

```sh
make up-obs      # everything plus Prometheus :9090, Grafana :3000 (dashboard preloaded), Jaeger :16686
make test-alerts # unit-test the Prometheus alert rules (promtool)
make validate-k8s
```

- **Metrics** are served on `:9090` inside each service (a separate port from the API, never exposed by the ingress). **Traces**: set `OTEL_EXPORTER_OTLP_ENDPOINT` (`make up-obs` does). **Logs** carry `request_id`, `tenant_id` and `trace_id`.
- **Quotas**: each tenant has `events_per_sec` (burst = 2s worth), `max_pending` and `max_inflight`. Over quota, `POST /v1/events` returns `429` with `Retry-After`. Set them with `go run ./cmd/admin set-limits <tenant-id> <events/s> <max-pending> <max-inflight>`.
- **Retention**: `RETAIN_SUCCEEDED` (7d), `RETAIN_DEAD` (30d), `RETAIN_EVENTS` (7d), `PURGE_INTERVAL` (5m; `0s` disables). An `Idempotency-Key` is forgotten when its event is purged.
- **Shutdown**: readiness fails for `SHUTDOWN_DRAIN_DELAY` (3s), then workers drain for `WORKER_DRAIN_TIMEOUT` (20s) and hand back unfinished deliveries. Keep Compose/Kubernetes grace periods longer (Compose is set to 45s).
- Kubernetes: see [deploy/k8s/README.md](deploy/k8s/README.md).

### Secret rotation, security and isolation

```sh
# Rotate with a 24h grace period: deliveries carry signatures for both secrets, so receivers can switch with no downtime.
curl -s -X POST localhost:8080/v1/endpoints/$EP/secrets/rotate -H "Authorization: Bearer $KEY" -d '{"grace_period":"24h"}'
curl -s localhost:8080/v1/endpoints/$EP/secrets -H "Authorization: Bearer $KEY"        # ids and expiry, never values
curl -s -X DELETE localhost:8080/v1/endpoints/$EP/secrets/$SECRET_ID -H "Authorization: Bearer $KEY"
# After a compromise: retire every previous secret immediately.
curl -s -X POST localhost:8080/v1/endpoints/$EP/secrets/rotate -H "Authorization: Bearer $KEY" -d '{"grace_period":"0s"}'

curl -s localhost:8080/v1/endpoints/$EP -H "Authorization: Bearer $KEY"   # status, max_concurrency and the circuit breaker's `health`
curl -s -X PATCH localhost:8080/v1/endpoints/$EP -H "Authorization: Bearer $KEY" -d '{"status":"active"}'   # re-enable an auto-disabled endpoint and reset its breaker
```

Production needs real settings (development defaults are deliberately insecure and the services log a warning):

| Setting | Purpose |
|---|---|
| `APP_ENV=prod` | https-only endpoints, no private/loopback delivery targets, JSON logs, requires `SECRET_ENCRYPTION_KEYS` |
| `SECRET_ENCRYPTION_KEYS` | `id:base64key[,id:base64key...]`; first is active, others only decrypt. Generate with `go run ./cmd/admin gen-secret-key k1`. After rotating keys run `go run ./cmd/admin encrypt-secrets` |
| `DB_ROLE` | role the service runs as (`webhooks_api` / `webhooks_worker` by default; `none` to disable). See `docs/architecture.md` for using restricted login users |
| `ALLOW_PRIVATE_TARGETS` | override the private-target restriction (default: on only in dev) |
| `BREAKER_THRESHOLD`, `BREAKER_BASE_OPEN`, `BREAKER_MAX_OPEN`, `AUTO_DISABLE_AFTER` | circuit breaker (20, 30s, 1h, 72h) |

Other admin commands: `admin revoke-key <prefix>`, `admin gen-secret-key <id>`, `admin encrypt-secrets`. The migrating database user needs `CREATEROLE` (migration 00004 creates the two restricted roles).

Retry settings (all optional): `MAX_ATTEMPTS` (8), `RETRY_SCHEDULE` (`30s,5m,30m,2h,5h,8h,8h`), `RETRY_AFTER_MAX` (6h), `WORKER_TENANT_SLICE` (5), `LEASE_DURATION`, `REAPER_INTERVAL`.

API reference: [api/openapi.yaml](api/openapi.yaml).

Without Docker for the Go services: start Postgres with `docker compose up -d postgres`, then `make migrate-up run-api`.

## Layout

```
cmd/{api,worker,migrate}   entrypoints
cmd/{admin,mockreceiver}   bootstrap CLI and dev webhook receiver
api/openapi.yaml           API spec
internal/api               /v1 handlers + API-key auth
internal/worker            claim -> send -> retry/DLQ decision loop, lease reaper
internal/retry             backoff schedule and jitter
internal/deliver           signed HTTP sender
internal/sign              Standard Webhooks HMAC signing
internal/auth              API key generation/hashing
internal/secretbox         envelope encryption of endpoint secrets
internal/ssrf              blocked-address policy enforced at connect time
internal/config            env-based configuration
internal/logging           slog setup
internal/server            HTTP shell + /healthz, /readyz
internal/store             Postgres queries (events, endpoints, queue, stats, retention)
internal/metrics           Prometheus metric definitions
internal/telemetry         OpenTelemetry setup and trace-context propagation
internal/ratelimit         per-tenant ingest quotas
internal/testutil          per-test Postgres databases
internal/harness/          verification tooling, kept apart from the service code
  invariants               database invariant checker (used by tests and the load tool)
  loadtest                 load generator, fault-injecting receiver, soak sampler, scenarios
  chaosproxy               TCP proxy that simulates network partitions
cmd/loadtest               load and chaos test driver
migrations/                goose SQL migrations (embedded)
deploy/                    prometheus rules, grafana dashboard, kubernetes manifests
docs/                      architecture.md, plan.md
```

## Checks

`make vet test lint`; `make bench` runs the claim-query benchmark. `make test` starts Postgres via Compose; integration tests are skipped if `DATABASE_URL` is unset. Lint needs `golangci-lint`.
