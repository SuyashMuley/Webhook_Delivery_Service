# Webhook Delivery

Multi-tenant webhook delivery service with retries and a dead letter queue. See [docs/architecture.md](docs/architecture.md) for the design and [docs/plan.md](docs/plan.md) for the build plan.

**Status:** Phases 0-1 complete: events fan out to subscribed endpoints and are delivered signed, once. Retries with backoff and the DLQ are Phase 2 (until then a failed delivery is retried every 30s).

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

API reference: [api/openapi.yaml](api/openapi.yaml).

Without Docker for the Go services: start Postgres with `docker compose up -d postgres`, then `make migrate-up run-api`.

## Layout

```
cmd/{api,worker,migrate}   entrypoints
cmd/{admin,mockreceiver}   bootstrap CLI and dev webhook receiver
api/openapi.yaml           API spec
internal/api               /v1 handlers + API-key auth
internal/worker            claim -> send -> record loop
internal/deliver           signed HTTP sender
internal/sign              Standard Webhooks HMAC signing
internal/auth              API key generation/hashing
internal/config            env-based configuration
internal/logging           slog setup
internal/server            HTTP shell + /healthz, /readyz
internal/store             Postgres queries (events, endpoints, queue)
internal/testutil          per-test Postgres databases
migrations/                goose SQL migrations (embedded)
docs/                      architecture.md, plan.md
```

## Checks

`make vet test lint`. `make test` starts Postgres via Compose; integration tests are skipped if `DATABASE_URL` is unset. Lint needs `golangci-lint`.
