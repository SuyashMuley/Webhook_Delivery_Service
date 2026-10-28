# Webhook Delivery

Multi-tenant webhook delivery service with retries and a dead letter queue. See [docs/architecture.md](docs/architecture.md) for the design and [docs/plan.md](docs/plan.md) for the build plan.

**Status:** Phase 0 (scaffold) complete. Delivery logic starts in Phase 1.

## Run locally

```sh
make up                      # postgres + migrations + api (:8080) + worker (:8081)
curl localhost:8080/readyz
make down
```

Without Docker for the Go services: start Postgres with `docker compose up -d postgres`, then `make migrate-up run-api`.

## Layout

```
cmd/{api,worker,migrate}   entrypoints
internal/config            env-based configuration
internal/logging           slog setup
internal/server            HTTP shell + /healthz, /readyz
internal/store             Postgres pool
migrations/                goose SQL migrations (embedded)
docs/                      architecture.md, plan.md
```

## Checks

`make vet test lint` (lint needs `golangci-lint`).
