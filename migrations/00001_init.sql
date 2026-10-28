-- +goose Up
-- +goose StatementBegin
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name          text        NOT NULL,
    status        text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
    max_inflight  int         NOT NULL DEFAULT 100 CHECK (max_inflight > 0),
    events_per_sec int        NOT NULL DEFAULT 100 CHECK (events_per_sec > 0),
    max_pending   int         NOT NULL DEFAULT 100000 CHECK (max_pending > 0),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    key_prefix   text        NOT NULL,
    key_hash     bytea       NOT NULL UNIQUE,
    scopes       text[]      NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

CREATE TABLE endpoints (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    url                  text        NOT NULL,
    description          text        NOT NULL DEFAULT '',
    event_types          text[]      NOT NULL DEFAULT '{*}',
    -- [{id, value (encrypted), created_at, expires_at}]; encryption lands in the security phase
    secrets              jsonb       NOT NULL DEFAULT '[]',
    status               text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'disabled')),
    max_concurrency      int         NOT NULL DEFAULT 5 CHECK (max_concurrency > 0),
    consecutive_failures int         NOT NULL DEFAULT 0,
    circuit_open_until   timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id);

CREATE TABLE events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    type            text        NOT NULL,
    payload         jsonb       NOT NULL,
    idempotency_key text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX events_tenant_created_idx ON events (tenant_id, created_at DESC);

-- One row per (event, endpoint). This table is the work queue and the DLQ.
CREATE TABLE deliveries (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    event_id         uuid        NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    endpoint_id      uuid        NOT NULL REFERENCES endpoints (id) ON DELETE CASCADE,
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'in_flight', 'succeeded', 'dead')),
    attempt_count    int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz,
    locked_by        text,
    last_status_code int,
    last_error       text,
    dead_at          timestamptz,
    dead_reason      text,
    trace_context    text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX deliveries_due_idx      ON deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_lease_idx    ON deliveries (lease_expires_at) WHERE status = 'in_flight';
CREATE INDEX deliveries_endpoint_idx ON deliveries (endpoint_id, status);
CREATE INDEX deliveries_tenant_idx   ON deliveries (tenant_id, status);
CREATE INDEX deliveries_dead_idx     ON deliveries (tenant_id, dead_at DESC) WHERE status = 'dead';

CREATE TABLE attempts (
    id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id           uuid        NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    attempt_no            int         NOT NULL,
    started_at            timestamptz NOT NULL DEFAULT now(),
    duration_ms           int,
    status_code           int,
    error                 text,
    response_body_snippet text
);
CREATE INDEX attempts_delivery_idx ON attempts (delivery_id, attempt_no);
-- +goose StatementEnd

-- +goose Down
DROP TABLE attempts;
DROP TABLE deliveries;
DROP TABLE events;
DROP TABLE endpoints;
DROP TABLE api_keys;
DROP TABLE tenants;
