-- +goose Up
-- Circuit breaker state and auto-disable bookkeeping. consecutive_failures,
-- circuit_open_until and max_concurrency already exist from 00001.
ALTER TABLE endpoints
    ADD COLUMN circuit_trips   int         NOT NULL DEFAULT 0, -- times the circuit opened since the last success (drives the backoff)
    ADD COLUMN failing_since   timestamptz,                    -- start of the current failure streak
    ADD COLUMN disabled_at     timestamptz,
    ADD COLUMN disabled_reason text;

-- Claiming now walks endpoints (to enforce per-endpoint concurrency and the
-- circuit breaker), so due deliveries are indexed by endpoint. It replaces the
-- per-tenant index from 00002.
DROP INDEX deliveries_tenant_due_idx;
CREATE INDEX deliveries_endpoint_due_idx ON deliveries (endpoint_id, next_attempt_at) WHERE status = 'pending';

-- +goose Down
DROP INDEX deliveries_endpoint_due_idx;
CREATE INDEX deliveries_tenant_due_idx ON deliveries (tenant_id, next_attempt_at) WHERE status = 'pending';
ALTER TABLE endpoints
    DROP COLUMN disabled_reason,
    DROP COLUMN disabled_at,
    DROP COLUMN failing_since,
    DROP COLUMN circuit_trips;
