-- +goose Up
-- Claiming is now per tenant (fair scheduling), so the due-index is keyed by tenant first.
DROP INDEX deliveries_due_idx;
CREATE INDEX deliveries_tenant_due_idx ON deliveries (tenant_id, next_attempt_at) WHERE status = 'pending';

-- Audit trail for DLQ replays.
CREATE TABLE delivery_replays (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id         uuid        NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    tenant_id           uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    replayed_at         timestamptz NOT NULL DEFAULT now(),
    prior_dead_reason   text,
    prior_attempt_count int         NOT NULL
);
CREATE INDEX delivery_replays_delivery_idx ON delivery_replays (delivery_id);

-- +goose Down
DROP TABLE delivery_replays;
DROP INDEX deliveries_tenant_due_idx;
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at) WHERE status = 'pending';
