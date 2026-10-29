-- +goose Up
-- Per-tenant ingest rate limiting (token bucket). The bucket lives in Postgres
-- so every API replica shares one limit; replicas lease small batches of tokens
-- instead of touching the row once per event.
ALTER TABLE tenants
    ADD COLUMN rl_tokens     double precision NOT NULL DEFAULT 1000000, -- capped to the burst on first use
    ADD COLUMN rl_updated_at timestamptz      NOT NULL DEFAULT now();

-- The API role may not read tenants, so it reaches its own tenant's limits and
-- bucket through these SECURITY DEFINER functions. Both act only on the tenant in
-- the transaction's app.tenant_id, so a compromised API cannot drain or read
-- another tenant's bucket.
-- +goose StatementBegin
CREATE FUNCTION take_ingest_tokens(p_want int) RETURNS TABLE (granted int, per_sec int)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp
    AS $$
DECLARE
    v_tenant  uuid := app_tenant();
    v_rate    int;
    v_tokens  double precision;
    v_updated timestamptz;
    v_avail   double precision;
    v_grant   int;
BEGIN
    IF v_tenant IS NULL THEN
        RAISE EXCEPTION 'no tenant context' USING ERRCODE = '42501';
    END IF;
    SELECT t.events_per_sec, t.rl_tokens, t.rl_updated_at
      INTO v_rate, v_tokens, v_updated
      FROM tenants t WHERE t.id = v_tenant FOR UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    -- refill at events_per_sec, with room for a burst of two seconds' worth
    v_avail := LEAST(v_rate * 2.0, v_tokens + extract(epoch FROM clock_timestamp() - v_updated) * v_rate);
    v_grant := LEAST(GREATEST(p_want, 0), floor(v_avail)::int);
    UPDATE tenants SET rl_tokens = v_avail - v_grant, rl_updated_at = clock_timestamp() WHERE id = v_tenant;
    granted := v_grant;
    per_sec := v_rate;
    RETURN NEXT;
END
$$;
-- +goose StatementEnd

CREATE FUNCTION tenant_limits() RETURNS TABLE (events_per_sec int, max_pending int)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp
    AS $$ SELECT t.events_per_sec, t.max_pending FROM tenants t WHERE t.id = app_tenant() $$;

REVOKE ALL ON FUNCTION take_ingest_tokens(int), tenant_limits() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION take_ingest_tokens(int), tenant_limits() TO webhooks_api;

-- Retention: the purge job removes old finished work in batches, ordered by these
-- indexes so each batch is a cheap range scan rather than a table scan. (The cost
-- is a little extra index maintenance on every completion and every ingest.)
CREATE INDEX deliveries_purge_succeeded_idx ON deliveries (updated_at) WHERE status = 'succeeded';
CREATE INDEX deliveries_purge_dead_idx      ON deliveries (dead_at)    WHERE status = 'dead';
CREATE INDEX events_created_idx             ON events (created_at);

GRANT DELETE ON deliveries, events TO webhooks_worker;

-- +goose Down
REVOKE DELETE ON deliveries, events FROM webhooks_worker;
DROP INDEX events_created_idx;
DROP INDEX deliveries_purge_dead_idx;
DROP INDEX deliveries_purge_succeeded_idx;
DROP FUNCTION tenant_limits();
DROP FUNCTION take_ingest_tokens(int);
ALTER TABLE tenants DROP COLUMN rl_updated_at, DROP COLUMN rl_tokens;
