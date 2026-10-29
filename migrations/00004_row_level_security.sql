-- +goose Up
-- Defense in depth for tenant isolation. Every query in the API already filters
-- by tenant_id; row-level security makes the database enforce it too, so a
-- missing WHERE clause returns nothing instead of leaking another tenant's rows.
--
-- Two NOLOGIN group roles are created (this needs the CREATEROLE privilege):
--   webhooks_api     sees only the tenant named in the transaction-local setting
--                    app.tenant_id, and has no access to tenants/api_keys.
--   webhooks_worker  sees all tenants (it delivers for everyone) but can only do
--                    what delivery needs.
-- The table owner (migrations, admin CLI) is exempt from RLS.

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'webhooks_api') THEN
        CREATE ROLE webhooks_api NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'webhooks_worker') THEN
        CREATE ROLE webhooks_worker NOLOGIN;
    END IF;
END
$$;
-- +goose StatementEnd

-- The tenant for the current transaction; NULL (so no rows match) when unset.
CREATE FUNCTION app_tenant() RETURNS uuid
    LANGUAGE sql STABLE
    AS $$ SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid $$;

-- API key lookup happens before the tenant is known, and the API role may not
-- read api_keys directly, so it goes through this SECURITY DEFINER function.
CREATE FUNCTION authenticate_api_key(p_hash bytea) RETURNS uuid
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp
    AS $$
        SELECT k.tenant_id
          FROM api_keys k JOIN tenants t ON t.id = k.tenant_id
         WHERE k.key_hash = p_hash AND k.revoked_at IS NULL AND t.status = 'active'
    $$;
REVOKE ALL ON FUNCTION authenticate_api_key(bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION authenticate_api_key(bytea) TO webhooks_api;

GRANT USAGE ON SCHEMA public TO webhooks_api, webhooks_worker;

-- API role
GRANT SELECT, INSERT, UPDATE, DELETE ON endpoints TO webhooks_api;
GRANT SELECT, INSERT ON events TO webhooks_api;
GRANT SELECT, INSERT, UPDATE ON deliveries TO webhooks_api;
GRANT SELECT ON attempts TO webhooks_api;
GRANT SELECT, INSERT ON delivery_replays TO webhooks_api;

-- Worker role
GRANT SELECT ON tenants TO webhooks_worker;
GRANT SELECT, UPDATE ON endpoints TO webhooks_worker;
GRANT SELECT ON events TO webhooks_worker;
GRANT SELECT, UPDATE ON deliveries TO webhooks_worker;
GRANT SELECT, INSERT ON attempts TO webhooks_worker;

ALTER TABLE endpoints         ENABLE ROW LEVEL SECURITY;
ALTER TABLE events            ENABLE ROW LEVEL SECURITY;
ALTER TABLE deliveries        ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempts          ENABLE ROW LEVEL SECURITY;
ALTER TABLE delivery_replays  ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON endpoints TO webhooks_api
    USING (tenant_id = app_tenant()) WITH CHECK (tenant_id = app_tenant());
CREATE POLICY tenant_isolation ON events TO webhooks_api
    USING (tenant_id = app_tenant()) WITH CHECK (tenant_id = app_tenant());
CREATE POLICY tenant_isolation ON deliveries TO webhooks_api
    USING (tenant_id = app_tenant()) WITH CHECK (tenant_id = app_tenant());
CREATE POLICY tenant_isolation ON delivery_replays TO webhooks_api
    USING (tenant_id = app_tenant()) WITH CHECK (tenant_id = app_tenant());
-- attempts has no tenant_id: it is visible exactly when its delivery is (the
-- subquery is itself subject to the deliveries policy).
CREATE POLICY tenant_isolation ON attempts FOR SELECT TO webhooks_api
    USING (EXISTS (SELECT 1 FROM deliveries d WHERE d.id = attempts.delivery_id));

CREATE POLICY worker_all ON endpoints TO webhooks_worker USING (true) WITH CHECK (true);
CREATE POLICY worker_all ON events TO webhooks_worker USING (true) WITH CHECK (true);
CREATE POLICY worker_all ON deliveries TO webhooks_worker USING (true) WITH CHECK (true);
CREATE POLICY worker_all ON attempts TO webhooks_worker USING (true) WITH CHECK (true);

-- +goose Down
DROP POLICY worker_all ON attempts;
DROP POLICY worker_all ON deliveries;
DROP POLICY worker_all ON events;
DROP POLICY worker_all ON endpoints;
DROP POLICY tenant_isolation ON attempts;
DROP POLICY tenant_isolation ON delivery_replays;
DROP POLICY tenant_isolation ON deliveries;
DROP POLICY tenant_isolation ON events;
DROP POLICY tenant_isolation ON endpoints;
ALTER TABLE delivery_replays DISABLE ROW LEVEL SECURITY;
ALTER TABLE attempts         DISABLE ROW LEVEL SECURITY;
ALTER TABLE deliveries       DISABLE ROW LEVEL SECURITY;
ALTER TABLE events           DISABLE ROW LEVEL SECURITY;
ALTER TABLE endpoints        DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM webhooks_api, webhooks_worker;
REVOKE USAGE ON SCHEMA public FROM webhooks_api, webhooks_worker;
DROP FUNCTION authenticate_api_key(bytea);
DROP FUNCTION app_tenant();
-- The roles are cluster-wide and may be shared by other databases, so they are not dropped.
