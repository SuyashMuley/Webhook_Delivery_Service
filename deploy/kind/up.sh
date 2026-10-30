#!/usr/bin/env bash
# Deploys deploy/k8s into the kind cluster, following the same steps the README
# gives an operator (secrets, migrate, database logins), so that those steps are
# tested too. Run `make kind-up` first to create the cluster and load the images.
set -euo pipefail
cd "$(dirname "$0")/../.."
CTX=kind-webhooks
k() { kubectl --context "$CTX" "$@"; }

echo "== namespaces and database"
k apply -f deploy/k8s/namespace.yaml
k apply -f deploy/kind/postgres.yaml
k -n database rollout status deploy/postgres --timeout=180s

echo "== secrets (generated; nothing is committed)"
mk() { k -n webhooks create secret generic "$1" --from-literal="$2=$3" --dry-run=client -o yaml | k apply -f - >/dev/null; }
DB=postgres.database.svc.cluster.local:5432/webhooks?sslmode=disable
mk webhook-db-owner DATABASE_URL "postgres://webhooks_owner:owner-test-password@$DB"
mk webhook-db-api DATABASE_URL "postgres://webhooks_api_login:api-test-password@$DB"
mk webhook-db-worker DATABASE_URL "postgres://webhooks_worker_login:worker-test-password@$DB"
# Never regenerate the encryption key: secrets already encrypted with it would become unreadable.
if ! k -n webhooks get secret webhook-encryption >/dev/null 2>&1; then
  mk webhook-encryption SECRET_ENCRYPTION_KEYS "$(go run ./cmd/admin gen-secret-key k1)"
fi

echo "== apply the manifests"
# A Job's pod template is immutable, so a previous run must be removed before applying again.
k -n webhooks delete job webhook-migrate --ignore-not-found >/dev/null
k apply -k deploy/kind

echo "== wait for the migration job"
k -n webhooks wait --for=condition=complete job/webhook-migrate --timeout=180s

echo "== create the least-privilege logins (README step 3)"
k -n database exec -i deploy/postgres -- psql -U webhooks_owner -d webhooks -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'webhooks_api_login') THEN
    CREATE ROLE webhooks_api_login LOGIN PASSWORD 'api-test-password' IN ROLE webhooks_api;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'webhooks_worker_login') THEN
    CREATE ROLE webhooks_worker_login LOGIN PASSWORD 'worker-test-password' IN ROLE webhooks_worker;
  END IF;
END \$\$;
SQL

echo "== wait for the workloads"
k -n webhooks rollout status deploy/webhook-api --timeout=240s
k -n webhooks rollout status deploy/webhook-worker --timeout=240s
k -n webhooks get pods -o wide
