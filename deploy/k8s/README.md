# Kubernetes deployment

A kustomize base for the api, worker and the migration job.

```sh
kubectl kustomize deploy/k8s | kubectl apply -f -      # or: kubectl apply -k deploy/k8s
make validate-k8s                                      # render + schema-check (kubeconform)
```

## Before you apply

1. **Build and push the images** (one image per binary: `docker build --build-arg CMD=api|worker|migrate .`) and set them in `kustomization.yaml` (`images:`), pinned to a version tag.
2. **Create the Secrets** described in `secret.example.yaml` with your secret manager: three database logins (api, worker, owner) and the encryption keys.
3. **Create the database roles and logins.** Migration 00004 creates the group roles `webhooks_api` and `webhooks_worker` (the migrating user needs `CREATEROLE`). Then create one login per service that is a member of *only* its role, so a compromised service cannot switch to broader privileges:
   ```sql
   CREATE ROLE webhooks_api_login    LOGIN PASSWORD '...' IN ROLE webhooks_api;
   CREATE ROLE webhooks_worker_login LOGIN PASSWORD '...' IN ROLE webhooks_worker;
   ```
   (The Deployments set `DB_ROLE=none` because of this.)
4. **Run the migrate Job** before rolling out a release (an Argo CD PreSync hook or Helm pre-upgrade hook does this automatically). Migrations are written to be compatible with the previous release.

## What is in the base

| File | Purpose |
|---|---|
| `namespace.yaml` | Namespace enforcing the *restricted* Pod Security Standard |
| `api.yaml`, `worker.yaml` | Deployments (non-root, read-only root filesystem, all capabilities dropped, seccomp), probes, and separate headless Services for metrics |
| `pdb.yaml` | PodDisruptionBudgets: at least one API pod and at most one worker down during node drains |
| `hpa.yaml` | API on CPU; worker on queue depth and oldest-pending age (External metrics) plus CPU |
| `networkpolicy.yaml` | Default deny; the API accepts traffic only from the ingress controller; workers reach the public internet but **not** private ranges |
| `migrate-job.yaml` | Applies migrations |

`optional/` holds things that need extra components: an Ingress example, a `PodMonitor` and a generated `PrometheusRule` (Prometheus Operator), and prometheus-adapter rules for the worker HPA.

## Shutdown behaviour

On `SIGTERM`: readiness starts failing (`SHUTDOWN_DRAIN_DELAY`, 3s) so the Service stops routing, the API finishes in-flight requests, and a worker stops claiming, waits up to `WORKER_DRAIN_TIMEOUT` (20s) for in-flight deliveries, then aborts any that remain and hands their leases straight back to the queue. `terminationGracePeriodSeconds` (45s) is deliberately longer than all of that; keep it so if you change the timeouts.

## Notes and assumptions

- The NetworkPolicies assume namespaces named `ingress-nginx`, `monitoring`, `database` and `observability`, and a CNI that enforces NetworkPolicy. Adjust the selectors for your cluster.
- Worker `maxReplicas` (10) bounds database connections: each worker holds a pool. Size Postgres, or put PgBouncer in front, for the maximum.
- The worker HPA needs prometheus-adapter (or KEDA with an equivalent Prometheus trigger). Without it, remove the two External metrics and scale on CPU only, or set replicas manually.
- This base is validated against the Kubernetes 1.30 schemas; it has not been applied to a live cluster.
