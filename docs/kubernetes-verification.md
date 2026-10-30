# Kubernetes verification

The manifests in `deploy/k8s` were schema-validated in CI, but "valid YAML" says nothing about whether the cluster accepts them, whether the policies are enforced, or how the workloads behave when Kubernetes disrupts them. This document records what happened when they were applied to a real cluster.

## Environment

- `kind` v0.33, three nodes (one control plane, two workers labelled as different zones), Kubernetes **v1.37**.
- **Calico v3.33** as the CNI. (The default CNI turned out to be unusable for this; see finding 2.)
- A throwaway Postgres in the cluster (`emptyDir`, fixed password), the four images built from this repository and loaded into the nodes, and, for the disruption tests, an in-cluster receiver.
- Everything on one laptop, so timings say little about production. Reproduce with `make kind-up`, `make kind-netpol`, `make kind-e2e`, `make kind-down`.

## What was verified

| Claim | Result |
|---|---|
| The documented deployment sequence works (secrets, migrate Job, create least-privilege logins, apply) | Yes: from a fresh cluster to all pods Ready in **18 s with 0 restarts**. The API and worker connect as login users that hold *only* `webhooks_api` / `webhooks_worker` (never tested before; earlier phases used `SET ROLE`), so row-level security and the grants work through real role membership. |
| Manifests are accepted by a current API server | Yes, on Kubernetes 1.37, with no deprecation warnings. |
| Pod Security *restricted* is enforced | Yes: a privileged pod in the namespace was rejected. The real pods are admitted and run as UID 65532, non-root, read-only root filesystem, all capabilities dropped, seccomp `RuntimeDefault`, no service-account token. |
| Pods spread across nodes | One API and one worker per node. |
| NetworkPolicies are enforced | **17 of 17 probes behaved as specified** (below). |
| Strict (production) mode works end to end | A real delivery to a public HTTPS endpoint succeeded from inside the cluster (through the egress policy, the SSRF guard and TLS). Registration of plain-http and private-address endpoints is rejected. |
| Zero-downtime rollouts, a crashed worker and a node drain lose nothing | **8 of 8 repeated runs passed** (below). |
| PodDisruptionBudgets | Yes: two back-to-back evictions of the two API pods: the first was allowed, the second was refused (*"would violate the pod's disruption budget"*), and another was allowed once the replacement was Ready. |
| API autoscaling on CPU | The HPA read real CPU once metrics-server was installed, and under ingest load (181% against a 70% target) scaled the API from 2 to 6 pods. Scale-*down* of the API was **not observed**. |
| Worker autoscaling on queue metrics (Prometheus, prometheus-adapter, external metrics API) | Yes. Under worker-bound load the HPA scaled the workers 2 to 10 within about 2 minutes, and in an earlier run scaled them back down 10 to 2 after the backlog drained. See finding 3. |

### NetworkPolicy results (`make kind-netpol`)

Each probe runs in a pod carrying the labels the policies select on, so it has the same network identity as the real pods. A policy drops traffic silently, so a timeout means "blocked".

- **Ingress:** the API is reachable from the ingress controller's namespace and blocked from `default` and `monitoring`. Metrics (`:9090`) are reachable from `monitoring` and blocked from `default` and the ingress namespace.
- **Worker egress:** allowed to Postgres and to the public internet; blocked to the Kubernetes API server, the API service, the node's kubelet and the cloud metadata address.
- **API egress:** allowed to Postgres only; blocked to the internet and the Kubernetes API server.

### Disruption test (`make kind-e2e`)

Four producer pods inside the cluster sent 3,200 events with **no retries**, so any failed request is visible, while the cluster was disrupted in order: roll the API, roll the workers, force-delete a worker pod with no grace period (a crash), drain a node (honouring the PDBs). Afterwards the database and the receiver's own log were checked.

- **8 consecutive runs: every producer request accepted (3,200 of 3,200 each), every event stored, exactly one delivery per endpoint, every delivery succeeded, none stuck, and the receiver independently saw every delivery the database says succeeded.** (Duplicate receipts were 0 in the single runs where that count was printed; the repeated runs checked for loss, not duplicates.)
- One earlier run had a single `503` out of 3,200 requests. **It did not recur in 11 later instrumented runs and I have not explained it.** Across all 13 runs that is one failure in about 41,600 requests. The producers now record the time and body of any failure, so a recurrence will be diagnosable.

## Findings

1. **A real defect in the startup retry added in Phase 5.** Services waited up to 60 s for the database *before* opening their HTTP port, so `/healthz` was unreachable during the wait and the 60 s startup probe killed the container: a crash loop, which is exactly what the retry was meant to prevent. Only a real cluster showed it. Now the HTTP server starts immediately behind a `Gate` (liveness answers at once, readiness stays red, everything else gets `503 + Retry-After`) and the database connection happens in the background. Verified live: with the database removed, fresh pods stayed alive (0 restarts) and not-ready, logging each failed attempt (`password authentication failed`, etc.); after the 60 s budget they exit non-zero so a genuine misconfiguration shows up as a restart loop rather than silent waiting.
2. **kind's default CNI does not enforce egress policy.** A control experiment with a bare *deny-all-egress* policy blocked nothing (not even DNS), so four of my egress checks "failed". Those failures were the test environment, not the manifests, and a green result there would have been meaningless. With Calico the same control blocks everything, and all 17 checks pass. If you test NetworkPolicy on kind, use a real CNI.
3. **Worker autoscaling: inert without the adapter, and the signal itself was wrong.**
   - Without prometheus-adapter the HPA reported `ScalingActive=False`: Kubernetes will not scale down while any configured metric is unavailable, and workers wait on customer endpoints so CPU never asks to scale up.
   - With the adapter installed it scaled correctly, **but testing it exposed a flaw in my metric**: the HPA used the total due backlog, which includes work held back by a slow customer's own concurrency cap. One endpoint capped at 5 concurrent deliveries and receiving 20/s grew the backlog to ~1,250 and scaled the workers from 2 to 6 (and climbing) for no benefit, while `oldest_pending_age` correctly stayed at 0 s.
   - Fix: workers now also publish `queue_depth{state="due_claimable"}` (due work whose endpoint has a free slot and whose tenant is under its cap), and the adapter rule uses its **minimum over the last minute**, because the instantaneous value flickers up whenever a capped endpoint momentarily has a free slot. Re-tested both ways: the capped endpoint no longer scales the workers (the HPA sees a steady 0 with workers at 2), and worker-bound load still scales them 2 to 10.
4. **Test-tooling issues, noted because they nearly produced wrong conclusions:** kubectl log rotation at 10 MiB hid the start of a long-lived receiver's history and briefly looked like data loss (disproved by reading the rotated file: all runs had 9,600 of 9,600 deliveries); a kustomize overlay cannot live inside the directory it extends; probe pods in a restricted namespace need a numeric UID; a script's `exec` without `-i` silently skipped the SQL that creates the database logins.

## What this does not show

- **The worker scale-down after the fix:** scale-down was observed before the signal change, not repeated after it. The API's scale-down was never observed. The HPA's production scale-down window (600 s) was shortened to 60 s for the test.
- **A real ingress controller, a cloud load balancer, real zones, and TLS to the database** (the test database used `sslmode=disable`).
- **The unexplained single `503`.** A rollout storm (14 rolling restarts), an eviction storm (14 evictions) and a drain storm (10 node drains), each under high-rate load with every API pod's log captured, produced 0 failures in 96,000 requests, so pod turnover does not cause it. It remains unexplained and unreproduced.
- **Long-running behaviour**: each disruption run lasted about 100 seconds.
- Only the NetworkPolicies as implemented by Calico: other CNIs may differ.
- The test Postgres is ephemeral, so database failover and persistence were not exercised in the cluster (the local load tests covered Postgres crashes).

## Not in CI

The cluster tests take several minutes and need Docker-in-Docker; CI runs the schema validation, the alert-rule tests and the real-process load smoke test instead. A scheduled job that runs `make kind-up kind-netpol kind-e2e` would be a reasonable addition.
