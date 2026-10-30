#!/usr/bin/env bash
# Hunts rare errors during pod turnover: producers hammer the API at a high rate with no
# retries while the API deployment is rolled over and over, so a request that lands on a
# pod at the wrong moment is likely to be caught. Every API pod's log is followed from the
# moment it appears (logs of deleted pods are otherwise lost) and any failure's time and
# body are recorded, so a failure can be matched to what the pod was doing.
# Needs the e2e overlay applied and the api/worker restarted (`make kind-e2e` does that).
set -uo pipefail
cd "$(dirname "$0")/../.."
CTX=kind-webhooks
k() { kubectl --context "$CTX" "$@"; }
PRODUCERS=${PRODUCERS:-8}; ITERATIONS=${ITERATIONS:-3000}; PAUSE=${PAUSE:-0.02}; ROLLOUTS=${ROLLOUTS:-14}
MODE=${MODE:-rollout}; RUN=$RANDOM; OUT=${OUT:-/tmp/rollout-storm-$RUN}; mkdir -p "$OUT"
SECRET=whsec_ZGV2LW9ubHktcmVjZWl2ZXItc2VjcmV0LTAxMjM0NTY=
psql() { k -n database exec deploy/postgres -- psql -U webhooks_owner -d webhooks -tA -c "$1"; }

k -n receiver rollout restart deploy/receiver >/dev/null && k -n receiver rollout status deploy/receiver --timeout=120s >/dev/null
k -n database port-forward svc/postgres 15432:5432 >/dev/null 2>&1 & PF1=$!; sleep 3
export DATABASE_URL='postgres://webhooks_owner:owner-test-password@127.0.0.1:15432/webhooks?sslmode=disable'
TENANT=$(go run ./cmd/admin create-tenant "storm-$RUN"); KEY=$(go run ./cmd/admin create-key "$TENANT")
go run ./cmd/admin set-limits "$TENANT" 1000000 100000000 5000
POD=$(k -n webhooks get pod -l app.kubernetes.io/name=webhook-api -o name | head -1)
k -n webhooks port-forward "$POD" 18092:8080 >/dev/null 2>&1 & PF2=$!; sleep 3
for h in a b; do curl -s localhost:18092/v1/endpoints -H "Authorization: Bearer $KEY" \
  -d "{\"url\":\"http://receiver.receiver.svc:9000/hook/$h-$RUN\",\"secret\":\"$SECRET\"}" >/dev/null; done
kill $PF2 2>/dev/null

# follow every API pod's log from the moment it appears
( : > "$OUT/seen"   # a file, not an associative array: macOS ships bash 3.2
  while [ ! -f "$OUT/stop" ]; do
    for p in $(k -n webhooks get pod -l app.kubernetes.io/name=webhook-api -o name 2>/dev/null); do
      n=${p#pod/}; grep -qx "$n" "$OUT/seen" && continue; echo "$n" >> "$OUT/seen"
      k -n webhooks logs -f "$n" >> "$OUT/api-$n.log" 2>&1 &
    done; sleep 0.5
  done ) &
COLLECTOR=$!

k create namespace ingress-nginx >/dev/null 2>&1 || true   # the API policy admits traffic from this namespace
k -n ingress-nginx delete job storm --ignore-not-found --wait=true >/dev/null
cat <<YAML | k apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: { name: storm, namespace: ingress-nginx }
spec:
  completions: $PRODUCERS
  parallelism: $PRODUCERS
  completionMode: Indexed
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: load
          image: curlimages/curl:latest
          imagePullPolicy: IfNotPresent
          command: ["sh","-c"]
          args:
            - |
              i=0
              while [ \$i -lt $ITERATIONS ]; do
                c=\$(curl -s -o /tmp/body -m 15 -w '%{http_code}' http://webhook-api.webhooks.svc/v1/events \\
                  -H "Authorization: Bearer $KEY" -H "Idempotency-Key: storm-$RUN-\$JOB_COMPLETION_INDEX-\$i" \\
                  -d '{"type":"storm","payload":{"n":1}}')
                echo "\$c"
                if [ "\$c" != "202" ]; then echo "FAILED \$(date -u +%H:%M:%S.%N | cut -c1-12) code=\$c body=\$(head -c 200 /tmp/body)" >&2; fi
                i=\$((i+1)); sleep $PAUSE
              done | sort | uniq -c | sed 's/^/codes: /'
YAML
sleep 5
evict() { printf '{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"webhooks"}}' "$1" \
            | k create --raw "/api/v1/namespaces/webhooks/pods/$1/eviction" -f - >/dev/null 2>&1; }
ready_api() { k -n webhooks get deploy webhook-api -o jsonpath='{.status.readyReplicas}' 2>/dev/null; }
for r in $(seq 1 "$ROLLOUTS"); do
  echo "$(date -u +%H:%M:%S.%N | cut -c1-12) $MODE $r" >> "$OUT/timeline.txt"
  if [ "$MODE" = drain ]; then
    # A full node drain, as an operator or autoscaler would: cordon, evict our pods through the
    # PDBs, wait for replacements, uncordon. Postgres and the receiver are left alone.
    NODE=$(k -n webhooks get pod -l app.kubernetes.io/name=webhook-api -o jsonpath='{.items[0].spec.nodeName}')
    k drain "$NODE" --pod-selector='app.kubernetes.io/part-of=webhook-delivery' --ignore-daemonsets --delete-emptydir-data --timeout=120s >/dev/null 2>&1
    k uncordon "$NODE" >/dev/null 2>&1
    for _ in $(seq 1 90); do [ "$(ready_api)" = "2" ] && break; sleep 1; done
    sleep 2
  elif [ "$MODE" = evict ]; then
    # What a node drain does: the old pod goes first and the budget only guarantees one stays.
    V=$(k -n webhooks get pod -l app.kubernetes.io/name=webhook-api -o name | head -1); V=${V#pod/}
    for _ in $(seq 1 30); do evict "$V" && break; sleep 1; done
    # Wait for the victim to be really gone first: readyReplicas lags by a second, and checking it
    # right away lets the loop race ahead and re-evict a pod that is already terminating.
    k -n webhooks wait --for=delete "pod/$V" --timeout=90s >/dev/null 2>&1
    for _ in $(seq 1 90); do [ "$(ready_api)" = "2" ] && break; sleep 1; done
    sleep 1
  else
    k -n webhooks rollout restart deploy/webhook-api >/dev/null
    k -n webhooks rollout status deploy/webhook-api --timeout=120s >/dev/null
  fi
done
k -n ingress-nginx wait --for=condition=complete job/storm --timeout=900s >/dev/null 2>&1 || echo "load job did not complete"
touch "$OUT/stop"; sleep 2; kill $COLLECTOR $PF1 2>/dev/null

echo "== producers:"
for p in $(k -n ingress-nginx get pods -l job-name=storm -o name); do k -n ingress-nginx logs "$p" 2>/dev/null; done > "$OUT/producers.txt"
grep codes "$OUT/producers.txt" | awk '{c[$3]+=$2} END{for (x in c) printf "  HTTP %s x%d\n", x, c[x]}'
echo "== failures (time, code, body):"; grep "^FAILED" "$OUT/producers.txt" | sort | head -20; echo "  total failures: $(grep -c '^FAILED' "$OUT/producers.txt")"
echo "== API pods followed: $(ls "$OUT"/api-*.log | wc -l)   503s in their access logs: $(cat "$OUT"/api-*.log | grep -c '"status":503')   'database unavailable': $(cat "$OUT"/api-*.log | grep -c 'database unavailable')"
cat "$OUT"/api-*.log | grep '"status":503' | head -3 | cut -c1-260
echo "output kept in $OUT"
