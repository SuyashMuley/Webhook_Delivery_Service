#!/usr/bin/env bash
# Runs sustained load from inside the cluster while disrupting it, then checks that
# nothing was lost. Needs: `make kind-up` and `kubectl apply -k deploy/kind-e2e`, then
# a restart of the api and worker so they pick up the e2e ConfigMap.
#
# Producers are plain curl loops with NO retries: any request that fails is counted,
# so this is a strict test of zero-downtime rollouts (a real producer would retry).
#
# Disruptions, in order: roll the api, roll the workers, force-delete a worker pod
# (a crash: no graceful shutdown), drain a node (honouring the PodDisruptionBudgets).
set -uo pipefail
cd "$(dirname "$0")/../.."
CTX=kind-webhooks
k() { kubectl --context "$CTX" "$@"; }
PRODUCERS=${PRODUCERS:-4}
ITERATIONS=${ITERATIONS:-800}      # events per producer
PAUSE=${PAUSE:-0.1}                # seconds between a producer's requests
RUN=$RANDOM
SECRET=whsec_ZGV2LW9ubHktcmVjZWl2ZXItc2VjcmV0LTAxMjM0NTY=   # the receiver's public dev secret
fail=0
ok()   { printf "  PASS  %s\n" "$*"; }
bad()  { printf "  FAIL  %s\n" "$*"; fail=1; }
psql() { k -n database exec deploy/postgres -- psql -U webhooks_owner -d webhooks -tA -c "$1"; }

echo "== setup: fresh receiver, tenant, key and 3 endpoints pointing at it"
# A fresh receiver means a fresh log: the receiver's own log is the independent evidence
# of what was delivered, and the kubelet rotates container logs at 10MiB, which would
# silently hide the start of a long-running receiver's history.
k -n receiver rollout restart deploy/receiver >/dev/null && k -n receiver rollout status deploy/receiver --timeout=120s >/dev/null
pkill -f "port-forward.*(postgres|webhook-api)" 2>/dev/null || true
k -n database port-forward svc/postgres 15432:5432 >/dev/null 2>&1 &
PF1=$!
sleep 3
export DATABASE_URL='postgres://webhooks_owner:owner-test-password@127.0.0.1:15432/webhooks?sslmode=disable'
TENANT=$(go run ./cmd/admin create-tenant "e2e-$RUN")
KEY=$(go run ./cmd/admin create-key "$TENANT")
go run ./cmd/admin set-limits "$TENANT" 100000 10000000 5000
POD=$(k -n webhooks get pod -l app.kubernetes.io/name=webhook-api -o name | head -1)
k -n webhooks port-forward "$POD" 18091:8080 >/dev/null 2>&1 &
PF2=$!
sleep 3
for h in ok1 ok2 ok3; do
  curl -s localhost:18091/v1/endpoints -H "Authorization: Bearer $KEY" \
    -d "{\"url\":\"http://receiver.receiver.svc:9000/hook/$h-$RUN\",\"secret\":\"$SECRET\",\"max_concurrency\":20}" >/dev/null
done
kill $PF2 2>/dev/null
echo "tenant=$TENANT endpoints=$(psql "select count(*) from endpoints where tenant_id='$TENANT'")"

echo "== start $PRODUCERS in-cluster producers x $ITERATIONS events (about $(python3 -c "print(int($ITERATIONS*($PAUSE+0.03)))")s)"
k -n ingress-nginx delete job load --ignore-not-found --wait=true >/dev/null
cat <<YAML | k apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: { name: load, namespace: ingress-nginx }
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
                  -H "Authorization: Bearer $KEY" -H "Idempotency-Key: e2e-$RUN-\$JOB_COMPLETION_INDEX-\$i" \\
                  -d '{"type":"e2e.test","payload":{"n":1}}')
                echo "\$c"
                if [ "\$c" != "202" ]; then echo "FAILED \$(date -u +%H:%M:%S) code=\$c body=\$(head -c 160 /tmp/body)" >&2; fi
                i=\$((i+1)); sleep $PAUSE
              done | sort | uniq -c | sed 's/^/codes: /'
YAML
T0=$(date +%s)
since() { echo "$(( $(date +%s) - T0 ))s @$(date -u +%H:%M:%S)"; }

sleep 8
echo "t+$(since)  rolling the API (maxUnavailable 0)"
k -n webhooks rollout restart deploy/webhook-api >/dev/null
k -n webhooks rollout status deploy/webhook-api --timeout=180s >/dev/null && echo "t+$(since)  API rollout complete"

sleep 8
echo "t+$(since)  rolling the workers"
k -n webhooks rollout restart deploy/webhook-worker >/dev/null
k -n webhooks rollout status deploy/webhook-worker --timeout=180s >/dev/null && echo "t+$(since)  worker rollout complete"

sleep 6
VICTIM=$(k -n webhooks get pod -l app.kubernetes.io/name=webhook-worker -o name | head -1)
echo "t+$(since)  CRASH: force-deleting $VICTIM with no grace period"
k -n webhooks delete "$VICTIM" --grace-period=0 --force >/dev/null 2>&1

sleep 12
NODE=$(k -n webhooks get pod -l app.kubernetes.io/name=webhook-api -o jsonpath='{.items[0].spec.nodeName}')
echo "t+$(since)  draining node $NODE (only our pods; PDBs apply)"
k drain "$NODE" --pod-selector='app.kubernetes.io/part-of=webhook-delivery' --ignore-daemonsets --delete-emptydir-data --timeout=120s 2>&1 | grep -E "evicting|evicted|drained|error|Cannot" | sed 's/^/        /' | head -8
k uncordon "$NODE" >/dev/null
echo "t+$(since)  node uncordoned"

echo "== wait for the producers to finish"
k -n ingress-nginx wait --for=condition=complete job/load --timeout=600s >/dev/null 2>&1 || bad "load job did not complete"
# One pod at a time: `logs --all-pods` prefixes each line with the pod name.
CODES=$(for p in $(k -n ingress-nginx get pods -l job-name=load -o name); do k -n ingress-nginx logs "$p" 2>/dev/null; done \
        | grep codes | awk '{c[$3]+=$2} END{for (x in c) printf "%s x%d  ", x, c[x]}')
echo "HTTP status codes seen by producers: $CODES"
for p in $(k -n ingress-nginx get pods -l job-name=load -o name); do k -n ingress-nginx logs "$p" 2>/dev/null | grep "^FAILED" | sed "s|^|  producer failure ($p): |"; done

echo "== wait for deliveries to drain"
for i in $(seq 1 120); do
  N=$(psql "select count(*) from deliveries where tenant_id='$TENANT' and status in ('pending','in_flight')")
  [ "$N" = "0" ] && break; sleep 2
done
echo "undelivered after drain wait: $N"

echo "== verify"
ACCEPTED=$(echo "$CODES" | grep -o '202 x[0-9]*' | grep -o '[0-9]*$'); ACCEPTED=${ACCEPTED:-0}
EVENTS=$(psql "select count(*) from events where tenant_id='$TENANT'")
DELIV=$(psql "select count(*) from deliveries where tenant_id='$TENANT'")
SUCC=$(psql "select count(*) from deliveries where tenant_id='$TENANT' and status='succeeded'")
DEAD=$(psql "select count(*) from deliveries where tenant_id='$TENANT' and status='dead'")
STUCK=$(psql "select count(*) from deliveries where tenant_id='$TENANT' and (status in ('pending','in_flight') or (status in ('succeeded','dead') and locked_by is not null))")
BADFAN=$(psql "select count(*) from (select event_id from deliveries where tenant_id='$TENANT' group by 1 having count(*) <> 3) x")
UNIQ=$(k -n receiver logs deploy/receiver 2>/dev/null | grep "received delivery" | grep -o "hook=ok[0-9]-$RUN webhook_id=[0-9a-f-]*" | sort -u | wc -l | tr -d ' ')
DUPS=$(( $(k -n receiver logs deploy/receiver 2>/dev/null | grep "received delivery" | grep -c "hook=ok[0-9]-$RUN") - UNIQ ))
echo "  producers got 202 for $ACCEPTED events; database has $EVENTS events, $DELIV deliveries ($SUCC succeeded, $DEAD dead, $STUCK stuck)"
echo "  receiver saw $UNIQ distinct deliveries ($DUPS duplicate receipts)"

TOTAL_REQ=$((PRODUCERS*ITERATIONS))
[ "$ACCEPTED" = "$TOTAL_REQ" ] && ok "all $TOTAL_REQ producer requests were accepted with no retries (zero failures during the rollouts, crash and drain)" || bad "only $ACCEPTED of $TOTAL_REQ producer requests succeeded: $CODES"
[ "$EVENTS" = "$ACCEPTED" ] && ok "every acknowledged event is in the database ($EVENTS)" || bad "events in DB ($EVENTS) differ from acknowledged ($ACCEPTED)"
[ "$BADFAN" = "0" ] && [ "$DELIV" = "$((EVENTS*3))" ] && ok "each event has exactly one delivery per endpoint ($DELIV)" || bad "fan-out wrong: $DELIV deliveries, $BADFAN events off"
[ "$STUCK" = "0" ] && ok "no delivery is stuck pending, in flight or holding a lease" || bad "$STUCK deliveries stuck"
[ "$SUCC" = "$DELIV" ] && ok "every delivery succeeded" || bad "only $SUCC of $DELIV succeeded ($DEAD dead)"
[ "$UNIQ" = "$SUCC" ] && ok "the receiver itself saw every delivery the database says succeeded (no loss)" || bad "receiver saw $UNIQ but $SUCC are marked succeeded"

kill $PF1 2>/dev/null
echo; [ $fail = 0 ] && echo "ALL DISRUPTION CHECKS PASSED" || { echo "SOME CHECKS FAILED"; exit 1; }
