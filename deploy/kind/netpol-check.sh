#!/usr/bin/env bash
# Verifies that the NetworkPolicies in deploy/k8s are not just valid YAML but are
# ENFORCED: each probe states whether traffic must be dropped or allowed, and the
# result is checked. A policy drops traffic silently (the connection times out), so
# curl exit 28 means "blocked"; any other outcome (a response, a refusal, a protocol
# error) means the network let the connection through.
#
# Probes run as pods carrying the labels the policies select on, so they have the
# same network identity as the real api and worker pods. Needs the cluster from
# `make kind-up`.
set -uo pipefail
CTX=kind-webhooks
k() { kubectl --context "$CTX" "$@"; }
IMG=curlimages/curl:latest
fail=0
# A numeric UID is required: with runAsNonRoot the kubelet cannot verify a named user (curl_user is uid 100).
SC='{"runAsNonRoot":true,"runAsUser":100,"seccompProfile":{"type":"RuntimeDefault"}}'
CSC='{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}'

# run NAMESPACE LABELS CURL-ARGS... -> prints curl's exit code
run() {
  local ns=$1 labels=$2; shift 2
  local name="probe-$RANDOM"
  k -n "$ns" run "$name" --image="$IMG" --restart=Never --labels="$labels" -q \
    --overrides="{\"spec\":{\"securityContext\":$SC,\"containers\":[{\"name\":\"$name\",\"image\":\"$IMG\",\"securityContext\":$CSC,\"command\":[\"sh\",\"-c\",\"curl -sS -m 6 -o /dev/null $* ; echo exit=\$?\"]}]}}" >/dev/null 2>&1
  local out=""
  for _ in $(seq 1 60); do
    out=$(k -n "$ns" logs "$name" 2>/dev/null | grep -o 'exit=[0-9]*' | tail -1)
    [ -n "$out" ] && break
    sleep 1
  done
  k -n "$ns" delete pod "$name" --now --wait=false >/dev/null 2>&1
  echo "${out#exit=}"
}

# check DESCRIPTION EXPECT(blocked|allowed) NAMESPACE LABELS URL [extra curl args]
check() {
  local desc=$1 expect=$2 ns=$3 labels=$4 url=$5; shift 5
  local code; code=$(run "$ns" "$labels" "$@" "$url")
  local got=allowed; [ "$code" = "28" ] && got=blocked
  [ -z "$code" ] && got="NO-RESULT"
  local mark=PASS; [ "$got" != "$expect" ] && { mark=FAIL; fail=1; }
  printf "  %-4s %-62s expected %-7s got %-7s (curl exit %s)\n" "$mark" "$desc" "$expect" "$got" "${code:-?}"
}

for ns in ingress-nginx monitoring; do k create namespace "$ns" >/dev/null 2>&1 || true; done
API_IP=$(k -n webhooks get svc webhook-api -o jsonpath='{.spec.clusterIP}')
NODE_IP=$(k get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
WORKER=app.kubernetes.io/name=webhook-worker,app.kubernetes.io/part-of=webhook-delivery
API=app.kubernetes.io/name=webhook-api,app.kubernetes.io/part-of=webhook-delivery

echo "Ingress to the API (only the ingress controller's namespace may reach it)"
check "ingress-nginx namespace -> api :80" allowed ingress-nginx x=y "http://$API_IP/healthz"
check "default namespace -> api :80"       blocked default x=y "http://$API_IP/healthz"
check "monitoring namespace -> api :80"    blocked monitoring x=y "http://$API_IP/healthz"

echo "Metrics (only the monitoring namespace may scrape :9090)"
check "monitoring -> api-metrics :9090"    allowed monitoring x=y "http://webhook-api-metrics.webhooks.svc:9090/metrics"
check "monitoring -> worker-metrics :9090" allowed monitoring x=y "http://webhook-worker-metrics.webhooks.svc:9090/metrics"
check "default -> api-metrics :9090"       blocked default x=y "http://webhook-api-metrics.webhooks.svc:9090/metrics"
check "ingress-nginx -> worker-metrics"    blocked ingress-nginx x=y "http://webhook-worker-metrics.webhooks.svc:9090/metrics"

echo "Worker egress (database and public internet only)"
check "worker -> postgres :5432"           allowed webhooks "$WORKER" "http://postgres.database.svc:5432/"
check "worker -> public https (example.com)" allowed webhooks "$WORKER" "https://example.com/"
check "worker -> kube-apiserver (cluster IP)" blocked webhooks "$WORKER" "https://kubernetes.default.svc/"
check "worker -> the api service"          blocked webhooks "$WORKER" "http://$API_IP/healthz"
check "worker -> node address ($NODE_IP:10250)" blocked webhooks "$WORKER" "https://$NODE_IP:10250/"
check "worker -> cloud metadata 169.254.169.254" blocked webhooks "$WORKER" "http://169.254.169.254/latest/meta-data/"

echo "API egress (database only: it never calls out)"
check "api -> postgres :5432"              allowed webhooks "$API" "http://postgres.database.svc:5432/"
check "api -> public https (example.com)"  blocked webhooks "$API" "https://example.com/"
check "api -> kube-apiserver"              blocked webhooks "$API" "https://kubernetes.default.svc/"

echo
[ $fail = 0 ] && echo "ALL NETWORK POLICY CHECKS PASSED" || { echo "SOME CHECKS FAILED"; exit 1; }
