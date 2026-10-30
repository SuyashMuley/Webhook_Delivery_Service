#!/usr/bin/env bash
# Creates the kind cluster with Calico (which enforces egress NetworkPolicy).
set -euo pipefail
cd "$(dirname "$0")/../.."
CALICO_VERSION="${CALICO_VERSION:-v3.33.0}"
CTX=kind-webhooks
k() { kubectl --context "$CTX" "$@"; }

kind create cluster --name webhooks --config deploy/kind/cluster.yaml
echo "== installing Calico $CALICO_VERSION"
k apply -f "https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/calico.yaml" >/dev/null
# kind nodes are containers; Calico's reverse-path check is too strict for that setup.
k -n kube-system set env daemonset/calico-node FELIX_IGNORELOOSERPF=true >/dev/null
k -n kube-system rollout status daemonset/calico-node --timeout=300s
k -n kube-system rollout status deployment/calico-kube-controllers --timeout=300s
k wait --for=condition=Ready nodes --all --timeout=300s
