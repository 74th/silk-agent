#!/usr/bin/env bash
# Build the image, create the Slack Secret from .env, and apply k8s/ to the
# gke-session-sandbox cluster. Requires the exact kube context below.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
expected_context="${KUBE_CONTEXT:-gke_nnyn-dev_us-central1_research-gke-agent-sandbox-with-sandboxd}"
current_context="$(kubectl config current-context)"
if [[ "${current_context}" != "${expected_context}" ]]; then
  echo "refusing to deploy: current context ${current_context}, expected ${expected_context}" >&2
  exit 2
fi

env_file="${SILK_AGENT_ENV_FILE:-${repo_root}/.env}"
for key in SLACK_BOT_TOKEN SLACK_APP_TOKEN; do
  grep -q "^${key}=." "${env_file}" || { echo "${key} is not set in ${env_file}" >&2; exit 3; }
done

if [[ -z "${SILK_AGENT_IMAGE:-}" ]]; then
  eval "$("${repo_root}/k8s/build-image.sh" | grep '^SILK_AGENT_IMAGE=')"
fi
echo "image: ${SILK_AGENT_IMAGE}"

# Only the Slack tokens go into the Secret.
kubectl -n session-sandbox-system create secret generic silk-agent-slack \
  --from-env-file=<(grep -E '^(SLACK_BOT_TOKEN|SLACK_APP_TOKEN)=' "${env_file}") \
  --dry-run=client -o yaml | kubectl apply -f -

# Fingerprint key for the sandbox creation env: create once, never rotate implicitly
# (existing sandboxes were fingerprinted with it).
if ! kubectl -n session-sandbox-system get secret silk-agent-sandbox >/dev/null 2>&1; then
  kubectl -n session-sandbox-system create secret generic silk-agent-sandbox \
    --from-literal=environment-fingerprint-key="$(openssl rand -hex 32)"
fi

kubectl kustomize "${repo_root}/k8s" \
  | sed "s#image: us-central1-docker.pkg.dev/nnyn-dev/research-gke-agent-sandbox-with-sandboxd-images/silk-agent\$#image: ${SILK_AGENT_IMAGE}#" \
  | kubectl apply -f -
kubectl -n session-sandbox-system rollout status deployment/silk-agent --timeout=300s
