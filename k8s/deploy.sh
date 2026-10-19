#!/usr/bin/env bash
# Build the image, create the Slack Secret from .env, and apply a kustomization (default:
# k8s/overlays/dev; KUSTOMIZE_DIR=usecases/<name>/k8s for a use case) to the
# Dedicated cluster from terraform/gke. Requires its exact kube context.
#
# Distributed mode (Gateway / Worker / Control / Sandbox Service):
#   DISTRIBUTED_DIR=usecases/<name>/k8s/distributed/bootstrap SYSTEM_NAMESPACE=<name>-system \
#     k8s/deploy.sh                     # first build: zero replicas, nothing connects to Slack
#   DISTRIBUTED_DIR=usecases/<name>/k8s/distributed/steady SYSTEM_NAMESPACE=<name>-system \
#     k8s/deploy.sh                     # steady state
# It creates the Slack Secret and the silk-agent-sandbox Secret in SYSTEM_NAMESPACE, applies the
# directory (the image is named in its kustomization, so no image is built) and waits for the
# four roles. Use SILK_AGENT_ENV_FILE for the use case's own Slack tokens.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kustomize_dir="${KUSTOMIZE_DIR:-${repo_root}/k8s/overlays/dev}"
distributed_dir="${DISTRIBUTED_DIR:-}"
if [[ -n "${distributed_dir}" && -n "${KUSTOMIZE_DIR:-}" ]]; then
  echo "refusing to deploy: set only one of DISTRIBUTED_DIR and KUSTOMIZE_DIR" >&2
  exit 2
fi
gke_root="${repo_root}/terraform/gke"
expected_context="$(terraform -chdir="${gke_root}" output -raw kube_context)"
current_context="$(kubectl config current-context)"
if [[ -n "${KUBE_CONTEXT:-}" && "${KUBE_CONTEXT}" != "${expected_context}" ]]; then
  echo "refusing to deploy: KUBE_CONTEXT ${KUBE_CONTEXT} does not match Terraform output ${expected_context}" >&2
  exit 2
fi
if [[ "${current_context}" != "${expected_context}" ]]; then
  echo "refusing to deploy: current context ${current_context}, expected ${expected_context}" >&2
  exit 2
fi

# The single-process Bot gets its Slack tokens from a Secret made from an env file. A distributed
# deployment does not: its Gateway and Control read them from Secret Manager (see below), so no
# env file is read, and none can be picked up by mistake.
if [[ -z "${distributed_dir}" ]]; then
  env_file="${SILK_AGENT_ENV_FILE:-${repo_root}/.env}"
  for key in SLACK_BOT_TOKEN SLACK_APP_TOKEN; do
    grep -q "^${key}=." "${env_file}" || { echo "${key} is not set in ${env_file}" >&2; exit 3; }
  done
fi

if [[ -n "${distributed_dir}" ]]; then
  : "${SYSTEM_NAMESPACE:?Set SYSTEM_NAMESPACE to the management namespace (<name>-system)}"
  if [[ "${SYSTEM_NAMESPACE}" != *-system ]]; then
    echo "refusing to deploy: SYSTEM_NAMESPACE ${SYSTEM_NAMESPACE} is not a management namespace (<name>-system); the Sandbox namespace is not one" >&2
    exit 4
  fi
  rendered="$(kubectl kustomize "${distributed_dir}")"
  if ! grep -qx "  name: ${SYSTEM_NAMESPACE}" <<<"$(grep -A2 -x 'kind: Namespace' <<<"${rendered}")"; then
    echo "refusing to deploy: ${distributed_dir} does not define namespace ${SYSTEM_NAMESPACE}" >&2
    exit 4
  fi
  kubectl create namespace "${SYSTEM_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
  # The Slack tokens are not created here. Register them in Secret Manager first
  # (`gcloud secrets versions add <name>-slack-bot-token --data-file=-`, and the app token); the
  # Gateway (both) and Control (bot token) read them with their Workload Identity, the Worker and
  # the Sandbox Service never do. A role without a readable token fails to start.
  # Fingerprint key for the sandbox creation env: create once, never rotate implicitly.
  if ! kubectl -n "${SYSTEM_NAMESPACE}" get secret silk-agent-sandbox >/dev/null 2>&1; then
    kubectl -n "${SYSTEM_NAMESPACE}" create secret generic silk-agent-sandbox \
      --from-literal=environment-fingerprint-key="$(openssl rand -hex 32)"
  fi
  kubectl apply -k "${distributed_dir}"
  for role in sandbox-service gateway worker control; do
    kubectl -n "${SYSTEM_NAMESPACE}" rollout status "deployment/silk-agent-${role}" --timeout=300s
  done
  exit 0
fi

# The default (k8s/overlays/dev) builds this repository's Bot image and pins the Deployment to it.
# A use case (KUSTOMIZE_DIR) names its own Bot image in its kustomization (images:).
if [[ -z "${KUSTOMIZE_DIR:-}" ]]; then
  : "${SANDBOX_RUNTIME_IMAGE:?Set SANDBOX_RUNTIME_IMAGE to the pushed session-runtime image digest}"
  if [[ -z "${SILK_AGENT_IMAGE:-}" ]]; then
    eval "$("${repo_root}/k8s/build-image.sh" | grep '^SILK_AGENT_IMAGE=')"
  fi
  echo "image: ${SILK_AGENT_IMAGE}"
fi

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

if [[ -z "${KUSTOMIZE_DIR:-}" ]]; then
  # The Sandbox image goes into the ConfigMap's source file *before* kustomize runs: the
  # ConfigMap name carries a hash of its content, so only then does a new image roll the Bot.
  work="$(mktemp -d)"
  trap 'rm -rf "${work}"' EXIT
  cp -r "${repo_root}/k8s" "${work}/k8s"
  sed -i "s#image: SANDBOX_RUNTIME_IMAGE#image: ${SANDBOX_RUNTIME_IMAGE}#" \
    "${work}/k8s/overlays/dev/config/sandbox-config.yaml"
  kubectl kustomize "${work}/k8s/overlays/dev" \
    | sed "s#image: silk-agent\$#image: ${SILK_AGENT_IMAGE}#" \
    | kubectl apply -f -
else
  kubectl kustomize "${kustomize_dir}" | kubectl apply -f -
fi
kubectl -n session-sandbox-system rollout status deployment/silk-agent --timeout=300s
