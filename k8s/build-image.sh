#!/usr/bin/env bash
# Build and push the silk-agent image; prints SILK_AGENT_IMAGE=<repository>@sha256:<digest>.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository="${SILK_AGENT_IMAGE_REPOSITORY:-us-central1-docker.pkg.dev/nnyn-dev/research-gke-agent-sandbox-with-sandboxd-images/silk-agent}"
tag="${SILK_AGENT_IMAGE_TAG:-$(date -u +%Y%m%d-%H%M%S)}"

gcloud auth configure-docker "${repository%%/*}" --quiet >/dev/null 2>&1
docker build --pull \
  --tag "${repository}:${tag}" "${repo_root}" >&2
docker push "${repository}:${tag}" >&2
digest="$(gcloud artifacts docker images describe "${repository}:${tag}" --format='value(image_summary.digest)')"
[[ -n "${digest}" ]] || { echo "no digest for ${repository}:${tag}" >&2; exit 4; }
echo "SILK_AGENT_IMAGE=${repository}@${digest}"
