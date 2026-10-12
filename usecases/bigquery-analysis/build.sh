#!/usr/bin/env bash
# Build this use case's Bot image and Sandbox image (and push them with PUSH=1).
#   SILK_AGENT_IMAGE       silk-agent Bot base image (default: silk-agent:latest, built locally)
#   SESSION_RUNTIME_IMAGE  Sandbox base image (default: the local session-runtime image)
#   IMAGE_REPOSITORY       e.g. us-central1-docker.pkg.dev/<project>/<repo>; image names are
#                          <repository>/<name>-bot and <repository>/<name>-sandbox
#   TAG                    image tag (default: the use case's version)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
name="${USECASE_NAME:-usecase}"
repository="${IMAGE_REPOSITORY:-local}"
tag="${TAG:-$(python3 -c "import sys, tomllib; print(tomllib.load(open(sys.argv[1], 'rb'))['project']['version'])" "${here}/pyproject.toml")}"
bot="${repository}/${name}-bot:${tag}"
sandbox="${repository}/${name}-sandbox:${tag}"

bot_args=()
[[ -n "${SILK_AGENT_IMAGE:-}" ]] && bot_args+=(--build-arg "SILK_AGENT_IMAGE=${SILK_AGENT_IMAGE}")
sandbox_args=()
[[ -n "${SESSION_RUNTIME_IMAGE:-}" ]] && sandbox_args+=(--build-arg "SESSION_RUNTIME_IMAGE=${SESSION_RUNTIME_IMAGE}")
echo_image="${repository}/${name}-echo-mcp:${tag}"

docker build "${bot_args[@]}" --file "${here}/bot.Dockerfile" --tag "${bot}" "${here}" >&2
docker build "${sandbox_args[@]}" --file "${here}/sandbox/Dockerfile" --tag "${sandbox}" "${here}" >&2
docker build --file "${here}/echo-mcp/Dockerfile" --tag "${echo_image}" "${here}/echo-mcp" >&2

if [[ "${PUSH:-}" == "1" ]]; then
  docker push "${bot}" >&2
  docker push "${sandbox}" >&2
  docker push "${echo_image}" >&2
  for image in "${bot}" "${sandbox}" "${echo_image}"; do
    docker inspect --format '{{index .RepoDigests 0}}' "${image}"
  done
else
  echo "${bot}"
  echo "${sandbox}"
  echo "${echo_image}"
fi
