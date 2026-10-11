#!/usr/bin/env bash
# Build and push the Sandbox Pod base image (sandboxd + the Python command tracker).
# The tag is <agent-sandbox version>-session-runtime-<silk-agent version>.
# SESSION_RUNTIME_BUILD_ONLY=1 builds the image locally (no gcloud, no push) and prints its tag.
set -euo pipefail

build_only="${SESSION_RUNTIME_BUILD_ONLY:-}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gke_root="${repo_root}/terraform/gke"
if [[ -z "${build_only}" ]]; then
  GKE_PROJECT_ID="${GKE_PROJECT_ID:-$(terraform -chdir="${gke_root}" output -raw project_id)}"
  GKE_LOCATION="${GKE_LOCATION:-$(terraform -chdir="${gke_root}" output -raw region)}"
  SANDBOXD_IMAGE_REPOSITORY="${SANDBOXD_IMAGE_REPOSITORY:-$(terraform -chdir="${gke_root}" output -raw artifact_registry_url | sed 's#^[^/]*/[^/]*/##')}"
  export GKE_PROJECT_ID GKE_LOCATION SANDBOXD_IMAGE_REPOSITORY
fi

version="v1.0.6"
archive_sha256="8a262b9773036b5a253a0c6d304461cc6bd7e6a68fb0ffb2ad77073d9f1b4a67"
archive_url="https://github.com/kubernetes-sigs/agent-sandbox/archive/refs/tags/${version}.tar.gz"
cache_dir="${XDG_CACHE_HOME:-${TMPDIR:-/tmp}}/agent-sandbox-${version}"
archive="${cache_dir}/source.tar.gz"
source_dir="${cache_dir}/source"
silk_agent_version="$(python3 -c "import sys, tomllib; print(tomllib.load(open(sys.argv[1], 'rb'))['project']['version'])" "${repo_root}/pyproject.toml")"
runtime_version="${version}-session-runtime-${silk_agent_version}"
if [[ -n "${build_only}" ]]; then
  image="session-runtime:${runtime_version}"
else
  image="${GKE_LOCATION}-docker.pkg.dev/${GKE_PROJECT_ID}/${SANDBOXD_IMAGE_REPOSITORY}/session-runtime:${runtime_version}"
fi

mkdir -p "${cache_dir}"
if [[ ! -f "${archive}" ]] || [[ "$(sha256sum "${archive}" | cut -d' ' -f1)" != "${archive_sha256}" ]]; then
  curl --fail --location --retry 3 "${archive_url}" --output "${archive}"
fi
echo "${archive_sha256}  ${archive}" | sha256sum --check --status || {
  printf 'Source archive checksum mismatch for %s\n' "${archive}" >&2
  exit 3
}
if [[ ! -d "${source_dir}/packages/sandboxd" ]]; then
  rm -rf "${source_dir}"
  mkdir -p "${source_dir}"
  tar -xzf "${archive}" --strip-components=1 -C "${source_dir}"
fi
mkdir -p "${source_dir}/runtime"
mkdir -p "${source_dir}/runtime/python"
rm -rf "${source_dir}/runtime/python/gke_session_sandbox"
runtime_package="${source_dir}/runtime/python/gke_session_sandbox"
mkdir -p "${runtime_package}"
# The modules and packages that run in the Pod are listed in one place (see that file).
while IFS= read -r entry; do
  if [[ "${entry}" == */ ]]; then
    cp -R "${repo_root}/src/gke_session_sandbox/${entry%/}" "${runtime_package}/${entry%/}"
  else
    cp "${repo_root}/src/gke_session_sandbox/${entry}" "${runtime_package}/"
  fi
done < <(python3 "${repo_root}/src/gke_session_sandbox/_runtime_modules.py")
# Pin the image's Python dependencies to the versions in uv.lock (the generated sandboxd code
# was built against them), including transitive ones; the Dockerfile uses it as constraints.
uv export --project "${repo_root}" --frozen --no-dev --no-hashes --no-emit-project --no-header \
  > "${source_dir}/runtime/constraints.txt"
printf '"""Minimal runtime package; the management SDK is not loaded in the sandbox."""\n' > "${runtime_package}/__init__.py"

if [[ -z "${build_only}" ]]; then
  gcloud auth configure-docker "${GKE_LOCATION}-docker.pkg.dev" --quiet
fi
docker build --pull \
  --file "${repo_root}/runtime/session-runtime.Dockerfile" \
  --tag "${image}" "${source_dir}"
if [[ -n "${build_only}" ]]; then
  printf 'SESSION_RUNTIME_IMAGE=%s\n' "${image}"
  exit 0
fi
docker push "${image}"
digest="$(gcloud artifacts docker images describe "${image}" --format='value(image_summary.digest)')"
if [[ -z "${digest}" ]]; then
  printf 'Artifact Registry did not return a digest for %s\n' "${image}" >&2
  exit 4
fi
printf 'SESSION_RUNTIME_IMAGE=%s@%s\n' "${image%:*}" "${digest}"
