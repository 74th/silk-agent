#!/usr/bin/env bash
set -euo pipefail

: "${GKE_PROJECT_ID:?Set GKE_PROJECT_ID explicitly}"
: "${GKE_LOCATION:?Set GKE_LOCATION explicitly}"
: "${SANDBOXD_IMAGE_REPOSITORY:?Set SANDBOXD_IMAGE_REPOSITORY explicitly}"

version="v1.0.6"
runtime_version="${version}-session-runtime"
archive_sha256="8a262b9773036b5a253a0c6d304461cc6bd7e6a68fb0ffb2ad77073d9f1b4a67"
archive_url="https://github.com/kubernetes-sigs/agent-sandbox/archive/refs/tags/${version}.tar.gz"
cache_dir="${XDG_CACHE_HOME:-${TMPDIR:-/tmp}}/agent-sandbox-${version}"
archive="${cache_dir}/source.tar.gz"
source_dir="${cache_dir}/source"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${GKE_LOCATION}-docker.pkg.dev/${GKE_PROJECT_ID}/${SANDBOXD_IMAGE_REPOSITORY}/session-runtime:${runtime_version}"

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
cp "${repo_root}/src/gke_session_sandbox/tracker.py" "${repo_root}/src/gke_session_sandbox/supervisor.py" \
  "${repo_root}/src/gke_session_sandbox/network_probe.py" \
  "${repo_root}/src/gke_session_sandbox/commands.py" "${repo_root}/src/gke_session_sandbox/files.py" \
  "${repo_root}/src/gke_session_sandbox/errors.py" \
  "${runtime_package}/"
cp -R "${repo_root}/src/gke_session_sandbox/_sandboxd_proto" "${runtime_package}/_sandboxd_proto"
printf '"""Minimal runtime package; the management SDK is not loaded in the sandbox."""\n' > "${runtime_package}/__init__.py"

gcloud auth configure-docker "${GKE_LOCATION}-docker.pkg.dev" --quiet
docker build --pull \
  --file "${repo_root}/runtime/session-runtime.Dockerfile" \
  --tag "${image}" "${source_dir}"
docker push "${image}"
digest="$(gcloud artifacts docker images describe "${image}" --format='value(image_summary.digest)')"
if [[ -z "${digest}" ]]; then
  printf 'Artifact Registry did not return a digest for %s\n' "${image}" >&2
  exit 4
fi
printf 'SESSION_RUNTIME_IMAGE=%s@%s\n' "${image%:*}" "${digest}"
