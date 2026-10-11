#!/usr/bin/env bash
# Grants the silk-agent Kubernetes ServiceAccount (Workload Identity Federation for GKE)
# the Google Cloud roles it needs. Idempotent.
set -euo pipefail

project="${GCP_PROJECT_ID:-nnyn-dev}"
number="$(gcloud projects describe "${project}" --format='value(projectNumber)')"
member="principal://iam.googleapis.com/projects/${number}/locations/global/workloadIdentityPools/${project}.svc.id.goog/subject/ns/session-sandbox-system/sa/silk-agent"

# aiplatform.user: Claude / Gemini on Agent Platform and Agent Platform Sessions.
# modelarmor.user: sanitizeModelResponse.
for role in roles/aiplatform.user roles/modelarmor.user; do
  gcloud projects add-iam-policy-binding "${project}" \
    --member="${member}" --role="${role}" --condition=None --quiet >/dev/null
  echo "granted ${role} to ${member}"
done
