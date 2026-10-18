resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# Server-side cap on what the agent's SQL can process per day. The agent runs its own queries from
# the Sandbox, so a per-query limit in its code is advice, not a control; the quota is.
# Values are in MiB (the metric's unit).
resource "google_service_usage_consumer_quota_override" "query_usage_per_project" {
  provider       = google-beta
  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project")
  override_value = tostring(var.daily_query_limit_gib * 1024)
  force          = true

  depends_on = [google_project_service.bigquery]
}

resource "google_service_usage_consumer_quota_override" "query_usage_per_user" {
  provider       = google-beta
  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project/user")
  override_value = tostring(var.daily_query_limit_gib * 1024)
  force          = true

  depends_on = [google_project_service.bigquery]
}

resource "google_service_account" "sandbox" {
  project      = var.project_id
  account_id   = var.gsa_account_id
  display_name = "BigQuery analysis sandbox"
}

resource "google_project_iam_member" "job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sandbox.email}"
}

resource "google_service_account_iam_member" "workload_identity_user" {
  service_account_id = google_service_account.sandbox.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.ksa_namespace}/${var.ksa_name}]"
}

output "gsa_email" {
  description = "GSA email to put in the Kubernetes service account annotation."
  value       = google_service_account.sandbox.email
}
