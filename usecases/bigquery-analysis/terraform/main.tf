resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
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
