variable "project_id" {
  description = "Project where BigQuery jobs run and the dedicated GSA is created."
  type        = string
}

variable "region" {
  description = "Provider region."
  type        = string
  default     = "asia-northeast1"
}

variable "gsa_account_id" {
  description = "Account ID for the dedicated BigQuery Google service account."
  type        = string
  default     = "bigquery-analysis-sandbox"
}

variable "ksa_namespace" {
  description = "Namespace containing the BigQuery sandbox Kubernetes service account."
  type        = string
  default     = "session-sandboxes"
}

variable "ksa_name" {
  description = "Name of the only Kubernetes service account allowed to impersonate the GSA."
  type        = string
  default     = "bigquery-analysis"
}
