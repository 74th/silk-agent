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
  default     = "bigquery-analysis-sandboxes"
}

variable "ksa_name" {
  description = "Name of the only Kubernetes service account allowed to impersonate the GSA."
  type        = string
  default     = "bigquery-analysis"
}

variable "daily_query_limit_gib" {
  description = "Daily BigQuery query usage (GiB processed) allowed for the project, and for each user (the GSA). A query over it is rejected with quotaExceeded. The quota is project-wide: it also limits other BigQuery use in the same project."
  type        = number
  default     = 100

  validation {
    condition     = var.daily_query_limit_gib > 0
    error_message = "daily_query_limit_gib must be positive."
  }
}
