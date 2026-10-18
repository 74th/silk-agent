output "agent_engine_name" {
  description = "設定ファイルの session_store.agent_runtime に設定する完全リソース名"
  value       = "projects/${var.project_id}/locations/${var.region}/reasoningEngines/${google_vertex_ai_reasoning_engine.sessions.name}"
}

output "model_armor_template" {
  description = "設定ファイルの output_sanitizer.template に設定するテンプレート名"
  value       = google_model_armor_template.simple.id
}

output "skills_bucket" {
  description = "設定ファイルの skills.source に設定するバケット（gs://<名前>）"
  value       = google_storage_bucket.skills.name
}

output "firestore_database_id" {
  description = "分散実行の Firestore database ID"
  value       = var.firestore_database_id
}

output "bot_service_account" {
  description = "Bot が使うサービスアカウント"
  value       = google_service_account.bot.email
}

output "usecases" {
  description = "ユースケースごとの namespace・Firestore の接頭辞・Slack の Secret・各役割の Workload Identity の主体"
  value = {
    for name, u in var.usecases : name => {
      system_namespace      = u.system_namespace
      sandbox_namespace     = u.sandbox_namespace
      firestore_prefix      = u.firestore_prefix
      slack_secret_ids      = u.slack_secret_ids
      command_output_bucket = google_storage_bucket.command_output[name].name
      principals            = local.gke_principals[name]
    }
  }
}

output "firestore_backup_bucket" {
  description = "Firestore のバックアップを置くバケット（運用者だけが使う。`gcloud firestore export gs://<このバケット>/...`）"
  value       = google_storage_bucket.firestore_backup.name
}
