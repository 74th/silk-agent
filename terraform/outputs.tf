output "agent_engine_name" {
  description = "設定ファイルの sessions.agent_engine に設定する完全リソース名"
  value       = "projects/${var.project_id}/locations/${var.region}/reasoningEngines/${google_vertex_ai_reasoning_engine.sessions.name}"
}

output "model_armor_template" {
  description = "設定ファイルの output_sanitizer.template に設定するテンプレート名"
  value       = google_model_armor_template.simple.id
}

output "skills_bucket" {
  description = "設定ファイルの skills.bucket に設定するバケット名"
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
