variable "project_id" {
  description = "silk-agent のリソースを作成する Google Cloud プロジェクト ID"
  type        = string
  default     = "nnyn-dev"
}

variable "region" {
  description = "Agent Runtime（Sessions）と Model Armor のリージョン。設定ファイルの値と一致させる。"
  type        = string
  default     = "us-central1"
}

variable "agent_engine_display_name" {
  description = "会話履歴と管理情報を保存する Agent Runtime インスタンスの表示名"
  type        = string
  default     = "silk-agent"
}

variable "model_armor_template_id" {
  description = "出力の sanitize に使う Model Armor テンプレート ID"
  type        = string
  default     = "simple"
}

variable "skills_bucket_name" {
  description = "Skills を置く GCS バケット名（グローバルに一意）"
  type        = string
  default     = "nnyn-dev-silk-agent-skills"
}

variable "slack_secret_ids" {
  description = "Slack トークンを入れる Secret Manager のシークレット ID（値は terraform に含めない）"
  type        = list(string)
  default     = ["silk-agent-slack-bot-token", "silk-agent-slack-app-token"]
}

variable "bot_service_account_id" {
  description = "Bot が使うサービスアカウントのID。Workload Identity などで利用する。"
  type        = string
  default     = "silk-agent-bot"
}
