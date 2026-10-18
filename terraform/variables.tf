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
  default     = "silk-agent-test"
}

variable "skills_bucket_name" {
  description = "Skills を置く GCS バケット名（グローバルに一意）"
  type        = string
  default     = "nnyn-dev-silk-agent-skills"
}

variable "command_output_bucket_name" {
  description = "分散コマンド出力を保存する GCS バケット名（グローバルに一意）"
  type        = string
  default     = "nnyn-dev-silk-agent-command-output"
}

variable "command_output_retention_days" {
  description = "コマンド出力オブジェクトを自動削除する保持日数"
  type        = number
  default     = 30

  validation {
    condition     = var.command_output_retention_days > 0
    error_message = "command_output_retention_days は正の値を指定してください。"
  }
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

variable "firestore_database_id" {
  description = "分散実行用の Firestore database ID。use-existing モードでは既存IDを指定する。"
  type        = string
  default     = "silk-agent"
}

variable "firestore_prefix" {
  description = "分散実行の Firestore コレクション名の接頭辞。設定ファイルの distributed.firestore_prefix と一致させる。"
  type        = string
  default     = "silk-agent"

  validation {
    condition     = length(var.firestore_prefix) > 0 && !strcontains(var.firestore_prefix, "/")
    error_message = "firestore_prefix は空でなく、'/' を含まない値を指定してください。"
  }
}

variable "firestore_database_mode" {
  description = "Firestore database の扱い: create、use-existing、import-existing。"
  type        = string
  default     = "create"

  validation {
    condition     = contains(["create", "use-existing", "import-existing"], var.firestore_database_mode)
    error_message = "firestore_database_mode は create、use-existing、import-existing のいずれかです。"
  }
}
