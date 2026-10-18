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

variable "firestore_database_mode" {
  description = "Firestore database の扱い: create、use-existing、import-existing。"
  type        = string
  default     = "create"

  validation {
    condition     = contains(["create", "use-existing", "import-existing"], var.firestore_database_mode)
    error_message = "firestore_database_mode は create、use-existing、import-existing のいずれかです。"
  }
}

variable "usecases" {
  description = <<-EOT
    ユースケースの一覧（キーはユースケース名）。ユースケースごとに、役割の Workload Identity の権限、
    Slack の Secret（値は terraform に含めない）、Firestore の索引と保持期限（TTL）を作る。
    system_namespace / sandbox_namespace / firestore_prefix は、そのユースケースの
    k8s の overlay と設定ファイルの distributed.firestore_prefix に一致させる。
    legacy_bot_access は単一プロセスの Bot 用サービスアカウントに Slack の Secret の読み取りを許す
    （開発環境だけ true）。
  EOT
  type = map(object({
    system_namespace  = string
    sandbox_namespace = string
    firestore_prefix  = string
    slack_secret_ids = object({
      bot_token = string
      app_token = string
    })
    legacy_bot_access = optional(bool, false)
  }))
  default = {
    "silk-agent" = {
      system_namespace  = "session-sandbox-system"
      sandbox_namespace = "session-sandboxes"
      firestore_prefix  = "silk-agent"
      slack_secret_ids = {
        bot_token = "silk-agent-slack-bot-token"
        app_token = "silk-agent-slack-app-token"
      }
      legacy_bot_access = true
    }
  }

  validation {
    condition = alltrue([
      for u in values(var.usecases) :
      length(u.firestore_prefix) > 0 && !strcontains(u.firestore_prefix, "/")
    ])
    error_message = "firestore_prefix は空でなく、'/' を含まない値を指定してください。"
  }

  validation {
    condition     = length(distinct([for u in values(var.usecases) : u.firestore_prefix])) == length(var.usecases)
    error_message = "firestore_prefix はユースケースごとに異なる値にしてください。"
  }

  validation {
    condition     = length(distinct([for u in values(var.usecases) : u.system_namespace])) == length(var.usecases)
    error_message = "system_namespace はユースケースごとに異なる値にしてください。"
  }

  validation {
    condition = alltrue([
      for u in values(var.usecases) : u.system_namespace != u.sandbox_namespace
    ])
    error_message = "system_namespace と sandbox_namespace は別の値にしてください。"
  }
}
