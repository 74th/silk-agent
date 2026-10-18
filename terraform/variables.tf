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

variable "firestore_backup_bucket_name" {
  description = "Firestore のバックアップ（エクスポート）を置く専用の GCS バケット名（グローバルに一意）。どのワークロードにも権限を与えない。"
  type        = string
  default     = "nnyn-dev-silk-agent-firestore-backup"
}

variable "firestore_backup_retention_days" {
  description = "バックアップのオブジェクトを自動削除する保持日数（コマンド出力より長くする）"
  type        = number
  default     = 90

  validation {
    condition     = var.firestore_backup_retention_days > 0
    error_message = "firestore_backup_retention_days は正の値を指定してください。"
  }
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
    command_output_bucket は、そのユースケースのコマンド出力を保存する GCS バケット名（グローバルに
    一意）。ユースケースごとに別のバケットを作り、そのユースケースの役割だけに権限を与える。
    設定ファイルの distributed.command_output_bucket に一致させる。
  EOT
  type = map(object({
    system_namespace  = string
    sandbox_namespace = string
    firestore_prefix  = string
    slack_secret_ids = object({
      bot_token = string
      app_token = string
    })
    legacy_bot_access     = optional(bool, false)
    command_output_bucket = string
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
      legacy_bot_access     = true
      command_output_bucket = "nnyn-dev-silk-agent-command-output"
    }
    "bigquery-analysis" = {
      system_namespace  = "bigquery-analysis-system"
      sandbox_namespace = "bigquery-analysis-sandboxes"
      firestore_prefix  = "bigquery-analysis"
      slack_secret_ids = {
        bot_token = "bigquery-analysis-slack-bot-token"
        app_token = "bigquery-analysis-slack-app-token"
      }
      command_output_bucket = "nnyn-dev-bigquery-analysis-command-output"
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

  # Every namespace (system and sandbox of all use cases) is used by exactly one use case: a shared
  # Sandbox namespace would overwrite another use case's RoleBindings.
  validation {
    condition = (
      length(distinct(flatten([for u in values(var.usecases) : [u.system_namespace, u.sandbox_namespace]])))
      == 2 * length(var.usecases)
    )
    error_message = "system_namespace と sandbox_namespace は、全ユースケースを通して重複しない値にしてください（同じ namespace を複数のユースケースや用途で共有できません）。"
  }

  # A command output bucket is used by exactly one use case: a shared one would let another use
  # case's Worker read its outputs.
  validation {
    condition     = length(distinct([for u in values(var.usecases) : u.command_output_bucket])) == length(var.usecases)
    error_message = "command_output_bucket はユースケースごとに異なる値にしてください（共有すると別のユースケースのコマンド出力を読めてしまいます）。"
  }

  # Every Slack Secret is used by exactly one role pair of one use case: a shared Secret would let
  # another use case's Gateway / Control read it.
  validation {
    condition = (
      length(distinct(flatten([for u in values(var.usecases) : [u.slack_secret_ids.bot_token, u.slack_secret_ids.app_token]])))
      == 2 * length(var.usecases)
    )
    error_message = "slack_secret_ids の bot_token / app_token は、全ユースケースを通して重複しない値にしてください（別のユースケースの Slack の認証情報を読めてしまいます）。"
  }
}
