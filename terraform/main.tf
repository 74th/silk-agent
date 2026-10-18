provider "google" {
  project = var.project_id
  region  = var.region

  # DLP などは ADC（ユーザ認証）では quota project が必要。
  user_project_override = true
  billing_project       = var.project_id
}

locals {
  services = [
    "aiplatform.googleapis.com",    # Claude / Gemini / Sessions
    "modelarmor.googleapis.com",    # 出力の sanitize
    "firestore.googleapis.com",     # 分散実行の Inbox / ownership / outbox
    "storage.googleapis.com",       # Skills and distributed command output
    "secretmanager.googleapis.com", # Slack トークン
    "dlp.googleapis.com",           # Model Armor の機密データ保護
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.services)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# 分散実行専用DB。既存DBを置換せず、専用IDで分離する。
resource "google_firestore_database" "silk_agent" {
  count = var.firestore_database_mode == "use-existing" ? 0 : 1

  project                           = var.project_id
  name                              = var.firestore_database_id
  location_id                       = var.region
  type                              = "FIRESTORE_NATIVE"
  database_edition                  = "STANDARD"
  concurrency_mode                  = "PESSIMISTIC"
  app_engine_integration_mode       = "DISABLED"
  point_in_time_recovery_enablement = "POINT_IN_TIME_RECOVERY_DISABLED"
  delete_protection_state           = "DELETE_PROTECTION_ENABLED"
  deletion_policy                   = "ABANDON"

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.apis]
}

# conversations/{id}/inputs の delivery_state + seq query 用 composite index。
resource "google_firestore_index" "unfinished_inputs" {
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "inputs"
  query_scope = "COLLECTION"

  fields {
    field_path = "delivery_state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "seq"
    order      = "ASCENDING"
  }

  depends_on = [google_firestore_database.silk_agent]
}

# Due nonterminal command query used by the distributed command Monitor.
resource "google_firestore_index" "due_commands" {
  for_each    = var.usecases
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "${each.value.firestore_prefix}_commands"
  query_scope = "COLLECTION"

  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "next_attempt_at"
    order      = "ASCENDING"
  }

  depends_on = [google_firestore_database.silk_agent]
}

# Conversation stop barriers enumerate accepted command intents by origin sequence.
resource "google_firestore_index" "stop_commands" {
  for_each    = var.usecases
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "${each.value.firestore_prefix}_commands"
  query_scope = "COLLECTION"

  fields {
    field_path = "conversation_key"
    order      = "ASCENDING"
  }
  fields {
    field_path = "conversation_generation"
    order      = "ASCENDING"
  }
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "origin_seq"
    order      = "ASCENDING"
  }

  depends_on = [google_firestore_database.silk_agent]
}

# Per-conversation Sender ordering and exclusive claim for sanitized output.
resource "google_firestore_index" "pending_outbox" {
  for_each    = var.usecases
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "${each.value.firestore_prefix}_outbox"
  query_scope = "COLLECTION"

  fields {
    field_path = "conversation_key"
    order      = "ASCENDING"
  }
  fields {
    field_path = "status"
    order      = "ASCENDING"
  }
  fields {
    field_path = "sequence"
    order      = "ASCENDING"
  }

  depends_on = [google_firestore_database.silk_agent]
}


resource "google_firestore_field" "input_payload_unindexed" {
  project    = var.project_id
  database   = var.firestore_database_id
  collection = "inputs"
  field      = "payload"

  index_config {}

  depends_on = [google_firestore_database.silk_agent]
}

resource "google_firestore_field" "control_payload_unindexed" {
  project    = var.project_id
  database   = var.firestore_database_id
  collection = "controls"
  field      = "payload"

  index_config {}

  depends_on = [google_firestore_database.silk_agent]
}

# list_sender_enrichment_due scans every conversation's inputs (collection group).
resource "google_firestore_field" "input_sender_lookup_group" {
  project    = var.project_id
  database   = var.firestore_database_id
  collection = "inputs"
  field      = "next_sender_lookup_at"

  index_config {
    indexes {
      order       = "ASCENDING"
      query_scope = "COLLECTION"
    }
    indexes {
      order       = "ASCENDING"
      query_scope = "COLLECTION_GROUP"
    }
  }

  depends_on = [google_firestore_database.silk_agent]
}

# 処理済みの記録の保持期限。`expire_at` は終端状態に変わる書き込みで書かれる（アプリ側の
# stores/firestore/retention.py）。所有権や lease の期限 `expires_at` には TTL を付けない。
# TTL の対象フィールドは索引を持たない（index_config {}）。
locals {
  # 会話のサブコレクション（コレクショングループ）
  retention_group_collections = toset(["inputs", "controls"])
  # 接頭辞つきのトップレベルのコレクション
  retention_prefixed_collections = toset([
    "outbox",
    "commands",
    "sandbox_operations",
    "quiesce_requests",
    "slack_receipts",
    "control_queue",
    "workers",
  ])
  # ユースケースごとの接頭辞つきコレクション。キーは "<ユースケース>/<コレクション>"。
  retention_prefixed_bindings = {
    for pair in setproduct(keys(var.usecases), local.retention_prefixed_collections) :
    "${pair[0]}/${pair[1]}" => {
      prefix     = var.usecases[pair[0]].firestore_prefix
      collection = pair[1]
    }
  }
}

resource "google_firestore_field" "expire_at_ttl_group" {
  for_each = local.retention_group_collections

  project    = var.project_id
  database   = var.firestore_database_id
  collection = each.value
  field      = "expire_at"

  ttl_config {}
  index_config {}

  depends_on = [google_firestore_database.silk_agent]
}

resource "google_firestore_field" "expire_at_ttl_prefixed" {
  for_each = local.retention_prefixed_bindings

  project    = var.project_id
  database   = var.firestore_database_id
  collection = "${each.value.prefix}_${each.value.collection}"
  field      = "expire_at"

  ttl_config {}
  index_config {}

  depends_on = [google_firestore_database.silk_agent]
}

moved {
  from = google_firestore_field.input_expire_at_ttl
  to   = google_firestore_field.expire_at_ttl_group["inputs"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["outbox"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/outbox"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["commands"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/commands"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["sandbox_operations"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/sandbox_operations"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["quiesce_requests"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/quiesce_requests"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["slack_receipts"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/slack_receipts"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["control_queue"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/control_queue"]
}

moved {
  from = google_firestore_field.expire_at_ttl_prefixed["workers"]
  to   = google_firestore_field.expire_at_ttl_prefixed["silk-agent/workers"]
}

moved {
  from = google_firestore_index.due_commands
  to   = google_firestore_index.due_commands["silk-agent"]
}

moved {
  from = google_firestore_index.stop_commands
  to   = google_firestore_index.stop_commands["silk-agent"]
}

moved {
  from = google_firestore_index.pending_outbox
  to   = google_firestore_index.pending_outbox["silk-agent"]
}

# Sessions（会話履歴と管理情報）の親になる Agent Runtime インスタンス。エージェントコードは配備しない。
resource "google_vertex_ai_reasoning_engine" "sessions" {
  project      = var.project_id
  region       = var.region
  display_name = var.agent_engine_display_name
  description  = "silk-agent の会話履歴（SessionStore）と管理情報の保存先"

  depends_on = [google_project_service.apis]
}

# 個人情報として検出する情報タイプ。
locals {
  pii_info_types = [
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "CREDIT_CARD_NUMBER",
    "JAPAN_INDIVIDUAL_NUMBER",
    "JAPAN_PASSPORT",
    "JAPAN_BANK_ACCOUNT",
  ]
}

resource "google_data_loss_prevention_inspect_template" "pii" {
  parent       = "projects/${var.project_id}/locations/${var.region}"
  display_name = "silk-agent-pii-inspect"

  inspect_config {
    dynamic "info_types" {
      for_each = local.pii_info_types
      content {
        name = info_types.value
      }
    }
    min_likelihood = "POSSIBLE"
  }

  depends_on = [google_project_service.apis]
}

# 検出した個人情報を、情報タイプ名（例: [EMAIL_ADDRESS]）に置き換える。
resource "google_data_loss_prevention_deidentify_template" "pii" {
  parent       = "projects/${var.project_id}/locations/${var.region}"
  display_name = "silk-agent-pii-deidentify"

  deidentify_config {
    info_type_transformations {
      transformations {
        primitive_transformation {
          replace_with_info_type_config = true
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# 出力の sanitize 用テンプレート。個人情報（SDP）は検出してマスクし、そのほかは検出して差し止める。
resource "google_model_armor_template" "simple" {
  project     = var.project_id
  location    = var.region
  template_id = var.model_armor_template_id

  filter_config {
    rai_settings {
      rai_filters {
        filter_type      = "HATE_SPEECH"
        confidence_level = "MEDIUM_AND_ABOVE"
      }
      rai_filters {
        filter_type      = "HARASSMENT"
        confidence_level = "MEDIUM_AND_ABOVE"
      }
      rai_filters {
        filter_type      = "DANGEROUS"
        confidence_level = "MEDIUM_AND_ABOVE"
      }
      rai_filters {
        filter_type      = "SEXUALLY_EXPLICIT"
        confidence_level = "MEDIUM_AND_ABOVE"
      }
    }
    pi_and_jailbreak_filter_settings {
      filter_enforcement = "ENABLED"
      confidence_level   = "MEDIUM_AND_ABOVE"
    }
    malicious_uri_filter_settings {
      filter_enforcement = "ENABLED"
    }
    sdp_settings {
      # マスクして表示するため、deidentify テンプレートを使う advanced_config にする。
      advanced_config {
        inspect_template    = google_data_loss_prevention_inspect_template.pii.id
        deidentify_template = google_data_loss_prevention_deidentify_template.pii.id
      }
    }
  }

  template_metadata {
    log_sanitize_operations = false
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "skills" {
  project                     = var.project_id
  name                        = var.skills_bucket_name
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  depends_on = [google_project_service.apis]
}

moved {
  from = google_storage_bucket.command_output
  to   = google_storage_bucket.command_output["silk-agent"]
}

# One bucket per use case: its Worker reads it and its Sandbox Service writes it, and no other
# use case's role has any access.
resource "google_storage_bucket" "command_output" {
  for_each                    = var.usecases
  project                     = var.project_id
  name                        = each.value.command_output_bucket
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = var.command_output_retention_days
    }
  }

  depends_on = [google_project_service.apis]
}

# Firestore exports. No workload identity gets any access to this bucket (the exports hold every
# use case's conversations); operators take and read them with their own credentials.
resource "google_storage_bucket" "firestore_backup" {
  project                     = var.project_id
  name                        = var.firestore_backup_bucket_name
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = var.firestore_backup_retention_days
    }
  }

  depends_on = [google_project_service.apis]
}

# トークンの値は terraform に含めない。作成後に `gcloud secrets versions add` で登録する。
locals {
  slack_secret_ids = toset(flatten([
    for u in values(var.usecases) : [u.slack_secret_ids.bot_token, u.slack_secret_ids.app_token]
  ]))
}

resource "google_secret_manager_secret" "slack" {
  for_each  = local.slack_secret_ids
  project   = var.project_id
  secret_id = each.value

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "bot" {
  project      = var.project_id
  account_id   = var.bot_service_account_id
  display_name = "silk-agent bot"
}

locals {
  bot_member = "serviceAccount:${google_service_account.bot.email}"
  bot_roles = [
    "roles/aiplatform.user", # Claude / Gemini / Sessions
    "roles/modelarmor.user", # sanitizeModelResponse
    "roles/datastore.user",  # Firestore distributed state
  ]
}

resource "google_project_iam_member" "bot" {
  for_each = toset(local.bot_roles)
  project  = var.project_id
  role     = each.value
  member   = local.bot_member
}

data "google_project" "current" {
  project_id = var.project_id
}

# The single-process Bot's KSA (<system_namespace>/silk-agent) of the use cases that set
# legacy_bot_access. It replaces the former k8s/grant-iam.sh.
locals {
  legacy_bot_principals = {
    for name, u in var.usecases :
    name => "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/${u.system_namespace}/sa/silk-agent"
    if u.legacy_bot_access
  }
  legacy_bot_roles = {
    for pair in setproduct(keys(local.legacy_bot_principals), local.bot_roles) :
    "${pair[0]}/${pair[1]}" => { member = local.legacy_bot_principals[pair[0]], role = pair[1] }
  }
}

moved {
  from = google_project_iam_member.gke_worker_firestore
  to   = google_project_iam_member.gke_legacy_bot["silk-agent/roles/datastore.user"]
}

resource "google_project_iam_member" "gke_legacy_bot" {
  for_each = local.legacy_bot_roles
  project  = var.project_id
  role     = each.value.role
  member   = each.value.member
}

# Role-specific GKE identities for the distributed deployment (k8s/distributed/base/*.yaml), one
# set per use case (var.usecases). Each KSA gets only what its role needs: the Worker has no Slack
# secret, Control has no Slack app token, and only the Sandbox Service manages Sandbox resources.
# A use case's principals live in its own system namespace, so they never reach another use
# case's Slack secrets.
locals {
  gke_principal_base = "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns"
  gke_principals = {
    for name, u in var.usecases : name => {
      for role in ["gateway", "worker", "control", "sandbox-service"] :
      role => "${local.gke_principal_base}/${u.system_namespace}/sa/silk-agent-${role}"
    }
  }
  gke_role_project_roles = {
    # Claude / Gemini / Sessions, Model Armor, and Firestore state.
    worker          = ["roles/aiplatform.user", "roles/modelarmor.user", "roles/datastore.user"]
    gateway         = ["roles/datastore.user"]
    control         = ["roles/datastore.user"]
    sandbox-service = ["roles/datastore.user"]
  }
  gke_role_project_bindings = merge([
    for name, principals in local.gke_principals : merge([
      for role, roles in local.gke_role_project_roles : {
        for item in roles : "${name}/${role}/${item}" => {
          member = principals[role]
          role   = item
        }
      }
    ]...)
  ]...)
  # Secret IDs by role so a role never receives a token it does not use.
  gke_role_slack_bindings = merge([
    for name, u in var.usecases : {
      "${name}/gateway/${u.slack_secret_ids.bot_token}" = { member = local.gke_principals[name].gateway, secret = u.slack_secret_ids.bot_token }
      "${name}/gateway/${u.slack_secret_ids.app_token}" = { member = local.gke_principals[name].gateway, secret = u.slack_secret_ids.app_token }
      "${name}/control/${u.slack_secret_ids.bot_token}" = { member = local.gke_principals[name].control, secret = u.slack_secret_ids.bot_token }
    }
  ]...)
}

moved {
  from = google_project_iam_member.gke_sandbox_service_firestore
  to   = google_project_iam_member.gke_role["silk-agent/sandbox-service/roles/datastore.user"]
}

moved {
  from = google_project_iam_member.gke_role["worker/roles/aiplatform.user"]
  to   = google_project_iam_member.gke_role["silk-agent/worker/roles/aiplatform.user"]
}

moved {
  from = google_project_iam_member.gke_role["worker/roles/modelarmor.user"]
  to   = google_project_iam_member.gke_role["silk-agent/worker/roles/modelarmor.user"]
}

moved {
  from = google_project_iam_member.gke_role["worker/roles/datastore.user"]
  to   = google_project_iam_member.gke_role["silk-agent/worker/roles/datastore.user"]
}

moved {
  from = google_project_iam_member.gke_role["gateway/roles/datastore.user"]
  to   = google_project_iam_member.gke_role["silk-agent/gateway/roles/datastore.user"]
}

moved {
  from = google_project_iam_member.gke_role["control/roles/datastore.user"]
  to   = google_project_iam_member.gke_role["silk-agent/control/roles/datastore.user"]
}

moved {
  from = google_secret_manager_secret_iam_member.gke_role_slack["gateway/silk-agent-slack-bot-token"]
  to   = google_secret_manager_secret_iam_member.gke_role_slack["silk-agent/gateway/silk-agent-slack-bot-token"]
}

moved {
  from = google_secret_manager_secret_iam_member.gke_role_slack["gateway/silk-agent-slack-app-token"]
  to   = google_secret_manager_secret_iam_member.gke_role_slack["silk-agent/gateway/silk-agent-slack-app-token"]
}

moved {
  from = google_secret_manager_secret_iam_member.gke_role_slack["control/silk-agent-slack-bot-token"]
  to   = google_secret_manager_secret_iam_member.gke_role_slack["silk-agent/control/silk-agent-slack-bot-token"]
}

resource "google_project_iam_member" "gke_role" {
  for_each = local.gke_role_project_bindings
  project  = var.project_id
  role     = each.value.role
  member   = each.value.member
}

resource "google_secret_manager_secret_iam_member" "gke_role_slack" {
  for_each  = local.gke_role_slack_bindings
  project   = var.project_id
  secret_id = google_secret_manager_secret.slack[each.value.secret].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}

moved {
  from = google_storage_bucket_iam_member.gke_worker_output_reader
  to   = google_storage_bucket_iam_member.gke_worker_output_reader["silk-agent"]
}

resource "google_storage_bucket_iam_member" "gke_worker_output_reader" {
  for_each = local.gke_principals
  bucket   = google_storage_bucket.command_output[each.key].name
  role     = "roles/storage.objectViewer"
  member   = each.value["worker"]
}

moved {
  from = google_storage_bucket_iam_member.gke_sandbox_service_output
  to   = google_storage_bucket_iam_member.gke_sandbox_service_output["silk-agent"]
}

resource "google_storage_bucket_iam_member" "gke_sandbox_service_output" {
  for_each = local.gke_principals
  bucket   = google_storage_bucket.command_output[each.key].name
  role     = "roles/storage.objectAdmin"
  member   = each.value["sandbox-service"]
}

moved {
  from = google_storage_bucket_iam_member.gke_worker_skills_reader
  to   = google_storage_bucket_iam_member.gke_worker_skills_reader["silk-agent"]
}

resource "google_storage_bucket_iam_member" "gke_worker_skills_reader" {
  for_each = local.gke_principals
  bucket   = google_storage_bucket.skills.name
  role     = "roles/storage.objectViewer"
  member   = each.value["worker"]
}

resource "google_storage_bucket_iam_member" "skills_reader" {
  bucket = google_storage_bucket.skills.name
  role   = "roles/storage.objectViewer"
  member = local.bot_member
}

moved {
  from = google_storage_bucket_iam_member.gke_worker_command_output
  to   = google_storage_bucket_iam_member.gke_legacy_bot_command_output["silk-agent"]
}

resource "google_storage_bucket_iam_member" "gke_legacy_bot_command_output" {
  for_each = local.legacy_bot_principals
  bucket   = google_storage_bucket.command_output[each.key].name
  role     = "roles/storage.objectAdmin"
  member   = each.value
}

# The single-process Bot's service account only reads the secrets of use cases that opt in.
resource "google_secret_manager_secret_iam_member" "slack_accessor" {
  for_each = toset(flatten([
    for u in values(var.usecases) :
    u.legacy_bot_access ? [u.slack_secret_ids.bot_token, u.slack_secret_ids.app_token] : []
  ]))
  project   = var.project_id
  secret_id = google_secret_manager_secret.slack[each.value].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.bot_member
}
