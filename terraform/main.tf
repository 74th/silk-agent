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
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "silk-agent_commands"
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
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "silk-agent_commands"
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
  project     = var.project_id
  database    = var.firestore_database_id
  collection  = "silk-agent_outbox"
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

resource "google_firestore_field" "input_expire_at_ttl" {
  project    = var.project_id
  database   = var.firestore_database_id
  collection = "inputs"
  field      = "expire_at"

  ttl_config {}
  index_config {}

  depends_on = [google_firestore_database.silk_agent]
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

resource "google_storage_bucket" "command_output" {
  project                     = var.project_id
  name                        = var.command_output_bucket_name
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

# トークンの値は terraform に含めない。作成後に `gcloud secrets versions add` で登録する。
resource "google_secret_manager_secret" "slack" {
  for_each  = toset(var.slack_secret_ids)
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

# Existing GKE workload identity uses the session-sandbox-system/silk-agent KSA
# directly (see k8s/grant-iam.sh). Give it only the Firestore data role needed
# for the live distributed-store smoke check and application runtime.
data "google_project" "current" {
  project_id = var.project_id
}

resource "google_project_iam_member" "gke_worker_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/session-sandbox-system/sa/silk-agent"
}

# Sandbox Service keeps the Worker-only Firestore reader/writer access separate from Slack.
resource "google_project_iam_member" "gke_sandbox_service_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/session-sandbox-system/sa/silk-agent-sandbox-service"
}

# Role-specific GKE identities for the distributed deployment (k8s/distributed/roles.yaml).
# Each KSA gets only what its role needs: the Worker has no Slack secret, Control has no Slack
# app token, and only the Sandbox Service (above) manages Sandbox resources.
locals {
  gke_principal_prefix = "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/session-sandbox-system/sa"
  gke_role_project_roles = {
    # Claude / Gemini / Sessions, Model Armor, and Firestore state.
    worker  = ["roles/aiplatform.user", "roles/modelarmor.user", "roles/datastore.user"]
    gateway = ["roles/datastore.user"]
    control = ["roles/datastore.user"]
  }
  gke_role_project_bindings = merge([
    for role, roles in local.gke_role_project_roles : {
      for item in roles : "${role}/${item}" => {
        member = "${local.gke_principal_prefix}/silk-agent-${role}"
        role   = item
      }
    }
  ]...)
  # Secret IDs by name so a role never receives a token it does not use.
  gke_role_slack_secrets = {
    gateway = ["silk-agent-slack-bot-token", "silk-agent-slack-app-token"]
    control = ["silk-agent-slack-bot-token"]
  }
  gke_role_slack_bindings = merge([
    for role, secrets in local.gke_role_slack_secrets : {
      for secret in secrets : "${role}/${secret}" => {
        member = "${local.gke_principal_prefix}/silk-agent-${role}"
        secret = secret
      }
    }
  ]...)
}

resource "google_project_iam_member" "gke_role" {
  for_each = local.gke_role_project_bindings
  project  = var.project_id
  role     = each.value.role
  member   = each.value.member
}

resource "google_secret_manager_secret_iam_member" "gke_role_slack" {
  for_each  = { for key, item in local.gke_role_slack_bindings : key => item if contains(var.slack_secret_ids, item.secret) }
  project   = var.project_id
  secret_id = google_secret_manager_secret.slack[each.value.secret].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}

# The Worker reads archived command output (the Sandbox Service writes it) and Skills.
resource "google_storage_bucket_iam_member" "gke_worker_output_reader" {
  bucket = google_storage_bucket.command_output.name
  role   = "roles/storage.objectViewer"
  member = "${local.gke_principal_prefix}/silk-agent-worker"
}

# The Sandbox Service's command monitor archives terminal command output.
resource "google_storage_bucket_iam_member" "gke_sandbox_service_output" {
  bucket = google_storage_bucket.command_output.name
  role   = "roles/storage.objectAdmin"
  member = "${local.gke_principal_prefix}/silk-agent-sandbox-service"
}

resource "google_storage_bucket_iam_member" "gke_worker_skills_reader" {
  bucket = google_storage_bucket.skills.name
  role   = "roles/storage.objectViewer"
  member = "${local.gke_principal_prefix}/silk-agent-worker"
}

resource "google_storage_bucket_iam_member" "skills_reader" {
  bucket = google_storage_bucket.skills.name
  role   = "roles/storage.objectViewer"
  member = local.bot_member
}

resource "google_storage_bucket_iam_member" "gke_worker_command_output" {
  bucket = google_storage_bucket.command_output.name
  role   = "roles/storage.objectAdmin"
  member = "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/session-sandbox-system/sa/silk-agent"
}

resource "google_secret_manager_secret_iam_member" "slack_accessor" {
  for_each  = google_secret_manager_secret.slack
  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.bot_member
}
