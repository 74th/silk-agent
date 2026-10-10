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
    "storage.googleapis.com",       # Skills
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
  ]
}

resource "google_project_iam_member" "bot" {
  for_each = toset(local.bot_roles)
  project  = var.project_id
  role     = each.value
  member   = local.bot_member
}

resource "google_storage_bucket_iam_member" "skills_reader" {
  bucket = google_storage_bucket.skills.name
  role   = "roles/storage.objectViewer"
  member = local.bot_member
}

resource "google_secret_manager_secret_iam_member" "slack_accessor" {
  for_each  = google_secret_manager_secret.slack
  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.bot_member
}
