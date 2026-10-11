terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.40"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = local.region
}

locals {
  region = join("-", slice(split("-", var.zone), 0, 2))
  services = toset([
    "artifactregistry.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
  ])
}
