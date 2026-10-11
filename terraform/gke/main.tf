resource "google_project_service" "apis" {
  for_each           = local.services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_compute_network" "gke" {
  project                 = var.project_id
  name                    = var.network_name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"

  depends_on = [google_project_service.apis]
}

resource "google_compute_subnetwork" "gke" {
  project                  = var.project_id
  name                     = var.subnet_name
  region                   = local.region
  network                  = google_compute_network.gke.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }
}

resource "google_compute_router" "gke" {
  project = var.project_id
  name    = "${var.cluster_name}-router"
  region  = local.region
  network = google_compute_network.gke.id
}

resource "google_compute_router_nat" "gke" {
  project                            = var.project_id
  name                               = "${var.cluster_name}-nat"
  router                             = google_compute_router.gke.name
  region                             = local.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

resource "google_service_account" "nodes" {
  project      = var.project_id
  account_id   = "${var.cluster_name}-nodes"
  display_name = "${var.cluster_name} GKE nodes"
}

resource "google_project_iam_member" "node_roles" {
  for_each = toset([
    "roles/artifactregistry.reader",
    "roles/container.defaultNodeServiceAccount",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.nodes.email}"
}

resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = local.region
  repository_id = var.artifact_repository_id
  description   = "Bot and Agent Sandbox images for ${var.cluster_name}"
  format        = "DOCKER"

  depends_on = [google_project_service.apis]
}

resource "google_container_cluster" "gke" {
  project                     = var.project_id
  name                        = var.cluster_name
  location                    = var.zone
  network                     = google_compute_network.gke.name
  subnetwork                  = google_compute_subnetwork.gke.name
  networking_mode             = "VPC_NATIVE"
  remove_default_node_pool    = true
  initial_node_count          = 1
  deletion_protection         = false
  enable_intranode_visibility = true
  enable_fqdn_network_policy  = true
  datapath_provider           = "ADVANCED_DATAPATH"

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = var.master_cidr
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.operator_cidrs
      content {
        cidr_block   = cidr_blocks.value
        display_name = "operator-${cidr_blocks.key + 1}"
      }
    }
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  release_channel {
    channel = "REGULAR"
  }

  depends_on = [
    google_project_service.apis,
    google_compute_router_nat.gke,
  ]
}

resource "google_container_node_pool" "system" {
  project    = var.project_id
  name       = "system"
  location   = var.zone
  cluster    = google_container_cluster.gke.name
  node_count = 1

  depends_on = [google_project_iam_member.node_roles]
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = var.node_machine_type
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    disk_type       = "pd-balanced"
    disk_size_gb    = 50
    image_type      = "COS_CONTAINERD"
    labels = {
      "silk-agent.io/pool" = "system"
    }
  }
}

resource "google_container_node_pool" "gvisor" {
  project    = var.project_id
  name       = "gvisor"
  location   = var.zone
  cluster    = google_container_cluster.gke.name
  node_count = 1

  depends_on = [google_project_iam_member.node_roles]
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = var.node_machine_type
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    disk_type       = "pd-balanced"
    disk_size_gb    = 50
    image_type      = "COS_CONTAINERD"

    sandbox_config {
      type = "GVISOR"
    }

    labels = {
      "silk-agent.io/pool" = "gvisor"
    }
  }
}
