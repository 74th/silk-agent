output "project_id" {
  description = "Project that owns the dedicated GKE environment."
  value       = var.project_id
}

output "cluster_name" {
  description = "Dedicated zonal cluster name."
  value       = google_container_cluster.gke.name
}

output "zone" {
  description = "Single zone used by the cluster and node pools."
  value       = var.zone
}

output "region" {
  description = "Registry and VPC region derived from the selected zone."
  value       = local.region
}

output "node_pool_names" {
  description = "Fixed-size node pool names."
  value       = [google_container_node_pool.system.name, google_container_node_pool.gvisor.name]
}

output "artifact_registry_url" {
  description = "Docker repository URL for Bot and Sandbox runtime images."
  value       = "${local.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "get_credentials_command" {
  description = "Command to configure kubectl for this cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.gke.name} --zone ${var.zone} --project ${var.project_id}"
}
output "kube_context" {
  description = "Expected kubeconfig context for deployment checks."
  value       = "gke_${var.project_id}_${var.zone}_${google_container_cluster.gke.name}"
}
