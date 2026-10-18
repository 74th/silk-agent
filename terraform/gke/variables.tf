variable "project_id" {
  description = "Google Cloud project that owns the dedicated test cluster."
  type        = string
  default     = "nnyn-dev"
}

variable "zone" {
  description = "Single Compute Engine zone for the cluster and both node pools."
  type        = string
  default     = "us-central1-a"
}

variable "cluster_name" {
  description = "Name of the dedicated zonal GKE cluster."
  type        = string
  default     = "silk-agent-gke"
}

variable "operator_cidrs" {
  description = "IPv4 CIDRs allowed to reach the GKE control plane. Use narrow operator egress ranges."
  type        = list(string)

  validation {
    condition = length(var.operator_cidrs) > 0 && alltrue([
      for cidr in var.operator_cidrs : can(cidrnetmask(cidr)) && can(regex("/(2[0-9]|3[0-2])$", cidr))
    ])
    error_message = "operator_cidrs must contain IPv4 ranges from /20 through /32; broad or empty ranges are not allowed."
  }
}

variable "network_name" {
  description = "Name of the VPC dedicated to the GKE test environment."
  type        = string
  default     = "silk-agent-gke-vpc"
}

variable "subnet_name" {
  description = "Name of the GKE subnet."
  type        = string
  default     = "silk-agent-gke-subnet"
}

variable "subnet_cidr" {
  description = "Primary IPv4 range for the GKE subnet."
  type        = string
  default     = "192.168.0.0/20"
}

variable "pods_cidr" {
  description = "Secondary IPv4 range for GKE Pods."
  type        = string
  default     = "192.168.32.0/19"
}

variable "services_cidr" {
  description = "Secondary IPv4 range for GKE Services."
  type        = string
  default     = "192.168.64.0/20"
}

variable "master_cidr" {
  description = "Private control-plane IPv4 range."
  type        = string
  default     = "172.20.0.0/28"
}

variable "artifact_repository_id" {
  description = "Artifact Registry repository for Bot and Sandbox runtime images."
  type        = string
  default     = "silk-agent-images"
}

variable "node_machine_type" {
  description = "Machine type used by both autoscaling node pools."
  type        = string
  default     = "e2-standard-2"
}

variable "enable_node_local_dns_cache" {
  description = "Enable NodeLocal DNSCache. The default matches the existing cluster; Sandbox DNS egress allows both kube-dns and node-local-dns either way."
  type        = bool
  default     = true
}
