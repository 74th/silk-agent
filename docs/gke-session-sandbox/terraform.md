# Dedicated GKE Terraform root

The dedicated zonal GKE cluster, network, node identity, Artifact Registry, and required project APIs live in `terraform/gke/`. This root has an independent local state and does not read or modify the application resources in `terraform/`.

See the [zonal cluster runbook](zonal-cluster-runbook.md) for input setup, init / validate / plan / apply, credentials, platform installation, image deployment, preflight, destroy planning, and retained disk inventory.

The default cluster is `silk-agent-gke` in `us-central1-a`, with one `system` node and one gVisor `gvisor` node. Set a real operator CIDR and confirm the configured subnet, Pod, Service, and control-plane ranges do not overlap existing networks before applying.
