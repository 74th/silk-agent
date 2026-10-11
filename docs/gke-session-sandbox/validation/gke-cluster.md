# GKE cluster provisioning record

Checked against the Google Cloud API on 2026-10-10 after applying the isolated Terraform plan.

| Property | Observed value |
| --- | --- |
| Project / region | `nnyn-dev` / `us-central1` |
| Cluster | `research-gke-agent-sandbox-with-sandboxd` |
| GKE version | `1.35.8-gke.1225000` |
| VPC / subnet | `research-gke-agent-sandbox-with-sandboxd-vpc` / `research-gke-agent-sandbox-with-sandboxd-subnet` |
| Node pools | `research-gke-as-sbd-system`, `research-gke-as-sbd-gvisor` |
| gVisor pool | `COS_CONTAINERD`, sandbox type `GVISOR`, GKE-managed runtime label and `NO_SCHEDULE` taint |
| Dataplane | `ADVANCED_DATAPATH` (Dataplane V2) |
| FQDN Network Policy | enabled |
| Workload Identity | `nnyn-dev.svc.id.goog` |
| PD CSI driver | enabled |
| Terraform provider | `hashicorp/google` 7.29.0 |

The initial plan contained 20 create actions, zero updates, and zero deletes. The failed first gVisor node-pool request used a GKE-managed taint; removing the manual taint allowed GKE to create it with the required label and taint automatically. No other project resources were changed by Terraform.
