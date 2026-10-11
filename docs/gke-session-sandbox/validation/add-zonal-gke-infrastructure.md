# Zonal GKE infrastructure acceptance

Checked against Google Cloud and the live Kubernetes cluster on 2026-10-11 after applying `terraform/gke/`.

| Check | Result |
| --- | --- |
| Project / zone | `nnyn-dev` / `us-central1-a` |
| Cluster | `silk-agent-gke` (`RUNNING`) |
| VPC / subnet | `silk-agent-gke-vpc` / `silk-agent-gke-subnet` |
| Nodes | `system` and `gvisor`, one `e2-standard-2` node each; both `Ready` |
| Private nodes | Enabled; node external IPs are absent |
| Control plane access | Authorized operator CIDR `118.158.236.102/32`; private endpoint enforcement enabled |
| Workload Identity | `nnyn-dev.svc.id.goog` |
| Platform | Agent Sandbox v1.0.6 controller rollout succeeded; `gvisor` RuntimeClass and workspace StorageClass present |
| Preflight | Passed, including required namespaces, CRD, RuntimeClass, StorageClass, and operator RBAC |
| Sandbox lifecycle | Created `session-0eee08dc71-g1`, observed it Ready on the gVisor node with its 10 GiB PVC Bound, then terminated it through `SandboxManager` |
| Cleanup | The acceptance Sandbox and workspace PVC were removed; `session-sandboxes` has no remaining resources |
| Bot deployment | `silk-agent` rollout succeeded with image digest `sha256:74210f3a306fa3c929fee868db09c305e666a175508ec003e14d421f0e052299` |
| Runtime image | `sha256:c2e5710e59791ec80adf3ede12347d68d8ce131aec31082a9241b42016113573` |

Terraform applied 19 resources with zero updates and zero deletes. The live cluster has two Ready nodes. All pods listed in `kubectl get pods -A` were Running at the time of verification; the `session-sandboxes` namespace was empty after acceptance cleanup.
