# Dedicated GKE destroy inventory

Inventory captured on 2026-10-11 for project `nnyn-dev`, region `us-central1`, cluster `research-gke-agent-sandbox-with-sandboxd`. The dedicated environment remains deployed for further acceptance work; no destroy was applied.

The live-refreshed `terraform plan -destroy` produced 20 deletes and no creates or updates. The targets are the dedicated GKE cluster and its two node pools, VPC/subnet/router/NAT, Artifact Registry repository, node service account and its four member-scoped IAM role grants, plus seven `google_project_service` state entries. Every API entry has `disable_on_destroy=false`, so removing this Terraform state does not disable project APIs. The four IAM grants apply only to `research-gke-as-sbd-node@nnyn-dev.iam.gserviceaccount.com`. No shared project IAM policy binding, common Terraform state, or adjacent validation resource appears in the plan.

The live Kubernetes volume inventory contained two claims in `session-sandbox-fixtures`: `runtime-code-rom` is bound to `runtime-code-rom-pv` with `Retain`, and `workspace-validation` is bound to a Delete-reclaim `session-workspace-rwo` PV. Their backing disks were present in `us-central1-f`. The retained ROM disk is intentionally shared fixture data and is not a Terraform destroy target. The latest pytest run `9a29229094e8` removed its run namespace and claims; no `gke-session-sandbox-validation` namespaces, test Jobs, or session PVCs remained afterward.

The Compute disk inventory also showed the cluster's six node boot disks and one disk named `gke-nagisa-f0e0587a-dy-pvc-5d224ae9-694d-11e9-825c-42010a80005c`, which is not referenced by this cluster's PVs and is not included in this destroy plan. It belongs outside this validation's ownership boundary and was left unchanged. The GKE API resources, node disks, and project API enablement were not destroyed during this inventory.

To reproduce the inventory, run `terraform -chdir=terraform plan -destroy -out=/tmp/destroy.tfplan`, then `terraform -chdir=terraform show -no-color /tmp/destroy.tfplan`. This records what a destroy would remove; it does not perform the destroy.
