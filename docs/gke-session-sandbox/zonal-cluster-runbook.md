# silk-agent zonal GKE runbook

This runbook provisions the dedicated validation cluster from `terraform/gke/`. Its local Terraform state is independent of the application resources in `terraform/`. The default cluster is `silk-agent-gke` in `us-central1-a`. Its `system` and gVisor `gvisor` node pools each autoscale from one to ten nodes (two to twenty nodes total).

## Configure and review Terraform

Copy the example inputs and set `operator_cidrs` to the operator's actual public egress CIDR. Check that the primary subnet, Pod, Service, and control-plane CIDRs do not overlap networks already used by the project.

```sh
cp terraform/gke/terraform.tfvars.example terraform/gke/terraform.tfvars
${EDITOR:-vi} terraform/gke/terraform.tfvars
terraform -chdir=terraform/gke init
terraform -chdir=terraform/gke fmt -check -recursive
terraform -chdir=terraform/gke validate
terraform -chdir=terraform/gke plan -var-file=terraform.tfvars -out=environment.tfplan
terraform -chdir=terraform/gke show environment.tfplan
```

Review the project, zone, all CIDRs, authorized control-plane ranges, resource names, and the autoscaling bounds of both node pools. Each pool scales down to one node when its workloads can fit there; the cluster never scales below two nodes. This plan must contain only resources from the dedicated root. Apply only the reviewed plan:

```sh
terraform -chdir=terraform/gke apply environment.tfplan
terraform -chdir=terraform/gke output
```

Terraform outputs identify the project, cluster, zone, node pools, Artifact Registry URL, expected kube context, and exact `gcloud container clusters get-credentials` command. Inspect and run the printed credentials command, then verify `kubectl config current-context` equals the `kube_context` output before proceeding.

## Install Kubernetes platform resources

The pinned Agent Sandbox v1.0.6 controller and its CRD are in `k8s/platform/vendor/agent-sandbox-v1.0.6.yaml`. Apply the platform bundle first; it creates the controller namespace, Bot and Sandbox namespaces, namespace-scoped Sandbox management RBAC, and `session-workspace-rwo` StorageClass. GKE supplies the `gvisor` RuntimeClass when the gVisor node pool is ready.

```sh
kubectl kustomize k8s/platform | kubectl apply -f -
kubectl kustomize k8s/distributed/overlays/dev-platform | kubectl apply -f -   # dev namespaces and the sandbox-management Role
kubectl -n agent-sandbox-system rollout status deployment/agent-sandbox-controller --timeout=300s
kubectl get crd sandboxes.agents.x-k8s.io
kubectl get runtimeclass gvisor
```

The Bot Kubernetes ServiceAccount gets its Google API roles through Workload Identity from the
root Terraform (`terraform -chdir=terraform apply`, the `usecases` variable). This replaces the former
`k8s/grant-iam.sh`.

## Build images and deploy the Bot

The Bot and Sandbox runtime repositories are based on the Terraform `artifact_registry_url` output. Build and push the session runtime, keep its digest, then deploy. The deploy script injects the explicit runtime digest into the generated ConfigMap and builds the Bot image from the same registry.

```sh
SESSION_RUNTIME_IMAGE="$(scripts/build-session-runtime-image.sh | sed -n 's/^SESSION_RUNTIME_IMAGE=//p')"
export SANDBOX_RUNTIME_IMAGE="${SESSION_RUNTIME_IMAGE}"
./k8s/deploy.sh
kubectl -n session-sandbox-system rollout status deployment/silk-agent --timeout=300s
```

For a use case overlay, build and push its image to the Terraform repository, then set `KUSTOMIZE_DIR=usecases/<name>/k8s` when running `k8s/deploy.sh`. The use case's kustomization owns its image name.

## Preflight and Sandbox acceptance

Run preflight against the new cluster before starting Bot traffic. Then create and stop a small Sandbox through the session manager to check controller reconciliation, gVisor scheduling, workspace provisioning, and cleanup.

```sh
uv run gke-session-sandbox preflight --config examples/sandbox-config.yaml
SANDBOX_RUNTIME_IMAGE="${SANDBOX_RUNTIME_IMAGE:?build and push the session runtime first}" uv run python - <<'PY'
import asyncio
import os
import shutil
import tempfile
from pathlib import Path

import yaml

from gke_session_sandbox import SandboxManager, SessionKey


async def main():
    temp_dir = Path(tempfile.mkdtemp(prefix="gke-sandbox-acceptance-"))
    config = yaml.safe_load(Path("examples/sandbox-config.yaml").read_text())
    config["runtime"]["image"] = os.environ["SANDBOX_RUNTIME_IMAGE"]
    config["repository_database_path"] = str(temp_dir / "sessions.sqlite3")
    config["repository_require_mount"] = False
    config["template_file"] = str(Path("templates/sandbox.yaml").resolve())
    config_path = temp_dir / "sandbox-config.yaml"
    config_path.write_text(yaml.safe_dump(config))
    try:
        async with await SandboxManager.from_config(config_path) as manager:
            key = SessionKey(tenant_id="gke-acceptance", session_id="create-and-terminate")
            session = await manager.ensure(key)
            print(f"created {session.sandbox_ref.name}")
            session = await manager.terminate(session, reason="infrastructure-acceptance")
            print(f"terminated {session.state.value}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


asyncio.run(main())
PY
```

The live-cluster preflight and Sandbox lifecycle acceptance are not run by repository-local validation. Record their actual output and date in the validation notes; do not report them as complete without a cluster run.

## Destroy and retained storage

Destroy uses only the state in `terraform/gke/`. Review and save the destroy plan before applying it:

```sh
terraform -chdir=terraform/gke plan -destroy -var-file=terraform.tfvars -out=destroy.tfplan
terraform -chdir=terraform/gke show destroy.tfplan
```

Confirm the plan targets only the dedicated VPC, subnet, router/NAT, cluster and node pools, node service account and its member-level role grants, Artifact Registry repository, and API state entries with `disable_on_destroy = false`. Before applying, inspect Kubernetes PVCs/PVs and Compute Engine disks. The Bot state StorageClass uses `Retain`; those disks can remain after the cluster is deleted. Record retained disks and delete them manually only after confirming their contents and ownership.

```sh
kubectl get pvc,pv -A -o wide
gcloud compute disks list --filter="zone:$(terraform -chdir=terraform/gke output -raw zone)"
```

Apply the reviewed destroy plan only after recording the remaining storage inventory:

```sh
terraform -chdir=terraform/gke apply destroy.tfplan
```
