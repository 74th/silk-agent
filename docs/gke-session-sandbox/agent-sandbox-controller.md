# Agent Sandbox controller and platform

The repository vendors the upstream core install manifest for Agent Sandbox v1.0.6 at `k8s/platform/vendor/agent-sandbox-v1.0.6.yaml`. The manifest installs the core `agents.x-k8s.io/v1beta1` API and controller. The same Kustomize bundle defines the Bot and Sandbox namespaces, the Sandbox management Role used by the Bot, and the `session-workspace-rwo` StorageClass. GKE provides the `gvisor` RuntimeClass when the gVisor node pool is ready.

First create the cluster and obtain credentials by following the [zonal cluster runbook](zonal-cluster-runbook.md). Confirm `kubectl config current-context` matches Terraform's `kube_context` output, then install and check the platform:

```sh
kubectl kustomize k8s/platform | kubectl apply -f -
kubectl kustomize k8s/distributed/overlays/dev-platform | kubectl apply -f -   # dev namespaces and the sandbox-management Role
kubectl -n agent-sandbox-system rollout status deployment/agent-sandbox-controller --timeout=300s
kubectl get crd sandboxes.agents.x-k8s.io
kubectl get runtimeclass gvisor
```

The runbook documents the remaining steps: Workload Identity grants, image push, Bot deployment, `gke-session-sandbox preflight`, Sandbox lifecycle acceptance, and safe destroy planning. Shared-code fixtures and the old validation worker are not part of this platform bundle.
