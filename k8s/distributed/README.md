# Distributed deployment (Sandbox Service and role Deployments)

```
base/        Gateway, Worker, Control, Sandbox Service. No namespace, image, project, Firestore
             prefix or bucket. Every role reads its namespace as POD_NAMESPACE (Downward API).
platform/    Component: the two Namespaces, the Sandbox-namespace Role / RoleBindings, and the
             replacements that put a use case's namespaces into base.
overlays/    dev (this repository's environment), dev-bootstrap (the same with zero replicas),
             dev-namespaces (the namespaces of dev), dev-platform (only the dev namespaces and Role).
```

A use case or environment adds a ConfigMap generator `silk-agent-namespaces` (`system`, `sandbox`;
local-only, never rendered), the platform component, its configuration (`silk-agent-distributed.yaml`,
the Sandbox Service config, `mcp.json`) and its image. See `usecases/template/k8s/distributed/` and
[the use case guide](../../docs/usecase-guide.md).

`base/sandbox-service.yaml` runs the authenticated internal Sandbox Service. `base/roles.yaml` defines
the Gateway, Worker and Control Deployments with their ServiceAccounts and NetworkPolicies.
**`kubectl apply -k k8s/distributed/overlays/dev` is the steady state and starts the roles** (Gateway 1,
Worker 2, Control 1): Gateway connects to Slack and Workers start as soon as it is applied. For the
first build of an environment, apply `k8s/distributed/overlays/dev-bootstrap` instead: the same
resources with the three role Deployments at zero replicas, so nothing connects to Slack or starts an
SDK. Start the roles in the order of [the operations runbook](../../docs/distributed-operations-runbook.md)
(§2) with `kubectl scale`, and apply `overlays/dev` only after the cut-over is done.
Re-applying either one resets the replica counts it declares.

Before starting a role, rebuild the image (`k8s/build-image.sh`) and put its digest in the
overlay's `kustomization.yaml`: an image that predates `silk-agent worker run` / `control` cannot start
them. The Worker mounts no Secret, Control receives only the Slack bot token, and no role
mounts a volume with SQLite state.

The `dev` config and image references are development-environment-specific and digest-pinned.

```sh
kubectl config current-context
kubectl kustomize k8s/distributed/overlays/dev
kubectl apply --dry-run=server -k k8s/distributed/overlays/dev
kubectl apply -k k8s/distributed/overlays/dev
kubectl -n session-sandbox-system rollout status deployment/silk-agent-sandbox-service
```

The Service is ClusterIP-only. Its NetworkPolicy allows Worker Pod ingress, DNS,
HTTPS, and the GKE Dataplane V2 metadata-server path needed for Workload Identity.
The ServiceAccount can submit Kubernetes TokenReview requests and manage Sandbox
resources in the Sandbox namespace; Firestore access is granted to the matching GKE
Workload Identity principal in `terraform/main.tf`.

Each role refuses to start when `POD_NAMESPACE` differs from the namespaces in its config
(`distributed.namespace_check`), so a role cannot be pointed at another use case.
