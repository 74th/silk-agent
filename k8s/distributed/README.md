# Distributed deployment (Sandbox Service and role Deployments)

`sandbox-service.yaml` runs the authenticated internal Sandbox Service. `roles.yaml` defines
the Gateway, Worker and Control Deployments with their ServiceAccounts and NetworkPolicies.
**`kubectl apply -k k8s/distributed` is the steady state and starts the roles** (Gateway 1,
Worker 2, Control 1): Gateway connects to Slack and Workers start as soon as it is applied. For the
first build of an environment, apply `k8s/distributed-bootstrap` instead: the same resources with
the three role Deployments at zero replicas, so nothing connects to Slack or starts an SDK. Start
the roles in the order of [the operations runbook](../../docs/distributed-operations-runbook.md)
(§2) with `kubectl scale`, and apply `k8s/distributed` only after the cut-over is done.
Re-applying either one resets the replica counts it declares.

Before starting a role, rebuild the image (`k8s/build-image.sh`) and put its digest in
`kustomization.yaml`: an image that predates `silk-agent worker run` / `control` cannot start
them. The Worker mounts no Secret, Control receives only the Slack bot token, and no role
mounts a volume with SQLite state.

The config and both image references are development-environment-specific and
digest-pinned. Review them before using another project or environment.

```sh
kubectl config current-context
kubectl kustomize k8s/distributed
kubectl apply --dry-run=server -k k8s/distributed
kubectl apply -k k8s/distributed
kubectl -n session-sandbox-system rollout status deployment/silk-agent-sandbox-service
```

The Service is ClusterIP-only. Its NetworkPolicy allows Worker Pod ingress, DNS,
HTTPS, and the GKE Dataplane V2 metadata-server path needed for Workload Identity.
The ServiceAccount can submit Kubernetes TokenReview requests and manage Sandbox
resources in `session-sandboxes`; Firestore access is granted to the matching GKE
Workload Identity principal in `terraform/main.tf`.
