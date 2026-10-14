# Distributed deployment (Sandbox Service and role Deployments)

`sandbox-service.yaml` runs the authenticated internal Sandbox Service. `roles.yaml` defines
the Gateway, Worker and Control Deployments with their ServiceAccounts and NetworkPolicies.
**The three role Deployments are committed with `replicas: 0`**, so applying this directory
does not connect to Slack, start an SDK, or switch traffic. Enable them one at a time as
described in [the operations runbook](../../docs/distributed-operations-runbook.md).

Before enabling a role, rebuild the image (`k8s/build-image.sh`) and put its digest in
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
