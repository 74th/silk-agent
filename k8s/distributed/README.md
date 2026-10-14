# Distributed Sandbox Service smoke deployment

This overlay deploys only the authenticated internal Sandbox Service. It does not
switch Slack traffic or start Gateway, Worker, or Control roles.

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
