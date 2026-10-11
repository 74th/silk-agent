# Agent Sandbox controller bootstrap

The repository vendors the upstream core install manifest for Agent Sandbox v1.0.6 at `k8s/vendor/agent-sandbox-v1.0.6.yaml`. The bootstrap script verifies its SHA-256, checks that the active `kubectl` context matches all explicitly provided cluster identifiers, applies the same manifest idempotently, and waits for the controller and core CRD. The pinned manifest is the upstream [v1.0.6 core release](https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.6/sandbox.yaml).

```sh
gcloud container clusters get-credentials research-gke-agent-sandbox-with-sandboxd \
  --region us-central1 --project nnyn-dev
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
  ./scripts/bootstrap-agent-sandbox.sh
```

The release manifest installs the core `agents.x-k8s.io/v1beta1` API and controller only. Optional extensions are not installed. Re-run the script to reconcile the pinned resources after an interrupted bootstrap.

## Platform and storage

Install the manager and runtime namespaces, namespace-scoped worker RBAC, workspace / shared-code StorageClasses, and verify the GKE-managed gVisor RuntimeClass:

```sh
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
  ./scripts/bootstrap-platform.sh
```

Prepare the shared code fixture and check dedicated workspace persistence, gVisor scheduling, read-only access, and simultaneous mounts on two nodes:

```sh
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
  ./scripts/validate-storage.sh
```

The fixture seed is written to a zonal PD, then its disk is retained and rebound as a static `ReadOnlyMany` volume. The script keeps the shared disk and fixture claim for reuse. To remove test consumers only, delete `shared-code-reader-a`, `shared-code-reader-b`, and the `workspace-validation` PVC from `session-sandbox-fixtures`; preserve `runtime-code-rom-pv` and its disk while the shared fixture is in use. GKE supports attaching Compute Engine disks in `ReadOnlyMany` mode to multiple Pods on different nodes ([GKE multiple-reader instructions](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/readonlymany-disks)).
