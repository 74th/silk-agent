# Controller and storage bootstrap record

Checked against the GKE cluster on 2026-10-10.

- Agent Sandbox controller image: `registry.k8s.io/agent-sandbox/agent-sandbox-controller:v1.0.6`; deployment ready replicas `1/1`.
- Core CRD: `sandboxes.agents.x-k8s.io`, `v1beta1` served and stored.
- Re-running `scripts/bootstrap-agent-sandbox.sh` returned `unchanged` for every pinned resource and the controller rollout stayed ready.
- GKE-provided RuntimeClass `gvisor` selects nodes labeled `sandbox.gke.io/runtime=gvisor` and tolerates the GKE-managed runtime taint.
- `runtime-code-rom` bound as `ReadOnlyMany` to a static PD CSI volume with `Retain`; `workspace-validation` bound as `ReadWriteOnce` to `session-workspace-rwo`.
- Reader A ran as non-root under `runtimeClassName: gvisor`, read the shared fixture, wrote to its dedicated workspace, and could not write to the shared mount.
- Reader B on a regular system-pool node read the same shared fixture concurrently and could not see Reader A's workspace file or write to the shared mount.
- Both Pods completed successfully on different nodes in the fixture disk's zone. The failed initial snapshot attempt created a transient Cloud snapshot; its Kubernetes objects and the Cloud snapshot were removed before the successful direct-disk ROM test.

To repeat the check, use `scripts/bootstrap-platform.sh` followed by `scripts/validate-storage.sh` as documented in `docs/agent-sandbox-controller.md`. The fixture uses a zonal disk, so the two readers are deliberately scheduled to separate node pools in the same zone.
