"""Checks for Kubernetes capabilities and permissions required by a manager."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .config import ManagerConfig
from .errors import ClusterCapabilityError


_WORKER_PERMISSIONS = (
    ("", "configmaps", ("create", "get", "list", "patch", "delete"), "control"),
    ("coordination.k8s.io", "leases", ("create", "get", "list", "patch", "update", "delete"), "control"),
    ("agents.x-k8s.io", "sandboxes", ("create", "get", "list", "watch", "patch", "delete"), "sandbox"),
    ("", "persistentvolumeclaims", ("create", "get", "list", "delete"), "sandbox"),
    ("", "pods", ("get", "list", "watch"), "sandbox"),
    ("", "services", ("get", "list"), "sandbox"),
    ("networking.k8s.io", "networkpolicies", ("create", "get", "list", "delete"), "sandbox"),
)


async def validate_cluster_capabilities(
    config: ManagerConfig,
    *,
    core_api: Any,
    node_api: Any,
    storage_api: Any,
    custom_api: Any,
    authorization_api: Any,
) -> tuple[str, ...]:
    """Return discovered capabilities, or raise with all missing prerequisites."""
    issues: list[str] = []
    found: list[str] = []

    async def check(label: str, operation: Any) -> Any | None:
        try:
            result = await operation
        except Exception as exc:
            status = getattr(exc, "status", None)
            detail = "not found" if status == 404 else f"API error {status or type(exc).__name__}: {getattr(exc, 'reason', str(exc))}"
            issues.append(f"{label}: {detail}")
            return None
        found.append(label)
        return result

    await check(f"namespace/{config.namespace}", core_api.read_namespace(config.namespace))
    await check(f"namespace/{config.control_namespace}", core_api.read_namespace(config.control_namespace))
    runtime_class = await check("RuntimeClass/gvisor", node_api.read_runtime_class("gvisor"))
    if runtime_class is not None:
        handler = _field(runtime_class, "handler")
        if not handler:
            issues.append("RuntimeClass/gvisor: handler is empty")

    storage_class = await check(
        f"StorageClass/{config.workspace_storage_class}",
        storage_api.read_storage_class(config.workspace_storage_class),
    )
    if storage_class is not None and _field(storage_class, "provisioner") != "pd.csi.storage.gke.io":
        issues.append(
            f"StorageClass/{config.workspace_storage_class}: expected provisioner pd.csi.storage.gke.io, "
            f"found {_field(storage_class, 'provisioner')!r}"
        )

    sandbox_crd = await check(
        "CustomResourceDefinition/sandboxes.agents.x-k8s.io",
        custom_api.get_cluster_custom_object(
            "apiextensions.k8s.io", "v1", "customresourcedefinitions", "sandboxes.agents.x-k8s.io"
        ),
    )
    if sandbox_crd is not None and not _served_version(sandbox_crd, "v1beta1"):
        issues.append("CustomResourceDefinition/sandboxes.agents.x-k8s.io: version v1beta1 is not served")

    if config.network.allowed_fqdns:
        fqdn_crd = await check(
            "CustomResourceDefinition/fqdnnetworkpolicies.networking.gke.io",
            custom_api.get_cluster_custom_object(
                "apiextensions.k8s.io", "v1", "customresourcedefinitions", "fqdnnetworkpolicies.networking.gke.io"
            ),
        )
        if fqdn_crd is not None and not _served_version(fqdn_crd, "v1alpha1"):
            issues.append("CustomResourceDefinition/fqdnnetworkpolicies.networking.gke.io: version v1alpha1 is not served")

    required_permissions = list(_WORKER_PERMISSIONS)
    if config.network.allowed_fqdns:
        required_permissions.append(("networking.gke.io", "fqdnnetworkpolicies", ("create", "get", "list", "delete"), "sandbox"))
    for group, resource, verbs, namespace_kind in required_permissions:
        namespace = config.control_namespace if namespace_kind == "control" else config.namespace
        for verb in verbs:
            body = {
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {"resourceAttributes": {
                    "namespace": namespace,
                    "verb": verb,
                    "group": group,
                    "resource": resource,
                }},
            }
            review = await check(
                f"RBAC {verb} {group + '/' if group else ''}{resource} in {namespace}",
                authorization_api.create_self_subject_access_review(body),
            )
            if review is not None:
                status = _field(review, "status")
                if not _field(status, "allowed", False):
                    reason = _field(status, "reason") or _field(status, "evaluationError") or "access denied"
                    issues.append(f"RBAC {verb} {group + '/' if group else ''}{resource} in {namespace}: {reason}")

    if issues:
        raise ClusterCapabilityError("cluster preflight failed:\n- " + "\n- ".join(issues))
    return tuple(found)


def _served_version(crd: Mapping[str, Any], version: str) -> bool:
    spec = _field(crd, "spec", {}) or {}
    versions = _field(spec, "versions", ()) or ()
    return any(_field(item, "name") == version and _field(item, "served", False) for item in versions)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)
