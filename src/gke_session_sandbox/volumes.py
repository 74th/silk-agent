"""PVC ownership cleanup and safe shared read-only workspace mounting."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, Protocol

from .errors import ResourceOwnershipError
from .types import ResourceRef


class PersistentVolumeClaimApi(Protocol):
    async def read_namespaced_persistent_volume_claim(self, name: str, namespace: str) -> Any: ...
    async def list_namespaced_persistent_volume_claim(self, namespace: str) -> Any: ...
    async def delete_namespaced_persistent_volume_claim(self, name: str, namespace: str, **kwargs: Any) -> Any: ...


def attach_shared_code_volume(
    manifest: Mapping[str, Any],
    *,
    container_name: str,
    claim_name: str,
    mount_path: str = "/workspace/shared-code",
    volume_name: str = "shared-code",
) -> dict[str, Any]:
    """Attach an existing same-namespace claim read-only at both volume layers."""
    if not claim_name.strip() or not container_name.strip() or not mount_path.startswith("/"):
        raise ValueError("claim_name, container_name, and absolute mount_path are required")
    result = copy.deepcopy(dict(manifest))
    try:
        pod_spec = result["spec"]["podTemplate"]["spec"]
        containers = pod_spec["containers"]
    except (KeyError, TypeError) as exc:
        raise ResourceOwnershipError("Sandbox manifest has no podTemplate container list") from exc
    targets = [container for container in containers if container.get("name") == container_name]
    if len(targets) != 1:
        raise ResourceOwnershipError(f"expected exactly one runtime container named {container_name!r}")
    volumes = pod_spec.setdefault("volumes", [])
    if any(volume.get("name") == volume_name for volume in volumes):
        raise ResourceOwnershipError(f"volume name {volume_name!r} is already defined")
    mounts = targets[0].setdefault("volumeMounts", [])
    if any(mount.get("mountPath") == mount_path or mount.get("name") == volume_name for mount in mounts):
        raise ResourceOwnershipError("shared code mount conflicts with an existing container mount")
    volumes.append({
        "name": volume_name,
        "persistentVolumeClaim": {"claimName": claim_name, "readOnly": True},
    })
    mounts.append({"name": volume_name, "mountPath": mount_path, "readOnly": True})
    return result


class OwnedWorkspacePVCManager:
    """Discover and delete only PVCs whose controller owner UID is this Sandbox UID."""

    def __init__(self, core_api: PersistentVolumeClaimApi) -> None:
        self._core = core_api

    async def list_owned(self, sandbox_ref: ResourceRef) -> tuple[ResourceRef, ...]:
        _validate_sandbox_ref(sandbox_ref)
        response = await self._core.list_namespaced_persistent_volume_claim(sandbox_ref.namespace)
        claims = _field(response, "items", []) or []
        refs: list[ResourceRef] = []
        for claim in claims:
            metadata = _field(claim, "metadata", {})
            if _owned_by(metadata, sandbox_ref.uid):
                refs.append(ResourceRef(
                    api_version="v1",
                    kind="PersistentVolumeClaim",
                    namespace=sandbox_ref.namespace,
                    name=_field(metadata, "name"),
                    uid=_field(metadata, "uid"),
                ))
        return tuple(refs)

    async def delete_owned(self, pvc_ref: ResourceRef, *, sandbox_uid: str) -> None:
        if pvc_ref.api_version != "v1" or pvc_ref.kind != "PersistentVolumeClaim" or not pvc_ref.namespace:
            raise ResourceOwnershipError("resource reference is not a namespaced PVC")
        try:
            claim = await self._core.read_namespaced_persistent_volume_claim(pvc_ref.name, pvc_ref.namespace)
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return
            raise
        metadata = _field(claim, "metadata", {})
        if _field(metadata, "uid") != pvc_ref.uid:
            raise ResourceOwnershipError("PVC UID changed; refusing to delete its replacement")
        if not _owned_by(metadata, sandbox_uid):
            raise ResourceOwnershipError("PVC is not owned by the expected Sandbox UID")
        await self._core.delete_namespaced_persistent_volume_claim(
            pvc_ref.name,
            pvc_ref.namespace,
            body={"preconditions": {"uid": pvc_ref.uid}},
        )

    async def verify_owned(self, pvc_ref: ResourceRef, *, sandbox_uid: str) -> None:
        """Confirm that a persisted PVC reference still names the same owned claim."""
        if pvc_ref.api_version != "v1" or pvc_ref.kind != "PersistentVolumeClaim" or not pvc_ref.namespace:
            raise ResourceOwnershipError("resource reference is not a namespaced PVC")
        try:
            claim = await self._core.read_namespaced_persistent_volume_claim(pvc_ref.name, pvc_ref.namespace)
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                raise ResourceOwnershipError("owned PVC no longer exists") from exc
            raise
        metadata = _field(claim, "metadata", {})
        if _field(metadata, "uid") != pvc_ref.uid:
            raise ResourceOwnershipError("PVC UID changed; refusing to adopt its replacement")
        if not _owned_by(metadata, sandbox_uid):
            raise ResourceOwnershipError("PVC is not owned by the expected Sandbox UID")


def _validate_sandbox_ref(ref: ResourceRef) -> None:
    if ref.api_version != "agents.x-k8s.io/v1beta1" or ref.kind != "Sandbox" or not ref.namespace:
        raise ResourceOwnershipError("resource reference is not a namespaced core Sandbox")


def _owned_by(metadata: Any, sandbox_uid: str) -> bool:
    owners = _field(metadata, "ownerReferences", []) or []
    return any(
        _field(owner, "uid") == sandbox_uid
        and _field(owner, "kind") == "Sandbox"
        and _field(owner, "controller", False) is True
        for owner in owners
    )


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    snake = "".join(("_" + char.lower()) if char.isupper() else char for char in name).lstrip("_")
    return getattr(value, snake, default)
