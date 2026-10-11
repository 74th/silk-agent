from __future__ import annotations

from copy import deepcopy

import pytest

from gke_session_sandbox import (
    OwnedWorkspacePVCManager,
    ResourceOwnershipError,
    ResourceRef,
    attach_shared_code_volume,
)


def sandbox_ref() -> ResourceRef:
    return ResourceRef(api_version="agents.x-k8s.io/v1beta1", kind="Sandbox", namespace="sandboxes", name="sb-1", uid="sandbox-uid")


def pvc_ref(name: str, uid: str) -> ResourceRef:
    return ResourceRef(api_version="v1", kind="PersistentVolumeClaim", namespace="sandboxes", name=name, uid=uid)


def test_shared_claim_is_read_only_in_volume_and_mount_and_input_is_unchanged() -> None:
    original = {
        "spec": {"podTemplate": {"spec": {"containers": [{"name": "runtime", "image": "runtime:v1"}]}}}
    }

    result = attach_shared_code_volume(
        original,
        container_name="runtime",
        claim_name="runtime-code-rom",
    )

    pod_spec = result["spec"]["podTemplate"]["spec"]
    assert pod_spec["volumes"] == [{
        "name": "shared-code",
        "persistentVolumeClaim": {"claimName": "runtime-code-rom", "readOnly": True},
    }]
    assert pod_spec["containers"][0]["volumeMounts"] == [
        {"name": "shared-code", "mountPath": "/workspace/shared-code", "readOnly": True}
    ]
    assert "volumes" not in original["spec"]["podTemplate"]["spec"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"container_name": "missing"},
        {"volume_name": "workspace"},
        {"mount_path": "/workspace"},
    ],
)
def test_shared_mount_conflicts_are_rejected(overrides: dict[str, str]) -> None:
    manifest = {"spec": {"podTemplate": {"spec": {
        "volumes": [{"name": "workspace", "emptyDir": {}}],
        "containers": [{"name": "runtime", "image": "runtime:v1", "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}]}],
    }}}}
    with pytest.raises(ResourceOwnershipError):
        args = {"container_name": "runtime", "claim_name": "shared", **overrides}
        attach_shared_code_volume(manifest, **args)


class FakePVCAPI:
    def __init__(self, claims: list[dict]) -> None:
        self.claims = {claim["metadata"]["name"]: deepcopy(claim) for claim in claims}
        self.deleted: list[tuple[str, dict]] = []

    async def list_namespaced_persistent_volume_claim(self, namespace: str) -> dict:
        return {"items": [deepcopy(claim) for claim in self.claims.values()]}

    async def read_namespaced_persistent_volume_claim(self, name: str, namespace: str) -> dict:
        return deepcopy(self.claims[name])

    async def delete_namespaced_persistent_volume_claim(self, name: str, namespace: str, **kwargs: dict) -> None:
        self.deleted.append((name, kwargs["body"]))


@pytest.mark.asyncio
async def test_only_pvc_with_matching_sandbox_controller_uid_is_discovered_or_deleted() -> None:
    api = FakePVCAPI([
        {"metadata": {
            "name": "workspace-sb-1", "namespace": "sandboxes", "uid": "pvc-uid",
            "ownerReferences": [{"kind": "Sandbox", "uid": "sandbox-uid", "controller": True}],
        }},
        {"metadata": {"name": "runtime-code-rom", "namespace": "sandboxes", "uid": "shared-uid"}},
    ])
    manager = OwnedWorkspacePVCManager(api)

    owned = await manager.list_owned(sandbox_ref())
    assert owned == (pvc_ref("workspace-sb-1", "pvc-uid"),)
    await manager.delete_owned(owned[0], sandbox_uid="sandbox-uid")
    assert api.deleted == [("workspace-sb-1", {"preconditions": {"uid": "pvc-uid"}})]

    shared = pvc_ref("runtime-code-rom", "shared-uid")
    with pytest.raises(ResourceOwnershipError, match="not owned"):
        await manager.delete_owned(shared, sandbox_uid="sandbox-uid")
    assert len(api.deleted) == 1


@pytest.mark.asyncio
async def test_uid_replacement_and_other_owner_are_refused_for_pvc_cleanup() -> None:
    api = FakePVCAPI([{"metadata": {
        "name": "workspace-sb-1", "namespace": "sandboxes", "uid": "replacement-uid",
        "ownerReferences": [{"kind": "Sandbox", "uid": "sandbox-uid", "controller": True}],
    }}])
    manager = OwnedWorkspacePVCManager(api)

    with pytest.raises(ResourceOwnershipError, match="UID changed"):
        await manager.delete_owned(pvc_ref("workspace-sb-1", "old-uid"), sandbox_uid="sandbox-uid")
    api.claims["workspace-sb-1"]["metadata"]["uid"] = "pvc-uid"
    api.claims["workspace-sb-1"]["metadata"]["ownerReferences"][0]["uid"] = "another-sandbox"
    with pytest.raises(ResourceOwnershipError, match="not owned"):
        await manager.delete_owned(pvc_ref("workspace-sb-1", "pvc-uid"), sandbox_uid="sandbox-uid")
    assert api.deleted == []
