from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest

from gke_session_sandbox import CoreSandboxBackend, ResourceOwnershipError, SandboxReadyTimeout


class FakeCustomApi:
    def __init__(self) -> None:
        self.sandbox: dict | None = None
        self.last_patch: dict | None = None

    async def create_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, body: dict) -> dict:
        self.sandbox = copy.deepcopy(body)
        self.sandbox["metadata"].update({"namespace": namespace, "uid": "sandbox-uid", "generation": 1})
        self.sandbox["status"] = {
            "selector": "sandbox-name=test-sandbox",
            "serviceFQDN": "test-sandbox.session-sandboxes.svc.cluster.local",
            "conditions": [{"type": "Ready", "status": "True", "observedGeneration": 1}],
        }
        return copy.deepcopy(self.sandbox)

    async def get_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, name: str) -> dict:
        return copy.deepcopy(self.sandbox)

    async def patch_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, name: str, body: dict) -> dict:
        self.last_patch = copy.deepcopy(body)
        if body[0]["value"] != self.sandbox["metadata"]["uid"]:
            raise RuntimeError("JSON Patch UID test failed")
        for operation in body[1:]:
            parts = [part.replace("~1", "/").replace("~0", "~")
                     for part in operation["path"].strip("/").split("/")]
            parent = self.sandbox
            for part in parts[:-1]:
                parent = parent.setdefault(part, {})
            path = parts[-1]
            if operation["op"] == "remove":
                parent.pop(path, None)
            else:
                parent[path] = operation["value"]
        return copy.deepcopy(self.sandbox)

    async def delete_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, name: str, body: dict | None = None) -> None:
        self.last_patch = copy.deepcopy(body)


class FakeCoreApi:
    async def list_namespaced_pod(self, namespace: str, label_selector: str | None = None) -> dict:
        return {"items": [{"metadata": {"uid": "pod-uid"}, "status": {
            "podIP": "192.0.2.20",
            "conditions": [{"type": "Ready", "status": "True"}],
        }}]}


class FakeNetworkingApi:
    def __init__(self) -> None:
        self.policies: list[dict] = []

    async def create_namespaced_network_policy(self, namespace: str, body: dict) -> dict:
        item = copy.deepcopy(body)
        item["metadata"].update({"namespace": namespace, "uid": f"policy-{len(self.policies)}"})
        self.policies.append(item)
        return copy.deepcopy(item)

    async def delete_namespaced_network_policy(self, name: str, namespace: str, body: dict | None = None) -> None:
        return None


def manifest() -> dict:
    return {
        "apiVersion": "agents.x-k8s.io/v1beta1",
        "kind": "Sandbox",
        "metadata": {"labels": {"app": "test"}},
        "spec": {"service": True, "podTemplate": {"spec": {"containers": [{"name": "sandboxd", "image": "runtime:v1"}]}}},
    }


@pytest.mark.asyncio
async def test_backend_creates_suspended_before_policy_and_running_transition() -> None:
    custom, core, network = FakeCustomApi(), FakeCoreApi(), FakeNetworkingApi()
    backend = CoreSandboxBackend(custom, core, network)
    ref = await backend.create_suspended(manifest(), namespace="session-sandboxes", name="test-sandbox")
    assert ref.uid == "sandbox-uid"
    assert custom.sandbox["spec"]["operatingMode"] == "Suspended"

    policy_refs = await backend.create_policies(
        ref.namespace,
        [{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "sandbox-test"}, "spec": {}}],
    )
    await backend.start(ref)
    ready = await backend.wait_ready(ref, timeout_seconds=0.1, poll_interval_seconds=0, health_check=lambda service, pod: _healthy(service, pod))

    assert len(policy_refs) == 1
    assert custom.last_patch[1]["value"] == "Running"
    assert ready["metadata"]["uid"] == ref.uid
    assert ready["status"]["podUID"] == "pod-uid"


@pytest.mark.asyncio
async def test_create_with_policies_starts_only_after_policy_creation() -> None:
    custom, core, network = FakeCustomApi(), FakeCoreApi(), FakeNetworkingApi()
    backend = CoreSandboxBackend(custom, core, network)
    ref, policies = await backend.create_with_policies(
        manifest(),
        [{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "sandbox-test"}, "spec": {}}],
        namespace="session-sandboxes",
        name="test-sandbox",
    )

    assert ref.uid == "sandbox-uid"
    assert len(policies) == 1
    assert custom.last_patch[1]["value"] == "Running"
    assert network.policies[0]["metadata"]["namespace"] == "session-sandboxes"


async def _healthy(service: str | None, pod: str | None) -> bool:
    return service == "test-sandbox.session-sandboxes.svc.cluster.local" and pod == "192.0.2.20"


@pytest.mark.asyncio
async def test_backend_refuses_replaced_uid() -> None:
    custom = FakeCustomApi()
    backend = CoreSandboxBackend(custom, FakeCoreApi(), FakeNetworkingApi())
    ref = await backend.create_suspended(manifest(), namespace="session-sandboxes", name="test-sandbox")
    custom.sandbox["metadata"]["uid"] = "replacement-uid"
    with pytest.raises(ResourceOwnershipError, match="UID changed"):
        await backend.start(ref)


@pytest.mark.asyncio
async def test_ready_timeout_reports_last_controller_condition() -> None:
    custom, core = FakeCustomApi(), FakeCoreApi()
    backend = CoreSandboxBackend(custom, core, FakeNetworkingApi())
    ref = await backend.create_suspended(manifest(), namespace="session-sandboxes", name="test-sandbox")
    custom.sandbox["status"]["conditions"] = [{
        "type": "Ready", "status": "False", "reason": "PodNotReady", "observedGeneration": 1,
    }]
    with pytest.raises(SandboxReadyTimeout, match="PodNotReady"):
        await backend.wait_ready(ref, timeout_seconds=0.001, poll_interval_seconds=0)


@pytest.mark.asyncio
async def test_expiration_fallback_and_resume_deadline_clear_are_uid_guarded() -> None:
    custom = FakeCustomApi()
    backend = CoreSandboxBackend(custom, FakeCoreApi(), FakeNetworkingApi())
    ref = await backend.create_suspended(manifest(), namespace="session-sandboxes", name="test-sandbox")
    expiry = datetime(2030, 1, 1, tzinfo=timezone.utc)

    await backend.set_expiration(ref, expiry)
    assert custom.sandbox["spec"]["shutdownTime"] == "2030-01-01T00:00:00Z"
    assert custom.sandbox["spec"]["shutdownPolicy"] == "Delete"
    await backend.resume(ref)

    assert "shutdownTime" not in custom.sandbox["spec"]
    assert custom.sandbox["spec"]["shutdownPolicy"] == "Retain"
    assert custom.sandbox["spec"]["operatingMode"] == "Running"
    assert [op["op"] for op in custom.last_patch] == ["test", "remove", "add", "replace"]


@pytest.mark.asyncio
async def test_annotation_patch_preserves_other_owners_and_checks_sandbox_uid() -> None:
    custom = FakeCustomApi()
    backend = CoreSandboxBackend(custom, FakeCoreApi(), FakeNetworkingApi())
    ref = await backend.create_suspended(manifest(), namespace="session-sandboxes", name="test-sandbox")
    custom.sandbox["metadata"]["annotations"] = {"other.example/owner": "value"}

    await backend.patch_annotations(ref, {"session-sandbox.example.com/state": "ready"})

    assert custom.sandbox["metadata"]["annotations"] == {
        "other.example/owner": "value",
        "session-sandbox.example.com/state": "ready",
    }
    assert custom.last_patch[0] == {"op": "test", "path": "/metadata/uid", "value": ref.uid}
