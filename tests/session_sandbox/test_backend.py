from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from gke_session_sandbox import CoreSandboxBackend, ResourceOwnershipError, SandboxReadyTimeout


class FakeCustomApi:
    def __init__(self) -> None:
        self.sandbox: dict | None = None
        self.last_patch: dict | None = None

    async def create_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, body: dict
    ) -> dict:
        self.sandbox = copy.deepcopy(body)
        self.sandbox["metadata"].update(
            {"namespace": namespace, "uid": "sandbox-uid", "generation": 1}
        )
        self.sandbox["status"] = {
            "selector": "sandbox-name=test-sandbox",
            "serviceFQDN": "test-sandbox.session-sandboxes.svc.cluster.local",
            "conditions": [{"type": "Ready", "status": "True", "observedGeneration": 1}],
        }
        return copy.deepcopy(self.sandbox)

    async def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict:
        return copy.deepcopy(self.sandbox)

    async def patch_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, body: dict
    ) -> dict:
        self.last_patch = copy.deepcopy(body)
        if body[0]["value"] != self.sandbox["metadata"]["uid"]:
            raise RuntimeError("JSON Patch UID test failed")
        for operation in body[1:]:
            parts = [
                part.replace("~1", "/").replace("~0", "~")
                for part in operation["path"].strip("/").split("/")
            ]
            parent = self.sandbox
            for part in parts[:-1]:
                parent = parent.setdefault(part, {})
            path = parts[-1]
            if operation["op"] == "remove":
                parent.pop(path, None)
            else:
                parent[path] = operation["value"]
        return copy.deepcopy(self.sandbox)

    async def delete_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: dict | None = None,
    ) -> None:
        self.last_patch = copy.deepcopy(body)


class FakeCoreApi:
    async def list_namespaced_pod(self, namespace: str, label_selector: str | None = None) -> dict:
        return {
            "items": [
                {
                    "metadata": {"uid": "pod-uid", "name": "test-sandbox-pod"},
                    "status": {
                        "podIP": "192.0.2.20",
                        "conditions": [{"type": "Ready", "status": "True"}],
                    },
                }
            ]
        }


class FakeNetworkingApi:
    def __init__(self) -> None:
        self.policies: list[dict] = []

    async def create_namespaced_network_policy(self, namespace: str, body: dict) -> dict:
        item = copy.deepcopy(body)
        item["metadata"].update({"namespace": namespace, "uid": f"policy-{len(self.policies)}"})
        self.policies.append(item)
        return copy.deepcopy(item)

    async def delete_namespaced_network_policy(
        self, name: str, namespace: str, body: dict | None = None
    ) -> None:
        return None


def manifest() -> dict:
    return {
        "apiVersion": "agents.x-k8s.io/v1beta1",
        "kind": "Sandbox",
        "metadata": {"labels": {"app": "test"}},
        "spec": {
            "service": True,
            "podTemplate": {"spec": {"containers": [{"name": "sandboxd", "image": "runtime:v1"}]}},
        },
    }


@pytest.mark.asyncio
async def test_backend_creates_suspended_before_policy_and_running_transition() -> None:
    custom, core, network = FakeCustomApi(), FakeCoreApi(), FakeNetworkingApi()
    backend = CoreSandboxBackend(custom, core, network)
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )
    assert ref.uid == "sandbox-uid"
    assert custom.sandbox["spec"]["operatingMode"] == "Suspended"

    policy_refs = await backend.create_policies(
        ref.namespace,
        [
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": "sandbox-test"},
                "spec": {},
            }
        ],
    )
    await backend.start(ref)
    ready = await backend.wait_ready(
        ref,
        timeout_seconds=0.1,
        poll_interval_seconds=0,
        health_check=lambda service, pod: _healthy(service, pod),
    )

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
        [
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": "sandbox-test"},
                "spec": {},
            }
        ],
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
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )
    custom.sandbox["metadata"]["uid"] = "replacement-uid"
    with pytest.raises(ResourceOwnershipError, match="UID changed"):
        await backend.start(ref)


@pytest.mark.asyncio
async def test_ready_timeout_reports_last_controller_condition() -> None:
    custom, core = FakeCustomApi(), FakeCoreApi()
    backend = CoreSandboxBackend(custom, core, FakeNetworkingApi())
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )
    custom.sandbox["status"]["conditions"] = [
        {
            "type": "Ready",
            "status": "False",
            "reason": "PodNotReady",
            "observedGeneration": 1,
        }
    ]
    with pytest.raises(SandboxReadyTimeout, match="PodNotReady"):
        await backend.wait_ready(ref, timeout_seconds=0.001, poll_interval_seconds=0)


@pytest.mark.asyncio
async def test_expiration_fallback_and_resume_deadline_clear_are_uid_guarded() -> None:
    custom = FakeCustomApi()
    backend = CoreSandboxBackend(custom, FakeCoreApi(), FakeNetworkingApi())
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )
    expiry = datetime(2030, 1, 1, tzinfo=UTC)

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
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )
    custom.sandbox["metadata"]["annotations"] = {"other.example/owner": "value"}

    await backend.patch_annotations(ref, {"session-sandbox.example.com/state": "ready"})

    assert custom.sandbox["metadata"]["annotations"] == {
        "other.example/owner": "value",
        "session-sandbox.example.com/state": "ready",
    }
    assert custom.last_patch[0] == {"op": "test", "path": "/metadata/uid", "value": ref.uid}


def test_wait_ready_defaults_to_180_seconds_and_takes_a_timeout() -> None:
    import inspect

    params = inspect.signature(CoreSandboxBackend.wait_ready).parameters
    assert params["timeout_seconds"].default == 180


class _Conflict(Exception):
    status = 409


class _NotFound(Exception):
    status = 404


class StatefulPolicyApi:
    """In-memory NetworkPolicy and FQDNNetworkPolicy store with 409 / 404 semantics."""

    def __init__(self) -> None:
        self.store: dict[tuple[str, str], dict] = {}
        self.replaced: list[str] = []
        self.deleted: list[str] = []

    def _create(self, kind: str, body: dict) -> dict:
        key = (kind, body["metadata"]["name"])
        if key in self.store:
            raise _Conflict()
        item = copy.deepcopy(body)
        item["metadata"].update({"uid": f"{kind}-uid-{len(self.store)}", "resourceVersion": "1"})
        self.store[key] = item
        return copy.deepcopy(item)

    def _get(self, kind: str, name: str) -> dict:
        try:
            return copy.deepcopy(self.store[(kind, name)])
        except KeyError:
            raise _NotFound() from None

    def _replace(self, kind: str, name: str, body: dict) -> dict:
        current = self.store[(kind, name)]
        assert body["metadata"]["resourceVersion"] == current["metadata"]["resourceVersion"]
        item = copy.deepcopy(body)
        item["metadata"]["uid"] = current["metadata"]["uid"]
        item["metadata"]["resourceVersion"] = "2"
        self.store[(kind, name)] = item
        self.replaced.append(name)
        return copy.deepcopy(item)

    def _delete(self, kind: str, name: str) -> None:
        del self.store[(kind, name)]
        self.deleted.append(name)

    # NetworkingV1Api
    async def create_namespaced_network_policy(self, namespace: str, body: dict) -> dict:
        return self._create("NetworkPolicy", body)

    async def read_namespaced_network_policy(self, name: str, namespace: str) -> dict:
        return self._get("NetworkPolicy", name)

    async def replace_namespaced_network_policy(
        self, name: str, namespace: str, body: dict
    ) -> dict:
        return self._replace("NetworkPolicy", name, body)

    async def delete_namespaced_network_policy(
        self, name: str, namespace: str, body: dict | None = None
    ) -> None:
        self._delete("NetworkPolicy", name)

    # CustomObjectsApi (FQDNNetworkPolicy only)
    async def create_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, body: dict
    ) -> dict:
        return self._create("FQDNNetworkPolicy", body)

    async def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict:
        return self._get("FQDNNetworkPolicy", name)

    async def replace_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, body: dict
    ) -> dict:
        return self._replace("FQDNNetworkPolicy", name, body)

    async def delete_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, body=None
    ) -> None:
        self._delete("FQDNNetworkPolicy", name)


_LABELS = {"session-sandbox.example.com/session-key-hash": "abc"}


def _policy(kind: str, name: str, spec: dict, labels: dict | None = None) -> dict:
    api_version = (
        "networking.gke.io/v1alpha1" if kind == "FQDNNetworkPolicy" else "networking.k8s.io/v1"
    )
    return {
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {"name": name, "labels": dict(labels or _LABELS)},
        "spec": spec,
    }


def _owner(uid: str = "sandbox-uid"):
    from gke_session_sandbox import ResourceRef

    return ResourceRef(
        api_version="agents.x-k8s.io/v1beta1",
        kind="Sandbox",
        namespace="session-sandboxes",
        name="test-sandbox",
        uid=uid,
    )


@pytest.mark.asyncio
async def test_apply_policies_creates_then_replaces_the_spec_of_owned_policies() -> None:
    api = StatefulPolicyApi()
    backend = CoreSandboxBackend(api, FakeCoreApi(), api)
    names = (("NetworkPolicy", "dns"), ("FQDNNetworkPolicy", "fqdn"))

    await backend.apply_policies(
        "session-sandboxes",
        [_policy(kind, name, {"v": 1}) for kind, name in names],
        owner=_owner(),
    )
    refs = await backend.apply_policies(
        "session-sandboxes",
        [_policy(kind, name, {"v": 2}) for kind, name in names],
        owner=_owner(),
    )

    assert api.replaced == ["dns", "fqdn"]
    assert [api.store[key]["spec"] for key in api.store] == [{"v": 2}, {"v": 2}]
    assert [ref.uid for ref in refs] == [api.store[key]["metadata"]["uid"] for key in api.store]
    assert api.store[("NetworkPolicy", "dns")]["metadata"]["ownerReferences"][0]["uid"] == (
        "sandbox-uid"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["NetworkPolicy", "FQDNNetworkPolicy"])
async def test_apply_policies_refuses_policies_owned_by_another_sandbox_or_labels(
    kind: str,
) -> None:
    api = StatefulPolicyApi()
    backend = CoreSandboxBackend(api, FakeCoreApi(), api)
    await backend.apply_policies(
        "session-sandboxes", [_policy(kind, "p", {"v": 1})], owner=_owner("other-uid")
    )

    with pytest.raises(ResourceOwnershipError, match="another owner"):
        await backend.apply_policies(
            "session-sandboxes", [_policy(kind, "p", {"v": 2})], owner=_owner()
        )
    other_labels = {"session-sandbox.example.com/session-key-hash": "other"}
    with pytest.raises(ResourceOwnershipError, match="another owner"):
        await backend.apply_policies(
            "session-sandboxes",
            [_policy(kind, "p", {"v": 2}, other_labels)],
            owner=_owner("other-uid"),
        )

    assert api.store[(kind, "p")]["spec"] == {"v": 1}
    assert api.replaced == []


@pytest.mark.asyncio
async def test_delete_policies_removes_only_owned_policies_and_skips_missing_ones() -> None:
    api = StatefulPolicyApi()
    backend = CoreSandboxBackend(api, FakeCoreApi(), api)
    await backend.apply_policies(
        "session-sandboxes", [_policy("FQDNNetworkPolicy", "fqdn", {})], owner=_owner()
    )
    await backend.apply_policies(
        "session-sandboxes",
        [_policy("NetworkPolicy", "foreign", {})],
        owner=_owner("other-uid"),
    )
    fqdn = ("networking.gke.io/v1alpha1", "FQDNNetworkPolicy", "fqdn")
    missing = ("networking.k8s.io/v1", "NetworkPolicy", "metadata-egress")
    foreign = ("networking.k8s.io/v1", "NetworkPolicy", "foreign")

    deleted = await backend.delete_policies(
        "session-sandboxes", [fqdn, missing], owner=_owner(), labels=_LABELS
    )
    assert deleted == (fqdn,)
    assert api.deleted == ["fqdn"]

    with pytest.raises(ResourceOwnershipError, match="another owner"):
        await backend.delete_policies(
            "session-sandboxes", [foreign], owner=_owner(), labels=_LABELS
        )
    assert ("NetworkPolicy", "foreign") in api.store


class FakeExecApi:
    def __init__(self, output: str) -> None:
        self.output, self.calls = output, []

    async def connect_get_namespaced_pod_exec(self, name: str, namespace: str, **kwargs) -> str:
        self.calls.append((name, namespace, kwargs))
        marker = kwargs["command"][3]
        return self.output.replace("MARKER", marker)


async def _exec_backend(output: str):
    custom, core = FakeCustomApi(), FakeCoreApi()
    exec_api = FakeExecApi(output)
    backend = CoreSandboxBackend(custom, core, FakeNetworkingApi(), exec_api=exec_api)
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )
    return backend, ref, exec_api


@pytest.mark.asyncio
async def test_exec_diagnostic_runs_in_the_ready_pod_and_parses_the_exit_status() -> None:
    backend, ref, exec_api = await _exec_backend("line one\nline two\nMARKER3\n")

    code, output = await backend.exec_diagnostic(ref, ["probe", "--x"], container="sandboxd")

    assert (code, output) == (3, "line one\nline two")
    name, namespace, kwargs = exec_api.calls[0]
    assert namespace == "session-sandboxes" and kwargs["container"] == "sandboxd"
    assert kwargs["stdin"] is False and kwargs["tty"] is False
    assert kwargs["command"][:2] == ["sh", "-c"]
    assert kwargs["command"][2].startswith('"$@" 2>&1; printf')
    assert kwargs["command"][4:] == ["probe", "--x"]


@pytest.mark.asyncio
async def test_exec_diagnostic_rejects_output_without_an_exit_status() -> None:
    backend, ref, _ = await _exec_backend("killed before it reported\n")

    with pytest.raises(ResourceOwnershipError, match="exit status"):
        await backend.exec_diagnostic(ref, ["probe"], container="sandboxd")


@pytest.mark.asyncio
async def test_exec_diagnostic_requires_a_configured_exec_api() -> None:
    backend = CoreSandboxBackend(FakeCustomApi(), FakeCoreApi(), FakeNetworkingApi())
    ref = await backend.create_suspended(
        manifest(), namespace="session-sandboxes", name="test-sandbox"
    )

    with pytest.raises(ResourceOwnershipError, match="not configured"):
        await backend.exec_diagnostic(ref, ["probe"], container="sandboxd")
