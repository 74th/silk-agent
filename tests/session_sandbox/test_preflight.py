from __future__ import annotations

import pytest

from gke_session_sandbox import ClusterCapabilityError, ManagerConfig
from gke_session_sandbox.preflight import validate_cluster_capabilities


class FakeApi:
    def __init__(self, *, denied: tuple[str, str, str] | None = None) -> None:
        self.denied = denied
        self.reviews = 0

    async def read_namespace(self, name: str):
        return {"metadata": {"name": name}}

    async def read_runtime_class(self, name: str):
        return {"handler": "runsc"}

    async def read_storage_class(self, name: str):
        return {"provisioner": "pd.csi.storage.gke.io"}

    async def get_cluster_custom_object(self, group: str, version: str, plural: str, name: str):
        served_version = "v1beta1" if name.startswith("sandboxes.") else "v1alpha1"
        return {"spec": {"versions": [{"name": served_version, "served": True}]}}

    async def create_self_subject_access_review(self, body: dict):
        self.reviews += 1
        attr = body["spec"]["resourceAttributes"]
        denied = self.denied == (attr["namespace"], attr["resource"], attr["verb"])
        return {"status": {"allowed": not denied, "reason": "missing RoleBinding" if denied else ""}}


class MissingCapabilityError(Exception):
    status = 404
    reason = "Not Found"


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("read_runtime_class", "RuntimeClass/gvisor: not found"),
        ("read_storage_class", "StorageClass/session-workspace-rwo: not found"),
        ("get_cluster_custom_object", "CustomResourceDefinition/sandboxes.agents.x-k8s.io: not found"),
    ],
)
@pytest.mark.asyncio
async def test_preflight_names_missing_cluster_capabilities(method, expected) -> None:
    class MissingApi(FakeApi):
        async def missing(self, *_args):
            raise MissingCapabilityError("Not Found")

    api = MissingApi()
    setattr(api, method, api.missing)
    apis = {"core_api": api, "node_api": api, "storage_api": api, "custom_api": api, "authorization_api": api}

    with pytest.raises(ClusterCapabilityError, match=expected.replace("/", r"/")):
        await validate_cluster_capabilities(ManagerConfig(runtime={"image": "example.invalid/sandbox:v1"}), **apis)


@pytest.mark.asyncio
async def test_preflight_checks_apis_namespaces_runtime_storage_and_rbac() -> None:
    api = FakeApi()
    config = ManagerConfig(
        runtime={"image": "example.invalid/sandbox:v1"},
        network={"allowed_fqdns": [{"name": "api.example.com", "ports": [{"protocol": "TCP", "port": 443}]}]},
    )

    checks = await validate_cluster_capabilities(
        config, core_api=api, node_api=api, storage_api=api, custom_api=api, authorization_api=api
    )

    assert "RuntimeClass/gvisor" in checks
    assert "CustomResourceDefinition/fqdnnetworkpolicies.networking.gke.io" in checks
    assert api.reviews > 0


@pytest.mark.asyncio
async def test_preflight_reports_missing_worker_permission_explicitly() -> None:
    api = FakeApi(denied=("session-sandboxes", "networkpolicies", "create"))

    with pytest.raises(ClusterCapabilityError, match="RBAC create networking.k8s.io/networkpolicies"):
        await validate_cluster_capabilities(
            ManagerConfig(runtime={"image": "example.invalid/sandbox:v1"}),
            core_api=api, node_api=api, storage_api=api, custom_api=api, authorization_api=api,
        )
