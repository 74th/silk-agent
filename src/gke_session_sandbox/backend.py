"""Core Agent Sandbox v1beta1 operations behind a narrow async API boundary."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Protocol

from .errors import ResourceOwnershipError, SandboxReadyTimeout
from .types import ResourceRef


class SandboxCustomObjectsApi(Protocol):
    async def create_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, body: Any) -> Any: ...
    async def get_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, name: str) -> Any: ...
    async def patch_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, name: str, body: Any) -> Any: ...
    async def delete_namespaced_custom_object(self, group: str, version: str, namespace: str, plural: str, name: str, body: Any = None) -> Any: ...


class SandboxCoreApi(Protocol):
    async def list_namespaced_pod(self, namespace: str, label_selector: str | None = None) -> Any: ...


class SandboxNetworkingApi(Protocol):
    async def create_namespaced_network_policy(self, namespace: str, body: Any) -> Any: ...
    async def read_namespaced_network_policy(self, name: str, namespace: str) -> Any: ...
    async def delete_namespaced_network_policy(self, name: str, namespace: str, body: Any = None) -> Any: ...


HealthCheck = Callable[[str | None, str | None], Awaitable[bool]]


class CoreSandboxBackend:
    """Create Suspended first, install policy, then start and observe the Sandbox."""

    GROUP = "agents.x-k8s.io"
    VERSION = "v1beta1"
    PLURAL = "sandboxes"

    def __init__(
        self,
        custom_api: SandboxCustomObjectsApi,
        core_api: SandboxCoreApi,
        networking_api: SandboxNetworkingApi,
    ) -> None:
        self._custom = custom_api
        self._core = core_api
        self._networking = networking_api

    async def create_suspended(
        self,
        manifest: Mapping[str, Any],
        *,
        namespace: str,
        name: str,
    ) -> ResourceRef:
        body = copy.deepcopy(dict(manifest))
        body["apiVersion"] = f"{self.GROUP}/{self.VERSION}"
        body["kind"] = "Sandbox"
        metadata = body.setdefault("metadata", {})
        metadata["name"] = name
        metadata["namespace"] = namespace
        spec = body.setdefault("spec", {})
        spec["operatingMode"] = "Suspended"
        try:
            created = await self._custom.create_namespaced_custom_object(
                self.GROUP, self.VERSION, namespace, self.PLURAL, body
            )
        except Exception as exc:
            if getattr(exc, "status", None) != 409:
                raise
            created = await self._custom.get_namespaced_custom_object(
                self.GROUP, self.VERSION, namespace, self.PLURAL, name
            )
            expected_labels = metadata.get("labels", {})
            actual_labels = _field(_field(created, "metadata", {}), "labels", {}) or {}
            if not expected_labels or any(actual_labels.get(key) != value for key, value in expected_labels.items()):
                raise ResourceOwnershipError("Sandbox name already belongs to another session generation") from exc
        return _sandbox_ref(created, namespace)

    async def create_policies(
        self,
        namespace: str,
        policies: Sequence[Mapping[str, Any]],
        *,
        owner: ResourceRef | None = None,
    ) -> tuple[ResourceRef, ...]:
        refs: list[ResourceRef] = []
        for policy in policies:
            body = copy.deepcopy(dict(policy))
            metadata = body.setdefault("metadata", {})
            metadata["namespace"] = namespace
            if owner is not None:
                metadata["ownerReferences"] = [{
                    "apiVersion": owner.api_version,
                    "kind": owner.kind,
                    "name": owner.name,
                    "uid": owner.uid,
                    "controller": False,
                    "blockOwnerDeletion": False,
                }]
            api_version, kind = body.get("apiVersion"), body.get("kind")
            name = metadata.get("name")
            if (api_version, kind) == ("networking.k8s.io/v1", "NetworkPolicy"):
                try:
                    created = await self._networking.create_namespaced_network_policy(namespace, body)
                except Exception as exc:
                    if getattr(exc, "status", None) != 409:
                        raise
                    created = await self._networking.read_namespaced_network_policy(name, namespace)
                    _validate_policy_owner(created, metadata, kind)
            elif (api_version, kind) == ("networking.gke.io/v1alpha1", "FQDNNetworkPolicy"):
                try:
                    created = await self._custom.create_namespaced_custom_object(
                        "networking.gke.io", "v1alpha1", namespace, "fqdnnetworkpolicies", body
                    )
                except Exception as exc:
                    if getattr(exc, "status", None) != 409:
                        raise
                    created = await self._custom.get_namespaced_custom_object(
                        "networking.gke.io", "v1alpha1", namespace, "fqdnnetworkpolicies", name
                    )
                    _validate_policy_owner(created, metadata, kind)
            else:
                raise ResourceOwnershipError(f"unsupported policy resource {api_version}/{kind}")
            refs.append(_resource_ref(created, namespace, api_version=api_version, kind=kind))
        return tuple(refs)

    async def create_with_policies(
        self,
        manifest: Mapping[str, Any],
        policies: Sequence[Mapping[str, Any]],
        *,
        namespace: str,
        name: str,
    ) -> tuple[ResourceRef, tuple[ResourceRef, ...]]:
        """Do not expose a Running Pod until its network policies exist."""
        sandbox_ref = await self.create_suspended(manifest, namespace=namespace, name=name)
        policy_refs = await self.create_policies(namespace, policies, owner=sandbox_ref)
        await self.start(sandbox_ref)
        return sandbox_ref, policy_refs

    async def start(self, ref: ResourceRef) -> ResourceRef:
        current = await self._read_owned(ref)
        patched = await self._custom.patch_namespaced_custom_object(
            self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name,
            [
                {"op": "test", "path": "/metadata/uid", "value": ref.uid},
                {"op": "replace", "path": "/spec/operatingMode", "value": "Running"},
            ],
        )
        return _sandbox_ref(patched or current, ref.namespace or "")

    async def suspend(self, ref: ResourceRef) -> None:
        await self._read_owned(ref)
        await self._custom.patch_namespaced_custom_object(
            self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name,
            [
                {"op": "test", "path": "/metadata/uid", "value": ref.uid},
                {"op": "replace", "path": "/spec/operatingMode", "value": "Suspended"},
            ],
        )

    async def set_expiration(self, ref: ResourceRef, expires_at) -> None:
        """Configure the controller's absolute Delete fallback for this Sandbox."""
        await self._read_owned(ref)
        timestamp = expires_at.isoformat().replace("+00:00", "Z")
        await self._custom.patch_namespaced_custom_object(
            self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name,
            [
                {"op": "test", "path": "/metadata/uid", "value": ref.uid},
                {"op": "add", "path": "/spec/shutdownTime", "value": timestamp},
                {"op": "add", "path": "/spec/shutdownPolicy", "value": "Delete"},
            ],
        )

    async def resume(self, ref: ResourceRef) -> ResourceRef:
        """Clear the absolute expiry and switch to Running in one UID-guarded patch."""
        current = await self._read_owned(ref)
        spec = _field(current, "spec", {}) or {}
        body = [{"op": "test", "path": "/metadata/uid", "value": ref.uid}]
        if _field(spec, "shutdownTime") is not None:
            body.append({"op": "remove", "path": "/spec/shutdownTime"})
        body.extend([
            {"op": "add", "path": "/spec/shutdownPolicy", "value": "Retain"},
            {"op": "replace", "path": "/spec/operatingMode", "value": "Running"},
        ])
        patched = await self._custom.patch_namespaced_custom_object(
            self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name, body
        )
        return _sandbox_ref(patched or current, ref.namespace or "")

    async def wait_suspended(
        self, ref: ResourceRef, *, timeout_seconds: float = 120, poll_interval_seconds: float = 2
    ) -> Mapping[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_diagnostic = "Sandbox has not reported suspension"
        while time.monotonic() < deadline:
            sandbox = await self._read_owned(ref)
            status = _field(sandbox, "status", {}) or {}
            conditions = _field(status, "conditions", []) or []
            condition = next((item for item in conditions if _field(item, "type") == "Suspended"), None)
            selector = _field(status, "selector")
            pods = await self._core.list_namespaced_pod(ref.namespace or "", label_selector=selector)
            if condition and _field(condition, "status") == "True" and not (_field(pods, "items", []) or []):
                return sandbox
            last_diagnostic = (
                f"Suspended={_field(condition, 'status')} reason={_field(condition, 'reason')} "
                f"matchingPods={len(_field(pods, 'items', []) or [])}"
            )
            await asyncio.sleep(poll_interval_seconds)
        raise SandboxReadyTimeout(f"Sandbox {ref.namespace}/{ref.name} did not suspend: {last_diagnostic}")

    async def wait_deleted(self, ref: ResourceRef, *, timeout_seconds: float = 120, poll_interval_seconds: float = 2) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                await self._read_owned(ref)
            except ResourceOwnershipError as exc:
                if "no longer exists" in str(exc):
                    return
                raise
            await asyncio.sleep(poll_interval_seconds)
        raise SandboxReadyTimeout(f"Sandbox {ref.namespace}/{ref.name} was not deleted before timeout")

    async def get_owned(self, ref: ResourceRef) -> Mapping[str, Any]:
        """Read the current owned Sandbox, including deletionTimestamp and conditions."""
        return await self._read_owned(ref)

    async def patch_annotations(self, ref: ResourceRef, annotations: Mapping[str, str]) -> None:
        """Merge library-owned annotations without replacing caller annotations."""
        current = await self._read_owned(ref)
        metadata = _field(current, "metadata", {}) or {}
        existing = _field(metadata, "annotations", {}) or {}
        body = [{"op": "test", "path": "/metadata/uid", "value": ref.uid}]
        if not existing:
            body.append({"op": "add", "path": "/metadata/annotations", "value": {}})
        for key, value in annotations.items():
            escaped = key.replace("~", "~0").replace("/", "~1")
            body.append({"op": "add", "path": f"/metadata/annotations/{escaped}", "value": value})
        await self._custom.patch_namespaced_custom_object(
            self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name, body
        )

    async def delete(self, ref: ResourceRef) -> None:
        await self._read_owned(ref)
        await self._custom.delete_namespaced_custom_object(
            self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name,
            body={"preconditions": {"uid": ref.uid}},
        )

    async def runtime_endpoint(self, ref: ResourceRef, *, tracker_port: int = 8081) -> str:
        """Resolve the current service FQDN for a Ready Sandbox generation."""
        sandbox = await self._read_owned(ref)
        status = _field(sandbox, "status", {}) or {}
        fqdn = _field(status, "serviceFQDN")
        if not isinstance(fqdn, str) or not fqdn.strip():
            raise ResourceOwnershipError("Sandbox status does not contain serviceFQDN")
        if "://" in fqdn or "/" in fqdn:
            raise ResourceOwnershipError("Sandbox serviceFQDN is not a DNS name")
        return f"http://{fqdn}:{tracker_port}"

    async def wait_ready(
        self,
        ref: ResourceRef,
        *,
        timeout_seconds: float = 180,
        poll_interval_seconds: float = 2,
        health_check: HealthCheck | None = None,
    ) -> Mapping[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_diagnostic = "Sandbox has not been observed"
        while time.monotonic() < deadline:
            sandbox = await self._read_owned(ref)
            metadata = _field(sandbox, "metadata", {})
            status = _field(sandbox, "status", {}) or {}
            conditions = _field(status, "conditions", []) or []
            ready = next((condition for condition in conditions if _field(condition, "type") == "Ready"), None)
            observed = _field(ready, "observedGeneration", 0) if ready else 0
            generation = _field(metadata, "generation", 0) or 0
            if ready and _field(ready, "status") == "True" and observed >= generation:
                selector = _field(status, "selector")
                pods = await self._core.list_namespaced_pod(ref.namespace or "", label_selector=selector)
                pod_items = _field(pods, "items", []) or []
                ready_pod = next((pod for pod in pod_items if _pod_ready(pod)), None)
                if ready_pod is not None:
                    pod_status = _field(ready_pod, "status", {}) or {}
                    service_fqdn = _field(status, "serviceFQDN")
                    pod_ip = _field(pod_status, "podIP")
                    if health_check is None or await health_check(service_fqdn, pod_ip):
                        snapshot = copy.deepcopy(sandbox)
                        if isinstance(snapshot, dict):
                            snapshot.setdefault("status", {})["podUID"] = _field(
                                _field(ready_pod, "metadata", {}), "uid"
                            )
                        return snapshot
                    last_diagnostic = "Sandbox controller and Pod are Ready, runtime health probe failed"
                else:
                    last_diagnostic = "Sandbox is Ready but no Ready Pod matches its selector"
            elif ready:
                last_diagnostic = (
                    f"Ready={_field(ready, 'status')} reason={_field(ready, 'reason')} "
                    f"observedGeneration={observed} generation={generation}"
                )
            else:
                last_diagnostic = "Sandbox has no Ready condition"
            await asyncio.sleep(poll_interval_seconds)
        raise SandboxReadyTimeout(f"Sandbox {ref.namespace}/{ref.name} did not become ready: {last_diagnostic}")

    async def _read_owned(self, ref: ResourceRef) -> Any:
        if ref.kind != "Sandbox" or ref.api_version != f"{self.GROUP}/{self.VERSION}" or not ref.namespace:
            raise ResourceOwnershipError("resource reference is not a namespaced core Sandbox")
        try:
            sandbox = await self._custom.get_namespaced_custom_object(
                self.GROUP, self.VERSION, ref.namespace, self.PLURAL, ref.name
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                raise ResourceOwnershipError("owned Sandbox no longer exists") from exc
            raise
        metadata = _field(sandbox, "metadata", {})
        if _field(metadata, "uid") != ref.uid:
            raise ResourceOwnershipError("Sandbox UID changed; refusing to mutate a replacement")
        return sandbox


def _sandbox_ref(sandbox: Any, namespace: str | None) -> ResourceRef:
    return _resource_ref(sandbox, namespace, api_version=f"{CoreSandboxBackend.GROUP}/{CoreSandboxBackend.VERSION}", kind="Sandbox")


def _resource_ref(resource: Any, namespace: str | None, *, api_version: str, kind: str) -> ResourceRef:
    metadata = _field(resource, "metadata", {})
    name = _field(metadata, "name")
    uid = _field(metadata, "uid")
    actual_namespace = _field(metadata, "namespace") or namespace
    if not name or not uid or not actual_namespace:
        raise ResourceOwnershipError("created Kubernetes resource has incomplete name / namespace / UID")
    return ResourceRef(api_version=api_version, kind=kind, namespace=actual_namespace, name=name, uid=uid)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    snake = "".join(("_" + char.lower()) if char.isupper() else char for char in name).lstrip("_")
    return getattr(value, snake, default)


def _pod_ready(pod: Any) -> bool:
    status = _field(pod, "status", {}) or {}
    conditions = _field(status, "conditions", []) or []
    return any(_field(condition, "type") == "Ready" and _field(condition, "status") == "True" for condition in conditions)


def _validate_policy_owner(existing: Any, expected_metadata: Mapping[str, Any], kind: str) -> None:
    metadata = _field(existing, "metadata", {})
    expected_labels = expected_metadata.get("labels", {}) or {}
    actual_labels = _field(metadata, "labels", {}) or {}
    if not expected_labels or any(actual_labels.get(key) != value for key, value in expected_labels.items()):
        raise ResourceOwnershipError(f"{kind} name already belongs to another owner")
