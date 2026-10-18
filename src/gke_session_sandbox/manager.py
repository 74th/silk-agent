"""Async session manager built from the repository and core Sandbox backend."""

from __future__ import annotations

import asyncio
import copy
import os
import socket
import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import yaml

from .backend import CoreSandboxBackend
from .commands import CommandTrackerClient
from .config import ManagerConfig
from .environments import (
    creation_environment_fingerprint,
    ensure_same_creation_environment,
    merge_template_environment,
)
from .errors import (
    ConfigurationError,
    ResourceOwnershipError,
    SandboxBusyError,
    SandboxLostError,
    SandboxNotReadyError,
    SessionExpiredError,
    SessionLeaseHeldError,
    SessionLeaseLostError,
    StaleGenerationError,
)
from .files import FileClient
from .kube import load_kube_client_config
from .network import build_session_network_policies, optional_policy_names, sandbox_pod_labels
from .repository import LeaseToken, StoredSession
from .repository_factory import create_local_repository
from .repository_port import SessionRepository
from .templates import TemplateRenderer
from .types import (
    CommandState,
    DesiredSandboxState,
    EventType,
    ResourceRef,
    SessionEvent,
    SessionKey,
    SessionRecord,
    SessionState,
    utc_now,
)
from .volumes import OwnedWorkspacePVCManager, attach_shared_code_volume

_ANNOTATION_TENANT_ID = "session-sandbox.example.com/tenant-id"
_ANNOTATION_SESSION_ID = "session-sandbox.example.com/session-id"

PolicyFactory = Callable[[SessionKey, int, Mapping[str, Any]], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True, slots=True)
class DiagnosticResult:
    exit_code: int
    output: str  # stdout and stderr combined


@dataclass(frozen=True, slots=True)
class NetworkReapplyResult:
    """Outcome of re-applying the current network settings to one conversation."""

    key_hash: str
    status: str  # "applied", "skipped" or "failed"
    detail: str = ""


@dataclass(frozen=True, slots=True)
class SessionHandle:
    """A generation-bound view; operations validate it against durable state."""

    _manager: SandboxManager
    key: SessionKey
    generation: int
    record: SessionRecord

    @property
    def state(self) -> SessionState:
        return self.record.state

    @property
    def sandbox_ref(self) -> ResourceRef | None:
        return self.record.sandbox_ref

    @property
    def pending_context_events(self) -> tuple[SessionEvent, ...]:
        return tuple(event for event in self.record.pending_events if event.acked_at is None)

    @property
    def commands(self) -> CommandTrackerClient:
        """Client that ensures this session is active and resolves its current runtime on demand."""

        async def resolve_endpoint() -> str:
            current = await self._manager.ensure(self.key)
            if current.generation != self.generation:
                raise StaleGenerationError("session handle refers to an older generation")
            if current.sandbox_ref is None:
                raise ResourceOwnershipError("session has no owned Sandbox reference")
            return await self._manager.backend.runtime_endpoint(current.sandbox_ref)

        return CommandTrackerClient(
            resolve_endpoint,
            buffered_response_limit_bytes=self._manager.config.lifecycle.buffered_response_limit_bytes,
            before_submit=lambda command_id: self._manager._before_command_submit(
                self.key, self.generation, command_id
            ),
            command_observer=lambda command_id, state: self._manager._observe_command(
                self.key, self.generation, command_id, state
            ),
        )

    @property
    def observe_commands(self) -> CommandTrackerClient:
        """A command client for observers (status, output): it never starts a Sandbox.

        `commands` resolves its endpoint with `ensure`, which resumes a suspended Sandbox and
        creates the next generation of a lost one. An observer polling for the result of a
        command must not do either: it raises `SandboxNotReadyError` while the Sandbox is not
        ready, and `SandboxLostError` / `StaleGenerationError` once the commands are gone.
        """

        async def resolve_endpoint() -> str:
            stored = await self._manager.repository.get(self.key)
            if stored is None:
                raise SandboxLostError("session no longer exists")
            record = stored.record
            if record.generation != self.generation:
                raise StaleGenerationError("session handle refers to an older generation")
            if record.state in {SessionState.LOST, SessionState.TERMINATED}:
                raise SandboxLostError(f"Sandbox is {record.state.value}")
            if record.state is not SessionState.READY:
                raise SandboxNotReadyError(f"Sandbox is {record.state.value}")
            if record.sandbox_ref is None:
                raise ResourceOwnershipError("session has no owned Sandbox reference")
            return await self._manager.backend.runtime_endpoint(record.sandbox_ref)

        return CommandTrackerClient(
            resolve_endpoint,
            buffered_response_limit_bytes=self._manager.config.lifecycle.buffered_response_limit_bytes,
            command_observer=lambda command_id, state: self._manager._observe_command(
                self.key, self.generation, command_id, state
            ),
        )

    @property
    def files(self) -> FileClient:
        """Client for the current sandboxd REST filesystem endpoint."""

        async def resolve_endpoint() -> str:
            current = await self._manager.ensure(self.key)
            if current.generation != self.generation:
                raise StaleGenerationError("session handle refers to an older generation")
            if current.sandbox_ref is None:
                raise ResourceOwnershipError("session has no owned Sandbox reference")
            return await self._manager.backend.runtime_endpoint(
                current.sandbox_ref, tracker_port=8080
            )

        return FileClient(
            resolve_endpoint,
            activity_context=lambda: self._manager._file_operation(self.key, self.generation),
        )

    async def refresh(self) -> SessionHandle:
        handle = await self._manager.get(self.key)
        if handle is None:
            raise ResourceOwnershipError("session no longer exists")
        if handle.generation != self.generation:
            raise StaleGenerationError("session handle refers to an older generation")
        return handle


class SandboxManager:
    """Manage one Sandbox generation per stable SessionKey."""

    def __init__(
        self,
        config: ManagerConfig,
        repository: SessionRepository,
        backend: CoreSandboxBackend,
        *,
        renderer: TemplateRenderer | None = None,
        template: Mapping[str, Any] | None = None,
        policy_factory: PolicyFactory | None = None,
        pvc_manager: OwnedWorkspacePVCManager | None = None,
        api_client: Any | None = None,
        exec_api_client: Any | None = None,
        holder_identity: str | None = None,
    ) -> None:
        self.config = config
        self.repository = repository
        self.backend = backend
        self._renderer = renderer or TemplateRenderer()
        self._template = copy.deepcopy(dict(template)) if template is not None else None
        self._policy_factory = policy_factory or (
            lambda key, generation, _manifest: build_session_network_policies(
                config, key, generation
            )
        )
        self._pvc_manager = pvc_manager
        self._api_client = api_client
        self._exec_api_client = exec_api_client
        self._holder_identity = (
            holder_identity or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex}"
        )
        self._closed = False

    @classmethod
    async def from_config(
        cls,
        config_path: str | Path,
        *,
        kube_context: str | None = None,
        policy_factory: PolicyFactory | None = None,
        repository: SessionRepository | None = None,
    ) -> SandboxManager:
        """Load config; create async Kubernetes API adapters (in-cluster or kubeconfig)."""
        config_file = Path(config_path)
        try:
            raw_config = yaml.safe_load(
                await asyncio.to_thread(config_file.read_text, encoding="utf-8")
            )
            settings = ManagerConfig.model_validate(raw_config)
        except Exception as exc:
            raise ConfigurationError(f"cannot load manager config {config_file}: {exc}") from exc
        try:
            from kubernetes_asyncio import client
        except ImportError as exc:
            raise ConfigurationError(
                "install the kubernetes-asyncio dependency to connect to Kubernetes"
            ) from exc

        await load_kube_client_config(kube_context)
        repository = repository if repository is not None else create_local_repository(settings)
        api_client = client.ApiClient()
        # kubernetes_asyncio's generated clients satisfy the backend protocols at runtime.
        core_api: Any = client.CoreV1Api(api_client)
        custom_api: Any = client.CustomObjectsApi(api_client)
        networking_api: Any = client.NetworkingV1Api(api_client)
        from kubernetes_asyncio.stream import WsApiClient

        exec_api_client = WsApiClient()
        exec_api: Any = client.CoreV1Api(exec_api_client)
        backend = CoreSandboxBackend(custom_api, core_api, networking_api, exec_api=exec_api)
        template_path = Path(settings.template_file)
        if not template_path.is_absolute():
            template_path = (
                config_file.parent.parent / template_path
                if config_file.parent.name == "examples"
                else config_file.parent / template_path
            )
        template_renderer = TemplateRenderer()
        template = template_renderer.render_file(
            template_path,
            parameters=_template_parameters(settings),
            overrides=settings.template_overrides,
            image_overrides={settings.runtime.container_name: settings.runtime.image},
        ).manifest
        return cls(
            settings,
            repository,
            backend,
            renderer=template_renderer,
            template=template,
            policy_factory=policy_factory,
            pvc_manager=OwnedWorkspacePVCManager(core_api),
            api_client=api_client,
            exec_api_client=exec_api_client,
        )

    async def ensure(
        self, key: SessionKey, *, env: Mapping[str, str] | None = None
    ) -> SessionHandle:
        self._check_open()
        async with self._session_lease(key) as token:
            stored = await self.repository.get(key)
            if stored is not None:
                self._check_creation_environment(stored.record, env)
                if stored.record.state is SessionState.READY:
                    return self._handle(key, stored.record)
                if stored.record.state is SessionState.SUSPENDED:
                    if (
                        stored.record.expires_at is not None
                        and utc_now() >= stored.record.expires_at
                    ):
                        stored = await self._expire_locked(token, key, stored)
                        return await self._create_locked(token, key, env, previous=stored)
                    return await self._resume_locked(token, key, stored)
                if stored.record.state is SessionState.CREATING:
                    if (
                        stored.record.creation_env_fingerprint
                        and env is None
                        and stored.record.sandbox_ref is None
                    ):
                        raise ConfigurationError(
                            "resuming an interrupted initial create requires the original env map"
                        )
                    return await self._continue_creation_locked(token, key, stored, env)
                if stored.record.state is SessionState.RESUMING and stored.record.sandbox_ref:
                    # An interrupted resume may have failed in the policy step: never start the
                    # Pod on policies older than the current settings.
                    await self._apply_current_policies_locked(
                        key, stored.record.generation, stored.record.sandbox_ref
                    )
                    await self.backend.resume(stored.record.sandbox_ref)
                    ready = await self.backend.wait_ready(
                        stored.record.sandbox_ref, timeout_seconds=self._ready_timeout
                    )
                    await self._open_tracker_gate(stored.record.sandbox_ref)
                    return await self._save_ready(token, key, stored, ready)
                if stored.record.state in {SessionState.TERMINATED, SessionState.LOST}:
                    return await self._create_locked(token, key, env, previous=stored)
                raise ResourceOwnershipError(
                    f"session is in unrecoverable operation state {stored.record.state.value!r}"
                )
            return await self._create_locked(token, key, env)

    async def get(self, key: SessionKey) -> SessionHandle | None:
        self._check_open()
        stored = await self.repository.get(key)
        if stored is None:
            return None
        return self._handle(key, stored.record)

    async def pending_context_events(self, key: SessionKey) -> tuple[SessionEvent, ...]:
        """Return unacknowledged workspace context events for at-least-once delivery."""
        handle = await self.get(key)
        return handle.pending_context_events if handle is not None else ()

    async def ack_context_event(self, key: SessionKey, event_id: str) -> bool:
        """Acknowledge an event after the caller has durably appended its context."""
        self._check_open()
        async with self._session_lease(key) as token:
            return await self.repository.acknowledge_event(token, event_id)

    async def suspend(self, session: SessionKey | SessionHandle) -> SessionHandle:
        self._check_open()
        key, generation = _session_args(session)
        async with self._session_lease(key) as token:
            stored = await self._current_record(key, generation)
            record = stored.record
            if record.state is SessionState.SUSPENDED:
                return self._handle(key, record)
            if record.state is not SessionState.READY:
                raise ResourceOwnershipError(
                    f"session cannot suspend from state {record.state.value!r}"
                )
            if record.sandbox_ref is None:
                raise ResourceOwnershipError("session has no owned Sandbox reference")
            endpoint = await self.backend.runtime_endpoint(record.sandbox_ref)
            active_count = await self._set_tracker_gate(endpoint, False)
            intent_saved = False
            if (
                active_count != 0
                or record.active_commands
                or record.active_file_operations
                or not record.command_state_known
            ):
                await self._set_tracker_gate(endpoint, True)
                raise SandboxBusyError("session has active or unconfirmed command/file work")
            intent = record.model_copy(
                update={
                    "state": SessionState.SUSPENDING,
                    "desired_state": DesiredSandboxState.SUSPENDED,
                    "operation_id": record.operation_id or uuid.uuid4().hex,
                    "operation_phase": "suspend-intent",
                }
            )
            try:
                stored = await self.repository.compare_and_swap(
                    token, intent, expected_resource_version=stored.resource_version
                )
                intent_saved = True
                await self.backend.suspend(record.sandbox_ref)
                await self.backend.wait_suspended(
                    record.sandbox_ref,
                    timeout_seconds=self.config.lifecycle.reconcile_interval_seconds * 2,
                )
                now = utc_now()
                expires_at = now + timedelta(seconds=self.config.lifecycle.retention_seconds)
                await self.backend.set_expiration(record.sandbox_ref, expires_at)
                finished = stored.record.model_copy(
                    update={
                        "state": SessionState.SUSPENDED,
                        "suspended_at": now,
                        "expires_at": expires_at,
                        "operation_phase": "suspended",
                    }
                )
                saved = await self.repository.compare_and_swap(
                    token, finished, expected_resource_version=stored.resource_version
                )
                await self.backend.patch_annotations(
                    record.sandbox_ref, _session_annotations(saved.record, 0)
                )
                return self._handle(key, saved.record)
            except Exception:
                if not intent_saved:
                    await self._set_tracker_gate(endpoint, True)
                raise

    async def resume(self, session: SessionKey | SessionHandle) -> SessionHandle:
        self._check_open()
        key, generation = _session_args(session)
        async with self._session_lease(key) as token:
            stored = await self._current_record(key, generation)
            return await self._resume_locked(token, key, stored)

    async def terminate(
        self, session: SessionKey | SessionHandle, *, reason: str = "explicit"
    ) -> SessionHandle:
        self._check_open()
        key, generation = _session_args(session)
        async with self._session_lease(key) as token:
            stored = await self._current_record(key, generation)
            record = stored.record
            if record.sandbox_ref is None:
                return self._handle(key, record)
            intent = record.model_copy(
                update={
                    "state": SessionState.TERMINATING,
                    "desired_state": None,
                    "operation_id": record.operation_id or uuid.uuid4().hex,
                    "operation_phase": f"terminate-intent:{reason}",
                }
            )
            stored = await self.repository.compare_and_swap(
                token, intent, expected_resource_version=stored.resource_version
            )
            await self.backend.delete(record.sandbox_ref)
            await self.backend.wait_deleted(
                record.sandbox_ref,
                timeout_seconds=self.config.lifecycle.reconcile_interval_seconds * 2,
            )
            terminated = stored.record.model_copy(
                update={
                    "state": SessionState.TERMINATED,
                    "operation_phase": "terminated",
                    "active_commands": (),
                    "pending_events": _append_workspace_event(
                        stored.record, EventType.WORKSPACE_CLEARED, reason, utc_now()
                    ),
                }
            )
            saved = await self.repository.compare_and_swap(
                token, terminated, expected_resource_version=stored.resource_version
            )
            return self._handle(key, saved.record)

    async def list_sessions(self, state: SessionState | None = None) -> tuple[SessionRecord, ...]:
        self._check_open()
        records = await self.repository.list_records()
        return tuple(item.record for item in records if state is None or item.record.state is state)

    async def run_diagnostic_command(
        self, key: SessionKey, command: Sequence[str], *, timeout_seconds: int = 60
    ) -> DiagnosticResult:
        """Run a command in a READY session without touching its lifecycle or workspace.

        Unlike ``SessionHandle.commands`` this never resumes, re-creates or extends the session
        and records nothing in the repository. It also does not go through the in-Pod command
        tracker, so nothing is journaled under /workspace and idle detection sees no activity.
        A session that is not READY is refused rather than woken up.
        """
        self._check_open()
        stored = await self.repository.get(key)
        if stored is None:
            raise ConfigurationError("no such session")
        record = stored.record
        if record.state is not SessionState.READY or record.sandbox_ref is None:
            raise ConfigurationError(
                f"session is {record.state.value}, not ready; diagnostics do not resume sessions"
            )
        exit_code, output = await self.backend.exec_diagnostic(
            record.sandbox_ref,
            command,
            container=self.config.runtime.container_name,
            timeout_seconds=timeout_seconds,
        )
        return DiagnosticResult(exit_code, output)

    async def reapply_network_policies(
        self, key: SessionKey | None = None
    ) -> tuple[NetworkReapplyResult, ...]:
        """Re-apply the current network settings without restarting any Pod.

        With a key, only that conversation; without one, every stored conversation. Each
        conversation is handled under its own lease, which is not waited for: one held by
        a lifecycle operation is reported as skipped so the caller can retry it.
        """
        self._check_open()
        if key is not None:
            return (await self._reapply_network_by_hash(key.stable_hash(), key),)
        records = await self.repository.list_records()
        return tuple(
            [await self._reapply_network_by_hash(item.record.key_hash, None) for item in records]
        )

    async def _reapply_network_by_hash(
        self, key_hash: str, key: SessionKey | None
    ) -> NetworkReapplyResult:
        try:
            async with self._session_lease_by_hash(key_hash, wait=False):
                stored = await self.repository.get_by_hash(key_hash)
                if stored is None:
                    return NetworkReapplyResult(key_hash, "skipped", "no such session")
                record = stored.record
                ref = record.sandbox_ref
                if ref is None or record.state not in (SessionState.READY, SessionState.SUSPENDED):
                    return NetworkReapplyResult(
                        key_hash, "skipped", f"state {record.state.value} has no settled Sandbox"
                    )
                if key is None:
                    key = await self._session_key_from_sandbox(key_hash, ref)
                await self._apply_current_policies_locked(key, record.generation, ref)
                return NetworkReapplyResult(
                    key_hash, "applied", f"generation {record.generation} {record.state.value}"
                )
        except SessionLeaseHeldError:
            return NetworkReapplyResult(key_hash, "skipped", "session lease is held")
        except Exception as exc:
            return NetworkReapplyResult(key_hash, "failed", f"{type(exc).__name__}: {exc}")

    async def apply_policies_before_resume_locked(
        self, record: SessionRecord, key_hash: str
    ) -> None:
        """Policy step of a resume whose caller only has the stored record (the reconciler).

        Called under the session lease. Failure propagates so the session stays RESUMING and
        is retried, rather than starting the Sandbox on stale network policies.
        """
        assert record.sandbox_ref is not None
        key = await self._session_key_from_sandbox(key_hash, record.sandbox_ref)
        await self._apply_current_policies_locked(key, record.generation, record.sandbox_ref)

    async def _session_key_from_sandbox(self, key_hash: str, ref: ResourceRef) -> SessionKey:
        """Recover the key from the Sandbox annotations, which the hash alone cannot give."""
        sandbox = await self.backend.get_owned(ref)
        annotations = (sandbox.get("metadata", {}) or {}).get("annotations", {}) or {}
        tenant_id = annotations.get(_ANNOTATION_TENANT_ID)
        session_id = annotations.get(_ANNOTATION_SESSION_ID)
        if not tenant_id or not session_id:
            raise ResourceOwnershipError("Sandbox has no tenant / session annotations")
        key = SessionKey(tenant_id=tenant_id, session_id=session_id)
        if key.stable_hash() != key_hash:
            raise ResourceOwnershipError("Sandbox annotations do not match the stored session")
        return key

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close_repository = getattr(self.repository, "close", None)
        if close_repository is not None:
            await close_repository()
        if self._exec_api_client is not None:
            await self._exec_api_client.close()
        if self._api_client is not None:
            await self._api_client.close()

    async def __aenter__(self) -> SandboxManager:
        self._check_open()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def _create_locked(
        self,
        token: LeaseToken,
        key: SessionKey,
        env: Mapping[str, str] | None,
        *,
        previous: StoredSession | None = None,
    ) -> SessionHandle:
        generation = previous.record.generation + 1 if previous is not None else 1
        manifest, digest = self._render_manifest(env)
        fingerprint = self._fingerprint(env)
        record = SessionRecord(
            key_hash=key.stable_hash(),
            generation=generation,
            state=SessionState.CREATING,
            desired_state=DesiredSandboxState.RUNNING,
            template_digest=digest,
            creation_env_fingerprint=fingerprint,
            operation_id=uuid.uuid4().hex,
            operation_phase="sandbox-create-intent",
        )
        if previous is None:
            stored = await self.repository.create(token, record)
        else:
            record = record.model_copy(update={"pending_events": previous.record.pending_events})
            stored = await self.repository.compare_and_swap(
                token, record, expected_resource_version=previous.resource_version
            )
        name = f"session-{key.stable_hash()[:10]}-g{generation}"
        return await self._continue_creation_locked(
            token, key, stored, env, manifest=manifest, name=name
        )

    async def _continue_creation_locked(
        self,
        token: LeaseToken,
        key: SessionKey,
        stored: StoredSession,
        env: Mapping[str, str] | None,
        *,
        manifest: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> SessionHandle:
        record = stored.record
        if manifest is None:
            manifest, digest = self._render_manifest(env)
            if record.template_digest != digest:
                raise ConfigurationError(
                    "template changed during interrupted create; explicit recovery is required"
                )
        generation = record.generation
        name = name or f"session-{key.stable_hash()[:10]}-g{generation}"
        labels = manifest.setdefault("metadata", {}).setdefault("labels", {})
        labels.update(
            {
                "session-sandbox.example.com/managed": "true",
                "session-sandbox.example.com/session-key-hash": key.stable_hash()[:63],
                "session-sandbox.example.com/generation": str(generation),
            }
        )
        manifest.setdefault("metadata", {}).setdefault("annotations", {}).update(
            {
                "session-sandbox.example.com/tenant-id": key.tenant_id,
                "session-sandbox.example.com/session-id": key.session_id,
            }
        )
        pod_template = manifest.setdefault("spec", {}).setdefault("podTemplate", {})
        pod_labels = pod_template.setdefault("metadata", {}).setdefault("labels", {})
        pod_labels.update(sandbox_pod_labels(key, generation))
        policies = self._render_policies(key, generation, manifest, labels)
        ref = record.sandbox_ref
        if ref is None:
            ref, _policy_refs = await self.backend.create_with_policies(
                manifest,
                policies,
                namespace=self.config.namespace,
                name=name,
            )
            workspace_refs = (
                await self._pvc_manager.list_owned(ref) if self._pvc_manager is not None else ()
            )
            creating = record.model_copy(
                update={
                    "sandbox_ref": ref,
                    "workspace_pvc_refs": workspace_refs,
                    "operation_phase": "sandbox-running",
                }
            )
            stored = await self.repository.compare_and_swap(
                token, creating, expected_resource_version=stored.resource_version
            )
        ready = await self.backend.wait_ready(ref, timeout_seconds=self._ready_timeout)
        return await self._save_ready(token, key, stored, ready)

    def _render_policies(
        self,
        key: SessionKey,
        generation: int,
        manifest: Mapping[str, Any],
        labels: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        policies = [
            copy.deepcopy(dict(policy))
            for policy in self._policy_factory(key, generation, manifest)
        ]
        for policy in policies:
            policy_labels = policy.setdefault("metadata", {}).setdefault("labels", {})
            policy_labels.update(labels)
        return policies

    async def _apply_current_policies_locked(
        self, key: SessionKey, generation: int, ref: ResourceRef
    ) -> tuple[ResourceRef, ...]:
        """Bring one generation's policies in line with the current config, without a restart."""
        manifest, _digest = self._render_manifest(None)
        labels = sandbox_pod_labels(key, generation)
        policies = self._render_policies(key, generation, manifest, labels)
        applied = await self.backend.apply_policies(self.config.namespace, policies, owner=ref)
        rendered = {policy["metadata"]["name"] for policy in policies}
        await self.backend.delete_policies(
            self.config.namespace,
            [name for name in optional_policy_names(key, generation) if name[2] not in rendered],
            owner=ref,
            labels=labels,
        )
        return applied

    async def _resume_locked(
        self, token: LeaseToken, key: SessionKey, stored: StoredSession
    ) -> SessionHandle:
        record = stored.record
        if record.state is not SessionState.SUSPENDED or record.sandbox_ref is None:
            if record.state is SessionState.READY:
                return self._handle(key, record)
            raise ResourceOwnershipError(f"session cannot resume from state {record.state.value!r}")
        if record.expires_at is not None and utc_now() >= record.expires_at:
            raise SessionExpiredError("session generation has passed its retention deadline")
        intent = record.model_copy(
            update={
                "state": SessionState.RESUMING,
                "desired_state": DesiredSandboxState.RUNNING,
                "operation_phase": "resume-intent",
            }
        )
        stored = await self.repository.compare_and_swap(
            token, intent, expected_resource_version=stored.resource_version
        )
        await self._apply_current_policies_locked(key, record.generation, record.sandbox_ref)
        await self.backend.resume(record.sandbox_ref)
        ready = await self.backend.wait_ready(
            record.sandbox_ref, timeout_seconds=self._ready_timeout
        )
        await self._open_tracker_gate(record.sandbox_ref)
        return await self._save_ready(token, key, stored, ready)

    async def _expire_locked(
        self, token: LeaseToken, key: SessionKey, stored: StoredSession
    ) -> StoredSession:
        record = stored.record
        if record.sandbox_ref is not None:
            snapshot = None
            try:
                snapshot = await self.backend.get_owned(record.sandbox_ref)
            except ResourceOwnershipError as exc:
                if "no longer exists" not in str(exc):
                    raise
            deletion_timestamp = (
                snapshot.get("metadata", {}).get("deletionTimestamp")
                if snapshot is not None
                else None
            )
            if snapshot is not None and not deletion_timestamp:
                try:
                    await self.backend.delete(record.sandbox_ref)
                except ResourceOwnershipError as exc:
                    # The controller may delete the Sandbox at shutdownTime between our
                    # ownership read and DELETE request. Treat that race as already removed.
                    if "no longer exists" not in str(exc):
                        raise
            await self.backend.wait_deleted(
                record.sandbox_ref,
                timeout_seconds=self.config.lifecycle.reconcile_interval_seconds * 2,
            )
            if self._pvc_manager is not None:
                refs = record.workspace_pvc_refs or await self._pvc_manager.list_owned(
                    record.sandbox_ref
                )
                for pvc_ref in refs:
                    await self._pvc_manager.delete_owned(
                        pvc_ref, sandbox_uid=record.sandbox_ref.uid
                    )
        current = await self.repository.get(key)
        if current is None:
            raise ResourceOwnershipError("session disappeared during expiry cleanup")
        terminated = current.record.model_copy(
            update={
                "state": SessionState.TERMINATED,
                "desired_state": None,
                "operation_phase": "expired-cleanup-complete",
                "active_commands": (),
                "active_file_operations": 0,
                "pending_events": _append_workspace_event(
                    current.record, EventType.WORKSPACE_CLEARED, "retention-expired", utc_now()
                ),
            }
        )
        return await self.repository.compare_and_swap(
            token, terminated, expected_resource_version=current.resource_version
        )

    async def _open_tracker_gate(self, ref: ResourceRef) -> None:
        endpoint = await self.backend.runtime_endpoint(ref)
        await self._set_tracker_gate(endpoint, True)

    @staticmethod
    async def _set_tracker_gate(endpoint: str, accepting: bool) -> int:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 30
        delay = 0.1
        last_error: BaseException | None = None
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=min(5.0, remaining))
                ) as http:
                    async with http.put(
                        f"{endpoint}/v1/commands/gate", json={"accepting": accepting}
                    ) as response:
                        if response.status >= 500:
                            last_error = RuntimeError(
                                f"tracker gate update returned HTTP {response.status}"
                            )
                        elif response.status != 200:
                            raise ResourceOwnershipError(
                                f"tracker gate update returned HTTP {response.status}"
                            )
                        else:
                            body = await response.json()
                            active_count = body.get("active_commands")
                            if (
                                not isinstance(active_count, int)
                                or isinstance(active_count, bool)
                                or active_count < 0
                            ):
                                raise ResourceOwnershipError(
                                    "tracker returned an invalid active command count"
                                )
                            return active_count
            except (TimeoutError, aiohttp.ClientError, OSError) as exc:
                last_error = exc
            remaining = deadline - loop.time()
            if remaining > 0:
                await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, 1.0)
        if last_error is not None:
            raise last_error
        raise TimeoutError("tracker gate update exceeded its 30-second deadline")

    async def _save_ready(
        self, token: LeaseToken, key: SessionKey, stored: StoredSession, sandbox: Mapping[str, Any]
    ) -> SessionHandle:
        ready_record = stored.record.model_copy(
            update={
                "state": SessionState.READY,
                "ready_at": utc_now(),
                "runtime_instance_id": _pod_uid(sandbox) or stored.record.runtime_instance_id,
                "desired_state": DesiredSandboxState.RUNNING,
                "operation_phase": "ready",
            }
        )
        saved = await self.repository.compare_and_swap(
            token, ready_record, expected_resource_version=stored.resource_version
        )
        return self._handle(key, saved.record)

    async def _current_record(self, key: SessionKey, generation: int | None) -> StoredSession:
        stored = await self.repository.get(key)
        if stored is None:
            raise ResourceOwnershipError("session does not exist")
        if generation is not None and generation != stored.record.generation:
            raise StaleGenerationError("session handle refers to an older generation")
        return stored

    def _render_manifest(self, env: Mapping[str, str] | None) -> tuple[dict[str, Any], str]:
        if self._template is None:
            template_path = Path(self.config.template_file)
            rendered = self._renderer.render_file(
                template_path, parameters=_template_parameters(self.config)
            )
            manifest, digest = rendered.manifest, rendered.digest
        else:
            rendered = self._renderer.render(
                self._template,
                parameters=_template_parameters(self.config),
                overrides=self.config.template_overrides,
                image_overrides={self.config.runtime.container_name: self.config.runtime.image},
            )
            manifest, digest = rendered.manifest, rendered.digest
        pod_spec = manifest["spec"]["podTemplate"]["spec"]
        containers = [
            container
            for container in pod_spec["containers"]
            if container.get("name") == self.config.runtime.container_name
        ]
        if len(containers) != 1:
            raise ResourceOwnershipError(
                f"template must contain one runtime container "
                f"{self.config.runtime.container_name!r}"
            )
        runtime_limits = {
            "SESSION_SANDBOX_COMMAND_OUTPUT_LIMIT_BYTES": str(
                self.config.lifecycle.command_output_limit_bytes
            ),
            "SESSION_SANDBOX_SESSION_OUTPUT_LIMIT_BYTES": str(
                self.config.lifecycle.session_output_limit_bytes
            ),
            "SESSION_SANDBOX_BUFFERED_RESPONSE_LIMIT_BYTES": str(
                self.config.lifecycle.buffered_response_limit_bytes
            ),
            "SESSION_SANDBOX_OUTPUT_RETENTION_SECONDS": str(
                self.config.lifecycle.retention_seconds
            ),
        }
        containers[0]["env"] = [
            *merge_template_environment(containers[0].get("env"), env),
            *({"name": name, "value": value} for name, value in runtime_limits.items()),
        ]
        if self.config.shared_code_claim:
            manifest = attach_shared_code_volume(
                manifest,
                container_name=self.config.runtime.container_name,
                claim_name=self.config.shared_code_claim,
            )
            digest = self._renderer.render(manifest).digest
        return manifest, digest

    def _fingerprint(self, env: Mapping[str, str] | None) -> str | None:
        if env is None:
            return None
        key = self.config.environment_fingerprint_key
        if key is None:
            raise ConfigurationError(
                "environment_fingerprint_key is required when ensure receives env"
            )
        return creation_environment_fingerprint(env, key.get_secret_value().encode("utf-8"))

    def _check_creation_environment(
        self, record: SessionRecord, env: Mapping[str, str] | None
    ) -> None:
        if env is None:
            return
        key = self.config.environment_fingerprint_key
        if key is None:
            raise ConfigurationError(
                "environment_fingerprint_key is required when ensure receives env"
            )
        ensure_same_creation_environment(
            record.creation_env_fingerprint,
            env,
            key.get_secret_value().encode("utf-8"),
        )

    async def _before_command_submit(
        self, key: SessionKey, generation: int, command_id: str
    ) -> None:
        handle = await self.ensure(key)
        if handle.generation != generation:
            raise StaleGenerationError("session handle refers to an older generation")
        if handle.record.runtime_instance_id is None:
            raise ResourceOwnershipError(
                "session runtime identity is not ready for command admission"
            )
        async with self._session_lease(key) as token:
            stored = await self._current_record(key, generation)
            if stored.record.state is not SessionState.READY:
                raise ResourceOwnershipError("session is not ready for command admission")
            runtime_instance_id = stored.record.runtime_instance_id
            if runtime_instance_id is None:
                raise ResourceOwnershipError("ready session has no runtime instance id")
            await self.repository.reserve_command(
                token,
                generation=generation,
                command_id=command_id,
                runtime_instance_id=runtime_instance_id,
            )

    async def _observe_command(
        self, key: SessionKey, generation: int, command_id: str, observation: Mapping[str, object]
    ) -> None:
        async with self._session_lease(key) as token:
            stored = await self._current_record(key, generation)
            await self._save_command_observation(token, stored, command_id, observation)

    async def _save_command_observation(
        self,
        token: LeaseToken,
        stored: StoredSession,
        command_id: str,
        observation: Mapping[str, object],
    ) -> StoredSession:
        record = stored.record
        command = next(
            (item for item in record.active_commands if item.command_id == command_id), None
        )
        if command is None:
            return stored
        raw_state = observation.get("state")
        state = (
            CommandState.FINISHED
            if raw_state == "finished"
            else CommandState.UNKNOWN
            if raw_state == "unknown"
            else CommandState.RUNNING
        )
        started_at = _parse_timestamp(observation.get("started_at")) or command.started_at
        finished_at = _parse_timestamp(observation.get("finished_at"))
        updated_command = command.model_copy(
            update={
                "state": state,
                "started_at": started_at,
                "finished_at": finished_at if state is CommandState.FINISHED else None,
                "exit_code": observation.get("exit_code")
                if isinstance(observation.get("exit_code"), int)
                else None,
                "output_complete": bool(
                    observation.get("output_complete", command.output_complete)
                ),
            }
        )
        commands = tuple(
            updated_command if item.command_id == command_id else item
            for item in record.active_commands
        )
        changes: dict[str, object] = {
            "active_commands": commands,
            "command_state_known": all(item.state is not CommandState.UNKNOWN for item in commands),
        }
        if started_at is not None:
            changes["last_command_started_at"] = started_at
        if state is CommandState.FINISHED:
            changes["active_commands"] = tuple(
                item for item in commands if item.command_id != command_id
            )
            changes["last_command_finished_at"] = finished_at or utc_now()
            if not changes["active_commands"]:
                changes["idle_since"] = changes["last_command_finished_at"]
                changes["command_state_known"] = True
        elif state is CommandState.RUNNING:
            changes["idle_since"] = None
        changed = record.model_copy(update=changes)
        if changed == record:
            return stored
        return await self.repository.compare_and_swap(
            token, changed, expected_resource_version=stored.resource_version
        )

    @asynccontextmanager
    async def _file_operation(self, key: SessionKey, generation: int) -> AsyncIterator[None]:
        operation_id = uuid.uuid4().hex
        handle = await self.ensure(key)
        if handle.generation != generation:
            raise StaleGenerationError("session handle refers to an older generation")
        async with self._session_lease(key) as token:
            stored = await self._current_record(key, generation)
            if stored.record.state is not SessionState.READY:
                raise ResourceOwnershipError("file operation requires a ready session")
            await self.repository.begin_file_operation(
                token,
                operation_id=operation_id,
                generation=generation,
                executor_id=self._holder_identity,
                expires_at=utc_now()
                + timedelta(seconds=self.config.session_lease_duration_seconds),
            )
        confirmed = False
        try:
            yield
            confirmed = True
        finally:
            async with self._session_lease(key) as token:
                await self.repository.finish_file_operation(
                    token,
                    operation_id=operation_id,
                    generation=generation,
                    executor_id=self._holder_identity,
                    confirmed=confirmed,
                )

    def _handle(self, key: SessionKey, record: SessionRecord) -> SessionHandle:
        return SessionHandle(self, key, record.generation, record)

    @property
    def _ready_timeout(self) -> float:
        return float(self.config.runtime.ready_timeout_seconds)

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("SandboxManager is closed")

    @asynccontextmanager
    async def _session_lease(self, key: SessionKey) -> AsyncIterator[LeaseToken]:
        async with self._session_lease_by_hash(key.stable_hash()) as token:
            yield token

    @asynccontextmanager
    async def _session_lease_by_hash(
        self, key_hash: str, *, wait: bool = True
    ) -> AsyncIterator[LeaseToken]:
        holder = f"{self._holder_identity}-{uuid.uuid4().hex}"
        deadline = asyncio.get_running_loop().time() + self.config.lease_acquire_timeout_seconds
        delay = 0.05
        while True:
            try:
                token = await self.repository.acquire_lease_by_hash(key_hash, holder)
                break
            except SessionLeaseHeldError:
                if not wait or asyncio.get_running_loop().time() >= deadline:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 0.5)
        heartbeat = asyncio.create_task(self._renew_lease(token))
        try:
            yield token
            if heartbeat.done():
                error = heartbeat.exception()
                if error is not None:
                    raise SessionLeaseLostError("session Lease renewal failed") from error
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            await self.repository.release_lease(token)

    async def _renew_lease(self, token: LeaseToken) -> None:
        interval = max(1, self.config.session_lease_duration_seconds // 3)
        while True:
            await asyncio.sleep(interval)
            await self.repository.renew_lease(token)


def _template_parameters(config: ManagerConfig) -> dict[str, Any]:
    return {
        "runtime": config.runtime.model_dump(),
        "workspace": {
            "storage_class": config.workspace_storage_class,
            "storage": config.workspace_storage,
        },
    }


def _session_args(session: SessionKey | SessionHandle) -> tuple[SessionKey, int | None]:
    if isinstance(session, SessionHandle):
        return session.key, session.generation
    return session, None


def _pod_uid(sandbox: Mapping[str, Any]) -> str | None:
    status = sandbox.get("status", {})
    return status.get("podUID") if isinstance(status, Mapping) else None


def _parse_timestamp(value: object):
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return timestamp if timestamp.tzinfo is not None and timestamp.utcoffset() is not None else None


def _session_annotations(record: SessionRecord, active_count: int | None) -> dict[str, str]:
    def timestamp(value):
        return value.isoformat().replace("+00:00", "Z") if value is not None else ""

    values = {
        "generation": str(record.generation),
        "state": record.state.value,
        "command-state-known": str(record.command_state_known).lower(),
        "active-file-operation-count": str(record.active_file_operations),
    }
    values["running-command-count"] = (
        str(active_count) if active_count is not None and record.command_state_known else "unknown"
    )
    for annotation, field in (
        ("last-command-started-at", record.last_command_started_at),
        ("last-command-finished-at", record.last_command_finished_at),
        ("last-commands-checked-at", record.last_commands_checked_at),
        ("last-no-commands-confirmed-at", record.last_no_commands_confirmed_at),
        ("idle-since", record.idle_since),
        ("suspended-at", record.suspended_at),
        ("expires-at", record.expires_at),
    ):
        values[annotation] = timestamp(field)
    values["runtime-instance-id"] = record.runtime_instance_id or ""
    return {f"session-sandbox.example.com/{key}": value for key, value in values.items()}


def _append_workspace_event(
    record: SessionRecord, event_type: EventType, reason: str, now: datetime
):
    event_id = f"{record.key_hash}:{record.generation}:workspace:{event_type.value}"
    if any(event.event_id == event_id for event in record.pending_events):
        return record.pending_events
    context = (
        "The previous workspace was cleared. Files stored there are no longer available; "
        "shared code remains mounted read-only."
        if event_type is EventType.WORKSPACE_CLEARED
        else "The previous workspace was lost. Its files are no longer available; "
        "shared code remains mounted read-only."
    )
    event = SessionEvent(
        event_id=event_id,
        generation=record.generation,
        event_type=event_type,
        occurred_at=now,
        reason=reason,
        context_message=context,
    )
    return (*record.pending_events, event)
