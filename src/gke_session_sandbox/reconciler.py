"""In-process session lifecycle reconciliation."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import Mapping
from datetime import datetime, timedelta
import uuid
from typing import TYPE_CHECKING

import aiohttp

from .errors import ResourceOwnershipError
from .types import CommandState, DesiredSandboxState, EventType, SessionEvent, SessionState, utc_now

if TYPE_CHECKING:
    from .manager import SandboxManager


class LifecycleReconciler:
    """Reconcile persisted sessions with their sandboxd CommandTracker."""

    def __init__(self, manager: "SandboxManager", *, clock=utc_now) -> None:
        self.manager = manager
        self.clock = clock
        self._stop = asyncio.Event()

    @asynccontextmanager
    async def lifespan(self):
        """Run the reconciler as a task owned by an API server's lifespan."""
        task = asyncio.create_task(self.run_forever(), name="gke-session-sandbox-reconciler")
        try:
            yield self
        finally:
            self.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        interval = self.manager.config.lifecycle.reconcile_interval_seconds
        recovered = False
        while not self._stop.is_set():
            try:
                if not recovered:
                    await self.recover_process_state()
                    recovered = True
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Repository/API failures are retried on the next interval.
                pass
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def reconcile_once(self) -> tuple[str, ...]:
        errors: list[str] = []
        for item in await self.manager.repository.list_records():
            if item.record.state not in {
                SessionState.READY, SessionState.SUSPENDED, SessionState.SUSPENDING,
                SessionState.RESUMING, SessionState.TERMINATING,
            }:
                continue
            try:
                async with self.manager._session_lease_by_hash(item.record.key_hash) as token:
                    await self._reconcile_session(token, item.record.key_hash)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(f"{item.record.key_hash}: {type(exc).__name__}: {exc}")
        return tuple(errors)

    async def _reconcile_session(self, token, key_hash: str) -> None:
        stored = await self.manager.repository.get_by_hash(key_hash)
        if stored is None:
            return
        if stored.record.state is SessionState.TERMINATING:
            await self._finish_termination(token, stored)
            return
        if stored.record.state is SessionState.SUSPENDING:
            await self._finish_suspension(token, stored)
            return
        if stored.record.state is SessionState.RESUMING:
            await self._finish_resume(token, stored)
            return
        if stored.record.state is SessionState.SUSPENDED:
            await self._expire_suspended(token, stored)
            return
        if stored.record.state is not SessionState.READY:
            return
        record = stored.record
        if record.sandbox_ref is None:
            return
        try:
            await self.manager.backend.get_owned(record.sandbox_ref)
        except ResourceOwnershipError as exc:
            if _resource_lost(exc):
                await self._mark_lost(token, stored)
                return
            raise
        if self.manager._pvc_manager is not None:
            try:
                for pvc_ref in record.workspace_pvc_refs:
                    await self.manager._pvc_manager.verify_owned(pvc_ref, sandbox_uid=record.sandbox_ref.uid)
            except ResourceOwnershipError as exc:
                if _resource_lost(exc):
                    await self._mark_lost(token, stored)
                    return
                raise
        endpoint = await self.manager.backend.runtime_endpoint(record.sandbox_ref)
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as http:
                for command in record.active_commands:
                    async with http.get(f"{endpoint}/v1/commands/{command.command_id}") as response:
                        if response.status == 200:
                            stored = await self.manager._save_command_observation(
                                token, stored, command.command_id, await response.json()
                            )
                        elif response.status != 404:
                            raise RuntimeError(f"tracker command query returned HTTP {response.status}")
                async with http.put(f"{endpoint}/v1/commands/gate", json={"accepting": True}) as response:
                    if response.status != 200:
                        raise RuntimeError(f"tracker gate query returned HTTP {response.status}")
                    gate = await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await self._record_uncertain(token, key_hash)
            return

        active_count = gate.get("active_commands")
        if not isinstance(active_count, int) or isinstance(active_count, bool) or active_count < 0:
            await self._record_uncertain(token, key_hash)
            return
        stored = await self.manager.repository.get_by_hash(key_hash)
        if stored is None:
            return
        record = stored.record
        now = _aware(self.clock())
        durable_active = any(command.state is not CommandState.FINISHED for command in record.active_commands)
        no_commands = active_count == 0 and not durable_active
        changes: dict[str, object] = {"last_commands_checked_at": now}
        if no_commands:
            changes["last_no_commands_confirmed_at"] = now
            if record.active_file_operations == 0 and record.idle_since is None:
                changes["idle_since"] = now if not record.command_state_known else (_idle_origin(record) or now)
        else:
            changes["idle_since"] = None
        if active_count > 0 and not durable_active:
            changes["command_state_known"] = False
        elif active_count == 0 and not durable_active:
            changes["command_state_known"] = True
        updated = record.model_copy(update=changes)
        if updated != record:
            stored = await self.manager.repository.compare_and_swap(
                token, updated, expected_resource_version=stored.resource_version
            )
            record = stored.record
        await self.manager.backend.patch_annotations(record.sandbox_ref, _session_annotations(record, active_count))
        if not no_commands or record.active_file_operations > 0 or record.idle_since is None:
            return
        if now < record.idle_since + timedelta(seconds=self.manager.config.lifecycle.idle_timeout_seconds):
            return
        await self._suspend_if_idle(token, key_hash, endpoint, now)

    async def _suspend_if_idle(self, token, key_hash: str, endpoint: str, now: datetime) -> None:
        gate_closed = False
        intent_saved = False
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as http:
                async with http.put(f"{endpoint}/v1/commands/gate", json={"accepting": False}) as response:
                    if response.status != 200:
                        raise RuntimeError(f"tracker gate close returned HTTP {response.status}")
                    gate = await response.json()
                    gate_closed = True
            stored = await self.manager.repository.get_by_hash(key_hash)
            if stored is None or stored.record.state is not SessionState.READY:
                return
            record = stored.record
            if (gate.get("active_commands") != 0 or record.active_commands
                    or record.active_file_operations or not record.command_state_known):
                await self._set_gate(endpoint, True)
                gate_closed = False
                changed = record.model_copy(update={
                    "last_commands_checked_at": now, "idle_since": None,
                    "command_state_known": record.command_state_known and gate.get("active_commands") == 0,
                })
                await self.manager.repository.compare_and_swap(
                    token, changed, expected_resource_version=stored.resource_version
                )
                return
            intent = record.model_copy(update={
                "state": SessionState.SUSPENDING,
                "desired_state": DesiredSandboxState.SUSPENDED,
                "operation_id": record.operation_id or uuid.uuid4().hex,
                "operation_phase": "idle-suspend-intent",
            })
            stored = await self.manager.repository.compare_and_swap(
                token, intent, expected_resource_version=stored.resource_version
            )
            intent_saved = True
            await self.manager.backend.suspend(record.sandbox_ref)
            await self.manager.backend.wait_suspended(
                record.sandbox_ref,
                timeout_seconds=self.manager.config.lifecycle.reconcile_interval_seconds * 2,
            )
            suspended_at = _aware(self.clock())
            expires_at = suspended_at + timedelta(seconds=self.manager.config.lifecycle.retention_seconds)
            await self.manager.backend.set_expiration(record.sandbox_ref, expires_at)
            completed = stored.record.model_copy(update={
                "state": SessionState.SUSPENDED,
                "suspended_at": suspended_at,
                "expires_at": expires_at,
                "operation_phase": "suspended",
            })
            completed = await self.manager.repository.compare_and_swap(
                token, completed, expected_resource_version=stored.resource_version
            )
            await self.manager.backend.patch_annotations(record.sandbox_ref, _session_annotations(completed.record, 0))
            gate_closed = False
        except Exception:
            if gate_closed and not intent_saved:
                try:
                    await self._set_gate(endpoint, True)
                except Exception:
                    pass
            raise

    async def _record_uncertain(self, token, key_hash: str) -> None:
        stored = await self.manager.repository.get_by_hash(key_hash)
        if stored is None or stored.record.state is not SessionState.READY:
            return
        record = stored.record.model_copy(update={
            "last_commands_checked_at": _aware(self.clock()),
            "command_state_known": False,
            "idle_since": None,
        })
        await self.manager.repository.compare_and_swap(
            token, record, expected_resource_version=stored.resource_version
        )

    async def _expire_suspended(self, token, stored) -> None:
        record = stored.record
        now = _aware(self.clock())
        if record.expires_at is None or now < record.expires_at:
            return
        if record.sandbox_ref is None:
            return
        intent = record.model_copy(update={
            "state": SessionState.TERMINATING,
            "desired_state": None,
            "operation_id": record.operation_id or uuid.uuid4().hex,
            "operation_phase": "retention-expiry-intent",
        })
        stored = await self.manager.repository.compare_and_swap(
            token, intent, expected_resource_version=stored.resource_version
        )
        await self._finish_termination(token, stored)

    async def _finish_termination(self, token, stored) -> None:
        record = stored.record
        if record.sandbox_ref is None:
            await self._save_terminated(token, stored)
            return
        try:
            snapshot = await self.manager.backend.get_owned(record.sandbox_ref)
        except ResourceOwnershipError as exc:
            if not _resource_lost(exc):
                raise
        else:
            deletion_timestamp = snapshot.get("metadata", {}).get("deletionTimestamp")
            if not deletion_timestamp:
                await self.manager.backend.delete(record.sandbox_ref)
        await self.manager.backend.wait_deleted(
            record.sandbox_ref,
            timeout_seconds=self.manager.config.lifecycle.reconcile_interval_seconds * 2,
        )
        await self._cleanup_pvcs(record)
        current = await self.manager.repository.get_by_hash(record.key_hash)
        if current is None:
            return
        await self._save_terminated(token, current)

    async def _save_terminated(self, token, stored) -> None:
        record = stored.record
        completed = record.model_copy(update={
            "state": SessionState.TERMINATED,
            "operation_phase": "expired-cleanup-complete",
            "active_commands": (),
            "active_file_operations": 0,
            "pending_events": _append_workspace_event(
                record, EventType.WORKSPACE_CLEARED,
                "retention-expired" if "retention-expiry" in (record.operation_phase or "") else "terminated",
                _aware(self.clock()),
            ),
        })
        completed = await self.manager.repository.compare_and_swap(
            token, completed, expected_resource_version=stored.resource_version
        )
        if completed.record.sandbox_ref is not None:
            try:
                await self.manager.backend.patch_annotations(
                    completed.record.sandbox_ref, _session_annotations(completed.record, 0)
                )
            except ResourceOwnershipError as exc:
                if not _resource_lost(exc):
                    raise

    async def _cleanup_pvcs(self, record) -> None:
        if self.manager._pvc_manager is None or record.sandbox_ref is None:
            return
        refs = record.workspace_pvc_refs or await self.manager._pvc_manager.list_owned(record.sandbox_ref)
        for pvc_ref in refs:
            await self.manager._pvc_manager.delete_owned(pvc_ref, sandbox_uid=record.sandbox_ref.uid)

    async def _finish_suspension(self, token, stored) -> None:
        record = stored.record
        if record.sandbox_ref is None:
            await self._mark_lost(token, stored)
            return
        try:
            snapshot = await self.manager.backend.get_owned(record.sandbox_ref)
        except ResourceOwnershipError as exc:
            if not _resource_lost(exc):
                raise
            await self._mark_lost(token, stored)
            return
        if snapshot.get("metadata", {}).get("deletionTimestamp"):
            await self._mark_lost(token, stored)
            return
        await self.manager.backend.suspend(record.sandbox_ref)
        await self.manager.backend.wait_suspended(
            record.sandbox_ref,
            timeout_seconds=self.manager.config.lifecycle.reconcile_interval_seconds * 2,
        )
        now = _aware(self.clock())
        expires_at = record.expires_at or now + timedelta(seconds=self.manager.config.lifecycle.retention_seconds)
        await self.manager.backend.set_expiration(record.sandbox_ref, expires_at)
        completed = record.model_copy(update={
            "state": SessionState.SUSPENDED,
            "suspended_at": record.suspended_at or now,
            "expires_at": expires_at,
            "operation_phase": "suspended",
        })
        saved = await self.manager.repository.compare_and_swap(
            token, completed, expected_resource_version=stored.resource_version
        )
        await self.manager.backend.patch_annotations(
            record.sandbox_ref, _session_annotations(saved.record, 0)
        )

    async def _finish_resume(self, token, stored) -> None:
        record = stored.record
        if record.sandbox_ref is None:
            await self._mark_lost(token, stored)
            return
        try:
            snapshot = await self.manager.backend.get_owned(record.sandbox_ref)
        except ResourceOwnershipError as exc:
            if not _resource_lost(exc):
                raise
            await self._mark_lost(token, stored)
            return
        if snapshot.get("metadata", {}).get("deletionTimestamp"):
            await self._mark_lost(token, stored)
            return
        await self.manager.backend.resume(record.sandbox_ref)
        ready = await self.manager.backend.wait_ready(record.sandbox_ref)
        await self.manager._open_tracker_gate(record.sandbox_ref)
        ready_record = record.model_copy(update={
            "state": SessionState.READY,
            "ready_at": _aware(self.clock()),
            "runtime_instance_id": _pod_uid(ready) or record.runtime_instance_id,
            "desired_state": DesiredSandboxState.RUNNING,
            "expires_at": None,
            "active_file_operations": 0,
            "operation_phase": "ready",
        })
        saved = await self.manager.repository.compare_and_swap(
            token, ready_record, expected_resource_version=stored.resource_version
        )
        await self.manager.backend.patch_annotations(
            record.sandbox_ref, _session_annotations(saved.record, 0)
        )

    async def _mark_lost(self, token, stored) -> None:
        previous = stored.record
        record = previous.model_copy(update={
            "state": SessionState.LOST,
            "desired_state": None,
            "operation_phase": "sandbox-lost",
            "active_commands": (),
            "active_file_operations": 0,
            "command_state_known": False,
            "pending_events": _append_workspace_event(
                previous, EventType.WORKSPACE_LOST, "sandbox-or-workspace-pvc-lost", _aware(self.clock())
            ),
        })
        await self.manager.repository.compare_and_swap(
            token, record, expected_resource_version=stored.resource_version
        )

    async def recover_process_state(self) -> None:
        """Clear in-flight filesystem counters left by the previous API process."""
        for item in await self.manager.repository.list_records():
            if item.record.active_file_operations == 0:
                continue
            async with self.manager._session_lease_by_hash(item.record.key_hash) as token:
                stored = await self.manager.repository.get_by_hash(item.record.key_hash)
                if stored is None or stored.record.active_file_operations == 0:
                    continue
                now = _aware(self.clock())
                record = stored.record.model_copy(update={
                    "active_file_operations": 0,
                    "last_file_operation_at": now,
                    "idle_since": now if stored.record.state is SessionState.READY else stored.record.idle_since,
                })
                await self.manager.repository.compare_and_swap(
                    token, record, expected_resource_version=stored.resource_version
                )

    @staticmethod
    async def _set_gate(endpoint: str, accepting: bool) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as http:
            async with http.put(f"{endpoint}/v1/commands/gate", json={"accepting": accepting}) as response:
                if response.status != 200:
                    raise RuntimeError(f"tracker gate update returned HTTP {response.status}")
                await response.read()


def _idle_origin(record) -> datetime | None:
    timestamps = [record.ready_at, record.last_command_finished_at, record.last_file_operation_at]
    present = [stamp for stamp in timestamps if stamp is not None]
    return max(present) if present else None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("lifecycle clock must return an aware datetime")
    return value


def _pod_uid(sandbox) -> str | None:
    status = sandbox.get("status", {}) if isinstance(sandbox, Mapping) else {}
    value = status.get("podUID") if isinstance(status, Mapping) else None
    return value if isinstance(value, str) and value else None


def _resource_lost(error: ResourceOwnershipError) -> bool:
    return any(term in str(error) for term in ("no longer exists", "UID changed", "not owned"))


def _append_workspace_event(record, event_type: EventType, reason: str, now: datetime):
    event_id = f"{record.key_hash}:{record.generation}:workspace:{event_type.value}"
    if any(event.event_id == event_id for event in record.pending_events):
        return record.pending_events
    description = "cleared" if event_type is EventType.WORKSPACE_CLEARED else "lost"
    event = SessionEvent(
        event_id=event_id,
        generation=record.generation,
        event_type=event_type,
        occurred_at=now,
        reason=reason,
        context_message=(
            f"The previous workspace was {description}. Files stored there are no longer available; "
            "shared code remains mounted read-only."
        ),
    )
    return (*record.pending_events, event)


def _session_annotations(record, active_count: int | None) -> dict[str, str]:
    def timestamp(value):
        return value.isoformat().replace("+00:00", "Z") if value is not None else ""

    values = {
        "generation": str(record.generation),
        "state": record.state.value,
        "command-state-known": str(record.command_state_known).lower(),
        "active-file-operation-count": str(record.active_file_operations),
        "running-command-count": (
            str(active_count) if active_count is not None and record.command_state_known else "unknown"
        ),
    }
    for annotation, value in (
        ("last-command-started-at", record.last_command_started_at),
        ("last-command-finished-at", record.last_command_finished_at),
        ("last-commands-checked-at", record.last_commands_checked_at),
        ("last-no-commands-confirmed-at", record.last_no_commands_confirmed_at),
        ("idle-since", record.idle_since),
        ("suspended-at", record.suspended_at),
        ("expires-at", record.expires_at),
    ):
        values[annotation] = timestamp(value)
    values["runtime-instance-id"] = record.runtime_instance_id or ""
    return {f"session-sandbox.example.com/{key}": value for key, value in values.items()}
