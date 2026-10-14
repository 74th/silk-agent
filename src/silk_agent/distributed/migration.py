"""Read-only export and dry-run reconciliation for legacy session data."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from gke_session_sandbox import SessionKey
from gke_session_sandbox.types import SessionRecord
from silk_agent.sandbox.gke import to_session_key_parts
from silk_agent.stores.state import CONTROL_USER_ID, state_from_dict


class LegacySessions(Protocol):
    async def list_sessions(self, user_id: str) -> list[str]: ...

    async def list_raw_events(self, session_id: str) -> list[dict[str, Any]] | None: ...


@dataclass(frozen=True)
class SandboxExport:
    key_hash: str
    generation: int
    state: str
    sandbox_pod: dict[str, str] | None
    workspace_pvcs: tuple[dict[str, str], ...]
    active_command_ids: tuple[str, ...]
    runtime_instance_id: str | None
    environment_fingerprint: str | None


@dataclass(frozen=True)
class ManagedSessionExport:
    conversation_key: str
    sandbox_key_hash: str
    sdk_session_id: str | None
    owner_user: str | None
    owner_email: str | None
    status: str
    running_command_ids: tuple[str, ...]


@dataclass(frozen=True)
class MigrationFinding:
    code: str
    conversation_key: str | None
    detail: str


@dataclass(frozen=True)
class MigrationReport:
    sandbox_record_count: int
    managed_session_count: int
    findings: tuple[MigrationFinding, ...]

    @property
    def clean(self) -> bool:
        return not self.findings


def read_sqlite_sandbox_records(database_path: str | Path) -> tuple[SandboxExport, ...]:
    """Read and validate session rows from SQLite without creating or changing the DB."""
    path = Path(database_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT key_hash, record_json FROM sessions ORDER BY key_hash"
        ).fetchall()
        exported: list[SandboxExport] = []
        for key_hash, raw in rows:
            record = SessionRecord.model_validate_json(raw)
            if record.key_hash != key_hash:
                raise ValueError("session key hash does not match its SQLite row")
            exported.append(_export_sandbox_record(record))
        return tuple(exported)
    finally:
        connection.close()


async def read_legacy_managed_sessions(
    sessions: LegacySessions,
) -> tuple[ManagedSessionExport, ...]:
    """Read the latest control event for each legacy Agent Platform conversation."""
    exported: list[ManagedSessionExport] = []
    for control_id in sorted(await sessions.list_sessions(CONTROL_USER_ID)):
        events = await sessions.list_raw_events(control_id)
        controls = [event for event in events or () if event.get("kind") == "control"]
        if not controls:
            continue
        latest = max(controls, key=lambda event: _valid_sequence(event.get("seq")))
        state_data = latest.get("state")
        if not isinstance(state_data, dict):
            raise ValueError(f"legacy control session {control_id} has an invalid state")
        state = state_from_dict(state_data)
        tenant, session = to_session_key_parts(state.conversation_key)
        sandbox_key_hash = SessionKey(tenant_id=tenant, session_id=session).stable_hash()
        exported.append(
            ManagedSessionExport(
                conversation_key=state.conversation_key,
                sandbox_key_hash=sandbox_key_hash,
                sdk_session_id=state.agent_session_id,
                owner_user=state.owner_user,
                owner_email=state.owner_email,
                status=state.status,
                running_command_ids=tuple(sorted(set(state.running_command_ids))),
            )
        )
    return tuple(sorted(exported, key=lambda item: item.conversation_key))


def reconcile_legacy_exports(
    sandbox_records: tuple[SandboxExport, ...],
    managed_sessions: tuple[ManagedSessionExport, ...],
    *,
    target_sandbox_records: tuple[SandboxExport, ...] = (),
) -> MigrationReport:
    """Report source inconsistencies and target drift without writing data."""
    findings: list[MigrationFinding] = []
    sandbox_by_key = {record.key_hash: record for record in sandbox_records}
    target_by_key = {record.key_hash: record for record in target_sandbox_records}
    managed_by_key_hash: dict[str, list[ManagedSessionExport]] = defaultdict(list)
    sdk_ids = [item.sdk_session_id for item in managed_sessions if item.sdk_session_id]
    for item in managed_sessions:
        managed_by_key_hash[item.sandbox_key_hash].append(item)

    for key_hash, owners in sorted(managed_by_key_hash.items()):
        if len(owners) > 1:
            findings.append(
                MigrationFinding(
                    "duplicate_conversation_mapping",
                    ",".join(sorted(item.conversation_key for item in owners)),
                    f"{len(owners)} managed conversations map to sandbox key hash {key_hash}",
                )
            )
        for item in owners:
            record = sandbox_by_key.get(key_hash)
            if record is None and (item.sdk_session_id or item.running_command_ids):
                findings.append(
                    MigrationFinding(
                        "missing_sandbox_record",
                        item.conversation_key,
                        "managed session references an SDK session or command "
                        "without a sandbox record",
                    )
                )
            if record is not None:
                sandbox_commands = set(record.active_command_ids)
                managed_commands = set(item.running_command_ids)
                if sandbox_commands != managed_commands:
                    findings.append(
                        MigrationFinding(
                            "running_command_mismatch",
                            item.conversation_key,
                            "legacy management and Sandbox list different running command IDs",
                        )
                    )

    for key_hash, count in Counter(sdk_ids).items():
        if count > 1:
            conversations = sorted(
                item.conversation_key
                for item in managed_sessions
                if item.sdk_session_id == key_hash
            )
            findings.append(
                MigrationFinding(
                    "duplicate_sdk_session_id",
                    ",".join(conversations),
                    f"SDK session ID {key_hash} is referenced by {count} conversations",
                )
            )

    for record in sandbox_records:
        if record.key_hash not in managed_by_key_hash:
            findings.append(
                MigrationFinding(
                    "orphaned_sandbox_record",
                    None,
                    "sandbox key hash "
                    f"{record.key_hash} generation {record.generation} has no managed session",
                )
            )

    for key_hash, count in Counter(record.key_hash for record in sandbox_records).items():
        if count > 1:
            findings.append(
                MigrationFinding(
                    "duplicate_sandbox_key_hash",
                    None,
                    f"source contains {count} sandbox records for key hash {key_hash}",
                )
            )

    for key_hash in sorted(sandbox_by_key.keys() & target_by_key.keys()):
        source = sandbox_by_key[key_hash]
        target = target_by_key[key_hash]
        if source.generation != target.generation:
            findings.append(
                MigrationFinding(
                    "sandbox_generation_mismatch",
                    None,
                    f"key hash {key_hash} source generation={source.generation}, "
                    f"target generation={target.generation}",
                )
            )
        if _uid(source.sandbox_pod) != _uid(target.sandbox_pod):
            findings.append(
                MigrationFinding(
                    "sandbox_pod_uid_mismatch",
                    None,
                    f"key hash {key_hash} has different source and target Sandbox Pod UIDs",
                )
            )
        if _uids(source.workspace_pvcs) != _uids(target.workspace_pvcs):
            findings.append(
                MigrationFinding(
                    "workspace_pvc_uid_mismatch",
                    None,
                    f"key hash {key_hash} has different source and target PVC UIDs",
                )
            )

    return MigrationReport(len(sandbox_records), len(managed_sessions), tuple(findings))


def snapshot_to_dict(
    sandbox_records: tuple[SandboxExport, ...],
    managed_sessions: tuple[ManagedSessionExport, ...],
    report: MigrationReport,
) -> dict[str, Any]:
    """Build a JSON-safe export. It contains identifiers, not transcripts or secrets."""
    return {
        "schema_version": 1,
        "sandbox_records": [asdict(record) for record in sandbox_records],
        "managed_sessions": [asdict(record) for record in managed_sessions],
        "report": {
            "sandbox_record_count": report.sandbox_record_count,
            "managed_session_count": report.managed_session_count,
            "clean": report.clean,
            "findings": [asdict(finding) for finding in report.findings],
        },
    }


def read_snapshot_sandbox_records(path: str | Path) -> tuple[SandboxExport, ...]:
    """Read the sandbox section of a previous JSON export for target comparison."""
    data = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise ValueError("unsupported migration snapshot schema")
    records = data.get("sandbox_records")
    if not isinstance(records, list):
        raise ValueError("migration snapshot has no sandbox_records list")
    exported = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("migration snapshot contains an invalid sandbox record")
        value = dict(record)
        value["workspace_pvcs"] = tuple(value.get("workspace_pvcs") or ())
        value["active_command_ids"] = tuple(value.get("active_command_ids") or ())
        exported.append(SandboxExport(**value))
    return tuple(exported)


async def export_migration_snapshot(
    database_path: str | Path,
    sessions: LegacySessions,
    *,
    target_sandbox_records: tuple[SandboxExport, ...] = (),
    output_path: str | Path | None = None,
) -> tuple[dict[str, Any], MigrationReport]:
    """Collect a read-only migration snapshot and optionally create a private JSON file."""
    source_records = await asyncio.to_thread(read_sqlite_source_records, database_path)
    sandbox_records = tuple(_export_sandbox_record(record) for record in source_records)
    managed_sessions = await read_legacy_managed_sessions(sessions)
    report = reconcile_legacy_exports(
        sandbox_records,
        managed_sessions,
        target_sandbox_records=target_sandbox_records,
    )
    snapshot = snapshot_to_dict(sandbox_records, managed_sessions, report)
    snapshot["source_version"] = 1
    snapshot["sandbox_source_records"] = [
        record.model_dump(mode="json") for record in source_records
    ]
    if output_path is not None:
        await asyncio.to_thread(_write_snapshot, output_path, snapshot)
    return snapshot, report


def _export_sandbox_record(record: SessionRecord) -> SandboxExport:
    return SandboxExport(
        key_hash=record.key_hash,
        generation=record.generation,
        state=record.state.value,
        sandbox_pod=_resource(record.sandbox_ref),
        workspace_pvcs=tuple(
            resource
            for ref in record.workspace_pvc_refs
            if (resource := _resource(ref)) is not None
        ),
        active_command_ids=tuple(sorted(command.command_id for command in record.active_commands)),
        runtime_instance_id=record.runtime_instance_id,
        environment_fingerprint=record.creation_env_fingerprint,
    )


def _resource(ref: Any) -> dict[str, str] | None:
    if ref is None:
        return None
    result = {"kind": ref.kind, "name": ref.name, "uid": ref.uid}
    if ref.namespace:
        result["namespace"] = ref.namespace
    return result


def _valid_sequence(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return -1


def _uid(resource: dict[str, str] | None) -> str | None:
    return resource.get("uid") if resource is not None else None


def _uids(resources: tuple[dict[str, str], ...]) -> tuple[str, ...]:
    return tuple(sorted(resource["uid"] for resource in resources))


def _write_snapshot(output_path: str | Path, snapshot: dict[str, Any]) -> None:
    path = Path(output_path).expanduser()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(snapshot, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def read_sqlite_source_records(database_path: str | Path) -> tuple[SessionRecord, ...]:
    """Read the full records needed to preserve lifecycle and resource UID metadata."""
    path = Path(database_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    try:
        records = []
        for key_hash, raw in connection.execute(
            "SELECT key_hash, record_json FROM sessions ORDER BY key_hash"
        ):
            record = SessionRecord.model_validate_json(raw)
            if record.key_hash != key_hash:
                raise ValueError("session key hash does not match its SQLite row")
            records.append(record)
        return tuple(records)
    finally:
        connection.close()
