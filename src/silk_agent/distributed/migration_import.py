"""Resumable, create-only import of a stopped legacy installation.

No SDK or Sandbox start operations are available here. The caller keeps both old
and new receivers stopped until import and reconciliation finish.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional

from gke_session_sandbox.firestore_repository import encode_session
from gke_session_sandbox.types import SessionKey, SessionRecord
from silk_agent.core.ports import CommandResult
from silk_agent.distributed.migration import (
    ManagedSessionExport,
    _export_sandbox_record,
    reconcile_legacy_exports,
)
from silk_agent.sandbox.gke import to_session_key_parts
from silk_agent.stores.firestore.ids import conversation_document_id, document_id


class ImportTarget(Protocol):
    async def create_once(self, collection: str, key: str, value: Mapping[str, Any]) -> bool: ...


class CommandReader(Protocol):
    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult: ...


def source_hash(snapshot: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def plan_import(snapshot: Mapping[str, Any]) -> tuple[tuple[str, str, dict[str, Any]], ...]:
    """Validate the entire source before the first write; preserve resource identities."""
    if snapshot.get("schema_version") != 1 or snapshot.get("source_version") != 1:
        raise ValueError("snapshot lacks importable source records; export it again")
    if not snapshot.get("report", {}).get("clean"):
        raise ValueError("resolve migration findings before importing")
    raw = snapshot.get("sandbox_source_records")
    managed = snapshot.get("managed_sessions")
    if not isinstance(raw, list) or not isinstance(managed, list):
        raise ValueError("snapshot has invalid source records")
    records = [SessionRecord.model_validate(value) for value in raw]
    source_sessions = tuple(
        ManagedSessionExport(
            conversation_key=item["conversation_key"],
            sandbox_key_hash=item["sandbox_key_hash"],
            sdk_session_id=item["sdk_session_id"],
            owner_user=item["owner_user"],
            owner_email=item["owner_email"],
            status=item["status"],
            running_command_ids=tuple(item["running_command_ids"]),
        )
        for item in managed
    )
    checked = reconcile_legacy_exports(
        tuple(_export_sandbox_record(record) for record in records), source_sessions
    )
    if not checked.clean:
        raise ValueError("full source records fail migration reconciliation")
    by_hash = {record.key_hash: record for record in records}
    if len(by_hash) != len(records):
        raise ValueError("duplicate Sandbox source record")
    # A resource in an in-flight mutation cannot safely be imported as ready.
    if any(
        record.active_file_operations
        or record.state.value not in {"ready", "suspended", "terminated", "lost"}
        or record.operation_phase
        not in {
            None,
            "ready",
            "suspended",
            "terminated",
            "expired-cleanup-complete",
            "sandbox-lost",
        }
        for record in records
    ):
        raise ValueError("stop in-flight Sandbox operations before exporting")
    observations = snapshot.get("command_observations", [])
    observed = {}
    for item in observations:
        if item["command_id"] in observed or item["state"] not in {
            "running",
            "finished",
            "cancelled",
            "unknown",
        }:
            raise ValueError("invalid or duplicate command observation")
        observed[item["command_id"]] = item
    digest = source_hash(snapshot)
    provenance = {"migration_source_hash": digest, "migration_source_version": 1}
    result: list[tuple[str, str, dict[str, Any]]] = []
    for record in records:
        result.append(("sandboxes", record.key_hash, encode_session(record, 1)))
    seen = set()
    for item in managed:
        key = item["conversation_key"]
        if not isinstance(key, str) or not key or key in seen:
            raise ValueError("invalid or duplicate conversation key")
        seen.add(key)
        tenant, session = to_session_key_parts(key)
        expected_hash = SessionKey(tenant_id=tenant, session_id=session).stable_hash()
        if item["sandbox_key_hash"] != expected_hash:
            raise ValueError("conversation key does not match Sandbox source identity")
        if item["status"] not in {"running", "idle", "interrupted_crash", "interrupted_shutdown"}:
            raise ValueError("invalid legacy turn status")
        sandbox = by_hash.get(item["sandbox_key_hash"])
        if sandbox is None and (item["sdk_session_id"] or item["running_command_ids"]):
            raise ValueError("managed conversation has no full Sandbox record")
        expected_commands = set(item["running_command_ids"])
        if sandbox is not None and expected_commands != {
            command.command_id for command in sandbox.active_commands
        }:
            raise ValueError("managed and Sandbox command IDs do not match")
        conversation = {
            "schema_version": 1,
            "conversation_key": key,
            "conversation_generation": 1,
            "sdk_session_id": item["sdk_session_id"],
            "owner_user": item["owner_user"],
            "owner_email": item["owner_email"],
            # Old SDK stacks cannot be migrated. Only explicit user input resumes them.
            "turn_status": "interrupted_shutdown"
            if item["status"] == "running"
            else item["status"],
            "turn_id": None,
            "participating": True,
            "allocation_state": "idle",
            "next_input_seq": 2 if expected_commands else 1,
            "stop_seq": 0,
            "version": 1,
            "guard_history": [],
            "history_uncertain": item["status"] == "running",
            **provenance,
        }
        result.append(("conversations", conversation_document_id(key), conversation))
        if sandbox is None:
            continue
        for command in sandbox.active_commands:
            # Imported commands remain queryable through the Service using their
            # original ID. They have no replayable start payload.
            result.append(
                (
                    "sandbox_operations",
                    document_id(command.command_id, prefix="operation"),
                    {
                        "schema_version": 1,
                        "operation_id": command.command_id,
                        "operation_kind": "command_start",
                        "request_hash": hashlib.sha256(
                            f"legacy:{sandbox.key_hash}:{command.command_id}".encode()
                        ).hexdigest(),
                        "conversation_key": key,
                        "conversation_generation": 1,
                        "sandbox_generation": sandbox.generation,
                        "sandbox_key_hash": sandbox.key_hash,
                        "turn_id": "legacy-migration",
                        "origin_seq": 1,
                        "origin_epoch": 0,
                        "worker_incarnation": "legacy-stopped",
                        "payload": {},
                        "state": "dispatched",
                        **provenance,
                    },
                )
            )
            # Until the tracker has been read by ID, do not infer running or restart it.
            result.append(
                (
                    "commands",
                    document_id(command.command_id, prefix="command"),
                    {
                        "schema_version": 1,
                        "command_id": command.command_id,
                        "conversation_key": key,
                        "conversation_generation": 1,
                        "sandbox_generation": sandbox.generation,
                        "sandbox_key_hash": sandbox.key_hash,
                        "origin_turn": "legacy-migration",
                        "origin_epoch": 0,
                        "origin_seq": 1,
                        "state": observed.get(command.command_id, {}).get("state", "unknown"),
                        "result": {
                            "exit_code": observed.get(command.command_id, {}).get("exit_code")
                        },
                        "delivery_mode": "orphaned",
                        "notification_state": "held",
                        "cancel_requested": False,
                        "next_attempt_at": datetime(1970, 1, 1, tzinfo=UTC),
                        **provenance,
                    },
                )
            )
    paths = [(collection, key) for collection, key, _ in result]
    if len(set(paths)) != len(paths):
        raise ValueError("duplicate command ID across Sandbox records")
    if set(observed) - {
        command.command_id for record in records for command in record.active_commands
    }:
        raise ValueError("observation refers to a command outside the source snapshot")
    mapped = {item["sandbox_key_hash"] for item in managed}
    if set(by_hash) - mapped:
        raise ValueError("orphaned Sandbox source records")
    return tuple(result)


async def import_snapshot(
    snapshot: Mapping[str, Any], target: ImportTarget, *, legacy_stopped: bool, dry_run: bool = True
) -> dict[str, Any]:
    if not legacy_stopped:
        raise ValueError("legacy receivers, SDKs and reconciler must be stopped")
    plan = plan_import(snapshot)
    digest = source_hash(snapshot)
    if dry_run:
        return {"source_hash": digest, "records": len(plan), "created": 0, "dry_run": True}
    # This marker commits before any target data; accepting_input remains false until
    # the operator finishes reconciliation and deliberately switches reception on.
    await target.create_once(
        "migrations",
        "cutover",
        {
            "schema_version": 1,
            "source_hash": digest,
            "source_version": 1,
            "phase": "importing",
            "accepting_input": False,
        },
    )
    created = 0
    for collection, key, value in plan:
        created += await target.create_once(collection, key, value)
    await target.create_once(
        "migrations",
        "import-complete",
        {
            "schema_version": 1,
            "source_hash": digest,
            "source_version": 1,
            "phase": "imported",
            "accepting_input": False,
            "records": len(plan),
        },
    )
    return {"source_hash": digest, "records": len(plan), "created": created, "dry_run": False}


async def reconcile_commands(
    snapshot: Mapping[str, Any], reader: CommandReader
) -> tuple[dict, ...]:
    """Read existing tracker IDs only; ambiguous/lost replies remain unknown."""
    plan = plan_import(snapshot)
    outcomes = []
    for collection, _key, record in plan:
        if collection != "commands":
            continue
        try:
            status = await reader.command_status(record["conversation_key"], record["command_id"])
            state = {"running": "running", "completed": "finished", "cancelled": "cancelled"}.get(
                status.outcome, "unknown"
            )
            exit_code = status.exit_code
        except Exception:
            state, exit_code = "unknown", None
        outcomes.append(
            {
                "command_id": record["command_id"],
                "state": state,
                "exit_code": exit_code,
                "delivery_mode": "orphaned",
            }
        )
    return tuple(outcomes)


class FirestoreImportTarget:
    def __init__(self, client: firestore.AsyncClient, *, collection_prefix: str):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("invalid collection prefix")
        self.client = client
        self.prefix = collection_prefix

    async def create_once(self, collection: str, key: str, value: Mapping[str, Any]) -> bool:
        ref = self.client.collection(f"{self.prefix}_{collection}").document(key)

        @async_transactional
        async def create(transaction):
            snapshot = await ref.get(transaction=transaction)
            if snapshot.exists:
                if snapshot.to_dict() != dict(value):
                    raise ValueError("migration target conflicts with source; refusing overwrite")
                return False
            transaction.create(ref, dict(value))
            return True

        return await create(self.client.transaction())
