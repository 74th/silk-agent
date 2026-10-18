from __future__ import annotations

from datetime import UTC, datetime

import pytest

from gke_session_sandbox import (
    CommandRecord,
    CommandState,
    ResourceRef,
    SessionKey,
    SessionRecord,
    SessionState,
    SQLiteSessionRepository,
)
from silk_agent.core.ports import ConversationState
from silk_agent.distributed.migration import (
    ManagedSessionExport,
    _export_sandbox_record,
    export_migration_snapshot,
    read_legacy_managed_sessions,
    read_snapshot_sandbox_records,
    read_sqlite_sandbox_records,
    reconcile_legacy_exports,
)
from silk_agent.sandbox.gke import to_session_key_parts
from silk_agent.stores.state import CONTROL_USER_ID, state_to_dict

NOW = datetime(2026, 10, 14, tzinfo=UTC)


@pytest.mark.asyncio
async def test_sqlite_migration_export_is_read_only_and_includes_pod_pvc_and_commands(tmp_path):
    database = tmp_path / "sessions.sqlite3"
    repository = SQLiteSessionRepository(database)
    key = SessionKey(tenant_id="T123", session_id="C456:123.4")
    token = await repository.acquire_lease(key, "migration-fixture")
    record = SessionRecord(
        key_hash=key.stable_hash(),
        generation=2,
        state=SessionState.READY,
        sandbox_ref=ResourceRef(api_version="v1", kind="Pod", name="sandbox-a", uid="pod-uid"),
        workspace_pvc_refs=(
            ResourceRef(
                api_version="v1", kind="PersistentVolumeClaim", name="workspace-a", uid="pvc-uid"
            ),
        ),
        active_commands=(
            CommandRecord(
                command_id="cmd-a",
                generation=2,
                runtime_instance_id="sandbox-a-uid",
                state=CommandState.RUNNING,
                accepted_at=NOW,
            ),
        ),
        creation_env_fingerprint="fingerprint",
    )
    await repository.create(token, record)
    await repository.close()
    bytes_before = database.read_bytes()

    exported = read_sqlite_sandbox_records(database)

    assert database.read_bytes() == bytes_before
    assert len(exported) == 1
    assert exported[0].generation == 2
    assert exported[0].sandbox_pod == {"kind": "Pod", "name": "sandbox-a", "uid": "pod-uid"}
    assert exported[0].workspace_pvcs[0]["uid"] == "pvc-uid"
    assert exported[0].active_command_ids == ("cmd-a",)
    assert exported[0].environment_fingerprint == "fingerprint"


@pytest.mark.asyncio
async def test_agent_platform_migration_export_reads_latest_control_state_only():
    conversation_key = "slack:T123:C456:123.4"
    first = state_to_dict(
        ConversationState(
            conversation_key,
            agent_session_id="sdk-session-1",
            owner_user="U123",
            owner_email="user@example.test",
            running_command_ids=["cmd-b", "cmd-a", "cmd-a"],
        )
    )
    latest = {**first, "status": "idle", "owner_email": "owner@example.test"}

    class FakeSessions:
        async def list_sessions(self, user_id):
            assert user_id == CONTROL_USER_ID
            return ["control-session"]

        async def list_raw_events(self, session_id):
            assert session_id == "control-session"
            return [
                {"kind": "control", "seq": 1, "state": first},
                {"kind": "transcript", "seq": 9},
                {"kind": "control", "seq": 2, "state": latest},
            ]

    exported = await read_legacy_managed_sessions(FakeSessions())
    tenant, session = to_session_key_parts(conversation_key)
    assert len(exported) == 1
    assert exported[0].conversation_key == conversation_key
    assert (
        exported[0].sandbox_key_hash
        == SessionKey(tenant_id=tenant, session_id=session).stable_hash()
    )
    assert exported[0].sdk_session_id == "sdk-session-1"
    assert exported[0].owner_user == "U123"
    assert exported[0].owner_email == "owner@example.test"
    assert exported[0].running_command_ids == ("cmd-a", "cmd-b")


def test_migration_dry_run_reports_generation_uid_and_command_mismatches():
    source = read_fixture_record(generation=2, pod_uid="pod-old", pvc_uid="pvc-old")
    target = read_fixture_record(generation=3, pod_uid="pod-new", pvc_uid="pvc-new")
    tenant, session = to_session_key_parts("slack:T123:C456:123.4")
    managed = (
        ManagedSessionExport(
            conversation_key="slack:T123:C456:123.4",
            sandbox_key_hash=SessionKey(tenant_id=tenant, session_id=session).stable_hash(),
            sdk_session_id="sdk-session-1",
            owner_user="U123",
            owner_email="owner@example.test",
            status="idle",
            running_command_ids=("cmd-a",),
        ),
    )

    report = reconcile_legacy_exports((source,), managed, target_sandbox_records=(target,))
    codes = {finding.code for finding in report.findings}
    assert {
        "sandbox_generation_mismatch",
        "sandbox_pod_uid_mismatch",
        "workspace_pvc_uid_mismatch",
    } <= codes


def test_migration_dry_run_reports_missing_and_duplicate_legacy_records():
    records = read_fixture_record(generation=2, pod_uid="pod-a", pvc_uid="pvc-a")
    tenant, session = to_session_key_parts("slack:T123:C456:123.4")
    key_hash = SessionKey(tenant_id=tenant, session_id=session).stable_hash()
    managed = (
        ManagedSessionExport(
            "slack:T123:C456:123.4", key_hash, "duplicate-sdk", "U1", None, "idle", ()
        ),
        ManagedSessionExport(
            "slack:T123:C456:123.4", key_hash, "duplicate-sdk", "U1", None, "idle", ()
        ),
        ManagedSessionExport(
            "slack:T123:C789:456.7", "missing-hash", "sdk-2", None, None, "idle", ()
        ),
    )

    report = reconcile_legacy_exports((records, records), managed)
    codes = [finding.code for finding in report.findings]
    assert "duplicate_conversation_mapping" in codes
    assert "duplicate_sdk_session_id" in codes
    assert "duplicate_sandbox_key_hash" in codes
    assert "missing_sandbox_record" in codes


@pytest.mark.asyncio
async def test_migration_snapshot_output_is_private_and_can_be_read_for_comparison(tmp_path):
    database = tmp_path / "sessions.sqlite3"
    repository = SQLiteSessionRepository(database)
    await repository.close()

    class EmptyLegacySessions:
        async def list_sessions(self, _user_id):
            return []

        async def list_raw_events(self, _session_id):
            return None

    output = tmp_path / "migration.json"
    snapshot, report = await export_migration_snapshot(
        database, EmptyLegacySessions(), output_path=output
    )
    assert report.clean
    assert snapshot["report"]["clean"] is True
    assert output.stat().st_mode & 0o777 == 0o600
    assert read_snapshot_sandbox_records(output) == ()


def read_fixture_record(*, generation: int, pod_uid: str, pvc_uid: str):
    key = SessionKey(tenant_id="T123", session_id="C456:123.4")
    return _export_sandbox_record(
        SessionRecord(
            key_hash=key.stable_hash(),
            generation=generation,
            state=SessionState.READY,
            sandbox_ref=ResourceRef(api_version="v1", kind="Pod", name="sandbox", uid=pod_uid),
            workspace_pvc_refs=(
                ResourceRef(
                    api_version="v1",
                    kind="PersistentVolumeClaim",
                    name="workspace",
                    uid=pvc_uid,
                ),
            ),
            active_commands=(
                CommandRecord(
                    command_id="cmd-a",
                    generation=generation,
                    runtime_instance_id="runtime",
                    state=CommandState.RUNNING,
                    accepted_at=NOW,
                ),
            ),
        )
    )
