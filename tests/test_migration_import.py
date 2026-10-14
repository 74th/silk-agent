from copy import deepcopy

import pytest

from gke_session_sandbox.firestore_repository import decode_session
from silk_agent.core.ports import CommandResult
from silk_agent.distributed.migration import (
    ManagedSessionExport,
    _export_sandbox_record,
    reconcile_legacy_exports,
    snapshot_to_dict,
)
from silk_agent.distributed.migration_import import (
    import_snapshot,
    plan_import,
    reconcile_commands,
)
from tests.session_sandbox.test_firestore_repository import complete_record


def importable_snapshot():
    record = complete_record().model_copy(update={"operation_phase": "ready"})
    sandbox = (_export_sandbox_record(record),)
    managed = (
        ManagedSessionExport(
            "slack:tenant:session",
            record.key_hash,
            "sdk-1",
            "U1",
            "u@example.test",
            "running",
            ("command-2",),
        ),
    )
    snapshot = snapshot_to_dict(sandbox, managed, reconcile_legacy_exports(sandbox, managed))
    snapshot.update(source_version=1, sandbox_source_records=[record.model_dump(mode="json")])
    return snapshot


class Target:
    def __init__(self, fail_at=None):
        self.documents = {}
        self.fail_at = fail_at

    async def create_once(self, collection, key, value):
        path = (collection, key)
        if len(self.documents) == self.fail_at and path not in self.documents:
            self.fail_at = None
            raise OSError("injected interruption")
        if path in self.documents:
            if self.documents[path] != value:
                raise ValueError("conflict")
            return False
        self.documents[path] = deepcopy(value)
        return True


async def test_import_dry_run_has_zero_writes_and_requires_stopped_legacy():
    target = Target()
    with pytest.raises(ValueError, match="stopped"):
        await import_snapshot(importable_snapshot(), target, legacy_stopped=False)
    report = await import_snapshot(importable_snapshot(), target, legacy_stopped=True)
    assert report["created"] == 0 and report["records"] == 4
    assert target.documents == {}


async def test_import_resumes_partial_failure_without_changing_completed_records():
    target = Target(fail_at=2)
    source = importable_snapshot()
    unchanged = deepcopy(source)
    with pytest.raises(OSError):
        await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
    prior = deepcopy(target.documents)
    result = await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
    assert result["created"] == 3
    assert all(target.documents[key] == value for key, value in prior.items())
    result = await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
    assert result["created"] == 0
    assert source == unchanged
    sandbox = complete_record()
    restored = decode_session(target.documents[("sandboxes", sandbox.key_hash)], sandbox.key_hash)
    assert restored.record.workspace_pvc_refs == sandbox.workspace_pvc_refs
    assert restored.record.sandbox_ref == sandbox.sandbox_ref
    assert restored.record.generation == sandbox.generation
    conversation = next(v for (c, _), v in target.documents.items() if c == "conversations")
    assert conversation["sdk_session_id"] == "sdk-1"
    assert conversation["owner_user"] == "U1"
    assert conversation["next_input_seq"] == 2
    assert conversation["turn_status"] == "interrupted_shutdown"
    assert target.documents[("migrations", "cutover")]["accepting_input"] is False


async def test_different_source_is_rejected_before_overwriting_imported_data():
    target = Target()
    source = importable_snapshot()
    await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
    prior = deepcopy(target.documents)
    source["managed_sessions"][0]["sdk_session_id"] = "changed-sdk"
    with pytest.raises(ValueError, match="conflict"):
        await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
    assert target.documents == prior


@pytest.mark.parametrize(
    "outcome,expected",
    [
        ("running", "running"),
        ("completed", "finished"),
        ("cancelled", "cancelled"),
        ("unknown", "unknown"),
        ("exception", "unknown"),
    ],
)
async def test_command_reconciliation_uses_existing_id_without_start_or_cancel(outcome, expected):
    reads = []

    class Reader:
        async def command_status(self, key, command_id):
            reads.append((key, command_id))
            if outcome == "exception":
                raise ConnectionError("response lost")
            return CommandResult(command_id, outcome, 0 if outcome == "completed" else None)

    source = importable_snapshot()
    observations = await reconcile_commands(source, Reader())
    assert reads == [("slack:tenant:session", "command-2")]
    assert observations[0]["state"] == expected
    source["command_observations"] = list(observations)
    command = next(
        value for collection, _, value in plan_import(source) if collection == "commands"
    )
    assert command["command_id"] == "command-2"
    assert command["sandbox_generation"] == 2
    assert command["state"] == expected
    assert command["delivery_mode"] == "orphaned"
    assert command["notification_state"] == "held"


async def test_import_rejects_incomplete_or_in_flight_snapshot_before_writing():
    source = importable_snapshot()
    source["sandbox_source_records"][0]["active_file_operations"] = 1
    target = Target()
    with pytest.raises(ValueError, match="in-flight"):
        await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
    assert target.documents == {}


@pytest.mark.parametrize(
    "state,phase",
    [("suspended", "suspended"), ("terminated", "terminated"), ("lost", "sandbox-lost")],
)
def test_import_preserves_stable_nonready_sandbox_state(state, phase):
    source = importable_snapshot()
    source["sandbox_source_records"][0].update(state=state, operation_phase=phase)
    record = next(
        value for collection, _, value in plan_import(source) if collection == "sandboxes"
    )
    assert record["state"] == state and record["operation_phase"] == phase
