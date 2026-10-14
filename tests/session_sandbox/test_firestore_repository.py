from datetime import UTC, datetime

import pytest

from gke_session_sandbox import (
    CommandRecord,
    CommandState,
    EventType,
    RepositoryConflictError,
    ResourceRef,
    SessionEvent,
    SessionKey,
    SessionRecord,
    SessionState,
)
from gke_session_sandbox.firestore_repository import decode_session, encode_session


def complete_record() -> SessionRecord:
    now = datetime(2026, 10, 14, tzinfo=UTC)
    key = SessionKey(tenant_id="tenant", session_id="session")
    return SessionRecord(
        key_hash=key.stable_hash(),
        generation=2,
        state=SessionState.READY,
        sandbox_ref=ResourceRef(
            api_version="v1", kind="Sandbox", namespace="sandbox", name="sandbox", uid="pod-uid"
        ),
        workspace_pvc_refs=(
            ResourceRef(
                api_version="v1",
                kind="PersistentVolumeClaim",
                namespace="sandbox",
                name="workspace",
                uid="pvc-uid",
            ),
        ),
        ready_at=now,
        idle_since=now,
        retry_after=now,
        creation_env_fingerprint="fingerprint",
        runtime_instance_id="runtime-2",
        operation_id="operation-2",
        operation_phase="ready",
        active_commands=(
            CommandRecord(
                command_id="command-2",
                generation=2,
                state=CommandState.RUNNING,
                accepted_at=now,
                runtime_instance_id="runtime-2",
            ),
        ),
        pending_events=(
            SessionEvent(
                event_id="notice-2",
                generation=2,
                event_type=EventType.WORKSPACE_LOST,
                occurred_at=now,
                reason="reason",
                context_message="notice",
            ),
        ),
    )


def test_session_codec_preserves_resources_commands_notices_and_native_dates():
    record = complete_record()
    values = encode_session(record, 7)
    assert isinstance(values["idle_since"], datetime)
    assert isinstance(values["retry_after"], datetime)
    assert isinstance(values["active_commands"][0]["accepted_at"], datetime)
    assert isinstance(values["pending_events"][0]["occurred_at"], datetime)
    restored = decode_session(values, record.key_hash)
    assert restored.record == record
    assert restored.resource_version == "7"


@pytest.mark.parametrize("patch", [{"schema_version": 2}, {"resource_version": True}])
def test_session_codec_rejects_unknown_schema_and_invalid_revision(patch):
    record = complete_record()
    with pytest.raises(RepositoryConflictError):
        decode_session({**encode_session(record, 1), **patch}, record.key_hash)


def test_session_codec_rejects_document_key_mismatch():
    with pytest.raises(RepositoryConflictError, match="key"):
        decode_session(encode_session(complete_record(), 1), "a" * 64)
