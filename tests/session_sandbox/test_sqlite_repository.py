from __future__ import annotations

from datetime import UTC, datetime

import pytest

from gke_session_sandbox import (
    ConfigurationError,
    EventType,
    RepositoryConflictError,
    SessionEvent,
    SessionKey,
    SessionLeaseHeldError,
    SessionRecord,
    SessionState,
    SQLiteSessionRepository,
)


def record_for(key: SessionKey, **changes: object) -> SessionRecord:
    return SessionRecord(
        key_hash=key.stable_hash(), generation=1, state=SessionState.CREATING, **changes
    )


def test_sqlite_repository_can_fail_fast_when_persistent_mount_is_missing(tmp_path) -> None:
    with pytest.raises(ConfigurationError, match="not a mounted volume"):
        SQLiteSessionRepository(tmp_path / "sessions.sqlite3", require_mount=True)


@pytest.mark.asyncio
async def test_sqlite_repository_survives_reopen_and_has_no_configmap_sized_record_limit(
    tmp_path,
) -> None:
    db_path = tmp_path / "pvc" / "sessions.sqlite3"
    key = SessionKey(tenant_id="tenant", session_id="large-session")
    now = datetime(2030, 1, 1, tzinfo=UTC)
    events = tuple(
        SessionEvent(
            event_id=f"event-{index}",
            generation=1,
            event_type=EventType.WORKSPACE_LOST,
            occurred_at=now,
            reason="r" * 20_000,
            context_message="c" * 10_000,
        )
        for index in range(60)
    )

    repo = SQLiteSessionRepository(db_path, max_pending_events=100)
    token = await repo.acquire_lease(key, "api-process-a")
    created = await repo.create(token, record_for(key, pending_events=events))
    assert len(created.record.model_dump_json().encode()) > 1024 * 1024
    await repo.close()

    restarted = SQLiteSessionRepository(db_path)
    restored = await restarted.get(key)
    assert restored is not None
    assert restored.record.pending_events == events
    assert len(await restarted.list_records()) == 1
    await restarted.close()


@pytest.mark.asyncio
async def test_sqlite_repository_serializes_competing_writers_and_session_leases(tmp_path) -> None:
    db_path = tmp_path / "sessions.sqlite3"
    key = SessionKey(tenant_id="tenant", session_id="session")
    repo_a = SQLiteSessionRepository(db_path)
    repo_b = SQLiteSessionRepository(db_path)
    token = await repo_a.acquire_lease(key, "api-process-a")

    with pytest.raises(SessionLeaseHeldError):
        await repo_b.acquire_lease(key, "api-process-b")

    created = await repo_a.create(token, record_for(key))
    desired_a = created.record.model_copy(update={"operation_phase": "phase-a"})
    desired_b = created.record.model_copy(update={"operation_phase": "phase-b"})
    await repo_a.compare_and_swap(
        token, desired_a, expected_resource_version=created.resource_version
    )
    with pytest.raises(RepositoryConflictError, match="version changed"):
        await repo_b.compare_and_swap(
            token, desired_b, expected_resource_version=created.resource_version
        )

    await repo_a.close()
    await repo_b.close()


@pytest.mark.asyncio
async def test_reconciliation_candidate_pages_filter_states_and_advance_cursor(tmp_path) -> None:
    repo = SQLiteSessionRepository(tmp_path / "sessions.sqlite3")
    keys = [SessionKey(tenant_id="tenant", session_id=f"session-{index}") for index in range(4)]
    for key, state in zip(
        keys,
        (
            SessionState.READY,
            SessionState.CREATING,
            SessionState.SUSPENDED,
            SessionState.TERMINATED,
        ),
        strict=True,
    ):
        token = await repo.acquire_lease(key, "test")
        await repo.create(token, record_for(key).model_copy(update={"state": state}))
        await repo.release_lease(token)

    expected = sorted(
        (
            (keys[0].stable_hash(), SessionState.READY),
            (keys[2].stable_hash(), SessionState.SUSPENDED),
        )
    )
    first, cursor = await repo.list_reconciliation_candidates(limit=1)
    assert len(first) == 1
    assert (first[0].record.key_hash, first[0].record.state) == expected[0]
    assert cursor == expected[0][0]

    second, cursor = await repo.list_reconciliation_candidates(limit=1, cursor=cursor)
    assert len(second) == 1
    assert (second[0].record.key_hash, second[0].record.state) == expected[1]
    assert cursor is None
    await repo.close()
