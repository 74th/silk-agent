from datetime import UTC, datetime, timedelta

import pytest

from gke_session_sandbox import (
    RepositoryConflictError,
    SessionKey,
    SessionRecord,
    SessionState,
    SQLiteSessionRepository,
    StaleGenerationError,
)


async def test_sqlite_operations_survive_restart_and_complete_once(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    key = SessionKey(tenant_id="tenant", session_id="files")
    path = tmp_path / "files.sqlite3"
    first = SQLiteSessionRepository(path, clock=lambda: now)
    token = await first.acquire_lease(key, "service-a")
    await first.create(
        token,
        SessionRecord(
            key_hash=key.stable_hash(),
            generation=1,
            state=SessionState.READY,
        ),
    )
    operation = dict(operation_id="op-a", generation=1, executor_id="service-a")
    accepted = await first.begin_file_operation(
        token, **operation, expires_at=now + timedelta(seconds=60)
    )
    assert accepted.record.active_file_operations == 1
    duplicate = await first.begin_file_operation(
        token, **operation, expires_at=now + timedelta(seconds=60)
    )
    assert duplicate == accepted
    await first.release_lease(token)
    await first.close()

    second = SQLiteSessionRepository(path, clock=lambda: now + timedelta(seconds=120))
    token = await second.acquire_lease(key, "service-b")
    try:
        assert (await second.get(key)).record.active_file_operations == 1
        with pytest.raises(RepositoryConflictError):
            await second.finish_file_operation(token, **{**operation, "executor_id": "service-b"})
        with pytest.raises(StaleGenerationError):
            await second.finish_file_operation(token, **{**operation, "generation": 2})
        uncertain = await second.finish_file_operation(token, **operation, confirmed=False)
        assert uncertain.record.active_file_operations == 1
        assert uncertain.record.idle_since is None
        finished = await second.finish_file_operation(token, **operation)
        assert finished.record.active_file_operations == 0
        assert finished.record.idle_since == now + timedelta(seconds=120)
        assert await second.finish_file_operation(token, **operation) == finished
    finally:
        await second.close()
