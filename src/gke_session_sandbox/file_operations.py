"""Durable file-operation identity and aggregate activity accounting."""

from datetime import datetime
from typing import Literal

from pydantic import Field, StrictInt, StrictStr

from .errors import RepositoryConflictError, StaleGenerationError
from .types import SessionRecord, SessionState, StrictModel


class FileOperation(StrictModel):
    operation_id: StrictStr = Field(min_length=1)
    executor_id: StrictStr = Field(min_length=1)
    generation: StrictInt = Field(ge=1)
    state: Literal["running", "completed", "unknown"]
    started_at: datetime
    expires_at: datetime
    completed_at: datetime | None = None
    schema_version: StrictInt = 1


def change_file_operation(
    record: SessionRecord,
    existing: FileOperation | None,
    *,
    operation_id: str,
    executor_id: str,
    generation: int,
    now: datetime,
    expires_at: datetime | None,
    confirmed: bool = True,
) -> tuple[SessionRecord, FileOperation, bool]:
    """An expired deadline keeps counting until the executor confirms completion."""
    if generation != record.generation:
        raise StaleGenerationError("file operation belongs to another Sandbox generation")
    if not operation_id.strip() or not executor_id.strip():
        raise ValueError("file operation ID and executor are required")
    if existing is not None and (
        existing.operation_id != operation_id
        or existing.executor_id != executor_id
        or existing.generation != generation
    ):
        raise RepositoryConflictError("file operation ID belongs to another executor or generation")
    if expires_at is not None:
        if expires_at.tzinfo is None or expires_at.utcoffset() is None or expires_at <= now:
            raise ValueError("file operation deadline must be aware and in the future")
        if existing is not None:
            return record, existing, False
        if record.state is not SessionState.READY:
            raise RepositoryConflictError("file operation requires a ready Sandbox")
        operation = FileOperation(
            operation_id=operation_id,
            executor_id=executor_id,
            generation=generation,
            state="running",
            started_at=now,
            expires_at=expires_at,
        )
        return (
            record.model_copy(
                update={
                    "active_file_operations": record.active_file_operations + 1,
                    "idle_since": None,
                }
            ),
            operation,
            True,
        )
    if existing is None:
        raise RepositoryConflictError("file operation was never accepted")
    if existing.state == "completed":
        return record, existing, False
    if not confirmed:
        return record, existing.model_copy(update={"state": "unknown"}), existing.state != "unknown"
    if record.active_file_operations < 1:
        raise RepositoryConflictError("file activity count is inconsistent")
    count = record.active_file_operations - 1
    return (
        record.model_copy(
            update={
                "active_file_operations": count,
                "last_file_operation_at": now,
                "idle_since": now if count == 0 else None,
            }
        ),
        existing.model_copy(update={"state": "completed", "completed_at": now}),
        True,
    )
