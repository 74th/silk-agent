"""Typed identities and durable state values used by public APIs and backends."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


class SessionKey(StrictModel):
    tenant_id: StrictStr = Field(min_length=1)
    session_id: StrictStr = Field(min_length=1)

    @field_validator("tenant_id", "session_id")
    @classmethod
    def reject_blank_values(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value

    def stable_hash(self) -> str:
        """Return a non-reversible stable identifier for Kubernetes names."""
        canonical = json.dumps(
            [self.tenant_id, self.session_id], ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


class SessionState(StrEnum):
    CREATING = "creating"
    READY = "ready"
    SUSPENDING = "suspending"
    SUSPENDED = "suspended"
    RESUMING = "resuming"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    LOST = "lost"


class CommandState(StrEnum):
    RUNNING = "running"
    FINISHED = "finished"
    UNKNOWN = "unknown"


class DesiredSandboxState(StrEnum):
    RUNNING = "Running"
    SUSPENDED = "Suspended"


class ResourceRef(StrictModel):
    api_version: StrictStr = Field(min_length=1)
    kind: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    uid: StrictStr = Field(min_length=1)
    namespace: StrictStr | None = None


class ProcessIdentity(StrictModel):
    boot_id: StrictStr = Field(min_length=1)
    pid: StrictInt = Field(gt=0)
    process_group_id: StrictInt = Field(gt=0)
    start_ticks: StrictInt = Field(gt=0)


class CommandRecord(StrictModel):
    command_id: StrictStr = Field(min_length=1)
    generation: StrictInt = Field(ge=1)
    runtime_instance_id: StrictStr = Field(min_length=1)
    state: CommandState
    process_id: StrictInt | None = Field(default=None, gt=0)
    process_identity: ProcessIdentity | None = None
    accepted_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: StrictInt | None = None
    output_complete: bool = True

    @field_validator("accepted_at", "started_at", "finished_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_state_timestamps(self) -> "CommandRecord":
        if self.state is CommandState.RUNNING and self.finished_at is not None:
            raise ValueError("running command must not have finished_at")
        if self.state is CommandState.FINISHED and self.finished_at is None:
            raise ValueError("finished command requires finished_at")
        if self.finished_at is not None and self.started_at is not None:
            if self.finished_at < self.started_at:
                raise ValueError("finished_at must not precede started_at")
        return self


class EventType(StrEnum):
    WORKSPACE_CLEARED = "workspace_cleared"
    WORKSPACE_LOST = "workspace_lost"


class SessionEvent(StrictModel):
    event_id: StrictStr = Field(min_length=1)
    generation: StrictInt = Field(ge=1)
    event_type: EventType
    occurred_at: datetime
    reason: StrictStr = Field(min_length=1)
    context_message: StrictStr = Field(min_length=1)
    acked_at: datetime | None = None

    @field_validator("occurred_at", "acked_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("timestamps must include a timezone")
        return value


class SessionRecord(StrictModel):
    key_hash: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    generation: StrictInt = Field(ge=1)
    state: SessionState
    sandbox_ref: ResourceRef | None = None
    workspace_pvc_refs: tuple[ResourceRef, ...] = ()
    template_digest: StrictStr | None = None
    policy_digest: StrictStr | None = None
    desired_state: DesiredSandboxState | None = None
    ready_at: datetime | None = None
    last_command_started_at: datetime | None = None
    last_command_finished_at: datetime | None = None
    last_commands_checked_at: datetime | None = None
    last_no_commands_confirmed_at: datetime | None = None
    last_file_operation_at: datetime | None = None
    idle_since: datetime | None = None
    active_commands: tuple[CommandRecord, ...] = ()
    active_file_operations: StrictInt = Field(default=0, ge=0)
    command_state_known: bool = True
    runtime_instance_id: StrictStr | None = None
    suspended_at: datetime | None = None
    expires_at: datetime | None = None
    creation_env_fingerprint: StrictStr | None = None
    operation_id: StrictStr | None = None
    operation_phase: StrictStr | None = None
    pending_events: tuple[SessionEvent, ...] = ()
    last_error: StrictStr | None = None
    retry_after: datetime | None = None

    @field_validator(
        "ready_at", "last_command_started_at", "last_command_finished_at",
        "last_commands_checked_at", "last_no_commands_confirmed_at", "last_file_operation_at", "idle_since",
        "suspended_at", "expires_at", "retry_after",
    )
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("timestamps must include a timezone")
        return value


def utc_now() -> datetime:
    """Return an aware UTC timestamp for record creation and injected test clocks."""
    return datetime.now(timezone.utc)
