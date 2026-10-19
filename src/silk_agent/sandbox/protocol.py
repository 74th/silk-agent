"""Validated request and response contracts for Sandbox command operations."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal

from pydantic import Field, StrictInt, StrictStr, field_validator

from gke_session_sandbox.types import StrictModel

CommandStartState = Literal["accepted", "running", "finished", "cancelled", "unknown"]


class CommandIdentity(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    conversation_key: StrictStr = Field(min_length=1)
    owner_epoch: StrictInt = Field(ge=1)
    conversation_generation: StrictInt = Field(ge=1)
    turn_id: StrictStr = Field(min_length=1)
    origin_seq: StrictInt = Field(ge=1)


class CommandStartRequest(CommandIdentity):
    command: StrictStr = Field(min_length=1)
    cwd: StrictStr = ""
    timeout_seconds: StrictInt = Field(default=0, ge=0, le=86400)


class CommandStatusRequest(CommandIdentity):
    pass


class CommandOutputRequest(CommandIdentity):
    stream: Literal["stdout", "stderr"] = "stdout"
    cursor: StrictInt = Field(default=0, ge=0)
    limit_bytes: StrictInt = Field(default=65536, gt=0, le=1048576)


class CommandControlRequest(CommandIdentity):
    reason: StrictStr = Field(default="user", min_length=1)


class CommandStartResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    command_id: StrictStr = Field(min_length=1)
    state: CommandStartState
    sandbox_generation: StrictInt = Field(ge=1)


class CommandStatusResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    command_id: StrictStr = Field(min_length=1)
    state: Literal["running", "finished", "cancelled", "unknown"]
    sandbox_generation: StrictInt = Field(ge=1)
    exit_code: StrictInt | None = None
    stop_reason: StrictStr | None = None


class CommandOutputResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    command_id: StrictStr = Field(min_length=1)
    sandbox_generation: StrictInt = Field(ge=1)
    data_base64: StrictStr
    next_cursor: StrictInt = Field(ge=0)
    complete: bool
    # The stream ended, but part of its output was never stored (size limit, write failure).
    truncated: bool = False


class CommandControlResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    command_id: StrictStr = Field(min_length=1)
    state: Literal["cancel_requested", "cancelled", "detached", "unknown"]
    sandbox_generation: StrictInt = Field(ge=1)


class FileReadRequest(CommandIdentity):
    path: StrictStr = Field(min_length=1)

    @field_validator("path")
    @classmethod
    def validate_workspace_path(cls, value: str) -> str:
        if not value or "\x00" in value or value.startswith("/"):
            raise ValueError("workspace path must be relative")
        parts = PurePosixPath(value).parts
        if ".." in parts or (parts and parts[0] == ".session-runtime"):
            raise ValueError("workspace path is outside the public workspace")
        return value


class FileWriteRequest(FileReadRequest):
    data_base64: StrictStr


class FileListRequest(FileReadRequest):
    path: StrictStr = "."


class SkillsSyncRequest(CommandIdentity):
    skills_generation: StrictStr = Field(min_length=1)
    files: dict[StrictStr, StrictStr]


class FileEntryResponse(StrictModel):
    path: StrictStr
    is_dir: bool
    size: StrictInt = Field(ge=0)


class FileReadResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    sandbox_generation: StrictInt = Field(ge=1)
    data_base64: StrictStr


class FileWriteResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    sandbox_generation: StrictInt = Field(ge=1)
    state: Literal["written", "unknown"]


class FileListResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    sandbox_generation: StrictInt = Field(ge=1)
    entries: list[FileEntryResponse]


class SkillsSyncResponse(StrictModel):
    request_id: StrictStr = Field(min_length=1)
    sandbox_generation: StrictInt = Field(ge=1)
    skills_generation: StrictStr
    complete: bool
    failed_paths: list[StrictStr]
