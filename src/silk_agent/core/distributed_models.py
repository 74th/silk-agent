"""Persistent records shared by distributed Silk Agent processes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Self

SCHEMA_VERSION = 1


def _validate_timestamp(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be UTC")


def _datetime_from_dict(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 string") from exc
    _validate_timestamp(result, field_name)
    return result.astimezone(UTC)


def _positive_int(value: int, field_name: str, *, allow_zero: bool = False) -> None:
    minimum = 0 if allow_zero else 1
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")


def _schema_version(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {value}")


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _integer(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _payload(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("payload must be a mapping")
    return value


def _boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a bool")
    return value


class InputDeliveryState(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    QUEUED = "queued"
    ACCEPTED = "accepted"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    SUPPRESSED = "suppressed"
    HELD = "held"
    UNCERTAIN = "uncertain"


class CommandExecutionState(StrEnum):
    INTENT = "intent"
    RUNNING = "running"
    FINISHED = "finished"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class CommandDeliveryMode(StrEnum):
    FOREGROUND = "foreground"
    BACKGROUND = "background"
    ORPHANED = "orphaned"


class NotificationState(StrEnum):
    NONE = "none"
    PENDING = "pending"
    DELIVERED = "delivered"
    SUPPRESSED = "suppressed"
    HELD = "held"


class OutputDeliveryState(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    DELIVERED = "delivered"
    SUPPRESSED = "suppressed"
    UNCERTAIN = "uncertain"
    # Known not to have been posted, and not retryable: only this output is given up.
    FAILED = "failed"


_TRANSITIONS: dict[type[StrEnum], dict[StrEnum, frozenset[StrEnum]]] = {
    InputDeliveryState: {
        InputDeliveryState.PENDING: frozenset(
            {
                InputDeliveryState.DISPATCHING,
                InputDeliveryState.BLOCKED,
                InputDeliveryState.CANCELLED,
                InputDeliveryState.SUPPRESSED,
                InputDeliveryState.HELD,
            }
        ),
        InputDeliveryState.DISPATCHING: frozenset(
            {
                InputDeliveryState.PENDING,
                InputDeliveryState.QUEUED,
                InputDeliveryState.ACCEPTED,
                InputDeliveryState.CANCELLED,
                InputDeliveryState.SUPPRESSED,
                InputDeliveryState.HELD,
                InputDeliveryState.UNCERTAIN,
            }
        ),
        InputDeliveryState.QUEUED: frozenset(
            {
                InputDeliveryState.DISPATCHING,
                InputDeliveryState.ACCEPTED,
                InputDeliveryState.CANCELLED,
                InputDeliveryState.SUPPRESSED,
                InputDeliveryState.HELD,
                InputDeliveryState.UNCERTAIN,
            }
        ),
        InputDeliveryState.ACCEPTED: frozenset(
            {
                InputDeliveryState.COMPLETED,
                InputDeliveryState.UNCERTAIN,
                InputDeliveryState.CANCELLED,
                InputDeliveryState.SUPPRESSED,
            }
        ),
        InputDeliveryState.COMPLETED: frozenset(),
        InputDeliveryState.BLOCKED: frozenset(),
        InputDeliveryState.CANCELLED: frozenset(),
        InputDeliveryState.SUPPRESSED: frozenset(),
        InputDeliveryState.HELD: frozenset(
            {InputDeliveryState.QUEUED, InputDeliveryState.SUPPRESSED}
        ),
        InputDeliveryState.UNCERTAIN: frozenset(),
    },
    CommandExecutionState: {
        CommandExecutionState.INTENT: frozenset(
            {
                CommandExecutionState.RUNNING,
                CommandExecutionState.FINISHED,
                CommandExecutionState.CANCEL_REQUESTED,
                CommandExecutionState.CANCELLED,
                CommandExecutionState.UNKNOWN,
            }
        ),
        CommandExecutionState.RUNNING: frozenset(
            {
                CommandExecutionState.FINISHED,
                CommandExecutionState.CANCEL_REQUESTED,
                CommandExecutionState.CANCELLED,
                CommandExecutionState.UNKNOWN,
            }
        ),
        CommandExecutionState.CANCEL_REQUESTED: frozenset(
            {
                CommandExecutionState.CANCELLED,
                CommandExecutionState.FINISHED,
                CommandExecutionState.UNKNOWN,
            }
        ),
        CommandExecutionState.FINISHED: frozenset(),
        CommandExecutionState.CANCELLED: frozenset(),
        CommandExecutionState.UNKNOWN: frozenset(),
    },
    NotificationState: {
        NotificationState.NONE: frozenset({NotificationState.PENDING}),
        NotificationState.PENDING: frozenset(
            {NotificationState.DELIVERED, NotificationState.SUPPRESSED, NotificationState.HELD}
        ),
        NotificationState.HELD: frozenset(
            {NotificationState.PENDING, NotificationState.SUPPRESSED}
        ),
        NotificationState.DELIVERED: frozenset(),
        NotificationState.SUPPRESSED: frozenset(),
    },
    OutputDeliveryState: {
        OutputDeliveryState.PENDING: frozenset(
            {OutputDeliveryState.DISPATCHING, OutputDeliveryState.SUPPRESSED}
        ),
        OutputDeliveryState.DISPATCHING: frozenset(
            {
                OutputDeliveryState.PENDING,
                OutputDeliveryState.DELIVERED,
                OutputDeliveryState.UNCERTAIN,
                OutputDeliveryState.SUPPRESSED,
                OutputDeliveryState.FAILED,
            }
        ),
        OutputDeliveryState.UNCERTAIN: frozenset(),
        OutputDeliveryState.FAILED: frozenset(),
        OutputDeliveryState.DELIVERED: frozenset(),
        OutputDeliveryState.SUPPRESSED: frozenset(),
    },
}


def validate_state_transition(current: StrEnum, new: StrEnum) -> None:
    """Reject illegal state changes; idempotent writes of the current state are allowed."""
    if type(current) is not type(new):
        raise ValueError("state transition must use the same enum")
    if current == new:
        return
    if new not in _TRANSITIONS[type(current)][current]:
        raise ValueError(f"invalid {type(current).__name__} transition: {current} -> {new}")


@dataclass(frozen=True)
class InputEnvelope:
    conversation_key: str
    seq: int
    input_id: str
    conversation_generation: int
    kind: str
    payload: Mapping[str, Any]
    created_at: datetime
    delivery_state: InputDeliveryState = InputDeliveryState.PENDING
    dispatch_attempt_id: str | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _required_string(self.conversation_key, "conversation_key")
        _required_string(self.input_id, "input_id")
        _required_string(self.kind, "kind")
        _positive_int(self.seq, "seq")
        _positive_int(self.conversation_generation, "conversation_generation")
        _validate_timestamp(self.created_at, "created_at")
        _schema_version(self.schema_version)
        if not isinstance(self.delivery_state, InputDeliveryState):
            raise ValueError("delivery_state must be an InputDeliveryState")
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be a mapping")
        if self.dispatch_attempt_id is not None and not isinstance(self.dispatch_attempt_id, str):
            raise ValueError("dispatch_attempt_id must be a string or None")

    def to_dict(self) -> dict[str, Any]:
        return {
            "conversation_key": self.conversation_key,
            "seq": self.seq,
            "input_id": self.input_id,
            "conversation_generation": self.conversation_generation,
            "kind": self.kind,
            "payload": dict(self.payload),
            "created_at": self.created_at.isoformat(),
            "delivery_state": self.delivery_state.value,
            "dispatch_attempt_id": self.dispatch_attempt_id,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Self:
        return cls(
            conversation_key=_required_string(value.get("conversation_key"), "conversation_key"),
            seq=_integer(value.get("seq"), "seq"),
            input_id=_required_string(value.get("input_id"), "input_id"),
            conversation_generation=_integer(
                value.get("conversation_generation"), "conversation_generation"
            ),
            kind=_required_string(value.get("kind"), "kind"),
            payload=_payload(value.get("payload")),
            created_at=_datetime_from_dict(value.get("created_at"), "created_at"),
            delivery_state=InputDeliveryState(value.get("delivery_state")),
            dispatch_attempt_id=value.get("dispatch_attempt_id"),
            schema_version=_integer(value.get("schema_version"), "schema_version"),
        )


@dataclass(frozen=True)
class ControlEnvelope:
    conversation_key: str
    seq: int
    input_id: str
    conversation_generation: int
    kind: str
    sender_id: str
    payload: Mapping[str, Any]
    created_at: datetime
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _required_string(self.conversation_key, "conversation_key")
        _required_string(self.input_id, "input_id")
        _required_string(self.kind, "kind")
        _required_string(self.sender_id, "sender_id")
        _positive_int(self.seq, "seq")
        _positive_int(self.conversation_generation, "conversation_generation")
        _validate_timestamp(self.created_at, "created_at")
        _schema_version(self.schema_version)
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be a mapping")

    def to_dict(self) -> dict[str, Any]:
        return {
            "conversation_key": self.conversation_key,
            "seq": self.seq,
            "input_id": self.input_id,
            "conversation_generation": self.conversation_generation,
            "kind": self.kind,
            "sender_id": self.sender_id,
            "payload": dict(self.payload),
            "created_at": self.created_at.isoformat(),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Self:
        return cls(
            conversation_key=_required_string(value.get("conversation_key"), "conversation_key"),
            seq=_integer(value.get("seq"), "seq"),
            input_id=_required_string(value.get("input_id"), "input_id"),
            conversation_generation=_integer(
                value.get("conversation_generation"), "conversation_generation"
            ),
            kind=_required_string(value.get("kind"), "kind"),
            sender_id=_required_string(value.get("sender_id"), "sender_id"),
            payload=_payload(value.get("payload")),
            created_at=_datetime_from_dict(value.get("created_at"), "created_at"),
            schema_version=_integer(value.get("schema_version"), "schema_version"),
        )


@dataclass(frozen=True)
class OwnershipToken:
    conversation_key: str
    worker_id: str
    worker_incarnation: str
    epoch: int
    conversation_generation: int
    expires_at: datetime
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _required_string(self.conversation_key, "conversation_key")
        _required_string(self.worker_id, "worker_id")
        _required_string(self.worker_incarnation, "worker_incarnation")
        _positive_int(self.epoch, "epoch")
        _positive_int(self.conversation_generation, "conversation_generation")
        _validate_timestamp(self.expires_at, "expires_at")
        _schema_version(self.schema_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "conversation_key": self.conversation_key,
            "worker_id": self.worker_id,
            "worker_incarnation": self.worker_incarnation,
            "epoch": self.epoch,
            "conversation_generation": self.conversation_generation,
            "expires_at": self.expires_at.isoformat(),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Self:
        return cls(
            conversation_key=_required_string(value.get("conversation_key"), "conversation_key"),
            worker_id=_required_string(value.get("worker_id"), "worker_id"),
            worker_incarnation=_required_string(
                value.get("worker_incarnation"), "worker_incarnation"
            ),
            epoch=_integer(value.get("epoch"), "epoch"),
            conversation_generation=_integer(
                value.get("conversation_generation"), "conversation_generation"
            ),
            expires_at=_datetime_from_dict(value.get("expires_at"), "expires_at"),
            schema_version=_integer(value.get("schema_version"), "schema_version"),
        )


@dataclass(frozen=True)
class WorkerRecord:
    worker_id: str
    incarnation: str
    capacity: int
    reserved: int
    active_sdk: int
    draining: bool
    heartbeat_at: datetime
    drain_deadline_at: datetime | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _required_string(self.worker_id, "worker_id")
        _required_string(self.incarnation, "incarnation")
        _positive_int(self.capacity, "capacity")
        _positive_int(self.reserved, "reserved", allow_zero=True)
        _positive_int(self.active_sdk, "active_sdk", allow_zero=True)
        if self.reserved > self.capacity or self.active_sdk > self.capacity:
            raise ValueError("worker usage cannot exceed capacity")
        if not isinstance(self.draining, bool):
            raise ValueError("draining must be a bool")
        _validate_timestamp(self.heartbeat_at, "heartbeat_at")
        if self.drain_deadline_at is not None:
            _validate_timestamp(self.drain_deadline_at, "drain_deadline_at")
        _schema_version(self.schema_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "incarnation": self.incarnation,
            "capacity": self.capacity,
            "reserved": self.reserved,
            "active_sdk": self.active_sdk,
            "draining": self.draining,
            "heartbeat_at": self.heartbeat_at.isoformat(),
            "drain_deadline_at": (
                self.drain_deadline_at.isoformat() if self.drain_deadline_at is not None else None
            ),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Self:
        return cls(
            worker_id=_required_string(value.get("worker_id"), "worker_id"),
            incarnation=_required_string(value.get("incarnation"), "incarnation"),
            capacity=_integer(value.get("capacity"), "capacity"),
            reserved=_integer(value.get("reserved"), "reserved"),
            active_sdk=_integer(value.get("active_sdk"), "active_sdk"),
            draining=_boolean(value.get("draining"), "draining"),
            heartbeat_at=_datetime_from_dict(value.get("heartbeat_at"), "heartbeat_at"),
            drain_deadline_at=(
                _datetime_from_dict(value["drain_deadline_at"], "drain_deadline_at")
                if value.get("drain_deadline_at") is not None
                else None
            ),
            schema_version=_integer(value.get("schema_version"), "schema_version"),
        )
