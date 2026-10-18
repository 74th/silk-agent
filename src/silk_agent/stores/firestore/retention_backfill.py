"""Give `expire_at` to records that reached a terminal state before retention existed.

The rules are the ones the stores apply when a record ends (`retention.py`). A record whose end
time was never stored counts from the time of the run, so it is not deleted sooner than a record
that ends today. Records that are still processing, uncertain, held, or whose command notice has
not been delivered are left alone. A run only counts unless `apply` is set, and `apply` re-checks
each record inside its transaction, so running it twice changes nothing the second time.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional

from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputDeliveryState,
    NotificationState,
    OutputDeliveryState,
)
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import document_id
from silk_agent.stores.firestore.retention import (
    RetentionPolicy,
    command_expiry,
    control_expiry,
    output_expiry,
    quiesce_expiry,
    receipt_expiry,
    terminal_expiry,
    worker_expiry,
)

ALREADY = "already has expire_at"


@dataclass(frozen=True)
class Decision:
    expire_at: datetime | None
    reason: str = ""  # why there is no expire_at


@dataclass
class KindReport:
    kind: str
    examined: int = 0
    eligible: int = 0
    written: int = 0
    excluded: Counter[str] = field(default_factory=Counter)

    @property
    def excluded_total(self) -> int:
        return sum(self.excluded.values())


def _at(value: Any, default: datetime) -> datetime:
    """A stored time (Timestamp or ISO string) in UTC, or `default` if there is none."""
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return default
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return default


def _first(data: Mapping[str, Any], *names: str, default: datetime) -> datetime:
    for name in names:
        if data.get(name) is not None:
            return _at(data[name], default)
    return default


def _enum[T](kind: type[T], value: Any) -> T | None:
    try:
        return kind(value)  # type: ignore[call-arg]
    except ValueError:
        return None


def decide_input(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    state = _enum(InputDeliveryState, data.get("delivery_state"))
    if state is None:
        return Decision(None, "unknown state")
    expiry = terminal_expiry(
        state, _first(data, "completed_at", default=now), retention=policy.terminal
    )
    return Decision(expiry, "" if expiry else f"state {state.value}")


def decide_control(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    state = str(data.get("authorization_state"))
    ended = _first(data, "handled_at", "authorized_at", default=now)
    expiry = control_expiry(state, ended, retention=policy.terminal)
    return Decision(expiry, "" if expiry else f"state {state}")


def decide_output(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    state = _enum(OutputDeliveryState, data.get("status"))
    if state is None:
        return Decision(None, "unknown state")
    expiry = output_expiry(
        state, _first(data, "updated_at", default=now), retention=policy.terminal
    )
    return Decision(expiry, "" if expiry else f"state {state.value}")


def decide_command(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    state = _enum(CommandExecutionState, data.get("state"))
    notice = _enum(NotificationState, data.get("notification_state", "none"))
    if state is None or notice is None:
        return Decision(None, "unknown state")
    expiry = command_expiry(
        state,
        notice,
        _first(data, "completed_at", default=now),
        _at(data["notification_updated_at"], now) if data.get("notification_updated_at") else None,
        retention=policy.terminal,
    )
    if expiry is not None:
        return Decision(expiry)
    if state in (
        CommandExecutionState.FINISHED,
        CommandExecutionState.CANCELLED,
        CommandExecutionState.UNKNOWN,
    ):
        return Decision(None, f"notice {notice.value}")
    return Decision(None, f"state {state.value}")


def decide_quiesce(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    state = str(data.get("state"))
    expiry = quiesce_expiry(
        state, _first(data, "answered_at", default=now), retention=policy.terminal
    )
    return Decision(expiry, "" if expiry else f"state {state}")


def decide_receipt(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    return Decision(
        receipt_expiry(_first(data, "received_at", default=now), retention=policy.receipt)
    )


def decide_worker(data: Mapping[str, Any], now: datetime, policy: RetentionPolicy) -> Decision:
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    return Decision(
        worker_expiry(_first(data, "heartbeat_at", default=now), retention=policy.worker)
    )


def decide_operation(
    data: Mapping[str, Any],
    command: Mapping[str, Any] | None,
    now: datetime,
    policy: RetentionPolicy,
) -> Decision:
    """A Sandbox operation expires with the command of the same ID (design D4).

    The command's own `expire_at` is used when it has one; otherwise the one the command is
    about to get, so a count before `--apply` matches what `--apply` then writes.
    """
    if data.get("expire_at") is not None:
        return Decision(None, ALREADY)
    if data.get("operation_kind") != "command_start" or command is None:
        return Decision(None, "no command")
    expire_at = command.get("expire_at")
    if isinstance(expire_at, datetime):
        return Decision(expire_at)
    pending = decide_command(command, now, policy)
    if pending.expire_at is None:
        return Decision(None, "command not expiring")
    return Decision(pending.expire_at)


@dataclass(frozen=True)
class _Kind:
    name: str
    refs: Callable[[firestore.AsyncClient, str], Any]  # a collection or collection group
    decide: Callable[[Mapping[str, Any], datetime, RetentionPolicy], Decision] | None
    # Sandbox operations decide from their command; they are the only kind that does.
    command_collection: str | None = None
    # Collection groups span every prefix; only records under `{prefix}_conversations` count.
    in_prefix: bool = False


def _kinds() -> tuple[_Kind, ...]:
    def top(name: str):
        return lambda client, prefix: client.collection(f"{prefix}_{name}")

    def group(name: str):
        return lambda client, prefix: client.collection_group(name)

    return (
        _Kind("inputs", group("inputs"), decide_input, in_prefix=True),
        _Kind("controls", group("controls"), decide_control, in_prefix=True),
        _Kind("control_queue", top("control_queue"), decide_control),
        _Kind("outbox", top("outbox"), decide_output),
        _Kind("commands", top("commands"), decide_command),
        # After commands: an operation follows the command that was just given its expiry.
        _Kind("sandbox_operations", top("sandbox_operations"), None, command_collection="commands"),
        _Kind("quiesce_requests", top("quiesce_requests"), decide_quiesce),
        _Kind("slack_receipts", top("slack_receipts"), decide_receipt),
        _Kind("workers", top("workers"), decide_worker),
    )


async def backfill(
    client: firestore.AsyncClient,
    *,
    prefix: str,
    policy: RetentionPolicy,
    now: datetime,
    apply: bool = False,
    limit: int | None = None,
) -> list[KindReport]:
    """Count (and with `apply`, write) the `expire_at` that terminal records are missing.

    `limit` caps the eligible records handled per kind, to bound one run.
    """
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    reports = []
    for kind in _kinds():
        report = KindReport(kind.name)
        reports.append(report)
        try:
            async for snapshot in kind.refs(client, prefix).stream():
                if kind.in_prefix and not snapshot.reference.path.startswith(
                    f"{prefix}_conversations/"
                ):
                    continue  # another prefix's conversation in the same database
                if limit is not None and report.eligible >= limit:
                    break
                report.examined += 1
                data = snapshot.to_dict() or {}
                decision = await _decide(client, prefix, kind, data, now, policy, None)
                if decision.expire_at is None:
                    report.excluded[decision.reason or "excluded"] += 1
                    continue
                report.eligible += 1
                if apply and await _write(client, prefix, kind, snapshot.reference, now, policy):
                    report.written += 1
        except Exception as exc:
            raise convert_firestore_error(f"retention_backfill_{kind.name}", exc) from exc
    return reports


async def _decide(
    client: firestore.AsyncClient,
    prefix: str,
    kind: _Kind,
    data: Mapping[str, Any],
    now: datetime,
    policy: RetentionPolicy,
    transaction: firestore.AsyncTransaction | None,
) -> Decision:
    if kind.decide is not None:
        return kind.decide(data, now, policy)
    operation_id = data.get("operation_id")
    command: Mapping[str, Any] | None = None
    if isinstance(operation_id, str) and data.get("operation_kind") == "command_start":
        command_ref = client.collection(f"{prefix}_{kind.command_collection}").document(
            document_id(operation_id, prefix="command")
        )
        snapshot = await command_ref.get(transaction=transaction)
        command = snapshot.to_dict() if snapshot.exists else None
    return decide_operation(data, command, now, policy)


async def _write(
    client: firestore.AsyncClient,
    prefix: str,
    kind: _Kind,
    ref: firestore.AsyncDocumentReference,
    now: datetime,
    policy: RetentionPolicy,
) -> bool:
    """Write `expire_at` if the record is still terminal and has none (one transaction)."""
    transaction = client.transaction()

    @async_transactional
    async def write(transaction: firestore.AsyncTransaction) -> bool:
        snapshot = await ref.get(transaction=transaction)
        if not snapshot.exists:
            return False
        decision = await _decide(
            client, prefix, kind, snapshot.to_dict() or {}, now, policy, transaction
        )
        if decision.expire_at is None:
            return False
        transaction.update(ref, {"expire_at": decision.expire_at})
        return True

    return await write(transaction)


def format_reports(reports: list[KindReport], *, apply: bool) -> str:
    lines = [
        f"{'kind':<20}{'examined':>9}{'eligible':>9}{'written':>8}{'excluded':>9}  excluded by",
    ]
    for report in reports:
        reasons = ", ".join(f"{name}: {count}" for name, count in sorted(report.excluded.items()))
        lines.append(
            f"{report.kind:<20}{report.examined:>9}{report.eligible:>9}"
            f"{report.written:>8}{report.excluded_total:>9}  {reasons}"
        )
    lines.append(
        "applied: expire_at was written to the eligible records"
        if apply
        else "dry run: nothing was written (add --apply to write expire_at)"
    )
    return "\n".join(lines)
