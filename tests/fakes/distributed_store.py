from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import uuid4

from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputEnvelope,
    OutputDeliveryState,
    OwnershipToken,
    WorkerRecord,
    validate_state_transition,
)
from silk_agent.core.distributed_ports import (
    CommandStore,
    InboxStore,
    OutboxStore,
    OwnershipStore,
    WorkerDirectory,
)
from tests.fakes.distributed_inbox import FakeInboxStore, make_yielding


class FakeSharedStore(FakeInboxStore):
    """One in-memory backing store shared by any number of worker test fixtures."""

    def __init__(self) -> None:
        super().__init__()
        self.epochs: dict[str, int] = {}
        self.ownership_phases: dict[str, str] = {}
        self.commands: dict[str, dict[str, Any]] = {}
        self.outputs: dict[str, dict[str, Any]] = {}
        self.output_leases: dict[str, tuple[str, datetime]] = {}
        self.workers: dict[str, WorkerRecord] = {}
        self.reservations: dict[tuple[str, str], set[str]] = {}
        self.cluster_draining = False

    async def get(self, conversation_key: str) -> OwnershipToken | None:
        if self.ownership_phases.get(conversation_key) == "released":
            return None
        return self.owners.get(conversation_key)

    async def acquire(
        self,
        *,
        conversation_key: str,
        worker_id: str,
        worker_incarnation: str,
        conversation_generation: int,
        now: datetime,
        lease_expires_at: datetime,
    ) -> OwnershipToken | None:
        existing = self.owners.get(conversation_key)
        if existing is not None and existing.expires_at > now:
            return None
        epoch = self.epochs.get(conversation_key, 0) + 1
        self.epochs[conversation_key] = epoch
        token = OwnershipToken(
            conversation_key,
            worker_id,
            worker_incarnation,
            epoch,
            conversation_generation,
            lease_expires_at,
        )
        self.owners[conversation_key] = token
        self.ownership_phases[conversation_key] = "assigned"
        return token

    async def activate(self, token: OwnershipToken, *, now: datetime) -> bool:
        if (
            self.owners.get(token.conversation_key) != token
            or token.expires_at <= now
            or self.ownership_phases.get(token.conversation_key) != "assigned"
        ):
            return False
        self.ownership_phases[token.conversation_key] = "active"
        return True

    async def renew(
        self, token: OwnershipToken, *, now: datetime, lease_expires_at: datetime
    ) -> OwnershipToken | None:
        if self.owners.get(token.conversation_key) != token or token.expires_at <= now:
            return None
        renewed = OwnershipToken(
            token.conversation_key,
            token.worker_id,
            token.worker_incarnation,
            token.epoch,
            token.conversation_generation,
            lease_expires_at,
        )
        self.owners[token.conversation_key] = renewed
        self.ownership_phases[token.conversation_key] = "active"
        return renewed

    async def release(self, token: OwnershipToken, *, now: datetime) -> bool:
        if (
            self.owners.get(token.conversation_key) != token
            or token.expires_at <= now
            or self.ownership_phases.get(token.conversation_key) not in {"assigned", "active"}
        ):
            return False
        self.ownership_phases[token.conversation_key] = "released"
        return True

    async def create_intent(
        self,
        *,
        command_id: str,
        request_hash: str,
        conversation_key: str,
        conversation_generation: int,
        sandbox_generation: int,
        origin_turn: str,
        origin_epoch: int,
        origin_seq: int,
        owner: OwnershipToken,
        created_at: datetime,
    ) -> bool:
        if not self._owns(owner, conversation_key, conversation_generation):
            return False
        intent = {
            "command_id": command_id,
            "request_hash": request_hash,
            "conversation_key": conversation_key,
            "conversation_generation": conversation_generation,
            "sandbox_generation": sandbox_generation,
            "origin_turn": origin_turn,
            "origin_epoch": origin_epoch,
            "origin_seq": origin_seq,
            "owner": owner,
            "state": CommandExecutionState.INTENT,
            "delivery_mode": "foreground",
            "notification_state": "none",
            "created_at": created_at,
            "next_attempt_at": created_at,
        }
        existing = self.commands.get(command_id)
        if existing is not None:
            return existing["request_hash"] == request_hash
        self.commands[command_id] = intent
        return True

    async def list_due(self, *, now: datetime, limit: int = 100) -> Sequence[Mapping[str, Any]]:
        active = {
            CommandExecutionState.INTENT,
            CommandExecutionState.RUNNING,
            CommandExecutionState.CANCEL_REQUESTED,
        }
        due = [
            record
            for record in self.commands.values()
            if record["state"] in active and record.get("next_attempt_at", now) <= now
        ]
        due.sort(key=lambda record: record.get("next_attempt_at", now))
        return due[:limit]

    async def get_command(self, command_id: str) -> Mapping[str, Any] | None:
        record = self.commands.get(command_id)
        return {**record, "command_id": command_id} if record is not None else None

    async def claim(
        self,
        command_id: str,
        *,
        monitor_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Mapping[str, Any] | None:
        record = self.commands.get(command_id)
        if record is None or record["state"] not in {
            CommandExecutionState.INTENT,
            CommandExecutionState.RUNNING,
            CommandExecutionState.CANCEL_REQUESTED,
        }:
            return None
        if record.get("next_attempt_at", now) > now:
            return None
        if record.get("monitor_lease_expires_at", now) > now:
            return None
        token = uuid4().hex
        record.update(
            monitor_lease_owner=monitor_id,
            monitor_lease_token=token,
            monitor_lease_expires_at=lease_expires_at,
        )
        return {**record, "monitor_lease_token": token}

    async def record_observation(
        self,
        command_id: str,
        *,
        claim_token: str,
        expected: CommandExecutionState,
        observed: CommandExecutionState | None,
        now: datetime,
        retry_at: datetime,
    ) -> bool:
        record = self.commands.get(command_id)
        if (
            record is None
            or record.get("monitor_lease_token") != claim_token
            or record["state"] is not expected
        ):
            return False
        if observed is None or observed is expected:
            return True
        validate_state_transition(expected, observed)
        record.update(
            state=observed,
            updated_at=now,
            next_attempt_at=retry_at,
        )
        for field in ("monitor_lease_owner", "monitor_lease_token", "monitor_lease_expires_at"):
            record.pop(field, None)
        return True

    async def detach(self, command_id: str, *, now: datetime) -> Mapping[str, Any] | None:
        record = self.commands.get(command_id)
        if record is None:
            return None
        if record["state"] in {
            CommandExecutionState.FINISHED,
            CommandExecutionState.CANCELLED,
            CommandExecutionState.UNKNOWN,
        }:
            return {**record, "decision": "return_result"}
        if record.get("delivery_mode", "foreground") == "foreground":
            record.update(delivery_mode="background", detached_at=now, updated_at=now)
        return {**record, "decision": "background"}

    async def orphan(self, command_id: str, *, now: datetime) -> Mapping[str, Any] | None:
        record = self.commands.get(command_id)
        if record is None:
            return None
        if record["state"] in {
            CommandExecutionState.FINISHED,
            CommandExecutionState.CANCELLED,
            CommandExecutionState.UNKNOWN,
        }:
            return {**record, "decision": "return_result"}
        if record.get("delivery_mode", "foreground") == "foreground":
            record.update(delivery_mode="orphaned", orphaned_at=now, updated_at=now)
        return {**record, "decision": record.get("delivery_mode", "foreground")}

    async def request_cancel(
        self, command_id: str, *, reason: str, now: datetime
    ) -> Mapping[str, Any] | None:
        record = self.commands.get(command_id)
        if record is None:
            return None
        if record["state"] in {
            CommandExecutionState.FINISHED,
            CommandExecutionState.CANCELLED,
            CommandExecutionState.UNKNOWN,
        }:
            record["cancel_outcome"] = "too_late"
        else:
            if record["state"] is not CommandExecutionState.CANCEL_REQUESTED:
                validate_state_transition(record["state"], CommandExecutionState.CANCEL_REQUESTED)
                record["state"] = CommandExecutionState.CANCEL_REQUESTED
            record["cancel_outcome"] = record.get("cancel_outcome", "pending")
        record.update(
            cancel_request_id=record.get("cancel_request_id", uuid4().hex),
            cancel_reason=reason,
            cancel_requested_at=record.get("cancel_requested_at", now),
            notification_suppressed=True,
            updated_at=now,
        )
        if record.get("notification_state") == "pending":
            record["notification_state"] = "suppressed"
        return {**record, "command_id": command_id}

    async def record_cancel_result(
        self,
        command_id: str,
        *,
        request_id: str,
        outcome: str,
        now: datetime,
    ) -> bool:
        record = self.commands.get(command_id)
        if (
            record is None
            or record.get("cancel_request_id") != request_id
            or record["state"] is not CommandExecutionState.CANCEL_REQUESTED
        ):
            return False
        record.update(cancel_outcome=outcome, cancel_response_at=now, updated_at=now)
        return True

    async def observe_terminal(
        self,
        command_id: str,
        *,
        claim_token: str,
        expected: CommandExecutionState,
        terminal: CommandExecutionState,
        result: Mapping[str, Any],
        now: datetime,
    ) -> Mapping[str, Any] | None:
        record = self.commands.get(command_id)
        if (
            record is None
            or record.get("monitor_lease_token") != claim_token
            or record["state"] is not expected
        ):
            return None
        validate_state_transition(expected, terminal)
        decision = (
            "suppressed"
            if record.get("notification_suppressed") is True
            else "return_result"
            if record.get("delivery_mode", "foreground") == "foreground"
            else "notify"
        )
        completion_input = None
        if decision == "notify":
            completion_id = f"command-finished:{record['sandbox_generation']}:{command_id}"
            completion_input = await self.accept_input(
                conversation_key=record["conversation_key"],
                input_id=completion_id,
                conversation_generation=record["conversation_generation"],
                kind="command_finished",
                payload={
                    "command_id": command_id,
                    "sandbox_generation": record["sandbox_generation"],
                    "state": terminal.value,
                    "result": dict(result),
                },
                created_at=now,
            )
        record.update(
            state=terminal,
            result=dict(result),
            updated_at=now,
            completed_at=now,
            terminal_delivery=decision,
            notification_state=(
                "pending"
                if decision == "notify"
                else "suppressed"
                if decision == "suppressed"
                else "none"
            ),
            cancel_confirmed_at=now if terminal is CommandExecutionState.CANCELLED else None,
            cancel_outcome=(
                "confirmed"
                if terminal is CommandExecutionState.CANCELLED
                else "not_confirmed"
                if expected is CommandExecutionState.CANCEL_REQUESTED
                else record.get("cancel_outcome")
            ),
        )
        for field in ("monitor_lease_owner", "monitor_lease_token", "monitor_lease_expires_at"):
            record.pop(field, None)
        return {**record, "decision": decision, "completion_input": completion_input}

    async def transition(
        self,
        *,
        command_id: str,
        owner: OwnershipToken,
        expected: CommandExecutionState,
        new_state: CommandExecutionState,
        updated_at: datetime,
    ) -> bool:
        record = self.commands.get(command_id)
        if record is None or not self._owns(
            owner, record["conversation_key"], record["conversation_generation"]
        ):
            return False
        if record["state"] is not expected:
            return False
        validate_state_transition(expected, new_state)
        record["state"] = new_state
        record["updated_at"] = updated_at
        return True

    async def finish_and_enqueue(
        self,
        *,
        command_id: str,
        owner: OwnershipToken,
        expected: CommandExecutionState,
        terminal_state: CommandExecutionState,
        result: Mapping[str, Any],
        completion_input: InputEnvelope,
        completed_at: datetime,
    ) -> bool:
        record = self.commands.get(command_id)
        if record is None or record["state"] is not expected:
            return False
        if not self._owns(owner, record["conversation_key"], record["conversation_generation"]):
            return False
        validate_state_transition(expected, terminal_state)
        input_key = (completion_input.conversation_key, completion_input.input_id)
        existing = self.inputs.get(input_key)
        if existing is not None and existing != completion_input:
            return False
        if existing is None and completion_input.seq != self._next_seq(
            completion_input.conversation_key
        ):
            return False
        record["state"] = terminal_state
        record["result"] = dict(result)
        record["completed_at"] = completed_at
        if existing is None:
            self.inputs[input_key] = completion_input
        return True

    async def put_sanitized(
        self,
        *,
        output_id: str,
        conversation_key: str,
        conversation_generation: int,
        sequence: int,
        owner: OwnershipToken,
        payload: Mapping[str, Any],
        created_at: datetime,
    ) -> bool:
        if not self._owns(owner, conversation_key, conversation_generation):
            return False
        existing = self.outputs.get(output_id)
        if existing is not None:
            return existing["payload"] == dict(payload)
        self.outputs[output_id] = {
            "output_id": output_id,
            "conversation_key": conversation_key,
            "conversation_generation": conversation_generation,
            "sequence": sequence,
            "owner": owner,
            "payload": dict(payload),
            "state": OutputDeliveryState.PENDING,
            "created_at": created_at,
        }
        return True

    async def claim_next(
        self,
        *,
        conversation_key: str,
        sender_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Mapping[str, Any] | None:
        lease = self.output_leases.get(conversation_key)
        if lease is not None and lease[1] > now and lease[0] != sender_id:
            return None
        eligible = sorted(
            (
                output
                for output in self.outputs.values()
                if output["conversation_key"] == conversation_key
                and output["state"] is OutputDeliveryState.PENDING
            ),
            key=lambda output: output["sequence"],
        )
        if not eligible:
            return None
        self.output_leases[conversation_key] = (sender_id, lease_expires_at)
        return dict(eligible[0])

    async def mark_delivery(
        self,
        *,
        output_id: str,
        expected: OutputDeliveryState,
        new_state: OutputDeliveryState,
        attempt_id: str,
        slack_ref: Mapping[str, str] | None = None,
        updated_at: datetime,
    ) -> bool:
        output = self.outputs.get(output_id)
        if output is None or output["state"] is not expected:
            return False
        validate_state_transition(expected, new_state)
        output["state"] = new_state
        output["attempt_id"] = attempt_id
        output["slack_ref"] = dict(slack_ref) if slack_ref is not None else None
        output["updated_at"] = updated_at
        return True

    async def register(self, record: WorkerRecord) -> bool:
        existing = self.workers.get(record.worker_id)
        if existing is not None and existing.incarnation == record.incarnation:
            return False
        self.workers[record.worker_id] = record
        self.reservations[(record.worker_id, record.incarnation)] = set()
        return True

    async def heartbeat(self, record: WorkerRecord) -> bool:
        existing = self.workers.get(record.worker_id)
        if existing is None or existing.incarnation != record.incarnation:
            return False
        if record.heartbeat_at < existing.heartbeat_at:
            return False
        self.workers[record.worker_id] = WorkerRecord(
            worker_id=record.worker_id,
            incarnation=record.incarnation,
            capacity=record.capacity,
            reserved=existing.reserved,
            active_sdk=record.active_sdk,
            draining=existing.draining,
            heartbeat_at=record.heartbeat_at,
            drain_deadline_at=existing.drain_deadline_at,
        )
        return True

    async def list_available(self, *, now: datetime) -> Sequence[WorkerRecord]:
        return tuple(
            record
            for record in self.workers.values()
            if not record.draining
            and record.heartbeat_at <= now
            and record.reserved + len(self.reservations[(record.worker_id, record.incarnation)])
            < record.capacity
        )

    async def reserve(self, *, worker_id: str, incarnation: str, reservation_id: str) -> bool:
        record = self.workers.get(worker_id)
        key = (worker_id, incarnation)
        reservations = self.reservations.get(key)
        if (
            record is None
            or record.incarnation != incarnation
            or record.draining
            or reservations is None
        ):
            return False
        if reservation_id in reservations:
            return True
        if record.reserved + len(reservations) >= record.capacity:
            return False
        reservations.add(reservation_id)
        return True

    async def release_reservation(
        self, *, worker_id: str, incarnation: str, reservation_id: str
    ) -> bool:
        reservations = self.reservations.get((worker_id, incarnation))
        if reservations is None or reservation_id not in reservations:
            return False
        reservations.remove(reservation_id)
        return True

    async def get_worker(self, worker_id: str) -> WorkerRecord | None:
        return self.workers.get(worker_id)

    async def list_workers(self) -> Sequence[WorkerRecord]:
        return tuple(self.workers.values())

    async def set_cluster_draining(self, draining: bool) -> None:
        self.cluster_draining = draining

    async def is_cluster_draining(self) -> bool:
        return self.cluster_draining

    async def set_draining(
        self,
        *,
        worker_id: str,
        incarnation: str,
        draining: bool,
        now: datetime,
        drain_deadline_at: datetime | None = None,
    ) -> WorkerRecord | None:
        current = self.workers.get(worker_id)
        if current is None or current.incarnation != incarnation:
            return None
        updated = WorkerRecord(
            worker_id=current.worker_id,
            incarnation=current.incarnation,
            capacity=current.capacity,
            reserved=current.reserved,
            active_sdk=current.active_sdk,
            draining=draining,
            heartbeat_at=current.heartbeat_at,
            drain_deadline_at=drain_deadline_at if draining else None,
        )
        self.workers[worker_id] = updated
        return updated

    def _owns(self, token: OwnershipToken, conversation_key: str, generation: int) -> bool:
        return (
            token.conversation_key == conversation_key
            and token.conversation_generation == generation
            and self.owners.get(conversation_key) == token
        )


def as_inbox_store(store: FakeSharedStore) -> InboxStore:
    return store


def as_ownership_store(store: FakeSharedStore) -> OwnershipStore:
    return store


def as_command_store(store: FakeSharedStore) -> CommandStore:
    return store


def as_outbox_store(store: FakeSharedStore) -> OutboxStore:
    return store


def as_worker_directory(store: FakeSharedStore) -> WorkerDirectory:
    return store


make_yielding(FakeSharedStore)
