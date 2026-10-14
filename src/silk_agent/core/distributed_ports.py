"""Atomic storage contracts used by distributed workers and control processes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from silk_agent.core.distributed_models import (
    CommandExecutionState,
    ControlEnvelope,
    InputDeliveryState,
    InputEnvelope,
    OutputDeliveryState,
    OwnershipToken,
    WorkerRecord,
)


@runtime_checkable
class InboxStore(Protocol):
    """Durable input acceptance and fenced input state transitions."""

    async def accept_input(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        payload: Mapping[str, Any],
        created_at: datetime,
    ) -> InputEnvelope:
        """Allocate the next per-conversation sequence and persist the input atomically."""
        ...

    async def accept_control(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        sender_id: str,
        payload: Mapping[str, Any],
        created_at: datetime,
    ) -> ControlEnvelope:
        """Allocate sequence and persist a control request using the same ordering domain."""
        ...

    async def accept_received(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        payload: Mapping[str, Any],
        created_at: datetime,
        workspace: str,
        team: str,
        channel: str,
        message_ts: str,
        event_id: str,
        sender_id: str | None = None,
        participating: bool = False,
    ) -> InputEnvelope | ControlEnvelope:
        """Persist an event receipt, message receipt, and envelope as one operation."""
        ...

    async def list_unfinished(
        self, conversation_key: str, *, after_seq: int = 0, limit: int = 100
    ) -> Sequence[InputEnvelope]:
        """Read durable work in sequence order after startup or watch reconnection."""
        ...

    async def advance_input(
        self,
        *,
        conversation_key: str,
        input_id: str,
        owner: OwnershipToken,
        expected: InputDeliveryState,
        new_state: InputDeliveryState,
        dispatch_attempt_id: str | None = None,
    ) -> bool:
        """Compare state and ownership epoch before committing a transition."""
        ...

    async def rebind_after_reset(
        self,
        *,
        conversation_key: str,
        previous_generation: int,
        reset_seq: int,
        owner: OwnershipToken,
    ) -> int:
        """Move only undispatched inputs after reset into the new conversation generation."""
        ...

    async def recover_dispatching(
        self,
        *,
        conversation_key: str,
        input_id: str,
        previous_owner_epoch: int,
        dispatch_attempt_id: str,
    ) -> bool:
        """Classify an unfinished prior attempt as uncertain instead of resending it."""
        ...

    async def record_query_start(
        self,
        *,
        conversation_key: str,
        input_id: str,
        owner: OwnershipToken,
        dispatch_attempt_id: str,
        sdk_session_id: str,
        now: datetime,
    ) -> bool:
        """Persist SDK session and input correlation before the query reaches the SDK."""
        ...

    async def list_sender_enrichment_due(
        self, *, now: datetime, limit: int = 100
    ) -> Sequence[Mapping[str, Any]]: ...

    async def record_sender_enrichment(
        self,
        *,
        conversation_key: str,
        input_id: str,
        record_collection: str,
        sender_info: Mapping[str, Any] | None,
        retry_at: datetime,
        now: datetime,
    ) -> bool: ...

    async def list_pending_controls(self, *, limit: int = 100) -> Sequence[Mapping[str, Any]]: ...

    async def list_authorized_controls(
        self, conversation_key: str, *, limit: int = 100
    ) -> Sequence[Mapping[str, Any]]: ...

    async def mark_control_handled(
        self, *, conversation_key: str, input_id: str, owner: OwnershipToken, now: datetime
    ) -> bool: ...

    async def resolve_control_authorization(
        self,
        *,
        conversation_key: str,
        input_id: str,
        authorized: bool,
        now: datetime,
    ) -> bool: ...


@runtime_checkable
class OwnershipStore(Protocol):
    """Conversation ownership operations with monotonic epochs and lease fencing."""

    async def get(self, conversation_key: str) -> OwnershipToken | None: ...

    async def list_for_worker(
        self, *, worker_id: str, worker_incarnation: str
    ) -> Sequence[OwnershipToken]:
        """Return assignments for this exact process incarnation."""
        ...

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
        """Atomically acquire unowned work and increment its fencing epoch."""
        ...

    async def activate(self, token: OwnershipToken, *, now: datetime) -> bool:
        """Move an unexpired assignment to active before creating its SDK actor."""
        ...

    async def activate_control(self, token: OwnershipToken, *, now: datetime) -> bool:
        """Activate control-only ownership without consuming an SDK slot."""
        ...

    async def renew(
        self, token: OwnershipToken, *, now: datetime, lease_expires_at: datetime
    ) -> OwnershipToken | None:
        """Renew only if worker incarnation and epoch still match and remain valid."""
        ...

    async def release(self, token: OwnershipToken, *, now: datetime) -> bool:
        """Release only the exact incarnation and epoch represented by the token."""
        ...

    async def begin_recovery(self, token: OwnershipToken, *, now: datetime) -> bool:
        """Fence expired ownership while retaining its reservation pending stop proof."""
        ...

    async def mark_recovery_blocked(
        self, token: OwnershipToken, *, reason: str, now: datetime
    ) -> bool: ...

    async def confirm_stopped(
        self,
        token: OwnershipToken,
        *,
        evidence_id: str,
        worker_incarnation: str,
        now: datetime,
    ) -> bool:
        """Release expired capacity only after the old Worker confirms SDK closure."""
        ...


@runtime_checkable
class ConversationStore(Protocol):
    """Partial conversation metadata updates protected by version and ownership."""

    async def get_conversation(self, conversation_key: str) -> Mapping[str, Any] | None: ...

    async def commit_turn_state(
        self,
        *,
        owner: OwnershipToken,
        expected_version: int,
        patch: Mapping[str, Any],
    ) -> int | None:
        """Apply only named management fields without replacing concurrent metadata."""
        ...

    async def set_initial_owner_once(
        self,
        *,
        owner: OwnershipToken,
        owner_user: str,
        owner_email: str | None,
    ) -> bool:
        """Set the initiating user only when it has never been recorded."""
        ...

    async def load_guard_history(self, conversation_key: str) -> Sequence[str]: ...

    async def append_guard_history(
        self, *, owner: OwnershipToken, text: str, limit: int = 5
    ) -> bool: ...

    async def begin_reset(self, *, owner: OwnershipToken, reset_seq: int) -> bool: ...

    async def allocate_output_sequence(self, conversation_key: str) -> int: ...

    async def complete_reset(
        self, *, owner: OwnershipToken, now: datetime
    ) -> OwnershipToken | None:
        """Advance generation and fencing epoch after the old turn has stopped."""
        ...


@runtime_checkable
class UserRateLimitStore(Protocol):
    async def allow_and_record(
        self, *, user_key: str, input_id: str, now: datetime, per_minute: int
    ) -> bool: ...


@runtime_checkable
class CommandStore(Protocol):
    """Command ledger operations; state, cancellation, and notification are distinct."""

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
        """Persist a stable command intent before any external start request."""
        ...

    async def transition(
        self,
        *,
        command_id: str,
        owner: OwnershipToken,
        expected: CommandExecutionState,
        new_state: CommandExecutionState,
        updated_at: datetime,
    ) -> bool:
        """Compare execution state and owner epoch in one atomic update."""
        ...

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
        """Commit terminal command state and its completion input together."""
        ...


@runtime_checkable
class CommandMonitorStore(Protocol):
    """Leased work discovery for independent command monitors."""

    async def get_command(self, command_id: str) -> Mapping[str, Any] | None: ...

    async def set_notification_state(
        self, command_id: str, *, expected: str, new_state: str, now: datetime
    ) -> bool: ...

    async def list_due(self, *, now: datetime, limit: int = 100) -> Sequence[Mapping[str, Any]]: ...

    async def list_for_stop(
        self,
        *,
        conversation_key: str,
        conversation_generation: int,
        stop_seq: int,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[Sequence[Mapping[str, Any]], str | None]: ...

    async def claim(
        self,
        command_id: str,
        *,
        monitor_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Mapping[str, Any] | None:
        """Claim a due nonterminal command and return a unique fencing token."""
        ...

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
        """Persist only a changed state, fenced by the current monitor claim."""
        ...

    async def detach(self, command_id: str, *, now: datetime) -> Mapping[str, Any] | None:
        """Atomically choose a foreground result or background delivery mode."""
        ...

    async def orphan(self, command_id: str, *, now: datetime) -> Mapping[str, Any] | None:
        """Mark a foreground command as orphaned after its Worker is lost."""
        ...

    async def request_cancel(
        self, command_id: str, *, reason: str, now: datetime
    ) -> Mapping[str, Any] | None:
        """Persist cancellation intent and suppress a later normal completion notice."""
        ...

    async def record_cancel_result(
        self,
        command_id: str,
        *,
        request_id: str,
        outcome: str,
        now: datetime,
    ) -> bool:
        """Record accepted/unknown/rejected remote cancel response without claiming stop."""
        ...

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
        """Save terminal state and decide whether it returns or becomes a notice."""
        ...


@runtime_checkable
class ServiceOperationStore(Protocol):
    """Sandbox mutation intents accepted atomically with ownership and stop barriers."""

    async def accept_intent(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        request_hash: str,
        conversation_key: str,
        conversation_generation: int,
        sandbox_generation: int,
        sandbox_key_hash: str | None = None,
        turn_id: str,
        origin_seq: int,
        owner: OwnershipToken,
        payload: Mapping[str, Any],
        now: datetime,
        created_at: datetime,
    ) -> bool:
        """Return true only when a new intent commits before external mutation begins."""
        ...

    async def get_operation(self, operation_id: str) -> Mapping[str, Any] | None: ...

    async def is_dispatch_allowed(
        self, operation_id: str, owner: OwnershipToken, *, now: datetime
    ) -> bool:
        """Recheck owner and stop barriers immediately before an external mutation."""
        ...

    async def list_for_stop(
        self,
        *,
        conversation_key: str,
        conversation_generation: int,
        stop_seq: int,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[Sequence[Mapping[str, Any]], str | None]:
        """Page accepted intents at or before a stop barrier for cancellation/recovery."""
        ...


@runtime_checkable
class OutboxStore(Protocol):
    """Sanitized output state and delivery claims."""

    async def put_sanitized(
        self,
        *,
        output_id: str,
        conversation_key: str,
        conversation_generation: int,
        sequence: int,
        origin_seq: int | None = None,
        owner: OwnershipToken,
        payload: Mapping[str, Any],
        now: datetime,
        created_at: datetime,
    ) -> bool:
        """Persist output only after sanitization and while its owner epoch is current."""
        ...

    async def list_conversations_with_pending(self, *, limit: int = 100) -> Sequence[str]: ...

    async def claim_next(
        self,
        *,
        conversation_key: str,
        sender_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Mapping[str, Any] | None:
        """Claim the earliest eligible output for this conversation."""
        ...

    async def mark_delivery(
        self,
        *,
        output_id: str,
        expected: OutputDeliveryState,
        new_state: OutputDeliveryState,
        attempt_id: str,
        slack_ref: Mapping[str, str] | None = None,
        next_attempt_at: datetime | None = None,
        updated_at: datetime,
    ) -> bool:
        """Record a compare-and-set delivery result and its external reference."""
        ...

    async def revalidate_claim(self, *, output_id: str, attempt_id: str, now: datetime) -> bool:
        """Recheck generation and stop barriers immediately before Slack delivery."""
        ...


@runtime_checkable
class WorkerDirectory(Protocol):
    """Worker liveness and capacity reservation operations."""

    async def register(self, record: WorkerRecord) -> bool:
        """Register a new incarnation without allowing an older heartbeat to replace it."""
        ...

    async def heartbeat(self, record: WorkerRecord) -> bool:
        """Update liveness and usage only for the currently registered incarnation."""
        ...

    async def list_available(self, *, now: datetime) -> Sequence[WorkerRecord]:
        """List live, non-draining workers with available capacity."""
        ...

    async def reserve(self, *, worker_id: str, incarnation: str, reservation_id: str) -> bool:
        """Atomically reserve one capacity slot if the worker has room and is not draining."""
        ...

    async def release_reservation(
        self, *, worker_id: str, incarnation: str, reservation_id: str
    ) -> bool:
        """Release only the named reservation from the matching incarnation."""
        ...

    async def set_draining(
        self,
        *,
        worker_id: str,
        incarnation: str,
        draining: bool,
        now: datetime,
        drain_deadline_at: datetime | None = None,
    ) -> WorkerRecord | None:
        """Set drain admission state without changing reservations or active SDK count."""
        ...

    async def get_worker(self, worker_id: str) -> WorkerRecord | None:
        """Return the currently registered Worker incarnation."""
        ...

    async def list_workers(self) -> Sequence[WorkerRecord]:
        """List the currently registered Worker incarnations for administration."""
        ...

    async def set_cluster_draining(self, draining: bool) -> None:
        """Pause or resume all Worker allocations, including future incarnations."""
        ...

    async def is_cluster_draining(self) -> bool:
        """Return whether global Worker allocation is paused."""
        ...
