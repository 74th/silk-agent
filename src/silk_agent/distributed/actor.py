"""Conversation actor joining durable input dispatch with the existing AgentSession."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from silk_agent.core.distributed_models import InputDeliveryState, InputEnvelope, OwnershipToken
from silk_agent.core.distributed_ports import CommandMonitorStore, InboxStore
from silk_agent.core.events import SenderInfo, UserMessage
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.session import AgentSession
from silk_agent.distributed.completion import (
    CompletionInputDelivery,
    release_held_completions,
)
from silk_agent.distributed.dispatcher import ConversationDispatcher, DispatchRetryable
from silk_agent.distributed.input_delivery import DurableInputDelivery
from silk_agent.distributed.metrics import METRICS

log = logging.getLogger(__name__)
INPUT_PAGE = 100
_GATE_RECHECK_SECONDS = 0.5
_DELIVERABLE = (InputDeliveryState.PENDING, InputDeliveryState.QUEUED)


ControlReader = Callable[[], Awaitable[Sequence[Mapping[str, Any]]]]
ControlHandler = Callable[[Mapping[str, Any]], Awaitable[None]]


class DistributedConversationActor:
    """Run one durable inbox reader and one existing AgentSession for an owner token.

    Control reads and handling are injected because stop/reset requires the Control role's
    authorization and command cancellation dependencies. A Worker cannot silently invent
    those policies.
    """

    def __init__(
        self,
        owner: OwnershipToken,
        inbox: InboxStore,
        orchestrator: ConversationOrchestrator,
        *,
        read_controls: ControlReader,
        handle_control: ControlHandler,
        command_monitor: CommandMonitorStore | None = None,
        poll_seconds: float = 1,
        control_only: bool = False,
        read_stop_seq: Callable[[], Awaitable[int]] | None = None,
        stop_gate_seconds: float = 60.0,
    ) -> None:
        self._read_stop_seq = read_stop_seq
        self._stop_gate_seconds = stop_gate_seconds
        self._input_seqs: dict[str, int] = {}
        self._stop_applied = asyncio.Event()
        self._gate_gave_up: set[str] = set()
        self._control_only = control_only
        self._owner = owner
        self._inbox = inbox
        self._orchestrator = orchestrator
        self._read_controls = read_controls
        self._handle_control = handle_control
        self._command_monitor = command_monitor
        self._poll_seconds = poll_seconds
        self._session: AgentSession | None = None
        self._delivery: DurableInputDelivery | None = None
        self._completion: CompletionInputDelivery | None = None
        self._dispatcher: ConversationDispatcher[Any, tuple[UserMessage, bool]] | None = None
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._origin_seq = 0
        self._background: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._closed:
            raise RuntimeError("conversation actor is closed")
        registry = self._orchestrator.registry
        registry.set_input_tracking_callbacks(
            completed=self._mark_completed,
            started=self._mark_started,
            uncertain=self._mark_uncertain,
            interrupted=self._mark_interrupted,
            dropped=self._on_inputs_dropped,
        )
        self._session = await registry.get_or_create(self._owner.conversation_key)
        self._delivery = DurableInputDelivery(self._inbox, lambda: self._owner, self._session)
        self._completion = CompletionInputDelivery(
            self._inbox,
            lambda: self._owner,
            self._session,
            command_monitor=self._command_monitor,
        )
        await self._settle_leftovers()
        self._dispatcher = ConversationDispatcher(
            read_inputs=self._read_inputs,
            read_controls=self._read_controls,
            prepare_input=self._prepare_input,
            dispatch_input=self._dispatch_input,
            handle_control=self._handle_control_and_signal,
            dispatch_completion=self._dispatch_completion,
            run_turns=self._wait_for_stop,
            poll_seconds=self._poll_seconds,
            read_limit=INPUT_PAGE,
            still_unfinished=self._still_unfinished,
            refresh_input=self._refresh_input,
            abandon_input=self._abandon_input,
        )
        self._task = asyncio.create_task(self._dispatcher.run(), name="distributed-conversation")
        await asyncio.sleep(0)

    @property
    def owner(self) -> OwnershipToken:
        return self._owner

    @property
    def session(self) -> AgentSession | None:
        return self._session

    def is_idle(self) -> bool:
        return self._session is None or self._session.state == "idle"

    def identity(self) -> dict[str, Any]:
        """Owner context that the Sandbox Service checks for every mutating operation.

        ``origin_seq`` is the newest input handed to this conversation. A stop barrier that
        covers it rejects the old turn's operations, while a later input starts past it.
        """
        turn_id = self._session.turn_id if self._session is not None else None
        return {
            "owner_epoch": self._owner.epoch,
            "conversation_generation": self._owner.conversation_generation,
            "turn_id": turn_id or "",
            "origin_seq": max(self._origin_seq, 1),
        }

    async def rebind_after_reset(self, token: OwnershipToken) -> None:
        """Adopt the next epoch/generation produced by this actor's own reset."""
        if token == self._owner:
            return
        if (
            token.conversation_key != self._owner.conversation_key
            or token.worker_id != self._owner.worker_id
            or token.worker_incarnation != self._owner.worker_incarnation
            or token.epoch <= self._owner.epoch
            or token.conversation_generation <= self._owner.conversation_generation
        ):
            raise ValueError("reset successor does not belong to this actor")
        self._owner = token
        self._refresh_delivery()
        # Inputs from the previous generation have been rebound by the reset transaction.
        self._origin_seq = 0

    def _refresh_delivery(self) -> None:
        if self._delivery is not None and self._session is not None:
            self._delivery = DurableInputDelivery(self._inbox, lambda: self._owner, self._session)
            self._completion = CompletionInputDelivery(
                self._inbox,
                lambda: self._owner,
                self._session,
                command_monitor=self._command_monitor,
            )

    async def update_ownership(self, token: OwnershipToken) -> None:
        if (
            token.conversation_key != self._owner.conversation_key
            or token.worker_id != self._owner.worker_id
            or token.worker_incarnation != self._owner.worker_incarnation
            or token.epoch < self._owner.epoch
            or token.conversation_generation != self._owner.conversation_generation
        ):
            raise ValueError("ownership update does not belong to this actor")
        self._owner = token
        self._refresh_delivery()

    async def drain(self, timeout_seconds: float | None) -> bool:
        """Let the running turn finish (or interrupt it at the deadline), then stop locally.

        Remote commands keep running (the session orphans them). Raising leaves ownership
        held, as the caller treats an unconfirmed stop as not drained.
        """
        if self._session is not None:
            await self._session.shutdown(timeout_seconds, reason="drain")
        await self.aclose()
        return True

    async def aclose(self) -> None:
        self._closed = True
        if self._dispatcher is not None:
            self._dispatcher.stop()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        for task in tuple(self._background):
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        await self._orchestrator.registry.aclose()

    async def _read_inputs(self) -> Sequence[InputEnvelope]:
        if self._control_only:
            # No SDK slot was reserved; user input waits for a normal assignment.
            return ()
        records = await self._inbox.list_unfinished(
            self._owner.conversation_key, limit=INPUT_PAGE, states=_DELIVERABLE
        )
        # Gateway persists and ACKs first, then resolves the sender (users.info). A message
        # whose sender is not stored yet must not be judged: policy and the conversation
        # owner depend on it, and a later input must not overtake it.
        ready = []
        for record in sorted(records, key=lambda item: item.seq):
            if record.kind == "message" and "sender" not in record.payload:
                break
            ready.append(record)
        return ready

    async def _prepare_input(self, envelope: InputEnvelope) -> tuple[UserMessage, bool]:
        message = _message_from_envelope(envelope)
        # Guard/rate-limit history was committed before a prior queue transition. A queued
        # item must be resumed without repeating those side effects after Worker takeover.
        allowed = (
            True
            if envelope.delivery_state is InputDeliveryState.QUEUED
            else await self._orchestrator.prepare_message(message)
        )
        return message, allowed

    async def _dispatch_input(
        self, envelope: InputEnvelope, prepared: tuple[UserMessage, bool]
    ) -> None:
        message, allowed = prepared
        await self._wait_for_pending_stop(envelope.seq)
        self._input_seqs[envelope.input_id] = envelope.seq
        self._origin_seq = max(self._origin_seq, envelope.seq)
        METRICS.observe(
            "input_dispatch_latency_seconds",
            max(0.0, (datetime.now(UTC) - envelope.created_at).total_seconds()),
        )
        if not allowed:
            self._input_seqs.pop(envelope.input_id, None)
            if envelope.delivery_state is not InputDeliveryState.PENDING:
                return
            await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=envelope.delivery_state,
                new_state=InputDeliveryState.BLOCKED,
            )
            return
        if self._delivery is None:
            raise RuntimeError("conversation actor has not started")
        outcome = await self._delivery.dispatch(envelope, message)
        if outcome == "rejected":
            # Nothing was sent (state is back to pending): try again instead of leaving the
            # input stranded behind a dispatcher that considers it handled.
            log.warning("input dispatch was rejected; will retry")
            raise DispatchRetryable()
        if outcome == "fenced":
            # The SDK already has it; the delivery marked it uncertain. Never retry.
            log.warning("input reached the SDK but its accepted record was fenced")
            self._input_seqs.pop(envelope.input_id, None)
            return
        if envelope.kind == "message":
            # The user is back: hand over command results kept while the turn was interrupted.
            # Run it on the actor's own task, not this input's: once the reply completes, the
            # input leaves the unfinished set and its dispatcher task is cancelled.
            self._spawn(self._release_held())

    def _spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine, name="distributed-actor-background")
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _release_held(self) -> None:
        try:
            released = await release_held_completions(
                self._inbox,
                self._owner,
                command_monitor=self._command_monitor,
                now=datetime.now(UTC),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Each result stays held (idempotent) and is released by the next user message.
            log.warning("could not release held results (%s)", type(exc).__name__)
            return
        if released and self._dispatcher is not None:
            self._dispatcher.wake_inputs()

    async def _refresh_input(self, record: InputEnvelope) -> InputEnvelope | None:
        """The stored input with this seq if it can still be delivered, else None."""
        latest = await self._inbox.list_unfinished(
            self._owner.conversation_key,
            after_seq=record.seq - 1,
            limit=1,
            states=_DELIVERABLE,
        )
        return latest[0] if latest and latest[0].seq == record.seq else None

    async def _still_unfinished(self, seq: int) -> bool:
        """Unfinished in ANY state: an input being handed to the SDK (dispatching) or already
        accepted is not cancelled, just no longer deliverable."""
        latest = await self._inbox.list_unfinished(
            self._owner.conversation_key, after_seq=seq - 1, limit=1
        )
        return bool(latest) and latest[0].seq == seq

    async def _abandon_input(self, record: InputEnvelope) -> None:
        """Give up on an input whose preparation never succeeds, so it cannot block the rest."""
        terminal = (
            InputDeliveryState.BLOCKED
            if record.delivery_state is InputDeliveryState.PENDING
            else InputDeliveryState.CANCELLED
        )
        await self._inbox.advance_input(
            conversation_key=record.conversation_key,
            input_id=record.input_id,
            owner=self._owner,
            expected=record.delivery_state,
            new_state=terminal,
        )
        self._input_seqs.pop(record.input_id, None)
        METRICS.inc("inputs_abandoned_total")

    async def _settle_leftovers(self) -> None:
        """A new actor has nothing in flight: inputs an earlier owner left as dispatching or
        accepted can no longer be resolved by a turn. Mark them uncertain (never resent) so
        they do not hold the conversation's slot or fill the inbox page."""
        leftovers = await self._inbox.list_unfinished(
            self._owner.conversation_key,
            limit=500,
            states=(InputDeliveryState.DISPATCHING, InputDeliveryState.ACCEPTED),
        )
        for record in leftovers:
            await self._inbox.advance_input(
                conversation_key=record.conversation_key,
                input_id=record.input_id,
                owner=self._owner,
                expected=record.delivery_state,
                new_state=InputDeliveryState.UNCERTAIN,
            )
        if leftovers:
            METRICS.inc("inputs_uncertain_total", len(leftovers))

    async def _wait_for_pending_stop(self, seq: int) -> None:
        """Do not hand an input to a turn that an authorized stop is about to interrupt.

        Control commits ``stop_seq`` when it authorizes the stop, but this Worker applies it up
        to a second or two later. An input that arrives in between has a later sequence than the
        stop and must run after it: appending it to the turn being stopped would lose it.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._stop_gate_seconds
        while True:
            controls = await self._inbox.list_authorized_controls(self._owner.conversation_key)
            pending = [
                str(control.get("input_id"))
                for control in controls
                if control.get("kind") in {"stop_candidate", "reset_candidate"}
                and isinstance(control.get("seq"), int)
                and control["seq"] < seq
                and str(control.get("input_id")) not in self._gate_gave_up
            ]
            if not pending:
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                # Remember it: every later input would otherwise wait the full time again for a
                # control that cannot be applied.
                self._gate_gave_up.update(pending)
                log.warning("a stop is still being applied; handing the input over anyway")
                return
            self._stop_applied.clear()
            try:
                await asyncio.wait_for(
                    self._stop_applied.wait(), timeout=min(_GATE_RECHECK_SECONDS, remaining)
                )
            except TimeoutError:
                pass

    async def _handle_control_and_signal(self, control: Any) -> None:
        await self._handle_control(control)
        self._stop_applied.set()  # wakes inputs waiting for a stop to be applied

    async def _mark_interrupted(self, input_id: str, reason: str) -> None:
        if self._delivery is None:
            return
        cancel: bool | None = None
        if reason in {"user", "reset"} and self._read_stop_seq is not None:
            # Only inputs the stop covers are cancelled. One behind the boundary reached this
            # turn in the window before the stop was applied: its fate is unknown, not "cancelled".
            seq = self._input_seqs.get(input_id)
            try:
                stop_seq = await self._read_stop_seq()
            except Exception as exc:
                log.warning("could not read the stop boundary (%s)", type(exc).__name__)
                stop_seq = None
            cancel = seq is not None and stop_seq is not None and seq <= stop_seq
        await self._delivery.interrupted(input_id, reason, cancel=cancel)
        self._input_seqs.pop(input_id, None)

    async def _on_inputs_dropped(self, input_ids: list[str]) -> None:
        for input_id in input_ids:
            self._input_seqs.pop(input_id, None)
        if self._dispatcher is not None:
            self._dispatcher.forget_inputs(input_ids)

    async def _dispatch_completion(self, envelope: InputEnvelope) -> None:
        if self._completion is None:
            raise RuntimeError("conversation actor has not started")
        await self._wait_for_pending_stop(envelope.seq)
        self._input_seqs[envelope.input_id] = envelope.seq
        self._origin_seq = max(self._origin_seq, envelope.seq)
        await self._completion.dispatch(envelope)

    async def _wait_for_stop(self, stop: asyncio.Event) -> None:
        await stop.wait()

    async def _mark_completed(self, input_id: str) -> None:
        if self._delivery is not None:
            await self._delivery.complete(input_id)
        self._input_seqs.pop(input_id, None)

    async def _mark_started(self, input_id: str) -> bool:
        if self._delivery is not None:
            return await self._delivery.mark_started(input_id)
        return False

    async def _mark_uncertain(self, input_id: str) -> None:
        METRICS.inc("inputs_uncertain_total")
        if self._delivery is not None:
            await self._delivery.mark_uncertain(input_id)
        self._input_seqs.pop(input_id, None)


def _message_from_envelope(envelope: InputEnvelope) -> UserMessage:
    payload = envelope.payload
    text = payload.get("text")
    user = payload.get("user")
    if not isinstance(text, str) or not isinstance(user, str):
        raise ValueError("stored user input is missing text or sender ID")
    raw_sender = payload.get("sender")
    sender = None
    if isinstance(raw_sender, Mapping):
        sender = SenderInfo(
            email=str(raw_sender.get("email") or ""),
            is_guest=raw_sender.get("is_guest")
            if isinstance(raw_sender.get("is_guest"), bool)
            else None,
            is_external=raw_sender.get("is_external")
            if isinstance(raw_sender.get("is_external"), bool)
            else None,
            team=str(raw_sender.get("team") or ""),
        )
    return UserMessage(
        conversation_key=envelope.conversation_key,
        user=user,
        text=text,
        message_id=str(payload.get("message_id") or ""),
        attachments=tuple(item for item in payload.get("attachments", ()) if isinstance(item, str)),
        team=str(payload.get("team") or ""),
        channel=str(payload.get("channel") or ""),
        sender_is_bot=payload.get("sender_is_bot") is True,
        is_direct=payload.get("is_direct") is True,
        sender=sender,
        kind=str(payload.get("kind") or "message"),
        input_id=envelope.input_id,
    )
