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
from silk_agent.distributed.completion import CompletionInputDelivery
from silk_agent.distributed.dispatcher import ConversationDispatcher
from silk_agent.distributed.input_delivery import DurableInputDelivery
from silk_agent.distributed.metrics import METRICS

log = logging.getLogger(__name__)
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
    ) -> None:
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
        )
        self._session = await registry.get_or_create(self._owner.conversation_key)
        self._delivery = DurableInputDelivery(self._inbox, self._owner, self._session)
        self._completion = CompletionInputDelivery(
            self._inbox,
            self._owner,
            self._session,
            command_monitor=self._command_monitor,
        )
        self._dispatcher = ConversationDispatcher(
            read_inputs=self._read_inputs,
            read_controls=self._read_controls,
            prepare_input=self._prepare_input,
            dispatch_input=self._dispatch_input,
            handle_control=self._handle_control,
            dispatch_completion=self._dispatch_completion,
            run_turns=self._wait_for_stop,
            poll_seconds=self._poll_seconds,
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
            self._delivery = DurableInputDelivery(self._inbox, self._owner, self._session)
            self._completion = CompletionInputDelivery(
                self._inbox,
                self._owner,
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
        await self._orchestrator.registry.aclose()

    async def _read_inputs(self) -> Sequence[InputEnvelope]:
        if self._control_only:
            # No SDK slot was reserved; user input waits for a normal assignment.
            return ()
        return await self._inbox.list_unfinished(self._owner.conversation_key, limit=100)

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
        self._origin_seq = max(self._origin_seq, envelope.seq)
        METRICS.observe(
            "input_dispatch_latency_seconds",
            max(0.0, (datetime.now(UTC) - envelope.created_at).total_seconds()),
        )
        if not allowed:
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
        await self._delivery.dispatch(envelope, message)

    async def _dispatch_completion(self, envelope: InputEnvelope) -> None:
        if self._completion is None:
            raise RuntimeError("conversation actor has not started")
        self._origin_seq = max(self._origin_seq, envelope.seq)
        await self._completion.dispatch(envelope)

    async def _wait_for_stop(self, stop: asyncio.Event) -> None:
        await stop.wait()

    async def _mark_completed(self, input_id: str) -> None:
        if self._delivery is not None:
            await self._delivery.complete(input_id)

    async def _mark_started(self, input_id: str) -> bool:
        if self._delivery is not None:
            return await self._delivery.mark_started(input_id)
        return False

    async def _mark_uncertain(self, input_id: str) -> None:
        METRICS.inc("inputs_uncertain_total")
        if self._delivery is not None:
            await self._delivery.mark_uncertain(input_id)


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
