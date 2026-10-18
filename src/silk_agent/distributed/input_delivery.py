"""Fence AgentSession handoff to the durable inbox delivery state."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import Literal, Protocol

from silk_agent.core.distributed_models import InputDeliveryState, InputEnvelope, OwnershipToken
from silk_agent.core.distributed_ports import InboxStore
from silk_agent.core.events import UserMessage
from silk_agent.core.session import InputQueue, SubmissionOutcome


class SessionSubmitter(Protocol):
    async def submit(
        self, message: UserMessage, *, before_queue: InputQueue | None = None
    ) -> SubmissionOutcome: ...


class DurableInputDelivery:
    """Persist dispatch intent before queue/append and preserve uncertain outcomes."""

    def __init__(
        self,
        inbox: InboxStore,
        owner: OwnershipToken | Callable[[], OwnershipToken],
        session: SessionSubmitter,
    ):
        self._inbox = inbox
        self._owner_source = owner
        self._session = session

    @property
    def _owner(self) -> OwnershipToken:
        """The current token: a lease renewal replaces it while an operation is in flight."""
        source = self._owner_source
        return source() if callable(source) else source

    async def dispatch(
        self, envelope: InputEnvelope, message: UserMessage
    ) -> SubmissionOutcome | Literal["fenced"]:
        if envelope.delivery_state not in {
            InputDeliveryState.PENDING,
            InputDeliveryState.QUEUED,
        }:
            return "rejected"
        attempt_id = uuid.uuid4().hex
        message = replace(
            message,
            input_id=envelope.input_id,
            dispatch_attempt_id=attempt_id,
        )
        started = await self._inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=self._owner,
            expected=envelope.delivery_state,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id=attempt_id,
        )
        if not started:
            return "rejected"

        async def mark_queued(_input_id: str) -> bool:
            return await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=InputDeliveryState.DISPATCHING,
                new_state=InputDeliveryState.QUEUED,
            )

        outcome = await self._session.submit(message, before_queue=mark_queued)
        if outcome == "rejected":
            await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=InputDeliveryState.DISPATCHING,
                new_state=InputDeliveryState.PENDING,
            )
            return outcome
        if outcome == "queued":
            return outcome
        accepted = await self._inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=self._owner,
            expected=InputDeliveryState.DISPATCHING,
            new_state=InputDeliveryState.ACCEPTED,
        )
        if accepted:
            return outcome
        # The SDK already has this input but the durable "accepted" record was refused (a stop
        # boundary or an owner change landed in between). Resending is wrong; so is retrying
        # the hand-off. Settle it as unknown so it cannot hold the conversation.
        await self._inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=self._owner,
            expected=InputDeliveryState.DISPATCHING,
            new_state=InputDeliveryState.UNCERTAIN,
        )
        return "fenced"

    async def complete(self, input_id: str) -> bool:
        return await self._inbox.advance_input(
            conversation_key=self._owner.conversation_key,
            input_id=input_id,
            owner=self._owner,
            expected=InputDeliveryState.ACCEPTED,
            new_state=InputDeliveryState.COMPLETED,
        )

    async def mark_started(self, input_id: str) -> bool:
        return await self._inbox.advance_input(
            conversation_key=self._owner.conversation_key,
            input_id=input_id,
            owner=self._owner,
            expected=InputDeliveryState.QUEUED,
            new_state=InputDeliveryState.ACCEPTED,
        )

    async def interrupted(self, input_id: str, reason: str) -> bool:
        """Settle an accepted input whose turn ended in an interrupt.

        A user stop/reset or a newer message that replaced the turn cancels it. A stop by the
        system (shutdown, drain, lost lease) leaves it unknown: never resent automatically.
        """
        user_initiated = reason in {"user", "reset", "new_message"}
        return await self._inbox.advance_input(
            conversation_key=self._owner.conversation_key,
            input_id=input_id,
            owner=self._owner,
            expected=InputDeliveryState.ACCEPTED,
            new_state=(
                InputDeliveryState.CANCELLED if user_initiated else InputDeliveryState.UNCERTAIN
            ),
        )

    async def mark_uncertain(self, input_id: str) -> bool:
        return await self._inbox.advance_input(
            conversation_key=self._owner.conversation_key,
            input_id=input_id,
            owner=self._owner,
            expected=InputDeliveryState.ACCEPTED,
            new_state=InputDeliveryState.UNCERTAIN,
        )
