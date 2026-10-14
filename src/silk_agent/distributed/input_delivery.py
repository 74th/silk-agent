"""Fence AgentSession handoff to the durable inbox delivery state."""

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import Protocol

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

    def __init__(self, inbox: InboxStore, owner: OwnershipToken, session: SessionSubmitter):
        self._inbox = inbox
        self._owner = owner
        self._session = session

    async def dispatch(self, envelope: InputEnvelope, message: UserMessage) -> SubmissionOutcome:
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
        return outcome if accepted else "rejected"

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

    async def mark_uncertain(self, input_id: str) -> bool:
        return await self._inbox.advance_input(
            conversation_key=self._owner.conversation_key,
            input_id=input_id,
            owner=self._owner,
            expected=InputDeliveryState.ACCEPTED,
            new_state=InputDeliveryState.UNCERTAIN,
        )
