"""Durable stop barrier coordination for one active conversation owner."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from silk_agent.core.distributed_models import InputDeliveryState, OwnershipToken
from silk_agent.core.distributed_ports import ConversationStore, InboxStore


@dataclass(frozen=True)
class StopSummary:
    inputs_cancelled: int
    interrupt_requested: bool
    command_cancellation: Any


class DistributedStopCoordinator:
    def __init__(
        self,
        inbox: InboxStore,
        conversations: ConversationStore,
        owner: OwnershipToken,
        *,
        interrupt_turn: Callable[[], Awaitable[None]] | None = None,
        cancel_commands: Callable[[], Awaitable[Any]],
        page_size: int = 100,
    ) -> None:
        if page_size < 1:
            raise ValueError("page_size must be positive")
        self._inbox = inbox
        self._conversations = conversations
        self._owner = owner
        self._interrupt = interrupt_turn
        self._cancel_commands = cancel_commands
        self._page_size = page_size

    async def stop(self) -> StopSummary:
        """Honor an already committed stop_seq and initiate both stop actions promptly."""
        conversation = await self._conversations.get_conversation(self._owner.conversation_key)
        stop_seq = conversation.get("stop_seq", 0) if conversation is not None else 0
        if not isinstance(stop_seq, int) or isinstance(stop_seq, bool) or stop_seq < 0:
            raise ValueError("conversation stop_seq is invalid")

        cancel_inputs = asyncio.create_task(self._cancel_queued(stop_seq))
        interrupt = asyncio.ensure_future(self._interrupt()) if self._interrupt else None
        cancel_commands = asyncio.ensure_future(self._cancel_commands())
        tasks = [cancel_inputs, cancel_commands]
        if interrupt is not None:
            tasks.append(interrupt)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for error in results[1:]:
            if isinstance(error, BaseException):
                raise error
        input_result = results[0]
        if isinstance(input_result, BaseException):
            raise input_result
        command_result = results[1]
        if isinstance(command_result, BaseException):
            raise command_result
        return StopSummary(input_result, interrupt is not None, command_result)

    async def _cancel_queued(self, stop_seq: int) -> int:
        cursor = 0
        cancelled = 0
        while cursor < stop_seq:
            records = await self._inbox.list_unfinished(
                self._owner.conversation_key,
                after_seq=cursor,
                limit=self._page_size,
            )
            if not records:
                break
            page_end = records[-1].seq
            for record in records:
                if record.seq > stop_seq:
                    break
                if record.delivery_state not in {
                    InputDeliveryState.PENDING,
                    InputDeliveryState.QUEUED,
                }:
                    continue
                if await self._inbox.advance_input(
                    conversation_key=record.conversation_key,
                    input_id=record.input_id,
                    owner=self._owner,
                    expected=record.delivery_state,
                    new_state=InputDeliveryState.CANCELLED,
                ):
                    cancelled += 1
            cursor = page_end
        return cancelled
