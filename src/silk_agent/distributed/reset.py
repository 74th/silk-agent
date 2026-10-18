"""Ordered conversation reset across the durable inbox and active worker."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import ConversationStore, InboxStore
from silk_agent.core.ports import Clock
from silk_agent.distributed.stop import DistributedStopCoordinator


@dataclass(frozen=True)
class ResetSummary:
    owner: OwnershipToken
    inputs_rebound: int


class DistributedResetCoordinator:
    """Stop old work before advancing the generation and reopening later input."""

    def __init__(
        self,
        inbox: InboxStore,
        conversations: ConversationStore,
        clock: Clock,
        owner: OwnershipToken,
        *,
        stop: DistributedStopCoordinator,
        update_actor: Callable[[OwnershipToken], Awaitable[None]],
        reset_seq: int,
    ) -> None:
        if reset_seq < 1:
            raise ValueError("reset_seq must be positive")
        self._inbox = inbox
        self._conversations = conversations
        self._clock = clock
        self._owner = owner
        self._stop = stop
        self._update_actor = update_actor
        self._reset_seq = reset_seq

    async def reset(self) -> ResetSummary | None:
        if not await self._conversations.begin_reset(owner=self._owner, reset_seq=self._reset_seq):
            return None
        await self._stop.stop()
        new_owner = await self._conversations.complete_reset(
            owner=self._owner, now=self._clock.now()
        )
        if new_owner is None:
            return None
        rebound = await self._inbox.rebind_after_reset(
            conversation_key=self._owner.conversation_key,
            previous_generation=self._owner.conversation_generation,
            reset_seq=self._reset_seq,
            owner=new_owner,
        )
        await self._update_actor(new_owner)
        return ResetSummary(new_owner, rebound)
