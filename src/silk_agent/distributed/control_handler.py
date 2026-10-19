"""Apply authorized stop/reset controls to one conversation actor."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

from silk_agent.core.background import BackgroundCommands, CommandCancelReport
from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import ConversationStore, InboxStore
from silk_agent.core.events import Notice
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.ports import Clock
from silk_agent.core.session import AgentSession
from silk_agent.distributed.metrics import METRICS
from silk_agent.distributed.reset import DistributedResetCoordinator
from silk_agent.distributed.stop import DistributedStopCoordinator

log = logging.getLogger(__name__)
RESET_NOTICE = "会話をリセットしました。"


class WorkerControlHandler:
    """Reader/handler pair injected into DistributedConversationActor.

    Authorization already happened in Control (stop_seq/reset_seq are committed), so this
    only carries out the effect and then closes the control document.
    """

    def __init__(
        self,
        *,
        inbox: InboxStore,
        conversations: ConversationStore,
        clock: Clock,
        orchestrator: ConversationOrchestrator,
        background: BackgroundCommands,
        owner: Callable[[], OwnershipToken],
        session: Callable[[], AgentSession | None],
        update_actor: Callable[[OwnershipToken], Awaitable[None]],
    ) -> None:
        self._inbox = inbox
        self._conversations = conversations
        self._clock = clock
        self._orchestrator = orchestrator
        self._background = background
        self._owner = owner
        self._session = session
        self._update_actor = update_actor

    async def read(self) -> Sequence[Mapping[str, Any]]:
        return await self._inbox.list_authorized_controls(self._owner().conversation_key)

    async def handle(self, control: Mapping[str, Any]) -> None:
        owner = self._owner()
        key = owner.conversation_key
        input_id = control.get("input_id")
        if not isinstance(input_id, str) or not input_id:
            raise ValueError("control record has no input_id")
        generation = control.get("conversation_generation")
        if isinstance(generation, int) and generation < owner.conversation_generation:
            # A reset already superseded it; closing it keeps the reader from rescanning it.
            await self._close(owner, input_id)
            return
        kind = control.get("kind")
        if kind == "stop_candidate":
            await self._stop(owner)
            await self._close(owner, input_id)
            self._observe_latency("stop", control)
        elif kind == "reset_candidate":
            seq = control.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool):
                # It can never succeed: close it rather than fail on every scan.
                log.warning("closing a reset control without a sequence for %s", key)
                await self._close(owner, input_id)
                return
            new_owner = await self._reset(owner, seq)
            if new_owner is not None:
                await self._close(new_owner, input_id)
                self._observe_latency("reset", control)
        else:
            # Nothing here will ever handle it: close it so the reader does not rescan it.
            log.warning("closing an unsupported control kind for %s", key)
            await self._close(owner, input_id)

    def _observe_latency(self, kind: str, control: Mapping[str, Any]) -> None:
        created = control.get("created_at")
        if isinstance(created, str):
            try:
                created = datetime.fromisoformat(created)
            except ValueError:
                return
        if isinstance(created, datetime) and created.tzinfo is not None:
            seconds = (self._clock.now() - created).total_seconds()
            METRICS.observe("control_latency_seconds", max(0.0, seconds), kind=kind)

    def _stop_coordinator(
        self, owner: OwnershipToken, results: dict[str, Any]
    ) -> DistributedStopCoordinator:
        key = owner.conversation_key

        async def interrupt() -> None:
            session = self._session()
            results["stopped"] = await session.stop() if session is not None else False

        async def cancel() -> CommandCancelReport:
            conversation = await self._conversations.get_conversation(key)
            stop_seq = conversation.get("stop_seq", 0) if conversation else 0
            report = await self._background.cancel_all_for_stop(
                key,
                conversation_generation=owner.conversation_generation,
                stop_seq=stop_seq if isinstance(stop_seq, int) else 0,
            )
            results["background"] = report
            return report

        return DistributedStopCoordinator(
            self._inbox,
            self._conversations,
            owner,
            interrupt_turn=interrupt,
            cancel_commands=cancel,
        )

    async def _stop(self, owner: OwnershipToken) -> None:
        results: dict[str, Any] = {}
        await self._stop_coordinator(owner, results).stop()
        await self._orchestrator.report_stop(
            owner.conversation_key,
            results.get("background", 0),
            bool(results.get("stopped", False)),
        )

    async def _reset(self, owner: OwnershipToken, reset_seq: int) -> OwnershipToken | None:
        results: dict[str, Any] = {}
        summary = await DistributedResetCoordinator(
            self._inbox,
            self._conversations,
            self._clock,
            owner,
            stop=self._stop_coordinator(owner, results),
            update_actor=self._update_actor,
            reset_seq=reset_seq,
        ).reset()
        if summary is None:
            return None
        session = self._session()
        if session is not None:
            # Engine history and sdk_session_id; the Firestore generation already advanced.
            await session.reset()
        await self._orchestrator.emit(Notice(owner.conversation_key, RESET_NOTICE))
        return summary.owner

    async def _close(self, owner: OwnershipToken, input_id: str) -> None:
        """Mark the control handled with the actor's *current* token.

        A lease renewal during a slow stop replaces the token; closing with the one captured
        at the start would be rejected and leave the control to be applied a second time.
        """
        for candidate in (self._owner(), owner):
            if await self._inbox.mark_control_handled(
                conversation_key=candidate.conversation_key,
                input_id=input_id,
                owner=candidate,
                now=self._clock.now(),
            ):
                return
        log.warning("control was not closed; ownership changed while it was applied")
