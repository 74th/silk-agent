"""Deliver durable background command completion inputs to the conversation actor."""

from __future__ import annotations

import uuid
from collections.abc import Callable

from silk_agent.core.distributed_models import InputDeliveryState, InputEnvelope, OwnershipToken
from silk_agent.core.distributed_ports import CommandMonitorStore, InboxStore
from silk_agent.core.session import AgentSession


class CompletionInputDelivery:
    def __init__(
        self,
        inbox: InboxStore,
        owner: OwnershipToken | Callable[[], OwnershipToken],
        session: AgentSession,
        *,
        command_monitor: CommandMonitorStore | None = None,
    ) -> None:
        self._inbox = inbox
        self._owner_source = owner
        self._session = session
        self._command_monitor = command_monitor

    @property
    def _owner(self) -> OwnershipToken:
        """The current token: a lease renewal replaces it while an operation is in flight."""
        source = self._owner_source
        return source() if callable(source) else source

    async def dispatch(self, envelope: InputEnvelope) -> str:
        if envelope.kind != "command_finished":
            return "ignored"
        if envelope.conversation_generation != self._owner.conversation_generation:
            return "stale"
        if envelope.delivery_state not in {InputDeliveryState.PENDING, InputDeliveryState.QUEUED}:
            return "claimed_elsewhere"  # accepted/uncertain/held results are never re-sent
        # Both a fresh result (PENDING) and one left QUEUED by a previous Worker (or released
        # from HELD by the user's resume) are claimed through the fenced CAS. It re-checks the
        # current owner, generation, stop barrier and state; if any changed since this record was
        # read (a STOP, a reset, another Worker), nothing is appended to the SDK.
        expected = envelope.delivery_state
        attempt_id = uuid.uuid4().hex
        if not await self._inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=self._owner,
            expected=expected,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id=attempt_id,
        ):
            return "claimed_elsewhere"
        if not await self._inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=self._owner,
            expected=InputDeliveryState.DISPATCHING,
            new_state=InputDeliveryState.QUEUED,
        ):
            return "fenced"
        try:
            outcome = await self._session.notify(
                _completion_text(envelope), input_id=envelope.input_id
            )
        except Exception:
            await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=InputDeliveryState.QUEUED,
                new_state=InputDeliveryState.UNCERTAIN,
            )
            return "uncertain"
        if outcome == "uncertain":
            # The agent may have the completion already: never send it again.
            await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=InputDeliveryState.QUEUED,
                new_state=InputDeliveryState.UNCERTAIN,
            )
            return "uncertain"
        if outcome == "held":
            await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=InputDeliveryState.QUEUED,
                new_state=InputDeliveryState.HELD,
            )
            if self._command_monitor is not None:
                command_id = envelope.payload.get("command_id")
                if isinstance(command_id, str):
                    await self._command_monitor.set_notification_state(
                        command_id,
                        expected="pending",
                        new_state="held",
                        now=envelope.created_at,
                    )
        elif outcome == "appended":
            await self._inbox.advance_input(
                conversation_key=envelope.conversation_key,
                input_id=envelope.input_id,
                owner=self._owner,
                expected=InputDeliveryState.QUEUED,
                new_state=InputDeliveryState.ACCEPTED,
            )
        return outcome


def _completion_text(envelope: InputEnvelope) -> str:
    payload = envelope.payload
    command_id = payload.get("command_id", "unknown")
    state = payload.get("state", "unknown")
    result = payload.get("result")
    output = result.get("output") if isinstance(result, dict) else None
    tails = []
    if isinstance(output, dict):
        for stream in ("stdout", "stderr"):
            item = output.get(stream)
            tail = item.get("tail") if isinstance(item, dict) else None
            if isinstance(tail, str) and tail:
                tails.append(f"[{stream}]\n{tail[-4000:]}")
    detail = "\n".join(tails) if tails else "出力はありません。"
    return (
        "<system-notice>バックグラウンドコマンドが終了しました。\n"
        f"command_id: {command_id}\n状態: {state}\n{detail}</system-notice>\n"
        "結果をユーザに簡潔に報告してください。"
    )


async def release_held_completions(
    inbox: InboxStore,
    owner: OwnershipToken,
    *,
    command_monitor: CommandMonitorStore | None,
    now,
) -> int:
    """On the user's resume, move held command results back to the queue (HELD -> QUEUED)."""
    released = 0
    for envelope in await inbox.list_held(owner.conversation_key, limit=50):
        if envelope.conversation_generation != owner.conversation_generation:
            continue
        if not await inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.HELD,
            new_state=InputDeliveryState.QUEUED,
        ):
            continue
        released += 1
        command_id = envelope.payload.get("command_id")
        if command_monitor is not None and isinstance(command_id, str):
            await command_monitor.set_notification_state(
                command_id, expected="held", new_state="pending", now=now
            )
    return released
