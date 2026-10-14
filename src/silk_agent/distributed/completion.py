"""Deliver durable background command completion inputs to the conversation actor."""

from __future__ import annotations

import uuid

from silk_agent.core.distributed_models import InputDeliveryState, InputEnvelope, OwnershipToken
from silk_agent.core.distributed_ports import CommandMonitorStore, InboxStore
from silk_agent.core.session import AgentSession


class CompletionInputDelivery:
    def __init__(
        self,
        inbox: InboxStore,
        owner: OwnershipToken,
        session: AgentSession,
        *,
        command_monitor: CommandMonitorStore | None = None,
    ) -> None:
        self._inbox = inbox
        self._owner = owner
        self._session = session
        self._command_monitor = command_monitor

    async def dispatch(self, envelope: InputEnvelope) -> str:
        if envelope.kind != "command_finished":
            return "ignored"
        if envelope.conversation_generation != self._owner.conversation_generation:
            return "stale"
        attempt_id = uuid.uuid4().hex
        if not await self._inbox.advance_input(
            conversation_key=envelope.conversation_key,
            input_id=envelope.input_id,
            owner=self._owner,
            expected=InputDeliveryState.PENDING,
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
