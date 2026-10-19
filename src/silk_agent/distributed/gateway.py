"""Socket Mode ingress that commits accepted Slack events before acknowledging them."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from slack_sdk.socket_mode.response import SocketModeResponse

from silk_agent.channels.slack_events import (
    attachment_names,
    make_key,
    normalize_message_text,
    slack_message_id,
)
from silk_agent.core.distributed_ports import InboxStore
from silk_agent.core.ports import Clock
from silk_agent.distributed.metrics import METRICS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReceivedSlackInput:
    conversation_key: str
    input_id: str
    kind: str
    payload: Mapping[str, Any]
    workspace: str
    team: str
    channel: str
    message_ts: str
    event_id: str
    sender_id: str | None = None
    conversation_generation: int = 1
    mark_participating: bool = False


NormalizeEnvelope = Callable[[Mapping[str, Any]], Awaitable[ReceivedSlackInput | None]]
SendResponse = Callable[[SocketModeResponse], Awaitable[None]]
AcceptedCallback = Callable[[ReceivedSlackInput], Awaitable[None]]


class SlackSocketGateway:
    """A raw Socket Mode listener with no Bolt auto-ack path.

    `normalize` may reject an irrelevant event by returning None. Accepted events
    are ACKed only after the inbox transaction has committed. Redelivery after an
    ACK transport failure is safe because the inbox writes both Slack receipts.
    """

    def __init__(
        self,
        inbox: InboxStore,
        clock: Clock,
        normalize: NormalizeEnvelope,
        on_accepted: AcceptedCallback | None = None,
    ) -> None:
        self._inbox = inbox
        self._clock = clock
        self._normalize = normalize
        self._on_accepted = on_accepted
        self._tasks: set[asyncio.Task[None]] = set()
        self._accepting = True
        self._handlers: set[asyncio.Task[Any]] = set()
        self._handlers_idle = asyncio.Event()
        self._handlers_idle.set()

    async def handle_message(self, raw_message: str, send_response: SendResponse) -> None:
        if not self._accepting:
            raise RuntimeError("Socket Mode gateway is shutting down")
        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("Socket Mode handler must run inside an asyncio task")
        self._handlers.add(current_task)
        self._handlers_idle.clear()
        try:
            await self._handle_message(raw_message, send_response)
        finally:
            self._handlers.discard(current_task)
            if not self._handlers:
                self._handlers_idle.set()

    async def _handle_message(self, raw_message: str, send_response: SendResponse) -> None:
        normalized: ReceivedSlackInput | None = None
        try:
            envelope = json.loads(raw_message)
        except (TypeError, ValueError):
            log.warning("discarding malformed Socket Mode frame")
            return
        if not isinstance(envelope, Mapping):
            log.warning("discarding non-object Socket Mode frame")
            return
        envelope_id = envelope.get("envelope_id")
        if not isinstance(envelope_id, str) or not envelope_id:
            log.warning("Socket Mode frame has no envelope ID")
            return
        try:
            normalized = await self._normalize(envelope)
            if normalized is not None:
                await self._inbox.accept_received(
                    conversation_key=normalized.conversation_key,
                    input_id=normalized.input_id,
                    conversation_generation=normalized.conversation_generation,
                    kind=normalized.kind,
                    payload=normalized.payload,
                    created_at=self._clock.now(),
                    workspace=normalized.workspace,
                    team=normalized.team,
                    channel=normalized.channel,
                    message_ts=normalized.message_ts,
                    event_id=normalized.event_id,
                    sender_id=normalized.sender_id,
                    participating=normalized.mark_participating,
                )
        except Exception as exc:
            METRICS.inc("gateway_persist_failures_total")
            # Do not include the Slack body in logs: it can contain user content.
            log.error(
                "could not persist Socket Mode event (%s); leaving it unacknowledged",
                type(exc).__name__,
            )
            raise
        await send_response(SocketModeResponse(envelope_id=envelope_id))
        METRICS.inc("gateway_events_total", result="accepted" if normalized else "ignored")
        if normalized is not None and self._on_accepted is not None:
            task = asyncio.create_task(self._run_accepted(normalized))
            self._tasks.add(task)
            task.add_done_callback(self._enrichment_done)

    async def _run_accepted(self, received: ReceivedSlackInput) -> None:
        callback = self._on_accepted
        if callback is not None:
            await callback(received)

    def _enrichment_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            log.warning("post-ACK sender enrichment failed (%s)", type(error).__name__)

    def stop_accepting(self) -> None:
        """Reject new frames while allowing in-flight commit/ACK outcomes to settle."""
        self._accepting = False

    async def aclose(self) -> None:
        self.stop_accepting()
        # The socket transport should be closed by its owner before aclose(). Any
        # handler already entered here is allowed to commit and attempt its ACK.
        await self._handlers_idle.wait()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def event_callback(envelope: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return the Events API callback payload, ignoring other Socket Mode frames."""
    if envelope.get("type") != "events_api":
        return None
    payload = envelope.get("payload")
    if not isinstance(payload, Mapping) or payload.get("type") != "event_callback":
        return None
    return payload


class SlackEventNormalizer:
    """Normalize message/app_mention frames without performing users.info lookups."""

    def __init__(
        self,
        *,
        bot_user_id: str,
        is_participating: Callable[[str], Awaitable[bool]],
        conversation_generation: Callable[[str], Awaitable[int]] | None = None,
        stop_words: tuple[str, ...] | list[str] = (),
        reset_words: tuple[str, ...] | list[str] = (),
    ) -> None:
        self._bot_user_id = bot_user_id
        self._is_participating = is_participating
        self._conversation_generation = conversation_generation
        self._stop_words = {word.strip().casefold() for word in stop_words if word.strip()}
        self._reset_words = {word.strip().casefold() for word in reset_words if word.strip()}

    async def __call__(self, envelope: Mapping[str, Any]) -> ReceivedSlackInput | None:
        callback = event_callback(envelope)
        if callback is None:
            return None
        event = callback.get("event")
        if not isinstance(event, Mapping):
            raise ValueError("Slack event callback is missing an event object")
        event_type = event.get("type")
        is_mention = event_type == "app_mention"
        if event_type not in {"app_mention", "message"}:
            return None
        if event.get("user") == self._bot_user_id or event.get("bot_id"):
            return None  # the Bot's own (or another bot's) message: never an input
        team = str(callback.get("team_id") or event.get("team") or "")
        channel = str(event.get("channel") or "")
        message_ts = str(event.get("ts") or "")
        event_id = str(callback.get("event_id") or "")
        if not all((team, channel, message_ts, event_id)):
            raise ValueError("Slack event callback is missing a stable identity")
        channel_type = event.get("channel_type")
        log.info(
            "Slack %s event (channel_type=%s, in_thread=%s)",
            event_type,
            channel_type,
            isinstance(event.get("thread_ts"), str),
        )
        mark_participating = False
        if is_mention:
            thread_ts = str(event.get("thread_ts") or message_ts)
            mark_participating = True
        elif channel_type == "im":
            thread_ts = str(event.get("thread_ts") or "-")
        else:
            thread_value = event.get("thread_ts")
            if not isinstance(thread_value, str) or thread_value == message_ts:
                # Not in a thread: a message in the channel without a mention is not for the Bot.
                log.info("ignored a Slack message outside a thread (no mention)")
                return None
            thread_ts = thread_value
            conversation_key = make_key(team, channel, thread_ts)
            if not await self._is_participating(conversation_key):
                # The Bot has not been mentioned in this thread, so it does not take part in it.
                log.info("ignored a Slack thread message: the Bot does not take part in the thread")
                return None
        conversation_key = make_key(team, channel, None if thread_ts == "-" else thread_ts)
        generation = (
            await self._conversation_generation(conversation_key)
            if self._conversation_generation is not None
            else 1
        )
        text = normalize_message_text(str(event.get("text") or ""))
        folded_text = text.casefold()
        is_reset_candidate = bool(text) and folded_text in self._reset_words
        is_stop_candidate = bool(text) and folded_text in self._stop_words
        payload = {
            "text": text,
            "user": str(event.get("user") or ""),
            "message_id": slack_message_id(channel, message_ts),
            "attachments": attachment_names(event.get("files", [])),
            "team": team,
            "channel": channel,
            "sender_is_bot": False,
            "is_direct": channel_type == "im",
            "kind": str(event.get("subtype") or "message"),
        }
        return ReceivedSlackInput(
            conversation_key=conversation_key,
            input_id=f"{channel}:{message_ts}",
            kind=("reset_candidate" if is_reset_candidate else "stop_candidate")
            if is_reset_candidate or is_stop_candidate
            else "message",
            payload=payload,
            workspace=team,
            team=team,
            channel=channel,
            message_ts=message_ts,
            event_id=event_id,
            sender_id=str(event.get("user") or "")
            if is_stop_candidate or is_reset_candidate
            else None,
            conversation_generation=generation,
            mark_participating=mark_participating,
        )
