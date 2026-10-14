"""ConversationOrchestrator: filter, stop word, input guard, session, sanitize, channel."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace

from silk_agent.core.background import CommandCancelReport
from silk_agent.core.events import (
    AssistantText,
    GuardBlocked,
    InboundEvent,
    Notice,
    OutboundEvent,
    ResetRequest,
    ResumeRequest,
    StopRequest,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    UserMessage,
)
from silk_agent.core.filters import FilterChain
from silk_agent.core.ports import (
    AgentEngineFactory,
    ChatChannel,
    Clock,
    ConversationState,
    InputGuard,
    OutputSanitizer,
    SanitizeResult,
    SessionStateStore,
)
from silk_agent.core.session import AgentSessionRegistry, SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.hooks import (
    AgentReply,
    HookDispatcher,
    HookEvent,
    InputDecision,
    SystemNotice,
    TurnEnd,
    TurnStart,
    UserInput,
)

log = logging.getLogger(__name__)

HISTORY_FOR_GUARD = 5
NOTHING_RUNNING = "実行中の処理はありません。"
BACKGROUND_STOPPED = "バックグラウンドで実行中のコマンド{n}件を停止しました。"
BACKGROUND_STOP_ACCEPTED = (
    "バックグラウンドコマンド{n}件へ停止要求を送信しました。"
    "実際の停止は command_status で確認してください。"
)
BACKGROUND_STOP_UNKNOWN = (
    "バックグラウンドコマンド{n}件の停止要求を確認できません。"
    "command_status で状態を確認してください。"
)
INPUT_CHECK_FAILED = (
    "入力を確認できなかったため、処理しませんでした。しばらくしてからもう一度お試しください。"
)
OUTPUT_BLOCKED = "ポリシーにより、この出力は表示できません。"
OUTPUT_CHECK_FAILED = "出力を検査できなかったため、表示できません。"
RESUME_TEXT = "続けてください。"


class ConversationOrchestrator:
    def __init__(
        self,
        engine_factory: AgentEngineFactory,
        channel: ChatChannel,
        clock: Clock,
        *,
        guard: InputGuard,
        sanitizer: OutputSanitizer,
        stop_words: StopWords,
        filters: FilterChain | None = None,
        guard_on_error: str = "block",
        settings: SessionSettings | None = None,
        state_store: SessionStateStore | None = None,
        bot_instance_id: str = "",
        cancel_commands: Callable[[str], Awaitable[None]] | None = None,
        orphan_commands: Callable[[str], Awaitable[None]] | None = None,
        cancel_background: Callable[[str], Awaitable[int | CommandCancelReport]] | None = None,
        notice_source: Callable[[str], Awaitable[list[str]]] | None = None,
        notice_ack: Callable[[str], Awaitable[None]] | None = None,
        sandbox_ready: Callable[[str], Awaitable[bool]] | None = None,
        hooks: HookDispatcher | None = None,
        guard_history_loader: Callable[[str], Awaitable[Sequence[str]]] | None = None,
        guard_history_recorder: Callable[[UserMessage], Awaitable[bool]] | None = None,
        distributed_rate_limit: Callable[[UserMessage], Awaitable[bool]] | None = None,
        initial_owner_recorder: Callable[[UserMessage], Awaitable[None]] | None = None,
        outbox_sink: Callable[[OutboundEvent], Awaitable[None]] | None = None,
        input_completed: Callable[[str], Awaitable[None]] | None = None,
        input_started: Callable[[str], Awaitable[bool]] | None = None,
        input_uncertain: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._clock = clock
        self._hooks = hooks
        self._registry = AgentSessionRegistry(
            engine_factory,
            self.emit,
            clock,
            settings,
            state_store=state_store,
            bot_instance_id=bot_instance_id,
            cancel_commands=cancel_commands,
            orphan_commands=orphan_commands,
            notice_source=notice_source,
            notice_ack=notice_ack,
            sandbox_ready=sandbox_ready,
            input_completed=input_completed,
            input_started=input_started,
            input_uncertain=input_uncertain,
        )
        self._cancel_background = cancel_background
        self._channel = channel
        self._guard = guard
        self._sanitizer = sanitizer
        self._stop_words = stop_words
        self._filters = filters
        self._guard_on_error = guard_on_error
        self._state_store = state_store
        self._owner_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._history: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=HISTORY_FOR_GUARD))
        self._guard_history_loader = guard_history_loader
        self._guard_history_recorder = guard_history_recorder
        self._distributed_rate_limit = distributed_rate_limit
        self._initial_owner_recorder = initial_owner_recorder
        self._outbox_sink = outbox_sink

    @property
    def registry(self) -> AgentSessionRegistry:
        return self._registry

    async def shutdown(self, grace_seconds: float = 25.0) -> None:
        """Stop accepting input, give running turns `grace_seconds` to finish, interrupt the
        rest (state: interrupted_shutdown) and release the engines."""
        await self._registry.shutdown(grace_seconds)

    # ------------------------------------------------------------ inbound

    async def handle(self, event: InboundEvent) -> None:
        if isinstance(event, UserMessage):
            await self._handle_message(event)
        elif isinstance(event, StopRequest):
            await self._handle_stop(event)
        elif isinstance(event, ResetRequest):
            session = await self._registry.get_or_create(event.conversation_key)
            await session.reset()
            self._history.pop(event.conversation_key, None)
            await self._channel.send(Notice(event.conversation_key, "会話をリセットしました。"))
        elif isinstance(event, ResumeRequest):
            await self._handle_message(UserMessage(event.conversation_key, event.user, RESUME_TEXT))

    async def _handle_message(self, message: UserMessage) -> None:
        if not await self.prepare_message(message):
            return
        session = await self._registry.get_or_create(message.conversation_key)
        await session.submit(message)

    async def prepare_message(self, message: UserMessage) -> bool:
        """Apply normal input checks and durable management updates without submitting.

        Distributed actors call this before fencing the Inbox transition and submitting
        through DurableInputDelivery. The in-process path continues through
        ``_handle_message`` and submits to the same AgentSession registry.
        """
        key = message.conversation_key
        if self._filters is not None:
            decision = self._filters.check(
                message, skip_rate_limit=self._distributed_rate_limit is not None
            )
            if not decision.allowed:
                self._input(message, "filtered", filter=decision.reason)
                if decision.reply:
                    await self._channel.send(Notice(key, decision.reply))
                return False

        if self._distributed_rate_limit is not None:
            if not await self._distributed_rate_limit(message):
                self._input(message, "filtered", filter="RateLimitFilter")
                await self._channel.send(
                    Notice(key, "送信が多すぎます。少し待ってから送ってください。")
                )
                return False

        if self._stop_words.matches(message.text):
            self._input(message, "stop_word")
            await self._handle_stop(StopRequest(key, message.user))
            return False

        history = (
            list(await self._guard_history_loader(key))
            if self._guard_history_loader is not None
            else list(self._history[key])
        )[-HISTORY_FOR_GUARD:]
        try:
            verdict = await self._guard.check(message.text, history)
        except Exception:
            log.exception("input guard failed")
            if self._guard_on_error == "allow":
                verdict = None
            else:
                self._input(message, "guard_failed", reason="input guard failed")
                await self._channel.send(GuardBlocked(key, "input", INPUT_CHECK_FAILED))
                return False
        if verdict is not None and verdict.decision != "allow":
            reason = verdict.reason or "入力がポリシーに違反している可能性があります。"
            self._input(
                message, "review" if verdict.decision == "review" else "blocked", reason=reason
            )
            await self._channel.send(GuardBlocked(key, "input", reason))
            return False

        if self._guard_history_recorder is not None:
            if not await self._guard_history_recorder(message):
                log.warning("could not save accepted input history for current owner")
                return False
        else:
            self._history[key].append(message.text)
        self._input(message, "accepted")
        await self._record_owner(message)
        return True

    async def _record_owner(self, message: UserMessage) -> None:
        """The user of the first accepted message owns the conversation (it is saved before
        the Sandbox is created, which takes AGENT_USER_EMAIL from it). A conversation that
        already has a record but no owner predates this and stays without one."""
        if self._initial_owner_recorder is not None:
            await self._initial_owner_recorder(message)
            return
        if self._state_store is None:
            return
        key = message.conversation_key
        async with self._owner_locks[key]:
            try:
                if await self._state_store.get(key) is not None:
                    return
                email = message.sender.email if message.sender else ""
                await self._state_store.put(
                    ConversationState(key, owner_user=message.user, owner_email=email)
                )
            except Exception:
                log.exception("could not record the owner of %s", key)

    async def _handle_stop(self, request: StopRequest) -> None:
        key = request.conversation_key

        # Background commands first: they are stopped on request and send no completion.
        async def cancel_background() -> int | CommandCancelReport:
            if self._cancel_background is None:
                return 0
            try:
                return await self._cancel_background(key)
            except Exception:
                log.exception("failed to stop background commands for %s", key)
                return 0

        session = self._registry.get(key)
        background_task = asyncio.create_task(cancel_background())
        stop_task = asyncio.create_task(session.stop()) if session is not None else None
        background = await background_task
        stopped = await stop_task if stop_task is not None else False
        await self.report_stop(key, background, stopped)

    async def report_stop(
        self, key: str, background: int | CommandCancelReport, stopped: bool
    ) -> None:
        """Tell the user what a stop request cancelled (shared with the distributed path)."""
        if isinstance(background, CommandCancelReport) and background.requested:
            if background.confirmed:
                await self._channel.send(
                    Notice(key, BACKGROUND_STOPPED.format(n=background.confirmed))
                )
            elif background.unknown or background.rejected:
                await self._channel.send(
                    Notice(key, BACKGROUND_STOP_UNKNOWN.format(n=background.requested))
                )
            else:
                await self._channel.send(
                    Notice(key, BACKGROUND_STOP_ACCEPTED.format(n=background.requested))
                )
        elif isinstance(background, int) and background:
            await self._channel.send(Notice(key, BACKGROUND_STOPPED.format(n=background)))
        elif not stopped:
            await self._channel.send(Notice(key, NOTHING_RUNNING))

    async def notify(self, key: str, text: str) -> None:
        """A system notice for the agent (e.g. a background command finished). It joins the
        running turn, or starts a turn so the agent can report to the user."""
        self._publish(SystemNotice(conversation_key=key, at=self._clock.now(), text=text))
        session = await self._registry.get_or_create(key)
        await session.notify(text)

    # ------------------------------------------------------------ outbound

    async def emit(self, event: OutboundEvent) -> None:
        """Sink for AgentSession: sanitize anything that shows agent-derived text."""
        self._publish_outbound(event)
        if isinstance(event, AssistantText):
            result = await self._sanitize(event.text)
            self._publish(
                AgentReply(
                    conversation_key=event.conversation_key,
                    turn_id=event.turn_id,
                    at=self._clock.now(),
                    text=event.text,
                    sanitize_status=result.status,
                )
            )
            if result.status in ("pass", "masked"):
                await self._deliver(replace(event, text=result.text))
            else:
                await self._deliver(
                    GuardBlocked(event.conversation_key, "output", _output_reason(result))
                )
        elif isinstance(event, ToolStarted | ToolFinished):
            result = await self._sanitize(event.summary) if event.summary else None
            if result is None:
                await self._deliver(event)
            elif result.status in ("pass", "masked"):
                await self._deliver(replace(event, summary=result.text))
            else:
                await self._deliver(replace(event, summary="（表示できません）"))
        else:
            await self._deliver(event)

    async def _deliver(self, event: OutboundEvent) -> None:
        if self._outbox_sink is not None:
            await self._outbox_sink(event)
        else:
            await self._channel.send(event)

    # ------------------------------------------------------------ hooks

    def _publish(self, event: HookEvent) -> None:
        if self._hooks is not None:
            self._hooks.publish(event)

    def _input(
        self, message: UserMessage, decision: InputDecision, *, reason: str = "", filter: str = ""
    ) -> None:
        if self._hooks is None:
            return
        self._hooks.publish(
            UserInput(
                conversation_key=message.conversation_key,
                at=self._clock.now(),
                user=message.user or None,
                user_email=(message.sender.email if message.sender else "") or None,
                text=message.text,
                message_id=message.message_id,
                attachments=message.attachments,
                decision=decision,
                reason=reason,
                filter=filter,
            )
        )

    def _publish_outbound(self, event: OutboundEvent) -> None:
        """Turn boundaries. (Tool events come from the engine, which has the raw input and
        result; the reply is published after its output check.)"""
        if self._hooks is None:
            return
        key = event.conversation_key if hasattr(event, "conversation_key") else ""
        if isinstance(event, TurnStarted):
            self._publish(
                TurnStart(conversation_key=key, turn_id=event.turn_id, at=event.started_at)
            )
        elif isinstance(event, TurnFinished):
            self._publish(
                TurnEnd(
                    conversation_key=key,
                    turn_id=event.turn_id,
                    at=event.finished_at or self._clock.now(),
                    outcome="completed",
                    input_tokens=event.usage.input_tokens,
                    output_tokens=event.usage.output_tokens,
                    cost_usd=event.usage.cost_usd,
                )
            )
        elif isinstance(event, TurnFailed):
            self._publish(
                TurnEnd(
                    conversation_key=key,
                    turn_id=event.turn_id,
                    at=self._clock.now(),
                    outcome="failed",
                    reason=event.error,
                )
            )
        elif isinstance(event, TurnInterrupted):
            self._publish(
                TurnEnd(
                    conversation_key=key,
                    turn_id=event.turn_id,
                    at=event.at or self._clock.now(),
                    outcome="interrupted",
                    reason=event.reason,
                )
            )

    async def _sanitize(self, text: str) -> SanitizeResult:
        try:
            return await self._sanitizer.sanitize(text)
        except Exception:
            log.exception("output sanitizer failed")
            return SanitizeResult("error", "", "sanitizer failure")


def _output_reason(result: SanitizeResult) -> str:
    return OUTPUT_CHECK_FAILED if result.status == "error" else OUTPUT_BLOCKED
