"""ConversationOrchestrator: filter, stop word, input guard, session, sanitize, channel."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import replace

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
    UserMessage,
)
from silk_agent.core.filters import FilterChain
from silk_agent.core.ports import (
    AgentEngineFactory,
    ChatChannel,
    Clock,
    InputGuard,
    OutputSanitizer,
    SanitizeResult,
    SessionStateStore,
)
from silk_agent.core.session import AgentSessionRegistry, SessionSettings
from silk_agent.core.stopwords import StopWords

log = logging.getLogger(__name__)

HISTORY_FOR_GUARD = 5
NOTHING_RUNNING = "実行中の処理はありません。"
BACKGROUND_STOPPED = "バックグラウンドで実行中のコマンド{n}件を停止しました。"
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
        cancel_background: Callable[[str], Awaitable[int]] | None = None,
        notice_source: Callable[[str], Awaitable[list[str]]] | None = None,
        notice_ack: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._registry = AgentSessionRegistry(
            engine_factory,
            self.emit,
            clock,
            settings,
            state_store=state_store,
            bot_instance_id=bot_instance_id,
            cancel_commands=cancel_commands,
            notice_source=notice_source,
            notice_ack=notice_ack,
        )
        self._cancel_background = cancel_background
        self._channel = channel
        self._guard = guard
        self._sanitizer = sanitizer
        self._stop_words = stop_words
        self._filters = filters
        self._guard_on_error = guard_on_error
        self._history: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=HISTORY_FOR_GUARD))

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
        key = message.conversation_key
        if self._filters is not None:
            decision = self._filters.check(message)
            if not decision.allowed:
                if decision.reply:
                    await self._channel.send(Notice(key, decision.reply))
                return

        if self._stop_words.matches(message.text):
            await self._handle_stop(StopRequest(key, message.user))
            return

        history = list(self._history[key])
        try:
            verdict = await self._guard.check(message.text, history)
        except Exception:
            log.exception("input guard failed")
            if self._guard_on_error == "allow":
                verdict = None
            else:
                await self._channel.send(GuardBlocked(key, "input", INPUT_CHECK_FAILED))
                return
        if verdict is not None and verdict.decision != "allow":
            reason = verdict.reason or "入力がポリシーに違反している可能性があります。"
            await self._channel.send(GuardBlocked(key, "input", reason))
            return

        self._history[key].append(message.text)
        session = await self._registry.get_or_create(key)
        await session.submit(message)

    async def _handle_stop(self, request: StopRequest) -> None:
        key = request.conversation_key
        # Background commands first: they are stopped on request and send no completion.
        background = 0
        if self._cancel_background is not None:
            try:
                background = await self._cancel_background(key)
            except Exception:
                log.exception("failed to stop background commands for %s", key)
        session = self._registry.get(key)
        stopped = await session.stop() if session is not None else False
        if background:
            await self._channel.send(Notice(key, BACKGROUND_STOPPED.format(n=background)))
        elif not stopped:
            await self._channel.send(Notice(key, NOTHING_RUNNING))

    async def notify(self, key: str, text: str) -> None:
        """A system notice for the agent (e.g. a background command finished). It joins the
        running turn, or starts a turn so the agent can report to the user."""
        session = await self._registry.get_or_create(key)
        await session.notify(text)

    # ------------------------------------------------------------ outbound

    async def emit(self, event: OutboundEvent) -> None:
        """Sink for AgentSession: sanitize anything that shows agent-derived text."""
        if isinstance(event, AssistantText):
            result = await self._sanitize(event.text)
            if result.status in ("pass", "masked"):
                await self._channel.send(replace(event, text=result.text))
            else:
                await self._channel.send(
                    GuardBlocked(event.conversation_key, "output", _output_reason(result))
                )
        elif isinstance(event, ToolStarted | ToolFinished):
            result = await self._sanitize(event.summary) if event.summary else None
            if result is None:
                await self._channel.send(event)
            elif result.status in ("pass", "masked"):
                await self._channel.send(replace(event, summary=result.text))
            else:
                await self._channel.send(replace(event, summary="（表示できません）"))
        else:
            await self._channel.send(event)

    async def _sanitize(self, text: str) -> SanitizeResult:
        try:
            return await self._sanitizer.sanitize(text)
        except Exception:
            log.exception("output sanitizer failed")
            return SanitizeResult("error", "", "sanitizer failure")


def _output_reason(result: SanitizeResult) -> str:
    return OUTPUT_CHECK_FAILED if result.status == "error" else OUTPUT_BLOCKED
