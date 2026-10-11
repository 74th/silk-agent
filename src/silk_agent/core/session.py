"""AgentSession (one per conversation) and AgentSessionRegistry."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from silk_agent.core.events import (
    Heartbeat,
    InterruptReason,
    MessageAppended,
    StatusChanged,
    TurnFailed,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    UserMessage,
)
from silk_agent.core.ports import (
    AgentEngine,
    AgentEngineFactory,
    Clock,
    ConversationState,
    EventSink,
    SessionStateStore,
    TurnRequest,
)

log = logging.getLogger(__name__)

SessionState = Literal["idle", "running", "interrupting"]

SYSTEM_USER = "system"  # author of notices that the agent receives as a message


@dataclass(frozen=True)
class SessionSettings:
    # append: add to the running turn (no interruption) / interrupt: stop it and start a
    # new turn / queue: run after the current turn.
    interrupt_mode: Literal["append", "interrupt", "queue"] = "append"
    heartbeat_interval_seconds: float = 60
    idle_timeout_seconds: float = 600


async def _cancel_and_wait(task: asyncio.Task[None]) -> None:
    """Cancel `task` and wait for it, without swallowing a cancellation of the caller."""
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
    except Exception:
        log.exception("task failed while being cancelled")


class AgentSession:
    """Owns at most one AgentEngine and runs the conversation's turns one at a time.

    All user input goes through `submit`; only the worker task creates turns, so a
    second engine instance can never be started for the same conversation.
    """

    def __init__(
        self,
        key: str,
        engine_factory: AgentEngineFactory,
        sink: EventSink,
        clock: Clock,
        settings: SessionSettings | None = None,
        *,
        state_store: SessionStateStore | None = None,
        bot_instance_id: str = "",
        cancel_commands: Callable[[], Awaitable[None]] | None = None,
        notice_source: Callable[[str], Awaitable[list[str]]] | None = None,
        notice_ack: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self.key = key
        self._notice_source = notice_source
        self._notice_ack = notice_ack
        self._engine_factory = engine_factory
        self._engine: AgentEngine | None = None
        self._sink = sink
        self._clock = clock
        self._settings = settings or SessionSettings()
        self._state_store = state_store
        self._bot_instance_id = bot_instance_id
        self._cancel_commands = cancel_commands

        self._queue: asyncio.Queue[UserMessage] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        self._idle_task: asyncio.Task[None] | None = None
        self._state: SessionState = "idle"
        self._turn_id: str | None = None
        self._interrupt_reason: InterruptReason | None = None
        self._turn_done = asyncio.Event()
        self._turn_done.set()
        self._closing = False

    # ------------------------------------------------------------ properties

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def turn_id(self) -> str | None:
        return self._turn_id

    @property
    def engine(self) -> AgentEngine | None:
        return self._engine

    # ------------------------------------------------------------ input

    async def submit(self, message: UserMessage) -> None:
        """Hand a user message to this session (never starts a second engine)."""
        if self._closing:
            return
        self._cancel_idle_timer()
        running = self._state != "idle"
        if (
            self._state == "running"
            and self._settings.interrupt_mode == "append"
            and self._engine is not None
            and await self._engine.append(message.text)
        ):
            # Shown in the progress message, not as a separate post.
            await self._sink(MessageAppended(self.key, self._turn_id or ""))
            return
        self._queue.put_nowait(message)
        self._ensure_worker()
        if running and self._settings.interrupt_mode == "interrupt":
            await self._interrupt("new_message")

    async def notify(self, text: str) -> None:
        """A system notice: added to the running turn, otherwise run as a turn of its own.
        It never interrupts a turn."""
        if self._closing:
            return
        if (
            self._state == "running"
            and self._engine is not None
            and await self._engine.append(text)
        ):
            return
        self._cancel_idle_timer()
        self._queue.put_nowait(UserMessage(self.key, SYSTEM_USER, text))
        self._ensure_worker()

    async def stop(self) -> bool:
        """Interrupt the running turn. Returns False if nothing was running."""
        dropped = self._drain_queue()
        if self._state == "idle":
            return dropped > 0
        await self._interrupt("user")
        await self._turn_done.wait()
        return True

    async def reset(self) -> None:
        """Forget the history: the next turn starts a fresh agent session."""
        await self.stop()
        if self._engine is not None:
            await self._engine.reset()
        if self._state_store is not None:
            state = await self._state_store.get(self.key) or ConversationState(self.key)
            state.agent_session_id = None
            await self._state_store.put(state)

    async def shutdown(self, grace_seconds: float) -> None:
        """Stop accepting input; let a running turn finish within the grace period,
        otherwise interrupt it (reason "shutdown")."""
        self._closing = True
        self._drain_queue()
        self._cancel_idle_timer()
        if self._state != "idle":
            if not await self._wait_turn_done(grace_seconds):
                await self._interrupt("shutdown")
                await self._turn_done.wait()
        await self._cancel_worker()
        if self._engine is not None:
            await self._engine.close()
            self._engine = None

    async def aclose(self) -> None:
        self._closing = True
        await self._cancel_worker()
        self._cancel_idle_timer()
        if self._engine is not None:
            await self._engine.close()
            self._engine = None

    # ------------------------------------------------------------ internals

    async def _wait_turn_done(self, timeout_seconds: float) -> bool:
        """Wait for the running turn on the session's Clock. True if it finished in time."""
        done = asyncio.ensure_future(self._turn_done.wait())
        timer = asyncio.ensure_future(self._clock.sleep(timeout_seconds))
        try:
            await asyncio.wait({done, timer}, return_when=asyncio.FIRST_COMPLETED)
            return done.done()
        finally:
            for task in (done, timer):
                task.cancel()

    def _drain_queue(self) -> int:
        n = 0
        while not self._queue.empty():
            self._queue.get_nowait()
            n += 1
        return n

    def _ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run(), name=f"agent-session-{self.key}")

    async def _cancel_worker(self) -> None:
        if self._worker is not None:
            worker, self._worker = self._worker, None
            await _cancel_and_wait(worker)

    async def _interrupt(self, reason: InterruptReason) -> None:
        if self._state == "idle":
            return
        # A stop / shutdown must not be downgraded to "new_message".
        if self._interrupt_reason is None or reason in ("user", "shutdown"):
            self._interrupt_reason = reason
        if self._state == "running":
            self._state = "interrupting"
            if self._turn_id:
                await self._sink(StatusChanged(self.key, self._turn_id, "interrupting"))
        if self._engine is not None:
            await self._engine.interrupt()
        if reason in ("user", "shutdown") and self._cancel_commands is not None:
            try:
                await self._cancel_commands()
            except Exception:
                log.exception("failed to cancel sandbox commands for %s", self.key)

    async def _run(self) -> None:
        while True:
            message = await self._queue.get()
            self._state = "running"
            self._interrupt_reason = None
            self._turn_done.clear()
            try:
                await self._run_turn(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("turn crashed for %s", self.key)
            finally:
                self._state = "idle"
                self._turn_id = None
                self._turn_done.set()
            if self._queue.empty():
                self._start_idle_timer()

    async def _run_turn(self, message: UserMessage) -> None:
        turn_id = f"turn-{uuid.uuid4().hex[:12]}"
        self._turn_id = turn_id
        if self._engine is None:
            self._engine = self._engine_factory(self.key)
        engine = self._engine

        prior = await self._load_state()
        notices: list[str] = []
        if prior is not None and prior.status in ("interrupted_crash", "interrupted_shutdown"):
            notices.append(resume_notice(prior))
        external_notices = await self._pending_notices()
        notices.extend(external_notices)
        await self._save_state("running", turn_id, clear_commands=bool(notices))
        await self._sink(TurnStarted(self.key, turn_id, self._clock.now()))
        heartbeat = asyncio.create_task(self._heartbeat(turn_id))
        terminal = False
        try:
            request = TurnRequest(
                self.key,
                turn_id,
                message.text,
                tuple(notices),
                prior.agent_session_id if prior is not None else None,
            )
            acked = not external_notices
            async for event in engine.run_turn(request):
                if not acked:
                    # The first event proves the engine accepted the prompt (query succeeded).
                    acked = True
                    await self._ack_notices()
                if isinstance(event, TurnFinished | TurnFailed):
                    await self._stop_task(heartbeat)
                    terminal = True
                await self._sink(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("engine failed for %s", self.key)
            await self._stop_task(heartbeat)
            terminal = True
            await self._sink(TurnFailed(self.key, turn_id, f"{type(e).__name__}: {e}"))
        finally:
            await self._stop_task(heartbeat)

        agent_session_id = getattr(engine, "session_id", None)
        reason = self._interrupt_reason
        if reason is not None and not terminal:
            await self._sink(StatusChanged(self.key, turn_id, "interrupted"))
            await self._sink(TurnInterrupted(self.key, turn_id, reason, self._clock.now()))
            await self._save_state(
                "interrupted_shutdown" if reason == "shutdown" else "idle",
                turn_id,
                agent_session_id=agent_session_id,
            )
        else:
            await self._save_state("idle", turn_id, agent_session_id=agent_session_id)

    async def _heartbeat(self, turn_id: str) -> None:
        interval = self._settings.heartbeat_interval_seconds
        while True:
            await self._clock.sleep(interval)
            await self._sink(Heartbeat(self.key, turn_id, self._clock.now()))

    @staticmethod
    async def _stop_task(task: asyncio.Task[None]) -> None:
        await _cancel_and_wait(task)

    def _start_idle_timer(self) -> None:
        if self._engine is None or self._settings.idle_timeout_seconds <= 0:
            return
        self._cancel_idle_timer()
        self._idle_task = asyncio.create_task(self._idle_close())

    def _cancel_idle_timer(self) -> None:
        if self._idle_task is not None and self._idle_task is not asyncio.current_task():
            self._idle_task.cancel()
        self._idle_task = None

    async def _idle_close(self) -> None:
        await self._clock.sleep(self._settings.idle_timeout_seconds)
        if self._state == "idle" and self._engine is not None:
            engine, self._engine = self._engine, None
            await engine.close()

    async def _pending_notices(self) -> list[str]:
        if self._notice_source is None:
            return []
        try:
            return await self._notice_source(self.key)
        except Exception:
            log.exception("could not read notices for %s", self.key)
            return []

    async def _ack_notices(self) -> None:
        if self._notice_ack is None:
            return
        try:
            await self._notice_ack(self.key)
        except Exception:
            log.exception("could not ack notices for %s", self.key)

    async def _load_state(self) -> ConversationState | None:
        if self._state_store is None:
            return None
        try:
            return await self._state_store.get(self.key)
        except Exception:
            log.exception("failed to load state for %s", self.key)
            return None

    async def _save_state(
        self,
        status: str,
        turn_id: str,
        *,
        agent_session_id: str | None = None,
        clear_commands: bool = False,
    ) -> None:
        if self._state_store is None:
            return
        try:
            state = await self._state_store.get(self.key) or ConversationState(self.key)
            state.status = status  # type: ignore[assignment]
            state.turn_id = turn_id
            state.bot_instance_id = self._bot_instance_id or state.bot_instance_id
            state.updated_at = self._clock.now()
            if agent_session_id:
                state.agent_session_id = agent_session_id
            if clear_commands:
                state.running_command_ids = []
            await self._state_store.put(state)
        except Exception:
            log.exception("failed to save state for %s", self.key)


def resume_notice(prior: ConversationState) -> str:
    cause = "Botの再起動" if prior.status == "interrupted_shutdown" else "Botの停止"
    text = (
        f"前回のturnは{cause}により途中で中断されました。"
        "新しいメッセージに応える前に、作業の状態（ファイルや実行結果）を確認してください。"
    )
    if prior.running_command_ids:
        ids = ", ".join(prior.running_command_ids)
        text += (
            f" 中断時に実行中だったSandboxコマンドのID: {ids}。"
            "command_status と command_output で状態を確認し、"
            "確認する前に同じコマンドを再実行しないでください。"
        )
    return text


class AgentSessionRegistry:
    """conversation_key -> AgentSession. Creation is serialized by a lock."""

    def __init__(
        self,
        engine_factory: AgentEngineFactory,
        sink: EventSink,
        clock: Clock,
        settings: SessionSettings | None = None,
        *,
        state_store: SessionStateStore | None = None,
        bot_instance_id: str = "",
        cancel_commands: Callable[[str], Awaitable[None]] | None = None,
        notice_source: Callable[[str], Awaitable[list[str]]] | None = None,
        notice_ack: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._notice_source = notice_source
        self._notice_ack = notice_ack
        self._engine_factory = engine_factory
        self._sink = sink
        self._clock = clock
        self._settings = settings
        self._state_store = state_store
        self._bot_instance_id = bot_instance_id
        self._cancel_commands = cancel_commands
        self._sessions: dict[str, AgentSession] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(self, key: str) -> AgentSession:
        async with self._lock:
            session = self._sessions.get(key)
            if session is None:
                cancel = self._cancel_commands
                session = AgentSession(
                    key,
                    self._engine_factory,
                    self._sink,
                    self._clock,
                    self._settings,
                    state_store=self._state_store,
                    bot_instance_id=self._bot_instance_id,
                    cancel_commands=(lambda k=key: cancel(k)) if cancel else None,
                    notice_source=self._notice_source,
                    notice_ack=self._notice_ack,
                )
                self._sessions[key] = session
            return session

    def get(self, key: str) -> AgentSession | None:
        return self._sessions.get(key)

    def sessions(self) -> list[AgentSession]:
        return list(self._sessions.values())

    async def shutdown(self, grace_seconds: float) -> None:
        await asyncio.gather(*(s.shutdown(grace_seconds) for s in self.sessions()))

    async def aclose(self) -> None:
        await asyncio.gather(*(s.aclose() for s in self.sessions()))
