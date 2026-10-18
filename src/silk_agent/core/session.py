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
    Notice,
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
SubmissionOutcome = Literal["queued", "appended", "rejected"]
InputCompletion = Callable[[str], Awaitable[None]]
InputStart = Callable[[str], Awaitable[bool]]
InputQueue = Callable[[str], Awaitable[bool]]
InputUncertain = Callable[[str], Awaitable[None]]
# (input_id, reason): a turn ended by an interrupt; the reason says why (user, reset,
# new_message, shutdown, drain, lease_lost) so the durable state can be settled.
InputInterrupted = Callable[[str, str], Awaitable[None]]
# input ids removed from the in-memory queue by a stop; their durable state is untouched.
InputsDropped = Callable[[list[str]], Awaitable[None]]

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
        orphan_commands: Callable[[], Awaitable[None]] | None = None,
        notice_source: Callable[[str], Awaitable[list[str]]] | None = None,
        notice_ack: Callable[[str], Awaitable[None]] | None = None,
        sandbox_ready: Callable[[str], Awaitable[bool]] | None = None,
        input_completed: InputCompletion | None = None,
        input_started: InputStart | None = None,
        input_uncertain: InputUncertain | None = None,
        input_interrupted: InputInterrupted | None = None,
        inputs_dropped: InputsDropped | None = None,
    ) -> None:
        self.key = key
        self._sandbox_ready = sandbox_ready
        self._input_completed = input_completed
        self._input_started = input_started
        self._input_uncertain = input_uncertain
        self._input_interrupted = input_interrupted
        self._inputs_dropped = inputs_dropped
        self._dropped_input_ids: list[str] = []
        self._notice_source = notice_source
        self._notice_ack = notice_ack
        self._engine_factory = engine_factory
        self._engine: AgentEngine | None = None
        self._sink = sink
        self._clock = clock
        self._settings = settings or SessionSettings()
        self._state_store = state_store
        self._state_save_failed = False
        self._bot_instance_id = bot_instance_id
        self._cancel_commands = cancel_commands
        self._orphan_commands = orphan_commands

        self._queue: asyncio.Queue[UserMessage] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        self._idle_task: asyncio.Task[None] | None = None
        self._state: SessionState = "idle"
        self._turn_id: str | None = None
        self._interrupt_reason: InterruptReason | None = None
        self._turn_done = asyncio.Event()
        self._turn_done.set()
        self._closing = False
        self._turn_input_ids: list[str] = []
        self._last_turn_interrupted = False

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

    async def submit(
        self, message: UserMessage, *, before_queue: InputQueue | None = None
    ) -> SubmissionOutcome:
        """Hand a user message to this session (never starts a second engine)."""
        if self._closing:
            return "rejected"
        self._last_turn_interrupted = False
        self._cancel_idle_timer()
        running = self._state != "idle"
        if (
            self._state == "running"
            and self._settings.interrupt_mode == "append"
            and self._engine is not None
            and await self._engine.append(message.text)
        ):
            # Shown in the progress message, not as a separate post.
            if message.input_id and message.input_id not in self._turn_input_ids:
                self._turn_input_ids.append(message.input_id)
            await self._sink(MessageAppended(self.key, self._turn_id or ""))
            return "appended"
        if before_queue is not None and message.input_id:
            if not await before_queue(message.input_id):
                return "rejected"
        self._queue.put_nowait(message)
        self._ensure_worker()
        if running and self._settings.interrupt_mode == "interrupt":
            await self._interrupt("new_message")
        return "queued"

    async def notify(
        self, text: str, *, input_id: str = ""
    ) -> Literal["appended", "queued", "held"]:
        """Deliver a system notice without interrupting a running turn."""
        if self._closing or self._state == "interrupting" or self._last_turn_interrupted:
            await self._sink(
                Notice(self.key, "バックグラウンド処理が完了しました。結果は保留中です。")
            )
            return "held"
        if (
            self._state == "running"
            and self._engine is not None
            and await self._engine.append(text)
        ):
            if input_id:
                if input_id not in self._turn_input_ids:
                    self._turn_input_ids.append(input_id)
            return "appended"
        self._cancel_idle_timer()
        self._queue.put_nowait(UserMessage(self.key, SYSTEM_USER, text, input_id=input_id))
        self._ensure_worker()
        return "queued"

    async def stop(self, *, reason: Literal["user", "reset"] = "user") -> bool:
        """Interrupt the running turn. Returns False if nothing was running."""
        requested = await self.request_stop(reason=reason)
        if requested and self._state != "idle":
            await self._turn_done.wait()
        return requested

    async def request_stop(self, *, reason: Literal["user", "reset"] = "user") -> bool:
        """Set the stop barrier in this actor and request interrupt without waiting for a turn."""
        dropped = self._drain_queue()
        await self._flush_dropped()
        if self._state == "idle":
            return dropped > 0
        await self._interrupt(reason)
        return True

    async def reset(self) -> None:
        """Forget the history: the next turn starts a fresh agent session."""
        await self.stop(reason="reset")
        if self._engine is not None:
            await self._engine.reset()
        if self._state_store is not None:
            state = await self._state_store.get(self.key) or ConversationState(self.key)
            state.agent_session_id = None
            state.history_reset_reason = None
            await self._state_store.put(state)

    async def shutdown(
        self,
        grace_seconds: float | None = None,
        *,
        reason: Literal["shutdown", "drain"] = "shutdown",
    ) -> None:
        """Stop accepting input; let a running turn finish within the grace period,
        otherwise interrupt it (reason "shutdown")."""
        self._closing = True
        self._drain_queue()
        await self._flush_dropped()
        self._cancel_idle_timer()
        if self._state != "idle":
            if not await self._wait_turn_done(grace_seconds):
                await self._interrupt(reason)
                await self._turn_done.wait()
        await self._cancel_worker()
        if self._engine is not None:
            await self._engine.close()
            self._engine = None
        if self._state_save_failed:
            raise RuntimeError("conversation state was not saved; ownership must remain held")

    async def aclose(
        self, *, reason: Literal["lease_lost", "drain", "shutdown"] = "lease_lost"
    ) -> None:
        self._closing = True
        if self._state != "idle":
            await self._interrupt(reason)
            await self._turn_done.wait()
        await self._cancel_worker()
        self._cancel_idle_timer()
        if self._engine is not None:
            await self._engine.close()
            self._engine = None
        if self._state_save_failed:
            raise RuntimeError("conversation state was not saved; ownership must remain held")

    # ------------------------------------------------------------ internals

    async def _wait_turn_done(self, timeout_seconds: float | None) -> bool:
        """Wait for the running turn on the session's Clock. True if it finished in time."""
        if timeout_seconds is None:
            await self._turn_done.wait()
            return True
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
            message = self._queue.get_nowait()
            n += 1
            if message.input_id:
                self._dropped_input_ids.append(message.input_id)
        return n

    async def _flush_dropped(self) -> None:
        """Tell the owner which durable inputs left the memory queue.

        A stop empties the queue regardless of an input's position, but an input behind the stop
        boundary must still run afterwards: its durable state is still queued, so the owner has
        to hand it over again (those at or before the boundary are cancelled in the store).
        """
        ids, self._dropped_input_ids = self._dropped_input_ids, []
        if ids and self._inputs_dropped is not None:
            try:
                await self._inputs_dropped(ids)
            except Exception:
                log.exception("failed to report dropped inputs for %s", self.key)

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
        if self._interrupt_reason is None or reason in (
            "user",
            "reset",
            "shutdown",
            "drain",
            "lease_lost",
        ):
            self._interrupt_reason = reason
        if self._state == "running":
            self._state = "interrupting"
            if self._turn_id:
                await self._sink(StatusChanged(self.key, self._turn_id, "interrupting"))
        if self._engine is not None:
            await self._engine.interrupt()
        if reason in ("user", "reset") and self._cancel_commands is not None:
            try:
                await self._cancel_commands()
            except Exception:
                log.exception("failed to cancel sandbox commands for %s", self.key)
        elif reason in ("shutdown", "drain", "lease_lost") and self._orphan_commands is not None:
            try:
                await self._orphan_commands()
            except Exception:
                log.exception("failed to orphan sandbox commands for %s", self.key)

    async def _run(self) -> None:
        while True:
            message = await self._queue.get()
            if message.input_id and self._input_started is not None:
                try:
                    if not await self._input_started(message.input_id):
                        continue
                except Exception:
                    log.exception("could not activate inbox input for %s", self.key)
                    continue
            self._state = "running"
            self._interrupt_reason = None
            self._turn_input_ids = [message.input_id] if message.input_id else []
            self._turn_done.clear()
            try:
                outcome = await self._run_turn(message)
                self._last_turn_interrupted = outcome == "interrupted"
                if outcome == "completed" and self._input_completed is not None:
                    for input_id in self._turn_input_ids:
                        try:
                            await self._input_completed(input_id)
                        except Exception:
                            log.exception("failed to complete inbox input for %s", self.key)
                elif outcome == "failed" and self._input_uncertain is not None:
                    for input_id in self._turn_input_ids:
                        try:
                            await self._input_uncertain(input_id)
                        except Exception:
                            log.exception("failed to mark inbox input uncertain for %s", self.key)
                elif outcome == "interrupted" and self._input_interrupted is not None:
                    # Without this the inputs of an interrupted turn stay "accepted" forever:
                    # they hold the conversation's slot and eventually fill the inbox page.
                    for input_id in self._turn_input_ids:
                        try:
                            await self._input_interrupted(
                                input_id, self._interrupt_reason or "unknown"
                            )
                        except Exception:
                            log.exception("failed to settle an interrupted input for %s", self.key)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("turn crashed for %s", self.key)
                if self._input_uncertain is not None:
                    for input_id in self._turn_input_ids:
                        try:
                            await self._input_uncertain(input_id)
                        except Exception:
                            log.exception("failed to mark inbox input uncertain for %s", self.key)
            finally:
                self._state = "idle"
                self._turn_id = None
                self._turn_done.set()
            if self._queue.empty():
                self._start_idle_timer()

    async def _run_turn(
        self, message: UserMessage
    ) -> Literal["completed", "failed", "interrupted"]:
        turn_id = f"turn-{uuid.uuid4().hex[:12]}"
        self._turn_id = turn_id
        if self._engine is None:
            self._engine = self._engine_factory(self.key)
        engine = self._engine

        prior = await self._load_state()
        notices: list[str] = []
        if prior is not None and prior.status in ("interrupted_crash", "interrupted_shutdown"):
            notices.append(resume_notice(prior))
        await self._save_state("running", turn_id, clear_commands=bool(notices))
        # Show the turn at once: preparing the sandbox can take tens of seconds.
        await self._sink(TurnStarted(self.key, turn_id, self._clock.now()))
        heartbeat = asyncio.create_task(self._heartbeat(turn_id))
        terminal = False
        outcome: Literal["completed", "failed", "interrupted"] = "interrupted"
        try:
            external_notices = await self._prepare_sandbox(turn_id)
            if external_notices:
                notices.extend(external_notices)
                await self._save_state("running", turn_id, clear_commands=True)
            # A stop can arrive while sandbox readiness/notices are being prepared,
            # before the engine has started a query that can observe its interrupt.
            # Do not submit that turn after its stop barrier has already been set.
            if self._interrupt_reason is None:
                request = TurnRequest(
                    self.key,
                    turn_id,
                    message.text,
                    tuple(notices),
                    prior.agent_session_id if prior is not None else None,
                    message.input_id,
                    message.dispatch_attempt_id,
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
                        outcome = "completed" if isinstance(event, TurnFinished) else "failed"
                    await self._sink(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("engine failed for %s", self.key)
            await self._stop_task(heartbeat)
            terminal = True
            outcome = "failed"
            await self._sink(TurnFailed(self.key, turn_id, f"{type(e).__name__}: {e}"))
        finally:
            await self._stop_task(heartbeat)

        agent_session_id = getattr(engine, "session_id", None)
        history_reset_reason = getattr(engine, "history_reset_reason", None)
        reason = self._interrupt_reason
        if reason is not None and not terminal:
            await self._sink(StatusChanged(self.key, turn_id, "interrupted"))
            await self._sink(TurnInterrupted(self.key, turn_id, reason, self._clock.now()))
            await self._save_state(
                "interrupted_shutdown" if reason in ("shutdown", "drain", "lease_lost") else "idle",
                turn_id,
                agent_session_id=agent_session_id,
                history_reset_reason=history_reset_reason,
            )
        else:
            await self._save_state(
                "idle",
                turn_id,
                agent_session_id=agent_session_id,
                history_reset_reason=history_reset_reason,
            )
        return outcome

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

    async def _prepare_sandbox(self, turn_id: str) -> list[str]:
        """Start / resume the sandbox (via the notice source) and return its notices.
        While a sandbox that is not running starts, the turn shows "preparing_sandbox"."""
        preparing = False
        if self._notice_source is not None and self._sandbox_ready is not None:
            try:
                preparing = not await self._sandbox_ready(self.key)
            except Exception:
                log.exception("could not check the sandbox of %s", self.key)
        if preparing:
            await self._sink(StatusChanged(self.key, turn_id, "preparing_sandbox"))
        notices = await self._pending_notices()
        if preparing:
            await self._sink(StatusChanged(self.key, turn_id, "thinking"))
        return notices

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
        history_reset_reason: str | None = None,
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
            if history_reset_reason:
                state.history_reset_reason = history_reset_reason
            if clear_commands:
                state.running_command_ids = []
            await self._state_store.put(state)
            self._state_save_failed = False
        except Exception:
            self._state_save_failed = True
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
        orphan_commands: Callable[[str], Awaitable[None]] | None = None,
        notice_source: Callable[[str], Awaitable[list[str]]] | None = None,
        notice_ack: Callable[[str], Awaitable[None]] | None = None,
        sandbox_ready: Callable[[str], Awaitable[bool]] | None = None,
        input_completed: InputCompletion | None = None,
        input_started: InputStart | None = None,
        input_uncertain: InputUncertain | None = None,
        input_interrupted: InputInterrupted | None = None,
        inputs_dropped: InputsDropped | None = None,
    ) -> None:
        self._sandbox_ready = sandbox_ready
        self._input_completed = input_completed
        self._input_started = input_started
        self._input_uncertain = input_uncertain
        self._input_interrupted = input_interrupted
        self._inputs_dropped = inputs_dropped
        self._notice_source = notice_source
        self._notice_ack = notice_ack
        self._engine_factory = engine_factory
        self._sink = sink
        self._clock = clock
        self._settings = settings
        self._state_store = state_store
        self._bot_instance_id = bot_instance_id
        self._cancel_commands = cancel_commands
        self._orphan_commands = orphan_commands
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
                    orphan_commands=(
                        (lambda k=key: self._orphan_commands(k)) if self._orphan_commands else None
                    ),
                    notice_source=self._notice_source,
                    notice_ack=self._notice_ack,
                    sandbox_ready=self._sandbox_ready,
                    input_completed=self._input_completed,
                    input_started=self._input_started,
                    input_uncertain=self._input_uncertain,
                    input_interrupted=self._input_interrupted,
                    inputs_dropped=self._inputs_dropped,
                )
                self._sessions[key] = session
            return session

    def get(self, key: str) -> AgentSession | None:
        return self._sessions.get(key)

    def sessions(self) -> list[AgentSession]:
        return list(self._sessions.values())

    def set_input_tracking_callbacks(
        self,
        *,
        completed: InputCompletion | None,
        started: InputStart | None,
        uncertain: InputUncertain | None,
        interrupted: InputInterrupted | None = None,
        dropped: InputsDropped | None = None,
    ) -> None:
        """Install durable input callbacks before creating any conversation session."""
        if self._sessions:
            raise RuntimeError("input tracking callbacks must be set before sessions are created")
        self._input_completed = completed
        self._input_started = started
        self._input_uncertain = uncertain
        self._input_interrupted = interrupted
        self._inputs_dropped = dropped

    async def shutdown(self, grace_seconds: float | None = None) -> None:
        await asyncio.gather(*(s.shutdown(grace_seconds) for s in self.sessions()))

    async def aclose(self) -> None:
        await asyncio.gather(*(s.aclose() for s in self.sessions()))
