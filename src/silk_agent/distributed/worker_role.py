"""Production composition for the Worker role: Agent SDK actors for assigned conversations."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

from silk_agent.config import Config, ConfigError
from silk_agent.core.background import BackgroundCommands
from silk_agent.core.clock import SystemClock
from silk_agent.core.commands import CommandTracker
from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.events import (
    AssistantText,
    Heartbeat,
    MessageAppended,
    OutboundEvent,
    ToolFinished,
    ToolStarted,
    TurnStarted,
)
from silk_agent.core.filters import default_chain
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.ports import Clock, SandboxRuntime, SessionStateStore, SkillSource
from silk_agent.core.session import SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.distributed.actor import DistributedConversationActor
from silk_agent.distributed.control_handler import WorkerControlHandler
from silk_agent.distributed.health import RoleHealth, process_memory_collector, serve_health
from silk_agent.distributed.metrics import METRICS
from silk_agent.distributed.outbox_sink import SanitizedOutboxSink
from silk_agent.distributed.quiesce import QuiesceResponder
from silk_agent.distributed.worker import DistributedWorker
from silk_agent.distributed.worker_heartbeat import WorkerHeartbeat, WorkerIdentity
from silk_agent.distributed.worker_runtime import WorkerProcessRuntime
from silk_agent.hooks import HookDispatcher
from silk_agent.sandbox.monitor import CommandMonitor
from silk_agent.wiring import build_engine_factory, build_input_guard, build_output_sanitizer

log = logging.getLogger(__name__)
EngineFactoryBuilder = Callable[..., Any]
# Output that can carry agent-written content; a STOP invalidates these for older turns.
FENCED_EVENTS = (AssistantText, ToolStarted, ToolFinished, MessageAppended, Heartbeat, TurnStarted)


class _SinkChannel:
    """Notices that bypass AgentSession still travel through the sanitized Outbox."""

    def __init__(self, sink: Callable[[OutboundEvent], Awaitable[None]]) -> None:
        self._sink = sink

    async def send(self, event: OutboundEvent) -> None:
        await self._sink(event)


class WorkerActorFactory:
    """Build one fenced conversation actor (own orchestrator and AgentSession) per ownership."""

    def __init__(
        self,
        *,
        config: Config,
        clock: Clock,
        inbox: Any,
        conversations: Any,
        outbox_writer: Any,
        rate_limits: Any,
        command_monitor_store: Any,
        runtime: SandboxRuntime,
        session_store: Any | None,
        skills: SkillSource | None,
        work_root: Path,
        hooks: HookDispatcher | None = None,
        output_reader: Any | None = None,
        monitor_id: str,
        build_engine_factory_fn: EngineFactoryBuilder = build_engine_factory,
        on_rebound: Callable[[OwnershipToken], Awaitable[Any]] | None = None,
        poll_seconds: float = 1.0,
    ) -> None:
        self._config = config
        self._clock = clock
        self._inbox = inbox
        self._conversations = conversations
        self._outbox_writer = outbox_writer
        self._rate_limits = rate_limits
        self._command_monitor_store = command_monitor_store
        self._runtime = runtime
        self._session_store = session_store
        self._skills = skills
        self._work_root = work_root
        self._hooks = hooks
        self._output_reader = output_reader
        self._monitor_id = monitor_id
        self._build_engine_factory = build_engine_factory_fn
        self.on_rebound = on_rebound
        self._poll_seconds = poll_seconds
        self.actors: dict[str, DistributedConversationActor] = {}

    # ------------------------------------------------------------ lookups used by adapters

    def owner_for_sync(self, conversation_key: str) -> OwnershipToken | None:
        actor = self.actors.get(conversation_key)
        return actor.owner if actor is not None else None

    async def owner_for(self, conversation_key: str) -> OwnershipToken | None:
        return self.owner_for_sync(conversation_key)

    async def identity(self, conversation_key: str) -> Mapping[str, Any]:
        actor = self.actors.get(conversation_key)
        if actor is None:
            raise RuntimeError("conversation has no active Worker actor")
        return actor.identity()

    async def origin_sequence(self, event: OutboundEvent) -> int | None:
        """Stop-barrier fence for agent-derived output only.

        Terminal and status events of an interrupted turn, and the notices that report a
        stop, carry no agent content and must still reach the user; fencing them would leave
        the Slack progress view stuck on "running".
        """
        if not isinstance(event, FENCED_EVENTS):
            return None
        actor = self.actors.get(getattr(event, "conversation_key", ""))
        return actor.identity()["origin_seq"] if actor is not None else None

    # ------------------------------------------------------------ actor construction

    def __call__(self, token: OwnershipToken) -> DistributedConversationActor:
        return self._build(token, control_only=False)

    def control_only(self, token: OwnershipToken) -> DistributedConversationActor:
        return self._build(token, control_only=True)

    def _build(self, token: OwnershipToken, *, control_only: bool) -> DistributedConversationActor:
        config = self._config
        key = token.conversation_key
        sink = SanitizedOutboxSink(
            self._outbox_writer,
            self._clock,
            self.owner_for,
            self._conversations.allocate_output_sequence,
            self.origin_sequence,
        )
        state_store = _state_store(self._conversations, self.owner_for_sync)
        tracker = CommandTracker(state_store)
        monitor = CommandMonitor(
            self._command_monitor_store,
            self._runtime,
            self._clock,
            monitor_id=self._monitor_id,
            output_reader=self._output_reader,
        )
        background = BackgroundCommands(self._runtime, tracker, command_monitor=monitor)

        record_query_start = query_start_recorder(
            self._inbox, self._clock, lambda: self.owner_for_sync(key) or token
        )

        engine_factory = self._build_engine_factory(
            config,
            self._runtime,
            self._clock,
            work_root=self._work_root / "work",
            session_store=self._session_store,
            skills=self._skills,
            tracker=tracker,
            background=background,
            on_query_start=record_query_start,
            on_hook_event=self._hooks.publish if self._hooks is not None else None,
        )
        rate_limit = config.filter.rate_limit_per_minute

        async def allow_rate(message: Any) -> bool:
            return await self._rate_limits.allow_and_record(
                user_key=message.user,
                input_id=message.input_id or message.message_id or key,
                now=self._clock.now(),
                per_minute=rate_limit,
            )

        async def record_owner(message: Any) -> None:
            email = message.sender.email if message.sender else ""
            await self._conversations.set_initial_owner_once(
                owner=self.owner_for_sync(key) or token,
                owner_user=message.user,
                owner_email=email,
            )

        async def load_history(conversation_key: str):
            return await self._conversations.load_guard_history(conversation_key)

        async def record_history(message: Any) -> bool:
            return bool(
                await self._conversations.append_guard_history(
                    owner=self.owner_for_sync(key) or token, text=message.text
                )
            )

        async def orphan(conversation_key: str) -> None:
            await background.orphan_all(conversation_key)

        orchestrator = ConversationOrchestrator(
            engine_factory,
            _SinkChannel(sink),
            self._clock,
            guard=build_input_guard(config),
            sanitizer=build_output_sanitizer(config),
            stop_words=StopWords(config.slack.stop_words),
            filters=default_chain(
                self._clock,
                teams=config.filter.allowed_teams,
                channels=config.filter.allowed_channels,
                users=config.filter.allowed_users,
                allow_direct_messages=config.filter.allow_direct_messages,
                deny_guests=config.filter.deny_guests,
                deny_external_users=config.filter.deny_external_users,
                max_chars=config.filter.max_message_chars,
                per_minute=rate_limit,
            ),
            guard_on_error=config.input_guard.on_error,
            settings=SessionSettings(
                interrupt_mode=config.slack.interrupt_mode,
                heartbeat_interval_seconds=config.slack.progress.heartbeat_interval_seconds,
                idle_timeout_seconds=config.agent.idle_client_timeout_seconds,
            ),
            state_store=state_store,
            bot_instance_id=token.worker_incarnation,
            orphan_commands=orphan,
            cancel_commands=self._runtime.cancel_commands,
            cancel_background=background.cancel_all,
            notice_source=self._runtime.pop_notices,
            notice_ack=self._runtime.ack_notices,
            sandbox_ready=self._runtime.is_ready,
            hooks=self._hooks,
            guard_history_loader=load_history,
            guard_history_recorder=record_history,
            distributed_rate_limit=allow_rate,
            initial_owner_recorder=record_owner,
            outbox_sink=sink,
        )
        background.on_finished = orchestrator.notify

        holder: dict[str, DistributedConversationActor] = {}

        async def read_stop_seq() -> int:
            conversation = await self._conversations.get_conversation(key)
            value = conversation.get("stop_seq", 0) if conversation else 0
            return value if isinstance(value, int) and not isinstance(value, bool) else 0

        async def update_actor(new_owner: OwnershipToken) -> None:
            if self.on_rebound is not None:
                await self.on_rebound(new_owner)
            else:
                await holder["actor"].rebind_after_reset(new_owner)

        handler = WorkerControlHandler(
            inbox=self._inbox,
            conversations=self._conversations,
            clock=self._clock,
            orchestrator=orchestrator,
            background=background,
            owner=lambda: holder["actor"].owner,
            session=lambda: holder["actor"].session,
            update_actor=update_actor,
        )
        actor = _RegisteredActor(
            self,
            token,
            self._inbox,
            orchestrator,
            read_controls=handler.read,
            handle_control=handler.handle,
            command_monitor=monitor,
            poll_seconds=self._poll_seconds,
            control_only=control_only,
            read_stop_seq=read_stop_seq,
        )
        holder["actor"] = actor
        return actor


class _RegisteredActor(DistributedConversationActor):
    """Actor that is visible to the Sandbox identity / Outbox owner adapters while running."""

    def __init__(self, factory: WorkerActorFactory, token: OwnershipToken, *args, **kwargs):
        super().__init__(token, *args, **kwargs)
        self._factory = factory

    async def start(self) -> None:
        self._factory.actors[self._owner.conversation_key] = self
        try:
            await super().start()
        except BaseException:
            self._factory.actors.pop(self._owner.conversation_key, None)
            raise

    async def aclose(self) -> None:
        try:
            await super().aclose()
        finally:
            if self._factory.actors.get(self._owner.conversation_key) is self:
                del self._factory.actors[self._owner.conversation_key]


class _HealthEndpoint:
    def __init__(self, health: RoleHealth, port: int) -> None:
        self._health = health
        self._port = port

    async def run(self, stop: asyncio.Event) -> None:
        await serve_health(self._health, self._port, stop)


def _state_store(conversations: Any, owner_for: Callable[[str], OwnershipToken | None]):
    from silk_agent.stores.firestore.session_state import FirestoreSessionStateStore

    store: SessionStateStore = FirestoreSessionStateStore(conversations, owner_for)
    return store


async def run_worker(config: Config) -> None:
    """Run the Worker role. It never receives Slack credentials."""
    if config.execution_mode != "distributed":
        raise ConfigError("worker requires execution_mode: distributed")
    config.validate_required()
    if config.session_store.provider != "agent_platform":
        raise ConfigError("worker requires session_store.provider: agent_platform")
    from silk_agent.app import build_skill_source
    from silk_agent.sandbox.output_archive import GCSCommandOutputStore
    from silk_agent.sandbox.remote import RemoteSandboxRuntime, RemoteSandboxSessions
    from silk_agent.skills.sources import run_skill_sync
    from silk_agent.stores.agent_platform import AgentPlatformSessions, AgentPlatformSessionStore
    from silk_agent.stores.firestore.client import (
        create_firestore_client,
        create_watch_firestore_client,
    )
    from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore
    from silk_agent.stores.firestore.conversations import FirestoreConversationStore
    from silk_agent.stores.firestore.inbox import FirestoreInboxStore
    from silk_agent.stores.firestore.outbox import FirestoreOutboxWriter
    from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
    from silk_agent.stores.firestore.quiesce import FirestoreQuiesceMailbox
    from silk_agent.stores.firestore.rate_limits import FirestoreUserRateLimitStore
    from silk_agent.stores.firestore.watch import worker_reservation_watch
    from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

    dist = config.distributed
    identity = WorkerIdentity.from_environment()
    clock = SystemClock()
    prefix = dist.firestore_prefix
    client = create_firestore_client(dist)
    watch_client = create_watch_firestore_client(dist)
    sessions_api = AgentPlatformSessions(
        config.session_store.agent_runtime,
        project=config.session_store.project,
        location=config.session_store.location,
        ttl_seconds=config.session_store.ttl_seconds,
    )
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    # The SDK keys stored history by working directory, so every Worker must use the same
    # path; a per-process temp directory made resume on another Worker fail
    # ("No conversation found with session ID").
    work_dir = Path(os.environ.get("SILK_AGENT_WORKDIR", "/tmp/silk-agent-worker"))
    await asyncio.to_thread(work_dir.mkdir, parents=True, exist_ok=True)
    skills = build_skill_source(config, work_dir / "skills-cache")
    from silk_agent.hooks import load_hooks

    hook_config = config.hook_delivery
    hooks = HookDispatcher(
        load_hooks(config),
        clock,
        queue_size=hook_config.queue_size,
        hook_timeout_seconds=hook_config.timeout_seconds,
    )
    factory_holder: dict[str, WorkerActorFactory] = {}
    runtime = RemoteSandboxRuntime(
        RemoteSandboxSessions(dist.sandbox_service_url),
        lambda key: factory_holder["factory"].identity(key),
    )
    factory = WorkerActorFactory(
        config=config,
        clock=clock,
        inbox=FirestoreInboxStore(client, collection_prefix=prefix),
        conversations=conversations,
        outbox_writer=FirestoreOutboxWriter(client, collection_prefix=prefix),
        rate_limits=FirestoreUserRateLimitStore(client, collection_prefix=prefix),
        command_monitor_store=FirestoreCommandMonitorStore(client, collection_prefix=prefix),
        runtime=runtime,
        session_store=AgentPlatformSessionStore(sessions_api),
        skills=skills,
        work_root=work_dir,
        hooks=hooks,
        output_reader=GCSCommandOutputStore(dist.command_output_bucket),
        monitor_id=identity.worker_id,
    )
    factory_holder["factory"] = factory

    async def is_control_only(key: str) -> bool:
        record = await conversations.get_conversation(key)
        return bool(record and record.get("control_only") is True)

    async def drain_actor(actor: Any, grace_seconds: float | None) -> bool:
        return await actor.drain(grace_seconds)

    health = RoleHealth("worker")

    def usage_collector(metrics: Any) -> None:
        reserved, active = worker.usage
        metrics.set("worker_reserved", reserved)
        metrics.set("worker_active_sdk", active)
        metrics.set("worker_capacity", dist.max_active_sdks_per_worker)

    METRICS.add_collector(usage_collector)
    METRICS.add_collector(process_memory_collector)
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id=identity.worker_id,
        worker_incarnation=identity.incarnation,
        actor_factory=factory,
        control_actor_factory=factory.control_only,
        is_control_only=is_control_only,
        subscribe=worker_reservation_watch(
            watch_client,
            collection_prefix=prefix,
            worker_id=identity.worker_id,
            incarnation=identity.incarnation,
        ),
        worker_directory=directory,
        drain_actor=drain_actor,
        lease_seconds=dist.lease_seconds,
        renew_seconds=dist.lease_renewal_seconds,
        idle_close_seconds=dist.idle_close_seconds,
        on_tick=health.loop("assignments", 3 * dist.lease_renewal_seconds),
    )
    factory.on_rebound = worker.rebind_actor
    heartbeat = WorkerHeartbeat(
        directory,
        clock,
        identity,
        capacity=dist.max_active_sdks_per_worker,
        usage=lambda: worker.usage,
        interval_seconds=dist.lease_renewal_seconds,
        on_tick=health.loop("heartbeat", 3 * dist.lease_renewal_seconds),
    )
    responder = QuiesceResponder(
        FirestoreQuiesceMailbox(client, collection_prefix=prefix),
        worker,
        clock,
        worker_id=identity.worker_id,
        incarnation=identity.incarnation,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-unix
            pass
    background: list[asyncio.Task[None]] = []
    if skills is not None:
        try:
            await skills.refresh()
        except Exception:
            log.warning("initial skill sync failed; continuing without skills")
        background.append(
            asyncio.create_task(
                run_skill_sync(skills, clock, config.skills.sync_interval_seconds, stop=stop)
            )
        )
    try:
        await WorkerProcessRuntime(
            worker, heartbeat, (responder, _HealthEndpoint(health, dist.health_port))
        ).run(stop)
    finally:
        stop.set()
        await hooks.aclose(hook_config.shutdown_timeout_seconds)
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        watch_client.close()
        client.close()


def query_start_recorder(
    inbox: Any, clock: Clock, owner: Callable[[], OwnershipToken]
) -> Callable[[str, str, str, str], Awaitable[None]]:
    """The Engine's ``on_query_start``: persist the session/input/attempt association before the
    SDK gets the prompt.

    Inputs without an attempt (internal prompts) have nothing to associate. A refusal means this
    Worker no longer owns the input (lost lease, new owner, a reset that settled it): the prompt
    must not be sent, and the turn ends as unknown rather than running under a stale lease. An
    applied stop is handled by the session before the query, not here. A store error is not told
    apart from a refusal: it also ends the turn, and the input is settled as ``uncertain``.
    """

    async def record(
        conversation_key: str, session_id: str, input_id: str, attempt_id: str
    ) -> None:
        if not input_id or not attempt_id:
            return
        recorded = await inbox.record_query_start(
            conversation_key=conversation_key,
            input_id=input_id,
            owner=owner(),
            dispatch_attempt_id=attempt_id,
            sdk_session_id=session_id,
            now=clock.now(),
        )
        if not recorded:
            raise RuntimeError("the query start was refused: the input is no longer ours")

    return record
