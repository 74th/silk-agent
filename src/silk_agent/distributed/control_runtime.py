"""Production composition for Control's durable conversation allocator."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

from silk_agent.config import Config, ConfigError
from silk_agent.core.clock import SystemClock
from silk_agent.core.ports import Clock
from silk_agent.distributed.metrics import METRICS
from silk_agent.distributed.pod_probe import pod_uid_of, pod_uid_of_incarnation
from silk_agent.stores.firestore.allocator import FirestoreAllocator
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

log = logging.getLogger(__name__)


class RecoveryLoop:
    """Recover expired ownership, but only after the old incarnation proves its SDK stopped."""

    def __init__(
        self,
        ownership: Any,
        coordinator: Any,
        request_quiesce: Any,
        clock: Clock,
        *,
        skew_seconds: int = 5,
        poll_seconds: float = 5,
        batch_size: int = 100,
        on_tick: Callable[[], None] | None = None,
        pod_gone: Callable[[Any], Awaitable[bool]] | None = None,
        current_incarnation: Callable[[str], Awaitable[str | None]] | None = None,
        auto_recover_after_seconds: float = 120,
        notify_blocked: Callable[[Any], Awaitable[None]] | None = None,
        has_demand: Callable[[Any], Awaitable[bool]] | None = None,
        idle_check_seconds: float = 30,
        backoff_base_seconds: float = 5,
        backoff_max_seconds: float = 300,
    ) -> None:
        # Recovery is driven by demand: an expired ownership is only worked on while a user
        # input is waiting for its conversation, and failed attempts back off exponentially.
        self._has_demand = has_demand
        self._idle_check = timedelta(seconds=idle_check_seconds)
        self._backoff_base = backoff_base_seconds
        self._backoff_max = backoff_max_seconds
        self._schedule: dict[str, tuple[int, datetime]] = {}  # key -> (failures, next attempt)
        self._cursor: Any | None = None
        self._cycle_seen: set[str] = set()
        # Waiting for stop evidence, kept apart from the retry schedule: a back-off must not
        # make the wait look resolved.
        self._blocked: set[str] = set()
        self._pod_gone = pod_gone
        self._current_incarnation = current_incarnation
        self._auto_after = timedelta(seconds=auto_recover_after_seconds)
        self._notify_blocked = notify_blocked
        self._on_tick = on_tick
        if skew_seconds < 0 or poll_seconds <= 0 or batch_size <= 0:
            raise ValueError("recovery loop values are invalid")
        self._ownership = ownership
        self._coordinator = coordinator
        self._request_quiesce = request_quiesce
        self._clock = clock
        self._skew = timedelta(seconds=skew_seconds)
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size

    async def run_once(self) -> int:
        before = self._clock.now() - self._skew
        paged = getattr(self._ownership, "list_expired_page", None)
        if paged is not None:
            # Resume after the previous page: expired ownerships nobody is waiting on must not
            # fill a fixed first page and hide the ones that are.
            expired, self._cursor = await paged(
                before=before, limit=self._batch_size, after=self._cursor
            )
        else:
            expired = await self._ownership.list_expired(before=before, limit=self._batch_size)
            self._cursor = None

        now = self._clock.now()
        self._cycle_seen.update(token.conversation_key for token in expired)
        if self._cursor is None:  # the pass over every ownership is complete
            for key in tuple(self._schedule):
                if key not in self._cycle_seen:
                    del self._schedule[key]  # released or renewed: forget its backoff
            self._blocked &= self._cycle_seen

        async def recover(token: Any) -> int:
            key = token.conversation_key
            failures, next_at = self._schedule.get(key, (0, now))
            if next_at > now:
                return 0
            try:
                if self._has_demand is not None and not await self._has_demand(token):
                    self._schedule[key] = (failures, now + self._idle_check)
                    self._blocked.discard(key)
                    return 0
                release = getattr(self._ownership, "release_unactivated", None)
                if release is not None and await release(token, now=self._clock.now()):
                    METRICS.inc("control_recovery_total", result="unactivated")
                    self._blocked.discard(key)
                    return 1

                result = await self._coordinator.recover(
                    token, request_quiesce=self._request_quiesce
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Ownership recovery failed (%s)", type(exc).__name__)
                METRICS.inc("control_recovery_total", result="error")
                self._back_off(key, failures)
                return 0
            state = result.state if result is not None else "skipped"
            if state == "blocked":
                auto = await self._released_with_evidence(token)
                if auto is not None:
                    METRICS.inc("control_recovery_total", result=auto)
                    self._blocked.discard(key)
                    return 1
                self._blocked.add(key)
                await self._tell_user(token)
                self._back_off(key, failures)
            if state != "blocked":
                self._blocked.discard(key)
            METRICS.inc("control_recovery_total", result=state)
            return 1 if state == "released" else 0

        released = sum(await asyncio.gather(*(recover(token) for token in expired)))
        if self._cursor is None:
            # Only ownerships waiting for stop evidence (recovery_blocked), whether or not this
            # pass retried them: an expired ownership nobody waits on is not an alarm.
            METRICS.set("control_recovery_pending", len(self._blocked & self._cycle_seen))
            self._cycle_seen.clear()
        return released

    def _back_off(self, key: str, failures: int) -> None:
        delay = min(self._backoff_base * (2**failures), self._backoff_max)
        self._schedule[key] = (failures + 1, self._clock.now() + timedelta(seconds=delay))

    async def _released_with_evidence(self, token: Any) -> str | None:
        """Release when something proves the old incarnation's SDK is gone, and enough time has
        passed for a partitioned live Worker to have stopped itself after losing its lease.

        Evidence: the Worker id now runs a newer incarnation of the same Pod (the container
        restarted, which ends the old process), or Kubernetes shows the Pod is absent or
        replaced. Returns the metric label of the evidence used, or None."""
        if self._clock.now() - token.expires_at < self._auto_after:
            return None
        try:
            evidence = await self._stop_evidence(token)
            if evidence is None:
                return None
            label, evidence_id = evidence
            released = await self._ownership.confirm_stopped(
                token,
                evidence_id=evidence_id,
                worker_incarnation=token.worker_incarnation,
                now=self._clock.now(),
            )
            return label if released else None
        except Exception as exc:
            log.warning("Automatic recovery failed (%s)", type(exc).__name__)
            return None

    async def _stop_evidence(self, token: Any) -> tuple[str, str] | None:
        if self._current_incarnation is not None:
            current = await self._current_incarnation(token.worker_id)
            uid = pod_uid_of(token)
            if (
                current is not None
                and current != token.worker_incarnation
                and uid is not None
                and pod_uid_of_incarnation(current) == uid
            ):
                return "incarnation_replaced", f"auto:incarnation-replaced:{token.worker_id}"
        if self._pod_gone is not None and await self._pod_gone(token):
            return "pod_absent", f"auto:pod-absent:{token.worker_id}"
        return None

    async def _tell_user(self, token: Any) -> None:
        if self._notify_blocked is None:
            return
        try:
            await self._notify_blocked(token)
        except Exception as exc:
            log.warning("could not post the recovery notice (%s)", type(exc).__name__)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self._on_tick is not None:
                self._on_tick()
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Recovery scan failed (%s)", type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                pass


class OutboxSenderLoop:
    """Deliver pending sanitized outputs; conversations are independent, each one ordered."""

    def __init__(
        self,
        outbox: Any,
        sender: Any,
        *,
        poll_seconds: float = 1,
        batch_size: int = 100,
        concurrency: int = 8,
        max_per_conversation: int = 20,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        self._on_tick = on_tick
        if min(poll_seconds, batch_size, concurrency, max_per_conversation) <= 0:
            raise ValueError("sender loop values must be positive")
        self._outbox = outbox
        self._sender = sender
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size
        self._slots = asyncio.Semaphore(concurrency)
        self._cursor: Any | None = None
        # Outputs sent for one conversation in one pass. A conversation that always has
        # something to send must not keep the pass from reaching the next page.
        self._max_per_conversation = max_per_conversation
        self.backlog = False  # some conversation still had output when its pass ended

    async def run_once(self) -> int:
        self.backlog = False  # a failed listing below must not inherit the previous pass's
        paged = getattr(self._outbox, "list_pending_page", None)
        if paged is not None:
            # Resume after the last page: a stuck conversation must not fill every page.
            keys, self._cursor = await paged(limit=self._batch_size, after=self._cursor)
        else:
            keys = await self._outbox.list_conversations_with_pending(limit=self._batch_size)
        delivered = 0

        async def drain(key: str) -> int:
            count = 0
            async with self._slots:
                try:
                    while count < self._max_per_conversation and await self._sender.run_once(key):
                        count += 1
                    if count >= self._max_per_conversation:
                        self.backlog = True
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # One conversation's failure must not hold back the others.
                    log.warning("Outbox delivery pass failed (%s)", type(exc).__name__)
            return count

        for count in await asyncio.gather(*(drain(key) for key in keys)):
            delivered += count
        return delivered

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self._on_tick is not None:
                self._on_tick()
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Outbox listing failed (%s)", type(exc).__name__)
            if self.backlog:
                await asyncio.sleep(0)  # more to send: go straight to the next page
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                pass


class ControlAllocatorLoop:
    """Allocate waiting conversations using the shared transactional Firestore lease."""

    def __init__(
        self,
        conversations: FirestoreConversationStore,
        workers: FirestoreWorkerDirectory,
        allocator: FirestoreAllocator,
        clock: Clock,
        *,
        allocator_id: str,
        lease_seconds: int = 60,
        worker_lease_seconds: int = 60,
        poll_seconds: float = 1,
        batch_size: int = 100,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        self._on_tick = on_tick
        if not allocator_id or min(lease_seconds, worker_lease_seconds, batch_size) <= 0:
            raise ValueError("allocator identity and positive lease/batch values are required")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self._conversations = conversations
        self._workers = workers
        self._allocator = allocator
        self._clock = clock
        self._allocator_id = allocator_id
        self._lease_seconds = lease_seconds
        self._worker_lease_seconds = worker_lease_seconds
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size

    async def run_once(self) -> int:
        now = self._clock.now()
        candidates = tuple(await self._workers.list_available(now=now))
        waiting = await self._conversations.list_waiting(limit=self._batch_size)
        # Observable even with no Worker: that is exactly when the queue matters.
        METRICS.set("control_waiting_conversations", len(waiting))
        # The page is bounded, so the count above stops at the batch size; say so, and report
        # how long the oldest has waited (which the bound does not hide).
        METRICS.set("control_waiting_conversations_capped", int(len(waiting) >= self._batch_size))
        oldest = getattr(self._conversations, "oldest_waiting_since", None)
        if oldest is not None:
            try:
                since = await oldest()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Oldest waiting lookup failed (%s)", type(exc).__name__)
            else:
                METRICS.set(
                    "control_oldest_waiting_seconds",
                    max(0.0, (now - since).total_seconds()) if since is not None else 0,
                )
        METRICS.set("control_available_workers", len(candidates))
        if not candidates:
            return 0
        assigned = 0
        for conversation_key in waiting:
            token = await self._allocator.allocate(
                allocator_id=self._allocator_id,
                conversation_key=conversation_key,
                candidates=candidates,
                now=now,
                allocator_lease_expires_at=now + timedelta(seconds=self._lease_seconds),
                worker_lease_expires_at=now + timedelta(seconds=self._worker_lease_seconds),
            )
            if token is not None:
                assigned += 1
        METRICS.inc("control_assignments_total", assigned)
        return assigned

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self._on_tick is not None:
                self._on_tick()
            try:
                assigned = await self.run_once()
                if assigned:
                    log.info("Control assigned waiting conversations", extra={"assigned": assigned})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Control allocation pass failed (%s)", type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                pass


async def run_control(config: Config) -> None:
    """Allocation, STOP/reset authorization and Slack delivery.

    Control holds the Slack bot token (post only, no Socket Mode) but no Agent SDK.
    """
    if config.execution_mode != "distributed":
        raise ConfigError("control requires execution_mode: distributed")
    config.validate_namespace("control")
    if not config.distributed.firestore_project:
        raise ConfigError("distributed.firestore_project is required")
    import aiohttp
    from slack_sdk.web.async_client import AsyncWebClient

    from silk_agent.channels.slack_output import SlackOutput
    from silk_agent.distributed.control_policy import build_control_authorizer
    from silk_agent.distributed.enrichment import ControlAuthorizer
    from silk_agent.distributed.health import (
        RoleHealth,
        process_memory_collector,
        serve_health,
    )
    from silk_agent.distributed.quiesce import QuiesceRequester
    from silk_agent.distributed.recovery import DistributedRecoveryCoordinator
    from silk_agent.distributed.sender import OutboxSender
    from silk_agent.distributed.slack_delivery import SlackOutboxDelivery
    from silk_agent.slack_app import load_bot_token
    from silk_agent.stores.firestore.inbox import FirestoreInboxStore
    from silk_agent.stores.firestore.outbox import FirestoreOutboxStore
    from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
    from silk_agent.stores.firestore.quiesce import FirestoreQuiesceMailbox
    from silk_agent.stores.firestore.retention import RetentionPolicy
    from silk_agent.stores.firestore.session_state import FirestoreProgressStateStore

    bot_token = load_bot_token(config)
    clock = SystemClock()
    health = RoleHealth("control")
    METRICS.add_collector(process_memory_collector)
    client = create_firestore_client(config.distributed)
    prefix = config.distributed.firestore_prefix
    retention = RetentionPolicy.from_config(config.distributed.retention)
    identity = f"{os.environ.get('HOSTNAME', 'control')}:{uuid.uuid4().hex}"
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix, retention=retention)

    async def current_incarnation(worker_id: str) -> str | None:
        worker = await directory.get_worker(worker_id)
        return worker.incarnation if worker is not None else None

    allocator = ControlAllocatorLoop(
        conversations,
        directory,
        FirestoreAllocator(client, collection_prefix=prefix),
        clock,
        allocator_id=identity,
        lease_seconds=config.distributed.lease_seconds,
        worker_lease_seconds=config.distributed.lease_seconds,
        on_tick=health.loop("allocator", 120),
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix, retention=retention)
    authorizer = ControlAuthorizer(
        inbox,
        clock,
        build_control_authorizer(config, clock),
    )
    http_session = aiohttp.ClientSession()
    web_client = AsyncWebClient(token=bot_token, session=http_session)
    outbox = FirestoreOutboxStore(client, collection_prefix=prefix, retention=retention)
    slack = SlackOutput(
        web_client,
        clock,
        config.slack.progress,
        state_store=FirestoreProgressStateStore(conversations),
    )
    sender_loop = OutboxSenderLoop(
        outbox,
        OutboxSender(outbox, clock, SlackOutboxDelivery(slack), sender_id=identity),
        on_tick=health.loop("sender", 120),
    )
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    pod_probe = None
    kube_client = None
    if config.distributed.auto_recover_missing_pods:
        from silk_agent.distributed.pod_probe import KubernetesPodProbe

        pod_probe, kube_client = await KubernetesPodProbe.in_cluster()

    async def notify_blocked(token: Any) -> None:
        # Once per ownership epoch: the user otherwise just sees the thread go quiet.
        if await conversations.mark_recovery_notified(token.conversation_key, token.epoch):
            from silk_agent.core.events import Notice

            await slack.send_durable(
                Notice(
                    token.conversation_key,
                    "担当の処理が途中で止まりました。復旧には少し時間がかかる場合があります。"
                    "しばらくしてもう一度送ってください。",
                )
            )

    recovery = RecoveryLoop(
        ownership,
        DistributedRecoveryCoordinator(ownership, clock),
        QuiesceRequester(
            FirestoreQuiesceMailbox(client, collection_prefix=prefix, retention=retention), clock
        ),
        clock,
        skew_seconds=config.distributed.lease_clock_skew_seconds,
        on_tick=health.loop("recovery", 180),
        pod_gone=pod_probe.is_gone if pod_probe is not None else None,
        current_incarnation=current_incarnation,
        auto_recover_after_seconds=(
            config.distributed.auto_recover_after_seconds or 2 * config.distributed.lease_seconds
        ),
        notify_blocked=notify_blocked,
        has_demand=lambda token: inbox.has_waiting_input(token.conversation_key),
    )
    authorizer_tick = health.loop("authorizer", 120)
    stop = asyncio.Event()
    running_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            running_loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-unix
            pass
    tasks = [
        asyncio.create_task(allocator.run(stop), name="control-allocator"),
        asyncio.create_task(
            authorizer.run_forever(stop, on_tick=authorizer_tick), name="control-authorizer"
        ),
        asyncio.create_task(sender_loop.run(stop), name="control-sender"),
        asyncio.create_task(recovery.run(stop), name="control-recovery"),
        asyncio.create_task(
            serve_health(health, config.distributed.health_port, stop), name="control-health"
        ),
    ]
    try:
        # Any role task ending (even cleanly) before stop is a failure the Pod restart fixes.
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        stop.set()
        for task in done:
            task.result()
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await http_session.close()
        if kube_client is not None:
            await kube_client.close()
        client.close()
