"""Worker-side ownership watcher and per-conversation actor lifecycle."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Protocol
from uuid import uuid4

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import OwnershipStore, WorkerDirectory
from silk_agent.core.ownership_lifecycle import OwnershipLifecycle
from silk_agent.core.ports import Clock
from silk_agent.distributed.metrics import METRICS
from silk_agent.stores.firestore.watch import WatchFactory, WatchHandle

log = logging.getLogger(__name__)
SIGTERM_DRAIN_GRACE_SECONDS = 25.0
# The turn gets 25 s to finish; after that it is interrupted and saved, which needs its own
# time. Cutting the whole close at 25 s left the lease unreleased (recovery_blocked).
SIGTERM_STOP_DEADLINE_SECONDS = 40.0


class ConversationActor(Protocol):
    async def start(self) -> None: ...

    async def update_ownership(self, token: OwnershipToken) -> None: ...

    async def aclose(self) -> None:
        """Return only after this actor's local SDK process has stopped."""
        ...


ActorFactory = Callable[[OwnershipToken], ConversationActor]
DrainActor = Callable[[ConversationActor, float | None], Awaitable[bool]]


class DistributedWorker:
    """Create actors only after this process activates a valid assignment.

    The watcher is only a wake-up signal. Every wake-up re-reads the reservation
    set from the durable ownership store so reconnects cannot lose assignments.
    """

    def __init__(
        self,
        ownership: OwnershipStore,
        clock: Clock,
        *,
        worker_id: str,
        worker_incarnation: str,
        actor_factory: ActorFactory,
        subscribe: WatchFactory,
        is_control_only: Callable[[str], Awaitable[bool]] | None = None,
        control_actor_factory: ActorFactory | None = None,
        worker_directory: WorkerDirectory | None = None,
        drain_actor: DrainActor | None = None,
        drain_grace_seconds: float | None = None,
        lease_seconds: int = 60,
        renew_seconds: float = 20,
        retry_seconds: float = 1,
        idle_close_seconds: float | None = None,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        if not worker_id or not worker_incarnation:
            raise ValueError("worker identity must be non-empty")
        if lease_seconds <= 0 or renew_seconds <= 0 or retry_seconds < 0:
            raise ValueError("worker timing values are invalid")
        if drain_grace_seconds is not None and drain_grace_seconds <= 0:
            raise ValueError("drain_grace_seconds must be positive when set")
        self._ownership = ownership
        self._clock = clock
        self._worker_id = worker_id
        self._incarnation = worker_incarnation
        self._actor_factory = actor_factory
        self._is_control_only = is_control_only
        self._control_actor_factory = control_actor_factory
        self._worker_directory = worker_directory
        self._drain_actor = drain_actor
        self._drain_grace_seconds = drain_grace_seconds
        self._subscribe = subscribe
        self._lease_seconds = lease_seconds
        self._renew_seconds = renew_seconds
        self._retry_seconds = retry_seconds
        if idle_close_seconds is not None and idle_close_seconds <= 0:
            raise ValueError("idle_close_seconds must be positive when set")
        self._on_tick = on_tick
        self._idle_close_seconds = idle_close_seconds
        self._idle_since: dict[str, datetime] = {}
        self._closed_epochs: dict[tuple[str, int], None] = {}
        self._lifecycle = OwnershipLifecycle(ownership, clock)
        self._actors: dict[str, tuple[OwnershipToken, ConversationActor]] = {}
        self._control_only: set[str] = set()
        self._closed = False
        self._draining = False
        self._drain_deadline_at: datetime | None = None

    @property
    def actors(self) -> tuple[str, ...]:
        return tuple(self._actors)

    @property
    def usage(self) -> tuple[int, int]:
        """Return (reserved, active SDK slots) for WorkerHeartbeat."""
        reserved = len(self._actors)
        active_sdk = sum(key not in self._control_only for key in self._actors)
        return reserved, active_sdk

    async def run_once(self) -> int:
        """Reconcile actors with the durable assignments for this incarnation."""
        if self._closed:
            return 0
        await self._observe_drain_state()
        if self._draining:
            await self._drain_actors()
            return len(self._actors)
        tokens = await self._ownership.list_for_worker(
            worker_id=self._worker_id, worker_incarnation=self._incarnation
        )
        current = {token.conversation_key: token for token in tokens}
        for key, (saved, actor) in tuple(self._actors.items()):
            token = current.get(key)
            if token is not None and await self._adopt_reset_successor(saved, token, actor):
                continue
            if token != saved or token is None or token.expires_at <= self._clock.now():
                await actor.aclose()
                self._forget(key, saved)

        for key, token in current.items():
            if key in self._actors or token.expires_at <= self._clock.now():
                continue
            control_only = (
                await self._is_control_only(key) if self._is_control_only is not None else False
            )
            if control_only and self._control_actor_factory is None:
                raise RuntimeError("control-only assignment requires a control actor factory")
            activated = (
                await self._ownership.activate_control(token, now=self._clock.now())
                if control_only
                else await self._ownership.activate(token, now=self._clock.now())
            )
            if not activated:
                # An active token is expected only when this actor already exists.
                # Never create a second SDK actor from a stale watch notification.
                continue
            actor_factory = self._control_actor_factory if control_only else self._actor_factory
            assert actor_factory is not None
            try:
                actor = actor_factory(token)
            except BaseException:
                # Nothing was started, so the reservation can go back: a conversation whose
                # actor cannot be built must not keep its slot and stay silent.
                await self._ownership.release(token, now=self._clock.now())
                raise
            METRICS.inc("worker_actor_starts_total", kind="control" if control_only else "sdk")
            try:
                await actor.start()
            except BaseException:
                try:
                    await actor.aclose()
                except Exception:
                    log.exception("failed to stop partially started actor for %s", key)
                else:
                    await self._ownership.release(token, now=self._clock.now())
                raise
            self._actors[key] = (token, actor)
            if control_only:
                self._control_only.add(key)
        return len(self._actors)

    async def _drain_actors(self) -> None:
        if self._drain_actor is None:
            return
        for key, (token, actor) in tuple(self._actors.items()):
            try:
                timeout = self._drain_grace_seconds
                if self._drain_deadline_at is not None:
                    timeout = max(
                        0.0,
                        (self._drain_deadline_at - self._clock.now()).total_seconds(),
                    )
                draining = asyncio.ensure_future(self._drain_actor(actor, timeout))
                try:
                    while not draining.done():
                        done, _ = await asyncio.wait({draining}, timeout=self._renew_seconds)
                        if not done:
                            # A long turn may drain indefinitely. Its ownership must remain
                            # live while its independent input/control readers keep running.
                            await self._renew_active_leases()
                    stopped = draining.result()
                finally:
                    if not draining.done():
                        draining.cancel()
                        await asyncio.gather(draining, return_exceptions=True)
            except Exception as exc:
                log.warning("worker actor drain failed (%s)", type(exc).__name__)
                continue
            if not stopped:
                continue
            current = self._actors.get(key)
            if current is None:
                continue
            token = current[0]
            if not await self._ownership.release(token, now=self._clock.now()):
                continue
            self._actors.pop(key, None)
            self._control_only.discard(key)

    async def _observe_drain_state(self) -> None:
        if self._worker_directory is None:
            return
        worker = await self._worker_directory.get_worker(self._worker_id)
        self._draining = (
            worker is None or worker.incarnation != self._incarnation or worker.draining
        )
        self._drain_deadline_at = worker.drain_deadline_at if worker is not None else None

    async def run(self, stop: asyncio.Event) -> None:
        """Watch reservation changes and periodically renew active leases."""
        loop = asyncio.get_running_loop()
        changed = asyncio.Event()
        errors: asyncio.Queue[BaseException] = asyncio.Queue()

        def signal_change() -> None:
            loop.call_soon_threadsafe(changed.set)

        def signal_error(error: BaseException) -> None:
            loop.call_soon_threadsafe(errors.put_nowait, error)

        try:
            next_renewal = loop.time() + self._renew_seconds
            while not stop.is_set() and not self._closed:
                if self._on_tick is not None:
                    self._on_tick()
                handle: WatchHandle | None = None
                try:
                    handle = self._subscribe(signal_change, signal_error)
                    changed.set()
                    while not stop.is_set() and not self._closed:
                        if self._on_tick is not None:
                            self._on_tick()
                        if loop.time() >= next_renewal:
                            await self._renew_actors()
                            next_renewal = loop.time() + self._renew_seconds
                            # The snapshot listener can die without telling us (its error does
                            # not reach the callback): read the assignments again every
                            # renewal, so a missed notification costs one interval at most.
                            changed.set()
                        change_task = asyncio.create_task(changed.wait())
                        stop_task = asyncio.create_task(stop.wait())
                        try:
                            await asyncio.wait(
                                {change_task, stop_task},
                                timeout=max(0, next_renewal - loop.time()),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if stop.is_set():
                                break
                            if changed.is_set():
                                changed.clear()
                                await self.run_once()
                        finally:
                            for task in (change_task, stop_task):
                                task.cancel()
                            await asyncio.gather(change_task, stop_task, return_exceptions=True)
                        if not errors.empty():
                            raise errors.get_nowait()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("worker assignment watch failed (%s)", type(exc).__name__)
                    await self._clock.sleep(self._retry_seconds)
                finally:
                    if handle is not None:
                        handle.unsubscribe()
        finally:
            await self.aclose()

    async def quiesce(self, token: OwnershipToken) -> dict[str, object] | None:
        """Return evidence that this incarnation's SDK for ``token`` is closed.

        Evidence exists only if this process closed (now or earlier) the actor for exactly
        that ownership epoch. A missing actor without a record, or a failed close, is not
        proof: Control keeps the ownership blocked until another request succeeds.
        """
        if token.worker_incarnation != self._incarnation:
            return None
        current = self._actors.get(token.conversation_key)
        if current is not None:
            saved, actor = current
            if saved.epoch != token.epoch:
                return None
            try:
                await actor.aclose()
            except Exception as exc:
                log.warning("worker quiesce failed (%s)", type(exc).__name__)
                return None
            self._forget(token.conversation_key, saved)
        elif (token.conversation_key, token.epoch) not in self._closed_epochs:
            return None
        return {
            "evidence_id": f"{self._incarnation}:{uuid4().hex}",
            "worker_incarnation": self._incarnation,
            "sdk_stopped": True,
        }

    def _forget(self, key: str, token: OwnershipToken) -> None:
        """Drop a closed actor and remember that this epoch's SDK is known to be stopped."""
        self._actors.pop(key, None)
        self._control_only.discard(key)
        self._idle_since.pop(key, None)
        self._closed_epochs[(key, token.epoch)] = None
        while len(self._closed_epochs) > 1024:
            del self._closed_epochs[next(iter(self._closed_epochs))]

    async def _renew_actors(self) -> None:
        await self._observe_drain_state()
        await self._renew_active_leases()
        if self._draining:
            await self._drain_actors()
        else:
            await self._release_idle_actors()

    async def _release_idle_actors(self) -> None:
        """Free the capacity slot of conversations idle for ``idle_close_seconds``.

        The actor is saved/closed first; the release transaction then decides whether a
        late input keeps the conversation waiting for (re)assignment.
        """
        if self._idle_close_seconds is None:
            return
        now = self._clock.now()
        for key, (token, actor) in tuple(self._actors.items()):
            is_idle = getattr(actor, "is_idle", None)
            if is_idle is None or not is_idle():
                self._idle_since.pop(key, None)
                continue
            since = self._idle_since.setdefault(key, now)
            if (now - since).total_seconds() < self._idle_close_seconds:
                continue
            try:
                released = await self._lifecycle.release_when_idle(
                    token, is_idle=is_idle, save_and_close=actor.aclose
                )
            except Exception as exc:
                log.warning("idle ownership release failed (%s)", type(exc).__name__)
                continue
            if released:
                self._forget(key, token)

    async def _renew_active_leases(self) -> None:
        if self._on_tick is not None:
            self._on_tick()  # drain keeps renewing while a long turn finishes
        for key, (token, actor) in tuple(self._actors.items()):
            now = self._clock.now()
            try:
                renewed = await self._ownership.renew(
                    token,
                    now=now,
                    lease_expires_at=now + timedelta(seconds=self._lease_seconds),
                )
            except Exception as exc:
                log.warning("worker lease renewal failed (%s)", type(exc).__name__)
                METRICS.inc("worker_lease_renewal_failures_total", reason="error")
                renewed = None
                renewal_failed = True
            else:
                renewal_failed = False
            if renewed is None:
                if (
                    renewal_failed
                    and (token.expires_at - now).total_seconds() > self._renew_seconds
                ):
                    # A transient store error is not a lost lease: the lease still outlasts the
                    # next renewal, so keep the conversation and try again then. Closing here
                    # would interrupt every conversation of this Worker on one failed call.
                    continue
                if not renewal_failed and await self._adopt_stored_successor(token, actor):
                    continue
                if not renewal_failed:
                    METRICS.inc("worker_lease_renewal_failures_total", reason="lost")
                await actor.aclose()
                self._forget(key, token)
            else:
                await actor.update_ownership(renewed)
                self._actors[key] = (renewed, actor)

    async def _stop_receiving_assignments(self) -> None:
        """Mark this incarnation draining first, so the allocator cannot hand it a conversation
        that is released a moment later (it would sit assigned to a dying process)."""
        if self._worker_directory is None:
            return
        try:
            await self._worker_directory.set_draining(
                worker_id=self._worker_id,
                incarnation=self._incarnation,
                draining=True,
                now=self._clock.now(),
            )
        except Exception as exc:
            log.warning("could not mark the worker draining on close (%s)", type(exc).__name__)

    async def _adopt_reset_successor(
        self, saved: OwnershipToken, token: OwnershipToken, actor: ConversationActor
    ) -> bool:
        """Keep an actor across its own conversation reset (same Worker, next epoch/generation).

        Reset advances both values in one transaction while this Worker still holds the
        active assignment, so the successor is the same owner and not a takeover.
        """
        if (
            token.conversation_key != saved.conversation_key
            or token.worker_id != saved.worker_id
            or token.worker_incarnation != saved.worker_incarnation
            or token.epoch != saved.epoch + 1
            or token.conversation_generation != saved.conversation_generation + 1
            or token.expires_at <= self._clock.now()
        ):
            return False
        rebind = getattr(actor, "rebind_after_reset", None)
        if rebind is None:
            return False
        await rebind(token)
        self._actors[token.conversation_key] = (token, actor)
        return True

    async def _adopt_stored_successor(
        self, saved: OwnershipToken, actor: ConversationActor
    ) -> bool:
        get = getattr(self._ownership, "get", None)
        if get is None:
            return False
        try:
            successor = await get(saved.conversation_key)
        except Exception as exc:
            log.warning("worker ownership lookup failed (%s)", type(exc).__name__)
            return False
        return successor is not None and await self._adopt_reset_successor(saved, successor, actor)

    async def rebind_actor(self, token: OwnershipToken) -> bool:
        """Adopt a reset successor immediately from the actor's own reset handler."""
        current = self._actors.get(token.conversation_key)
        if current is None:
            return False
        saved, actor = current
        if saved == token:
            return True
        return await self._adopt_reset_successor(saved, token, actor)

    async def aclose(self) -> None:
        self._closed = True
        await self._stop_receiving_assignments()
        actors = tuple(self._actors.values())
        if actors:
            log.info("worker closing", extra={"assigned": len(actors)})
        self._actors.clear()
        self._control_only.clear()
        await asyncio.gather(
            *(self._close_actor(token, actor) for token, actor in actors), return_exceptions=False
        )

    async def _close_actor(self, token: OwnershipToken, actor: ConversationActor) -> None:
        try:
            if self._drain_actor is not None:
                # The process has one grace window, shared concurrently by all actors.
                stopped = await asyncio.wait_for(
                    self._drain_actor(actor, SIGTERM_DRAIN_GRACE_SECONDS),
                    timeout=SIGTERM_STOP_DEADLINE_SECONDS,
                )
            else:
                await asyncio.wait_for(actor.aclose(), timeout=SIGTERM_STOP_DEADLINE_SECONDS)
                stopped = True
        except Exception as exc:
            log.warning("could not confirm local actor stop (%s)", type(exc).__name__)
            return
        if stopped:
            try:
                released = await self._ownership.release(token, now=self._clock.now())
            except Exception as exc:
                log.warning("could not release stopped actor ownership (%s)", type(exc).__name__)
            else:
                if released:
                    log.info("released ownership of a stopped actor")
                else:
                    log.warning("ownership release was rejected for a stopped actor")
        else:
            log.warning("actor drain did not confirm a stop; ownership stays held")
