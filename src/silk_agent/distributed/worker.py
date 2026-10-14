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
from silk_agent.core.ports import Clock
from silk_agent.stores.firestore.watch import WatchFactory, WatchHandle

log = logging.getLogger(__name__)
SIGTERM_DRAIN_GRACE_SECONDS = 25.0


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
        self._actors: dict[str, tuple[OwnershipToken, ConversationActor]] = {}
        self._closed = False
        self._draining = False
        self._drain_deadline_at: datetime | None = None

    @property
    def actors(self) -> tuple[str, ...]:
        return tuple(self._actors)

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
            if token != saved or token is None or token.expires_at <= self._clock.now():
                await actor.aclose()
                del self._actors[key]

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
            actor = actor_factory(token)
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
                stopped = await self._drain_actor(actor, timeout)
            except Exception as exc:
                log.warning("worker actor drain failed (%s)", type(exc).__name__)
                continue
            if not stopped:
                continue
            if not await self._ownership.release(token, now=self._clock.now()):
                continue
            self._actors.pop(key, None)

    async def _observe_drain_state(self) -> None:
        if self._worker_directory is None:
            return
        worker = await self._worker_directory.get_worker(self._worker_id)
        if worker is None or worker.incarnation != self._incarnation or worker.draining:
            self._draining = True
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

        while not stop.is_set() and not self._closed:
            handle: WatchHandle | None = None
            try:
                handle = self._subscribe(signal_change, signal_error)
                changed.set()
                while not stop.is_set() and not self._closed:
                    try:
                        await asyncio.wait_for(changed.wait(), timeout=self._renew_seconds)
                    except TimeoutError:
                        await self._renew_actors()
                    else:
                        changed.clear()
                        await self.run_once()
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
        await self.aclose()

    async def quiesce(self, token: OwnershipToken) -> dict[str, object] | None:
        """Stop one local actor and return evidence for Control to persist.

        A missing actor or failed close is not proof: Control must leave the
        ownership blocked until another explicit stop signal arrives.
        """
        current = self._actors.get(token.conversation_key)
        if current is None or current[0] != token:
            return None
        try:
            await current[1].aclose()
        except Exception as exc:
            log.warning("worker quiesce failed (%s)", type(exc).__name__)
            return None
        self._actors.pop(token.conversation_key, None)
        return {
            "evidence_id": f"{self._incarnation}:{uuid4().hex}",
            "worker_incarnation": self._incarnation,
            "sdk_stopped": True,
        }

    async def _renew_actors(self) -> None:
        await self._observe_drain_state()
        if self._draining:
            await self._drain_actors()
            return
        for key, (token, actor) in tuple(self._actors.items()):
            now = self._clock.now()
            renewed = await self._ownership.renew(
                token,
                now=now,
                lease_expires_at=now + timedelta(seconds=self._lease_seconds),
            )
            if renewed is None:
                await actor.aclose()
                del self._actors[key]
            else:
                await actor.update_ownership(renewed)
                self._actors[key] = (renewed, actor)

    async def aclose(self) -> None:
        self._closed = True
        actors = tuple(self._actors.values())
        self._actors.clear()
        for token, actor in actors:
            try:
                if self._drain_actor is not None:
                    # A planned drain can wait indefinitely (or use its explicit
                    # deadline); process termination has only the Pod's 25s fallback.
                    stopped = await asyncio.wait_for(
                        self._drain_actor(actor, SIGTERM_DRAIN_GRACE_SECONDS),
                        timeout=SIGTERM_DRAIN_GRACE_SECONDS,
                    )
                else:
                    await asyncio.wait_for(actor.aclose(), timeout=SIGTERM_DRAIN_GRACE_SECONDS)
                    stopped = True
            except Exception:
                log.exception("could not confirm local actor stop for %s", token.conversation_key)
            else:
                if not stopped:
                    continue
                await self._ownership.release(token, now=self._clock.now())
