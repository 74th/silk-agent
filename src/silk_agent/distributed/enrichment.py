"""Durable, post-ACK Slack sender enrichment for the Control/Gateway roles."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import timedelta
from typing import Any

from silk_agent.channels.slack_users import UserDirectory
from silk_agent.core.distributed_ports import InboxStore
from silk_agent.core.ports import Clock
from silk_agent.distributed.metrics import METRICS

log = logging.getLogger(__name__)


class SlackSenderEnricher:
    """Resolve Slack user policy attributes after durable acceptance.

    A missing lookup result remains pending with bounded exponential backoff. The
    record stays recoverable for a Control restart and is never interpreted as an
    authorized user while lookup is unresolved.
    """

    def __init__(
        self,
        inbox: InboxStore,
        users: UserDirectory,
        clock: Clock,
        *,
        workspace_team: str,
        workspace_enterprise: str = "",
        retry_base_seconds: int = 5,
        retry_max_seconds: int = 300,
        batch_size: int = 100,
        max_attempts: int = 3,
        on_gave_up: Callable[[str, str, str], Awaitable[None]] | None = None,
    ) -> None:
        if retry_base_seconds <= 0 or retry_max_seconds < retry_base_seconds or batch_size <= 0:
            raise ValueError("sender enrichment retry and batch values are invalid")
        self._inbox = inbox
        self._users = users
        self._clock = clock
        self._team = workspace_team
        self._enterprise = workspace_enterprise
        self._retry_base = retry_base_seconds
        self._retry_max = retry_max_seconds
        self._batch = batch_size
        self._max_attempts = max_attempts
        self._on_gave_up = on_gave_up

    async def run_once(self) -> int:
        now = self._clock.now()
        records = await self._inbox.list_sender_enrichment_due(now=now, limit=self._batch)
        if not records:
            return 0
        await asyncio.gather(*(self._enrich(record) for record in records))
        return len(records)

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        poll_seconds: float = 5.0,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        while not stop.is_set():
            if on_tick is not None:
                on_tick()
            try:
                processed = await self.run_once()
            except Exception as exc:
                log.warning("sender enrichment scan failed (%s)", type(exc).__name__)
                processed = 0
            if processed == 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
                except TimeoutError:
                    pass

    async def enrich_one(self, conversation_key: str, input_id: str) -> bool:
        records = await self._inbox.list_sender_enrichment_due(
            now=self._clock.now(), limit=self._batch
        )
        record = next(
            (
                item
                for item in records
                if item.get("conversation_key") == conversation_key
                and item.get("input_id") == input_id
            ),
            None,
        )
        if record is None:
            return False
        await self._enrich(record)
        return True

    async def _enrich(self, record: Mapping[str, Any]) -> None:
        conversation_key = record.get("conversation_key")
        input_id = record.get("input_id")
        record_collection = record.get("record_collection")
        user_id = record.get("sender_lookup_user")
        if not isinstance(conversation_key, str) or not conversation_key:
            return
        if not isinstance(input_id, str) or not input_id:
            return
        if not isinstance(record_collection, str) or not record_collection:
            return
        if not isinstance(user_id, str) or not user_id:
            return
        definitive = False
        try:
            checked = getattr(self._users, "lookup_checked", None)
            if checked is not None:
                user, definitive = await checked(user_id)
            else:
                user = await self._users.lookup(user_id)
        except Exception as exc:
            log.warning("Slack sender lookup failed (%s)", type(exc).__name__)
            user = None
        now = self._clock.now()
        if user is None:
            attempt = record.get("sender_lookup_attempts", 0)
            attempt = attempt if isinstance(attempt, int) and not isinstance(attempt, bool) else 0
            delay = min(self._retry_base * (2 ** min(attempt, 16)), self._retry_max)
            # A sender that can never be resolved (scope revoked, deleted or invisible user)
            # must not hold the conversation, or its STOP, forever.
            # Only a definitive answer ("no such user", "not visible") ends the lookup. A rate
            # limit, an outage or a token problem (missing scope) keeps retrying with the capped
            # backoff: giving up there would permanently discard every message that arrived
            # during the outage.
            give_up = definitive and attempt + 1 >= self._max_attempts
            recorded = await self._inbox.record_sender_enrichment(
                conversation_key=conversation_key,
                input_id=input_id,
                record_collection=record_collection,
                sender_info=None,
                retry_at=now + timedelta(seconds=delay),
                now=now,
                give_up=give_up,
            )
            METRICS.inc("gateway_sender_lookup_total", result="gave_up" if give_up else "retry")
            if give_up and recorded and self._on_gave_up is not None:
                try:
                    await self._on_gave_up(conversation_key, input_id, record_collection)
                except Exception as exc:
                    log.warning("could not report an unresolved sender (%s)", type(exc).__name__)
            return
        user_team = user.team_id
        is_external = self._external(user_team, user.enterprise_id)
        sender = {
            "email": user.email,
            "is_guest": user.is_guest,
            "is_external": is_external,
            "team": user_team,
        }
        await self._inbox.record_sender_enrichment(
            conversation_key=conversation_key,
            input_id=input_id,
            record_collection=record_collection,
            sender_info=sender,
            retry_at=now,
            now=now,
        )

    def _external(self, user_team: str, enterprise: str) -> bool | None:
        if not self._team or not user_team:
            return None
        if user_team == self._team:
            return False
        if not self._enterprise:
            return True
        if not enterprise:
            return None
        return enterprise != self._enterprise


class ControlAuthorizer:
    """Resolve persisted control candidates only after verified sender enrichment."""

    def __init__(
        self,
        inbox: InboxStore,
        clock: Clock,
        authorize: Callable[[Mapping[str, Any]], bool],
        *,
        batch_size: int = 100,
    ):
        if batch_size < 1:
            raise ValueError("authorize callback and positive batch_size are required")
        self._inbox = inbox
        self._clock = clock
        self._authorize = authorize
        self._batch = batch_size

    async def run_once(self) -> int:
        records = await self._inbox.list_pending_controls(limit=self._batch)
        resolved = 0
        for record in records:
            if record.get("sender_lookup_state") != "resolved":
                continue
            payload = record.get("payload")
            sender = payload.get("sender") if isinstance(payload, Mapping) else None
            if not isinstance(sender, Mapping):
                continue
            authorized = bool(self._authorize(record))
            if await self._inbox.resolve_control_authorization(
                conversation_key=str(record.get("conversation_key", "")),
                input_id=str(record.get("input_id", "")),
                authorized=authorized,
                now=self._clock.now(),
            ):
                resolved += 1
        return resolved

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        poll_seconds: float = 5.0,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        while not stop.is_set():
            if on_tick is not None:
                on_tick()
            try:
                resolved = await self.run_once()
            except Exception as exc:
                log.warning("control authorization scan failed (%s)", type(exc).__name__)
                resolved = 0
            if resolved == 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
                except TimeoutError:
                    pass
