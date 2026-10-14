"""Production composition for the durable Slack Socket Mode Gateway."""

from __future__ import annotations

import asyncio
import logging
import signal

import aiohttp

from silk_agent.channels.slack_users import UserDirectory
from silk_agent.config import Config, ConfigError
from silk_agent.core.clock import SystemClock
from silk_agent.distributed.enrichment import SlackSenderEnricher
from silk_agent.distributed.gateway import (
    ReceivedSlackInput,
    SlackEventNormalizer,
    SlackSocketGateway,
)
from silk_agent.slack_app import load_tokens
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.inbox import FirestoreInboxStore

log = logging.getLogger(__name__)


async def run_gateway(config: Config) -> None:
    """Run raw Socket Mode reception with persist-before-ACK semantics."""
    if config.execution_mode != "distributed":
        raise ConfigError("gateway requires execution_mode: distributed")
    if not config.distributed.firestore_project:
        raise ConfigError("distributed.firestore_project is required")
    bot_token, app_token = load_tokens(config)

    from slack_sdk.socket_mode.aiohttp import SocketModeClient
    from slack_sdk.web.async_client import AsyncWebClient

    clock = SystemClock()
    database = create_firestore_client(config.distributed)
    prefix = config.distributed.firestore_prefix
    inbox = FirestoreInboxStore(database, collection_prefix=prefix)
    conversations = FirestoreConversationStore(database, collection_prefix=prefix)
    http_session = aiohttp.ClientSession()
    web_client = AsyncWebClient(token=bot_token, session=http_session)
    try:
        auth = await web_client.auth_test()
        bot_user_id = str(auth.get("user_id") or "")
        workspace_team = str(auth.get("team_id") or "")
        workspace_enterprise = str(auth.get("enterprise_id") or "")
        if not bot_user_id or not workspace_team:
            raise RuntimeError("Slack auth.test did not return a bot user and workspace")

        async def is_participating(key: str) -> bool:
            record = await conversations.get_conversation(key)
            return bool(record and record.get("participating") is True)

        async def conversation_generation(key: str) -> int:
            record = await conversations.get_conversation(key)
            generation = record.get("conversation_generation", 1) if record else 1
            return generation if isinstance(generation, int) and generation > 0 else 1

        normalizer = SlackEventNormalizer(
            bot_user_id=bot_user_id,
            is_participating=is_participating,
            conversation_generation=conversation_generation,
            stop_words=config.slack.stop_words,
        )
        users = UserDirectory(web_client, clock)
        enricher = SlackSenderEnricher(
            inbox,
            users,
            clock,
            workspace_team=workspace_team,
            workspace_enterprise=workspace_enterprise,
        )

        async def enrich_received(item: ReceivedSlackInput) -> None:
            await enricher.enrich_one(item.conversation_key, item.input_id)

        gateway = SlackSocketGateway(
            inbox,
            clock,
            normalizer,
            on_accepted=enrich_received,
        )
        socket = SocketModeClient(app_token=app_token)

        async def on_message(_client, _message, raw_message: str) -> None:
            await gateway.handle_message(raw_message, socket.send_socket_mode_response)

        socket.message_listeners.append(on_message)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # pragma: no cover - non-unix
                pass
        enrichment_task = asyncio.create_task(enricher.run_forever(stop))
        try:
            await socket.connect()
            log.info(
                "distributed Slack Gateway connected",
                extra={"workspace_team": workspace_team},
            )
            await stop.wait()
        finally:
            gateway.stop_accepting()
            await socket.close()
            await gateway.aclose()
            stop.set()
            enrichment_task.cancel()
            await asyncio.gather(enrichment_task, return_exceptions=True)
    finally:
        await http_session.close()
        database.close()
