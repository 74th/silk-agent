"""Slack bot process: start-up recovery, Socket Mode, graceful shutdown."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from silk_agent.app import Services, build_services
from silk_agent.channels.slack import SlackChannel
from silk_agent.config import Config
from silk_agent.core.events import InboundEvent, UserMessage
from silk_agent.core.lifecycle import DEFAULT_SHUTDOWN_GRACE_SECONDS, recover_interrupted
from silk_agent.core.ports import Clock
from silk_agent.testing.fakes import SystemClock

log = logging.getLogger(__name__)


class SocketHandler(Protocol):
    async def start_async(self) -> None: ...

    async def close_async(self) -> None: ...


def read_secret(resource: str) -> str:
    """`projects/p/secrets/name` -> latest version payload (Secret Manager)."""
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = resource if "/versions/" in resource else f"{resource}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode("utf-8").strip()


def load_tokens(config: Config, env: Mapping[str, str] | None = None) -> tuple[str, str]:
    """(bot token, app token). Environment variables win (local runs); otherwise Secret Manager."""
    env = os.environ if env is None else env
    bot = env.get("SLACK_BOT_TOKEN") or (
        read_secret(config.slack.bot_token_secret) if config.slack.bot_token_secret else ""
    )
    app = env.get("SLACK_APP_TOKEN") or (
        read_secret(config.slack.app_token_secret) if config.slack.app_token_secret else ""
    )
    if not bot or not app:
        raise RuntimeError("Slack tokens are not configured (SLACK_BOT_TOKEN / SLACK_APP_TOKEN)")
    return bot, app


def check_config(config: Config) -> None:
    if config.sandbox.runtime == "local_process":
        raise RuntimeError(
            "sandbox.runtime=local_process runs agent commands on the Bot host without "
            "isolation and cannot be used by the Slack bot"
        )


class UserDirectory:
    """Resolves a Slack user id to an e-mail address (needs `users:read.email`)."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._cache: dict[str, str] = {}

    async def email(self, user_id: str) -> str:
        if user_id in self._cache:
            return self._cache[user_id]
        try:
            res = await self._client.users_info(user=user_id)
            email = res["user"]["profile"].get("email", "")
        except Exception:
            log.warning("could not resolve the e-mail address of %s", user_id)
            email = ""
        self._cache[user_id] = email
        return email


class SlackBot:
    def __init__(
        self,
        config: Config,
        *,
        root: Path,
        work_dir: Path,
        app: Any,
        socket_handler: SocketHandler,
        clock: Clock | None = None,
        bot_user_id: str = "",
        shutdown_grace_seconds: float = DEFAULT_SHUTDOWN_GRACE_SECONDS,
    ) -> None:
        self._config = config
        self._root, self._work_dir = root, work_dir
        self._app = app
        self._socket = socket_handler
        self._clock = clock or SystemClock()
        self._bot_user_id = bot_user_id
        self._grace = shutdown_grace_seconds
        self._stop = asyncio.Event()
        self._users = UserDirectory(app.client)
        self._last_user: dict[str, str] = {}
        self.services: Services | None = None
        self.channel: SlackChannel | None = None
        self.recovered: list[str] = []

    async def _env(self, key: str) -> Mapping[str, str]:
        user = self._last_user.get(key)
        email = await self._users.email(user) if user else ""
        return {"USER_EMAIL": email} if email else {}

    async def _handle(self, event: InboundEvent) -> None:
        if isinstance(event, UserMessage) and event.user:
            self._last_user[event.conversation_key] = event.user
        assert self.services is not None
        await self.services.orchestrator.handle(event)

    async def start(self, runtime: Any | None = None) -> None:
        check_config(self._config)
        self.channel = SlackChannel(
            self._app,
            self._handle,
            self._clock,
            self._config.slack.progress,
            bot_user_id=self._bot_user_id,
        )
        self.services = await build_services(
            self._config,
            self.channel,
            root=self._root,
            work_dir=self._work_dir,
            clock=self._clock,
            env_provider=self._env,
            runtime=runtime,
        )
        # The channel needs the state store (participation, progress message position).
        self.channel._state_store = self.services.state_store
        self.channel.output._state_store = self.services.state_store
        self.channel.register()
        # Turns that were running under a previous Bot instance were interrupted.
        self.recovered = await recover_interrupted(
            self.services.state_store, self.channel, self.services.bot_instance_id, self._clock
        )
        await self._socket.start_async()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Start, then wait for a stop request (SIGTERM / SIGINT), then shut down."""
        await self.start()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except NotImplementedError:  # pragma: no cover - non-unix
                pass
        await self._stop.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        """1. stop receiving events  2. let running turns finish (grace)  3. interrupt the
        rest and record "interrupted(shutdown)"  4. release resources."""
        log.info("shutting down")
        try:
            await self._socket.close_async()
        except Exception:
            log.exception("closing Socket Mode failed")
        if self.channel is not None:
            await self.channel.wait_idle()
        if self.services is not None:
            await self.services.aclose(self._grace)


async def run_from_environment(config: Config, root: Path) -> None:  # pragma: no cover
    from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
    from slack_bolt.async_app import AsyncApp

    check_config(config)
    bot_token, app_token = load_tokens(config)
    app = AsyncApp(token=bot_token)
    auth = await app.client.auth_test()
    work_dir = Path(os.environ.get("SILK_AGENT_WORKDIR", "/tmp/silk-agent"))
    await asyncio.to_thread(work_dir.mkdir, parents=True, exist_ok=True)
    bot = SlackBot(
        config,
        root=root,
        work_dir=work_dir,
        app=app,
        socket_handler=AsyncSocketModeHandler(app, app_token),
        bot_user_id=str(auth["user_id"]),
    )
    await bot.run()
