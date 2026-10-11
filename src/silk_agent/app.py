"""Assembles the whole system from Config (used by apps/slack_bot.py and apps/streamlit_app.py)."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from silk_agent.config import Config
from silk_agent.core.background import BackgroundCommands
from silk_agent.core.commands import CommandTracker
from silk_agent.core.filters import FilterChain, default_chain
from silk_agent.core.lifecycle import new_bot_instance_id
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.ports import ChatChannel, Clock, SandboxRuntime, SessionStateStore, SkillSource
from silk_agent.core.session import SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.sandbox.local import LocalProcessRuntime
from silk_agent.skills.sources import GcsSkillSource, LocalDirSkillSource, run_skill_sync
from silk_agent.testing.fakes import InMemoryStateStore, SystemClock
from silk_agent.wiring import build_engine_factory, build_input_guard, build_output_sanitizer

log = logging.getLogger(__name__)


@dataclass
class Services:
    orchestrator: ConversationOrchestrator
    runtime: SandboxRuntime
    state_store: SessionStateStore
    bot_instance_id: str
    skills: SkillSource | None = None
    background: list[asyncio.Task[None]] = field(default_factory=list)
    _stop: asyncio.Event = field(default_factory=asyncio.Event)

    async def aclose(self, grace_seconds: float = 25.0) -> None:
        self._stop.set()
        await self.orchestrator.shutdown(grace_seconds)
        for task in self.background:
            task.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        close = getattr(self.runtime, "aclose", None)
        if close is not None:
            await close()


def build_state_store(config: Config) -> tuple[SessionStateStore, Any | None]:
    """(SessionStateStore, SDK SessionStore or None)."""
    cfg = config.session_store
    if cfg.provider == "memory" or not cfg.agent_runtime or "<id>" in cfg.agent_runtime:
        return InMemoryStateStore(), None
    from silk_agent.stores.agent_platform import AgentPlatformSessions, AgentPlatformSessionStore
    from silk_agent.stores.state import AgentPlatformStateStore

    sessions = AgentPlatformSessions(
        cfg.agent_runtime, project=cfg.project, location=cfg.location, ttl_seconds=cfg.ttl_seconds
    )
    return AgentPlatformStateStore(sessions), AgentPlatformSessionStore(sessions)


def build_skill_source(config: Config, cache_dir: Path) -> SkillSource | None:
    source = config.skills.source
    if not source:
        return None
    if source.startswith("gs://"):
        return GcsSkillSource(source, cache_dir)  # type: ignore[return-value]
    return LocalDirSkillSource(source, cache_dir)  # type: ignore[return-value]


# HMAC key the library needs when `ensure` receives env (USER_EMAIL). It must stay the
# same for the life of the sandboxes, so it comes from a Secret, not the ConfigMap.
FINGERPRINT_KEY_ENV = "SILK_AGENT_SANDBOX_FINGERPRINT_KEY"


def with_fingerprint_key(config_file: Path, key: str, out: Path) -> Path:
    """Write a copy of the sandbox config with `environment_fingerprint_key` set (mode 0600)."""
    data = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    template = Path(data.get("template_file", "templates/sandbox.yaml"))
    if not template.is_absolute():
        # The library resolves a relative template next to the config file.
        data["template_file"] = str(config_file.parent / template)
    data["environment_fingerprint_key"] = key
    out.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True)
    return out


async def build_runtime(
    config: Config, root: Path, work_dir: Path, env_provider: Any
) -> SandboxRuntime:
    if config.sandbox.runtime == "local_process":
        return LocalProcessRuntime(work_dir / "sandbox")
    from gke_session_sandbox import SandboxManager
    from silk_agent.sandbox.gke import GkeSessionSandboxRuntime

    config_file = root / (config.sandbox.config_file or "config/sandbox-config.yaml")
    key = os.environ.get(FINGERPRINT_KEY_ENV, "")
    if key:
        config_file = await asyncio.to_thread(
            with_fingerprint_key, config_file, key, work_dir / "sandbox-config.yaml"
        )
    manager = await SandboxManager.from_config(str(config_file))
    return GkeSessionSandboxRuntime(manager, env_provider=env_provider)


async def build_services(
    config: Config,
    channel: ChatChannel,
    *,
    root: Path,
    work_dir: Path,
    clock: Clock | None = None,
    filters: FilterChain | None = None,
    env_provider: Callable[[str], Awaitable[Mapping[str, str]]] | None = None,
    runtime: SandboxRuntime | None = None,
    start_background: bool = True,
) -> Services:
    clock = clock or SystemClock()
    state_store, session_store = build_state_store(config)
    built_runtime = runtime is None
    runtime = runtime or await build_runtime(config, root, work_dir, env_provider)
    skills = build_skill_source(config, work_dir / "skills-cache")
    bot_instance_id = new_bot_instance_id()

    background = BackgroundCommands(runtime, CommandTracker(state_store))
    engine_factory = build_engine_factory(
        config,
        runtime,
        clock,
        root=root,
        work_root=work_dir / "work",
        session_store=session_store,
        skills=skills,
        state_store=state_store,
        background=background,
    )
    orchestrator = ConversationOrchestrator(
        engine_factory,
        channel,
        clock,
        guard=build_input_guard(config, root),
        sanitizer=build_output_sanitizer(config),
        stop_words=StopWords(config.slack.stop_words),
        filters=filters
        or default_chain(
            clock,
            teams=config.filter.allowed_teams,
            channels=config.filter.allowed_channels,
            users=config.filter.allowed_users,
            max_chars=config.filter.max_message_chars,
            per_minute=config.filter.rate_limit_per_minute,
        ),
        guard_on_error=config.input_guard.on_error,
        settings=SessionSettings(
            interrupt_mode=config.slack.interrupt_mode,
            heartbeat_interval_seconds=config.slack.progress.heartbeat_interval_seconds,
            idle_timeout_seconds=config.agent.idle_client_timeout_seconds,
        ),
        state_store=state_store,
        bot_instance_id=bot_instance_id,
        cancel_commands=runtime.cancel_commands,
        cancel_background=background.cancel_all,
        notice_source=runtime.pop_notices,
        notice_ack=runtime.ack_notices,
        sandbox_ready=runtime.is_ready,
    )
    # A backgrounded command reports its completion to its conversation.
    background.on_finished = orchestrator.notify
    services = Services(orchestrator, runtime, state_store, bot_instance_id, skills)
    if (
        built_runtime
        and start_background
        and config.sandbox.runtime == "gke_session_sandbox"
        and config.sandbox.run_reconciler
    ):
        # The Bot is the library's single "API process": it owns the SQLite repository,
        # so the idle-suspend / retention reconciler runs here too.
        from gke_session_sandbox import LifecycleReconciler

        reconciler = LifecycleReconciler(runtime.manager)  # type: ignore[attr-defined]
        services.background.append(asyncio.create_task(reconciler.run_forever()))
    if skills is not None and start_background:
        try:
            await skills.refresh()
        except Exception:
            log.exception("initial skill sync failed; continuing without skills")
        services.background.append(
            asyncio.create_task(
                run_skill_sync(
                    skills, clock, config.skills.sync_interval_seconds, stop=services._stop
                )
            )
        )
    return services
