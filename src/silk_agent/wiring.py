"""Builds the engine factory and guards from Config (shared by the Slack and Streamlit apps)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from silk_agent.agent.engine import ClaudeAgentEngine, ClaudeEngineSettings
from silk_agent.agent.mcp_config import load_mcp_servers
from silk_agent.agent.sandbox_tools import build_sandbox_server
from silk_agent.config import Config
from silk_agent.core.background import BackgroundCommands
from silk_agent.core.commands import CommandTracker
from silk_agent.core.ports import (
    AgentEngineFactory,
    Clock,
    InputGuard,
    OutputSanitizer,
    SandboxRuntime,
    SkillGeneration,
    SkillSource,
)
from silk_agent.guards.noop import AllowAllGuard, NoopSanitizer
from silk_agent.hooks import HookEvent
from silk_agent.resources import read_resource


def build_input_guard(config: Config) -> InputGuard:
    cfg = config.input_guard
    if cfg.provider == "allow_all":
        return AllowAllGuard()
    from silk_agent.guards.gemini import GeminiPolicyGuard

    # A configured file that does not exist stops the start-up (no silent fallback).
    policy = (
        config.resolve(cfg.policy_file).read_text(encoding="utf-8")
        if cfg.policy_file
        else read_resource("input-guard.md")
    )
    return GeminiPolicyGuard(
        policy=policy,
        model=cfg.model,
        project=cfg.project,
        location=cfg.location,
    )


def model_armor_endpoint(template: str) -> str | None:
    """`projects/P/locations/L/templates/T` -> `modelarmor.L.rep.googleapis.com`."""
    parts = template.split("/")
    if len(parts) >= 4 and parts[2] == "locations":
        return f"modelarmor.{parts[3]}.rep.googleapis.com"
    return None


def build_system_prompt(config: Config) -> str:
    if config.agent.system_prompt_file:
        return config.resolve(config.agent.system_prompt_file).read_text(encoding="utf-8")
    return read_resource("agent-system.md")


def build_output_sanitizer(config: Config) -> OutputSanitizer:
    cfg = config.output_sanitizer
    if cfg.provider == "noop":
        return NoopSanitizer()
    from silk_agent.guards.model_armor import ModelArmorSanitizer

    return ModelArmorSanitizer(
        template=cfg.template,
        endpoint=cfg.endpoint or model_armor_endpoint(cfg.template),
        on_sdp_match=cfg.on_sdp_match,
        ignored_filters=cfg.ignored_filters,
    )


def build_engine_factory(
    config: Config,
    runtime: SandboxRuntime,
    clock: Clock,
    *,
    work_root: Path,
    session_store: Any | None = None,
    skills: SkillSource | None = None,
    tracker: CommandTracker | None = None,
    on_session_id: Callable[[str, str], None] | None = None,
    on_query_start: Callable[[str, str, str, str], Awaitable[None]] | None = None,
    model: str | None = None,
    extra_env: dict[str, str] | None = None,
    background: BackgroundCommands | None = None,
    on_hook_event: Callable[[HookEvent], None] | None = None,
) -> AgentEngineFactory:
    mcp = load_mcp_servers(
        config.resolve(config.mcp.config_file) if config.mcp.config_file else None
    )
    system_prompt = build_system_prompt(config)

    bg = background or BackgroundCommands(runtime, tracker)

    def servers(key: str) -> dict[str, Any]:
        return {"sandbox": build_sandbox_server(key, runtime, tracker, bg), **mcp}

    async def on_skills(key: str, generation: SkillGeneration) -> None:
        await runtime.copy_skills(key, generation.root, generation.generation_id)

    settings = ClaudeEngineSettings(
        model=model or config.agent.model,
        project=config.agent.project,
        region=config.agent.region,
        work_root=work_root,
        system_prompt=system_prompt,
        max_turns=config.agent.max_turns,
        max_budget_usd=config.agent.max_budget_usd_per_turn,
        session_store=session_store,
        mcp_servers=servers,
        skills=skills,
        on_skills_generation=on_skills if skills is not None else None,
        on_session_id=on_session_id,
        on_query_start=on_query_start,
        on_hook_event=on_hook_event,
        extra_env=extra_env or {},
    )

    def factory(key: str) -> ClaudeAgentEngine:
        return ClaudeAgentEngine(key, settings, clock)

    return factory
