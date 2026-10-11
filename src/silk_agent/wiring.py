"""Builds the engine factory and guards from Config (shared by the Slack and Streamlit apps)."""

from __future__ import annotations

from collections.abc import Callable
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
    SessionStateStore,
    SkillGeneration,
    SkillSource,
)
from silk_agent.testing.fakes import AllowAllGuard, NoopSanitizer


def build_input_guard(config: Config, root: Path) -> InputGuard:
    cfg = config.input_guard
    if cfg.provider == "allow_all":
        return AllowAllGuard()
    from silk_agent.guards.gemini import GeminiPolicyGuard

    return GeminiPolicyGuard(
        policy_file=root / cfg.policy_file,
        model=cfg.model,
        project=cfg.project,
        location=cfg.location,
    )


def build_output_sanitizer(config: Config) -> OutputSanitizer:
    cfg = config.output_sanitizer
    if cfg.provider == "noop":
        return NoopSanitizer()
    from silk_agent.guards.model_armor import ModelArmorSanitizer

    return ModelArmorSanitizer(
        template=cfg.template,
        endpoint=cfg.endpoint,
        on_sdp_match=cfg.on_sdp_match,
        ignored_filters=cfg.ignored_filters,
    )


def build_engine_factory(
    config: Config,
    runtime: SandboxRuntime,
    clock: Clock,
    *,
    root: Path,
    work_root: Path,
    session_store: Any | None = None,
    skills: SkillSource | None = None,
    state_store: SessionStateStore | None = None,
    on_session_id: Callable[[str, str], None] | None = None,
    model: str | None = None,
    extra_env: dict[str, str] | None = None,
    background: BackgroundCommands | None = None,
) -> AgentEngineFactory:
    mcp = load_mcp_servers((root / config.mcp.config_file) if config.mcp.config_file else None)
    prompt_path = root / "policies" / "agent-system.md"
    system_prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""

    tracker = CommandTracker(state_store) if state_store is not None else None
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
        extra_env=extra_env or {},
    )

    def factory(key: str) -> ClaudeAgentEngine:
        return ClaudeAgentEngine(key, settings, clock)

    return factory
