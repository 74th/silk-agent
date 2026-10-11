"""Configuration models and loader."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProgressConfig(_Model):
    heartbeat_interval_seconds: float = 60
    timezone: str = "Asia/Tokyo"
    max_tools_shown: int = 20
    tool_display: Literal["name", "summary", "verbose"] = "summary"


class SlackConfig(_Model):
    app_token_secret: str = ""
    bot_token_secret: str = ""
    stop_words: list[str] = Field(default_factory=lambda: ["停止", "中止", "中断", "STOP"])
    interrupt_mode: Literal["append", "interrupt", "queue"] = "append"
    progress: ProgressConfig = Field(default_factory=ProgressConfig)


class FilterConfig(_Model):
    allowed_teams: list[str] = Field(default_factory=list)
    allowed_channels: list[str] = Field(default_factory=list)
    allowed_users: list[str] = Field(default_factory=list)
    max_message_chars: int = 8000
    rate_limit_per_minute: int = 20


class InputGuardConfig(_Model):
    provider: Literal["gemini", "allow_all"] = "gemini"
    model: str = "gemini-3.8-flash"
    project: str = "nnyn-dev"
    location: str = "global"
    policy_file: str = "policies/input-guard.md"
    on_error: Literal["block", "allow"] = "block"


class OutputSanitizerConfig(_Model):
    provider: Literal["model_armor", "noop"] = "model_armor"
    template: str = "projects/nnyn-dev/locations/us-central1/templates/simple"
    endpoint: str = "modelarmor.us-central1.rep.googleapis.com"
    on_sdp_match: Literal["mask", "block"] = "mask"
    on_other_match: Literal["block"] = "block"
    on_error: Literal["block"] = "block"
    # Model Armor filters (filter_results keys) whose matches are ignored on outputs.
    ignored_filters: list[str] = Field(default_factory=lambda: ["pi_and_jailbreak"])


class SessionStoreConfig(_Model):
    provider: Literal["agent_platform", "memory"] = "agent_platform"
    project: str = "nnyn-dev"
    location: str = "us-central1"
    agent_runtime: str = ""
    ttl_seconds: int = 2592000


class AgentConfig(_Model):
    provider: Literal["agent_platform"] = "agent_platform"
    project: str = "nnyn-dev"
    region: str = "global"
    model: str = "claude-opus-5-5"
    max_turns: int = 50
    max_budget_usd_per_turn: float = 2.0
    idle_client_timeout_seconds: float = 600


class McpConfig(_Model):
    config_file: str | None = None


class SkillsConfig(_Model):
    source: str | None = None
    sync_interval_seconds: float = 300


class SandboxConfig(_Model):
    runtime: Literal["gke_session_sandbox", "local_process"] = "gke_session_sandbox"
    config_file: str | None = None
    # Run the library's LifecycleReconciler (idle suspend / retention) in this process.
    run_reconciler: bool = True


class Config(_Model):
    slack: SlackConfig = Field(default_factory=SlackConfig)
    filter: FilterConfig = Field(default_factory=FilterConfig)
    input_guard: InputGuardConfig = Field(default_factory=InputGuardConfig)
    output_sanitizer: OutputSanitizerConfig = Field(default_factory=OutputSanitizerConfig)
    session_store: SessionStoreConfig = Field(default_factory=SessionStoreConfig)
    agent: AgentConfig = Field(default_factory=AgentConfig)
    mcp: McpConfig = Field(default_factory=McpConfig)
    skills: SkillsConfig = Field(default_factory=SkillsConfig)
    sandbox: SandboxConfig = Field(default_factory=SandboxConfig)


def load_config(path: str | Path) -> Config:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return Config.model_validate(data)
