"""Configuration models and loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


class ConfigError(ValueError):
    """The configuration file is missing a required value or is otherwise unusable."""


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
    allow_direct_messages: bool = True
    deny_guests: bool = False
    deny_external_users: bool = False
    max_message_chars: int = 8000
    rate_limit_per_minute: int = 20


class InputGuardConfig(_Model):
    provider: Literal["gemini", "allow_all"] = "gemini"
    model: str = "gemini-3.8-flash"
    project: str = ""  # required for provider "gemini"
    location: str = "global"
    # Relative to the config file. Empty = the generic policy bundled with silk-agent.
    policy_file: str | None = None
    on_error: Literal["block", "allow"] = "block"


class OutputSanitizerConfig(_Model):
    provider: Literal["model_armor", "noop"] = "model_armor"
    template: str = ""  # required for provider "model_armor": projects/P/locations/L/templates/T
    # Empty = derived from the template's location.
    endpoint: str = ""
    on_sdp_match: Literal["mask", "block"] = "mask"
    on_other_match: Literal["block"] = "block"
    on_error: Literal["block"] = "block"
    # Model Armor filters (filter_results keys) whose matches are ignored on outputs.
    ignored_filters: list[str] = Field(default_factory=lambda: ["pi_and_jailbreak"])


class SessionStoreConfig(_Model):
    provider: Literal["agent_platform", "memory"] = "agent_platform"
    project: str = ""  # required for provider "agent_platform"
    location: str = "us-central1"
    agent_runtime: str = ""  # required for provider "agent_platform"
    ttl_seconds: int = 2592000


class AgentConfig(_Model):
    provider: Literal["agent_platform"] = "agent_platform"
    project: str = ""  # required
    region: str = "global"
    model: str = "claude-opus-5-5"
    max_turns: int = 50
    max_budget_usd_per_turn: float = 2.0
    idle_client_timeout_seconds: float = 600
    # Relative to the config file. Empty = the generic prompt bundled with silk-agent.
    system_prompt_file: str | None = None


class McpConfig(_Model):
    config_file: str | None = None


class SkillsConfig(_Model):
    source: str | None = None
    sync_interval_seconds: float = 300


class SandboxConfig(_Model):
    runtime: Literal["gke_session_sandbox", "local_process"] = "gke_session_sandbox"
    config_file: str | None = None  # required for runtime "gke_session_sandbox"
    # Run the library's LifecycleReconciler (idle suspend / retention) in this process.
    run_reconciler: bool = True


class HookEntry(_Model):
    factory: str  # "package.module:function"
    options: dict[str, Any] = Field(default_factory=dict)


class HookDeliveryConfig(_Model):
    queue_size: int = 10000  # events waiting for delivery; the oldest are dropped beyond this
    timeout_seconds: float = 10  # per hook call
    shutdown_timeout_seconds: float = 10  # delivery of queued events when the Bot stops


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
    # Hooks: "package.module:factory" or {factory, options}. See silk_agent.hooks.
    hooks: list[str | HookEntry] = Field(default_factory=list)
    hook_delivery: HookDeliveryConfig = Field(default_factory=HookDeliveryConfig)

    # Directory of the config file; relative paths in it are resolved against this.
    _base_dir: Path = PrivateAttr(default_factory=Path.cwd)

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def resolve(self, path: str | Path) -> Path:
        """An absolute path stays as is; a relative one is relative to the config file."""
        p = Path(path)
        return p if p.is_absolute() else self._base_dir / p

    def missing_required(self) -> list[str]:
        """Names of the settings that are required by the enabled features but not set."""
        missing: list[str] = []
        if not self.agent.project:
            missing.append("agent.project")
        if self.input_guard.provider == "gemini" and not self.input_guard.project:
            missing.append("input_guard.project")
        if self.output_sanitizer.provider == "model_armor" and not self.output_sanitizer.template:
            missing.append("output_sanitizer.template")
        if self.session_store.provider == "agent_platform":
            if not self.session_store.project:
                missing.append("session_store.project")
            if not self.session_store.agent_runtime:
                missing.append("session_store.agent_runtime")
        if self.sandbox.runtime == "gke_session_sandbox" and not self.sandbox.config_file:
            missing.append("sandbox.config_file")
        return missing

    def validate_required(self) -> None:
        missing = self.missing_required()
        if missing:
            raise ConfigError("missing required settings: " + ", ".join(missing))


def load_config(path: str | Path) -> Config:
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    config = Config.model_validate(data)
    config._base_dir = path.resolve().parent
    config.validate_required()
    return config
