"""Configuration models and loader."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

log = logging.getLogger(__name__)


class ConfigError(ValueError):
    """The configuration file is missing a required value or is otherwise unusable."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProgressConfig(_Model):
    heartbeat_interval_seconds: float = 60
    timezone: str = "Asia/Tokyo"
    max_tools_shown: int = 20
    tool_display: Literal["name", "summary", "verbose"] = "summary"

    @field_validator("heartbeat_interval_seconds")
    @classmethod
    def _positive_interval(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("must be greater than 0")
        return value

    @field_validator("timezone")
    @classmethod
    def _known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError(f"unknown time zone {value!r}") from e
        return value


class SlackConfig(_Model):
    app_token_secret: str = ""
    bot_token_secret: str = ""
    stop_words: list[str] = Field(default_factory=lambda: ["停止", "中止", "中断", "STOP"])
    # A message that is exactly one of these resets the conversation (distributed Gateway only).
    reset_words: list[str] = Field(default_factory=lambda: ["リセット", "RESET"])
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
    # Agent Platform rejects a session `ttl` under 24 hours.
    ttl_seconds: int = Field(default=2592000, ge=86400)


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


class RetentionConfig(_Model):
    """Days a processed Firestore record is kept before TTL deletes it."""

    terminal_days: int = Field(default=30, gt=0)  # inputs, controls, outbox, commands, quiesce
    receipt_days: int = Field(default=7, gt=0)  # Slack receipts (must outlive Slack's replays)
    worker_days: int = Field(default=7, gt=0)  # from the last heartbeat


class DistributedExecutionConfig(_Model):
    firestore_project: str = ""
    firestore_database: str = "(default)"
    # No default: a deployment that forgets it must not join another use case's collections.
    firestore_prefix: str = ""
    # Compare POD_NAMESPACE with the namespaces named in this config at startup. Turn it off
    # only outside Kubernetes (local development).
    namespace_check: bool = True
    sandbox_service_url: str = ""
    command_output_bucket: str = ""
    command_output_retention_days: int = Field(default=30, gt=0)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    lease_seconds: int = Field(default=60, gt=0)
    lease_renewal_seconds: int = Field(default=20, gt=0)
    lease_clock_skew_seconds: int = Field(default=5, ge=0)
    max_active_sdks_per_worker: int = Field(default=4, gt=0)
    idle_close_seconds: int = Field(default=600, gt=0)
    drain_timeout_seconds: int | None = Field(default=None, gt=0)
    sandbox_service_audience: str = "silk-agent-sandbox-service"
    sandbox_service_allowed_ksas: list[str] = Field(default_factory=list)
    sandbox_service_port: int = Field(default=8080, ge=1, le=65535)
    # Liveness/readiness/metrics endpoint of the Gateway, Worker and Control roles.
    health_port: int = Field(default=9090, ge=1, le=65535)
    # Recover an expired ownership without Worker evidence when Kubernetes says its Pod UID no
    # longer exists, after `auto_recover_after_seconds` past lease expiry (default: twice the
    # lease, long enough for a partitioned live Worker to stop itself). Off by default: a
    # force-deleted Pod on a partitioned node can still be running.
    auto_recover_missing_pods: bool = False
    auto_recover_after_seconds: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def lease_renewal_precedes_expiration(self) -> DistributedExecutionConfig:
        if self.lease_renewal_seconds >= self.lease_seconds:
            raise ValueError("lease_renewal_seconds must be less than lease_seconds")
        return self


class HookEntry(_Model):
    factory: str  # "package.module:function"
    options: dict[str, Any] = Field(default_factory=dict)


class HookDeliveryConfig(_Model):
    queue_size: int = 10000  # events waiting for delivery; the oldest are dropped beyond this
    timeout_seconds: float = 10  # per hook call
    shutdown_timeout_seconds: float = 10  # delivery of queued events when the Bot stops


class Config(_Model):
    execution_mode: Literal["in_process", "distributed"] = "in_process"
    distributed: DistributedExecutionConfig = Field(default_factory=DistributedExecutionConfig)
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
        if self.execution_mode == "distributed":
            if not self.distributed.firestore_project:
                raise ConfigError("missing required settings: distributed.firestore_project")
            if not self.distributed.firestore_prefix.strip():
                raise ConfigError("missing required settings: distributed.firestore_prefix")
            if not self.distributed.sandbox_service_url:
                raise ConfigError("missing required settings: distributed.sandbox_service_url")
            if not self.distributed.command_output_bucket.strip():
                raise ConfigError("missing required settings: distributed.command_output_bucket")
            if self.sandbox.runtime != "gke_session_sandbox":
                raise ConfigError("distributed mode requires sandbox.runtime=gke_session_sandbox")

    def validate_distributed_sandbox_config(self) -> None:
        """Do not let a distributed deployment silently share SQLite manager state."""
        if self.execution_mode != "distributed" or not self.sandbox.config_file:
            return
        path = self.resolve(self.sandbox.config_file)
        try:
            with path.open(encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except OSError as exc:
            raise ConfigError(f"cannot read sandbox config for distributed mode: {path}") from exc
        if isinstance(data, dict) and data.get("repository_database_path"):
            raise ConfigError(
                "distributed mode cannot use SQLite sandbox state; remove "
                "repository_database_path from the sandbox config"
            )

    def validate_namespace(
        self,
        role: Literal["gateway", "worker", "control", "sandbox-service"],
        env: Mapping[str, str] | None = None,
    ) -> None:
        """Refuse to start when this Pod's namespace differs from the namespaces in the config.

        Every mismatch is reported with the setting name, the expected and the actual value, so a
        role started with another use case's config never connects to that use case.
        """
        if self.execution_mode != "distributed":
            return
        if not self.distributed.namespace_check:
            log.warning("distributed.namespace_check is false: namespace validation is disabled")
            return
        pod_namespace = (os.environ if env is None else env).get("POD_NAMESPACE", "").strip()
        if not pod_namespace:
            raise ConfigError(
                "POD_NAMESPACE is not set; set it with the Downward API, or set "
                "distributed.namespace_check: false outside Kubernetes"
            )
        problems: list[str] = []

        def expect(name: str, actual: str) -> None:
            if actual != pod_namespace:
                problems.append(f"{name}: expected namespace {pod_namespace!r}, got {actual!r}")

        if role == "sandbox-service":
            expect("sandbox.control_namespace", self._sandbox_control_namespace())
            for ksa in self.distributed.sandbox_service_allowed_ksas:
                parts = ksa.split(":")
                namespace = (
                    parts[2]
                    if len(parts) == 4 and parts[:2] == ["system", "serviceaccount"]
                    else ""
                )
                expect(f"distributed.sandbox_service_allowed_ksas[{ksa}]", namespace)
        else:
            host = urlparse(self.distributed.sandbox_service_url).hostname or ""
            labels = host.split(".")
            expect(
                "distributed.sandbox_service_url",
                labels[1] if len(labels) >= 3 and labels[2] == "svc" else "",
            )
        if problems:
            raise ConfigError("configuration does not match the deployment: " + "; ".join(problems))

    def _sandbox_control_namespace(self) -> str:
        from gke_session_sandbox.config import ManagerConfig

        default = str(ManagerConfig.model_fields["control_namespace"].default)
        if not self.sandbox.config_file:
            return default
        try:
            with self.resolve(self.sandbox.config_file).open(encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except OSError as exc:
            raise ConfigError("cannot read the sandbox config to check its namespace") from exc
        value = data.get("control_namespace") if isinstance(data, dict) else None
        return str(value) if value else default


def load_config(path: str | Path) -> Config:
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    config = Config.model_validate(data)
    config._base_dir = path.resolve().parent
    config.validate_required()
    config.validate_distributed_sandbox_config()
    return config
