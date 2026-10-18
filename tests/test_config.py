from pathlib import Path

import pytest

from silk_agent.app import build_state_store
from silk_agent.config import Config, ConfigError, load_config
from silk_agent.core.ports import ConversationState
from silk_agent.resources import read_resource
from silk_agent.wiring import build_input_guard, build_system_prompt

ROOT = Path(__file__).resolve().parent.parent

MINIMAL = """
agent:
  project: p
input_guard:
  provider: allow_all
output_sanitizer:
  provider: noop
session_store:
  provider: memory
sandbox:
  runtime: local_process
"""


def write(tmp_path: Path, text: str, name: str = "silk-agent.yaml") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_example_config_loads():
    cfg = load_config(ROOT / "config" / "silk-agent.example.yaml")
    assert cfg.slack.stop_words == ["停止", "中止", "中断", "STOP"]
    assert cfg.slack.interrupt_mode == "append"
    assert cfg.slack.progress.heartbeat_interval_seconds == 60
    assert cfg.slack.progress.max_tools_shown == 20
    assert cfg.filter.max_message_chars == 8000
    assert cfg.input_guard.model == "gemini-3.8-flash"
    assert cfg.input_guard.on_error == "block"
    assert cfg.output_sanitizer.template.endswith("/templates/simple")
    assert cfg.session_store.ttl_seconds == 2592000
    assert cfg.agent.model == "claude-opus-5-5"
    assert cfg.skills.sync_interval_seconds == 300


def test_defaults_with_minimal_file(tmp_path):
    cfg = load_config(write(tmp_path, MINIMAL))
    assert cfg.slack.interrupt_mode == "append"
    assert cfg.execution_mode == "in_process"
    assert cfg.distributed.lease_seconds == 60
    assert cfg.distributed.lease_renewal_seconds == 20
    assert cfg.distributed.lease_clock_skew_seconds == 5
    assert cfg.distributed.max_active_sdks_per_worker == 4
    assert cfg.distributed.idle_close_seconds == 600
    assert cfg.distributed.drain_timeout_seconds is None
    assert cfg.distributed.command_output_retention_days == 30
    assert cfg.distributed.retention.terminal_days == 30
    assert cfg.distributed.retention.receipt_days == 7
    assert cfg.distributed.retention.worker_days == 7


@pytest.mark.parametrize(
    "field,value",
    [
        ("lease_seconds", -1),
        ("lease_renewal_seconds", 0),
        ("lease_clock_skew_seconds", -1),
        ("max_active_sdks_per_worker", 0),
        ("idle_close_seconds", -1),
        ("drain_timeout_seconds", -1),
        ("command_output_retention_days", 0),
    ],
)
def test_distributed_config_rejects_invalid_durations_and_limits(field, value):
    with pytest.raises(ValueError):
        Config(distributed={field: value})


@pytest.mark.parametrize("field", ["terminal_days", "receipt_days", "worker_days"])
@pytest.mark.parametrize("value", [0, -1])
def test_distributed_retention_rejects_non_positive_days(field, value):
    with pytest.raises(ValueError):
        Config(distributed={"retention": {field: value}})


def test_distributed_retention_accepts_overrides():
    cfg = Config(distributed={"retention": {"terminal_days": 14, "receipt_days": 3}})
    assert cfg.distributed.retention.terminal_days == 14
    assert cfg.distributed.retention.receipt_days == 3
    assert cfg.distributed.retention.worker_days == 7


def test_session_ttl_below_agent_platform_minimum_is_rejected():
    with pytest.raises(ValueError):
        Config(session_store={"ttl_seconds": 3600})
    assert Config(session_store={"ttl_seconds": 86400}).session_store.ttl_seconds == 86400


def test_distributed_config_requires_renewal_interval_below_lease():
    with pytest.raises(ValueError, match="less than lease_seconds"):
        Config(distributed={"lease_seconds": 20, "lease_renewal_seconds": 20})


def test_distributed_mode_requires_firestore_and_sandbox_service_settings(tmp_path):
    text = MINIMAL.replace(
        "sandbox:\n  runtime: local_process\n",
        "sandbox:\n  runtime: gke_session_sandbox\n  config_file: sandbox.yaml\n",
    )
    path = write(tmp_path, "execution_mode: distributed\n" + text)
    (tmp_path / "sandbox.yaml").write_text("{}", encoding="utf-8")

    with pytest.raises(ConfigError, match="distributed.firestore_project"):
        load_config(path)


def test_distributed_mode_rejects_sqlite_sandbox_repository(tmp_path):
    text = MINIMAL.replace(
        "sandbox:\n  runtime: local_process\n",
        "sandbox:\n  runtime: gke_session_sandbox\n  config_file: sandbox.yaml\n",
    )
    path = write(
        tmp_path,
        "execution_mode: distributed\ndistributed:\n"
        "  firestore_project: test-project\n  sandbox_service_url: http://sandbox\n"
        "  command_output_bucket: output-bucket\n" + text,
    )
    (tmp_path / "sandbox.yaml").write_text(
        "repository_database_path: /var/lib/sessions.sqlite3\n", encoding="utf-8"
    )

    with pytest.raises(ConfigError, match="cannot use SQLite sandbox state"):
        load_config(path)


def test_distributed_mode_accepts_shared_sandbox_repository_config(tmp_path):
    text = MINIMAL.replace(
        "sandbox:\n  runtime: local_process\n",
        "sandbox:\n  runtime: gke_session_sandbox\n  config_file: sandbox.yaml\n",
    )
    path = write(
        tmp_path,
        "execution_mode: distributed\ndistributed:\n"
        "  firestore_project: test-project\n  sandbox_service_url: http://sandbox\n"
        "  command_output_bucket: output-bucket\n" + text,
    )
    (tmp_path / "sandbox.yaml").write_text("{}", encoding="utf-8")

    cfg = load_config(path)
    assert cfg.execution_mode == "distributed"


def test_distributed_mode_requires_command_output_bucket(tmp_path):
    text = MINIMAL.replace(
        "sandbox:\n  runtime: local_process\n",
        "sandbox:\n  runtime: gke_session_sandbox\n  config_file: sandbox.yaml\n",
    )
    path = write(
        tmp_path,
        "execution_mode: distributed\ndistributed:\n"
        "  firestore_project: test-project\n  sandbox_service_url: http://sandbox\n" + text,
    )
    (tmp_path / "sandbox.yaml").write_text("{}", encoding="utf-8")
    with pytest.raises(ConfigError, match="distributed.command_output_bucket"):
        load_config(path)


def test_relative_paths_are_relative_to_the_config_file(tmp_path):
    cfg_dir = tmp_path / "x"
    cfg_dir.mkdir()
    path = write(cfg_dir, "")
    path.write_text(
        MINIMAL.replace(
            "input_guard:\n  provider: allow_all\n",
            "input_guard:\n  provider: allow_all\n  policy_file: policies/a.md\n",
        ),
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert cfg.resolve(cfg.input_guard.policy_file or "") == cfg_dir.resolve() / "policies/a.md"
    assert cfg.resolve("/abs/b.md") == Path("/abs/b.md")


def test_empty_file_reports_the_missing_settings(tmp_path):
    with pytest.raises(ConfigError) as e:
        load_config(write(tmp_path, ""))
    message = str(e.value)
    for name in (
        "agent.project",
        "input_guard.project",
        "output_sanitizer.template",
        "session_store.project",
        "session_store.agent_runtime",
        "sandbox.config_file",
    ):
        assert name in message


def test_missing_agent_project(tmp_path):
    with pytest.raises(ConfigError, match=r"agent\.project"):
        load_config(write(tmp_path, MINIMAL.replace("  project: p\n", "  project: ''\n")))


def test_settings_of_disabled_features_are_not_required(tmp_path):
    cfg = load_config(write(tmp_path, MINIMAL))
    assert cfg.missing_required() == []
    assert cfg.input_guard.project == ""
    assert cfg.output_sanitizer.template == ""
    assert cfg.session_store.agent_runtime == ""


def test_agent_runtime_with_a_placeholder_is_used_as_is(tmp_path):
    cfg = Config()
    cfg.session_store.agent_runtime = "projects/p/locations/l/reasoningEngines/<id>"
    cfg.session_store.project = "p"
    state_store, session_store = build_state_store(cfg)
    assert session_store is not None  # no silent switch to the in-memory store
    assert not hasattr(state_store, "states")


async def test_memory_provider_uses_the_in_memory_store():
    cfg = Config()
    cfg.session_store.provider = "memory"
    state_store, session_store = build_state_store(cfg)
    assert session_store is None
    await state_store.put(ConversationState("k"))
    assert await state_store.get("k") is not None


def test_system_prompt_default_is_the_bundled_one():
    assert build_system_prompt(Config()) == read_resource("agent-system.md")
    assert "mcp__sandbox__" in build_system_prompt(Config())


def test_system_prompt_file_is_read_relative_to_the_config(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "p.md").write_text("Answer in English.", encoding="utf-8")
    path = write(
        tmp_path,
        MINIMAL.replace("  project: p\n", "  project: p\n  system_prompt_file: prompts/p.md\n"),
    )
    assert build_system_prompt(load_config(path)) == "Answer in English."


def test_missing_system_prompt_file_stops_the_start_up(tmp_path):
    path = write(
        tmp_path, MINIMAL.replace("  project: p\n", "  project: p\n  system_prompt_file: nope.md\n")
    )
    with pytest.raises(FileNotFoundError, match="nope.md"):
        build_system_prompt(load_config(path))


def test_input_guard_policy_file_missing_stops_the_start_up(tmp_path):
    cfg = load_config(write(tmp_path, MINIMAL))
    cfg.input_guard.provider = "gemini"
    cfg.input_guard.project = "p"
    cfg.input_guard.policy_file = "nope.md"
    with pytest.raises(FileNotFoundError, match="nope.md"):
        build_input_guard(cfg)


def test_input_guard_uses_the_bundled_policy_by_default():
    cfg = Config()
    cfg.input_guard.project = "p"
    guard = build_input_guard(cfg)
    assert guard._policy == read_resource("input-guard.md")  # type: ignore[attr-defined]
