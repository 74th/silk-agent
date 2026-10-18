import json

import pytest

from silk_agent.agent.mcp_config import McpConfigError
from silk_agent.config import Config
from silk_agent.distributed.worker_role import validate_actor_inputs


def make_config(tmp_path, **sections):
    config = Config()
    config._base_dir = tmp_path
    config.input_guard.provider = "allow_all"
    config.output_sanitizer.provider = "noop"
    for section, values in sections.items():
        for name, value in values.items():
            setattr(getattr(config, section), name, value)
    return config


def test_a_default_configuration_passes(tmp_path):
    validate_actor_inputs(make_config(tmp_path))


def test_a_missing_system_prompt_stops_the_worker_at_start_up(tmp_path):
    config = make_config(tmp_path, agent={"system_prompt_file": "nope.md"})
    with pytest.raises(FileNotFoundError):
        validate_actor_inputs(config)


def test_an_undefined_env_in_mcp_json_stops_the_worker_at_start_up(tmp_path, monkeypatch):
    monkeypatch.delenv("SILK_TEST_MCP_TOKEN", raising=False)
    (tmp_path / "mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "echo": {
                        "type": "http",
                        "url": "http://echo",
                        "headers": {"Authorization": "Bearer ${SILK_TEST_MCP_TOKEN}"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    config = make_config(tmp_path, mcp={"config_file": "mcp.json"})
    with pytest.raises(McpConfigError):
        validate_actor_inputs(config)
