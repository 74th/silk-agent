import json

import pytest
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

from silk_agent.agent.mcp_config import McpConfigError, load_mcp_servers


def write(tmp_path, data):
    p = tmp_path / "mcp.json"
    p.write_text(json.dumps(data))
    return p


def test_loads_http_and_sse_and_expands_env(tmp_path):
    p = write(
        tmp_path,
        {
            "mcpServers": {
                "docs": {
                    "type": "http",
                    "url": "https://docs.example/mcp",
                    "headers": {"Authorization": "Bearer ${DOCS_TOKEN}"},
                },
                "events": {"type": "sse", "url": "https://events.example/sse"},
            }
        },
    )
    servers = load_mcp_servers(p, {"DOCS_TOKEN": "t0k"})
    assert servers["docs"]["headers"]["Authorization"] == "Bearer t0k"
    assert servers["events"]["type"] == "sse"


def test_missing_env_is_an_error(tmp_path):
    p = write(tmp_path, {"mcpServers": {"d": {"type": "http", "url": "https://${HOST}/"}}})
    with pytest.raises(McpConfigError, match="HOST"):
        load_mcp_servers(p, {})


def test_stdio_is_rejected(tmp_path):
    p = write(tmp_path, {"mcpServers": {"local": {"command": "npx", "args": ["x"]}}})
    with pytest.raises(McpConfigError, match="not allowed"):
        load_mcp_servers(p, {})


def test_sandbox_name_is_reserved(tmp_path):
    p = write(tmp_path, {"mcpServers": {"sandbox": {"type": "http", "url": "https://x"}}})
    with pytest.raises(McpConfigError, match="reserved"):
        load_mcp_servers(p, {})


def test_no_file_means_no_servers():
    assert load_mcp_servers(None) == {}


def test_cwd_mcp_json_is_not_read(tmp_path):
    """The workspace has a .mcp.json declaring `other`; the CLI is started with
    --strict-mcp-config and only the configured servers, so `other` is never loaded."""
    (tmp_path / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"other": {"type": "http", "url": "https://other.example"}}})
    )
    cfg = write(tmp_path, {"mcpServers": {"docs": {"type": "http", "url": "https://docs.example"}}})
    options = ClaudeAgentOptions(
        cwd=str(tmp_path), mcp_servers=load_mcp_servers(cfg, {}), strict_mcp_config=True
    )
    transport = SubprocessCLITransport(prompt="x", options=options)
    transport._cli_path = "claude"  # resolved on connect() normally; no process is started here
    cmd = transport._build_command()
    assert "--strict-mcp-config" in cmd
    mcp_arg = cmd[cmd.index("--mcp-config") + 1]
    assert "docs" in mcp_arg and "other" not in mcp_arg
