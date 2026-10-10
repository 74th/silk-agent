from pathlib import Path

from silk_agent.agent.mcp_config import load_mcp_servers
from silk_agent.config import load_config

ROOT = Path(__file__).resolve().parent.parent


def test_files_referenced_by_the_example_config_exist():
    cfg = load_config(ROOT / "config" / "silk-agent.example.yaml")
    assert (ROOT / cfg.input_guard.policy_file).is_file()
    assert cfg.mcp.config_file and (ROOT / cfg.mcp.config_file).is_file()
    assert load_mcp_servers(ROOT / cfg.mcp.config_file, {}) == {}
    assert (ROOT / "policies" / "agent-system.md").is_file()
