from pathlib import Path

from silk_agent.agent.mcp_config import load_mcp_servers
from silk_agent.config import load_config

ROOT = Path(__file__).resolve().parent.parent


def test_files_referenced_by_the_example_config_exist():
    cfg = load_config(ROOT / "config" / "silk-agent.example.yaml")
    assert cfg.mcp.config_file and cfg.resolve(cfg.mcp.config_file).is_file()
    assert load_mcp_servers(cfg.resolve(cfg.mcp.config_file), {}) == {}


def test_k8s_config_loads():
    cfg = load_config(ROOT / "k8s" / "overlays" / "dev" / "config" / "silk-agent.yaml")
    assert cfg.agent.project
    assert cfg.sandbox.config_file == "/config/sandbox-config.yaml"
