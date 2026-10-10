from pathlib import Path

from silk_agent.config import load_config

ROOT = Path(__file__).resolve().parent.parent


def test_example_config_loads():
    cfg = load_config(ROOT / "config" / "silk-agent.example.yaml")
    assert cfg.slack.stop_words == ["停止", "中止", "中断", "STOP"]
    assert cfg.slack.interrupt_mode == "interrupt"
    assert cfg.slack.progress.heartbeat_interval_seconds == 60
    assert cfg.slack.progress.max_tools_shown == 20
    assert cfg.filter.max_message_chars == 8000
    assert cfg.input_guard.model == "gemini-3.8-flash"
    assert cfg.input_guard.on_error == "block"
    assert cfg.output_sanitizer.template.endswith("/templates/simple")
    assert cfg.session_store.ttl_seconds == 2592000
    assert cfg.agent.model == "claude-opus-5-5"
    assert cfg.skills.sync_interval_seconds == 300


def test_defaults_without_file_content(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("")
    cfg = load_config(p)
    assert cfg.slack.interrupt_mode == "interrupt"
