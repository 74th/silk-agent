import pytest

from silk_agent.cli import CONFIG_ENV, main


def test_config_is_required(monkeypatch):
    monkeypatch.delenv(CONFIG_ENV, raising=False)
    with pytest.raises(SystemExit) as e:
        main(["slack"])
    assert CONFIG_ENV in str(e.value)


def test_missing_config_file(monkeypatch, tmp_path):
    monkeypatch.setenv(CONFIG_ENV, str(tmp_path / "nope.yaml"))
    with pytest.raises(SystemExit) as e:
        main(["slack"])
    assert "nope.yaml" in str(e.value)


def test_config_argument_wins_over_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv(CONFIG_ENV, str(tmp_path / "env.yaml"))
    with pytest.raises(SystemExit) as e:
        main(["slack", "--config", str(tmp_path / "arg.yaml")])
    assert "arg.yaml" in str(e.value)


def test_invalid_config_names_the_missing_setting(monkeypatch, tmp_path):
    path = tmp_path / "silk-agent.yaml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(SystemExit) as e:
        main(["slack", "--config", str(path)])
    assert "agent.project" in str(e.value)
