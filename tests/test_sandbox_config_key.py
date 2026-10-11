import stat

import yaml

from silk_agent.app import with_fingerprint_key


def test_key_is_added_and_relative_template_is_resolved(tmp_path):
    src = tmp_path / "conf" / "sandbox-config.yaml"
    src.parent.mkdir()
    src.write_text("namespace: ns\ntemplate_file: templates/sandbox.yaml\n")
    out = with_fingerprint_key(src, "k" * 32, tmp_path / "work" / "sandbox-config.yaml")
    data = yaml.safe_load(out.read_text())
    assert data["environment_fingerprint_key"] == "k" * 32
    assert data["template_file"] == str(src.parent / "templates" / "sandbox.yaml")
    assert data["namespace"] == "ns"
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    assert "environment_fingerprint_key" not in src.read_text()


def test_absolute_template_is_kept(tmp_path):
    src = tmp_path / "sandbox-config.yaml"
    src.write_text("template_file: /app/sandbox-templates/sandbox.yaml\n")
    out = with_fingerprint_key(src, "k" * 32, tmp_path / "out.yaml")
    assert yaml.safe_load(out.read_text())["template_file"] == "/app/sandbox-templates/sandbox.yaml"
