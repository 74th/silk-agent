"""usecases/template: the template's settings load and the files they refer to exist."""

import io
import json
import shutil
import subprocess
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path

import pytest

from silk_agent.config import Config, HookEntry, load_config
from silk_agent.hooks import HookDispatcher, ToolEnd, UserInput, load_hooks

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "usecases" / "template"
IN_IMAGE = "/app/usecase/"  # where bot.Dockerfile puts the use case's files


@pytest.fixture
def template_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(TEMPLATE / "src"))


def test_template_config_loads():
    cfg = load_config(TEMPLATE / "config" / "silk-agent.yaml")
    assert cfg.agent.system_prompt_file == f"{IN_IMAGE}prompts/agent-system.md"
    assert cfg.skills.source == f"{IN_IMAGE}skills"
    assert [h if isinstance(h, str) else h.factory for h in cfg.hooks] == [
        "silk_usecase_template.hooks:create",
        "silk_agent.hooks:json_lines",
    ]


def test_files_the_config_refers_to_are_in_the_template():
    cfg = load_config(TEMPLATE / "config" / "silk-agent.yaml")
    refs = [
        cfg.agent.system_prompt_file,
        cfg.input_guard.policy_file,
        cfg.skills.source,
    ]
    for ref in refs:
        assert ref and ref.startswith(IN_IMAGE), ref
        assert (TEMPLATE / ref.removeprefix(IN_IMAGE)).exists(), ref
    # The ConfigMap files are mounted at /config.
    assert cfg.mcp.config_file == "/config/mcp.json" and (TEMPLATE / "config/mcp.json").is_file()
    assert cfg.sandbox.config_file == "/config/sandbox-config.yaml"
    assert (TEMPLATE / "config/sandbox-config.yaml").is_file()


def test_template_skill_has_a_front_matter_and_a_script():
    skill = (TEMPLATE / "skills" / "example" / "SKILL.md").read_text(encoding="utf-8")
    assert skill.startswith("---\nname: example\n")
    assert (TEMPLATE / "skills" / "example" / "scripts" / "hello.sh").is_file()


def test_template_input_guard_policy_keeps_the_generic_rules():
    generic = (ROOT / "src/silk_agent/resources/input-guard.md").read_text(encoding="utf-8")
    assert (
        (TEMPLATE / "policies" / "input-guard.md")
        .read_text(encoding="utf-8")
        .startswith(generic.rstrip("\n"))
    )


def test_template_prompt_keeps_the_sandbox_instructions():
    text = (TEMPLATE / "prompts" / "agent-system.md").read_text(encoding="utf-8")
    assert "mcp__sandbox__" in text and "<system-notice>" in text


async def test_template_hooks_load_and_mask(template_on_path):
    cfg = load_config(TEMPLATE / "config" / "silk-agent.yaml")
    cfg.hooks = [
        HookEntry(factory="silk_usecase_template.hooks:create", options={"include_content": True})
    ]
    (hook,) = load_hooks(cfg)
    at = datetime(2026, 10, 10, 9, 0, tzinfo=UTC)
    out = io.StringIO()
    with redirect_stdout(out):
        dispatcher = HookDispatcher([hook])
        dispatcher.publish(
            UserInput(
                conversation_key="slack:T:C:1",
                at=at,
                text="mail me at taro@example.com, api_key=abc123",
                decision="accepted",
                user="U1",
            )
        )
        dispatcher.publish(
            ToolEnd(
                conversation_key="slack:T:C:1",
                at=at,
                tool_use_id="x",
                tool_name="bash",
                status="success",
                tool_response={"stdout": "token: s3cret\n"},
            )
        )
        await dispatcher.aclose()
    first, second = (json.loads(line) for line in out.getvalue().splitlines())
    assert first["text"] == "mail me at [EMAIL], api_key=[MASKED]"
    assert second["tool_response"] == {"stdout": "token: [MASKED]\n"}


async def test_template_hooks_leave_content_out_by_default(template_on_path):
    cfg = Config()
    cfg.hooks = [HookEntry(factory="silk_usecase_template.hooks:create")]
    (hook,) = load_hooks(cfg)
    out = io.StringIO()
    with redirect_stdout(out):
        dispatcher = HookDispatcher([hook])
        dispatcher.publish(
            UserInput(
                conversation_key="slack:T:C:1",
                at=datetime(2026, 10, 10, 9, 0, tzinfo=UTC),
                text="secret text",
                decision="accepted",
            )
        )
        await dispatcher.aclose()
    (line,) = out.getvalue().splitlines()
    assert "text" not in json.loads(line) and "secret" not in line


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl is not installed")
@pytest.mark.parametrize("path", ["usecases/template/k8s", "k8s/overlays/dev", "k8s/base"])
def test_kustomizations_render(path):
    result = subprocess.run(
        ["kubectl", "kustomize", str(ROOT / path)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "kind: Deployment" in result.stdout
    assert ("kind: ConfigMap" in result.stdout) == (path != "k8s/base")
