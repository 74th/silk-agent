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


needs_kubectl = pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl is required")


def _render_distributed_template(tmp_path: Path, values: dict[str, str]) -> tuple[list[dict], Path]:
    """Copy the template's distributed overlay next to the shared base, replace the `your-...`
    values, and render it with kustomize."""
    import yaml

    shutil.copytree(ROOT / "k8s/distributed/base", tmp_path / "k8s/distributed/base")
    shutil.copytree(ROOT / "k8s/distributed/platform", tmp_path / "k8s/distributed/platform")
    target = tmp_path / "usecases/demo/k8s/distributed"
    shutil.copytree(TEMPLATE / "k8s/distributed", target)
    steady = target / "steady"
    for path in target.rglob("*"):
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            for old, new in values.items():
                text = text.replace(old, new)
            path.write_text(text, encoding="utf-8")
    out = subprocess.run(
        ["kubectl", "kustomize", str(steady)], check=True, capture_output=True, text=True
    ).stdout
    return [item for item in yaml.safe_load_all(out) if item], target


DISTRIBUTED_VALUES = {
    "your-usecase": "demo",
    "your-gcp-project": "proj",
    "your-repository": "repo",
    "your-command-output-bucket": "bucket",
    "your-template": "armor",
}


@needs_kubectl
def test_distributed_template_renders_in_the_chosen_namespaces(tmp_path):
    rendered, _ = _render_distributed_template(tmp_path, DISTRIBUTED_VALUES)
    namespaced = [i for i in rendered if i["kind"] not in ("Namespace", "ClusterRole")]
    spaces = {
        i["metadata"].get("namespace") for i in namespaced if i["kind"] != "ClusterRoleBinding"
    }
    assert spaces == {"demo-system", "demo-sandboxes"}
    assert {i["metadata"]["name"] for i in rendered if i["kind"] == "Namespace"} == spaces
    binding = next(i for i in rendered if i["kind"] == "ClusterRoleBinding")
    assert binding["metadata"]["name"] == "silk-agent-sandbox-service-token-review-demo"
    assert binding["subjects"][0]["namespace"] == "demo-system"
    # Nothing of the silk-agent development environment is left.
    text = str(rendered)
    assert "session-sandbox-system" not in text and "session-sandboxes" not in text
    assert "nnyn-dev" not in text


@needs_kubectl
def test_distributed_template_config_loads_and_passes_the_namespace_check(tmp_path, monkeypatch):
    import yaml

    _, target = _render_distributed_template(tmp_path, DISTRIBUTED_VALUES)
    config_file = target / "steady/config/silk-agent-distributed.yaml"
    data = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    sandbox_file = target / "steady/config/sandbox-service-config.yaml"
    data["sandbox"]["config_file"] = str(sandbox_file)
    data["mcp"]["config_file"] = str(target / "steady/config/mcp.json")
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")

    cfg = load_config(config_file)  # runs validate_required
    assert cfg.distributed.firestore_prefix == "demo"
    assert "repository_database_path" not in sandbox_file.read_text(encoding="utf-8")
    for role in ("gateway", "worker", "control", "sandbox-service"):
        cfg.validate_namespace(role, {"POD_NAMESPACE": "demo-system"})


@needs_kubectl
def test_distributed_template_bootstrap_has_no_running_role(tmp_path):
    import yaml

    _, target = _render_distributed_template(tmp_path, DISTRIBUTED_VALUES)
    out = subprocess.run(
        ["kubectl", "kustomize", str(target / "bootstrap")],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    replicas = {
        i["metadata"]["name"]: i["spec"]["replicas"]
        for i in yaml.safe_load_all(out)
        if i and i["kind"] == "Deployment"
    }
    assert replicas["silk-agent-gateway"] == replicas["silk-agent-worker"] == 0
    assert replicas["silk-agent-control"] == 0
