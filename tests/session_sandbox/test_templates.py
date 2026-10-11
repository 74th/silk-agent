from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import yaml

import pytest

from gke_session_sandbox import TemplateRenderer, TemplateValidationError


def sandbox_template(image: object = None) -> dict[str, object]:
    return {
        "apiVersion": "agents.x-k8s.io/v1beta1",
        "kind": "Sandbox",
        "metadata": {"labels": {"app.kubernetes.io/name": "agent-session"}},
        "spec": {
            "service": {"$param": "runtime.enabled"},
            "podTemplate": {
                "spec": {
                    "containers": [
                        {"name": "sandboxd", "image": image or {"$param": "runtime.image"}},
                        {"name": "agent-tools", "image": "example.invalid/tools:v1"},
                    ],
                }
            }
        },
    }


def valid_parameters() -> dict[str, object]:
    return {"runtime": {"image": "example.invalid/sandbox:v1", "enabled": True}}


def test_render_keeps_parameter_value_types_and_is_deterministic() -> None:
    renderer = TemplateRenderer()
    parameters = {"runtime": {"image": "example.invalid/sandbox:v1", "enabled": True}}
    template = sandbox_template()

    result = renderer.render(template, parameters=parameters)
    same = renderer.render(template, parameters=parameters)

    assert result.manifest["spec"]["podTemplate"]["spec"]["containers"][0]["image"] == "example.invalid/sandbox:v1"
    assert result.manifest["spec"]["service"] is True
    assert result.digest == same.digest
    assert len(result.digest) == 64
    assert template["spec"]["podTemplate"]["spec"]["containers"][0]["image"] == {"$param": "runtime.image"}


def test_render_applies_named_image_overrides_and_digest_tracks_them() -> None:
    renderer = TemplateRenderer()
    template = sandbox_template()
    params = valid_parameters()
    first = renderer.render(template, parameters=params)
    second = renderer.render(
        template,
        parameters=params,
        image_overrides={"agent-tools": "example.invalid/tools@sha256:abcd"},
    )

    assert second.manifest["spec"]["podTemplate"]["spec"]["containers"][1]["image"] == "example.invalid/tools@sha256:abcd"
    assert second.digest != first.digest


def test_render_rejects_undefined_parameter_before_any_api_is_needed() -> None:
    with pytest.raises(TemplateValidationError, match="undefined parameter"):
        TemplateRenderer().render(sandbox_template(), parameters={})


@pytest.mark.parametrize(
    "overrides",
    [
        {"metadata": {"name": "caller-owned-name"}},
        {"metadata": {"labels": {"session-sandbox.example.com/managed": "false"}}},
        {"spec": {"operatingMode": "Running"}},
        {"spec": {"shutdownTime": "2030-01-01T00:00:00Z"}},
    ],
)
def test_render_rejects_reserved_overrides(overrides: dict[str, object]) -> None:
    with pytest.raises(TemplateValidationError, match="reserved field"):
        TemplateRenderer().render(
            sandbox_template(),
            parameters=valid_parameters(),
            overrides=overrides,
        )


def test_render_rejects_unknown_image_container_and_missing_image() -> None:
    renderer = TemplateRenderer()
    with pytest.raises(TemplateValidationError, match="unknown container"):
        renderer.render(
            sandbox_template(),
            parameters=valid_parameters(),
            image_overrides={"missing": "example.invalid/image:v1"},
        )

    bad = sandbox_template(image=None)
    bad["spec"]["podTemplate"]["spec"]["containers"][0]["image"] = ""
    with pytest.raises(TemplateValidationError, match="requires a non-empty image"):
        renderer.render(bad, parameters=valid_parameters())


def test_render_file_uses_safe_yaml_loader_and_validates_root(tmp_path: Path) -> None:
    source = tmp_path / "sandbox.yaml"
    source.write_text(
        "apiVersion: agents.x-k8s.io/v1beta1\n"
        "kind: Sandbox\n"
        "metadata: {}\n"
        "spec:\n"
        "  podTemplate:\n"
        "    spec:\n"
        "      containers:\n"
        "        - name: sandboxd\n"
        "          image: {$param: runtime.image}\n",
        encoding="utf-8",
    )

    rendered = TemplateRenderer().render_file(
        source,
        parameters=valid_parameters(),
    )
    assert rendered.manifest["kind"] == "Sandbox"
    assert rendered.digest

    source.write_text("not a sandbox: [", encoding="utf-8")
    with pytest.raises(TemplateValidationError, match="cannot load template"):
        TemplateRenderer().render_file(source)


def test_repository_template_and_example_config_render_without_cluster_access() -> None:
    root = Path(__file__).resolve().parents[2]
    config_raw = yaml.safe_load((root / "examples/sandbox-config.yaml").read_text(encoding="utf-8"))
    from gke_session_sandbox import ManagerConfig

    config = ManagerConfig.model_validate(config_raw)
    rendered = TemplateRenderer().render_file(
        root / config.template_file,
        parameters={
            "runtime": config.runtime.model_dump(),
            "workspace": {
                "storage_class": config.workspace_storage_class,
                "storage": config.workspace_storage,
            },
        },
    )
    container = rendered.manifest["spec"]["podTemplate"]["spec"]["containers"][0]

    assert container["name"] == config.runtime.container_name
    assert container["image"] == config.runtime.image
    assert container["resources"]["requests"] == {"cpu": "100m", "memory": "128Mi"}
    result = subprocess.run(
        [sys.executable, str(root / "examples/render_template.py"), "--config", str(root / "examples/sandbox-config.yaml")],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    assert rendered.digest in result.stdout
    assert "kind: Sandbox" in result.stdout
