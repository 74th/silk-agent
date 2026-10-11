"""Safe structured YAML rendering for core Sandbox manifests."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .config import reject_reserved_manifest_fields
from .errors import TemplateValidationError

_PARAMETER_TAG = "$param"


@dataclass(frozen=True, slots=True)
class RenderedTemplate:
    """A validated manifest and its deterministic content digest."""

    manifest: dict[str, Any]
    digest: str


class TemplateRenderer:
    """Resolve typed ``$param`` nodes and overrides without text substitution."""

    def render_file(
        self,
        path: str | Path,
        *,
        parameters: Mapping[str, Any] | None = None,
        image_overrides: Mapping[str, str] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> RenderedTemplate:
        template_path = Path(path)
        try:
            with template_path.open("r", encoding="utf-8") as source:
                loaded = yaml.safe_load(source)
        except (OSError, yaml.YAMLError) as exc:
            raise TemplateValidationError(f"cannot load template {template_path}: {exc}") from exc
        return self.render(
            loaded,
            parameters=parameters,
            image_overrides=image_overrides,
            overrides=overrides,
        )

    def render(
        self,
        template: Mapping[str, Any],
        *,
        parameters: Mapping[str, Any] | None = None,
        image_overrides: Mapping[str, str] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> RenderedTemplate:
        if not isinstance(template, Mapping):
            raise TemplateValidationError("template root must be a YAML mapping")
        source = copy.deepcopy(dict(template))
        reject_reserved_manifest_fields(source)
        if overrides:
            reject_reserved_manifest_fields(overrides)
        resolved = _resolve_parameters(source, parameters or {})
        if overrides:
            _deep_merge(resolved, overrides)
        if image_overrides:
            _apply_image_overrides(resolved, image_overrides)
        reject_reserved_manifest_fields(resolved)
        _validate_sandbox_manifest(resolved)

        canonical = json.dumps(
            resolved,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return RenderedTemplate(
            manifest=resolved,
            digest=hashlib.sha256(canonical).hexdigest(),
        )


def _resolve_parameters(value: Any, parameters: Mapping[str, Any], path: str = "") -> Any:
    if isinstance(value, Mapping):
        if _PARAMETER_TAG in value:
            if set(value) != {_PARAMETER_TAG} or not isinstance(value[_PARAMETER_TAG], str):
                raise TemplateValidationError(f"malformed parameter node at {path or '<root>'}")
            name = value[_PARAMETER_TAG]
            resolved = _lookup_parameter(parameters, name)
            return copy.deepcopy(resolved)
        return {
            key: _resolve_parameters(child, parameters, f"{path}.{key}" if path else str(key))
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [
            _resolve_parameters(child, parameters, f"{path}[{index}]")
            for index, child in enumerate(value)
        ]
    return copy.deepcopy(value)


def _lookup_parameter(parameters: Mapping[str, Any], name: str) -> Any:
    current: Any = parameters
    for segment in name.split("."):
        if not segment or not isinstance(current, Mapping) or segment not in current:
            raise TemplateValidationError(f"template references undefined parameter {name!r}")
        current = current[segment]
    return current


def _deep_merge(target: dict[str, Any], source: Mapping[str, Any], path: str = "") -> None:
    for key, value in source.items():
        if not isinstance(key, str):
            raise TemplateValidationError(f"override key at {path or '<root>'} must be a string")
        if isinstance(value, Mapping) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value, f"{path}.{key}" if path else key)
        else:
            target[key] = copy.deepcopy(value)


def _apply_image_overrides(manifest: dict[str, Any], images: Mapping[str, str]) -> None:
    pod_template = manifest.get("spec", {}).get("podTemplate", {})
    pod_spec = pod_template.get("spec", {}) if isinstance(pod_template, Mapping) else {}
    containers = pod_spec.get("containers") if isinstance(pod_spec, Mapping) else None
    if not isinstance(containers, list):
        raise TemplateValidationError("image overrides require spec.podTemplate.spec.containers")

    by_name = {
        container.get("name"): container
        for container in containers
        if isinstance(container, dict) and isinstance(container.get("name"), str)
    }
    for container_name, image in images.items():
        if not isinstance(container_name, str) or not isinstance(image, str) or not image.strip():
            raise TemplateValidationError("container image overrides require non-empty string names and images")
        if container_name not in by_name:
            raise TemplateValidationError(f"image override refers to unknown container {container_name!r}")
        by_name[container_name]["image"] = image


def _validate_sandbox_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("apiVersion") != "agents.x-k8s.io/v1beta1":
        raise TemplateValidationError("template apiVersion must be agents.x-k8s.io/v1beta1")
    if manifest.get("kind") != "Sandbox":
        raise TemplateValidationError("template kind must be Sandbox")
    spec = manifest.get("spec")
    if not isinstance(spec, Mapping):
        raise TemplateValidationError("template spec must be a mapping")
    pod_template = spec.get("podTemplate")
    if not isinstance(pod_template, Mapping):
        raise TemplateValidationError("template spec.podTemplate must be a mapping")
    pod_spec = pod_template.get("spec")
    if not isinstance(pod_spec, Mapping):
        raise TemplateValidationError("template spec.podTemplate.spec must be a mapping")
    containers = pod_spec.get("containers")
    if not isinstance(containers, list) or not containers:
        raise TemplateValidationError("template must define at least one pod container")
    names: set[str] = set()
    for container in containers:
        if not isinstance(container, Mapping):
            raise TemplateValidationError("each pod container must be a mapping")
        name, image = container.get("name"), container.get("image")
        if not isinstance(name, str) or not name.strip():
            raise TemplateValidationError("every pod container requires a non-empty name")
        if name in names:
            raise TemplateValidationError(f"duplicate container name {name!r}")
        names.add(name)
        if not isinstance(image, str) or not image.strip():
            raise TemplateValidationError(f"container {name!r} requires a non-empty image")
