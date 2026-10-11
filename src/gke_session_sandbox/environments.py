"""Environment merging and privacy-preserving creation-env fingerprints."""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from typing import Any

from .errors import SessionConfigurationConflictError, TemplateValidationError

_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_ENV_PREFIX = "SESSION_SANDBOX_"


def merge_template_environment(
    template_env: list[Mapping[str, Any]] | None,
    creation_env: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Merge creation values over template ``env`` while preserving valueFrom entries."""
    result: list[dict[str, Any]] = []
    positions: dict[str, int] = {}
    for index, raw_entry in enumerate(template_env or []):
        if not isinstance(raw_entry, Mapping):
            raise TemplateValidationError(f"template env entry at index {index} must be a mapping")
        entry = copy.deepcopy(dict(raw_entry))
        name = entry.get("name")
        _validate_env_name(name)
        if name in positions:
            raise TemplateValidationError(f"duplicate template env name {name!r}")
        if ("value" in entry) == ("valueFrom" in entry):
            raise TemplateValidationError(f"template env {name!r} must specify exactly one of value or valueFrom")
        if "value" in entry and not isinstance(entry["value"], str):
            raise TemplateValidationError(f"template env {name!r} value must be a string")
        if "valueFrom" in entry and not isinstance(entry["valueFrom"], Mapping):
            raise TemplateValidationError(f"template env {name!r} valueFrom must be a mapping")
        positions[name] = len(result)
        result.append(entry)

    for name, value in (creation_env or {}).items():
        _validate_env_name(name)
        if not isinstance(value, str):
            raise TemplateValidationError(f"creation env {name!r} value must be a string")
        entry = {"name": name, "value": value}
        if name in positions:
            result[positions[name]] = entry
        else:
            positions[name] = len(result)
            result.append(entry)
    return result


def command_environment(command_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Validate and copy per-command env for sandboxd ProcessConfig.env_vars."""
    return _validate_env_mapping(command_env or {}, "command")


def merge_command_environment(
    inherited_env: Mapping[str, str], command_env: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Return a testable effective env map with command-specific values at highest priority."""
    result = _validate_env_mapping(inherited_env, "inherited")
    result.update(command_environment(command_env))
    return result


def creation_environment_fingerprint(environment: Mapping[str, str], key: bytes) -> str:
    """Hash env identity with a caller-supplied stable secret key; raw values are never returned."""
    if len(key) < 32:
        raise ValueError("environment fingerprint key must contain at least 32 bytes")
    clean = _validate_env_mapping(environment, "creation")
    canonical = json.dumps(clean, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hmac.new(key, canonical, hashlib.sha256).hexdigest()


def ensure_same_creation_environment(
    stored_fingerprint: str | None,
    requested_environment: Mapping[str, str] | None,
    key: bytes,
) -> bool:
    """Return whether an omitted env reuses an existing generation; reject changes."""
    if requested_environment is None:
        return True
    requested = creation_environment_fingerprint(requested_environment, key)
    if stored_fingerprint is None or not hmac.compare_digest(stored_fingerprint, requested):
        raise SessionConfigurationConflictError("creation environment differs from the existing session generation")
    return False


def _validate_env_mapping(values: Mapping[str, str], source: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in values.items():
        _validate_env_name(name)
        if not isinstance(value, str):
            raise TemplateValidationError(f"{source} env {name!r} value must be a string")
        result[name] = value
    return result


def _validate_env_name(name: Any) -> None:
    if not isinstance(name, str) or _ENV_NAME_RE.fullmatch(name) is None:
        raise TemplateValidationError("environment variable name must be a valid identifier")
    if name.startswith(_RESERVED_ENV_PREFIX):
        raise TemplateValidationError(f"environment variable {name!r} uses a reserved prefix")
