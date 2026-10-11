from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from gke_session_sandbox import (
    ManagerConfig,
    SessionConfigurationConflictError,
    TemplateValidationError,
    command_environment,
    creation_environment_fingerprint,
    ensure_same_creation_environment,
    merge_command_environment,
    merge_template_environment,
)


FINGERPRINT_KEY = b"unit-test-fingerprint-key-material-32-bytes!"


def test_creation_environment_overrides_template_and_preserves_secret_references() -> None:
    template_env = [
        {"name": "USER_EMAIL", "value": "image@example.com"},
        {"name": "API_TOKEN", "valueFrom": {"secretKeyRef": {"name": "agent", "key": "token"}}},
        {"name": "KEEP_ME", "value": "template"},
    ]

    merged = merge_template_environment(
        template_env,
        {"USER_EMAIL": "user@example.com", "API_TOKEN": "creation-token", "ADDED": "yes"},
    )

    assert merged == [
        {"name": "USER_EMAIL", "value": "user@example.com"},
        {"name": "API_TOKEN", "value": "creation-token"},
        {"name": "KEEP_ME", "value": "template"},
        {"name": "ADDED", "value": "yes"},
    ]
    assert template_env[1]["valueFrom"] == {"secretKeyRef": {"name": "agent", "key": "token"}}


def test_command_environment_has_highest_precedence_without_mutating_inputs() -> None:
    inherited = {"USER_EMAIL": "user@example.com", "MODE": "normal"}
    command = {"MODE": "debug", "RUN_ID": "cmd-1"}

    assert merge_command_environment(inherited, command) == {
        "USER_EMAIL": "user@example.com",
        "MODE": "debug",
        "RUN_ID": "cmd-1",
    }
    assert inherited == {"USER_EMAIL": "user@example.com", "MODE": "normal"}
    assert command_environment(command) == command


@pytest.mark.parametrize(
    "operation",
    [
        lambda: merge_template_environment(None, {"BAD": True}),
        lambda: merge_template_environment(None, {"SESSION_SANDBOX_INTERNAL": "x"}),
        lambda: merge_template_environment(None, {"INVALID-NAME": "x"}),
        lambda: merge_template_environment([{"name": "DUP", "value": "a"}, {"name": "DUP", "value": "b"}]),
        lambda: merge_template_environment([{"name": "BAD", "value": "x", "valueFrom": {}}]),
        lambda: merge_template_environment([{"name": "BAD", "value": "x"}], {"GOOD": False}),
        lambda: command_environment({"SESSION_SANDBOX_RESERVED": "x"}),
    ],
)
def test_invalid_environment_is_rejected(operation) -> None:
    with pytest.raises(TemplateValidationError):
        operation()


def test_creation_fingerprint_is_stable_keyed_and_does_not_expose_values() -> None:
    env = {"USER_EMAIL": "user@example.com", "TOKEN": "private-token-value"}
    fingerprint = creation_environment_fingerprint(env, FINGERPRINT_KEY)

    assert fingerprint == creation_environment_fingerprint(dict(reversed(list(env.items()))), FINGERPRINT_KEY)
    assert len(fingerprint) == 64
    assert "user@example.com" not in fingerprint
    assert "private-token-value" not in fingerprint


def test_omitted_creation_environment_reuses_but_changed_environment_conflicts() -> None:
    fingerprint = creation_environment_fingerprint({"USER_EMAIL": "user@example.com"}, FINGERPRINT_KEY)

    assert ensure_same_creation_environment(fingerprint, None, FINGERPRINT_KEY) is True
    assert ensure_same_creation_environment(fingerprint, {"USER_EMAIL": "user@example.com"}, FINGERPRINT_KEY) is False
    with pytest.raises(SessionConfigurationConflictError, match="differs from the existing"):
        ensure_same_creation_environment(fingerprint, {"USER_EMAIL": "other@example.com"}, FINGERPRINT_KEY)


def test_config_masks_fingerprint_key_and_rejects_short_key() -> None:
    config = ManagerConfig(runtime={"image": "example.invalid/sandbox:v1"}, environment_fingerprint_key="secret-key-material-long-enough-for-testing")
    assert isinstance(config.environment_fingerprint_key, SecretStr)
    assert "secret-key-material" not in repr(config)

    with pytest.raises(ValidationError):
        ManagerConfig(runtime={"image": "example.invalid/sandbox:v1"}, environment_fingerprint_key="short")
