from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from gke_session_sandbox import (
    CommandRecord,
    CommandState,
    ManagerConfig,
    ProcessIdentity,
    ResourceRef,
    SessionKey,
    SessionRecord,
    SessionState,
    TemplateValidationError,
)


def runtime_settings(**overrides: object) -> dict[str, object]:
    return {"image": "example.invalid/sandbox@sha256:abc", **overrides}


def test_session_key_has_a_stable_non_reversible_name_hash() -> None:
    key = SessionKey(tenant_id="tenant-a", session_id="channel/thread")

    assert key.stable_hash() == SessionKey(tenant_id="tenant-a", session_id="channel/thread").stable_hash()
    assert len(key.stable_hash()) == 64
    assert "channel/thread" not in key.stable_hash()


@pytest.mark.parametrize(
    "payload",
    [
        {"tenant_id": "", "session_id": "s"},
        {"tenant_id": "tenant", "session_id": "  "},
        {"tenant_id": 7, "session_id": "s"},
        {"tenant_id": "tenant", "session_id": "s", "unexpected": True},
    ],
)
def test_session_key_rejects_invalid_types_and_values(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        SessionKey.model_validate(payload)


def test_resource_references_require_identity_fields() -> None:
    ref = ResourceRef(
        api_version="agents.x-k8s.io/v1beta1",
        kind="Sandbox",
        namespace="session-sandboxes",
        name="sb-123",
        uid="uid-123",
    )
    assert ref.uid == "uid-123"

    with pytest.raises(ValidationError):
        ResourceRef(api_version="v1", kind="Pod", name="pod", uid="")


def test_process_identity_rejects_invalid_numeric_fields() -> None:
    with pytest.raises(ValidationError):
        ProcessIdentity(boot_id="boot", pid=0, process_group_id=1, start_ticks=1)
    with pytest.raises(ValidationError):
        ProcessIdentity(boot_id="boot", pid=1, process_group_id=1, start_ticks=-1)


def test_finished_command_requires_an_aware_finish_time() -> None:
    now = datetime.now(timezone.utc)
    base = {
        "command_id": "cmd-1",
        "generation": 1,
        "runtime_instance_id": "runtime-1",
        "state": CommandState.FINISHED,
        "accepted_at": now,
        "started_at": now,
        "finished_at": now,
    }
    assert CommandRecord.model_validate(base).state is CommandState.FINISHED

    with pytest.raises(ValidationError):
        CommandRecord.model_validate({**base, "generation": 0})
    with pytest.raises(ValidationError):
        CommandRecord.model_validate({**base, "finished_at": datetime.now()})


def test_session_record_rejects_invalid_generation() -> None:
    with pytest.raises(ValidationError):
        SessionRecord(key_hash="a" * 64, generation=0, state=SessionState.CREATING)


@pytest.mark.parametrize("quantity", ["", "1widgets", "NaN", "0", "-1Gi", "1.2.3Gi"])
def test_config_rejects_invalid_resource_quantities(quantity: str) -> None:
    with pytest.raises(ValidationError):
        ManagerConfig(
            runtime=runtime_settings(memory_request=quantity),
        )


def test_config_requires_strict_integer_ports_and_consistent_resources() -> None:
    with pytest.raises(ValidationError):
        ManagerConfig(runtime=runtime_settings(rest_port="8080"))
    with pytest.raises(ValidationError):
        ManagerConfig(runtime=runtime_settings(cpu_request="2", cpu_limit="1"))
    with pytest.raises(ValidationError):
        ManagerConfig(runtime=runtime_settings(rest_port=9090))


def test_config_rejects_library_owned_manifest_fields() -> None:
    with pytest.raises(TemplateValidationError, match="spec.operatingMode"):
        ManagerConfig(
            runtime=runtime_settings(),
            template_overrides={"spec": {"operatingMode": "Running"}},
        )


def test_config_accepts_normal_overrides_and_has_lifecycle_defaults() -> None:
    config = ManagerConfig(
        runtime=runtime_settings(),
        template_overrides={"podTemplate": {"spec": {"nodeSelector": {"pool": "sandbox"}}}},
    )

    assert config.lifecycle.idle_timeout_seconds == 300
    assert config.lifecycle.retention_seconds == 604800
