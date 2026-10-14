import os
from unittest.mock import Mock

import pytest
from google.auth.credentials import AnonymousCredentials

from silk_agent.config import DistributedExecutionConfig
from silk_agent.stores.firestore import client as firestore_client


def test_production_firestore_client_uses_adc_and_configured_database(monkeypatch):
    build = Mock(return_value=object())
    monkeypatch.setattr(firestore_client.firestore, "AsyncClient", build)

    firestore_client.create_firestore_client(
        DistributedExecutionConfig(firestore_project="project-1", firestore_database="agent-db")
    )

    build.assert_called_once_with(project="project-1", database="agent-db")


def test_emulator_client_uses_explicit_endpoint_and_anonymous_credentials(monkeypatch):
    build = Mock(side_effect=lambda **kwargs: (kwargs, os.environ.get("FIRESTORE_EMULATOR_HOST")))
    monkeypatch.setattr(firestore_client.firestore, "AsyncClient", build)
    monkeypatch.delenv("FIRESTORE_EMULATOR_HOST", raising=False)

    client = firestore_client.create_firestore_client(
        DistributedExecutionConfig(firestore_project="test-project"),
        emulator_host="127.0.0.1:9090",
    )

    kwargs, selected_host = client
    assert kwargs["project"] == "test-project"
    assert kwargs["database"] == "(default)"
    assert isinstance(kwargs["credentials"], AnonymousCredentials)
    assert selected_host == "127.0.0.1:9090"
    assert "FIRESTORE_EMULATOR_HOST" not in os.environ


def test_inherited_emulator_environment_requires_explicit_selection(monkeypatch):
    build = Mock(return_value=object())
    monkeypatch.setattr(firestore_client.firestore, "AsyncClient", build)
    monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", "127.0.0.1:8080")

    with pytest.raises(ValueError, match="pass emulator_host explicitly"):
        firestore_client.create_firestore_client(
            DistributedExecutionConfig(firestore_project="production-project")
        )

    build.assert_not_called()


@pytest.mark.parametrize("host", ["", "http://127.0.0.1:8080"])
def test_emulator_host_must_be_explicit_host_and_port(monkeypatch, host):
    monkeypatch.delenv("FIRESTORE_EMULATOR_HOST", raising=False)

    with pytest.raises(ValueError, match="host:port"):
        firestore_client.create_firestore_client(
            DistributedExecutionConfig(firestore_project="test-project"), emulator_host=host
        )
