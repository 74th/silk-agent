"""Firestore client construction for ADC production and explicitly selected emulators."""

from __future__ import annotations

import os
from threading import RLock

from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore

from silk_agent.config import DistributedExecutionConfig

_EMULATOR_ENV = "FIRESTORE_EMULATOR_HOST"
_EMULATOR_ENV_LOCK = RLock()


def create_firestore_client(
    config: DistributedExecutionConfig, *, emulator_host: str | None = None
) -> firestore.AsyncClient:
    """Create a project/database client, using ADC unless an emulator was explicitly chosen.

    The emulator gets anonymous credentials and an explicit endpoint. Merely inheriting
    FIRESTORE_EMULATOR_HOST is rejected so a test cannot silently change its target.
    """
    return _create(firestore.AsyncClient, config, emulator_host)


def create_watch_firestore_client(
    config: DistributedExecutionConfig, *, emulator_host: str | None = None
) -> firestore.Client:
    """Synchronous client; the async client has no snapshot listener support."""
    return _create(firestore.Client, config, emulator_host)


def _create(client_class, config: DistributedExecutionConfig, emulator_host: str | None):
    environment_host = os.environ.get(_EMULATOR_ENV)
    if environment_host and environment_host != emulator_host:
        raise ValueError(f"pass emulator_host explicitly to use {_EMULATOR_ENV}")

    if emulator_host is None:
        return client_class(
            project=config.firestore_project,
            database=config.firestore_database,
        )

    host = emulator_host.strip()
    if not host or "://" in host:
        raise ValueError("emulator_host must be a host:port value")
    # The supported Python client API selects an insecure emulator channel from this
    # environment variable and captures it during construction. Limit the environment
    # override to construction, serialized against other calls to this factory.
    with _EMULATOR_ENV_LOCK:
        previous = os.environ.get(_EMULATOR_ENV)
        os.environ[_EMULATOR_ENV] = host
        try:
            return client_class(
                project=config.firestore_project,
                database=config.firestore_database,
                credentials=AnonymousCredentials(),
            )
        finally:
            if previous is None:
                os.environ.pop(_EMULATOR_ENV, None)
            else:
                os.environ[_EMULATOR_ENV] = previous
