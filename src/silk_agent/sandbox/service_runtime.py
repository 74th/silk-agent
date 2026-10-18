"""Production composition for the authenticated Sandbox Service role."""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

import yaml

from silk_agent.config import Config, ConfigError
from silk_agent.sandbox.service import create_app

log = logging.getLogger(__name__)


async def _create_manager(config: Config, firestore_client: Any):
    """Build the Firestore-backed SandboxManager; returns it with its temp config directory."""
    from gke_session_sandbox import SandboxManager
    from gke_session_sandbox.config import ManagerConfig
    from gke_session_sandbox.firestore_repository import FirestoreSessionRepository
    from silk_agent.app import FINGERPRINT_KEY_ENV, with_fingerprint_key

    prefix = config.distributed.firestore_prefix
    manager_config = config.resolve(config.sandbox.config_file)
    temp_config_dir = tempfile.TemporaryDirectory(prefix="silk-agent-sandbox-service-")
    fingerprint_key = os.environ.get(FINGERPRINT_KEY_ENV, "")
    if fingerprint_key:
        manager_config = await asyncio.to_thread(
            with_fingerprint_key,
            manager_config,
            fingerprint_key,
            Path(temp_config_dir.name) / "sandbox-config.yaml",
        )
    try:
        manager_settings = ManagerConfig.model_validate(
            yaml.safe_load(manager_config.read_text(encoding="utf-8")) or {}
        )
        repository = FirestoreSessionRepository(
            firestore_client,
            collection_prefix=prefix,
            lease_duration_seconds=manager_settings.session_lease_duration_seconds,
            max_pending_events=manager_settings.max_pending_events,
        )
        manager = await SandboxManager.from_config(str(manager_config), repository=repository)
    except Exception:
        temp_config_dir.cleanup()
        raise
    return manager, temp_config_dir


async def run_sandbox_network_admin(
    config: Config,
    action: str,
    *,
    conversation_key: str | None,
    names: list[str] | None = None,
    allow: list[str] | None = None,
    deny: list[str] | None = None,
    ephemeral: bool = False,
) -> int:
    """Re-apply network policies to, or verify the network of, existing Sandboxes."""
    if config.execution_mode != "distributed":
        raise ConfigError("sandbox network administration requires execution_mode: distributed")
    if not config.sandbox.config_file:
        raise ConfigError("sandbox.config_file is required")
    from gke_session_sandbox.cli import verify_network
    from silk_agent.sandbox.service import _session_key
    from silk_agent.stores.firestore.client import create_firestore_client

    firestore_client = create_firestore_client(config.distributed)
    try:
        manager, temp_config_dir = await _create_manager(config, firestore_client)
    except Exception:
        firestore_client.close()
        raise
    try:
        key = _session_key(conversation_key) if conversation_key else None
        if action == "verify":
            assert key is not None
            code, output = await verify_network(
                manager,
                key,
                names=names,
                allow=allow or (),
                deny=deny or (),
                ephemeral=ephemeral,
            )
            print(output, end="" if output.endswith("\n") else "\n")
            return 0 if code == 0 else 1
        results = await manager.reapply_network_policies(key)
        for result in results:
            print(f"{result.key_hash[:10]} {result.status} {result.detail}".rstrip())
        return 1 if any(result.status == "failed" for result in results) else 0
    finally:
        try:
            await manager.close()
        finally:
            firestore_client.close()
            temp_config_dir.cleanup()


async def run_sandbox_service(config: Config) -> None:
    """Start the internal API with Firestore-backed sandbox and operation state."""
    if config.execution_mode != "distributed":
        raise ConfigError("sandbox-service requires execution_mode: distributed")
    config.validate_required()
    config.validate_distributed_sandbox_config()
    if not config.distributed.sandbox_service_allowed_ksas:
        raise ConfigError("distributed.sandbox_service_allowed_ksas must list Worker KSAs")
    if not config.sandbox.config_file:
        raise ConfigError("sandbox.config_file is required")

    from silk_agent.core.clock import SystemClock
    from silk_agent.stores.firestore.client import create_firestore_client
    from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
    from silk_agent.stores.firestore.service_operations import FirestoreServiceOperationStore

    clock = SystemClock()
    firestore_client = create_firestore_client(config.distributed)
    prefix = config.distributed.firestore_prefix
    ownership = FirestoreOwnershipStore(firestore_client, collection_prefix=prefix)
    operations = FirestoreServiceOperationStore(firestore_client, collection_prefix=prefix)
    try:
        manager, temp_config_dir = await _create_manager(config, firestore_client)
    except Exception:
        firestore_client.close()
        raise

    allowed_ksas = tuple(config.distributed.sandbox_service_allowed_ksas)

    async def authorize(caller_ksa: str, conversation_key: str) -> bool:
        if caller_ksa not in allowed_ksas:
            return False
        owner = await ownership.get(conversation_key)
        return owner is not None and owner.expires_at > clock.now()

    async def owner_for(conversation_key: str):
        owner = await ownership.get(conversation_key)
        if owner is None or owner.expires_at <= clock.now():
            return None
        return owner

    app = create_app(
        audience=config.distributed.sandbox_service_audience,
        allowed_ksas=allowed_ksas,
        manager=manager,
        conversation_authorizer=authorize,
        owner_provider=owner_for,
        operation_store=operations,
        clock=clock.now,
    )

    from silk_agent.sandbox.monitor import CommandMonitor
    from silk_agent.sandbox.output_archive import CommandOutputArchiver, GCSCommandOutputStore
    from silk_agent.sandbox.service_monitor import ServiceCommandRuntime, run_monitor_loop
    from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore

    monitor = CommandMonitor(
        FirestoreCommandMonitorStore(firestore_client, collection_prefix=prefix),
        ServiceCommandRuntime(manager),
        clock,
        monitor_id=f"{os.environ.get('HOSTNAME', 'sandbox-service')}:{uuid.uuid4().hex}",
        output_archiver=CommandOutputArchiver(
            GCSCommandOutputStore(config.distributed.command_output_bucket),
            clock=clock,
            retention_days=config.distributed.command_output_retention_days,
        ),
    )
    monitor_stop = asyncio.Event()
    monitor_task = asyncio.create_task(run_monitor_loop(monitor, monitor_stop), name="monitor")

    # Idle suspend (lifecycle.idle_timeout_seconds) and retention delete
    # (lifecycle.retention_seconds -> Sandbox spec.shutdownTime) are done by the reconciler.
    # Without it Sandboxes and their PVCs are never reclaimed. Several replicas are safe: the
    # reconciler takes the per-session resource lease before it acts.
    reconciler_task = None
    if config.sandbox.run_reconciler:
        from gke_session_sandbox import LifecycleReconciler

        reconciler_task = asyncio.create_task(
            LifecycleReconciler(manager).run_forever(), name="reconciler"
        )

    async def cleanup(_app: Any) -> None:
        if reconciler_task is not None:
            reconciler_task.cancel()
            await asyncio.gather(reconciler_task, return_exceptions=True)
        monitor_stop.set()
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)
        try:
            await manager.close()
        finally:
            firestore_client.close()

    app.on_cleanup.append(cleanup)
    from aiohttp import web

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=config.distributed.sandbox_service_port)
    try:
        await site.start()
        log.info(
            "sandbox service listening",
            extra={"port": config.distributed.sandbox_service_port},
        )
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
        temp_config_dir.cleanup()
