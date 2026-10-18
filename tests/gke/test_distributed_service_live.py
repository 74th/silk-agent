"""Explicit GKE acceptance without Slack: real Firestore, API and gVisor commands.

The test creates its own conversation and Worker record under the `silk-agent` prefix, so a
running Control would allocate that conversation to a real Worker first. Run it with the dev
Control scaled to zero (`kubectl -n session-sandbox-system scale deploy/silk-agent-control
--replicas=0`) and scale it back to 1 afterwards.
"""

import asyncio
import base64
import json
import os
import socket
import subprocess
import time
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from aiohttp import ClientSession

from gke_session_sandbox import SandboxManager, SessionKey
from gke_session_sandbox.firestore_repository import FirestoreSessionRepository
from silk_agent.config import DistributedExecutionConfig
from silk_agent.core.clock import SystemClock
from silk_agent.core.distributed_models import WorkerRecord
from silk_agent.sandbox.gke import to_session_key_parts
from silk_agent.sandbox.monitor import CommandMonitor
from silk_agent.sandbox.remote import (
    RemoteSandboxRuntime,
    RemoteSandboxSessions,
    SandboxServiceError,
)
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore
from silk_agent.stores.firestore.ids import conversation_document_id, document_id
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

pytestmark = pytest.mark.gke


async def test_real_commands_files_dedup_cancel_and_owner_fencing_without_slack(tmp_path):
    expected_context = os.environ.get("SILK_AGENT_GKE_CONTEXT")
    if not expected_context:
        pytest.skip("select SILK_AGENT_GKE_CONTEXT explicitly")
    context = (
        await asyncio.to_thread(
            subprocess.check_output, ["kubectl", "config", "current-context"], text=True
        )
    ).strip()
    assert context == expected_context == "gke_nnyn-dev_us-central1-a_silk-agent-gke"
    run_id = uuid.uuid4().hex
    key = f"distributed-validation:{run_id}"
    prefix = "silk-agent"
    clock = SystemClock()
    started = time.monotonic()
    result = {"context": context, "started_at": clock.now().isoformat(), "conversation_key": key}
    database = create_firestore_client(
        DistributedExecutionConfig(
            firestore_project="nnyn-dev", firestore_database="silk-agent", firestore_prefix=prefix
        )
    )
    directory = FirestoreWorkerDirectory(database, collection_prefix=prefix)
    owners = FirestoreOwnershipStore(database, collection_prefix=prefix)
    inbox = FirestoreInboxStore(database, collection_prefix=prefix)
    ledger = FirestoreCommandMonitorStore(database, collection_prefix=prefix)
    repository = FirestoreSessionRepository(database, collection_prefix=prefix)
    worker_id = f"validation-{run_id}"
    incarnation = f"validation-pod:{run_id}"
    command_id = f"validation-command-{run_id}"
    cancel_id = f"validation-cancel-{run_id}"
    write_id = f"validation-file-{run_id}"
    tenant, session = to_session_key_parts(key)
    sandbox_key = SessionKey(tenant_id=tenant, session_id=session)
    token = None
    manager = None
    forward = None
    resources_created = False
    try:
        await directory.register(WorkerRecord(worker_id, incarnation, 4, 0, 0, False, clock.now()))
        await inbox.accept_input(
            conversation_key=key,
            input_id=f"validation-input-{run_id}",
            conversation_generation=1,
            kind="message",
            # No "user": a sender would be looked up by the running Gateway, which gives up on this
            # made-up user and moves the input to "blocked", ending the ownership test early.
            payload={"text": "validation"},
            created_at=clock.now(),
        )
        token = await owners.acquire(
            conversation_key=key,
            worker_id=worker_id,
            worker_incarnation=incarnation,
            conversation_generation=1,
            now=clock.now(),
            lease_expires_at=clock.now() + timedelta(minutes=20),
        )
        assert token is not None
        assert await owners.activate(token, now=clock.now())
        bearer = await asyncio.to_thread(
            subprocess.check_output,
            [
                "kubectl",
                "-n",
                "session-sandbox-system",
                "create",
                "token",
                "silk-agent-worker",
                "--audience=silk-agent-sandbox-service",
                "--duration=20m",
            ],
            text=True,
        )
        token_file = tmp_path / "token"
        token_file.write_text(bearer)
        token_file.chmod(0o600)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        forward = await asyncio.to_thread(
            subprocess.Popen,
            [
                "kubectl",
                "-n",
                "session-sandbox-system",
                "port-forward",
                "service/silk-agent-sandbox-service",
                f"{port}:8080",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        url = f"http://127.0.0.1:{port}"
        async with ClientSession() as http:
            for _ in range(100):
                try:
                    async with http.get(f"{url}/readyz") as ready:
                        if ready.status == 200:
                            break
                except OSError:
                    pass
                await asyncio.sleep(0.1)
            else:
                pytest.fail("Sandbox Service port-forward did not become ready")
        remote = RemoteSandboxSessions(url, token_file=token_file, timeout_seconds=180)
        resources_created = True
        ready = await remote.ensure(key)
        assert ready["ready"] is True
        result["sandbox"] = ready
        identity = dict(
            owner_epoch=token.epoch, conversation_generation=1, turn_id=run_id, origin_seq=1
        )
        await remote.write_file(
            key, "validation.txt", b"file-roundtrip", request_id=write_id, **identity
        )
        file_result = await remote.read_file(key, "validation.txt", **identity)
        assert file_result == b"file-roundtrip"
        args = dict(cwd="/workspace", timeout_seconds=120, request_id=command_id, **identity)
        command = "printf 'once\\n' >> /workspace/once.txt; sleep 12; cat /workspace/once.txt"
        first = await remote.start_command(key, command, **args)
        duplicate = await remote.start_command(key, command, **args)
        assert first["command_id"] == duplicate["command_id"] == command_id
        await ledger.detach(command_id, now=clock.now())
        # Manipulate logical ownership directly; no SDK/Worker Pod is started here.
        # The Sandbox process must outlive release and reacquisition of its assignment.
        assert await owners.release(token, now=clock.now())
        replacement = await owners.acquire(
            conversation_key=key,
            worker_id=worker_id,
            worker_incarnation=incarnation,
            conversation_generation=1,
            now=clock.now(),
            lease_expires_at=clock.now() + timedelta(minutes=20),
        )
        assert replacement is not None and replacement.epoch > token.epoch
        assert await owners.activate(replacement, now=clock.now())
        stale = dict(identity)
        identity["owner_epoch"] = replacement.epoch
        token = replacement
        with pytest.raises(SandboxServiceError) as rejected:
            await remote.start_command(
                key, "echo stale", **{**args, "request_id": f"stale-{run_id}"}
            )
        assert rejected.value.status == 409
        continuing = await remote.command_status(key, command_id, **identity)
        assert continuing["state"] == "running"
        result["state_after_handoff"] = continuing["state"]
        for _ in range(120):
            state = await remote.command_status(key, command_id, **identity)
            if state["state"] == "finished":
                break
            await asyncio.sleep(0.25)
        assert state["state"] == "finished" and state["exit_code"] == 0
        page = await remote.command_output(key, command_id, stream="stdout", cursor=0, **identity)
        assert base64.b64decode(page["data_base64"]) == b"once\n"

        # Scope discovery to this run so live validation never monitors other users.
        class OnlyThisCommand:
            async def list_due(self, **_kwargs):
                record = await ledger.get_command(command_id)
                return (record,) if record is not None else ()

            def __getattr__(self, name):
                return getattr(ledger, name)

        async def current_identity(_key):
            return identity

        monitor_runtime = RemoteSandboxRuntime(remote, current_identity)
        monitors = [
            CommandMonitor(OnlyThisCommand(), monitor_runtime, clock, monitor_id=f"{run_id}-{i}")
            for i in range(2)
        ]
        # The Sandbox Service runs its own monitor, which may well hold the lease: the command
        # is completed by whichever monitor gets it, never by two. Poll until it is finished.
        claims = [0]
        for _ in range(40):
            claims.extend(await asyncio.gather(*(monitor.poll_once() for monitor in monitors)))
            finished = await ledger.get_command(command_id)
            if finished is not None and finished["state"] == "finished":
                break
            await asyncio.sleep(2)
        assert sum(claims) <= 1
        assert finished is not None and finished["state"] == "finished"
        unfinished = await inbox.list_unfinished(key)
        assert sum(item.payload.get("command_id") == command_id for item in unfinished) == 1
        result["monitor_claims"] = sum(claims)
        result["completion_inputs"] = 1
        result["command_id"] = command_id
        result["command_state"] = state
        result["handoff_epoch"] = token.epoch
        result["old_epoch"] = stale["owner_epoch"]
        await remote.start_command(
            key,
            "sleep 120",
            cwd="/workspace",
            timeout_seconds=180,
            request_id=cancel_id,
            **identity,
        )
        cancel = await remote.cancel_command(key, cancel_id, **identity)
        assert cancel["state"] == "cancel_requested"
        for _ in range(120):
            cancelled = await remote.command_status(key, cancel_id, **identity)
            if cancelled["state"] == "cancelled":
                break
            await asyncio.sleep(0.25)
        assert cancelled["state"] == "cancelled"
        result["cancel_command_id"] = cancel_id
        result["cancel_state"] = cancelled
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        result["checks"] = [
            "ready",
            "file_roundtrip",
            "same_id_one_start",
            "background_continues",
            "ownership_handoff",
            "old_epoch_rejected",
            "output",
            "explicit_cancel",
            "two_monitors_one_completion",
        ]
    finally:
        # Only this random conversation's resources and records are removed.
        try:
            if resources_created:
                manager_config = yaml.safe_load(
                    await asyncio.to_thread(
                        Path(
                            "k8s/distributed/overlays/dev/config/sandbox-service-config.yaml"
                        ).read_text
                    )
                )
                manager_config["template_file"] = str(
                    await asyncio.to_thread(Path("templates/sandbox.yaml").resolve)
                )
                config_path = tmp_path / "manager.yaml"
                config_path.write_text(yaml.safe_dump(manager_config))
                manager = await SandboxManager.from_config(config_path, repository=repository)
                saved = await repository.get(sandbox_key)
                if saved is not None and saved.record.sandbox_ref is not None:
                    original = saved.record
                    await manager.terminate(sandbox_key, reason="validation-cleanup")
                    for pvc in original.workspace_pvc_refs:
                        await manager._pvc_manager.delete_owned(
                            pvc, sandbox_uid=original.sandbox_ref.uid
                        )
                result["cleanup"] = "Sandbox terminated; owned workspace PVCs removed"
        finally:
            if manager is not None:
                await manager.close()
            if forward is not None:
                forward.terminate()
                await asyncio.to_thread(forward.wait, timeout=10)
            conversation_ref = database.collection(f"{prefix}_conversations").document(
                conversation_document_id(key)
            )
            for name in ("inputs", "controls"):
                async for doc in conversation_ref.collection(name).stream():
                    await doc.reference.delete()
            for name in ("commands", "sandbox_operations"):
                for identifier in (command_id, cancel_id, write_id):
                    kind = "command" if name == "commands" else "operation"
                    await (
                        database.collection(f"{prefix}_{name}")
                        .document(document_id(identifier, prefix=kind))
                        .delete()
                    )
            if token is not None:
                await owners.release(token, now=clock.now())
            worker_ref = directory._worker_ref(worker_id)
            async for incarnation_ref in worker_ref.collection("incarnations").stream():
                async for doc in incarnation_ref.reference.collection("reservations").stream():
                    await doc.reference.delete()
                await incarnation_ref.reference.delete()
            await worker_ref.delete()
            await conversation_ref.delete()
            await (
                database.collection(f"{prefix}_ownership")
                .document(conversation_document_id(key))
                .delete()
            )
            sandbox_ref = database.collection(f"{prefix}_sandboxes").document(
                sandbox_key.stable_hash()
            )
            async for operation in sandbox_ref.collection("file_operations").stream():
                await operation.reference.delete()
            await sandbox_ref.delete()
            await (
                database.collection(f"{prefix}_resource_leases")
                .document(f"sandbox-{sandbox_key.stable_hash()}")
                .delete()
            )
            result["finished_at"] = clock.now().isoformat()
            report_path = os.environ.get("SILK_AGENT_GKE_REPORT")
            if report_path:
                await asyncio.to_thread(
                    Path(report_path).write_text, json.dumps(result, indent=2, default=str) + "\n"
                )
            database.close()
