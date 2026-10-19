"""GKE check of the owner e-mail the Sandbox Service passes to Sandboxes (no Slack).

Two conversations, each with a recorded owner e-mail:
- a new one: its Sandbox gets AGENT_USER_EMAIL, visible to the agent's commands;
- one whose Sandbox was created before the e-mail was passed (created here without env, like the
  old service did): it keeps working (no creation-environment conflict) and has no e-mail.

Like test_distributed_service_live, it creates its own conversations and Worker record under the
`silk-agent` prefix, so run it with the dev Control scaled to zero and scale it back afterwards.
"""

import asyncio
import base64
import os
import socket
import subprocess
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
from silk_agent.sandbox.remote import RemoteSandboxSessions
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.ids import conversation_document_id
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory
from tests.gke.cleanup import delete_records_of_conversation

pytestmark = pytest.mark.gke

CONTEXT = "gke_nnyn-dev_us-central1-a_silk-agent-gke"
EMAIL = "owner-check@example.com"


def _write_manager_config(path: Path) -> Path:
    config = yaml.safe_load(
        Path("k8s/distributed/overlays/dev/config/sandbox-service-config.yaml").read_text()
    )
    config["template_file"] = str(Path("templates/sandbox.yaml").resolve())
    path.write_text(yaml.safe_dump(config))
    return path


async def test_owner_email_reaches_new_sandboxes_and_legacy_ones_keep_working(tmp_path):
    if os.environ.get("SILK_AGENT_GKE_CONTEXT") != CONTEXT:
        pytest.skip("select SILK_AGENT_GKE_CONTEXT explicitly")
    context = (
        await asyncio.to_thread(
            subprocess.check_output, ["kubectl", "config", "current-context"], text=True
        )
    ).strip()
    assert context == CONTEXT
    run_id = uuid.uuid4().hex[:12]
    prefix = "silk-agent"
    clock = SystemClock()
    database = create_firestore_client(
        DistributedExecutionConfig(
            firestore_project="nnyn-dev", firestore_database="silk-agent", firestore_prefix=prefix
        )
    )
    directory = FirestoreWorkerDirectory(database, collection_prefix=prefix)
    owners = FirestoreOwnershipStore(database, collection_prefix=prefix)
    inbox = FirestoreInboxStore(database, collection_prefix=prefix)
    conversations = FirestoreConversationStore(database, collection_prefix=prefix)
    repository = FirestoreSessionRepository(database, collection_prefix=prefix)
    worker_id, incarnation = f"owner-email-{run_id}", f"owner-email-pod:{run_id}"
    keys = {"new": f"owner-email-new:{run_id}", "legacy": f"owner-email-legacy:{run_id}"}
    tokens: dict[str, object] = {}
    manager = None
    forward = None
    try:
        await directory.register(WorkerRecord(worker_id, incarnation, 4, 0, 0, False, clock.now()))
        for name, key in keys.items():
            await inbox.accept_input(
                conversation_key=key,
                input_id=f"input-{name}-{run_id}",
                conversation_generation=1,
                kind="message",
                payload={"text": "owner email check"},
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
            assert token is not None and await owners.activate(token, now=clock.now())
            assert await conversations.set_initial_owner_once(
                owner=token, owner_user="U-OWNER", owner_email=EMAIL
            )
            tokens[name] = token

        # The legacy Sandbox: created straight through the library, without env.
        config_path = await asyncio.to_thread(_write_manager_config, tmp_path / "manager.yaml")
        manager = await SandboxManager.from_config(config_path, repository=repository)
        tenant, session = to_session_key_parts(keys["legacy"])
        legacy = await manager.ensure(SessionKey(tenant_id=tenant, session_id=session))
        assert legacy.record.creation_env_fingerprint is None

        bearer = await asyncio.to_thread(
            subprocess.check_output,
            [
                "kubectl", "-n", "session-sandbox-system", "create", "token",
                "silk-agent-worker", "--audience=silk-agent-sandbox-service", "--duration=20m",
            ],
            text=True,
        )  # fmt: skip
        token_file = tmp_path / "token"
        token_file.write_text(bearer)
        token_file.chmod(0o600)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        forward = await asyncio.to_thread(
            subprocess.Popen,
            [
                "kubectl", "-n", "session-sandbox-system", "port-forward",
                "service/silk-agent-sandbox-service", f"{port}:8080",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )  # fmt: skip
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

        seen = {}
        for name, key in keys.items():
            ready = await remote.ensure(key)  # the legacy one must not conflict
            assert ready["ready"] is True, (name, ready)
            token = tokens[name]
            identity = dict(
                owner_epoch=token.epoch, conversation_generation=1, turn_id=run_id, origin_seq=1
            )
            command_id = f"owner-email-{name}-{run_id}"
            await remote.start_command(
                key,
                "printf '%s' \"$AGENT_USER_EMAIL\"",
                cwd="/workspace",
                timeout_seconds=60,
                request_id=command_id,
                **identity,
            )
            for _ in range(120):
                state = await remote.command_status(key, command_id, **identity)
                if state["state"] == "finished":
                    break
                await asyncio.sleep(0.25)
            assert state["state"] == "finished" and state["exit_code"] == 0
            page = await remote.command_output(
                key, command_id, stream="stdout", cursor=0, **identity
            )
            seen[name] = base64.b64decode(page["data_base64"]).decode()
        assert seen["new"] == EMAIL
        assert seen["legacy"] == ""  # created before the e-mail was passed: unchanged
    finally:
        try:
            if manager is None:
                config_path = await asyncio.to_thread(
                    _write_manager_config, tmp_path / "manager-cleanup.yaml"
                )
                manager = await SandboxManager.from_config(config_path, repository=repository)
            for key in keys.values():
                tenant, session = to_session_key_parts(key)
                sandbox_key = SessionKey(tenant_id=tenant, session_id=session)
                saved = await repository.get(sandbox_key)
                if saved is not None and saved.record.sandbox_ref is not None:
                    original = saved.record
                    await manager.terminate(sandbox_key, reason="owner-email-cleanup")
                    for pvc in original.workspace_pvc_refs:
                        await manager._pvc_manager.delete_owned(
                            pvc, sandbox_uid=original.sandbox_ref.uid
                        )
        finally:
            if manager is not None:
                await manager.close()
            if forward is not None:
                forward.terminate()
                await asyncio.to_thread(forward.wait, timeout=10)
            for name, key in keys.items():
                conversation_ref = database.collection(f"{prefix}_conversations").document(
                    conversation_document_id(key)
                )
                async for doc in conversation_ref.collection("inputs").stream():
                    await doc.reference.delete()
                await delete_records_of_conversation(database, prefix, key)
                token = tokens.get(name)
                if token is not None:
                    await owners.release(token, now=clock.now())
                await conversation_ref.delete()
                await (
                    database.collection(f"{prefix}_ownership")
                    .document(conversation_document_id(key))
                    .delete()
                )
                tenant, session = to_session_key_parts(key)
                sandbox_hash = SessionKey(tenant_id=tenant, session_id=session).stable_hash()
                sandbox_doc = database.collection(f"{prefix}_sandboxes").document(sandbox_hash)
                async for operation in sandbox_doc.collection("file_operations").stream():
                    await operation.reference.delete()
                await sandbox_doc.delete()
                await (
                    database.collection(f"{prefix}_resource_leases")
                    .document(f"sandbox-{sandbox_hash}")
                    .delete()
                )
            worker_ref = directory._worker_ref(worker_id)
            async for incarnation_ref in worker_ref.collection("incarnations").stream():
                async for doc in incarnation_ref.reference.collection("reservations").stream():
                    await doc.reference.delete()
                await incarnation_ref.reference.delete()
            await worker_ref.delete()
            database.close()
