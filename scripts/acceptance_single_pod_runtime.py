"""Check the single-process runtime (GkeSessionSandboxRuntime) against a real Sandbox and tracker.

Its Sandboxes are only reachable from inside the cluster, so run it in the Sandbox Service's Pod
(same identity, same network):

    kubectl -n session-sandbox-system exec -i deploy/silk-agent-sandbox-service -- \\
        python - < scripts/acceptance_single_pod_runtime.py

It creates a Sandbox for a random conversation, then checks with the real tracker that
  - a command runs and returns its output,
  - cancel_command stops a command and reports "confirmed" once the tracker says it finished,
  - cancel_commands (a turn stop) escalates to SIGKILL for a command that ignores SIGTERM,
and removes the Sandbox, its workspace and its records afterwards.
"""

import asyncio
import os
import time
import uuid

from silk_agent.config import load_config
from silk_agent.core.clock import SystemClock
from silk_agent.sandbox.gke import GkeSessionSandboxRuntime, to_session_key_parts
from silk_agent.sandbox.service_runtime import _create_manager
from silk_agent.stores.firestore.client import create_firestore_client


async def started_command_id(runtime, key, wait_seconds=60.0):
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        ids = list(runtime._running.get(key, {}))
        if ids:
            return ids[0]
        await asyncio.sleep(0.2)
    raise TimeoutError("the command did not start")


async def main() -> None:
    config = load_config(os.environ["SILK_AGENT_CONFIG"])
    database = create_firestore_client(config.distributed)
    prefix = config.distributed.firestore_prefix
    manager, temp_dir = await _create_manager(config, database)
    key = f"acceptance-sp:{uuid.uuid4().hex[:8]}"
    tenant, session = to_session_key_parts(key)
    from gke_session_sandbox import SessionKey

    sandbox_key = SessionKey(tenant_id=tenant, session_id=session)
    runtime = GkeSessionSandboxRuntime(manager)
    clock = SystemClock()
    started = clock.now()
    try:
        result = await runtime.run_command(key, "printf hello", timeout_seconds=120)
        print(
            "1. run_command:",
            result.outcome,
            repr(result.stdout),
            f"({(clock.now() - started).seconds}s incl. cold start)",
        )
        assert (result.outcome, result.stdout) == ("completed", "hello")

        # 2. cancel_command: SIGTERM stops `sleep`; confirmed only once the tracker says finished.
        task = asyncio.create_task(runtime.run_command(key, "sleep 300", timeout_seconds=250))
        command_id = await started_command_id(runtime, key)
        t = time.time()
        outcome = await runtime.cancel_command(key, command_id)
        print(f"2. cancel_command(sleep): {outcome} in {time.time() - t:.1f}s")
        assert outcome == "confirmed"
        assert command_id not in runtime._running.get(key, {})
        print("   run_command ended:", (await asyncio.wait_for(task, 30)).outcome)

        # 3. cancel_commands (a turn stop) for a command that ignores SIGTERM: the tracker sends
        # SIGKILL after 5 s, the runtime waits for the finished record.
        task = asyncio.create_task(
            runtime.run_command(key, "trap '' TERM; sleep 300", timeout_seconds=250)
        )
        command_id = await started_command_id(runtime, key)
        t = time.time()
        await runtime.cancel_commands(key)
        elapsed = time.time() - t
        left = command_id in runtime._running.get(key, {})
        print(f"3. cancel_commands(ignores TERM): {elapsed:.1f}s, handle left: {left}")
        assert not left, "the process should have been SIGKILLed and confirmed finished"
        assert 3 <= elapsed <= 15, elapsed  # SIGTERM was ignored: it took the SIGKILL escalation
        print("   run_command ended:", (await asyncio.wait_for(task, 30)).outcome)
        print("OK")
    finally:
        saved = await manager.repository.get(sandbox_key)
        try:
            if saved is not None and saved.record.sandbox_ref is not None:
                original = saved.record
                await manager.terminate(sandbox_key, reason="acceptance-cleanup")
                for pvc in original.workspace_pvc_refs:
                    await manager._pvc_manager.delete_owned(
                        pvc, sandbox_uid=original.sandbox_ref.uid
                    )
        finally:
            sandbox_doc = database.collection(f"{prefix}_sandboxes").document(
                sandbox_key.stable_hash()
            )
            async for operation in sandbox_doc.collection("file_operations").stream():
                await operation.reference.delete()
            await sandbox_doc.delete()
            await (
                database.collection(f"{prefix}_resource_leases")
                .document(f"sandbox-{sandbox_key.stable_hash()}")
                .delete()
            )
            await manager.close()
            temp_dir.cleanup()
            database.close()


asyncio.run(main())
