"""Check that a command's leftover processes are stopped when its shell exits (real tracker).

Run it in the Sandbox Service's Pod (the Sandboxes are only reachable from inside the cluster):

    kubectl -n session-sandbox-system exec -i deploy/silk-agent-sandbox-service -- \\
        python - < scripts/acceptance_leftover_group.py

It creates a Sandbox for a random conversation and runs commands whose shell exits at once while
a child stays in the same process group:
  - `sleep 300 & echo started`: the tracker ends the child (SIGTERM) before the command counts
    as finished, so no `sleep` is left;
  - a child that ignores SIGTERM is SIGKILLed.
The Sandbox, its workspace and its records are removed afterwards.
"""

import asyncio
import os
import time
import uuid

from silk_agent.config import load_config
from silk_agent.sandbox.gke import GkeSessionSandboxRuntime, to_session_key_parts
from silk_agent.sandbox.service_runtime import _create_manager
from silk_agent.stores.firestore.client import create_firestore_client


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
    try:
        warm = await runtime.run_command(key, "printf hello", timeout_seconds=120)
        if warm.outcome != "completed":
            handle = await manager.get(sandbox_key)
            record = await handle.observe_commands.get(warm.command_id)
            print(
                "tracker record:",
                {k: v for k, v in record.items() if k not in ("stdout", "stderr", "env")},
            )
        assert (warm.outcome, warm.stdout) == ("completed", "hello"), warm
        print("1. Sandbox ready")

        async def leftovers():
            r = await runtime.run_command(
                key,
                "ps -eo stat=,comm= | grep -v '^Z' | grep -c ' sleep$' || true",
                timeout_seconds=60,
            )
            return int(r.stdout.strip() or 0)

        assert await leftovers() == 0
        t = time.time()
        plain = await runtime.run_command(key, "printf x", timeout_seconds=60)
        print(f"   a command with nothing left behind: {plain.outcome} in {time.time() - t:.1f}s")
        assert plain.outcome == "completed"
        t = time.time()
        ran = await runtime.run_command(key, "sleep 300 & echo started", timeout_seconds=120)
        print(f"2. shell exited, child left in the group: {ran.outcome} in {time.time() - t:.1f}s")
        if ran.outcome != "completed":
            handle = await manager.get(sandbox_key)
            record = await handle.observe_commands.get(ran.command_id)
            print("   tracker error:", record.get("error"), "| state:", record.get("state"))
        assert ran.outcome == "completed", ran
        left = await leftovers()
        print("   sleep processes left afterwards:", left)
        assert left == 0, "the leftover child of the command was not stopped"

        t = time.time()
        ran = await runtime.run_command(
            key, "(trap '' TERM; sleep 300) & echo started", timeout_seconds=120
        )
        print(f"3. leftover that ignores TERM: {ran.outcome} in {time.time() - t:.1f}s")
        assert ran.outcome == "completed", ran
        left = await leftovers()
        print("   sleep processes left afterwards:", left)
        assert left == 0, "the leftover child that ignores TERM was not killed"
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
