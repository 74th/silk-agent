"""AGENT_USER_EMAIL reaches the agent's commands in a real Sandbox (needs a cluster).

Run with `-m gke` and SILK_AGENT_GKE_SANDBOX_CONFIG=<sandbox-config.yaml of the validation
cluster>. The Sandbox is created with the env the Bot passes (`AGENT_USER_EMAIL`)."""

from __future__ import annotations

import os
import uuid

import pytest

from silk_agent.core.ports import AGENT_USER_EMAIL_ENV

pytestmark = pytest.mark.gke

EMAIL = "alice@example.com"


@pytest.fixture
async def runtime():
    config = os.environ.get("SILK_AGENT_GKE_SANDBOX_CONFIG")
    if not config:
        pytest.skip("SILK_AGENT_GKE_SANDBOX_CONFIG is not set")
    from gke_session_sandbox import SandboxManager
    from silk_agent.sandbox.gke import GkeSessionSandboxRuntime

    async def env(_key):
        return {AGENT_USER_EMAIL_ENV: EMAIL}

    manager = await SandboxManager.from_config(config)
    rt = GkeSessionSandboxRuntime(manager, env_provider=env)
    try:
        yield rt
    finally:
        await rt.aclose()


async def test_agent_commands_see_the_email_before_and_after_resume(runtime):
    key = f"slack:Tgke:C{uuid.uuid4().hex[:8]}:1.0"
    try:
        foreground = await runtime.run_command(key, "echo $AGENT_USER_EMAIL")
        assert foreground.stdout.strip() == EMAIL

        # A command that runs longer, as the Bot's background commands do.
        slow = await runtime.run_command(key, "sleep 5; echo $AGENT_USER_EMAIL", timeout_seconds=60)
        assert slow.stdout.strip() == EMAIL

        manager = runtime.manager
        handle = runtime._handles[key]
        await manager.suspend(handle.key)
        runtime._handles.pop(key)  # a new Bot process has no handle
        resumed = await runtime.run_command(key, "echo $AGENT_USER_EMAIL")
        assert resumed.stdout.strip() == EMAIL
    finally:
        handle = runtime._handles.get(key)
        if handle is not None:
            await runtime.manager.terminate(handle.key, reason="test cleanup")
