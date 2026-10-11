"""Create, suspend, resume, and reconnect to a session using the async API."""

from __future__ import annotations

import argparse
import asyncio

from gke_session_sandbox import SandboxManager, SessionKey


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="examples/sandbox-config.yaml")
    parser.add_argument("--tenant", default="example-tenant")
    parser.add_argument("--session", default="example-thread")
    args = parser.parse_args()

    async with await SandboxManager.from_config(args.config) as manager:
        key = SessionKey(tenant_id=args.tenant, session_id=args.session)
        session = await manager.ensure(key)
        session = await manager.suspend(session)
        session = await manager.resume(session)
        print(f"session generation={session.generation} state={session.state.value} sandbox={session.sandbox_ref.name}")


if __name__ == "__main__":
    asyncio.run(main())
