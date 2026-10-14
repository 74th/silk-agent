"""Role entry points and distributed administration commands."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from silk_agent.config import ConfigError, load_config

CONFIG_ENV = "SILK_AGENT_CONFIG"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="silk-agent", description="silk-agent")
    sub = parser.add_subparsers(dest="command", required=True)
    slack = sub.add_parser("slack", help="run the Slack bot (Socket Mode)")
    slack.add_argument(
        "--config",
        "-c",
        help=f"config file (default: ${CONFIG_ENV}); relative paths in it "
        "are relative to this file",
    )
    gateway = sub.add_parser("gateway", help="run the durable Slack Socket Mode Gateway")
    gateway.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    sandbox_service = sub.add_parser(
        "sandbox-service", help="run the authenticated internal Sandbox API"
    )
    sandbox_service.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    admin = sub.add_parser("worker", help="inspect or drain a distributed Worker")
    admin.add_argument(
        "action",
        choices=("status", "drain", "undrain", "status-all", "drain-all", "undrain-all"),
    )
    admin.add_argument("worker_id", nargs="?")
    admin.add_argument("--incarnation", help="required for drain/undrain to avoid stale actions")
    admin.add_argument(
        "--wait", action="store_true", help="wait until reserved and active_sdk reach zero"
    )
    admin.add_argument(
        "--timeout", type=float, help="override the configured drain timeout in seconds"
    )
    admin.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    value = args.config or os.environ.get(CONFIG_ENV)
    if not value:
        sys.exit(f"config file is not specified (use --config or set {CONFIG_ENV})")
    path = Path(value)
    if not path.is_file():
        sys.exit(f"config file not found: {path}")
    try:
        config = load_config(path)
    except ConfigError as e:
        sys.exit(f"invalid config {path}: {e}")

    if args.command == "slack":
        from silk_agent.slack_app import run_from_environment

        asyncio.run(run_from_environment(config))
        return

    if args.command == "gateway":
        from silk_agent.distributed.gateway_runtime import run_gateway

        asyncio.run(run_gateway(config))
        return

    if args.command == "sandbox-service":
        from silk_agent.sandbox.service_runtime import run_sandbox_service

        asyncio.run(run_sandbox_service(config))
        return

    if config.execution_mode != "distributed":
        sys.exit("worker administration requires execution_mode: distributed")
    if args.timeout is not None and args.timeout <= 0:
        sys.exit("--timeout must be positive")
    all_workers = args.action.endswith("-all")
    base_action = args.action.removesuffix("-all")
    if args.worker_id is None and not all_workers:
        sys.exit("worker_id is required for this action")
    if args.worker_id is not None and all_workers:
        sys.exit("worker_id cannot be used with an all-worker action")
    if args.timeout is not None and base_action != "drain":
        sys.exit("--timeout can only be used with a drain action")
    if args.wait and base_action != "drain":
        sys.exit("--wait can only be used with a drain action")
    if base_action != "status" and not all_workers and not args.incarnation:
        sys.exit("--incarnation is required for drain and undrain")
    from silk_agent.core.clock import SystemClock
    from silk_agent.stores.firestore.client import create_firestore_client
    from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

    async def administer() -> int:
        import time

        now = SystemClock().now()
        drain_timeout = (
            args.timeout if args.timeout is not None else config.distributed.drain_timeout_seconds
        )
        drain_deadline_at = (
            now + timedelta(seconds=drain_timeout)
            if base_action == "drain" and drain_timeout is not None
            else None
        )
        client = create_firestore_client(config.distributed)
        try:
            directory = FirestoreWorkerDirectory(
                client, collection_prefix=config.distributed.firestore_prefix
            )
            if all_workers:
                records = tuple(await directory.list_workers())
                if base_action == "drain":
                    await directory.set_cluster_draining(True)
                if base_action != "status":
                    updated = []
                    for current in records:
                        record = await directory.set_draining(
                            worker_id=current.worker_id,
                            incarnation=current.incarnation,
                            draining=base_action == "drain",
                            now=now,
                            drain_deadline_at=drain_deadline_at,
                        )
                        if record is not None:
                            updated.append(record)
                    records = tuple(updated)
                if base_action == "undrain":
                    await directory.set_cluster_draining(False)
            else:
                record = (
                    await directory.get_worker(args.worker_id)
                    if base_action == "status"
                    else await directory.set_draining(
                        worker_id=args.worker_id,
                        incarnation=args.incarnation,
                        draining=base_action == "drain",
                        now=now,
                        drain_deadline_at=drain_deadline_at,
                    )
                )
                records = (record,) if record is not None else ()
            if not records:
                print("Worker not found or incarnation changed")
                return 1
            timed_out = False
            if args.wait:
                deadline = time.monotonic() + drain_timeout if drain_timeout is not None else None
                while any(record.reserved or record.active_sdk for record in records):
                    if deadline is not None and time.monotonic() >= deadline:
                        print("drain timed out; Worker(s) remain draining")
                        timed_out = True
                        break
                    await asyncio.sleep(2)
                    if all_workers:
                        records = tuple(await directory.list_workers())
                    else:
                        record = await directory.get_worker(args.worker_id)
                        if record is None or record.incarnation != args.incarnation:
                            print("Worker incarnation changed while waiting")
                            return 1
                        records = (record,)
            for record in records:
                print(
                    f"worker={record.worker_id} incarnation={record.incarnation} "
                    f"draining={str(record.draining).lower()} reserved={record.reserved} "
                    f"active_sdk={record.active_sdk} capacity={record.capacity}"
                )
            if all_workers:
                print(f"cluster_draining={str(await directory.is_cluster_draining()).lower()}")
            return 2 if args.wait and timed_out else 0
        finally:
            client.close()

    raise SystemExit(asyncio.run(administer()))
