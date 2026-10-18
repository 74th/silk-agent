"""Role entry points and distributed administration commands."""

from __future__ import annotations

import argparse
import asyncio
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
    control = sub.add_parser("control", help="run distributed allocation and control processing")
    control.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    sandbox_service = sub.add_parser(
        "sandbox-service", help="run the authenticated internal Sandbox API"
    )
    sandbox_service.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    network = sub.add_parser(
        "sandbox-network",
        help="re-apply network policies to, or verify the network of, existing Sandboxes",
    )
    network.add_argument("action", choices=("reapply", "verify"))
    network.add_argument(
        "--conversation", help="conversation key, e.g. slack:T123:C123.456 (required for verify)"
    )
    network.add_argument("--all", action="store_true", help="reapply: every conversation")
    network.add_argument(
        "--ephemeral", action="store_true", help="verify: create a test session, then terminate it"
    )
    network.add_argument("--name", action="append", dest="names", help="verify: name to resolve")
    network.add_argument("--allow", action="append", default=[], help="verify: HOST:PORT to reach")
    network.add_argument("--deny", action="append", default=[], help="verify: HOST:PORT blocked")
    network.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    admin = sub.add_parser("worker", help="run a distributed Worker, or inspect/drain Workers")
    admin.add_argument(
        "action",
        choices=("run", "status", "drain", "undrain", "status-all", "drain-all", "undrain-all"),
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
    recovery = sub.add_parser("recovery", help="inspect or manually release blocked recoveries")
    recovery_actions = recovery.add_subparsers(dest="recovery_action", required=True)
    recovery_list = recovery_actions.add_parser("list", help="list expired ownerships")
    recovery_list.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    recovery_confirm = recovery_actions.add_parser(
        "confirm-stopped", help="record out-of-band proof that the old Worker SDK is stopped"
    )
    recovery_confirm.add_argument("conversation_key")
    recovery_confirm.add_argument(
        "--evidence",
        required=True,
        help="how the stop was verified (e.g. node fenced, ticket); stored with the release",
    )
    recovery_confirm.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    retention = sub.add_parser("retention", help="manage the retention of processed records")
    retention_actions = retention.add_subparsers(dest="retention_action", required=True)
    retention_backfill = retention_actions.add_parser(
        "backfill",
        help="give expire_at to records that ended before retention existed "
        "(counts only unless --apply)",
    )
    retention_backfill.add_argument(
        "--apply", action="store_true", help="write expire_at (default: only count)"
    )
    retention_backfill.add_argument(
        "--limit", type=int, help="handle at most this many eligible records per kind"
    )
    retention_backfill.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    migration = sub.add_parser(
        "migration", help="export, reconcile and import stopped legacy state"
    )
    migration_actions = migration.add_subparsers(dest="migration_action", required=True)
    migration_export = migration_actions.add_parser("export", help="export legacy state")
    migration_export.add_argument("--sqlite-db", required=True, help="legacy Sandbox SQLite DB")
    migration_export.add_argument(
        "--output", help="write a new mode-0600 JSON snapshot (default: stdout)"
    )
    migration_export.add_argument(
        "--target-snapshot", help="compare the legacy data with another migration snapshot"
    )
    migration_export.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    migration_import = migration_actions.add_parser("import", help="create-only Firestore import")
    migration_import.add_argument("--snapshot", required=True)
    migration_import.add_argument("--legacy-stopped", action="store_true", required=True)
    migration_import.add_argument("--execute", action="store_true", help="write (default: dry-run)")
    migration_import.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    reconcile = migration_actions.add_parser("reconcile-commands", help="read existing command IDs")
    reconcile.add_argument("--snapshot", required=True)
    reconcile.add_argument(
        "--tracker-endpoints",
        required=True,
        help="JSON conversation-key to port-forwarded tracker URL map",
    )
    reconcile.add_argument("--output", required=True)
    reconcile.add_argument("--config", "-c", help=f"config file (default: ${CONFIG_ENV})")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    from silk_agent.distributed.logging import configure_role_logging

    configure_role_logging(os.environ.get("LOG_LEVEL", "INFO"))
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

    if args.command == "control":
        from silk_agent.distributed.control_runtime import run_control

        asyncio.run(run_control(config))
        return

    if args.command == "worker" and args.action == "run":
        from silk_agent.distributed.worker_role import run_worker

        if args.worker_id is not None or args.wait or args.timeout is not None:
            sys.exit("worker run takes only --config")
        asyncio.run(run_worker(config))
        return

    if args.command == "sandbox-service":
        from silk_agent.sandbox.service_runtime import run_sandbox_service

        asyncio.run(run_sandbox_service(config))
        return

    if args.command == "sandbox-network":
        if args.action == "verify" and not args.conversation:
            sys.exit("sandbox-network verify requires --conversation")
        if args.action == "reapply" and bool(args.conversation) == bool(args.all):
            sys.exit("sandbox-network reapply takes exactly one of --conversation or --all")
        from silk_agent.sandbox.service_runtime import run_sandbox_network_admin

        raise SystemExit(
            asyncio.run(
                run_sandbox_network_admin(
                    config,
                    args.action,
                    conversation_key=args.conversation,
                    names=args.names,
                    allow=args.allow,
                    deny=args.deny,
                    ephemeral=args.ephemeral,
                )
            )
        )

    if args.command == "recovery":
        if config.execution_mode != "distributed":
            sys.exit("recovery administration requires execution_mode: distributed")
        raise SystemExit(asyncio.run(_administer_recovery(config, args)))

    if args.command == "retention":
        if config.execution_mode != "distributed":
            sys.exit("retention administration requires execution_mode: distributed")
        if args.limit is not None and args.limit < 1:
            sys.exit("--limit must be positive")
        raise SystemExit(asyncio.run(_retention_backfill(config, args)))

    if args.command == "migration":
        import json

        if args.migration_action != "export":
            from silk_agent.distributed.migration_import import (
                FirestoreImportTarget,
                import_snapshot,
                reconcile_commands,
            )

            snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
            if args.migration_action == "reconcile-commands":
                from gke_session_sandbox.commands import CommandTrackerClient
                from silk_agent.core.ports import CommandResult
                from silk_agent.distributed.migration import _write_snapshot

                endpoints = json.loads(Path(args.tracker_endpoints).read_text(encoding="utf-8"))

                class ReadOnlyTrackers:
                    async def command_status(self, conversation_key, command_id):
                        async def endpoint():
                            return endpoints[conversation_key]

                        async with CommandTrackerClient(endpoint) as tracker:
                            result = await tracker.get(command_id)
                        state = result.get("state")
                        outcome = "running" if state == "running" else "unknown"
                        if state == "finished":
                            outcome = (
                                "cancelled"
                                if result.get("stop_reason") == "cancelled"
                                else "completed"
                            )
                        return CommandResult(command_id, outcome, result.get("exit_code"))

                snapshot["command_observations"] = list(
                    asyncio.run(reconcile_commands(snapshot, ReadOnlyTrackers()))
                )
                _write_snapshot(args.output, snapshot)
                print(f"reconciled migration snapshot written: {args.output}")
                return
            if config.execution_mode != "distributed":
                sys.exit("migration import requires execution_mode: distributed")
            from silk_agent.stores.firestore.client import create_firestore_client

            async def import_data():
                client = create_firestore_client(config.distributed)
                try:
                    return await import_snapshot(
                        snapshot,
                        FirestoreImportTarget(
                            client, collection_prefix=config.distributed.firestore_prefix
                        ),
                        legacy_stopped=args.legacy_stopped,
                        dry_run=not args.execute,
                    )
                finally:
                    client.close()

            print(json.dumps(asyncio.run(import_data()), sort_keys=True))
            return

        from silk_agent.distributed.migration import (
            export_migration_snapshot,
            read_snapshot_sandbox_records,
        )
        from silk_agent.stores.agent_platform import AgentPlatformSessions

        if config.session_store.provider != "agent_platform":
            sys.exit("migration export requires session_store.provider: agent_platform")
        sessions = AgentPlatformSessions(
            config.session_store.agent_runtime,
            project=config.session_store.project,
            location=config.session_store.location,
            ttl_seconds=config.session_store.ttl_seconds,
        )

        async def export() -> None:
            target_records = (
                read_snapshot_sandbox_records(args.target_snapshot) if args.target_snapshot else ()
            )
            snapshot, _report = await export_migration_snapshot(
                args.sqlite_db,
                sessions,
                target_sandbox_records=target_records,
                output_path=args.output,
            )
            if args.output is None:
                print(json.dumps(snapshot, ensure_ascii=False, indent=2))
            else:
                print(f"migration snapshot written: {args.output}")

        asyncio.run(export())
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
                # A Pod that died keeps its last counts forever; only live Workers can drain.
                # Its conversations are recovered by Control, not by waiting here.
                while any(
                    record.reserved or record.active_sdk
                    for record in records
                    if _is_live(record, SystemClock().now(), config.distributed.lease_seconds)
                ):
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
                    + (
                        ""
                        if _is_live(record, now, config.distributed.lease_seconds)
                        else " stale=true"
                    )
                )
            if all_workers:
                print(f"cluster_draining={str(await directory.is_cluster_draining()).lower()}")
            return 2 if args.wait and timed_out else 0
        finally:
            client.close()

    raise SystemExit(asyncio.run(administer()))


async def _retention_backfill(config, args) -> int:
    from silk_agent.core.clock import SystemClock
    from silk_agent.stores.firestore.client import create_firestore_client
    from silk_agent.stores.firestore.retention import RetentionPolicy
    from silk_agent.stores.firestore.retention_backfill import backfill, format_reports

    client = create_firestore_client(config.distributed)
    try:
        reports = await backfill(
            client,
            prefix=config.distributed.firestore_prefix,
            policy=RetentionPolicy.from_config(config.distributed.retention),
            now=SystemClock().now(),
            apply=args.apply,
            limit=args.limit,
        )
    finally:
        client.close()
    print(format_reports(reports, apply=args.apply))
    return 0


async def _administer_recovery(config, args) -> int:
    from silk_agent.core.clock import SystemClock
    from silk_agent.stores.firestore.client import create_firestore_client
    from silk_agent.stores.firestore.conversations import FirestoreConversationStore
    from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore

    client = create_firestore_client(config.distributed)
    prefix = config.distributed.firestore_prefix
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    now = SystemClock().now()
    try:
        if args.recovery_action == "list":
            conversations = FirestoreConversationStore(client, collection_prefix=prefix)
            for token in await ownership.list_expired(before=now, limit=1000):
                record = await conversations.get_conversation(token.conversation_key) or {}
                print(
                    f"conversation={token.conversation_key} worker={token.worker_id} "
                    f"incarnation={token.worker_incarnation} epoch={token.epoch} "
                    f"state={record.get('allocation_state', 'unknown')} "
                    f"reason={record.get('recovery_block_reason', '')}"
                )
            return 0
        token = await ownership.get(args.conversation_key)
        if token is None:
            print("no live ownership for this conversation")
            return 1
        if token.expires_at > now:
            print("the lease has not expired; refusing to confirm a stop")
            return 1
        evidence = f"manual:{args.evidence.strip()}"
        if evidence == "manual:":
            print("--evidence must describe how the stop was verified")
            return 1
        if not await ownership.begin_recovery(token, now=now):
            print("ownership changed; re-run `recovery list`")
            return 1
        released = await ownership.confirm_stopped(
            token,
            evidence_id=evidence,
            worker_incarnation=token.worker_incarnation,
            now=now,
        )
        print(
            "released; uncertain inputs and foreground commands were orphaned"
            if released
            else "release was rejected; re-run `recovery list`"
        )
        return 0 if released else 1
    finally:
        client.close()


def _is_live(record, now, window_seconds: int) -> bool:
    """A Worker is live while its heartbeat is within the lease window."""
    return (now - record.heartbeat_at).total_seconds() <= window_seconds
