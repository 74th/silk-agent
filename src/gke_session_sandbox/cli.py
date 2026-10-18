"""Command-line entry point for the GKE session sandbox package."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

import yaml

from . import __version__, network_probe
from .config import ManagerConfig
from .errors import ClusterCapabilityError, ConfigurationError
from .kube import load_kube_client_config
from .manager import NetworkReapplyResult, SandboxManager
from .preflight import validate_cluster_capabilities
from .reconciler import LifecycleReconciler
from .types import SessionKey, SessionState


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gke-session-sandbox",
        description="Manage session-scoped Agent Sandboxes on GKE.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    commands = parser.add_subparsers(dest="command")
    preflight = commands.add_parser(
        "preflight", help="check cluster APIs, runtime, storage, namespaces, and RBAC"
    )
    preflight.add_argument("--config", required=True, help="manager YAML configuration")
    preflight.add_argument("--context", help="kubeconfig context; defaults to current context")
    reconcile = commands.add_parser("reconcile", help="run session lifecycle reconciliation")
    reconcile.add_argument("--config", required=True, help="manager YAML configuration")
    reconcile.add_argument("--context", help="kubeconfig context; defaults to current context")
    reconcile.add_argument(
        "--once", action="store_true", help="perform one reconciliation pass and exit"
    )
    reapply = commands.add_parser(
        "reapply-network",
        help="re-apply the current network settings to existing Sandboxes without restarting them",
    )
    reapply.add_argument("--config", required=True, help="manager YAML configuration")
    reapply.add_argument("--context", help="kubeconfig context; defaults to current context")
    target = reapply.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--session",
        nargs=2,
        metavar=("TENANT_ID", "SESSION_ID"),
        help="re-apply to one conversation",
    )
    target.add_argument("--all", action="store_true", help="re-apply to every conversation")
    verify = commands.add_parser(
        "verify-network",
        help="check DNS, allowed and blocked destinations from inside a session's Sandbox",
    )
    verify.add_argument("--config", required=True, help="manager YAML configuration")
    verify.add_argument("--context", help="kubeconfig context; defaults to current context")
    verify.add_argument(
        "--session", nargs=2, required=True, metavar=("TENANT_ID", "SESSION_ID"), help="session"
    )
    verify.add_argument(
        "--ephemeral",
        action="store_true",
        help="create the session if it does not exist, and terminate it after the check",
    )
    verify.add_argument("--name", action="append", dest="names", help="extra name to resolve")
    verify.add_argument("--allow", action="append", default=[], help="HOST:PORT that must connect")
    verify.add_argument(
        "--deny",
        action="append",
        default=[],
        help="HOST:PORT that must be blocked (default: example.com:443 when nothing is given)",
    )
    return parser


async def _run_preflight(config_path: Path, kube_context: str | None) -> tuple[str, ...]:
    try:
        settings = ManagerConfig.model_validate(
            yaml.safe_load(await asyncio.to_thread(config_path.read_text, encoding="utf-8"))
        )
    except Exception as exc:
        raise ConfigurationError(f"cannot load manager config {config_path}: {exc}") from exc
    try:
        from kubernetes_asyncio import client
    except ImportError as exc:
        raise ConfigurationError(
            "install the kubernetes-asyncio dependency to run preflight"
        ) from exc

    await load_kube_client_config(kube_context)
    api_client = client.ApiClient()
    try:
        return await validate_cluster_capabilities(
            settings,
            core_api=client.CoreV1Api(api_client),
            node_api=client.NodeV1Api(api_client),
            storage_api=client.StorageV1Api(api_client),
            custom_api=client.CustomObjectsApi(api_client),
            authorization_api=client.AuthorizationV1Api(api_client),
        )
    finally:
        await api_client.close()


async def _run_reconcile(
    config_path: Path, kube_context: str | None, *, once: bool
) -> tuple[str, ...]:
    manager = await SandboxManager.from_config(config_path, kube_context=kube_context)
    try:
        reconciler = LifecycleReconciler(manager)
        if once:
            return await reconciler.reconcile_once()
        await reconciler.run_forever()
        return ()
    finally:
        await manager.close()


async def _run_reapply_network(
    config_path: Path, kube_context: str | None, key: SessionKey | None
) -> tuple[NetworkReapplyResult, ...]:
    manager = await SandboxManager.from_config(config_path, kube_context=kube_context)
    try:
        return await manager.reapply_network_policies(key)
    finally:
        await manager.close()


def verify_network_command(
    names: Sequence[str] | None, allow: Sequence[str], deny: Sequence[str]
) -> list[str]:
    """Command that runs the in-image probe from stdin-free source, so older images work too.

    The probe source is passed with `-c` and only reads and connects: nothing is written to disk.
    (sandboxd cannot start a command with `cwd=/tmp` in these Sandboxes, so no cwd is set.)
    """
    source = Path(network_probe.__file__).read_text(encoding="utf-8")
    # `bash -lc` like every other command: sandboxd alone cannot find python3 on its PATH.
    command = ["bash", "-lc", 'exec python3 -c "$0" verify "$@"', source]
    for name in names or ():
        command += ["--name", name]
    for target in allow:
        command += ["--allow", target]
    for target in deny or (() if allow else ("example.com:443",)):
        command += ["--deny", target]
    return command


async def verify_network(
    manager: SandboxManager,
    key: SessionKey,
    *,
    names: Sequence[str] | None = None,
    allow: Sequence[str] = (),
    deny: Sequence[str] = (),
    ephemeral: bool = False,
) -> tuple[int, str]:
    """Run the probe in the session's Sandbox; return (exit code, output).

    With ``ephemeral``, a session that does not exist yet is created for the check and
    terminated afterwards; an existing session is never terminated.
    """
    handle = await manager.get(key)
    created = False
    if handle is None or handle.state is SessionState.TERMINATED:
        if not ephemeral:
            raise ConfigurationError("no such session (use --ephemeral to create a test session)")
        await manager.ensure(key)
        created = True
    try:
        # A diagnostic never resumes or re-creates an existing session and leaves its idle and
        # retention clocks alone; the probe's own output says which check failed.
        result = await manager.run_diagnostic_command(
            key, verify_network_command(names, allow, deny), timeout_seconds=60
        )
    finally:
        if created:
            await manager.terminate(key, reason="network-verification")
    return result.exit_code, result.output


async def _run_verify_network(
    config_path: Path,
    kube_context: str | None,
    key: SessionKey,
    names: Sequence[str] | None,
    allow: Sequence[str],
    deny: Sequence[str],
    ephemeral: bool,
) -> tuple[int, str]:
    manager = await SandboxManager.from_config(config_path, kube_context=kube_context)
    try:
        return await verify_network(
            manager, key, names=names, allow=allow, deny=deny, ephemeral=ephemeral
        )
    finally:
        await manager.close()


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.command == "preflight":
        try:
            checks = asyncio.run(_run_preflight(Path(args.config), args.context))
        except (ClusterCapabilityError, ConfigurationError) as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2) from exc
        print("cluster preflight passed:")
        for check in checks:
            print(f"- {check}")
    elif args.command == "reconcile":
        try:
            errors = asyncio.run(_run_reconcile(Path(args.config), args.context, once=args.once))
        except (ClusterCapabilityError, ConfigurationError) as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2) from exc
        if errors:
            for error in errors:
                print(error, file=sys.stderr)
            raise SystemExit(1)
        print("reconciliation completed" if args.once else "reconciliation stopped")
    elif args.command == "reapply-network":
        key = (
            SessionKey(tenant_id=args.session[0], session_id=args.session[1])
            if args.session
            else None
        )
        try:
            results = asyncio.run(_run_reapply_network(Path(args.config), args.context, key))
        except (ClusterCapabilityError, ConfigurationError) as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2) from exc
        for result in results:
            print(f"{result.key_hash[:10]} {result.status} {result.detail}".rstrip())
        if any(result.status == "failed" for result in results):
            raise SystemExit(1)
    elif args.command == "verify-network":
        key = SessionKey(tenant_id=args.session[0], session_id=args.session[1])
        try:
            code, output = asyncio.run(
                _run_verify_network(
                    Path(args.config),
                    args.context,
                    key,
                    args.names,
                    args.allow,
                    args.deny,
                    args.ephemeral,
                )
            )
        except (ClusterCapabilityError, ConfigurationError) as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2) from exc
        print(output, end="" if output.endswith("\n") else "\n")
        if code != 0:
            raise SystemExit(1)
