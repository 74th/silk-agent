"""Command-line entry point for the GKE session sandbox package."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import yaml

from . import __version__
from .config import ManagerConfig
from .errors import ClusterCapabilityError, ConfigurationError
from .kube import load_kube_client_config
from .manager import SandboxManager
from .preflight import validate_cluster_capabilities
from .reconciler import LifecycleReconciler


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
