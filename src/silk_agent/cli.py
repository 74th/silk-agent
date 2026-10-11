"""`silk-agent` command: `silk-agent slack [--config FILE]`."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Sequence
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

    from silk_agent.slack_app import run_from_environment

    asyncio.run(run_from_environment(config))
