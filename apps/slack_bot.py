"""Slack bot entry point: `uv run python apps/slack_bot.py` (config: SILK_AGENT_CONFIG)."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from silk_agent.config import load_config
from silk_agent.slack_app import run_from_environment

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    path = Path(os.environ.get("SILK_AGENT_CONFIG", ROOT / "config" / "silk-agent.yaml"))
    if not path.exists():
        raise SystemExit(f"config file not found: {path} (copy config/silk-agent.example.yaml)")
    asyncio.run(run_from_environment(load_config(path), ROOT))


if __name__ == "__main__":
    main()
