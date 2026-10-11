"""Slack bot entry point: `uv run python apps/slack_bot.py` (same as `silk-agent slack`).

Config: SILK_AGENT_CONFIG, or config/silk-agent.yaml in this repository if it exists."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from silk_agent.cli import CONFIG_ENV, main

ROOT = Path(__file__).resolve().parent.parent

if __name__ == "__main__":
    default = ROOT / "config" / "silk-agent.yaml"
    if CONFIG_ENV not in os.environ and default.exists():
        os.environ[CONFIG_ENV] = str(default)
    main(["slack", *sys.argv[1:]])
