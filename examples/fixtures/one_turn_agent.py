"""Finite agent command used by the Bot adapter example and lifecycle tests."""

from __future__ import annotations

import argparse
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("prompt")
    parser.add_argument("--wait-seconds", type=float, default=0)
    args = parser.parse_args()
    if args.wait_seconds < 0:
        parser.error("--wait-seconds must be non-negative")
    time.sleep(args.wait_seconds)
    print(f"turn complete: {args.prompt}")


if __name__ == "__main__":
    main()
