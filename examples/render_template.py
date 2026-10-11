"""Validate the example config and render its Sandbox manifest locally."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from gke_session_sandbox import ManagerConfig, TemplateRenderer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path("examples/sandbox-config.yaml"))
    args = parser.parse_args()

    raw_config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    config = ManagerConfig.model_validate(raw_config)
    rendered = TemplateRenderer().render_file(
        config.template_file,
        parameters={
            "runtime": config.runtime.model_dump(),
            "workspace": {
                "storage_class": config.workspace_storage_class,
                "storage": config.workspace_storage,
            },
        },
    )
    print(f"# sha256: {rendered.digest}")
    print(yaml.safe_dump(rendered.manifest, sort_keys=False), end="")


if __name__ == "__main__":
    main()
