"""The operations runbook only names commands and resources that exist."""

import re
import shlex
from pathlib import Path

import pytest
import yaml

from silk_agent.cli import build_parser

RUNBOOKS = [
    Path("docs/distributed-operations-runbook.md"),
    Path("docs/distributed-migration-runbook.md"),
]


def _commands(path: Path):
    text = path.read_text(encoding="utf-8")
    for block in re.findall(r"```sh\n(.*?)```", text, flags=re.S):
        joined = block.replace("\\\n", " ")
        for line in joined.splitlines():
            line = line.strip().removeprefix("uv run ")
            if line.startswith("silk-agent "):
                yield line


@pytest.mark.parametrize("path", RUNBOOKS, ids=lambda p: p.name)
def test_every_silk_agent_command_parses_with_the_real_cli(path):
    commands = list(_commands(path))
    assert commands, "the runbook should contain runnable silk-agent commands"
    for line in commands:
        argv = [
            arg.replace("CONFIG", "config.yaml") for arg in shlex.split(line.replace("...", ""))
        ][1:]
        # Placeholders for values the operator fills in.
        argv = [re.sub(r"^[A-Z_]{4,}$", "value", arg) for arg in argv]
        try:
            build_parser().parse_args(argv)
        except SystemExit as error:  # argparse exits on an unknown command or option
            raise AssertionError(f"runbook command does not parse: {line}") from error


def test_kubernetes_resources_named_in_the_runbook_exist_in_the_manifests():
    resources = {
        (item["kind"].lower(), item["metadata"]["name"])
        for name in ("roles.yaml", "sandbox-service.yaml")
        for item in yaml.safe_load_all(Path("k8s/distributed", name).read_text())
        if item
    }
    text = Path("docs/distributed-operations-runbook.md").read_text(encoding="utf-8")
    named = set(re.findall(r"\b(deployment)/(silk-agent-[a-z-]+)", text))
    assert named
    assert named <= resources


def test_the_runbook_has_no_step_that_runs_old_and_new_systems_together():
    text = Path("docs/distributed-operations-runbook.md").read_text(encoding="utf-8")
    assert "同時に起動する手順はない" in text
    assert "silk-agent slack" not in "\n".join(
        _commands(Path("docs/distributed-operations-runbook.md"))
    )
