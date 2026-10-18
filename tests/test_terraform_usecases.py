"""`usecases` input validation of terraform/variables.tf (no provider, no cloud access)."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform is required")


def usecase(name: str, **overrides) -> dict:
    value = {
        "system_namespace": f"{name}-system",
        "sandbox_namespace": f"{name}-sandboxes",
        "firestore_prefix": name,
        "slack_secret_ids": {
            "bot_token": f"{name}-slack-bot-token",
            "app_token": f"{name}-slack-app-token",
        },
    }
    value.update(overrides)
    return value


def plan(tmp_path: Path, usecases: dict) -> subprocess.CompletedProcess:
    """Run `terraform plan` on a root that holds only the variable definitions."""
    shutil.copy(ROOT / "terraform/variables.tf", tmp_path / "variables.tf")
    (tmp_path / "terraform.tfvars.json").write_text(json.dumps({"usecases": usecases}))
    return subprocess.run(
        ["terraform", "plan", "-input=false", "-lock=false", "-no-color"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )


def rejected(result: subprocess.CompletedProcess, message: str) -> bool:
    # terraform wraps long error messages, so compare without whitespace.
    return result.returncode != 0 and message in "".join(result.stderr.split())


def test_distinct_use_cases_are_accepted(tmp_path):
    assert plan(tmp_path, {"alpha": usecase("alpha"), "beta": usecase("beta")}).returncode == 0


def test_the_default_use_case_list_is_accepted(tmp_path):
    shutil.copy(ROOT / "terraform/variables.tf", tmp_path / "variables.tf")
    result = subprocess.run(
        ["terraform", "plan", "-input=false", "-lock=false", "-no-color"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_a_shared_sandbox_namespace_is_rejected(tmp_path):
    shared = usecase("beta", sandbox_namespace="alpha-sandboxes")
    result = plan(tmp_path, {"alpha": usecase("alpha"), "beta": shared})
    assert rejected(result, "全ユースケースを通して重複しない")


def test_a_sandbox_namespace_equal_to_another_use_cases_system_namespace_is_rejected(tmp_path):
    clash = usecase("beta", sandbox_namespace="alpha-system")
    result = plan(tmp_path, {"alpha": usecase("alpha"), "beta": clash})
    assert rejected(result, "全ユースケースを通して重複しない")


def test_a_shared_slack_secret_is_rejected_even_with_distinct_namespaces(tmp_path):
    shared = usecase(
        "beta",
        slack_secret_ids={
            "bot_token": "alpha-slack-bot-token",
            "app_token": "alpha-slack-app-token",
        },
    )
    result = plan(tmp_path, {"alpha": usecase("alpha"), "beta": shared})
    assert rejected(result, "slack_secret_ids")


def test_the_same_secret_for_bot_and_app_token_is_rejected(tmp_path):
    same = usecase("alpha", slack_secret_ids={"bot_token": "one", "app_token": "one"})
    assert rejected(plan(tmp_path, {"alpha": same}), "slack_secret_ids")
