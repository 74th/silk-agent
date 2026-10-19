"""Configuration and fixed-query behavior for the BigQuery analysis use case."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from gke_session_sandbox import (
    ManagerConfig,
    SessionKey,
    TemplateRenderer,
    build_session_network_policies,
)
from silk_agent.agent.mcp_config import load_mcp_servers
from silk_agent.config import Config

ROOT = Path(__file__).resolve().parents[1]
USECASE = ROOT / "usecases" / "bigquery-analysis"
STEADY = USECASE / "k8s/distributed/steady"
SYSTEM = "bigquery-analysis-system"
SANDBOXES = "bigquery-analysis-sandboxes"
SCRIPT_PATH = USECASE / "skills/google-trends/scripts/query.py"
SPEC = importlib.util.spec_from_file_location("bigquery_query", SCRIPT_PATH)
assert SPEC and SPEC.loader
query_script = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = query_script
SPEC.loader.exec_module(query_script)


def test_usecase_config_and_image_references_exist() -> None:
    config = Config.model_validate(
        yaml.safe_load((STEADY / "config/silk-agent-distributed.yaml").read_text(encoding="utf-8"))
    )
    assert config.distributed.firestore_prefix == "bigquery-analysis"
    assert config.agent.system_prompt_file == "/app/usecase/prompts/agent-system.md"
    assert config.skills.source == "/app/usecase/skills"
    assert config.mcp.config_file == "/config/mcp.json"
    assert (USECASE / "prompts/agent-system.md").is_file()
    assert (USECASE / "skills/google-trends/SKILL.md").is_file()
    assert (USECASE / "bot.Dockerfile").is_file()
    assert (USECASE / "sandbox/Dockerfile").is_file()


def test_fixed_query_uses_required_date_country_columns_dedup_order_and_limit() -> None:
    sql = (SCRIPT_PATH.parent / "query.sql").read_text(encoding="utf-8")
    normalized = " ".join(sql.split())
    assert "SELECT DISTINCT refresh_date, rank, term" in normalized
    assert "FROM `bigquery-public-data.google_trends.international_top_rising_terms`" in normalized
    assert "country_name = 'Japan'" in normalized
    assert "CURRENT_DATE('Asia/Tokyo')" in normalized
    assert "INTERVAL 1 DAY" in normalized
    assert "ORDER BY refresh_date DESC, rank ASC" in normalized
    assert "LIMIT 10" in normalized


class FakeJob:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def result(self) -> list[dict[str, Any]]:
        return self.rows


class FakeClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.sql = ""

    def query(self, sql: str) -> FakeJob:
        self.sql = sql
        return FakeJob(self.rows)


@pytest.mark.parametrize(
    ("rows", "expected_rows"),
    [
        ([{"refresh_date": date(2026, 10, 12), "rank": 1, "term": "sample"}], 1),
        ([], 0),
    ],
)
def test_script_emits_success_json_for_data_and_empty_results(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    rows: list[dict],
    expected_rows: int,
) -> None:
    monkeypatch.setenv("BIGQUERY_JOB_PROJECT", "billing-project")
    client = FakeClient(rows)
    result = query_script.main(lambda **kwargs: client)
    captured = capsys.readouterr()

    assert result == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["status"] == "ok"
    assert len(payload["rows"]) == expected_rows
    if expected_rows:
        assert payload["rows"][0] == {
            "refresh_date": "2026-10-12",
            "rank": 1,
            "term": "sample",
        }


DAILY_MESSAGE = (
    "usage exceeded the custom quota for QueryUsagePerDay, which is set by your administrator."
)


def _forbidden_with_reason(reason: str, message: str = "denied") -> Exception:
    error = type("Forbidden", (Exception,), {})("403")
    error.errors = [{"reason": reason, "message": message}]  # type: ignore[attr-defined]
    return error


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (type("PermissionDenied", (Exception,), {})("denied"), "permission failure"),
        (type("ServiceUnavailable", (Exception,), {})("offline"), "communication failure"),
        # The daily query quota: 403 Forbidden, but not a permission problem, and not for today.
        (_forbidden_with_reason("quotaExceeded", DAILY_MESSAGE), "daily quota failure"),
        # Short-term limits (and a quotaExceeded that is not the daily usage one): retry later.
        (_forbidden_with_reason("rateLimitExceeded", "too many requests"), "rate limit failure"),
        (
            _forbidden_with_reason("quotaExceeded", "Quota exceeded: concurrent queries"),
            "rate limit failure",
        ),
        (type("TooManyRequests", (Exception,), {})("429"), "rate limit failure"),
        (_forbidden_with_reason("accessDenied"), "permission failure"),
    ],
)
def test_script_reports_api_failures_without_returning_result(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
    message: str,
) -> None:
    monkeypatch.setenv("BIGQUERY_JOB_PROJECT", "billing-project")

    class BrokenClient:
        def query(self, _sql: str) -> FakeJob:
            raise error

    result = query_script.main(lambda **kwargs: BrokenClient())
    captured = capsys.readouterr()

    assert result == 1
    assert message in captured.err
    assert captured.out == ""


def test_script_requires_project_for_injected_client(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("BIGQUERY_JOB_PROJECT", raising=False)
    assert query_script.main(lambda **kwargs: FakeClient([])) == 2
    assert "BIGQUERY_JOB_PROJECT is not set" in capsys.readouterr().err


def test_prompt_and_skill_require_evidence_and_failure_handling() -> None:
    prompt = (USECASE / "prompts/agent-system.md").read_text(encoding="utf-8")
    skill = (USECASE / "skills/google-trends/SKILL.md").read_text(encoding="utf-8")
    assert "google-trends" in prompt and "echo" in prompt
    assert "query.sh" in skill
    assert "終了コードが 0 以外" in skill and "結果を作らない" in skill
    assert "refresh_date" in skill and "rank" in skill and "term" in skill


def test_sandbox_image_includes_the_fixed_query_at_the_skill_path() -> None:
    dockerfile = (USECASE / "sandbox/Dockerfile").read_text(encoding="utf-8")
    assert "skills/google-trends/scripts/" in dockerfile
    assert "/app/usecase/skills/google-trends/scripts/" in dockerfile


def test_sandbox_config_wires_ksa_and_only_required_egress() -> None:
    raw = yaml.safe_load(
        (STEADY / "config/sandbox-service-config.yaml").read_text(encoding="utf-8")
    )
    config = ManagerConfig.model_validate(raw)
    assert (
        config.template_overrides["spec"]["podTemplate"]["spec"]["serviceAccountName"]
        == "bigquery-analysis"
    )
    assert config.network.allowed_fqdns[0].name == "bigquery.googleapis.com"
    assert config.network.allowed_fqdns[0].ports[0].port == 443
    assert config.network.allow_gke_metadata_server is True
    policies = build_session_network_policies(config, SessionKey(tenant_id="t", session_id="s"), 1)
    metadata = next(
        policy for policy in policies if "metadata-egress" in policy["metadata"]["name"]
    )
    assert metadata["spec"]["egress"][0]["to"] == [{"ipBlock": {"cidr": "169.254.169.254/32"}}]

    rendered = TemplateRenderer().render_file(
        ROOT / "templates/sandbox.yaml",
        parameters={
            "runtime": config.runtime.model_dump(),
            "workspace": {
                "storage_class": config.workspace_storage_class,
                "storage": config.workspace_storage,
            },
        },
        overrides=config.template_overrides,
    )
    assert (
        rendered.manifest["spec"]["podTemplate"]["spec"]["serviceAccountName"]
        == "bigquery-analysis"
    )


def test_ksa_manifest_and_terraform_identity_names_match() -> None:
    ksa = yaml.safe_load((STEADY / "bigquery-ksa.yaml").read_text(encoding="utf-8"))
    variables = (USECASE / "terraform/variables.tf").read_text(encoding="utf-8")
    assert ksa["metadata"]["name"] == "bigquery-analysis"
    assert ksa["metadata"]["namespace"] == SANDBOXES
    assert ksa["metadata"]["annotations"]["iam.gke.io/gcp-service-account"] == "${GSA_EMAIL}"
    assert f'default     = "{SANDBOXES}"' in variables
    assert 'default     = "bigquery-analysis"' in variables


def test_mcp_http_url_and_api_key_resolve_through_existing_loader() -> None:
    servers = load_mcp_servers(STEADY / "config/mcp.json", {"ECHO_MCP_API_KEY": "unit-test-key"})
    assert servers["echo"]["type"] == "http"
    assert servers["echo"]["url"] == (f"http://echo-mcp.{SYSTEM}.svc.cluster.local:8000/mcp")
    assert servers["echo"]["headers"] == {"X_API_KEY": "unit-test-key"}


def test_kustomize_echo_service_and_secret_wiring_render() -> None:
    import shutil
    import subprocess

    if shutil.which("kubectl") is None:
        pytest.skip("kubectl is not installed")
    result = subprocess.run(
        ["kubectl", "kustomize", str(STEADY)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    echo_deployment = next(
        doc
        for doc in docs
        if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == "echo-mcp"
    )
    echo_env = echo_deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    bot = next(
        doc
        for doc in docs
        if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == "silk-agent-worker"
    )
    bot_env = next(
        container
        for container in bot["spec"]["template"]["spec"]["containers"]
        if container["name"] == "worker"
    )["env"]
    for env in (echo_env, bot_env):
        echo_key = next(entry for entry in env if entry["name"] == "ECHO_MCP_API_KEY")
        assert echo_key["valueFrom"]["secretKeyRef"] == {
            "name": "bigquery-analysis-secrets",
            "key": "ECHO_MCP_API_KEY",
        }
    service = next(
        doc
        for doc in docs
        if doc.get("kind") == "Service" and doc["metadata"]["name"] == "echo-mcp"
    )
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["ports"][0]["port"] == 8000
    policy = next(
        doc
        for doc in docs
        if doc.get("kind") == "NetworkPolicy" and doc["metadata"]["name"] == "echo-mcp-ingress"
    )
    assert policy["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "silk-agent-worker"
    }
    # The Worker's NetworkPolicy has no rule for echo-mcp, so this use case adds one.
    egress = next(
        doc
        for doc in docs
        if doc.get("kind") == "NetworkPolicy"
        and doc["metadata"]["name"] == "echo-mcp-egress-from-worker"
    )
    assert egress["spec"]["egress"][0]["ports"] == [{"protocol": "TCP", "port": 8000}]
    # Every namespaced resource is in one of the use case's two namespaces.
    spaces = {
        doc["metadata"].get("namespace")
        for doc in docs
        if doc["kind"] not in ("Namespace", "ClusterRole", "ClusterRoleBinding")
    }
    assert spaces == {SYSTEM, SANDBOXES}
    assert {d["metadata"]["name"] for d in docs if d["kind"] == "Namespace"} == spaces
    assert all(
        container["image"] != "silk-agent"
        for d in docs
        if d["kind"] == "Deployment"
        for container in d["spec"]["template"]["spec"]["containers"]
    )


def test_operations_guide_has_required_provisioning_and_rollback_paths() -> None:
    guide = (USECASE / "OPERATIONS.md").read_text(encoding="utf-8")
    for required in (
        "terraform apply",
        "output -raw gsa_email",
        "bigquery-ksa.yaml",
        "bigquery-analysis-system",
        "./build.sh",
        "bigquery-analysis-secrets",
        "DISTRIBUTED_DIR=usecases/bigquery-analysis/k8s/distributed/steady",
        "terraform destroy",
        "project とクエリ料金",
    ):
        assert required in guide
    for path in (
        "terraform/terraform.tfvars.example",
        "k8s/distributed/steady/bigquery-ksa.yaml",
        "k8s/distributed/steady/config/sandbox-service-config.yaml",
        "k8s/distributed/steady/kustomization.yaml",
        "k8s/distributed/bootstrap/kustomization.yaml",
    ):
        assert (USECASE / path).exists(), path
