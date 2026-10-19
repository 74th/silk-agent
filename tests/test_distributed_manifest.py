import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

needs_kubectl = pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl is required")


def kustomize(path: str) -> list[dict]:
    out = subprocess.run(
        ["kubectl", "kustomize", path], check=True, capture_output=True, text=True
    ).stdout
    return [item for item in yaml.safe_load_all(out) if item]


@needs_kubectl
def test_sandbox_service_egress_can_reach_only_managed_sandbox_http_ports():
    resources = kustomize("k8s/distributed/overlays/dev")
    policy = next(
        item
        for item in resources
        if item["kind"] == "NetworkPolicy"
        and item["metadata"]["name"] == "silk-agent-sandbox-service"
    )
    sandbox_rule = next(
        rule
        for rule in policy["spec"]["egress"]
        if any("namespaceSelector" in target for target in rule.get("to", []))
    )
    assert sandbox_rule["to"] == [
        {
            "namespaceSelector": {
                "matchLabels": {"kubernetes.io/metadata.name": "session-sandboxes"}
            },
            "podSelector": {"matchLabels": {"session-sandbox.example.com/managed": "true"}},
        }
    ]
    assert {port["port"] for port in sandbox_rule["ports"]} == {8080, 8081}


def _resources(path: str = "k8s/distributed/base/roles.yaml"):
    return [item for item in yaml.safe_load_all(Path(path).read_text()) if item]


def _deployments():
    return {item["metadata"]["name"]: item for item in _resources() if item["kind"] == "Deployment"}


def _container(deployment):
    return deployment["spec"]["template"]["spec"]["containers"][0]


def _secret_names(deployment):
    container = _container(deployment)
    names = {ref["secretRef"]["name"] for ref in container.get("envFrom", [])}
    names |= {
        env["valueFrom"]["secretKeyRef"]["name"]
        for env in container.get("env", [])
        if "secretKeyRef" in env.get("valueFrom", {})
    }
    return names


def test_three_roles_have_probes_limits_and_the_released_replica_counts():
    deployments = _deployments()
    assert set(deployments) == {
        "silk-agent-gateway",
        "silk-agent-worker",
        "silk-agent-control",
    }
    replicas = {name: item["spec"]["replicas"] for name, item in deployments.items()}
    assert replicas == {
        "silk-agent-gateway": 1,
        "silk-agent-worker": 2,
        "silk-agent-control": 1,
    }
    for deployment in deployments.values():
        container = _container(deployment)
        assert container["image"] == "silk-agent"
        for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
            assert container[probe]["httpGet"]["port"] == "health"
        assert container["resources"]["requests"] and container["resources"]["limits"]["memory"]
        assert container["securityContext"]["allowPrivilegeEscalation"] is False
        # Only Control talks to the Kubernetes API (to read Pods); the others use Workload Identity.
        expected = deployment["metadata"]["name"] == "silk-agent-control"
        assert deployment["spec"]["template"]["spec"]["automountServiceAccountToken"] is expected


def test_no_role_gets_a_slack_token_through_the_environment_or_a_kubernetes_secret():
    for name, deployment in _deployments().items():
        container = _container(deployment)
        env_names = {env["name"] for env in container.get("env", [])}
        assert not {item for item in env_names if item.startswith("SLACK_")}, name
        assert _secret_names(deployment) == set(), name


def test_the_slack_tokens_are_read_from_secret_manager_by_the_config():
    config = yaml.safe_load(
        Path("k8s/distributed/overlays/dev/config/silk-agent-distributed.yaml").read_text()
    )
    assert config["slack"]["bot_token_secret"].startswith("projects/nnyn-dev/secrets/")
    assert config["slack"]["app_token_secret"].startswith("projects/nnyn-dev/secrets/")
    assert config["slack"]["bot_token_secret"] != config["slack"]["app_token_secret"]


def test_the_distributed_deploy_path_reads_no_env_file():
    script = Path("k8s/deploy.sh").read_text()
    distributed = script[script.index('if [[ -n "${distributed_dir}" ]]; then') :]
    distributed = distributed[: distributed.index("\n  exit 0\nfi")]
    assert "env_file" not in distributed
    assert "create secret generic silk-agent-slack" not in distributed


def test_no_distributed_role_mounts_sqlite_or_persistent_state():
    rendered = Path("k8s/distributed/base/roles.yaml").read_text()
    for deployment in _deployments().values():
        for volume in deployment["spec"]["template"]["spec"]["volumes"]:
            assert "persistentVolumeClaim" not in volume
    assert "sandbox-state" not in rendered and "/var/lib/session-sandbox" not in rendered
    sandbox_service = Path("k8s/distributed/base/sandbox-service.yaml").read_text()
    assert "persistentVolumeClaim" not in sandbox_service


def test_worker_is_a_separate_pod_with_downward_identity_and_a_sandbox_token():
    worker = _deployments()["silk-agent-worker"]
    spec = worker["spec"]["template"]["spec"]
    container = _container(worker)
    assert container["command"] == ["silk-agent", "worker", "run"]
    fields = {
        env["name"]: env["valueFrom"]["fieldRef"]["fieldPath"]
        for env in container["env"]
        if "fieldRef" in env.get("valueFrom", {})
    }
    assert fields == {
        "POD_NAME": "metadata.name",
        "POD_UID": "metadata.uid",
        "POD_NAMESPACE": "metadata.namespace",
    }
    token = next(volume for volume in spec["volumes"] if volume["name"] == "sandbox-token")
    source = token["projected"]["sources"][0]["serviceAccountToken"]
    assert source["audience"] == "silk-agent-sandbox-service"
    assert source["path"] == "silk-sandbox"
    mount = next(m for m in container["volumeMounts"] if m["name"] == "sandbox-token")
    assert mount["mountPath"] == "/var/run/secrets/tokens"
    assert spec["serviceAccountName"] == "silk-agent-worker"
    assert "silk-agent-sandbox-service" not in {
        item["spec"]["template"]["spec"]["serviceAccountName"] for item in _deployments().values()
    }


def test_worker_service_account_matches_the_sandbox_service_allow_list_and_ingress():
    config = yaml.safe_load(
        Path("k8s/distributed/overlays/dev/config/silk-agent-distributed.yaml").read_text()
    )
    assert config["distributed"]["sandbox_service_allowed_ksas"] == [
        "system:serviceaccount:session-sandbox-system:silk-agent-worker"
    ]
    policy = next(
        item
        for item in _resources("k8s/distributed/base/sandbox-service.yaml")
        if item["kind"] == "NetworkPolicy"
    )
    assert policy["spec"]["ingress"][0]["from"] == [
        {"podSelector": {"matchLabels": {"app.kubernetes.io/name": "silk-agent-worker"}}}
    ]
    worker_policy = next(
        item
        for item in _resources()
        if item["kind"] == "NetworkPolicy" and item["metadata"]["name"] == "silk-agent-worker"
    )
    assert worker_policy["spec"]["ingress"] == []
    assert any(
        rule.get("to", [{}])[0].get("podSelector", {}).get("matchLabels")
        == {"app.kubernetes.io/name": "silk-agent-sandbox-service"}
        for rule in worker_policy["spec"]["egress"]
    )


def test_roles_are_part_of_the_base_and_dev_uses_one_image():
    base = yaml.safe_load(Path("k8s/distributed/base/kustomization.yaml").read_text())
    assert "roles.yaml" in base["resources"]
    assert "images" not in base
    dev = yaml.safe_load(Path("k8s/distributed/overlays/dev/kustomization.yaml").read_text())
    assert [image["name"] for image in dev["images"]] == ["silk-agent"]


def test_control_may_only_read_pods_in_its_own_namespace():
    resources = _resources()
    role = next(
        i for i in resources if i["kind"] == "Role" and "pod-reader" in i["metadata"]["name"]
    )
    assert role["rules"] == [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}]
    binding = next(i for i in resources if i["kind"] == "RoleBinding")
    assert binding["subjects"][0]["name"] == "silk-agent-control"


def test_the_sandbox_service_runs_the_lifecycle_reconciler():
    """Without it Sandboxes and PVCs are never suspended or deleted (found on a live cluster)."""
    config = yaml.safe_load(
        Path("k8s/distributed/overlays/dev/config/silk-agent-distributed.yaml").read_text()
    )
    assert config["sandbox"]["run_reconciler"] is True


def test_bootstrap_overlay_starts_no_role_and_docs_do_not_claim_the_steady_state_does_not():
    bootstrap = yaml.safe_load(
        Path("k8s/distributed/overlays/dev-bootstrap/kustomization.yaml").read_text()
    )
    assert bootstrap["resources"] == ["../dev"]
    zero = {item["name"]: item["count"] for item in bootstrap["replicas"]}
    assert zero == {"silk-agent-gateway": 0, "silk-agent-worker": 0, "silk-agent-control": 0}
    # The steady-state manifest starts the roles; the docs must say so (they used to say it was
    # committed with replicas: 0, which made applying the steady-state overlay look safe).
    for doc in (
        Path("k8s/distributed/README.md"),
        Path("docs/distributed-operations-runbook.md"),
    ):
        text = doc.read_text(encoding="utf-8")
        assert "committed with `replicas: 0`" not in text
        assert "k8s/distributed/overlays/dev-bootstrap" in text


def test_every_role_gets_its_namespace_from_the_downward_api():
    for path in ("k8s/distributed/base/roles.yaml", "k8s/distributed/base/sandbox-service.yaml"):
        for deployment in (i for i in _resources(path) if i["kind"] == "Deployment"):
            env = {e["name"]: e for e in _container(deployment)["env"]}
            assert env["POD_NAMESPACE"]["valueFrom"]["fieldRef"]["fieldPath"] == (
                "metadata.namespace"
            ), deployment["metadata"]["name"]


def test_the_base_holds_no_environment_or_use_case_value():
    forbidden = re.compile(
        r"nnyn-dev|sha256:|silk-agent_|session-sandbox-system|session-sandboxes|usecase-"
    )
    for path in Path("k8s/distributed/base").rglob("*"):
        if path.is_file():
            assert not forbidden.search(path.read_text()), path
    for path in Path("k8s/distributed/platform").rglob("*"):
        if path.is_file():
            assert not forbidden.search(path.read_text()), path


def _comparable(documents: list[dict]) -> dict:
    """The rendered dev environment without what the split added on purpose.

    The platform component now also renders the Namespaces and the `sandbox-management` Role
    (they used to live in k8s/platform/base.yaml), labels the resources, and the roles read
    POD_NAMESPACE, and the shared ClusterRole moved to k8s/platform. Everything else must equal
    the output before the split.
    """
    out = {}
    for item in documents:
        name = item["metadata"]["name"]
        # The ClusterRole moved to k8s/platform/base.yaml (shared by every use case).
        if item["kind"] in ("Namespace", "ClusterRole") or name == "sandbox-management":
            continue
        labels = item["metadata"].get("labels", {})
        for key in ("app.kubernetes.io/part-of", "silk-agent.dev/in-sandbox-namespace"):
            labels.pop(key, None)
        if not labels:
            item["metadata"].pop("labels", None)
        if item["kind"] == "Deployment":
            for container in item["spec"]["template"]["spec"]["containers"]:
                container["env"] = [e for e in container["env"] if e["name"] != "POD_NAMESPACE"]
        out[(item["kind"], item["metadata"].get("namespace"), name)] = item
    return out


@needs_kubectl
@pytest.mark.parametrize(
    ("overlay", "before"),
    [
        ("k8s/distributed/overlays/dev", "distributed-dev-before.yaml"),
        ("k8s/distributed/overlays/dev-bootstrap", "distributed-dev-bootstrap-before.yaml"),
    ],
)
def test_dev_overlay_renders_what_k8s_distributed_rendered_before_the_split(overlay, before):
    expected = list(yaml.safe_load_all(Path("tests/fixtures", before).read_text()))
    assert _comparable(kustomize(overlay)) == _comparable([i for i in expected if i])


@needs_kubectl
def test_dev_platform_renders_the_namespaces_and_the_sandbox_management_role():
    rendered = {
        (i["kind"], i["metadata"]["name"]): i
        for i in kustomize("k8s/distributed/overlays/dev-platform")
    }
    assert {n for k, n in rendered if k == "Namespace"} == {
        "session-sandbox-system",
        "session-sandboxes",
    }
    role = rendered[("Role", "sandbox-management")]
    assert role["metadata"]["namespace"] == "session-sandboxes"


def test_the_shared_token_review_cluster_role_is_in_the_platform_not_in_any_use_case():
    name = "silk-agent-sandbox-service-token-review"
    platform = [i for i in _resources("k8s/platform/base.yaml") if i["kind"] == "ClusterRole"]
    assert [i["metadata"]["name"] for i in platform] == [name]
    for path in Path("k8s/distributed/base").glob("*.yaml"):
        assert "ClusterRole" not in {i["kind"] for i in _resources(str(path))}, path


@needs_kubectl
def test_deleting_one_use_case_leaves_the_cluster_scoped_resources_of_the_other():
    """Cluster-scoped resources rendered by two use cases never share a name, so
    `kubectl delete -k <use case>` cannot remove what another use case needs."""

    def cluster_scoped(path: str) -> set[tuple[str, str]]:
        return {
            (i["kind"], i["metadata"]["name"])
            for i in kustomize(path)
            if i["kind"] in ("Namespace", "ClusterRole", "ClusterRoleBinding")
        }

    dev = cluster_scoped("k8s/distributed/overlays/dev")
    bigquery = cluster_scoped("usecases/bigquery-analysis/k8s/distributed/steady")
    assert dev and bigquery
    assert not dev & bigquery
    assert not {kind for kind, _ in dev | bigquery} & {"ClusterRole"}


@needs_kubectl
def test_one_worker_at_a_time_can_be_evicted_by_a_drain_or_a_scale_down():
    budgets = [
        i for i in kustomize("k8s/distributed/overlays/dev") if i["kind"] == "PodDisruptionBudget"
    ]
    # Only the Worker has one: with a single replica a budget would block every drain of the node.
    assert [b["metadata"]["name"] for b in budgets] == ["silk-agent-worker"]
    budget = budgets[0]
    assert budget["spec"]["maxUnavailable"] == 1
    worker = _deployments()["silk-agent-worker"]
    labels = worker["spec"]["template"]["metadata"]["labels"]
    assert budget["spec"]["selector"]["matchLabels"].items() <= labels.items()


def test_the_distributed_use_case_configs_keep_the_audit_hooks_of_the_single_process_template():
    for path, package in (
        ("usecases/template/k8s/distributed/steady/config/silk-agent-distributed.yaml", "template"),
        (
            "usecases/bigquery-analysis/k8s/distributed/steady/config/silk-agent-distributed.yaml",
            "bigquery_analysis",
        ),
    ):
        hooks = yaml.safe_load(Path(path).read_text())["hooks"]
        assert "silk_agent.hooks:json_lines" in hooks, path
        factories = [h["factory"] for h in hooks if isinstance(h, dict)]
        assert factories == [f"silk_usecase_{package}.hooks:create"], path


def test_single_replica_roles_are_not_evicted_by_the_autoscaler_but_the_worker_still_can_be():
    key = "cluster-autoscaler.kubernetes.io/safe-to-evict"
    rendered = {
        item["metadata"]["name"]: item
        for item in kustomize("k8s/distributed/overlays/dev")
        if item["kind"] == "Deployment"
    }
    notes = {
        name: item["spec"]["template"]["metadata"].get("annotations", {}).get(key)
        for name, item in rendered.items()
    }
    assert notes == {
        "silk-agent-gateway": "false",
        "silk-agent-control": "false",
        "silk-agent-sandbox-service": "false",
        "silk-agent-worker": None,  # two replicas and a PodDisruptionBudget: it may move
    }
