from pathlib import Path

import yaml


def test_sandbox_service_egress_can_reach_only_managed_sandbox_http_ports():
    resources = list(yaml.safe_load_all(Path("k8s/distributed/sandbox-service.yaml").read_text()))
    policy = next(item for item in resources if item["kind"] == "NetworkPolicy")
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


def _resources(path: str = "k8s/distributed/roles.yaml"):
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
        assert deployment["spec"]["template"]["spec"]["automountServiceAccountToken"] is False


def test_slack_secrets_reach_only_the_roles_that_need_them_and_never_the_worker():
    deployments = _deployments()
    assert _secret_names(deployments["silk-agent-worker"]) == set()
    worker_env = {env["name"] for env in _container(deployments["silk-agent-worker"])["env"]}
    assert not {name for name in worker_env if name.startswith("SLACK_")}
    gateway = _container(deployments["silk-agent-gateway"])
    assert gateway["envFrom"] == [{"secretRef": {"name": "silk-agent-slack"}}]
    control = _container(deployments["silk-agent-control"])
    assert "envFrom" not in control
    keys = [
        env["valueFrom"]["secretKeyRef"]["key"]
        for env in control["env"]
        if "secretKeyRef" in env.get("valueFrom", {})
    ]
    assert keys == ["SLACK_BOT_TOKEN"]  # never the Socket Mode app token


def test_no_distributed_role_mounts_sqlite_or_persistent_state():
    rendered = Path("k8s/distributed/roles.yaml").read_text()
    for deployment in _deployments().values():
        for volume in deployment["spec"]["template"]["spec"]["volumes"]:
            assert "persistentVolumeClaim" not in volume
    assert "sandbox-state" not in rendered and "/var/lib/session-sandbox" not in rendered
    sandbox_service = Path("k8s/distributed/sandbox-service.yaml").read_text()
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
    assert fields == {"POD_NAME": "metadata.name", "POD_UID": "metadata.uid"}
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
        Path("k8s/distributed/sandbox-service/config/silk-agent-distributed.yaml").read_text()
    )
    assert config["distributed"]["sandbox_service_allowed_ksas"] == [
        "system:serviceaccount:session-sandbox-system:silk-agent-worker"
    ]
    policy = next(
        item
        for item in _resources("k8s/distributed/sandbox-service.yaml")
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


def test_roles_are_part_of_the_kustomization_and_use_one_image():
    kustomization = yaml.safe_load(Path("k8s/distributed/kustomization.yaml").read_text())
    assert "roles.yaml" in kustomization["resources"]
    assert [image["name"] for image in kustomization["images"]] == ["silk-agent"]
