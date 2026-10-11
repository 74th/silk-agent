from __future__ import annotations

import pytest
from pydantic import ValidationError

from gke_session_sandbox import ManagerConfig, SessionKey, build_session_network_policies, sandbox_pod_labels


def test_network_policies_deny_by_default_allow_management_and_cluster_dns() -> None:
    config = ManagerConfig(
        runtime={"image": "example.invalid/sandbox:v1", "rest_port": 8080, "tracker_port": 8081},
        control_namespace="session-sandbox-system",
    )
    key = SessionKey(tenant_id="t", session_id="s")
    policies = build_session_network_policies(config, key, 3)
    by_name = {policy["metadata"]["name"].split("-" if "fqdn" in policy["metadata"]["name"] else "_")[0]: policy for policy in policies}

    assert len(policies) == 3
    deny = next(policy for policy in policies if "deny" in policy["metadata"]["name"])
    assert deny["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert "ingress" not in deny["spec"] and "egress" not in deny["spec"]
    ingress = next(policy for policy in policies if "management-ingress" in policy["metadata"]["name"])
    assert ingress["spec"]["ingress"][0]["ports"] == [
        {"protocol": "TCP", "port": 8080}, {"protocol": "TCP", "port": 8081}
    ]
    assert ingress["spec"]["ingress"][0]["from"][0]["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": "session-sandbox-system"
    }
    dns = next(policy for policy in policies if "dns-egress" in policy["metadata"]["name"])
    assert dns["spec"]["egress"][0]["ports"] == [
        {"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}
    ]
    assert deny["spec"]["podSelector"] == {"matchLabels": sandbox_pod_labels(key, 3)}


def test_fqdn_rules_are_rendered_only_for_configured_names_and_ports() -> None:
    config = ManagerConfig(
        runtime={"image": "example.invalid/sandbox:v1"},
        network={"allowed_fqdns": [
            {"name": "api.example.com", "ports": [{"protocol": "TCP", "port": 443}]},
            {"pattern": "*.packages.example.com", "ports": [{"protocol": "TCP", "port": 443}, {"protocol": "TCP", "port": 8443}]},
        ]},
    )
    policies = build_session_network_policies(config, SessionKey(tenant_id="t", session_id="s"), 1)
    fqdn = next(policy for policy in policies if policy["kind"] == "FQDNNetworkPolicy")

    assert fqdn["apiVersion"] == "networking.gke.io/v1alpha1"
    assert fqdn["spec"]["egress"] == [
        {"matches": [{"name": "api.example.com"}], "ports": [{"protocol": "TCP", "port": 443}]},
        {"matches": [{"pattern": "*.packages.example.com"}], "ports": [
            {"protocol": "TCP", "port": 443}, {"protocol": "TCP", "port": 8443}
        ]},
    ]
    assert len({tuple(sorted(p["spec"]["podSelector"]["matchLabels"].items())) for p in policies}) == 1


def test_resource_labels_do_not_change_policy_pod_selectors() -> None:
    key = SessionKey(tenant_id="t", session_id="s")
    policies = build_session_network_policies(
        ManagerConfig(runtime={"image": "example.invalid/sandbox:v1"}), key, 2
    )

    for policy in policies:
        policy["metadata"]["labels"].update({"app.kubernetes.io/name": "gke-session-sandbox"})

    expected = {"matchLabels": sandbox_pod_labels(key, 2)}
    assert all(policy["spec"]["podSelector"] == expected for policy in policies)


@pytest.mark.parametrize(
    "rule",
    [
        {"name": "api.example.com", "pattern": "*.example.com"},
        {"name": "*.example.com"},
        {"name": "api.example.com", "ports": []},
        {"pattern": "api.example.com", "ports": [{"protocol": "SCTP", "port": 443}]},
    ],
)
def test_invalid_fqdn_rules_are_rejected(rule: dict) -> None:
    with pytest.raises(ValidationError):
        ManagerConfig(runtime={"image": "example.invalid/sandbox:v1"}, network={"allowed_fqdns": [rule]})
