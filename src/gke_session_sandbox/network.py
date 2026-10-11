"""Per-generation Kubernetes and GKE FQDN network policy rendering."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from .config import ManagerConfig
from .types import SessionKey

_LABEL_MANAGED = "session-sandbox.example.com/managed"
_LABEL_KEY_HASH = "session-sandbox.example.com/session-key-hash"
_LABEL_GENERATION = "session-sandbox.example.com/generation"


def sandbox_pod_labels(key: SessionKey, generation: int) -> dict[str, str]:
    return {
        _LABEL_MANAGED: "true",
        _LABEL_KEY_HASH: key.stable_hash()[:63],
        _LABEL_GENERATION: str(generation),
    }


def build_session_network_policies(
    config: ManagerConfig,
    key: SessionKey,
    generation: int,
) -> tuple[dict[str, Any], ...]:
    """Render deny-by-default, management ingress, DNS, and optional FQDN allow policies."""
    labels = sandbox_pod_labels(key, generation)
    # Keep selectors independent from metadata labels. SandboxManager adds its
    # own resource labels after rendering, and mutating a shared dict here
    # would silently change which Pods these policies select.
    selector = {"matchLabels": copy.deepcopy(labels)}
    suffix = f"{key.stable_hash()[:10]}-g{generation}"
    policies: list[dict[str, Any]] = [
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": f"sandbox-deny-{suffix}", "labels": copy.deepcopy(labels)},
            "spec": {"podSelector": selector, "policyTypes": ["Ingress", "Egress"]},
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": f"sandbox-management-ingress-{suffix}", "labels": copy.deepcopy(labels)},
            "spec": {
                "podSelector": selector,
                "policyTypes": ["Ingress"],
                "ingress": [{
                    "from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": config.control_namespace}}}],
                    "ports": [
                        {"protocol": "TCP", "port": config.runtime.rest_port},
                        {"protocol": "TCP", "port": config.runtime.tracker_port},
                    ],
                }],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": f"sandbox-dns-egress-{suffix}", "labels": copy.deepcopy(labels)},
            "spec": {
                "podSelector": selector,
                "policyTypes": ["Egress"],
                "egress": [{
                    "to": [{
                        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": config.network.dns_namespace}},
                        "podSelector": {"matchLabels": config.network.dns_pod_selector},
                    }],
                    "ports": [{"protocol": protocol, "port": 53} for protocol in ("UDP", "TCP")],
                }],
            },
        },
    ]
    if config.network.allowed_fqdns:
        egress = []
        for rule in config.network.allowed_fqdns:
            match = {"name": rule.name} if rule.name is not None else {"pattern": rule.pattern}
            egress.append({
                "matches": [match],
                "ports": [{"protocol": port.protocol, "port": port.port} for port in rule.ports],
            })
        policies.append({
            "apiVersion": "networking.gke.io/v1alpha1",
            "kind": "FQDNNetworkPolicy",
            "metadata": {"name": f"sandbox-fqdn-egress-{suffix}", "labels": copy.deepcopy(labels)},
            "spec": {"podSelector": selector, "egress": egress},
        })
    return tuple(policies)
