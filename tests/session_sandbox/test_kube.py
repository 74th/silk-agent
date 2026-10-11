import pytest
from kubernetes_asyncio import config as kube_config

from gke_session_sandbox.kube import load_kube_client_config


async def test_in_cluster_config_is_loaded_without_await(monkeypatch):
    calls = []
    monkeypatch.setattr(kube_config, "load_incluster_config", lambda: calls.append("incluster"))

    async def kubeconfig(context=None):
        calls.append("kubeconfig")

    monkeypatch.setattr(kube_config, "load_kube_config", kubeconfig)
    await load_kube_client_config()
    assert calls == ["incluster"]


async def test_falls_back_to_kubeconfig_outside_a_cluster(monkeypatch):
    calls = []

    def not_in_cluster():
        raise kube_config.ConfigException("Service host/port is not set.")

    async def kubeconfig(context=None):
        calls.append(("kubeconfig", context))

    monkeypatch.setattr(kube_config, "load_incluster_config", not_in_cluster)
    monkeypatch.setattr(kube_config, "load_kube_config", kubeconfig)
    await load_kube_client_config("gke_ctx")
    assert calls == [("kubeconfig", "gke_ctx")]


async def test_other_errors_are_not_hidden(monkeypatch):
    def broken():
        raise PermissionError("token unreadable")

    monkeypatch.setattr(kube_config, "load_incluster_config", broken)
    with pytest.raises(PermissionError):
        await load_kube_client_config()
