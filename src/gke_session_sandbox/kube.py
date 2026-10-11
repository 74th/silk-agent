"""Kubernetes client configuration shared by the manager and the CLI."""

from __future__ import annotations


async def load_kube_client_config(kube_context: str | None = None) -> None:
    """Use the Pod's service account in a cluster, otherwise the kubeconfig.

    kubernetes_asyncio's `load_incluster_config` is synchronous (only `load_kube_config`
    is a coroutine); awaiting it raised TypeError inside Pods and fell back to a missing
    kubeconfig.
    """
    from kubernetes_asyncio import config as kube_config

    try:
        kube_config.load_incluster_config()
    except kube_config.ConfigException:
        await kube_config.load_kube_config(context=kube_context)
