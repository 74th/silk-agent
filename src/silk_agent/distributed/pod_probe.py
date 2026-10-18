"""Kubernetes check that a Worker incarnation's Pod no longer exists."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from silk_agent.core.distributed_models import OwnershipToken

log = logging.getLogger(__name__)
_NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def pod_uid_of(token: OwnershipToken) -> str | None:
    """The Pod UID inside ``worker_incarnation`` (``[pod_uid, startup_uuid]``)."""
    return pod_uid_of_incarnation(token.worker_incarnation)


def pod_uid_of_incarnation(incarnation: str) -> str | None:
    try:
        value = json.loads(incarnation)
    except ValueError:
        return None
    if isinstance(value, list) and value and isinstance(value[0], str) and value[0]:
        return value[0]
    return None


class KubernetesPodProbe:
    """``is_gone`` is True only when the API says the named Pod is absent or is a different
    Pod (new UID). Any error or doubt returns False, so recovery stays blocked."""

    def __init__(self, core_api: Any, namespace: str) -> None:
        self._api = core_api
        self._namespace = namespace

    @classmethod
    async def in_cluster(cls) -> tuple[KubernetesPodProbe, Any]:
        from kubernetes_asyncio import client, config

        config.load_incluster_config()
        api_client = client.ApiClient()
        text = await asyncio.to_thread(Path(_NAMESPACE_FILE).read_text, encoding="utf-8")
        namespace = text.strip()
        return cls(client.CoreV1Api(api_client), namespace), api_client

    async def is_gone(self, token: OwnershipToken) -> bool:
        uid = pod_uid_of(token)
        if uid is None:
            return False
        try:
            pod = await self._api.read_namespaced_pod(token.worker_id, self._namespace)
        except Exception as exc:
            # kubernetes_asyncio raises ApiException(status=404) for an absent Pod.
            return getattr(exc, "status", None) == 404
        return getattr(getattr(pod, "metadata", None), "uid", None) != uid
