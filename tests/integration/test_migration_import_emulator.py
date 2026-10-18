import os
import uuid
from copy import deepcopy

import pytest

from gke_session_sandbox.firestore_repository import FirestoreSessionRepository
from silk_agent.config import DistributedExecutionConfig
from silk_agent.distributed.migration_import import FirestoreImportTarget, import_snapshot
from silk_agent.stores.firestore.client import create_firestore_client
from tests.test_migration_import import importable_snapshot

pytestmark = pytest.mark.integration


async def test_create_only_import_round_trips_and_conflicts_without_overwriting():
    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run emulator tests")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    prefix = f"migration-{uuid.uuid4().hex[:12]}"
    target = FirestoreImportTarget(client, collection_prefix=prefix)
    source = importable_snapshot()
    try:
        first = await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
        second = await import_snapshot(source, target, legacy_stopped=True, dry_run=False)
        assert first["created"] == 4 and second["created"] == 0
        repository = FirestoreSessionRepository(client, collection_prefix=prefix)
        record = await repository.get_by_hash(source["sandbox_records"][0]["key_hash"])
        assert record is not None
        assert record.record.generation == 2
        assert record.record.sandbox_ref.uid == "pod-uid"
        assert record.record.workspace_pvc_refs[0].uid == "pvc-uid"
        changed = deepcopy(source)
        changed["managed_sessions"][0]["sdk_session_id"] = "different-sdk"
        with pytest.raises(ValueError, match="conflict"):
            await import_snapshot(changed, target, legacy_stopped=True, dry_run=False)
        assert (await repository.get_by_hash(record.record.key_hash)) == record
    finally:
        client.close()
