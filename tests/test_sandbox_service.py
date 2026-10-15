from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from gke_session_sandbox.types import EventType, SessionState
from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.sandbox.service import create_app


async def _client(
    review,
    *,
    ready=lambda: True,
    manager=None,
    authorizer=None,
    owner_provider=None,
    operation_store=None,
    clock=None,
):
    app = create_app(
        audience="silk-sandbox",
        allowed_ksas=("system:serviceaccount:silk:worker",),
        token_review=review,
        ready=ready,
        manager=manager,
        conversation_authorizer=authorizer,
        owner_provider=owner_provider,
        operation_store=operation_store,
        clock=clock or (lambda: datetime.now(UTC)),
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


async def test_health_and_readiness_are_open_and_readiness_is_read_only():
    checks = []

    def ready():
        checks.append("ready")
        return True

    async def review(_token, _audience):
        raise AssertionError("probe must not call TokenReview")

    client = await _client(review, ready=ready)
    try:
        assert (await client.get("/healthz")).status == 200
        response = await client.get("/readyz")
        assert response.status == 200
        assert await response.json() == {"status": "ready"}
        metrics = await client.get("/metrics")
        assert metrics.status == 200
        assert "silk_agent_sandbox_http_requests_total" in await metrics.text()
        assert checks == ["ready"]
    finally:
        await client.close()


async def test_protected_route_rejects_missing_token():
    async def review(_token, _audience):
        raise AssertionError("missing token must be rejected before review")

    client = await _client(review)
    try:
        response = await client.get("/v1/protected")
        assert response.status == 401
        assert (await response.json())["error"]["code"] == "unauthenticated"
    finally:
        await client.close()


async def test_protected_route_rejects_expired_token():
    async def review(token, audience):
        assert token == "expired-token"
        assert audience == "silk-sandbox"
        return {"authenticated": False}

    client = await _client(review)
    try:
        response = await client.get(
            "/v1/protected", headers={"Authorization": "Bearer expired-token"}
        )
        assert response.status == 401
    finally:
        await client.close()


async def test_protected_route_rejects_wrong_audience():
    async def review(_token, _audience):
        return {
            "authenticated": True,
            "audiences": ["some-other-service"],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    client = await _client(review)
    try:
        response = await client.get("/v1/protected", headers={"Authorization": "Bearer token"})
        assert response.status == 401
    finally:
        await client.close()


async def test_protected_route_rejects_unapproved_ksa():
    async def review(_token, _audience):
        return {
            "authenticated": True,
            "audiences": ["silk-sandbox"],
            "user": {"username": "system:serviceaccount:silk:other"},
        }

    client = await _client(review)
    try:
        response = await client.get("/v1/protected", headers={"Authorization": "Bearer token"})
        assert response.status == 403
    finally:
        await client.close()


async def test_session_endpoints_ensure_status_notices_and_ack():
    class Manager:
        ensure_calls = 0
        get_calls = 0

        async def ensure(self, key):
            self.ensure_calls += 1
            return SimpleNamespace(
                generation=3,
                state=SessionState.READY,
                record=SimpleNamespace(generation=3),
            )

        async def get(self, key):
            self.get_calls += 1
            return None

        async def pending_context_events(self, key):
            return [
                SimpleNamespace(
                    event_id="event-a",
                    generation=3,
                    event_type=EventType.WORKSPACE_LOST,
                    context_message="Workspace data was lost.",
                )
            ]

        async def ack_context_event(self, key, event_id):
            return event_id == "event-a"

    async def review(_token, audience):
        assert audience == "silk-sandbox"
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    manager = Manager()

    async def authorize(_ksa, conversation_key):
        return conversation_key == "slack:T:C:123"

    client = await _client(review, manager=manager, authorizer=authorize)
    headers = {"Authorization": "Bearer token"}
    try:
        response = await client.get(
            "/v1/sessions/status", params={"conversation_key": "slack:T:C:123"}, headers=headers
        )
        assert await response.json() == {
            "exists": False,
            "ready": False,
            "state": "missing",
        }
        assert manager.get_calls == 1
        assert manager.ensure_calls == 0

        response = await client.get(
            "/v1/sessions/notices",
            params={"conversation_key": "slack:T:C:123"},
            headers=headers,
        )
        assert (await response.json())["notices"][0]["event_id"] == "event-a"
        response = await client.post(
            "/v1/sessions/notices/ack",
            json={"conversation_key": "slack:T:C:123", "event_id": "event-a"},
            headers=headers,
        )
        assert (await response.json())["acknowledged"] is True
        response = await client.post(
            "/v1/sessions/ensure",
            json={"conversation_key": "slack:T:C:123"},
            headers=headers,
        )
        assert (await response.json())["ready"] is True
        assert manager.ensure_calls == 1

        response = await client.get(
            "/v1/sessions/status",
            params={"conversation_key": "slack:T:OTHER:456"},
            headers=headers,
        )
        assert response.status == 403
    finally:
        await client.close()


async def test_command_start_persists_idempotent_intent_before_tracker_submission():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    events = []
    owner = OwnershipToken(
        conversation_key="slack:T:C:123",
        worker_id="worker-a",
        worker_incarnation="incarnation-a",
        epoch=2,
        conversation_generation=4,
        expires_at=now + timedelta(seconds=60),
    )

    class Manager:
        async def ensure(self, _key):
            events.append("ensure")
            return SimpleNamespace(
                generation=3,
                commands=SimpleNamespace(start=start, get=get),
            )

    class Operations:
        record = None

        async def accept_intent(self, **kwargs):
            events.append("intent")
            if self.record is not None:
                return False
            self.record = {"request_hash": kwargs["request_hash"]}
            return True

        async def get_operation(self, _operation_id):
            return self.record

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            return True

    async def start(*_args, **_kwargs):
        events.append("tracker-start")
        return {"state": "running"}

    async def get(_command_id):
        return {"state": "running"}

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, conversation_key):
        return conversation_key == owner.conversation_key

    async def owner_provider(_conversation_key):
        return owner

    operations = Operations()
    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=operations,
        clock=lambda: now,
    )
    headers = {"Authorization": "Bearer token"}
    body = {
        "request_id": "request-1",
        "conversation_key": owner.conversation_key,
        "owner_epoch": 2,
        "conversation_generation": 4,
        "turn_id": "turn-7",
        "origin_seq": 8,
        "command": "echo ready",
    }
    try:
        response = await client.post("/v1/commands", json=body, headers=headers)
        assert response.status == 202
        assert (await response.json())["command_id"] == "request-1"
        assert events == ["ensure", "intent", "tracker-start"]

        response = await client.post("/v1/commands", json=body, headers=headers)
        assert response.status == 202
        assert (await response.json())["state"] == "running"
        assert events == ["ensure", "intent", "tracker-start", "ensure", "intent"]

        conflicting = {**body, "command": "echo different"}
        response = await client.post("/v1/commands", json=conflicting, headers=headers)
        assert response.status == 409
        assert (await response.json())["error"]["code"] == "request_id_conflict"
        assert events[-2:] == ["ensure", "intent"]
    finally:
        await client.close()


async def test_lost_start_response_queries_by_id_without_resending():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    owner = OwnershipToken(
        conversation_key="slack:T:C:123",
        worker_id="worker-a",
        worker_incarnation="incarnation-a",
        epoch=2,
        conversation_generation=4,
        expires_at=now + timedelta(seconds=60),
    )
    calls = {"start": 0, "get": 0}

    class ExecutionOutcomeUnknown(Exception):
        pass

    async def start(*_args, **_kwargs):
        calls["start"] += 1
        raise ExecutionOutcomeUnknown("response lost")

    async def get(_command_id):
        calls["get"] += 1
        raise LookupError("tracker has not exposed the command yet")

    class Manager:
        async def ensure(self, _key):
            return SimpleNamespace(
                generation=3,
                commands=SimpleNamespace(start=start, get=get),
            )

    class Operations:
        async def accept_intent(self, **_kwargs):
            return True

        async def get_operation(self, _operation_id):
            return None

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            return True

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, _conversation_key):
        return True

    async def owner_provider(_conversation_key):
        return owner

    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=Operations(),
        clock=lambda: now,
    )
    try:
        response = await client.post(
            "/v1/commands",
            json={
                "request_id": "request-uncertain",
                "conversation_key": owner.conversation_key,
                "owner_epoch": owner.epoch,
                "conversation_generation": owner.conversation_generation,
                "turn_id": "turn-7",
                "origin_seq": 8,
                "command": "echo once",
            },
            headers={"Authorization": "Bearer token"},
        )
        assert response.status == 202
        assert (await response.json())["state"] == "unknown"
        assert calls == {"start": 1, "get": 1}
    finally:
        await client.close()


async def test_command_intent_behind_stop_barrier_is_not_sent_to_tracker():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    owner = OwnershipToken(
        conversation_key="slack:T:C:123",
        worker_id="worker-a",
        worker_incarnation="incarnation-a",
        epoch=2,
        conversation_generation=4,
        expires_at=now + timedelta(seconds=60),
    )
    starts = []
    dispatch_check_entered = asyncio.Event()
    allow_dispatch_check = asyncio.Event()
    stop_seq = 0

    class Manager:
        async def ensure(self, _key):
            return SimpleNamespace(
                generation=3,
                commands=SimpleNamespace(start=lambda *args, **kwargs: starts.append(args)),
            )

    class Operations:
        async def accept_intent(self, **_kwargs):
            return True

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            dispatch_check_entered.set()
            await allow_dispatch_check.wait()
            return 8 > stop_seq

        async def get_operation(self, _operation_id):
            return None

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, _conversation_key):
        return True

    async def owner_provider(_conversation_key):
        return owner

    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=Operations(),
        clock=lambda: now,
    )
    try:
        request = asyncio.create_task(
            client.post(
                "/v1/commands",
                json={
                    "request_id": "blocked-command",
                    "conversation_key": owner.conversation_key,
                    "owner_epoch": 2,
                    "conversation_generation": 4,
                    "turn_id": "turn-7",
                    "origin_seq": 8,
                    "command": "echo must-not-start",
                },
                headers={"Authorization": "Bearer token"},
            )
        )
        await asyncio.wait_for(dispatch_check_entered.wait(), timeout=1)
        stop_seq = 8
        allow_dispatch_check.set()
        response = await request
        assert response.status == 409
        assert (await response.json())["error"]["code"] == "intent_stopped"
        assert starts == []
    finally:
        await client.close()


async def test_intent_accepted_before_stop_remains_findable_and_can_be_cancelled():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    start_entered = asyncio.Event()
    finish_start = asyncio.Event()
    stop_seq = 0
    canceled = []
    owner = OwnershipToken(
        conversation_key="slack:T:C:123",
        worker_id="worker-a",
        worker_incarnation="incarnation-a",
        epoch=2,
        conversation_generation=4,
        expires_at=now + timedelta(seconds=60),
    )

    class Commands:
        async def start(self, *_args, command_id, **_kwargs):
            start_entered.set()
            await finish_start.wait()
            return {
                "command_id": command_id,
                "state": "cancelled" if command_id in canceled else "running",
            }

        async def cancel(self, command_id):
            canceled.append(command_id)

    commands = Commands()

    class Manager:
        async def ensure(self, _key):
            return SimpleNamespace(generation=3, commands=commands)

    class Operations:
        records = {}

        async def accept_intent(self, **kwargs):
            self.records[kwargs["operation_id"]] = {
                **kwargs,
                "state": "accepted",
            }
            return True

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            return True

        async def get_operation(self, operation_id):
            return self.records.get(operation_id)

        async def list_for_stop(self, **kwargs):
            records = [
                record
                for record in self.records.values()
                if record["conversation_key"] == kwargs["conversation_key"]
                and record["conversation_generation"] == kwargs["conversation_generation"]
                and record["origin_seq"] <= kwargs["stop_seq"]
                and record["operation_kind"] == "command_start"
            ]
            return tuple(records), None

    operations = Operations()

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, _conversation_key):
        return True

    async def owner_provider(_conversation_key):
        return owner

    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=operations,
        clock=lambda: now,
    )
    headers = {"Authorization": "Bearer token"}
    identity = {
        "conversation_key": owner.conversation_key,
        "owner_epoch": 2,
        "conversation_generation": 4,
        "turn_id": "turn-7",
        "origin_seq": 8,
    }
    try:
        start_request = asyncio.create_task(
            client.post(
                "/v1/commands",
                json={**identity, "request_id": "start-before-stop", "command": "sleep 60"},
                headers=headers,
            )
        )
        await asyncio.wait_for(start_entered.wait(), timeout=1)

        stop_seq = 8
        pending, cursor = await operations.list_for_stop(
            conversation_key=owner.conversation_key,
            conversation_generation=4,
            stop_seq=stop_seq,
        )
        assert cursor is None
        assert [item["operation_id"] for item in pending] == ["start-before-stop"]

        cancel_response = await client.post(
            "/v1/commands/start-before-stop/cancel",
            json={**identity, "request_id": "cancel-after-stop"},
            headers=headers,
        )
        assert (await cancel_response.json())["state"] == "cancel_requested"
        assert canceled == ["start-before-stop"]

        finish_start.set()
        start_response = await start_request
        assert (await start_response.json())["state"] == "cancelled"
    finally:
        finish_start.set()
        await client.close()


async def test_file_and_skills_routes_are_conversation_scoped_and_generation_fenced():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    files_by_key = {}
    ensured = []

    class Files:
        def __init__(self, storage):
            self.storage = storage

        async def read(self, path):
            try:
                return self.storage[path]
            except KeyError as exc:
                raise FileNotFoundError(path) from exc

        async def write(self, path, content):
            if path == ".skills/fail.md":
                raise OSError("simulated partial sync failure")
            self.storage[path] = content

        async def list(self, _path):
            return [SimpleNamespace(path="notes.txt", type="file", size=3)]

    class Manager:
        async def ensure(self, key):
            ensured.append(key.session_id)
            storage = files_by_key.setdefault(key.stable_hash(), {})
            return SimpleNamespace(generation=3, files=Files(storage))

    class Operations:
        records = {}

        async def accept_intent(self, **kwargs):
            operation_id = kwargs["operation_id"]
            if operation_id in self.records:
                return False
            self.records[operation_id] = {"request_hash": kwargs["request_hash"]}
            return True

        async def get_operation(self, operation_id):
            return self.records.get(operation_id)

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            return True

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, _conversation_key):
        return True

    async def owner_provider(conversation_key):
        return OwnershipToken(
            conversation_key=conversation_key,
            worker_id="worker-a",
            worker_incarnation="incarnation-a",
            epoch=2,
            conversation_generation=4,
            expires_at=now + timedelta(seconds=60),
        )

    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=Operations(),
        clock=lambda: now,
    )
    headers = {"Authorization": "Bearer token"}
    identity = {
        "conversation_key": "slack:T:C:123",
        "owner_epoch": 2,
        "conversation_generation": 4,
        "turn_id": "turn-7",
        "origin_seq": 8,
    }
    try:
        response = await client.put(
            "/v1/files/write",
            json={
                **identity,
                "request_id": "write-a",
                "path": "notes.txt",
                "data_base64": "YWJj",
            },
            headers=headers,
        )
        assert (await response.json())["state"] == "written"

        response = await client.post(
            "/v1/files/read",
            json={**identity, "request_id": "read-a", "path": "notes.txt"},
            headers=headers,
        )
        assert (await response.json())["data_base64"] == "YWJj"

        response = await client.post(
            "/v1/files/list",
            json={**identity, "request_id": "list-a", "path": "."},
            headers=headers,
        )
        assert (await response.json())["entries"] == [
            {"path": "notes.txt", "is_dir": False, "size": 3}
        ]

        other = {**identity, "conversation_key": "slack:T:C:other"}
        response = await client.post(
            "/v1/files/read",
            json={**other, "request_id": "read-b", "path": "notes.txt"},
            headers=headers,
        )
        assert response.status == 404

        response = await client.put(
            "/v1/files/write",
            json={
                **identity,
                "owner_epoch": 1,
                "request_id": "stale-write",
                "path": "notes.txt",
                "data_base64": "bm8=",
            },
            headers=headers,
        )
        assert response.status == 409

        response = await client.put(
            "/v1/skills/sync",
            json={
                **identity,
                "request_id": "skills-a",
                "skills_generation": "skills-v1",
                "files": {"ok.md": "b2s=", "fail.md": "ZmFpbA=="},
            },
            headers=headers,
        )
        result = await response.json()
        assert response.status == 207
        assert result["complete"] is False
        assert result["failed_paths"] == ["fail.md"]

        response = await client.put(
            "/v1/files/write",
            json={
                **identity,
                "request_id": "large-write",
                "path": "large.bin",
                "data_base64": base64.b64encode(b"a" * (8 * 1024 * 1024 + 1)).decode("ascii"),
            },
            headers=headers,
        )
        assert response.status == 413
    finally:
        await client.close()


async def test_command_status_output_and_cancel_use_the_recorded_sandbox_generation():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    owner = OwnershipToken(
        conversation_key="slack:T:C:123",
        worker_id="worker-a",
        worker_incarnation="incarnation-a",
        epoch=2,
        conversation_generation=4,
        expires_at=now + timedelta(seconds=60),
    )
    operation = {
        "operation_kind": "command_start",
        "conversation_key": owner.conversation_key,
        "conversation_generation": 4,
        "sandbox_generation": 3,
        "turn_id": "turn-7",
        "origin_seq": 8,
    }
    canceled = []

    class Operations:
        records = {"cmd-1": operation}

        async def get_operation(self, _operation_id):
            return self.records.get(_operation_id)

        async def accept_intent(self, **kwargs):
            self.records[kwargs["operation_id"]] = {
                "request_hash": kwargs["request_hash"],
                "operation_kind": kwargs["operation_kind"],
            }
            return True

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            return True

    class Commands:
        async def get(self, _command_id):
            return {"state": "running", "exit_code": None}

        async def read_output(self, _command_id, *, stream, cursor, limit):
            assert stream == "stdout" and cursor == 0 and limit == 128
            return SimpleNamespace(data=b"hello", cursor=5, complete=True, output_complete=True)

        async def cancel(self, command_id):
            canceled.append(command_id)

    class Manager:
        async def ensure(self, _key):
            return SimpleNamespace(generation=3, commands=Commands())

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, _conversation_key):
        return True

    async def owner_provider(_conversation_key):
        return owner

    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=Operations(),
        clock=lambda: now,
    )
    headers = {"Authorization": "Bearer token"}
    identity = {
        "conversation_key": owner.conversation_key,
        "owner_epoch": 2,
        "conversation_generation": 4,
        "turn_id": "turn-7",
        "origin_seq": 8,
    }
    try:
        response = await client.post(
            "/v1/commands/cmd-1/status",
            json={**identity, "request_id": "cmd-1"},
            headers=headers,
        )
        assert (await response.json())["state"] == "running"

        response = await client.post(
            "/v1/commands/cmd-1/output",
            json={
                **identity,
                "request_id": "cmd-1",
                "cursor": 0,
                "limit_bytes": 128,
            },
            headers=headers,
        )
        assert (await response.json())["data_base64"] == "aGVsbG8="

        response = await client.post(
            "/v1/commands/cmd-1/cancel",
            json={**identity, "request_id": "cancel-1"},
            headers=headers,
        )
        assert (await response.json())["state"] == "cancel_requested"
        assert canceled == ["cmd-1"]
    finally:
        await client.close()


async def test_request_scoped_clients_close_on_success_and_failure():
    from silk_agent.sandbox.service import _call_and_close

    class Client:
        def __init__(self):
            self.closed = 0

        async def read(self, fail=False):
            if fail:
                raise OSError("response lost")
            return b"content"

        async def close(self):
            self.closed += 1

    resource = Client()
    assert await _call_and_close(resource, "read") == b"content"
    assert resource.closed == 1
    try:
        await _call_and_close(resource, "read", fail=True)
    except OSError:
        pass
    else:
        raise AssertionError("client failure must propagate")
    assert resource.closed == 2


async def test_invalid_file_path_returns_400_without_ensuring_sandbox():
    async def review(_token, _audience):
        return {
            "authenticated": True,
            "audiences": ["silk-sandbox"],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    class Manager:
        async def ensure(self, _key):
            raise AssertionError("invalid path must not reach the manager")

    client = await _client(
        review,
        manager=Manager(),
        authorizer=lambda *_: None,
        owner_provider=lambda *_: None,
        operation_store=object(),
    )
    try:
        response = await client.put(
            "/v1/files/write",
            headers={"Authorization": "Bearer test-token"},
            json={
                "request_id": "invalid",
                "conversation_key": "c",
                "owner_epoch": 1,
                "conversation_generation": 1,
                "turn_id": "t",
                "origin_seq": 1,
                "path": "/workspace/invalid",
                "data_base64": "",
            },
        )
        assert response.status == 400
    finally:
        await client.close()


async def test_command_uses_the_renewed_owner_token_after_a_slow_sandbox_start():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    before = OwnershipToken("slack:T:C:9", "w", "i", 2, 4, now + timedelta(seconds=60))
    renewed = OwnershipToken("slack:T:C:9", "w", "i", 2, 4, now + timedelta(seconds=80))
    state = {"owner": before}
    accepted_with = []

    class Manager:
        async def ensure(self, _key):
            state["owner"] = renewed  # the Worker renews its lease while the Sandbox boots
            return SimpleNamespace(generation=1, commands=SimpleNamespace(start=start, get=get))

    class Operations:
        record = None

        async def accept_intent(self, **kwargs):
            accepted_with.append(kwargs["owner"])
            self.record = {"request_hash": kwargs["request_hash"]}
            return True

        async def get_operation(self, _operation_id):
            return self.record

        async def is_dispatch_allowed(self, *_args, **_kwargs):
            return True

    async def start(*_args, **_kwargs):
        return {"state": "running"}

    async def get(_command_id):
        return {"state": "running"}

    async def review(_token, audience):
        return {
            "authenticated": True,
            "audiences": [audience],
            "user": {"username": "system:serviceaccount:silk:worker"},
        }

    async def authorize(_ksa, _key):
        return True

    async def owner_provider(_key):
        return state["owner"]

    client = await _client(
        review,
        manager=Manager(),
        authorizer=authorize,
        owner_provider=owner_provider,
        operation_store=Operations(),
        clock=lambda: now,
    )
    try:
        response = await client.post(
            "/v1/commands",
            json={
                "request_id": "r1",
                "conversation_key": "slack:T:C:9",
                "owner_epoch": 2,
                "conversation_generation": 4,
                "turn_id": "t",
                "origin_seq": 1,
                "command": "echo hi",
            },
            headers={"Authorization": "Bearer token"},
        )
        assert response.status == 202
        assert accepted_with == [renewed]
    finally:
        await client.close()
