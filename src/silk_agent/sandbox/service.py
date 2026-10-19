"""Authenticated internal HTTP service for remote Sandbox operations."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import PurePosixPath
from time import perf_counter
from typing import Any, cast

from aiohttp import web
from kubernetes_asyncio import client

from gke_session_sandbox.errors import FileOperationError
from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import ServiceOperationStore
from silk_agent.sandbox.protocol import (
    CommandControlRequest,
    CommandControlResponse,
    CommandOutputRequest,
    CommandOutputResponse,
    CommandStartRequest,
    CommandStartResponse,
    CommandStartState,
    CommandStatusRequest,
    CommandStatusResponse,
    FileEntryResponse,
    FileListRequest,
    FileListResponse,
    FileReadRequest,
    FileReadResponse,
    FileWriteRequest,
    FileWriteResponse,
    SkillsSyncRequest,
    SkillsSyncResponse,
)

logger = logging.getLogger(__name__)

TokenReview = Callable[[str, str], Awaitable[Mapping[str, Any]]]
ConversationAuthorizer = Callable[[str, str], Awaitable[bool]]
OwnerProvider = Callable[[str], Awaitable[OwnershipToken | None]]
READY_KEY = web.AppKey("ready", object)
MANAGER_KEY = web.AppKey("manager", object)
AUTHORIZER_KEY = web.AppKey("conversation_authorizer", object)
OWNER_PROVIDER_KEY = web.AppKey("owner_provider", object)
OPERATION_STORE_KEY = web.AppKey("operation_store", object)
ENV_PROVIDER_KEY = web.AppKey("env_provider", object)
CLOCK_KEY = web.AppKey("clock", object)
METRICS_KEY = web.AppKey("request_metrics", dict)
MAX_FILE_BYTES = 8 * 1024 * 1024
REQUEST_ID_KEY = web.RequestKey("request_id", str)
CALLER_KSA_KEY = web.RequestKey("caller_ksa", str)


class ServiceError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


def _utc_now() -> datetime:
    return datetime.now(UTC)


def create_app(
    *,
    audience: str,
    allowed_ksas: Sequence[str],
    token_review: TokenReview | None = None,
    ready: Callable[[], bool] | None = None,
    manager: Any | None = None,
    conversation_authorizer: ConversationAuthorizer | None = None,
    owner_provider: OwnerProvider | None = None,
    operation_store: ServiceOperationStore | None = None,
    env_provider: Callable[[str], Awaitable[Mapping[str, str]]] | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> web.Application:
    """Build a service app; injected review/readiness callbacks keep tests isolated."""
    if not audience.strip():
        raise ValueError("TokenReview audience must not be blank")
    allowlist = frozenset(item for item in allowed_ksas if item.strip())
    if not allowlist:
        raise ValueError("at least one allowed KSA must be configured")
    review = token_review or _token_review

    @web.middleware
    async def errors_and_auth(request: web.Request, handler):
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request[REQUEST_ID_KEY] = request_id
        started = perf_counter()
        response: web.StreamResponse
        try:
            if request.path not in {"/healthz", "/readyz", "/metrics"}:
                await _authenticate(request, review, audience, allowlist)
            response = await handler(request)
            response.headers["X-Request-ID"] = request_id
        except ServiceError as exc:
            response = web.json_response(
                {"error": {"code": exc.code, "message": str(exc)}, "request_id": request_id},
                status=exc.status,
                headers={"X-Request-ID": request_id},
            )
        except web.HTTPException as exc:
            response = web.json_response(
                {
                    "error": {"code": "http_error", "message": exc.reason},
                    "request_id": request_id,
                },
                status=exc.status,
                headers={"X-Request-ID": request_id},
            )
        except Exception as exc:
            logger.error(
                "sandbox service request failed",
                extra={"request_id": request_id, "error_type": type(exc).__name__},
            )
            response = web.json_response(
                {
                    "error": {"code": "internal_error", "message": "request failed"},
                    "request_id": request_id,
                },
                status=500,
                headers={"X-Request-ID": request_id},
            )
        elapsed = perf_counter() - started
        method = request.method
        status = response.status
        metrics = request.app[METRICS_KEY]
        key = (method, status)
        count, total_seconds = metrics.get(key, (0, 0.0))
        metrics[key] = (count + 1, total_seconds + elapsed)
        route = getattr(request.match_info.route.resource, "canonical", "<unmatched>")
        logger.info(
            "sandbox service request",
            extra={
                "request_id": request_id,
                "method": method,
                "route": route,
                "status": status,
                "duration_ms": round(elapsed * 1000, 2),
            },
        )
        return response

    app = web.Application(middlewares=[errors_and_auth], client_max_size=12 * 1024 * 1024)
    app[READY_KEY] = ready or (lambda: True)
    app[METRICS_KEY] = {}
    if manager is not None:
        app[MANAGER_KEY] = manager
    if conversation_authorizer is not None:
        app[AUTHORIZER_KEY] = conversation_authorizer
    if owner_provider is not None:
        app[OWNER_PROVIDER_KEY] = owner_provider
    if operation_store is not None:
        app[OPERATION_STORE_KEY] = operation_store
    if env_provider is not None:
        app[ENV_PROVIDER_KEY] = env_provider
    app[CLOCK_KEY] = clock
    app.router.add_get("/healthz", _health)
    app.router.add_get("/readyz", _readiness)
    app.router.add_get("/metrics", _metrics)
    if manager is not None:
        app.router.add_post("/v1/sessions/ensure", _ensure_session)
        app.router.add_get("/v1/sessions/status", _session_status)
        app.router.add_get("/v1/sessions/notices", _session_notices)
        app.router.add_post("/v1/sessions/notices/ack", _ack_notice)
        if owner_provider is not None:
            app.router.add_post("/v1/files/read", _read_file)
            app.router.add_post("/v1/files/list", _list_files)
        if owner_provider is not None and operation_store is not None:
            app.router.add_post("/v1/commands", _start_command)
            app.router.add_post("/v1/commands/{command_id}/status", _command_status)
            app.router.add_post("/v1/commands/{command_id}/output", _command_output)
            app.router.add_post("/v1/commands/{command_id}/cancel", _cancel_command)
            app.router.add_put("/v1/files/write", _write_file)
            app.router.add_put("/v1/skills/sync", _sync_skills)
    return app


async def _health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def _metrics(request: web.Request) -> web.Response:
    """Expose bounded-cardinality request totals without conversation labels."""
    metrics = request.app[METRICS_KEY]
    lines = [
        "# HELP silk_agent_sandbox_http_requests_total HTTP requests handled by status.",
        "# TYPE silk_agent_sandbox_http_requests_total counter",
        "# HELP silk_agent_sandbox_http_request_duration_seconds_sum Total request time.",
        "# TYPE silk_agent_sandbox_http_request_duration_seconds_sum counter",
    ]
    for (method, status), (count, duration) in sorted(metrics.items()):
        labels = f'method="{method}",status="{status}"'
        lines.append(f"silk_agent_sandbox_http_requests_total{{{labels}}} {count}")
        lines.append(
            f"silk_agent_sandbox_http_request_duration_seconds_sum{{{labels}}} {duration:.6f}"
        )
    return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")


async def _readiness(request: web.Request) -> web.Response:
    if not cast(Callable[[], bool], request.app[READY_KEY])():
        raise ServiceError(503, "not_ready", "service dependencies are not ready")
    return web.json_response({"status": "ready"})


async def _ensure_session(request: web.Request) -> web.Response:
    body = await _json_body(request)
    conversation_key = _required_string(body, "conversation_key")
    await _authorize_conversation(request, conversation_key)
    handle = await _ensure_handle(request, conversation_key)
    return web.json_response(_session_payload(handle))


async def _session_status(request: web.Request) -> web.Response:
    conversation_key = _required_string(request.query, "conversation_key")
    await _authorize_conversation(request, conversation_key)
    key = _session_key(conversation_key)
    manager = _service_manager(request)
    handle = await manager.get(key)
    if handle is None:
        return web.json_response({"exists": False, "ready": False, "state": "missing"})
    payload = _session_payload(handle)
    return web.json_response({**payload, "exists": True})


async def _session_notices(request: web.Request) -> web.Response:
    conversation_key = _required_string(request.query, "conversation_key")
    await _authorize_conversation(request, conversation_key)
    key = _session_key(conversation_key)
    events = await _service_manager(request).pending_context_events(key)
    return web.json_response(
        {
            "notices": [
                {
                    "event_id": event.event_id,
                    "generation": event.generation,
                    "event_type": event.event_type.value,
                    "context_message": event.context_message,
                }
                for event in events
            ]
        }
    )


async def _ack_notice(request: web.Request) -> web.Response:
    body = await _json_body(request)
    conversation_key = _required_string(body, "conversation_key")
    await _authorize_conversation(request, conversation_key)
    key = _session_key(conversation_key)
    event_id = _required_string(body, "event_id")
    acknowledged = await _service_manager(request).ack_context_event(key, event_id)
    return web.json_response({"acknowledged": acknowledged})


async def _start_command(request: web.Request) -> web.Response:
    data = await _json_body(request)
    try:
        command = CommandStartRequest.model_validate(data)
    except ValueError as exc:
        raise ServiceError(400, "invalid_request", "command request is invalid") from exc
    await _authorize_conversation(request, command.conversation_key)
    await _current_owner(request, command)  # fail fast before starting anything

    handle = await _ensure_handle(request, command.conversation_key)
    if handle.generation < 1:
        raise ServiceError(409, "sandbox_unavailable", "Sandbox generation is unavailable")
    # A cold start can outlast a lease renewal; use the token that is current now.
    owner = await _current_owner(request, command)
    operation_store = cast(ServiceOperationStore, request.app[OPERATION_STORE_KEY])
    request_hash = _command_request_hash(command)
    now = cast(Callable[[], datetime], request.app[CLOCK_KEY])()
    accepted = await operation_store.accept_intent(
        operation_id=command.request_id,
        operation_kind="command_start",
        request_hash=request_hash,
        conversation_key=command.conversation_key,
        conversation_generation=command.conversation_generation,
        sandbox_generation=handle.generation,
        sandbox_key_hash=_session_key(command.conversation_key).stable_hash(),
        turn_id=command.turn_id,
        origin_seq=command.origin_seq,
        owner=owner,
        payload={
            "command": command.command,
            "cwd": command.cwd,
            "timeout_seconds": command.timeout_seconds,
        },
        now=now,
        created_at=now,
    )
    if not accepted:
        existing = await operation_store.get_operation(command.request_id)
        if existing is None:
            raise ServiceError(409, "intent_rejected", "command intent was not accepted")
        if existing.get("request_hash") != request_hash:
            raise ServiceError(
                409, "request_id_conflict", "request ID was used with another payload"
            )
        response_state = await _existing_command_state(handle, command.request_id)
        return web.json_response(
            CommandStartResponse(
                request_id=command.request_id,
                command_id=command.request_id,
                state=response_state,
                sandbox_generation=handle.generation,
            ).model_dump(mode="json"),
            status=202,
        )

    if not await operation_store.is_dispatch_allowed(command.request_id, owner, now=now):
        raise ServiceError(409, "intent_stopped", "command is behind an owner or stop barrier")

    try:
        result = await _call_and_close(
            handle.commands,
            "start",
            ["bash", "-lc", command.command],
            command_id=command.request_id,
            cwd=command.cwd,
            timeout_seconds=command.timeout_seconds,
        )
    except Exception as exc:
        if type(exc).__name__ == "ExecutionOutcomeUnknown":
            response_state = await _existing_command_state(handle, command.request_id)
            return web.json_response(
                CommandStartResponse(
                    request_id=command.request_id,
                    command_id=command.request_id,
                    state=response_state,
                    sandbox_generation=handle.generation,
                ).model_dump(mode="json"),
                status=202,
            )
        raise
    record_state = result.get("state") if isinstance(result, Mapping) else None
    state = record_state if record_state in {"running", "finished", "cancelled"} else "accepted"
    return web.json_response(
        CommandStartResponse(
            request_id=command.request_id,
            command_id=command.request_id,
            state=state,
            sandbox_generation=handle.generation,
        ).model_dump(mode="json"),
        status=202,
    )


async def _command_status(request: web.Request) -> web.Response:
    command_request = _validate_request(CommandStatusRequest, await _json_body(request))
    command_id = request.match_info["command_id"]
    if command_request.request_id != command_id:
        raise ServiceError(400, "invalid_request", "request_id must match command_id")
    handle, operation = await _command_handle(request, command_request, command_id)
    try:
        record = await _call_and_close(handle.commands, "get", command_id)
    except Exception:
        record = {"state": "unknown"}
    raw_state = record.get("state") if isinstance(record, Mapping) else None
    # "reserved": the tracker took the command and has not reported the process yet (a command
    # that has just been started). It is on its way, not of unknown outcome.
    if raw_state in {"running", "reserved"}:
        state = "running"
    elif raw_state == "finished":
        state = "cancelled" if record.get("stop_reason") == "cancelled" else "finished"
    else:
        state = "unknown"
    response = CommandStatusResponse(
        request_id=command_id,
        command_id=command_id,
        state=state,
        sandbox_generation=int(operation["sandbox_generation"]),
        exit_code=record.get("exit_code") if isinstance(record, Mapping) else None,
        stop_reason=record.get("stop_reason") if isinstance(record, Mapping) else None,
    )
    return web.json_response(response.model_dump(mode="json"))


async def _command_output(request: web.Request) -> web.Response:
    output_request = _validate_request(CommandOutputRequest, await _json_body(request))
    command_id = request.match_info["command_id"]
    if output_request.request_id != command_id:
        raise ServiceError(400, "invalid_request", "request_id must match command_id")
    handle, operation = await _command_handle(request, output_request, command_id)
    page = await _call_and_close(
        handle.commands,
        "read_output",
        command_id,
        stream=output_request.stream,
        cursor=output_request.cursor,
        limit=output_request.limit_bytes,
    )
    response = CommandOutputResponse(
        request_id=command_id,
        command_id=command_id,
        sandbox_generation=int(operation["sandbox_generation"]),
        data_base64=base64.b64encode(page.data).decode("ascii"),
        next_cursor=page.cursor,
        complete=page.complete,
        truncated=page.complete and not page.output_complete,
    )
    return web.json_response(response.model_dump(mode="json"))


async def _cancel_command(request: web.Request) -> web.Response:
    cancel_request = _validate_request(CommandControlRequest, await _json_body(request))
    command_id = request.match_info["command_id"]
    handle, _operation = await _command_handle(request, cancel_request, command_id)
    try:
        await _call_and_close(handle.commands, "cancel", command_id)
        response_state = "cancel_requested"
    except Exception:
        # A second stop path (the turn's own cancel) may already have ended the command; the
        # tracker then rejects the repeat. A finished command is stopped, not unknown.
        response_state = "unknown"
        try:
            record = await _call_and_close(handle.commands, "get", command_id)
            if isinstance(record, Mapping) and record.get("state") == "finished":
                response_state = "cancelled"
        except Exception:
            pass
    response = CommandControlResponse(
        request_id=cancel_request.request_id,
        command_id=command_id,
        state=response_state,
        sandbox_generation=handle.generation,
    )
    return web.json_response(response.model_dump(mode="json"), status=202)


async def _command_handle(
    request: web.Request, identity: Any, command_id: str
) -> tuple[Any, Mapping[str, Any]]:
    handle, _owner = await _owned_handle(request, identity)
    store = cast(ServiceOperationStore, request.app[OPERATION_STORE_KEY])
    operation = await store.get_operation(command_id)
    if operation is None or operation.get("operation_kind") != "command_start":
        raise ServiceError(404, "unknown_command", "command operation was not found")
    # Status, output and cancel belong to the conversation and Sandbox generation, not to the
    # turn that started the command: after a Worker change the new owner has another turn_id and
    # origin_seq, and must still be able to read and cancel the commands it took over. The
    # start (turn_id, origin_seq) is fenced by the stop barrier; these are not starts.
    for field in ("conversation_key", "conversation_generation"):
        if operation.get(field) != getattr(identity, field):
            raise ServiceError(409, "command_identity_mismatch", "command identity does not match")
    if operation.get("sandbox_generation") != handle.generation:
        raise ServiceError(409, "stale_sandbox", "command belongs to another Sandbox generation")
    return handle, operation


async def _current_owner(request: web.Request, identity: Any) -> OwnershipToken:
    owner = await cast(OwnerProvider, request.app[OWNER_PROVIDER_KEY])(identity.conversation_key)
    now = cast(Callable[[], datetime], request.app[CLOCK_KEY])()
    if (
        owner is None
        or owner.epoch != identity.owner_epoch
        or owner.conversation_generation != identity.conversation_generation
        or owner.conversation_key != identity.conversation_key
        or owner.expires_at <= now
    ):
        raise ServiceError(409, "stale_owner", "operation owner token is stale")
    return owner


async def _read_file(request: web.Request) -> web.Response:
    file_request = _validate_request(FileReadRequest, await _json_body(request))
    handle, _owner = await _owned_handle(request, file_request)
    try:
        data = await _call_and_close(handle.files, "read", file_request.path)
    except FileNotFoundError as exc:
        raise ServiceError(404, "not_found", "file does not exist") from exc
    except FileOperationError as exc:
        # sandboxd answers 404 for a missing file (the real client never raises FileNotFoundError).
        if exc.status != 404:
            raise
        raise ServiceError(404, "not_found", "file does not exist") from exc
    if len(data) > MAX_FILE_BYTES:
        raise ServiceError(413, "file_too_large", "file exceeds the service read limit")
    response = FileReadResponse(
        request_id=file_request.request_id,
        sandbox_generation=handle.generation,
        data_base64=base64.b64encode(data).decode("ascii"),
    )
    return web.json_response(response.model_dump(mode="json"))


async def _list_files(request: web.Request) -> web.Response:
    list_request = _validate_request(FileListRequest, await _json_body(request))
    handle, _owner = await _owned_handle(request, list_request)
    entries = await _call_and_close(handle.files, "list", list_request.path)
    if len(entries) > 1000:
        raise ServiceError(413, "listing_too_large", "directory contains too many entries")
    response = FileListResponse(
        request_id=list_request.request_id,
        sandbox_generation=handle.generation,
        entries=[
            FileEntryResponse(
                path=str(getattr(item, "path", "")),
                is_dir=getattr(item, "type", "") == "directory",
                size=int(getattr(item, "size", 0) or 0),
            )
            for item in entries
        ],
    )
    return web.json_response(response.model_dump(mode="json"))


async def _write_file(request: web.Request) -> web.Response:
    file_request = _validate_request(FileWriteRequest, await _json_body(request))
    data = _decode_content(file_request.data_base64)
    handle, owner = await _owned_handle(request, file_request)
    digest = _request_hash(file_request)
    accepted = await _accept_mutation(
        request,
        file_request,
        owner,
        sandbox_generation=handle.generation,
        operation_kind="file_write",
        request_hash=digest,
        payload={"path": file_request.path, "content_sha256": hashlib.sha256(data).hexdigest()},
    )
    if not accepted:
        return web.json_response(
            FileWriteResponse(
                request_id=file_request.request_id,
                sandbox_generation=handle.generation,
                state="unknown",
            ).model_dump(mode="json"),
            status=202,
        )
    await _call_and_close(handle.files, "write", file_request.path, data)
    response = FileWriteResponse(
        request_id=file_request.request_id,
        sandbox_generation=handle.generation,
        state="written",
    )
    return web.json_response(response.model_dump(mode="json"), status=202)


async def _sync_skills(request: web.Request) -> web.Response:
    sync_request = _validate_request(SkillsSyncRequest, await _json_body(request))
    if len(sync_request.files) > 1000:
        raise ServiceError(413, "manifest_too_large", "Skills manifest contains too many files")
    decoded: list[tuple[str, bytes]] = []
    total_bytes = 0
    for relative_path, encoded in sorted(sync_request.files.items()):
        _validate_skill_path(relative_path)
        content = _decode_content(encoded)
        total_bytes += len(content)
        if total_bytes > MAX_FILE_BYTES:
            raise ServiceError(413, "skills_too_large", "Skills payload exceeds the size limit")
        decoded.append((relative_path, content))

    handle, owner = await _owned_handle(request, sync_request)
    accepted = await _accept_mutation(
        request,
        sync_request,
        owner,
        sandbox_generation=handle.generation,
        operation_kind="skills_sync",
        request_hash=_request_hash(sync_request),
        payload={
            "skills_generation": sync_request.skills_generation,
            "file_count": len(decoded),
        },
    )
    if not accepted:
        raise ServiceError(409, "operation_uncertain", "Skills sync intent already exists")

    failed_paths: list[str] = []
    for relative_path, content in decoded:
        try:
            await _call_and_close(handle.files, "write", f".skills/{relative_path}", content)
        except Exception:
            failed_paths.append(relative_path)
    if not failed_paths:
        failed_paths += await _remove_stale_skill_files(handle, {path for path, _ in decoded})
    response = SkillsSyncResponse(
        request_id=sync_request.request_id,
        sandbox_generation=handle.generation,
        skills_generation=sync_request.skills_generation,
        complete=not failed_paths,
        failed_paths=failed_paths,
    )
    return web.json_response(
        response.model_dump(mode="json"), status=200 if not failed_paths else 207
    )


async def _remove_stale_skill_files(handle: Any, wanted: set[str]) -> list[str]:
    """Delete what an earlier generation left in `.skills/` and this one no longer has.

    Returns the paths that could not be removed (a leftover file is a failed sync, not a silent
    difference between what the Worker holds and what the Sandbox runs)."""
    failed: list[str] = []
    pending = [".skills"]
    while pending:
        directory = pending.pop()
        try:
            entries = await _call_and_close(handle.files, "list", directory)
        except FileOperationError as exc:
            if directory == ".skills" and exc.status == 404:
                return []  # nothing was ever written here
            failed.append(directory.removeprefix(".skills/"))
            continue
        except Exception:
            failed.append(directory.removeprefix(".skills/"))
            continue
        for entry in entries:
            path = str(entry.path)
            relative = path.removeprefix(".skills/")
            if entry.type == "directory":
                pending.append(path)
            elif relative not in wanted:
                try:
                    await _call_and_close(handle.files, "delete", path)
                except Exception:
                    failed.append(relative)
    return failed


async def _owned_handle(request: web.Request, identity: Any) -> tuple[Any, OwnershipToken]:
    await _authorize_conversation(request, identity.conversation_key)
    await _current_owner(request, identity)  # fail fast before starting anything
    handle = await _ensure_handle(request, identity.conversation_key)
    # ensure() can take a minute on a cold start, during which the Worker renews its lease;
    # operate with the token that is current now, still for the same epoch and generation.
    owner = await _current_owner(request, identity)
    return handle, owner


async def _accept_mutation(
    request: web.Request,
    identity: Any,
    owner: OwnershipToken,
    *,
    sandbox_generation: int,
    operation_kind: str,
    request_hash: str,
    payload: Mapping[str, Any],
) -> bool:
    now = cast(Callable[[], datetime], request.app[CLOCK_KEY])()
    store = cast(ServiceOperationStore, request.app[OPERATION_STORE_KEY])
    accepted = await store.accept_intent(
        operation_id=identity.request_id,
        operation_kind=operation_kind,
        request_hash=request_hash,
        conversation_key=identity.conversation_key,
        conversation_generation=identity.conversation_generation,
        sandbox_generation=sandbox_generation,
        sandbox_key_hash=_session_key(identity.conversation_key).stable_hash(),
        turn_id=identity.turn_id,
        origin_seq=identity.origin_seq,
        owner=owner,
        payload=payload,
        now=now,
        created_at=now,
    )
    if accepted:
        if not await store.is_dispatch_allowed(identity.request_id, owner, now=now):
            raise ServiceError(
                409, "intent_stopped", "operation is behind an owner or stop barrier"
            )
        return True
    existing = await store.get_operation(identity.request_id)
    if existing is None:
        raise ServiceError(409, "intent_rejected", "operation intent was not accepted")
    if existing.get("request_hash") != request_hash:
        raise ServiceError(409, "request_id_conflict", "request ID was used with another payload")
    return False


def _validate_request(model: Any, data: Mapping[str, Any]) -> Any:
    try:
        return model.model_validate(data)
    except ValueError as exc:
        raise ServiceError(400, "invalid_request", "request is invalid") from exc


def _request_hash(model: Any) -> str:
    payload = model.model_dump(mode="json", exclude={"request_id"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _decode_content(encoded: str) -> bytes:
    try:
        value = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ServiceError(400, "invalid_content", "file content must be base64") from exc
    if len(value) > MAX_FILE_BYTES:
        raise ServiceError(413, "file_too_large", "file exceeds the service size limit")
    return value


def _validate_skill_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or value == ".session-runtime"
        or value.startswith(".session-runtime/")
    ):
        raise ServiceError(400, "invalid_path", "Skills paths must stay inside the Skills root")


def _command_request_hash(command: CommandStartRequest) -> str:
    payload = command.model_dump(mode="json", exclude={"request_id"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


async def _existing_command_state(handle: Any, command_id: str) -> CommandStartState:
    try:
        record = await _call_and_close(handle.commands, "get", command_id)
    except Exception:
        # A 404 may mean the start was not committed, or that the tracker is
        # between recovery states. Never turn that uncertainty into a resend.
        return "unknown"
    state = record.get("state") if isinstance(record, Mapping) else None
    if state == "reserved":
        return "accepted"  # taken by the tracker, not running yet
    if state == "running":
        return "running"
    if state == "finished":
        return "finished"
    if state == "cancelled":
        return "cancelled"
    return "unknown"


async def _json_body(request: web.Request) -> Mapping[str, Any]:
    try:
        value = await request.json()
    except (ValueError, web.HTTPException) as exc:
        raise ServiceError(400, "invalid_request", "request body must be valid JSON") from exc
    if not isinstance(value, Mapping):
        raise ServiceError(400, "invalid_request", "request body must be an object")
    return value


def _required_string(body: Mapping[str, Any], name: str) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ServiceError(400, "invalid_request", f"{name} is required")
    return value


def _session_key(conversation_key: str):
    from gke_session_sandbox import SessionKey

    scheme, _, rest = conversation_key.partition(":")
    if scheme == "slack":
        tenant, _, session = rest.partition(":")
        return SessionKey(tenant_id=tenant or "slack", session_id=session or "-")
    return SessionKey(tenant_id=scheme, session_id=rest or "-")


def _session_payload(handle: Any) -> dict[str, Any]:
    record = handle.record
    return {
        "generation": handle.generation,
        "state": handle.state.value,
        "ready": handle.state.value == "ready",
        "sandbox_generation": record.generation,
    }


async def _ensure_handle(request: web.Request, conversation_key: str) -> Any:
    """ensure() the Sandbox with the environment of the conversation's owner.

    `AGENT_USER_EMAIL` is the e-mail of the user who started the conversation (saved before
    the first ensure and never changed), so every ensure, including a resume after somebody
    else wrote in the thread, passes the same value.
    """
    provider = request.app.get(ENV_PROVIDER_KEY)
    env: Mapping[str, str] = {}
    if provider is not None:
        env = await cast(Callable[[str], Awaitable[Mapping[str, str]]], provider)(conversation_key)
    manager = _service_manager(request)
    key = _session_key(conversation_key)
    # `new_generation_env`: Sandboxes created before the owner's e-mail was passed have no
    # creation environment and keep working as they are; the e-mail applies to every
    # generation created from now on.
    return await (manager.ensure(key, new_generation_env=dict(env)) if env else manager.ensure(key))


def _service_manager(request: web.Request) -> Any:
    return cast(Any, request.app[MANAGER_KEY])


async def _authorize_conversation(request: web.Request, conversation_key: str) -> None:
    authorizer = request.app.get(AUTHORIZER_KEY)
    if authorizer is None:
        raise ServiceError(
            503, "authorization_unavailable", "conversation ownership is unavailable"
        )
    authorized = await cast(ConversationAuthorizer, authorizer)(
        request[CALLER_KSA_KEY], conversation_key
    )
    if not authorized:
        raise ServiceError(403, "forbidden", "caller does not own this conversation")


async def _authenticate(
    request: web.Request,
    token_review: TokenReview,
    audience: str,
    allowed_ksas: frozenset[str],
) -> None:
    authorization = request.headers.get("Authorization", "")
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        raise ServiceError(401, "unauthenticated", "a projected bearer token is required")
    try:
        result = await token_review(token.strip(), audience)
    except Exception as exc:
        # TokenReview errors are denied without including request credentials.
        logger.warning("Kubernetes TokenReview failed: %s", type(exc).__name__)
        raise ServiceError(
            503, "authentication_unavailable", "authentication is unavailable"
        ) from None
    status = result.get("status", result)
    if not isinstance(status, Mapping) or status.get("authenticated") is not True:
        raise ServiceError(401, "unauthenticated", "projected token is invalid or expired")
    audiences = status.get("audiences")
    username = (
        (status.get("user") or {}).get("username")
        if isinstance(status.get("user"), Mapping)
        else None
    )
    if not isinstance(audiences, Sequence) or isinstance(audiences, (str, bytes)):
        raise ServiceError(401, "unauthenticated", "projected token has no verified audience")
    if audience not in audiences:
        raise ServiceError(401, "unauthenticated", "projected token audience is not accepted")
    if not isinstance(username, str) or username not in allowed_ksas:
        raise ServiceError(403, "forbidden", "calling service account is not allowed")
    request[CALLER_KSA_KEY] = username


async def _token_review(token: str, audience: str) -> Mapping[str, Any]:
    """Validate a projected token with the Kubernetes API server."""
    configuration = client.Configuration.get_default_copy()
    api_client = client.ApiClient(configuration)
    api = client.AuthenticationV1Api(api_client)
    try:
        response = await api.create_token_review(
            client.V1TokenReview(spec=client.V1TokenReviewSpec(token=token, audiences=[audience]))
        )
        status = response.status
        return {
            "status": {
                "authenticated": status.authenticated,
                "audiences": status.audiences or [],
                "user": {"username": status.user.username if status.user else None},
            }
        }
    finally:
        await api_client.close()


async def _call_and_close(resource: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    """SessionHandle creates a new HTTP client per property access; always close it."""
    try:
        return await getattr(resource, method)(*args, **kwargs)
    finally:
        close = getattr(resource, "close", None)
        if close is not None:
            await close()
