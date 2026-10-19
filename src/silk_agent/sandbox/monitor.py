"""Lease based polling for long running Sandbox commands."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from typing import Protocol

from silk_agent.core.distributed_models import CommandExecutionState
from silk_agent.core.distributed_ports import CommandMonitorStore
from silk_agent.core.ports import CommandResult
from silk_agent.sandbox.output_archive import (
    CommandOutputArchiver,
    OutputObjectReader,
    OutputPageReader,
)


class CommandStatusReader(Protocol):
    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


CancelSender = Callable[[str, str, str], Awaitable[str | None]]


class CommandMonitor:
    """One poller instance; any instance can take over when another lease expires."""

    def __init__(
        self,
        store: CommandMonitorStore,
        runtime: CommandStatusReader,
        clock: Clock,
        *,
        monitor_id: str,
        lease_seconds: int = 30,
        retry_seconds: int = 5,
        batch_size: int = 100,
        output_archiver: CommandOutputArchiver | None = None,
        output_reader: OutputObjectReader | None = None,
        stale_after_seconds: float = 6 * 3600,
    ) -> None:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        # A command whose state could not be read, without a break, for this long is given up:
        # an idle-suspended Sandbox had no running command, a lost one has none either. Without
        # a limit its record is polled (and logged) every pass for ever. The time is counted
        # from the first failed read (stored on the record) and starts over after any good read,
        # so a long-running command that is unreadable for one moment is never given up.
        self._stale_after = timedelta(seconds=stale_after_seconds)
        if not monitor_id or lease_seconds <= 0 or retry_seconds <= 0 or batch_size <= 0:
            raise ValueError("monitor_id and positive lease/retry/batch values are required")
        self._store = store
        self._runtime = runtime
        self._clock = clock
        self._monitor_id = monitor_id
        self._lease = timedelta(seconds=lease_seconds)
        self._retry = timedelta(seconds=retry_seconds)
        self._batch_size = batch_size
        self._output_archiver = output_archiver
        self._output_reader = output_reader

    async def poll_once(self) -> int:
        """Poll due records and return the number of status requests made."""
        now = self._clock.now()
        due = await self._store.list_due(now=now, limit=self._batch_size)
        polled = 0
        for candidate in due:
            command_id = _required(candidate, "command_id")
            claim = await self._store.claim(
                command_id,
                monitor_id=self._monitor_id,
                now=now,
                lease_expires_at=now + self._lease,
            )
            if claim is None:
                continue
            polled += 1
            await self._poll_claim(command_id, claim, now)
        return polled

    async def cancel(
        self, command_id: str, *, reason: str, send_cancel: CancelSender
    ) -> Mapping[str, object] | None:
        """Persist intent first, then issue the remote cancel with a stable request ID."""
        now = self._clock.now()
        request = await self._store.request_cancel(command_id, reason=reason, now=now)
        if request is None:
            return None
        state = request.get("state")
        if state in {
            CommandExecutionState.FINISHED,
            CommandExecutionState.CANCELLED,
            CommandExecutionState.UNKNOWN,
            CommandExecutionState.FINISHED.value,
            CommandExecutionState.CANCELLED.value,
            CommandExecutionState.UNKNOWN.value,
        }:
            return request
        conversation_key = _required(request, "conversation_key")
        request_id = _required(request, "cancel_request_id")
        try:
            outcome = await send_cancel(conversation_key, command_id, request_id)
            if outcome is None:
                outcome = "accepted"
            elif outcome not in {"accepted", "unknown", "rejected"}:
                outcome = "unknown"
        except Exception:
            outcome = "unknown"
        await self._store.record_cancel_result(
            command_id,
            request_id=request_id,
            outcome=outcome,
            now=self._clock.now(),
        )
        return {**request, "cancel_outcome": outcome}

    async def list_for_stop(
        self,
        *,
        conversation_key: str,
        conversation_generation: int,
        stop_seq: int,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[tuple[Mapping[str, object], ...], str | None]:
        records, next_cursor = await self._store.list_for_stop(
            conversation_key=conversation_key,
            conversation_generation=conversation_generation,
            stop_seq=stop_seq,
            limit=limit,
            cursor=cursor,
        )
        return tuple(records), next_cursor

    async def get_command(self, command_id: str) -> Mapping[str, object] | None:
        return await self._store.get_command(command_id)

    async def detach(self, command_id: str) -> Mapping[str, object] | None:
        return await self._store.detach(command_id, now=self._clock.now())

    async def orphan(self, command_id: str) -> Mapping[str, object] | None:
        """Preserve a foreground command after its Worker no longer owns the turn."""
        return await self._store.orphan(command_id, now=self._clock.now())

    async def command_output(
        self, command_id: str, *, cursor: int, limit_bytes: int = 65536
    ) -> tuple[str, int] | None:
        """Read archived stdout then stderr using one stable byte cursor."""
        if self._output_reader is None:
            return None
        record = await self._store.get_command(command_id)
        result = record.get("result") if record is not None else None
        output = result.get("output") if isinstance(result, Mapping) else None
        if not isinstance(output, Mapping):
            return None
        stdout = output.get("stdout")
        stderr = output.get("stderr")
        if not isinstance(stdout, Mapping) or not isinstance(stderr, Mapping):
            return None
        stdout_size = stdout.get("size_bytes")
        stderr_size = stderr.get("size_bytes")
        if (
            not isinstance(stdout_size, int)
            or isinstance(stdout_size, bool)
            or not isinstance(stderr_size, int)
            or isinstance(stderr_size, bool)
            or cursor < 0
            or limit_bytes < 1
        ):
            raise ValueError("stored output metadata is invalid")
        stream_record: Mapping[str, object]
        stream_cursor: int
        # One cursor runs over stdout then stderr. An empty stdout has nothing at cursor 0, so
        # the cursor already points into stderr.
        if cursor < stdout_size:
            stream_record = stdout
            stream_cursor = cursor
            offset = 0
        else:
            stream_record = stderr
            stream_cursor = cursor - stdout_size
            offset = stdout_size
        uri = stream_record.get("uri")
        if not isinstance(uri, str):
            raise ValueError("stored output reference is invalid")
        data, next_stream_cursor, _complete = await self._output_reader.read(
            uri,
            cursor=stream_cursor,
            limit_bytes=limit_bytes,
            now=self._clock.now(),
        )
        return data.decode("utf-8", errors="replace"), offset + next_stream_cursor

    async def _note_unreadable(
        self, claim: Mapping[str, object], claim_token: str, command_id: str, now: datetime
    ) -> bool:
        """Remember when the state first became unreadable; True once it has been for too long."""
        since = claim.get("unreadable_since")
        if isinstance(since, str):
            try:
                since = datetime.fromisoformat(since)
            except ValueError:
                since = None
        if not isinstance(since, datetime) or since.tzinfo is None:
            await self._store.set_unreadable_since(command_id, claim_token=claim_token, since=now)
            return False
        return now - since > self._stale_after

    async def _poll_claim(
        self, command_id: str, claim: Mapping[str, object], now: datetime
    ) -> None:
        conversation_key = _required(claim, "conversation_key")
        claim_token = _required(claim, "monitor_lease_token")
        current = CommandExecutionState(_required(claim, "state"))
        generation = claim.get("sandbox_generation")
        status_args: dict[str, int] = (
            {"sandbox_generation": generation}
            if getattr(self._runtime, "reads_by_generation", False)
            and isinstance(generation, int)
            and not isinstance(generation, bool)
            else {}
        )
        try:
            result = await self._runtime.command_status(conversation_key, command_id, **status_args)
        except Exception:
            # The claim expires naturally; the next monitor retries the read, never start.
            return
        observed = _observed_state(result, current)
        stale = False
        if result.outcome == "unknown":
            stale = await self._note_unreadable(claim, claim_token, command_id, now)
            if stale:
                observed = CommandExecutionState.UNKNOWN
        elif claim.get("unreadable_since") is not None:
            await self._store.set_unreadable_since(
                command_id, claim_token=claim_token, since=None
            )  # readable again: the count starts over
        if observed in {
            CommandExecutionState.FINISHED,
            CommandExecutionState.CANCELLED,
            CommandExecutionState.UNKNOWN,
        }:
            result_data: dict[str, object] = {"exit_code": result.exit_code}
            if observed is CommandExecutionState.UNKNOWN:
                # Nothing left to archive. "lost" is a fact (the Sandbox is gone); an
                # unreadable command that was given up on is only recorded as such.
                result_data["sandbox_lost" if not stale else "unreadable_given_up"] = True
            elif self._output_archiver is not None:
                if not isinstance(self._runtime, OutputPageReader):
                    raise TypeError("command runtime does not support paged output reads")
                generation = claim.get("sandbox_generation")
                if (
                    not isinstance(generation, int)
                    or isinstance(generation, bool)
                    or generation < 1
                ):
                    raise ValueError("command monitor record has invalid sandbox_generation")
                try:
                    result_data["output"] = await self._output_archiver.archive(
                        self._runtime,
                        conversation_key=conversation_key,
                        command_id=command_id,
                        sandbox_generation=generation,
                    )
                except Exception:
                    # A terminal tracker result is not complete until both streams have
                    # been durably archived. Let this lease expire and retry by ID.
                    return
            await self._store.observe_terminal(
                command_id,
                claim_token=claim_token,
                expected=current,
                terminal=observed,
                result=result_data,
                now=self._clock.now(),
            )
            return
        await self._store.record_observation(
            command_id,
            claim_token=claim_token,
            expected=current,
            observed=observed,
            now=now,
            retry_at=now + self._retry,
        )


def _observed_state(
    result: CommandResult, current: CommandExecutionState
) -> CommandExecutionState | None:
    if result.outcome == "running":
        if current is CommandExecutionState.CANCEL_REQUESTED:
            return None
        return CommandExecutionState.RUNNING
    if result.outcome == "completed":
        return CommandExecutionState.FINISHED
    if result.outcome == "cancelled":
        return CommandExecutionState.CANCELLED
    if result.outcome == "lost":
        # The Sandbox that ran it is gone for good: end it, stop polling, tell the user once.
        return CommandExecutionState.UNKNOWN
    # A missing/ambiguous tracker record is not evidence of completion or permission to
    # submit another start. Preserve the ledger state and try a later status read.
    return None


def _required(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"command monitor record is missing {key}")
    return value
