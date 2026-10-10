"""ClaudeAgentEngine: one ClaudeSDKClient per conversation, on Claude via Agent Platform."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookJSONOutput,
    HookMatcher,
    MirrorErrorMessage,
    ResultMessage,
    TextBlock,
)

from silk_agent.agent.summaries import display_name, summarize
from silk_agent.core.events import (
    AssistantText,
    OutboundEvent,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
    Usage,
)
from silk_agent.core.ports import Clock, SkillGeneration, SkillSource, TurnRequest
from silk_agent.skills.sources import link_skills

log = logging.getLogger(__name__)

# Built-in tools that run on the Bot host. All of them are disabled.
HOST_TOOLS = [
    "Bash",
    "BashOutput",
    "KillShell",
    "Read",
    "Write",
    "Edit",
    "MultiEdit",
    "Glob",
    "Grep",
    "NotebookEdit",
    "WebFetch",
    "WebSearch",
    "Task",
    "TodoWrite",
]

MIRROR_WARNING = "会話履歴の一部が保存されていない可能性があります。"


@dataclass
class ClaudeEngineSettings:
    model: str
    project: str
    region: str
    work_root: Path
    system_prompt: str = ""
    max_turns: int | None = 50
    max_budget_usd: float | None = 2.0
    session_store: Any | None = None
    # (conversation_key) -> {name: McpServerConfig}; includes the sandbox SDK server.
    mcp_servers: Callable[[str], dict[str, Any]] = field(default=lambda key: {})
    # Skills: the generation current at the start of a turn is pinned for that turn.
    skills: SkillSource | None = None
    # Async callback (conversation_key, generation) when this conversation first sees a new
    # skills generation (used to copy the skills into the sandbox).
    on_skills_generation: Callable[[str, SkillGeneration], Awaitable[None]] | None = None
    on_session_id: Callable[[str, str], None] | None = None
    extra_env: dict[str, str] = field(default_factory=dict)


def vertex_env(project: str, region: str) -> dict[str, str]:
    return {
        "CLAUDE_CODE_USE_VERTEX": "1",
        "ANTHROPIC_VERTEX_PROJECT_ID": project,
        "CLOUD_ML_REGION": region,
    }


def conversation_dir(work_root: Path, key: str) -> Path:
    return work_root / hashlib.sha256(key.encode()).hexdigest()[:24]


class ClaudeAgentEngine:
    def __init__(
        self,
        key: str,
        settings: ClaudeEngineSettings,
        clock: Clock,
        *,
        client_factory: Callable[[ClaudeAgentOptions], Any] = ClaudeSDKClient,
    ) -> None:
        self._key = key
        self._s = settings
        self._clock = clock
        self._client_factory = client_factory
        self._client: Any | None = None
        self._session_id: str | None = None
        self._client_generation: str | None = None
        self._sandbox_generation: str | None = None
        self._fresh = False  # set by reset(): never resume the stored session
        self._interrupted = False
        self._queue: asyncio.Queue[Any] | None = None
        self._turn_id = ""
        self._running: dict[str, tuple[Any, str, str]] = {}  # tool_use_id -> (start, name, summary)

    # ------------------------------------------------------------ options

    def build_options(self, resume_session_id: str | None = None) -> ClaudeAgentOptions:
        cwd = conversation_dir(self._s.work_root, self._key)
        cwd.mkdir(parents=True, exist_ok=True)
        generation = self._s.skills.current() if self._s.skills is not None else None
        link_skills(cwd, generation)
        self._client_generation = generation.generation_id if generation else None

        resume = None
        if not self._fresh:
            resume = self._session_id or resume_session_id
        hooks = {
            "PreToolUse": [HookMatcher(hooks=[self._on_pre_tool])],
            "PostToolUse": [HookMatcher(hooks=[self._on_post_tool])],
            "PostToolUseFailure": [HookMatcher(hooks=[self._on_tool_failure])],
        }
        return ClaudeAgentOptions(
            model=self._s.model,
            env={**vertex_env(self._s.project, self._s.region), **self._s.extra_env},
            system_prompt=self._s.system_prompt or None,
            cwd=str(cwd),
            disallowed_tools=list(HOST_TOOLS),
            mcp_servers=self._s.mcp_servers(self._key),
            strict_mcp_config=True,
            setting_sources=["project"],
            skills="all",
            hooks=hooks,  # type: ignore[arg-type]
            session_store=self._s.session_store,
            session_store_flush="eager" if self._s.session_store is not None else "batched",
            resume=resume,
            session_id=None if resume else str(uuid.uuid4()),
            max_turns=self._s.max_turns,
            max_budget_usd=self._s.max_budget_usd,
        )

    @property
    def session_id(self) -> str | None:
        """The Claude session id of the running conversation (None before the first result)."""
        return self._session_id

    async def _ensure_client(self, resume_session_id: str | None = None) -> Any:
        if self._client is None:
            client = self._client_factory(self.build_options(resume_session_id))
            await client.connect()
            self._client = client
        return self._client

    # ------------------------------------------------------------ hooks

    async def _emit(self, event: OutboundEvent) -> None:
        if self._queue is not None:
            self._queue.put_nowait(event)

    async def _on_pre_tool(
        self, hook_input: Any, tool_use_id: str | None, context: Any
    ) -> HookJSONOutput:
        tool_id = tool_use_id or hook_input.get("tool_use_id", "")
        name = display_name(hook_input["tool_name"])
        summary = summarize(hook_input["tool_name"], hook_input.get("tool_input") or {})
        started = self._clock.now()
        self._running[tool_id] = (started, name, summary)
        await self._emit(ToolStarted(self._key, self._turn_id, tool_id, name, summary, started))
        return {}

    async def _sync_skills(self) -> None:
        """Pin the current skills generation for this turn. A new generation reconnects the
        client (resuming the same session) so the CLI re-reads the skills directory."""
        skills = self._s.skills
        if skills is None:
            return
        generation = skills.current()
        gen_id = generation.generation_id if generation else None
        if self._client is not None and gen_id != self._client_generation:
            await self._drop_client()
        if (
            generation is not None
            and gen_id != self._sandbox_generation
            and self._s.on_skills_generation is not None
        ):
            await self._s.on_skills_generation(self._key, generation)
            self._sandbox_generation = gen_id

    async def _finish_tool(self, tool_id: str, status: str) -> None:
        started, name, summary = self._running.pop(tool_id, (self._clock.now(), "", ""))
        finished = self._clock.now()
        await self._emit(
            ToolFinished(
                self._key,
                self._turn_id,
                tool_id,
                status,  # type: ignore[arg-type]
                summary,
                finished,
                (finished - started).total_seconds(),
            )
        )

    async def _on_post_tool(
        self, hook_input: Any, tool_use_id: str | None, context: Any
    ) -> HookJSONOutput:
        await self._finish_tool(tool_use_id or hook_input.get("tool_use_id", ""), "success")
        return {}

    async def _on_tool_failure(
        self, hook_input: Any, tool_use_id: str | None, context: Any
    ) -> HookJSONOutput:
        status = "interrupted" if hook_input.get("is_interrupt") else "failure"
        await self._finish_tool(tool_use_id or hook_input.get("tool_use_id", ""), status)
        return {}

    # ------------------------------------------------------------ run

    @staticmethod
    def compose_prompt(request: TurnRequest) -> str:
        notices = "".join(f"<system-notice>{n}</system-notice>\n" for n in request.notices)
        return notices + request.text

    async def run_turn(self, request: TurnRequest) -> AsyncIterator[OutboundEvent]:
        self._turn_id = request.turn_id
        self._interrupted = False
        self._running.clear()
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._queue = queue
        warnings: list[str] = []
        pump: asyncio.Task[None] | None = None
        try:
            await self._sync_skills()
            client = await self._ensure_client(request.resume_session_id)
            await client.query(self.compose_prompt(request))
            pump = asyncio.create_task(self._pump(client, queue))
            while True:
                item = await queue.get()
                if item is _DONE:
                    break
                if isinstance(item, BaseException):
                    raise item
                if isinstance(item, MirrorErrorMessage):
                    if MIRROR_WARNING not in warnings:
                        warnings.append(MIRROR_WARNING)
                    continue
                if isinstance(item, ResultMessage):
                    for event in await self._on_result(item, request, warnings):
                        yield event
                    continue
                if isinstance(item, AssistantMessage):
                    if item.parent_tool_use_id:
                        continue  # subagent text is not shown
                    for block in item.content:
                        if isinstance(block, TextBlock) and block.text.strip():
                            yield AssistantText(self._key, request.turn_id, block.text)
                    continue
                yield item  # events produced by hooks
        except asyncio.CancelledError:
            await self._abort()
            raise
        except Exception as e:
            log.exception("claude turn failed for %s", self._key)
            await self._drop_client()
            yield TurnFailed(self._key, request.turn_id, f"{type(e).__name__}: {e}")
        finally:
            if pump is not None and not pump.done():
                pump.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await pump
            self._queue = None
            self._running.clear()

    async def _pump(self, client: Any, queue: asyncio.Queue[Any]) -> None:
        try:
            async for message in client.receive_response():
                queue.put_nowait(message)
        except Exception as e:
            queue.put_nowait(e)
        finally:
            queue.put_nowait(_DONE)

    async def _on_result(
        self, result: ResultMessage, request: TurnRequest, warnings: list[str]
    ) -> list[OutboundEvent]:
        events: list[OutboundEvent] = []
        # Tools still marked running at the result were cancelled (e.g. by interrupt).
        for tool_id in list(self._running):
            started, _, summary = self._running.pop(tool_id)
            finished = self._clock.now()
            events.append(
                ToolFinished(
                    self._key,
                    request.turn_id,
                    tool_id,
                    "interrupted",
                    summary,
                    finished,
                    (finished - started).total_seconds(),
                )
            )
        if result.session_id:
            self._session_id = result.session_id
            if self._s.on_session_id is not None:
                self._s.on_session_id(self._key, result.session_id)
        if self._interrupted:
            return events  # the caller reports TurnInterrupted
        if result.is_error:
            detail = "; ".join(result.errors or []) or result.result or result.subtype
            events.append(TurnFailed(self._key, request.turn_id, detail))
        else:
            usage = result.usage or {}
            events.append(
                TurnFinished(
                    self._key,
                    request.turn_id,
                    result.result or "",
                    Usage(
                        int(usage.get("input_tokens", 0) or 0),
                        int(usage.get("output_tokens", 0) or 0),
                        result.total_cost_usd,
                    ),
                    tuple(warnings),
                    self._clock.now(),
                )
            )
        return events

    # ------------------------------------------------------------ control

    async def interrupt(self) -> None:
        """Stop the running turn. The reader loop in run_turn drains the stream until
        the ResultMessage, so the next query() starts on a clean connection."""
        self._interrupted = True
        if self._client is not None:
            await self._client.interrupt()

    async def reset(self) -> None:
        await self._drop_client()
        self._session_id = None
        self._fresh = True

    async def close(self) -> None:
        await self._drop_client()

    async def _abort(self) -> None:
        with contextlib.suppress(Exception):
            if self._client is not None:
                await self._client.interrupt()
        await self._drop_client()

    async def _drop_client(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect()


_DONE = object()
