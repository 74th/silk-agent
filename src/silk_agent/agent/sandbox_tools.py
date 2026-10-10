"""SDK MCP tools that run in the conversation's sandbox (replacing the built-in tools)."""

from __future__ import annotations

import logging
import shlex
from typing import Any

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server, tool

from silk_agent.core.commands import CommandTracker
from silk_agent.core.ports import CommandResult, SandboxRuntime

log = logging.getLogger(__name__)

SERVER_NAME = "sandbox"
OUTPUT_LIMIT = 30_000
DEFAULT_TIMEOUT = 120.0
READ_DEFAULT_LIMIT = 2000


def _text(text: str, *, error: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {"content": [{"type": "text", "text": text}]}
    if error:
        result["is_error"] = True
    return result


def _clip(text: str, limit: int = OUTPUT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…（{len(text) - limit} 文字を省略）"


def format_command(result: CommandResult) -> dict[str, Any]:
    cid = result.command_id
    if result.outcome == "unknown":
        return _text(
            f"結果不明: コマンドの結果を確認できませんでした（command_id={cid}）。"
            "同じコマンドを再実行しないでください。"
            "command_status と command_output でこの command_id の状態を確認してください。",
            error=True,
        )
    if result.outcome == "running":
        return _text(
            f"実行中です（command_id={cid}）。command_status / command_output で確認してください。"
        )
    if result.outcome == "cancelled":
        return _text(f"コマンドは取り消されました（command_id={cid}）。", error=True)
    parts = [f"exit code: {result.exit_code}", f"command_id: {cid}"]
    if result.stdout:
        parts.append("[stdout]\n" + result.stdout)
    if result.stderr:
        parts.append("[stderr]\n" + result.stderr)
    return _text(_clip("\n".join(parts)), error=bool(result.exit_code))


def _obj(properties: dict[str, dict[str, Any]], required: list[str]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required}


_STR = {"type": "string"}
_INT = {"type": "integer"}
_NUM = {"type": "number"}


def build_sandbox_tools(
    key: str, runtime: SandboxRuntime, tracker: CommandTracker | None = None
) -> list[SdkMcpTool[Any]]:
    """Tools bound to conversation `key`. The agent never chooses the sandbox."""

    @tool(
        "bash",
        "作業環境でシェルコマンドを実行する。結果が不明な場合は command_id が返り、"
        "同じコマンドは再実行せず command_status で確認する。",
        _obj({"command": _STR, "cwd": _STR, "timeout_seconds": _NUM}, ["command"]),
    )
    async def bash(args: dict[str, Any]) -> dict[str, Any]:
        started_id: list[str] = []

        async def on_started(command_id: str) -> None:
            started_id.append(command_id)
            if tracker is not None:
                await tracker.started(key, command_id)

        result: CommandResult | None = None
        try:
            result = await runtime.run_command(
                key,
                args["command"],
                cwd=args.get("cwd"),
                timeout_seconds=args.get("timeout_seconds") or DEFAULT_TIMEOUT,
                on_started=on_started,
            )
        finally:
            # Keep the id only while the command may still be running (unknown / running);
            # on completion, cancellation or an error it is no longer a running command.
            may_run = result is not None and result.outcome in ("unknown", "running")
            if tracker is not None and started_id and not may_run:
                await tracker.finished(key, started_id[0])
        assert result is not None  # an exception would have propagated
        return format_command(result)

    @tool(
        "read_file",
        "作業環境のファイルを行番号付きで読む。",
        _obj({"path": _STR, "offset": _INT, "limit": _INT}, ["path"]),
    )
    async def read_file(args: dict[str, Any]) -> dict[str, Any]:
        try:
            data = await runtime.read_file(key, args["path"])
        except FileNotFoundError:
            return _text(f"ファイルが見つかりません: {args['path']}", error=True)
        lines = data.decode("utf-8", errors="replace").splitlines()
        offset = max(int(args.get("offset") or 0), 0)
        limit = int(args.get("limit") or READ_DEFAULT_LIMIT)
        chunk = lines[offset : offset + limit]
        body = "\n".join(f"{offset + i + 1}\t{line}" for i, line in enumerate(chunk))
        if offset + limit < len(lines):
            body += f"\n…（全{len(lines)}行。offset={offset + limit} で続きを読めます）"
        return _text(_clip(body) or "（空のファイル）")

    @tool(
        "write_file",
        "作業環境にファイルを書く（既存なら上書き）。",
        _obj({"path": _STR, "content": _STR}, ["path", "content"]),
    )
    async def write_file(args: dict[str, Any]) -> dict[str, Any]:
        content: str = args["content"]
        await runtime.write_file(key, args["path"], content.encode("utf-8"))
        return _text(f"書き込みました: {args['path']}（{len(content)} 文字）")

    @tool(
        "edit_file",
        "ファイルの文字列を置換する。old が一意でない場合はエラーになる（replace_all で全置換）。",
        _obj(
            {"path": _STR, "old": _STR, "new": _STR, "replace_all": {"type": "boolean"}},
            ["path", "old", "new"],
        ),
    )
    async def edit_file(args: dict[str, Any]) -> dict[str, Any]:
        path, old, new = args["path"], args["old"], args["new"]
        try:
            text = (await runtime.read_file(key, path)).decode("utf-8")
        except FileNotFoundError:
            return _text(f"ファイルが見つかりません: {path}", error=True)
        count = text.count(old)
        if not old or count == 0:
            return _text("置換する文字列が見つかりません。", error=True)
        if count > 1 and not args.get("replace_all"):
            return _text(
                f"置換する文字列が{count}か所にあります。前後を含めて一意にするか、"
                "replace_all を指定してください。",
                error=True,
            )
        await runtime.write_file(key, path, text.replace(old, new).encode("utf-8"))
        return _text(f"置換しました: {path}（{count}か所）")

    @tool("list_files", "ディレクトリの内容を一覧する。", _obj({"path": _STR}, ["path"]))
    async def list_files(args: dict[str, Any]) -> dict[str, Any]:
        entries = await runtime.list_files(key, args["path"])
        if not entries:
            return _text("（空）")
        return _text(_clip("\n".join(f"{e.path}{'/' if e.is_dir else ''}" for e in entries)))

    @tool(
        "glob",
        "パターン（例: **/*.py）に一致するファイルを探す。",
        _obj({"pattern": _STR, "path": _STR}, ["pattern"]),
    )
    async def glob(args: dict[str, Any]) -> dict[str, Any]:
        pattern, path = shlex.quote(args["pattern"]), shlex.quote(args.get("path") or ".")
        command = (
            f"(rg --files -g {pattern} {path} 2>/dev/null) || "
            f"find {path} -path {shlex.quote('*/' + args['pattern'].lstrip('*/'))} -type f"
        )
        return format_command(await runtime.run_command(key, command))

    @tool(
        "grep",
        "ファイルの内容を正規表現で検索する。",
        _obj({"pattern": _STR, "path": _STR}, ["pattern"]),
    )
    async def grep(args: dict[str, Any]) -> dict[str, Any]:
        pattern, path = shlex.quote(args["pattern"]), shlex.quote(args.get("path") or ".")
        command = (
            f"(rg -n --no-heading -e {pattern} {path} 2>/dev/null) || grep -rnE -e {pattern} {path}"
        )
        return format_command(await runtime.run_command(key, command))

    @tool(
        "command_status",
        "コマンドの状態（実行中・完了・結果不明）と終了コードを返す。",
        _obj({"command_id": _STR}, ["command_id"]),
    )
    async def command_status(args: dict[str, Any]) -> dict[str, Any]:
        result = await runtime.command_status(key, args["command_id"])
        if result.outcome == "unknown":
            return _text(
                f"状態を確認できません（command_id={args['command_id']}）。"
                "コマンドは再実行しないでください。",
                error=True,
            )
        return _text(f"outcome: {result.outcome}\nexit code: {result.exit_code}")

    @tool(
        "command_output",
        "コマンドの出力を cursor 以降から読む。続きは返された next_cursor から読める。",
        _obj({"command_id": _STR, "cursor": _INT}, ["command_id"]),
    )
    async def command_output(args: dict[str, Any]) -> dict[str, Any]:
        out, cursor = await runtime.command_output(
            key, args["command_id"], int(args.get("cursor") or 0)
        )
        return _text(_clip(out) + f"\nnext_cursor: {cursor}")

    return [
        bash,
        read_file,
        write_file,
        edit_file,
        list_files,
        glob,
        grep,
        command_status,
        command_output,
    ]


def build_sandbox_server(
    key: str, runtime: SandboxRuntime, tracker: CommandTracker | None = None
) -> Any:
    return create_sdk_mcp_server(SERVER_NAME, tools=build_sandbox_tools(key, runtime, tracker))
