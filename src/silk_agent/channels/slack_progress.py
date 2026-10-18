"""ProgressView: the state behind the progress message of one turn, and its rendering."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

from silk_agent.core.events import (
    Heartbeat,
    InterruptReason,
    MessageAppended,
    OutboundEvent,
    StatusChanged,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
    TurnInterrupted,
)

STOP_HINT = "停止するには『停止』と書き込んでください"
APPENDED_LINE = "📝 追加の指示 {n}件（実行中のツールが終わったら、この処理の続きで対応します）"
RESUME_HINT = "「続けて」と書き込むと再開できます"
STALE_HINT = "最終更新から5分以上経っても変わらない場合、Botが停止している可能性があります"

ViewStatus = Literal["running", "interrupting", "completed", "failed", "interrupted"]

_TOOL_ICON = {
    "running": "⏳",
    "success": "✅",
    "failure": "❌",
    "interrupted": "⏹",
    "background": "🔄",
}
_TOOL_WORD = {
    "running": "実行中",
    "success": "成功",
    "failure": "失敗",
    "interrupted": "中断",
    "background": "バックグラウンドで実行中",
}
_INTERRUPT_HEADER: dict[InterruptReason, str] = {
    "user": "⏹ 中断しました",
    "new_message": "↪ 新しい指示で再開",
    "shutdown": "⚠️ Botの再起動により中断",
    "drain": "⚠️ Workerの縮退により中断",
    "lease_lost": "⚠️ Workerの所有権失効により中断",
    "reset": "🔄 会話をリセットしました",
    "crash": "⚠️ Botの停止により中断",
}


def format_duration(seconds: float) -> str:
    s = max(int(seconds), 0)
    if s < 60:
        return f"{s}秒"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}分{s}秒" if s else f"{m}分"
    h, m = divmod(m, 60)
    return f"{h}時間{m}分" if m else f"{h}時間"


def format_stamp(at: datetime, tz: ZoneInfo) -> str:
    return at.astimezone(tz).strftime("%Y-%m-%d %H:%M")


@dataclass
class ToolLine:
    tool_id: str
    name: str
    summary: str
    status: str = "running"
    duration: float | None = None


@dataclass
class ProgressView:
    started_at: datetime
    timezone: str = "Asia/Tokyo"
    max_tools_shown: int = 20
    tool_display: Literal["name", "summary", "verbose"] = "summary"
    status: ViewStatus = "running"
    interrupt_reason: InterruptReason | None = None
    finished_at: datetime | None = None
    tools: list[ToolLine] = field(default_factory=list)
    cost_usd: float | None = None
    warnings: tuple[str, ...] = ()
    error: str = ""
    appended: int = 0  # user messages added to this turn while it was running
    preparing_sandbox: bool = False

    @property
    def finished(self) -> bool:
        return self.status in ("completed", "failed", "interrupted")

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["started_at"] = self.started_at.isoformat()
        data["finished_at"] = self.finished_at.isoformat() if self.finished_at else None
        return data

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ProgressView:
        started_at = data.get("started_at")
        if not isinstance(started_at, str):
            raise ValueError("progress snapshot has no start timestamp")
        finished_at = data.get("finished_at")
        raw_tools = data.get("tools", [])
        tools = (
            [
                ToolLine(**tool)
                for tool in raw_tools
                if isinstance(tool, dict)
                and all(
                    isinstance(tool.get(field), str)
                    for field in ("tool_id", "name", "summary", "status")
                )
            ]
            if isinstance(raw_tools, list)
            else []
        )
        max_tools = data.get("max_tools_shown", 20)
        max_tools = max_tools if isinstance(max_tools, int) else 20
        display = data.get("tool_display", "summary")
        if display not in {"name", "summary", "verbose"}:
            display = "summary"
        status = data.get("status", "running")
        if status not in {"running", "interrupting", "completed", "failed", "interrupted"}:
            status = "running"
        reason = data.get("interrupt_reason")
        if reason not in {
            None,
            "user",
            "new_message",
            "shutdown",
            "drain",
            "lease_lost",
            "reset",
            "crash",
        }:
            reason = None
        raw_warnings = data.get("warnings", [])
        warnings = (
            tuple(item for item in raw_warnings if isinstance(item, str))
            if isinstance(raw_warnings, list)
            else ()
        )
        cost = data.get("cost_usd")
        appended = data.get("appended", 0)
        return cls(
            started_at=datetime.fromisoformat(started_at),
            timezone=str(data.get("timezone", "Asia/Tokyo")),
            max_tools_shown=max_tools,
            tool_display=display,
            status=status,
            interrupt_reason=reason,
            finished_at=datetime.fromisoformat(finished_at)
            if isinstance(finished_at, str)
            else None,
            tools=tools,
            cost_usd=cost if isinstance(cost, int | float) else None,
            warnings=warnings,
            error=str(data.get("error", "")),
            appended=appended if isinstance(appended, int) else 0,
            preparing_sandbox=bool(data.get("preparing_sandbox", False)),
        )

    # ------------------------------------------------------------ state

    def apply(self, event: OutboundEvent) -> None:
        if isinstance(event, ToolStarted):
            self.preparing_sandbox = False
            self.tools.append(ToolLine(event.tool_use_id, event.name, event.summary))
        elif isinstance(event, ToolFinished):
            for line in reversed(self.tools):
                if line.tool_id == event.tool_use_id:
                    line.status = event.status
                    line.duration = event.duration_seconds
                    if event.summary:
                        line.summary = event.summary
                    break
        elif isinstance(event, StatusChanged):
            self.preparing_sandbox = event.status == "preparing_sandbox"
            if event.status == "interrupting" and not self.finished:
                self.status = "interrupting"
        elif isinstance(event, TurnFinished):
            self.status = "completed"
            self.finished_at = event.finished_at
            self.cost_usd = event.usage.cost_usd
            self.warnings = event.warnings
        elif isinstance(event, TurnFailed):
            self.status = "failed"
            self.error = event.error
        elif isinstance(event, TurnInterrupted):
            self.status = "interrupted"
            self.interrupt_reason = event.reason
            self.finished_at = event.at
            for line in self.tools:
                if line.status == "running":
                    line.status = "interrupted"
        elif isinstance(event, MessageAppended):
            self.appended += 1
        elif isinstance(event, Heartbeat):
            pass  # only the "last updated" stamp changes, and that is taken at render time

    # ------------------------------------------------------------ rendering

    def render(self, now: datetime) -> str:
        tz = ZoneInfo(self.timezone)
        stamp = format_stamp(now, tz)
        end = self.finished_at or now
        elapsed = format_duration((end - self.started_at).total_seconds())

        if self.status == "running" and self.preparing_sandbox:
            header = f"🛠 作業環境を起動しています…（経過 {elapsed}）　最終更新 {stamp}"
        elif self.status == "running":
            header = f"⏳ 実行中（経過 {elapsed}）　最終更新 {stamp}"
        elif self.status == "interrupting":
            header = f"⏹ 中断しています…（経過 {elapsed}）　最終更新 {stamp}"
        elif self.status == "completed":
            extra = f"ツール {len(self.tools)}回"
            if self.cost_usd is not None:
                extra += f" / ${self.cost_usd:.2f}"
            header = f"✅ 完了（所要 {elapsed} / {extra}）　最終更新 {stamp}"
        elif self.status == "failed":
            header = f"❌ 失敗（所要 {elapsed}）　最終更新 {stamp}"
        else:
            reason = self.interrupt_reason or "user"
            header = f"{_INTERRUPT_HEADER[reason]}　最終更新 {stamp}"

        lines = [header]
        if self.error:
            lines.append(f"　{self.error}")
        lines.extend(f"⚠️ {w}" for w in self.warnings)
        lines.extend(self._tool_lines())
        if self.appended and self.status in ("running", "interrupting"):
            lines.append(APPENDED_LINE.format(n=self.appended))
        if self.status == "running":
            lines.append(STOP_HINT)
        elif self.status == "interrupted" and self.interrupt_reason in (
            "shutdown",
            "drain",
            "lease_lost",
            "crash",
        ):
            lines.append(RESUME_HINT)
        return "\n".join(lines)

    def _tool_lines(self) -> list[str]:
        shown = self.tools[-self.max_tools_shown :] if self.max_tools_shown > 0 else []
        hidden = len(self.tools) - len(shown)
        lines = [f"…ほか{hidden}件"] if hidden > 0 else []
        for t in shown:
            parts = [_TOOL_ICON[t.status], _TOOL_WORD[t.status], t.name]
            if self.tool_display != "name" and t.summary:
                parts.append(t.summary)
            line = " ".join(parts)
            if t.duration is not None and t.status != "running":
                line += f"（{format_duration(t.duration)}）"
            lines.append(line)
        return lines
