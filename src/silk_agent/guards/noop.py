"""Guards that let everything through (provider: allow_all / noop)."""

from __future__ import annotations

from silk_agent.core.ports import GuardVerdict, SanitizeResult


class AllowAllGuard:
    async def check(self, text: str, history: list[str]) -> GuardVerdict:
        return GuardVerdict("allow")


class NoopSanitizer:
    async def sanitize(self, text: str) -> SanitizeResult:
        return SanitizeResult("pass", text)
