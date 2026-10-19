"""Output sanitizer backed by Model Armor (sanitizeModelResponse)."""

from __future__ import annotations

import logging
import re
from typing import Any

from silk_agent.core.ports import SanitizeResult

log = logging.getLogger(__name__)

DEFAULT_CHUNK_CHARS = 8000
# Filters whose matches do not stop an output. pi_and_jailbreak flags ordinary shell
# commands in tool summaries; jailbreaks are handled by guards/bulk_files.py instead.
DEFAULT_IGNORED_FILTERS = ("pi_and_jailbreak",)
_FENCE = re.compile(r"^\s*(```|~~~)")


def _segments(text: str) -> list[str]:
    """Split into paragraph / fenced-code-block segments that concatenate back to `text`."""
    segments: list[str] = []
    current: list[str] = []
    in_fence = False
    for line in text.splitlines(keepends=True):
        current.append(line)
        if _FENCE.match(line):
            in_fence = not in_fence
        if not in_fence and line.strip() == "":
            segments.append("".join(current))
            current = []
    if current:
        segments.append("".join(current))
    return segments


def _hard_split(segment: str, limit: int) -> list[str]:
    parts: list[str] = []
    buf = ""
    for line in segment.splitlines(keepends=True):
        while len(line) > limit:
            if buf:
                parts.append(buf)
                buf = ""
            parts.append(line[:limit])
            line = line[limit:]
        if len(buf) + len(line) > limit:
            parts.append(buf)
            buf = ""
        buf += line
    if buf:
        parts.append(buf)
    return parts


def split_text(text: str, limit: int = DEFAULT_CHUNK_CHARS) -> list[str]:
    """Split at paragraph / code-block boundaries; `"".join(result) == text`."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    buf = ""
    for seg in _segments(text):
        pieces = [seg] if len(seg) <= limit else _hard_split(seg, limit)
        for piece in pieces:
            if buf and len(buf) + len(piece) > limit:
                chunks.append(buf)
                buf = ""
            buf += piece
    if buf:
        chunks.append(buf)
    return chunks


class ModelArmorSanitizer:
    def __init__(
        self,
        *,
        template: str,
        endpoint: str | None = None,
        on_sdp_match: str = "mask",
        chunk_chars: int = DEFAULT_CHUNK_CHARS,
        ignored_filters: tuple[str, ...] | list[str] = DEFAULT_IGNORED_FILTERS,
        client: Any | None = None,
    ) -> None:
        self._ignored = frozenset(ignored_filters)
        self._template = template
        self._endpoint = endpoint
        self._on_sdp_match = on_sdp_match
        self._chunk_chars = chunk_chars
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            from google.api_core.client_options import ClientOptions
            from google.cloud import modelarmor_v1

            options = ClientOptions(api_endpoint=self._endpoint) if self._endpoint else None
            self._client = modelarmor_v1.ModelArmorAsyncClient(client_options=options)
        return self._client

    async def sanitize(self, text: str) -> SanitizeResult:
        if not text.strip():
            return SanitizeResult("pass", text)
        parts: list[str] = []
        masked = False
        for chunk in split_text(text, self._chunk_chars):
            try:
                result = await self._sanitize_chunk(chunk)
            except Exception as e:
                log.exception("Model Armor call failed")
                return SanitizeResult("error", "", f"{type(e).__name__}: {e}")
            if result.status in ("blocked", "error"):
                # One chunk that could not be checked makes the whole output undisplayable:
                # dropping it and sending the rest would show a reply with a hole in it.
                return result
            masked = masked or result.status == "masked"
            parts.append(result.text)
        return SanitizeResult("masked" if masked else "pass", "".join(parts))

    async def _sanitize_chunk(self, chunk: str) -> SanitizeResult:
        from google.cloud import modelarmor_v1

        request = modelarmor_v1.SanitizeModelResponseRequest(
            name=self._template,
            model_response_data=modelarmor_v1.DataItem(text=chunk),
        )
        response = await self._get_client().sanitize_model_response(request=request)
        return self.interpret(
            response.sanitization_result, chunk, self._on_sdp_match, self._ignored
        )

    @staticmethod
    def interpret(
        result: Any,
        chunk: str,
        on_sdp_match: str = "mask",
        ignored_filters: frozenset[str] | set[str] = frozenset(),
    ) -> SanitizeResult:
        from google.cloud.modelarmor_v1 import FilterMatchState

        # Every filter must have run before any verdict means anything: a partial or failed
        # invocation may have skipped the filter that would have matched, and a match from the
        # others (even a masked one) says nothing about the text a skipped filter would flag.
        invocation = getattr(result, "invocation_result", None)
        if invocation is not None and getattr(invocation, "name", "SUCCESS") != "SUCCESS":
            return SanitizeResult("error", "", f"filters incomplete: {invocation.name}")
        if result.filter_match_state != FilterMatchState.MATCH_FOUND:
            return SanitizeResult("pass", chunk)

        matched: dict[str, Any] = {}
        ignored_match = False
        for name, fr in result.filter_results.items():
            kind = type(fr).pb(fr).WhichOneof("filter_result")
            if kind is None:
                continue
            sub = getattr(fr, kind)
            if kind == "sdp_filter_result":
                if _sdp_matched(sub):
                    matched["sdp"] = sub
            elif sub.match_state == FilterMatchState.MATCH_FOUND:
                if name in ignored_filters:
                    ignored_match = True
                else:
                    matched[kind] = sub

        if not matched and ignored_match:
            return SanitizeResult("pass", chunk)
        if not matched:
            # The overall state says MATCH_FOUND but we cannot tell which filter: fail closed.
            return SanitizeResult("blocked", "", "unidentified filter match")
        if set(matched) != {"sdp"}:
            return SanitizeResult("blocked", "", "matched: " + ", ".join(sorted(matched)))
        if on_sdp_match == "block":
            return SanitizeResult("blocked", "", "sensitive data matched")

        deidentified = _deidentified_text(matched["sdp"])
        if not deidentified:
            return SanitizeResult("blocked", "", "no de-identified text returned")
        return SanitizeResult("masked", deidentified, "sensitive data masked")


def _sdp_matched(sdp: Any) -> bool:
    from google.cloud.modelarmor_v1 import FilterMatchState

    kind = type(sdp).pb(sdp).WhichOneof("result")
    if kind is None:
        return False
    return getattr(sdp, kind).match_state == FilterMatchState.MATCH_FOUND


def _deidentified_text(sdp: Any) -> str:
    if type(sdp).pb(sdp).WhichOneof("result") != "deidentify_result":
        return ""
    return sdp.deidentify_result.data.text or ""
