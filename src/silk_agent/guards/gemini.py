"""Input guard that judges instruction violations with Gemini on Agent Platform."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from silk_agent.core.ports import GuardVerdict

log = logging.getLogger(__name__)


class InputVerdict(BaseModel):
    decision: Literal["allow", "block", "review"]
    reason: str = ""


class GeminiPolicyGuard:
    def __init__(
        self,
        *,
        policy_file: str | Path | None = None,
        policy: str | None = None,
        model: str = "gemini-3.8-flash",
        project: str,
        location: str = "global",
        client: Any | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        if policy is None:
            if policy_file is None:
                raise ValueError("either policy or policy_file is required")
            policy = Path(policy_file).read_text(encoding="utf-8")
        self._policy = policy
        self._model = model
        self._project = project
        self._location = location
        self._client = client
        self._timeout_ms = int(timeout_seconds * 1000)

    def _get_client(self) -> Any:
        if self._client is None:
            from google import genai

            self._client = genai.Client(
                vertexai=True, project=self._project, location=self._location
            )
        return self._client

    @staticmethod
    def build_contents(text: str, history: list[str]) -> str:
        lines = ["<previous_messages>"]
        lines += [f"<message>{m}</message>" for m in history[-5:]]
        lines += ["</previous_messages>", "<message_to_review>", text, "</message_to_review>"]
        return "\n".join(lines)

    async def check(self, text: str, history: list[str]) -> GuardVerdict:
        """Raises on failure; the orchestrator applies the on_error setting."""
        from google.genai import types

        config = types.GenerateContentConfig(
            system_instruction=self._policy,
            response_mime_type="application/json",
            response_schema=InputVerdict,
            temperature=0.0,
            http_options=types.HttpOptions(timeout=self._timeout_ms),
        )
        response = await self._get_client().aio.models.generate_content(
            model=self._model,
            contents=self.build_contents(text, history),
            config=config,
        )
        parsed = getattr(response, "parsed", None)
        if isinstance(parsed, InputVerdict):
            verdict = parsed
        else:
            verdict = InputVerdict.model_validate_json(response.text)
        return GuardVerdict(verdict.decision, verdict.reason)
