"""FastMCP HTTP server with a required X_API_KEY header."""

from __future__ import annotations

import hmac
import json
import os
from collections.abc import Awaitable, Callable
from typing import Any

from fastmcp import FastMCP

ASGI = Callable[
    [dict[str, Any], Callable[..., Awaitable[Any]], Callable[..., Awaitable[Any]]], Awaitable[None]
]


class ApiKeyAuth:
    """Reject every HTTP request without the configured API key."""

    def __init__(self, app: ASGI, api_key: str) -> None:
        if not api_key:
            raise ValueError("ECHO_MCP_API_KEY must be set and non-empty")
        self._app = app
        self._api_key = api_key.encode("utf-8")

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        supplied = headers.get(b"x_api_key", b"")
        if not hmac.compare_digest(supplied, self._api_key):
            body = json.dumps({"error": "unauthorized"}).encode("utf-8")
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode()),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        await self._app(scope, receive, send)


def create_app(api_key: str | None = None) -> ApiKeyAuth:
    key = os.environ.get("ECHO_MCP_API_KEY", "") if api_key is None else api_key
    if not key:
        raise ValueError("ECHO_MCP_API_KEY must be set and non-empty")

    mcp = FastMCP("echo-mcp")

    @mcp.tool
    def echo(text: str) -> str:
        """Return the input text unchanged."""
        return text

    return ApiKeyAuth(mcp.http_app(path="/mcp"), key)


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False)
