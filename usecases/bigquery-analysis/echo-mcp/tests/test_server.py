from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def free_port() -> int:
    with closing(socket.socket()) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def server_url(monkeypatch: pytest.MonkeyPatch):
    port = free_port()
    env = os.environ | {"ECHO_MCP_API_KEY": "test-secret", "PYTHONPATH": str(ROOT / "src")}
    url = f"http://127.0.0.1:{port}/mcp"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "echo_mcp.server:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--no-access-log",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError(process.stderr.read())
        try:
            httpx.get(url, timeout=0.1)
        except httpx.HTTPError:
            time.sleep(0.05)
        else:
            break
    yield url
    process.terminate()
    process.wait(timeout=5)


@pytest.mark.asyncio
async def test_echo_mcp_accepts_correct_key_and_returns_same_text(server_url: str) -> None:
    transport = StreamableHttpTransport(server_url, headers={"X_API_KEY": "test-secret"})
    async with Client(transport) as client:
        result = await client.call_tool("echo", {"text": "hello, MCP"})
    assert result.data == "hello, MCP"


@pytest.mark.parametrize("headers", [{}, {"X_API_KEY": "wrong"}])
def test_missing_or_wrong_key_is_rejected_before_mcp_request(
    headers: dict[str, str], server_url: str
) -> None:
    response = httpx.post(
        server_url,
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "echo", "arguments": {"text": "must not run"}},
        },
    )
    assert response.status_code == 401
    assert "unauthorized" in response.text


def test_empty_key_fails_startup_without_logging_value() -> None:
    env = os.environ.copy()
    env.pop("ECHO_MCP_API_KEY", None)
    result = subprocess.run(
        [sys.executable, "-c", "from echo_mcp.server import create_app; create_app()"],
        cwd=ROOT,
        env=env | {"PYTHONPATH": str(ROOT / "src")},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "ECHO_MCP_API_KEY must be set" in result.stderr
    assert "test-secret" not in result.stderr


def test_auth_middleware_never_logs_configured_secret(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ECHO_MCP_API_KEY", "unique-secret-value")
    from echo_mcp.server import create_app

    create_app("unique-secret-value")
    captured = capsys.readouterr()
    assert "unique-secret-value" not in captured.out
    assert "unique-secret-value" not in captured.err
