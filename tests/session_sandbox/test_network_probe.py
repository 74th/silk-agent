from __future__ import annotations

import inspect
import socket

import pytest

from gke_session_sandbox import cli
from gke_session_sandbox.manager import DiagnosticResult
from gke_session_sandbox.network_probe import _nameservers, verify
from gke_session_sandbox.types import SessionKey, SessionState


def _resolver(unresolvable: frozenset[str] = frozenset()):
    def resolve(name: str, _port):
        if name in unresolvable:
            raise socket.gaierror(-3, "Temporary failure in name resolution")
        return [(socket.AF_INET, 0, 0, "", ("192.0.2.1", 0))]

    return resolve


def _connector(reachable: set[tuple[str, int]]):
    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def connect(address, timeout=None):
        if address not in reachable:
            raise TimeoutError("timed out")
        return _Connection()

    return connect


def _resolv(tmp_path, text: str = "nameserver 192.168.64.10\noptions ndots:5\n") -> str:
    path = tmp_path / "resolv.conf"
    path.write_text(text)
    return str(path)


def test_verify_passes_when_names_resolve_allowed_connects_and_denied_is_blocked(tmp_path) -> None:
    lines: list[str] = []
    ok = verify(
        ["a.example"],
        ["allowed.example:443"],
        ["blocked.example:443"],
        resolv_conf=_resolv(tmp_path),
        resolve=_resolver(),
        connect=_connector({("allowed.example", 443)}),
        emit=lines.append,
    )

    assert ok is True
    assert lines[0] == "nameservers ['192.168.64.10']"
    assert [line.split()[0] for line in lines[1:]] == ["dns-ok", "allow-ok", "deny-ok"]


def test_verify_fails_when_a_blocked_destination_connects(tmp_path) -> None:
    lines: list[str] = []
    ok = verify(
        [],
        [],
        ["blocked.example:443"],
        resolv_conf=_resolv(tmp_path),
        resolve=_resolver(),
        connect=_connector({("blocked.example", 443)}),
        emit=lines.append,
    )

    assert ok is False
    assert lines[-1].startswith("deny-fail target=blocked.example:443")


def test_verify_reports_dns_failure_with_the_nameserver_and_fails(tmp_path) -> None:
    lines: list[str] = []
    ok = verify(
        ["github.com"],
        [],
        [],
        resolv_conf=_resolv(tmp_path),
        resolve=_resolver(frozenset({"github.com"})),
        emit=lines.append,
    )

    assert ok is False
    assert lines[0] == "nameservers ['192.168.64.10']"
    assert lines[1].startswith("dns-fail name=github.com")


def test_verify_does_not_count_an_unresolvable_denied_target_as_blocked(tmp_path) -> None:
    lines: list[str] = []
    ok = verify(
        [],
        [],
        ["blocked.example:443"],
        resolv_conf=_resolv(tmp_path),
        resolve=_resolver(frozenset({"blocked.example"})),
        emit=lines.append,
    )

    assert ok is False
    assert lines[-1].startswith("deny-unverified")


def test_verify_command_never_writes_or_references_the_workspace() -> None:
    command = cli.verify_network_command(None, ["allowed.example:443"], [])
    assert command[:2] == ["bash", "-lc"] and "python3 -c" in command[2]
    assert command[:2] == ["bash", "-lc"] and "python3 -c" in command[2]
    assert command[4:] == ["--allow", "allowed.example:443"]
    assert command[3].startswith('"""Small in-container probes')
    # Only `verify` runs; the tracker probe in the same file is the one that uses /workspace.
    verify_source = inspect.getsource(verify) + inspect.getsource(_nameservers)
    assert "/workspace" not in verify_source and "write" not in verify_source
    assert "/workspace" not in " ".join(command[2:3] + command[4:])
    assert cli.verify_network_command(None, [], [])[-2:] == ["--deny", "example.com:443"]


class _Commands:
    def __init__(self, result: DiagnosticResult) -> None:
        self.result, self.calls = result, []


class _Manager:
    def __init__(self, commands: _Commands | None) -> None:
        self.commands = commands

    async def get(self, key):
        if self.commands is None:
            return None
        return type("Handle", (), {"state": SessionState.READY})()

    async def run_diagnostic_command(self, key, command, **options):
        self.commands.calls.append((list(command), options))
        return self.commands.result


@pytest.mark.asyncio
@pytest.mark.parametrize(("exit_code", "expected"), [(0, 0), (1, 1)])
async def test_verify_network_returns_the_probe_exit_code(exit_code, expected) -> None:
    commands = _Commands(DiagnosticResult(exit_code, "dns-fail name=x\n"))

    code, output = await cli.verify_network(
        _Manager(commands), SessionKey(tenant_id="t", session_id="s")
    )

    assert code == expected
    assert output == "dns-fail name=x\n"
    assert "cwd" not in commands.calls[0][1]  # the probe never needs the workspace


class _EphemeralManager(_Manager):
    def __init__(self, commands: _Commands, *, exists: bool) -> None:
        super().__init__(commands if exists else None)
        self._commands_for_new = commands
        self.created, self.terminated = False, []

    async def ensure(self, key):
        self.created = True
        self.commands = self._commands_for_new

    async def terminate(self, key, *, reason="explicit"):
        self.terminated.append(reason)


@pytest.mark.asyncio
async def test_verify_network_requires_ephemeral_to_create_and_cleans_up_only_what_it_made() -> (
    None
):
    key = SessionKey(tenant_id="t", session_id="s")
    commands = _Commands(DiagnosticResult(0, "ok\n"))

    missing = _EphemeralManager(commands, exists=False)
    with pytest.raises(cli.ConfigurationError, match="--ephemeral"):
        await cli.verify_network(missing, key)
    assert missing.created is False

    made = _EphemeralManager(commands, exists=False)
    assert (await cli.verify_network(made, key, ephemeral=True))[0] == 0
    assert made.created and made.terminated == ["network-verification"]

    existing = _EphemeralManager(commands, exists=True)
    await cli.verify_network(existing, key, ephemeral=True)
    assert not existing.created and existing.terminated == []


@pytest.mark.asyncio
async def test_verify_network_recreates_a_terminated_session_and_terminates_it_again() -> None:
    commands = _Commands(DiagnosticResult(1, "dns-fail name=x\n"))

    class _Terminated(_EphemeralManager):
        async def get(self, key):
            return type("Handle", (), {"state": SessionState.TERMINATED})()

    manager = _Terminated(commands, exists=False)
    code, _ = await cli.verify_network(
        manager, SessionKey(tenant_id="t", session_id="s"), ephemeral=True
    )

    assert code == 1  # a failing check still ends the session it created
    assert manager.created and manager.terminated == ["network-verification"]
