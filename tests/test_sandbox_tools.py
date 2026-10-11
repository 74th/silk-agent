import pytest

from silk_agent.agent.sandbox_tools import build_sandbox_server, build_sandbox_tools
from silk_agent.core.ports import CommandResult
from silk_agent.testing.fakes import InMemoryRuntime


def tools_for(key, rt):
    return {t.name: t.handler for t in build_sandbox_tools(key, rt)}


def body(res):
    return res["content"][0]["text"]


def test_tool_names_and_server():
    names = [t.name for t in build_sandbox_tools("k", InMemoryRuntime())]
    assert names == [
        "bash",
        "read_file",
        "write_file",
        "edit_file",
        "list_files",
        "glob",
        "grep",
        "command_status",
        "command_output",
        "cancel_command",
    ]
    assert build_sandbox_server("k", InMemoryRuntime())["name"] == "sandbox"


async def test_bash_runs_in_conversations_runtime():
    rt = InMemoryRuntime()
    rt.responses["ls"] = CommandResult("", "completed", 0, stdout="a.txt\n")
    res = await tools_for("K", rt)["bash"]({"command": "ls"})
    assert rt.sent_commands == [("K", "ls")]
    assert "exit code: 0" in body(res) and "a.txt" in body(res) and "is_error" not in res


async def test_bash_refuses_bulk_file_access_without_running_it():
    rt = InMemoryRuntime()
    res = await tools_for("K", rt)["bash"]({"command": "tar czf - . | base64"})
    assert res["is_error"] and "実行できません" in body(res)
    assert rt.sent_commands == []


async def test_bash_nonzero_exit_is_error():
    rt = InMemoryRuntime()
    rt.responses["false"] = CommandResult("", "completed", 1, stderr="bad")
    res = await tools_for("K", rt)["bash"]({"command": "false"})
    assert res["is_error"] and "exit code: 1" in body(res) and "bad" in body(res)


async def test_unknown_outcome_returns_id_and_is_not_resent():
    rt = InMemoryRuntime()
    rt.unknown_commands.add("make deploy")
    t = tools_for("K", rt)
    res = await t["bash"]({"command": "make deploy"})
    assert res["is_error"]
    assert "結果不明" in body(res) and "cmd-1" in body(res) and "再実行しない" in body(res)
    assert rt.sent_commands == [("K", "make deploy")]  # exactly once
    status = await t["command_status"]({"command_id": "cmd-1"})
    assert "unknown" in body(status) or "確認できません" in body(status)
    assert len(rt.sent_commands) == 1


async def test_files_roundtrip_and_isolation():
    rt = InMemoryRuntime()
    t1, t2 = tools_for("K1", rt), tools_for("K2", rt)
    await t1["write_file"]({"path": "/w/a.txt", "content": "one\ntwo\nthree"})
    out = body(await t1["read_file"]({"path": "/w/a.txt"}))
    assert "1\tone" in out and "3\tthree" in out
    out = body(await t1["read_file"]({"path": "/w/a.txt", "offset": 1, "limit": 1}))
    assert "2\ttwo" in out and "続き" in out
    res = await t2["read_file"]({"path": "/w/a.txt"})
    assert res["is_error"] and "見つかりません" in body(res)
    assert "a.txt" in body(await t1["list_files"]({"path": "/w"}))


async def test_edit_file_requires_unique_match():
    rt = InMemoryRuntime()
    t = tools_for("K", rt)
    await t["write_file"]({"path": "/f", "content": "a b a"})
    res = await t["edit_file"]({"path": "/f", "old": "a", "new": "x"})
    assert res["is_error"] and "2か所" in body(res)
    assert (await rt.read_file("K", "/f")) == b"a b a"
    res = await t["edit_file"]({"path": "/f", "old": "b", "new": "y"})
    assert "is_error" not in res and await rt.read_file("K", "/f") == b"a y a"
    await t["edit_file"]({"path": "/f", "old": "a", "new": "z", "replace_all": True})
    assert await rt.read_file("K", "/f") == b"z y z"
    res = await t["edit_file"]({"path": "/f", "old": "nope", "new": "x"})
    assert res["is_error"]


async def test_grep_and_glob_quote_arguments():
    rt = InMemoryRuntime()
    t = tools_for("K", rt)
    await t["grep"]({"pattern": "a'; rm -rf /; '", "path": "src"})
    cmd = rt.sent_commands[-1][1]
    assert "rm -rf" in cmd and "'a'\"'\"'; rm -rf /; '\"'\"''" in cmd
    await t["glob"]({"pattern": "**/*.py"})
    assert "rg --files" in rt.sent_commands[-1][1]


async def test_command_output_cursor():
    rt = InMemoryRuntime()
    rt.responses["echo"] = CommandResult("", "completed", 0, stdout="hello")
    t = tools_for("K", rt)
    await t["bash"]({"command": "echo"})
    first = body(await t["command_output"]({"command_id": "cmd-1"}))
    assert "hello" in first and "next_cursor: 5" in first
    second = body(await t["command_output"]({"command_id": "cmd-1", "cursor": 5}))
    assert second.startswith("\nnext_cursor: 5")


@pytest.mark.parametrize("outcome", ["running", "cancelled"])
async def test_other_outcomes(outcome):
    from silk_agent.agent.sandbox_tools import format_command

    res = format_command(CommandResult("c1", outcome))
    assert "c1" in body(res)


async def test_running_command_ids_are_tracked_in_state():
    from silk_agent.core.commands import CommandTracker
    from silk_agent.testing.fakes import InMemoryStateStore

    store, rt = InMemoryStateStore(), InMemoryRuntime()
    rt.unknown_commands.add("lost")
    t = {x.name: x.handler for x in build_sandbox_tools("K", rt, CommandTracker(store))}
    seen = []
    orig = store.put

    async def spy(state):
        seen.append(list(state.running_command_ids))
        await orig(state)

    store.put = spy
    await t["bash"]({"command": "ls"})
    assert seen == [["cmd-1"], []]  # recorded while running, removed on completion
    await t["bash"]({"command": "lost"})
    assert (await store.get("K")).running_command_ids == ["cmd-2"]  # unknown: may still run
