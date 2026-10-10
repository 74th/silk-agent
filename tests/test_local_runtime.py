import pytest

from silk_agent.sandbox.local import LocalProcessRuntime


@pytest.fixture
def rt(tmp_path):
    return LocalProcessRuntime(tmp_path)


async def test_command_runs_in_conversation_dir(rt):
    res = await rt.run_command("K1", "pwd && echo hello >&2; exit 3")
    assert res.outcome == "completed" and res.exit_code == 3
    assert str(rt.root("K1")) in res.stdout and "hello" in res.stderr


async def test_files_are_isolated_between_conversations(rt):
    await rt.write_file("K1", "/work/a.txt", b"secret")
    assert await rt.read_file("K1", "/work/a.txt") == b"secret"
    with pytest.raises(FileNotFoundError):
        await rt.read_file("K2", "/work/a.txt")
    res = await rt.run_command("K2", "cat work/a.txt")
    assert res.exit_code != 0
    res = await rt.run_command("K1", "cat work/a.txt")
    assert res.stdout == "secret"
    assert [e.path for e in await rt.list_files("K1", "/work")] == ["/work/a.txt"]
    assert await rt.list_files("K2", "/work") == []


async def test_path_cannot_escape(rt):
    with pytest.raises(PermissionError):
        await rt.read_file("K1", "../../etc/passwd")
    with pytest.raises(PermissionError):
        await rt.write_file("K1", "/a/../../x", b"")


async def test_status_and_output(rt):
    res = await rt.run_command("K", "echo out; echo err >&2")
    st = await rt.command_status("K", res.command_id)
    assert st.outcome == "completed" and st.exit_code == 0
    out, cur = await rt.command_output("K", res.command_id)
    assert "out" in out and "err" in out and cur == len(out)
    assert (await rt.command_status("K", "nope")).outcome == "unknown"


async def test_timeout_kills_process(rt):
    res = await rt.run_command("K", "sleep 30", timeout_seconds=0.3)
    assert res.outcome == "cancelled" and res.stderr == "timeout"


async def test_cancel_commands_stops_running_command(rt):
    import asyncio

    task = asyncio.create_task(rt.run_command("K", "sleep 30"))
    await asyncio.sleep(0.3)
    await rt.cancel_commands("K")
    res = await asyncio.wait_for(task, 5)
    assert res.outcome == "cancelled"
