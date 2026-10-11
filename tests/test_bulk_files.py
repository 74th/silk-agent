import pytest

from silk_agent.guards.bulk_files import find_bulk_file_access


@pytest.mark.parametrize(
    "command",
    [
        "tar czf all.tgz .",
        "tar -cf - /workspace | base64",
        "cd src && zip -r out.zip .",
        "7z a out.7z /workspace",
        "cat *",
        "cat src/*",
        "head -n 1000 '*.py'",
        r"find . -type f -exec cat {} \;",
        "find /workspace -name '*.env' -exec base64 {} +",
        "find . -type f | xargs cat",
        "ls | xargs -0 head",
        "for f in *; do cat $f; done",
        "for f in $(find . -type f); do echo $f; cat $f; done",
        "grep -r '' .",
        "grep -rn . src",
        "rg '.*' /workspace",
    ],
)
def test_bulk_file_access_is_refused(command):
    assert find_bulk_file_access(command) is not None


@pytest.mark.parametrize(
    "command",
    [
        "ls -la",
        "cat a.txt",
        "head -n 20 src/app.py",
        "echo hello > a.txt && cat a.txt && whoami && pwd",
        "uv run pytest",
        "tar xzf archive.tgz",
        "tar -tf archive.tgz",
        "unzip out.zip",
        "find . -name '*.py'",
        "grep -rn 'def main' src",
        "grep -rn 'def main' .",
        "grep . a.txt",
        "rg TODO",
        "rg TODO .",
        "git status",
        "python3 -c 'print(1)'",
        "ls *.py",
    ],
)
def test_ordinary_commands_run(command):
    assert find_bulk_file_access(command) is None
