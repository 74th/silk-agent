"""Lightweight jailbreak check: refuse shell commands that pull out every file in a directory.

Model Armor's `pi_and_jailbreak` filter flags ordinary shell commands, so it is not used on
outputs. Instead the `bash` tool rejects the small set of commands that dump or pack a whole
directory (archives, `cat *`, `find -exec cat`, `xargs cat`, recursive grep for everything).
This is deliberately coarse: it does not try to catch every way to read many files.
"""

from __future__ import annotations

import re

_READERS = r"(?:cat|head|tail|less|more|base64|xxd|od|strings|hexdump|nl|tac)"
_SEGMENT = r"[^|;&\n]*"  # stays within one simple command

_RULES: list[tuple[str, re.Pattern[str]]] = [
    (
        "ディレクトリをアーカイブにまとめる",
        re.compile(
            rf"\btar\b{_SEGMENT}\s(?:--create\b|-?[a-zA-Z]*c[a-zA-Z]*(?=\s|$))"
            rf"|\b(?:zip|jar)\b{_SEGMENT}\s-[a-zA-Z]*r"
            rf"|\b7za?\s+a\b|\brar\s+a\b|\bcpio\b{_SEGMENT}\s-[a-zA-Z]*o"
        ),
    ),
    (
        "ワイルドカードで全ファイルの中身を出力する",
        re.compile(rf"\b{_READERS}\b{_SEGMENT}(?:^|[\s/'\"])\*"),
    ),
    (
        "find で見つけた全ファイルの中身を出力する",
        re.compile(rf"\bfind\b{_SEGMENT}-exec(?:dir)?\s+{_READERS}\b"),
    ),
    (
        "xargs で全ファイルの中身を出力する",
        re.compile(rf"\bxargs\b(?:\s+-\S+)*\s+{_READERS}\b"),
    ),
    (
        "ループで全ファイルの中身を出力する",
        re.compile(rf"\bfor\s+\w+\s+in\b[^;\n]*[*`$][^\n]*\bdo\b[^\n]*\b{_READERS}\b"),
    ),
    (
        "再帰検索で全ファイルの全行を出力する",
        # The search pattern (first argument after the options) matches every line.
        re.compile(
            rf"(?:\bgrep\b(?={_SEGMENT}\s-[a-zA-Z]*[rR])|\brg\b)(?:\s+-\S+)*\s+"
            r"(?:''|\"\"|'\.'|\"\.\"|'\^'|\"\^\"|\^|'\.\*'|\"\.\*\"|\.\*|\.)(?:\s|$)"
        ),
    ),
]


def find_bulk_file_access(command: str) -> str | None:
    """The reason the command is refused, or None when it may run."""
    for reason, pattern in _RULES:
        if pattern.search(command):
            return reason
    return None
