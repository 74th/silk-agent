from __future__ import annotations

import subprocess
import sys

import gke_session_sandbox


def test_package_imports_without_cluster_access() -> None:
    assert gke_session_sandbox.__version__ == "0.1.0"


def test_module_cli_reports_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "gke_session_sandbox", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "gke-session-sandbox 0.1.0"
