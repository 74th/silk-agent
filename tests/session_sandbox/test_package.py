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


def _imports(path):
    """(module, level) of every import in a source file."""
    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, 0
        elif isinstance(node, ast.ImportFrom):
            if node.level and not node.module:
                for alias in node.names:  # `from . import x`
                    yield alias.name, node.level
            else:
                yield node.module or "", node.level


def test_runtime_modules_import_only_what_the_runtime_image_has() -> None:
    from pathlib import Path

    from gke_session_sandbox import _runtime_modules as rt

    package = Path(gke_session_sandbox.__file__).parent
    allowed_local = set(rt.RUNTIME_MODULES) | set(rt.RUNTIME_PACKAGES)
    for name in rt.RUNTIME_MODULES:
        path = package / f"{name}.py"
        assert path.is_file(), f"{name}.py is listed in RUNTIME_MODULES but does not exist"
        for module, level in _imports(path):
            top = module.split(".")[0]
            if level:
                assert top in allowed_local, f"{name}.py imports {module!r} (not in the runtime)"
            else:
                assert top in sys.stdlib_module_names or top in rt.RUNTIME_THIRD_PARTY, (
                    f"{name}.py imports {module!r}, which the runtime image does not install"
                )
    for name in rt.RUNTIME_PACKAGES:
        assert (package / name / "__init__.py").is_file()


def test_runtime_module_list_is_what_the_build_script_copies() -> None:
    from pathlib import Path

    root = Path(gke_session_sandbox.__file__).parents[2]
    script = (root / "scripts" / "build-session-runtime-image.sh").read_text(encoding="utf-8")
    assert "_runtime_modules.py" in script
    result = subprocess.run(
        [sys.executable, str(Path(gke_session_sandbox.__file__).parent / "_runtime_modules.py")],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "tracker.py" in result.stdout and "_sandboxd_proto/" in result.stdout
