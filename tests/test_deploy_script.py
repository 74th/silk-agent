"""k8s/deploy.sh in distributed mode, with kubectl / terraform / openssl replaced by stubs."""

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

KUBECTL = """#!/usr/bin/env bash
echo "kubectl $*" >> "$STUB_LOG"
if [[ " $* " == *" get secret "* ]]; then exit 1; fi
case "$1" in
  config) echo stub-context ;;
  kustomize) printf '%s\\n' 'apiVersion: v1' 'kind: Namespace' 'metadata:' '  name: demo-system' ;;
  apply) if [[ "$2" == "-f" ]]; then cat >/dev/null; fi ;;
esac
"""
TERRAFORM = "#!/usr/bin/env bash\necho stub-context\n"
OPENSSL = "#!/usr/bin/env bash\necho 0123456789abcdef\n"


def run_deploy(tmp_path: Path, **env: str) -> tuple[subprocess.CompletedProcess, list[str]]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("kubectl", KUBECTL), ("terraform", TERRAFORM), ("openssl", OPENSSL)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    env_file = tmp_path / ".env"
    env_file.write_text("SLACK_BOT_TOKEN=xoxb-1\nSLACK_APP_TOKEN=xapp-1\n")
    log = tmp_path / "calls.log"
    log.touch()
    result = subprocess.run(
        ["bash", str(ROOT / "k8s/deploy.sh")],
        env={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_LOG": str(log),
            "SILK_AGENT_ENV_FILE": str(env_file),
            **env,
        },
        capture_output=True,
        text=True,
    )
    return result, log.read_text().splitlines()


def test_script_is_valid_bash():
    subprocess.run(["bash", "-n", str(ROOT / "k8s/deploy.sh")], check=True)


def test_distributed_mode_creates_the_sandbox_secret_but_no_slack_secret_and_waits_for_four_roles(
    tmp_path,
):
    result, calls = run_deploy(
        tmp_path,
        DISTRIBUTED_DIR="usecases/demo/k8s/distributed/steady",
        SYSTEM_NAMESPACE="demo-system",
    )
    assert result.returncode == 0, result.stderr
    assert "kubectl create namespace demo-system --dry-run=client -o yaml" in calls
    # The Slack tokens come from Secret Manager: no Slack Secret is made, and no env file read.
    assert not any("silk-agent-slack" in c for c in calls)
    assert "kubectl -n demo-system get secret silk-agent-sandbox" in calls
    assert any(
        c.startswith("kubectl -n demo-system create secret generic silk-agent-sandbox ")
        for c in calls
    )
    assert "kubectl apply -k usecases/demo/k8s/distributed/steady" in calls
    waits = [c for c in calls if " rollout status " in c]
    assert waits == [
        f"kubectl -n demo-system rollout status deployment/silk-agent-{role} --timeout=300s"
        for role in ("sandbox-service", "gateway", "worker", "control")
    ]
    # Nothing is created in the single-process Bot's fixed namespace.
    assert not any("session-sandbox-system" in c for c in calls)


def test_distributed_mode_refuses_a_directory_without_the_namespace(tmp_path):
    result, calls = run_deploy(
        tmp_path, DISTRIBUTED_DIR="somewhere", SYSTEM_NAMESPACE="other-system"
    )
    assert result.returncode == 4
    assert "does not define namespace other-system" in result.stderr
    assert not any(c.startswith("kubectl apply") or "create secret" in c for c in calls)


def test_distributed_mode_requires_the_namespace_and_excludes_kustomize_dir(tmp_path):
    result, _ = run_deploy(tmp_path, DISTRIBUTED_DIR="somewhere")
    assert result.returncode != 0 and "SYSTEM_NAMESPACE" in result.stderr
    result, _ = run_deploy(
        tmp_path / "b" if (tmp_path / "b").mkdir() is None else tmp_path,
        DISTRIBUTED_DIR="a",
        KUSTOMIZE_DIR="b",
        SYSTEM_NAMESPACE="x",
    )
    assert result.returncode == 2


def test_distributed_mode_does_not_need_an_env_file(tmp_path):
    result, calls = run_deploy(
        tmp_path,
        DISTRIBUTED_DIR="usecases/demo/k8s/distributed/steady",
        SYSTEM_NAMESPACE="demo-system",
        SILK_AGENT_ENV_FILE=str(tmp_path / "does-not-exist.env"),
    )
    assert result.returncode == 0, result.stderr
    assert "kubectl apply -k usecases/demo/k8s/distributed/steady" in calls
