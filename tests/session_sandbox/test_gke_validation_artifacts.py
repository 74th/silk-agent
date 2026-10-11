import json
from pathlib import Path
from types import SimpleNamespace

from gke_session_sandbox import gke_validation as validation


def test_failure_artifact_collector_saves_owned_resources_and_pod_logs(tmp_path, monkeypatch):
    calls = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        if command[:3] == ["kubectl", "get", "pods"]:
            return SimpleNamespace(stdout=json.dumps({"items": [{"metadata": {"name": "sandbox-a"}}]}), stderr="", returncode=0)
        if command[:3] == ["kubectl", "logs", "sandbox-a"]:
            return SimpleNamespace(stdout="sandbox log output\n", stderr="", returncode=0)
        return SimpleNamespace(stdout="resource snapshot\n", stderr="", returncode=0)

    monkeypatch.setattr(validation.subprocess, "run", fake_run)
    output = validation.collect_failure_artifacts(
        tmp_path,
        run_id="run-123",
        project="project-a",
        context="gke_project-a_us-central1_cluster-a",
        namespace="session-sandboxes-run-123",
        namespace_uid="namespace-uid",
        failure_details="AssertionError: failure\n",
    )

    assert output == tmp_path / "run-123"
    assert "namespace_uid=namespace-uid" in (output / "failure.txt").read_text()
    assert (output / "namespace.yaml").read_text() == "resource snapshot\n"
    assert (output / "resources.yaml").read_text() == "resource snapshot\n"
    assert (output / "configmaps.txt").read_text() == "resource snapshot\n"
    assert (output / "pod-sandbox-a.log").read_text() == "sandbox log output\n"
    assert (output / "pod-sandbox-a.previous.log").exists()
    assert any("--previous" in command for command in calls)
    configmap_command = next(command for command in calls if command[:3] == ["kubectl", "get", "configmaps"])
    assert "custom-columns=NAME:.metadata.name,UID:.metadata.uid,CREATED:.metadata.creationTimestamp" in configmap_command


def test_failure_artifact_collector_records_diagnostic_command_errors(tmp_path, monkeypatch):
    def fake_run(_command, **_kwargs):
        raise TimeoutError("kubectl unavailable")

    monkeypatch.setattr(validation.subprocess, "run", fake_run)
    output = validation.collect_failure_artifacts(
        tmp_path,
        run_id="run-456",
        project="project-a",
        context="gke_project-a_us-central1_cluster-a",
        namespace="session-sandboxes-run-456",
        namespace_uid=None,
        failure_details="RuntimeError: validation failed\n",
    )

    assert "namespace_uid=not-created" in (output / "failure.txt").read_text()
    assert "artifact command failed: TimeoutError" in (output / "namespace.yaml").read_text()
    assert "artifact command failed: TimeoutError" in (output / "resources.yaml").read_text()
