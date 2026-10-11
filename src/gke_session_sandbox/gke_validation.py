"""Safety checks and failure diagnostics for the dedicated GKE validation runner."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path


def verify_target(
    project: str, location: str, cluster: str, *, current_context: str, active_project: str
) -> str:
    """Refuse to create validation resources against a mismatched GKE target."""
    expected_context = f"gke_{project}_{location}_{cluster}"
    if current_context != expected_context:
        raise SystemExit(
            f"refusing GKE validation: context {current_context!r}, expected {expected_context!r}"
        )
    if active_project != project:
        raise SystemExit(
            f"refusing GKE validation: gcloud project {active_project!r}, expected {project!r}"
        )
    return expected_context


def collect_failure_artifacts(
    artifact_root: Path,
    *,
    run_id: str,
    project: str,
    context: str,
    namespace: str,
    namespace_uid: str | None,
    failure_details: str,
) -> Path:
    """Save run-owned Kubernetes state and logs before validation cleanup deletes them."""
    output_dir = artifact_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "failure.txt").write_text(
        f"run_id={run_id}\nproject={project}\ncontext={context}\n"
        f"namespace={namespace}\nnamespace_uid={namespace_uid or 'not-created'}\n\n"
        f"{failure_details}\n"
    )

    def capture(filename: str, command: list[str]) -> str:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
            content = result.stdout
            if result.stderr:
                content += f"\n--- stderr (exit {result.returncode}) ---\n{result.stderr}"
        except Exception as exc:  # Preserve the primary test failure if diagnostics also fail.
            content = f"artifact command failed: {type(exc).__name__}: {exc}\n"
        (output_dir / filename).write_text(content)
        return content

    capture("namespace.yaml", ["kubectl", "get", "namespace", namespace, "-o", "yaml"])
    capture(
        "resources.yaml",
        ["kubectl", "get", "sandbox,pod,pvc,service,networkpolicy,events", "-n", namespace, "-o", "yaml"],
    )
    # ConfigMaps can contain credentials, so retain only identifying metadata.
    capture(
        "configmaps.txt",
        [
            "kubectl", "get", "configmaps", "-n", namespace,
            "-o", "custom-columns=NAME:.metadata.name,UID:.metadata.uid,CREATED:.metadata.creationTimestamp",
        ],
    )
    pod_result = capture("pods.json", ["kubectl", "get", "pods", "-n", namespace, "-o", "json"])
    try:
        pod_names = [item["metadata"]["name"] for item in json.loads(pod_result).get("items", [])]
    except (json.JSONDecodeError, KeyError, TypeError):
        pod_names = []
    for pod_name in pod_names:
        safe_pod_name = "".join(ch if ch.isalnum() or ch in "-." else "_" for ch in pod_name)
        capture(
            f"pod-{safe_pod_name}.log",
            ["kubectl", "logs", pod_name, "-n", namespace, "--all-containers=true", "--prefix=true"],
        )
        capture(
            f"pod-{safe_pod_name}.previous.log",
            ["kubectl", "logs", pod_name, "-n", namespace, "--all-containers=true", "--previous"],
        )
    return output_dir
