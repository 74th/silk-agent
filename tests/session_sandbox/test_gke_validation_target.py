import pytest

from gke_session_sandbox.gke_validation import verify_target


def test_validation_target_accepts_only_the_expected_gke_context_and_project():
    context = verify_target(
        "project-a",
        "us-central1",
        "sandbox-cluster",
        current_context="gke_project-a_us-central1_sandbox-cluster",
        active_project="project-a",
    )

    assert context == "gke_project-a_us-central1_sandbox-cluster"


def test_validation_target_rejects_a_different_kubectl_context():
    with pytest.raises(SystemExit, match="GKE validation: context 'gke_other_us-central1_other-cluster'"):
        verify_target(
            "project-a",
            "us-central1",
            "sandbox-cluster",
            current_context="gke_other_us-central1_other-cluster",
            active_project="project-a",
        )


def test_validation_target_rejects_a_different_gcloud_project():
    with pytest.raises(SystemExit, match="gcloud project 'project-b'"):
        verify_target(
            "project-a",
            "us-central1",
            "sandbox-cluster",
            current_context="gke_project-a_us-central1_sandbox-cluster",
            active_project="project-b",
        )
