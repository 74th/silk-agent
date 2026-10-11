from gke_session_sandbox.cli import build_parser


def test_reconcile_cli_supports_one_shot_and_context():
    args = build_parser().parse_args([
        "reconcile", "--config", "sandbox.yaml", "--context", "test-cluster", "--once"
    ])

    assert args.command == "reconcile"
    assert args.config == "sandbox.yaml"
    assert args.context == "test-cluster"
    assert args.once is True
