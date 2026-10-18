import pytest

from gke_session_sandbox import cli
from gke_session_sandbox.cli import build_parser
from gke_session_sandbox.manager import NetworkReapplyResult


def test_reconcile_cli_supports_one_shot_and_context():
    args = build_parser().parse_args(
        ["reconcile", "--config", "sandbox.yaml", "--context", "test-cluster", "--once"]
    )

    assert args.command == "reconcile"
    assert args.config == "sandbox.yaml"
    assert args.context == "test-cluster"
    assert args.once is True


def test_reapply_network_cli_takes_one_session_or_all():
    parser = build_parser()
    one = parser.parse_args(["reapply-network", "--config", "c.yaml", "--session", "t", "s"])
    assert one.session == ["t", "s"] and one.all is False
    assert parser.parse_args(["reapply-network", "--config", "c.yaml", "--all"]).all is True
    with pytest.raises(SystemExit):
        parser.parse_args(["reapply-network", "--config", "c.yaml"])


@pytest.mark.parametrize(
    ("statuses", "exit_code"),
    [(["applied", "skipped"], None), (["applied", "failed"], 1)],
)
def test_reapply_network_cli_prints_one_line_per_session_and_fails_on_any_failure(
    monkeypatch, capsys, statuses, exit_code
):
    async def fake_run(config_path, context, key):
        return tuple(NetworkReapplyResult(f"{i:064x}", status) for i, status in enumerate(statuses))

    monkeypatch.setattr(cli, "_run_reapply_network", fake_run)
    argv = ["reapply-network", "--config", "c.yaml", "--all"]
    if exit_code is None:
        cli.main(argv)
    else:
        with pytest.raises(SystemExit) as raised:
            cli.main(argv)
        assert raised.value.code == exit_code

    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[1] for line in lines] == statuses
