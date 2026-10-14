from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from silk_agent.cli import CONFIG_ENV, build_parser, main


def test_config_is_required(monkeypatch):
    monkeypatch.delenv(CONFIG_ENV, raising=False)
    with pytest.raises(SystemExit) as e:
        main(["slack"])
    assert CONFIG_ENV in str(e.value)


def test_missing_config_file(monkeypatch, tmp_path):
    monkeypatch.setenv(CONFIG_ENV, str(tmp_path / "nope.yaml"))
    with pytest.raises(SystemExit) as e:
        main(["slack"])
    assert "nope.yaml" in str(e.value)


def test_config_argument_wins_over_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv(CONFIG_ENV, str(tmp_path / "env.yaml"))
    with pytest.raises(SystemExit) as e:
        main(["slack", "--config", str(tmp_path / "arg.yaml")])
    assert "arg.yaml" in str(e.value)


def test_invalid_config_names_the_missing_setting(monkeypatch, tmp_path):
    path = tmp_path / "silk-agent.yaml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(SystemExit) as e:
        main(["slack", "--config", str(path)])
    assert "agent.project" in str(e.value)


def test_worker_admin_parser_requires_an_explicit_action_and_worker():
    args = build_parser().parse_args(
        ["worker", "drain", "worker-a", "--incarnation", "pod-a:startup-1", "--wait"]
    )
    assert args.action == "drain"
    assert args.worker_id == "worker-a"
    assert args.incarnation == "pod-a:startup-1"
    assert args.wait


def test_worker_admin_parser_supports_all_worker_actions():
    args = build_parser().parse_args(["worker", "drain-all", "--wait", "--timeout", "120"])
    assert args.action == "drain-all"
    assert args.worker_id is None
    assert args.wait and args.timeout == 120


def test_sandbox_service_parser_accepts_config():
    args = build_parser().parse_args(["sandbox-service", "--config", "worker.yaml"])
    assert args.command == "sandbox-service"
    assert args.config == "worker.yaml"


def test_gateway_parser_accepts_config():
    args = build_parser().parse_args(["gateway", "--config", "gateway.yaml"])
    assert args.command == "gateway"
    assert args.config == "gateway.yaml"


async def test_gateway_factory_rejects_in_process_config_before_credentials():
    from silk_agent.config import Config, ConfigError
    from silk_agent.distributed.gateway_runtime import run_gateway

    with pytest.raises(ConfigError, match="execution_mode"):
        await run_gateway(Config())


def test_worker_drain_all_command_sets_global_and_per_worker_barriers(
    monkeypatch, tmp_path, capsys
):
    from silk_agent.core.distributed_models import WorkerRecord
    from silk_agent.stores.firestore import client as firestore_client
    from silk_agent.stores.firestore import workers as firestore_workers

    now = datetime(2026, 10, 14, tzinfo=UTC)

    class FakeClient:
        closed = False

        def close(self):
            self.closed = True

    class FakeDirectory:
        def __init__(self, _client, *, collection_prefix):
            assert collection_prefix == "silk-agent-test"
            self.records = [WorkerRecord("worker-a", "inc-1", 2, 0, 0, False, now)]
            self.cluster_draining = False

        async def list_workers(self):
            return tuple(self.records)

        async def set_cluster_draining(self, draining):
            self.cluster_draining = draining

        async def is_cluster_draining(self):
            return self.cluster_draining

        async def set_draining(
            self, *, worker_id, incarnation, draining, now, drain_deadline_at=None
        ):
            for index, record in enumerate(self.records):
                if record.worker_id == worker_id and record.incarnation == incarnation:
                    self.records[index] = replace(
                        record,
                        draining=draining,
                        drain_deadline_at=drain_deadline_at if draining else None,
                    )
                    return self.records[index]
            return None

    fake_client = FakeClient()
    directory = None
    monkeypatch.setattr(
        "silk_agent.cli.load_config",
        lambda _path: SimpleNamespace(
            execution_mode="distributed",
            distributed=SimpleNamespace(
                firestore_project="test-project",
                firestore_database="(default)",
                firestore_prefix="silk-agent-test",
                drain_timeout_seconds=None,
            ),
        ),
    )
    monkeypatch.setattr(firestore_client, "create_firestore_client", lambda _config: fake_client)

    def make_directory(client, *, collection_prefix):
        nonlocal directory
        directory = FakeDirectory(client, collection_prefix=collection_prefix)
        return directory

    monkeypatch.setattr(firestore_workers, "FirestoreWorkerDirectory", make_directory)
    config_path = tmp_path / "config.yaml"
    config_path.write_text("unused: true\n", encoding="utf-8")

    with pytest.raises(SystemExit) as result:
        main(["worker", "drain-all", "--config", str(config_path), "--timeout", "90"])

    assert result.value.code == 0
    assert directory is not None and directory.cluster_draining
    assert directory.records[0].draining
    assert directory.records[0].drain_deadline_at is not None
    assert fake_client.closed
    assert "worker=worker-a" in capsys.readouterr().out
