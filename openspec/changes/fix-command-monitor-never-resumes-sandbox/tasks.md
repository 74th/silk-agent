# Tasks

## 1. 読み取り専用のコマンドクライアント

- [ ] 1.1 `SessionHandle` に `ensure` を呼ばない `observe_commands` を追加し、`READY` 以外と世代の不一致で例外を投げるようにする。`tests/session_sandbox/test_manager.py` に、`SUSPENDED` の記録で `ensure` が呼ばれず Sandbox が再開されないことを確認するテストを追加する

## 2. 監視での使用と終端化

- [ ] 2.1 `ServiceCommandRuntime` が `observe_commands` を使い、停止中は `unknown`（再試行）、失われたときは終端の `unknown` を返すようにする。`tests/test_sandbox_service_monitor.py` で、3つの状態の結果を確認する
- [ ] 2.2 `CommandMonitor` が終端の `unknown` を記録して通知を1回作り、再試行しないことを、`tests/test_command_monitor.py` で確認する

## 3. 仕上げ

- [ ] 3.1 `uv run pytest`、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する。`service_monitor.py` の docstring が実装と一致することを読み合わせて確認する
