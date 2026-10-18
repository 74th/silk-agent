# Tasks

## 1. GKE runtime

- [x] 1.1 `GkeSandboxRuntime.cancel_command(conversation_key, command_id, request_id=None)` を追加する（成功は `confirmed`、失敗・handle なしは `unknown`）。`tests/test_gke_runtime.py` に、tracker の cancel が呼ばれることと、失敗時に `unknown` を返すことのテストを追加し、通ることを確認する

## 2. BackgroundCommands

- [x] 2.1 モニタなしの `_cancel` が sender に command_id 単位で取消を送り、結果を返すようにする。`unknown` のときは結果を `unknown` にして tracker の ID を残す。`tests/test_background.py` に、sender が呼ばれる・`unknown` で ID が残る・sender のない runtime は従来どおり、のテストを追加し、通ることを確認する
- [x] 2.2 `cancel_all`（と `cancel_all_for_stop` のモニタなしの経路）が `CommandCancelReport` を返すようにする。turn が動いておらずバックグラウンドのコマンドだけがあるときの停止で、GKE runtime の fake に取消が届き、`unknown` なら「停止を確認できません」と表示されることを、orchestrator のテストで確認する
- [x] 2.3 `cancel_command` ツールが、モニタなしの経路でも `unknown` を成功として返さないことを `tests/test_sandbox_tools.py` で確認する（表示は既存の文言を使う）

## 3. 仕上げ

- [x] 3.1 `uv run pytest`、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
