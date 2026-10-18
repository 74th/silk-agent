# Tasks

## 1. 再バインドを再試行できるようにする

- [ ] 1.1 `complete_reset` が世代の更新と同じトランザクションで `rebind_pending` を書くようにする。emulator テストで、世代の更新後に `rebind_pending` が残ることを確認する
- [ ] 1.2 `rebind_after_reset` が完了したときに `rebind_pending` を消し、途中で失敗したときは残すようにする。emulator テストで、途中の入力で例外を起こして記録が残ること、再実行で残りが移って記録が消えることを確認する

## 2. 起動時の完了

- [ ] 2.1 actor の起動で `rebind_pending` を読み、あれば再バインドを完了させる。`tests/test_distributed_actor.py` に、再バインドの途中で停止した後の起動で、旧世代の入力が新しい世代になり、処理されることを確認するテストを追加する

## 3. 仕上げ

- [ ] 3.1 `uv run pytest`、emulator の統合テスト、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
