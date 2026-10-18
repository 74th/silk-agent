# Tasks

## 1. カウンタを incarnation で守る

- [x] 1.1 `confirm_stopped`（`release_after_stop`）が、Worker ドキュメントの incarnation が一致するときだけ `reserved` / `active_sdk` を減らすようにし、旧 incarnation の手動解除が新しい incarnation の予約数を変えないことを、emulator テスト（`tests/integration/test_firestore_inbox_emulator.py`）で確認する
- [x] 1.2 `release`、`release_unactivated`、`confirm_stopped` が、Worker ドキュメントがなくても所有権を解放し、カウンタの更新を省くようにする。ドキュメントを消した状態の emulator テストで、解放されることを確認する

## 2. コンテナの再起動を停止の証拠にする

- [x] 2.1 `FirestoreWorkerDirectory` に、Worker の現在の incarnation を読む関数を追加し、`RecoveryLoop` に `incarnation_replaced(token)` を注入して `_released_for_missing_pod` と同じ待ちの後に、証拠 ID `auto:incarnation-replaced:<worker_id>` で解放する。unit テストで、同じ Pod UID の新しい incarnation があるとき解放され、異なる Pod UID や同じ incarnation のときは解放されないことを確認する
- [x] 2.2 `control_runtime.py` の起動で、2.1 の判定を配線し、emulator の `worker_role` テストで、コンテナの再起動を模した（同じ Pod UID の新しい incarnation の登録）会話が自動で解放されることを確認する

## 3. 文書

- [x] 3.1 `docs/distributed-operations-runbook.md` §5 を、コンテナの再起動と Worker 記録の消失の場合に合わせて直し、`docs/firestore-index-plan.md` の読み取りの監査に Worker ドキュメントの読み取りを加える。記述が実装と一致することを読み合わせて確認する

## 4. 仕上げ

- [x] 4.1 `uv run pytest`、emulator の統合テスト、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
