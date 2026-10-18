# Tasks

## 1. 巡回する一覧

- [x] 1.1 `list_conversations_with_pending` が `start_after`（ドキュメント ID）を受け取り、次の位置を返すようにし、`OutboxSenderLoop` が位置を持って巡回するようにする。emulator テストで、一つの会話に120件の `pending` を積んでも、他の会話が一覧に出て配送されることを確認する
- [x] 1.2 一覧が最後まで進んだら先頭に戻ることを、unit テスト（fake の Outbox）で確認する

## 2. 失敗の分類

- [x] 2.1 `OutputDeliveryState.FAILED` と `DISPATCHING → FAILED` の遷移を追加し、`mark_delivery` と保持期間（retention）の対象に反映する。遷移の unit テストと、emulator テストで確認する
- [x] 2.2 `sender.py` に失敗の分類（確定した一時的／確定した恒久的／結果不明／429）を実装し、`ValueError` と `SlackApiError` の `error` コードごとの結果を unit テストで確認する
- [x] 2.3 `failed` の出力の後続が配送されること、`uncertain`（接続断）が従来どおり止まることを、`OutboxSender` の unit テストで確認する

## 3. 運用

- [x] 3.1 メトリクス `outputs_total{result="failed"}` を追加し、`docs/firestore-index-plan.md` と `docs/distributed-operations-runbook.md` に一覧クエリと `failed` / `uncertain` の扱いを書く。記述が実装と一致することを読み合わせて確認する

## 4. 仕上げ

- [x] 4.1 `uv run pytest`、emulator の統合テスト、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
