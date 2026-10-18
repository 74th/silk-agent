# Proposal

## Why

分散構成の Outbox（Slack への出力の配送）には、一つの会話の不調が全会話の配送を止める経路がある（`xprivate/product-review-20261018.md` P1-8、P2-1）。

- **P1-8**: 会話 X の先頭の出力が `uncertain` になると、X の `pending` が積み上がる。配送待ちの会話の一覧は、順序指定なしの `limit 100` なので、毎回 X の同じ100件だけを返す。他の会話は一覧に一度も出ず、Slack に何も届かない（emulator で、X に120件積むと `('X',)` だけが返ることを確認済み）。
- **P2-1**: 429 以外の送信失敗は、すべて `uncertain` にされる。`SlackApiError`（`ok: false` で、投稿されていないことが確定している）や、送信前の `ValueError` も含む。`uncertain` は手動で対応するまでその会話の出力を止めるので、一時的な Slack のエラーが P1-8 の引き金になる。

## What Changes

- 配送待ちの会話の一覧を、ページの先頭から毎回取らず、前回の位置から続けて巡回する（最後まで進んだら先頭に戻る）。先頭が `uncertain` の会話は、一覧で他の会話の枠を占有しない。
- 送信の失敗を3種類に分ける。
  - 投稿されていないことが確定している失敗（`ok: false` の Slack エラー、送信前の検証の失敗）: 一時的なエラーコードは待ちを置いて再試行し、恒久的なものは `failed`（終端）にして、その出力だけを以降の配送から外す。
  - 送信後に応答を失った可能性がある失敗（接続断、タイムアウト）: 従来どおり `uncertain`。
  - 429: 従来どおり `Retry-After` の後に再試行。
- `failed` と `uncertain` の件数をメトリクスに出す。
- 回帰テストを追加する（emulator を含む）。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `distributed-execution`: Outbox の配送について、会話間の公平性（一つの会話の滞留が他の会話の配送を止めない）と、送信失敗の分類（確定した失敗は `uncertain` にしない）の要件を加える。

## Impact

- `src/silk_agent/stores/firestore/outbox.py`、`src/silk_agent/distributed/control_runtime.py`（`OutboxSenderLoop`）、`src/silk_agent/distributed/sender.py`、`src/silk_agent/distributed/slack_delivery.py`、`src/silk_agent/core/distributed_models.py`（`OutputDeliveryState`）
- `docs/firestore-index-plan.md`（一覧クエリの索引）、`docs/distributed-operations-runbook.md`（failed / uncertain の扱い）
- `tests/` の unit と `tests/integration/`（emulator）
