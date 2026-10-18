# Design

## Context

- `FirestoreOutboxStore.list_conversations_with_pending` は `status in (pending, dispatching)` の `limit(100)` で、並び順がない。`OutboxSenderLoop.run_once` は、この一覧の各会話を `drain` する。先頭が `uncertain`、または `next_attempt_at` が未来の会話は、`claim_next` が None を返すだけで、後続の `pending` は一覧に残り続ける。
- `OutboxSender.run_once`（`sender.py`）は、429 だけを再試行し、他の例外はすべて `uncertain` にする。`SlackOutboxDelivery` は、ペイロードの不備で `ValueError` を送信前に投げる。
- `OutputDeliveryState` は `pending / dispatching / delivered / suppressed / uncertain` で、終端の「失敗」がない。

## Goals / Non-Goals

**Goals:**
- 滞留した会話が一覧を占有しないようにする。
- 確定した失敗を `uncertain` にしないようにする。

**Non-Goals:**
- 会話単位の配送待ちインデックス（専用のコレクション）の導入。必要になるまで採らない。
- `uncertain` の自動解決（重複投稿の恐れがあるため、手動の運用のまま）。

## Decisions

1. **一覧はドキュメント ID の順に、前回の位置から続けて取る。** `OutboxSenderLoop` が最後に見た Outbox ドキュメント ID を持ち、`start_after` で続きを取る。ページが `limit` に満たなければ、位置を先頭に戻す。これで、一つの会話が同じ100件を占有していても、巡回で他の会話に到達する。`__name__` の並びは追加の索引を要しない。代案の「会話単位の配送待ちインデックス」は、書き込みの経路（put / mark_delivery）に副作用が増えるので採らない。
2. **`OutputDeliveryState.FAILED` を追加し、遷移は `DISPATCHING → FAILED` のみ許す。** `claim_next` の対象（`pending / dispatching / uncertain`）に `failed` は含めず、一覧のクエリにも含めないので、後続の配送を止めない。保持期間（retention）では、`delivered` と同様に削除の対象とするが、`uncertain` と違って自動で消してよい。
3. **失敗の分類は `sender.py` の一つの関数で行う。** `SlackApiError` は `error` コードで分ける。一時的（`internal_error`、`service_unavailable`、`fatal_error`、`request_timeout`）は、指数的な待ち（上限あり）で `pending` に戻し、`attempt` が上限（既定10回）を超えたら `failed`。それ以外のコード（`channel_not_found`、`is_archived`、`not_in_channel`、`msg_too_long` など）は、すぐに `failed`。`ValueError`（ペイロードの不備）も `failed`。aiohttp の接続・タイムアウトの例外は `uncertain` のまま。分類できない例外は、安全側の `uncertain`。
4. **メトリクス**: `outputs_total{result="failed"}` を追加する。`uncertain` の滞留の通知は、既存の運用の経路（runbook）に従う。

## Risks / Trade-offs

- [Slack が `ok: false` を返したが、実際には投稿されている] → Slack の API は、`ok: false` では投稿しない。曖昧なコードや応答の欠落は `uncertain` に倒す。
- [`failed` で出力が落ちて、利用者に何も届かない] → `failed` は、会話の後続の出力を止めない代わりに、メトリクスと構造化ログで観測できるようにする。
- [巡回の位置が、並行して動く複数の Control で異なる] → 位置はプロセスごとの最適化で、正しさに影響しない（`claim_next` の lease が二重配送を防ぐ）。

## Migration Plan

- `failed` を扱えない旧版の Control が、新版が書いた `failed` の記録を読んでも、一覧と `claim_next` の対象に入らないので無害。Control を先に更新し、その後 Worker を更新する（書き込みは Control の Sender だけ）。
