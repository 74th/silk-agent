# Design

## Context

- 保持期限の部品は一部だけある: `src/silk_agent/stores/firestore/retention.py` の `terminal_expiry()`（既定 30 日、入力の completed / blocked / cancelled / suppressed だけ）と、`terraform/main.tf:182-192` の `google_firestore_field "input_expire_at_ttl"`（コレクショングループ `inputs` の `expire_at`）。`terminal_expiry()` を呼ぶ処理はない。`inbox.advance_input` は `completed_at` も記録しない。
- コレクション名は `{firestore_prefix}_<name>`（既定 `silk-agent`）。`inputs` と `controls` は会話のサブコレクション。Terraform の索引は `silk-agent_commands` などを直接書いており、接頭辞の変数がない。
- `expires_at` は所有権・lease・Sandbox の保持の期限として使われている。TTL のフィールドは既存の `expire_at` に統一し、`expires_at` には TTL を付けない。
- TTL の対象のフィールドは Firestore の Timestamp でなければならない。`codec.encode_record` を通る入力とコマンド以外は、`*_at` を ISO 文字列で書いている。
- 各コレクションの状態と終端の値（2026-10-18 の調査）:

  | コレクション | 状態 | 期限を書く状態 | 書かない状態 |
  |---|---|---|---|
  | `conversations/*/inputs` | `delivery_state` | completed, blocked, cancelled, suppressed | pending, dispatching, queued, accepted, held, uncertain |
  | `conversations/*/controls`、`control_queue` | `authorization_state` | handled, filtered, stale | pending, authorized |
  | `outbox` | `status` | delivered, suppressed | pending, dispatching, uncertain |
  | `commands` | `state` + `notification_state` | finished / cancelled / unknown かつ通知が pending・held でない | intent, running, cancel_requested、通知が pending・held |
  | `sandbox_operations` | `state`（accepted のまま変わらない） | 対応するコマンドに期限を書くとき同時に | それ以外 |
  | `quiesce_requests` | `state` | answered | requested |
  | `slack_receipts` | なし（時刻のフィールドもない） | 作成時 | — |
  | `workers` | なし | heartbeat ごと | — |

- Agent Platform Sessions: `AgentPlatformSessions.ensure_session`（`stores/agent_platform.py:92-114`）が作成時に `ttl` を設定する。更新の処理はない。作成済みの ID はプロセス内の `_known` に記録され、期限切れで消えても作り直さない。SDK（`vertexai._genai.sessions`）には `_update`（`ttl`、`expire_time`、`update_mask` を受け付ける）があるが、公開の `update` はなく、実際に期限を延ばせるかは未確認。
- 再開は `agent/engine.py:130-159` で、`resume` に前回の SDK セッション ID を渡す。保存先が `load` で None を返したときの扱いは SDK 任せで、検出も通知もない。

## Goals / Non-Goals

**Goals:**
- 増え続ける記録だけを保持期間の後に消す。判断に使う記録（処理中・未確定・通知前）は消さない。
- 既存の記録にも期限を付け、今ある 1,000 件ほどを片付けられるようにする。
- 活動中の会話の履歴を守り、失われた場合でも会話を続けられるようにする。

**Non-Goals:**
- 会話ごとの記録（`conversations`、`ownership`、`sandboxes`、`resource_leases`、`user_rate_limits`）の削除。会話の数に比例するだけで、入力の数では増えない。会話全体の削除（サブコレクションを先に消し親を最後に消す処理）は別の change で扱う。
- 試験用の会話（`acceptance:*`、`loadtest:*`）の一括削除。
- 期限切れの前の警告や、履歴の要約による引継ぎ（D6 の延長が使えない場合に再検討する）。
- 単一プロセス構成（SQLite）のデータの保持。

## Decisions

### D1. 期限のフィールドは `expire_at`（Timestamp）に統一する

すべての対象コレクションで、終端状態に変える同じ書き込み（transaction）で `expire_at` を Timestamp として書く。状態を戻す遷移（例: 再割当で pending に戻る）が許される場合は、同じ書き込みで `expire_at` を消す。`retention.py` を、種類ごとに「状態を受け取り、期限または None を返す」関数の集まりに広げ、各ストアはそれを呼ぶだけにする。判断の規則を 1 か所に集め、表のとおりかを単体テストで網羅する。

- 却下した案: 定期的に走るバッチが終端の記録を探して消す案。削除の処理と索引が増え、TTL なら Firestore がやることを自前で持つことになる。
- 却下した案: 既存の `expires_at` を使う案。lease の期限と混ざり、TTL で生きている lease が消える。

### D2. 保持期間の設定

`distributed` の設定に次を追加する（単位は日）: `retention.terminal_days`（既定 30）、`retention.receipt_days`（既定 7）、`retention.worker_days`（既定 7）。0 以下は設定エラー。Slack の再送は数時間以内なので、受信記録は 7 日で十分。Worker の登録は `status-all` で調査に使うので 7 日残す。

### D3. 終端時刻の記録

期限は終端になった時刻から数える。入力には `completed_at`（Timestamp）を同時に書く。コマンドは既存の `completed_at` ではなく、通知が済んだ時刻（`notification_state` が delivered / suppressed / none になった時刻）から数える。終端と通知のどちらか遅い方で期限を決める。受信記録には `received_at` を新しく書く。

コマンドの通知が済んだことの記録（2026-10-18 に追記）: 実装には `notification_state` を `delivered` にする処理がなく、`notify` の経路のコマンドは `pending` のまま残っていた。そこで、`advance_input` が `command_finished` の入力を COMPLETED にする同じ transaction で、`payload.command_id` のコマンドを `notification_state=delivered`（`notification_updated_at`、`expire_at`）にし、同じ操作 ID の `sandbox_operations` にも同じ `expire_at` を書く。コマンドが既に終端でないとき（通常は起きない）や、通知の状態が pending / held 以外に変わっているときは触らない。`foreground` で結果を返した（`none`）コマンドと、`suppressed` のコマンドは、終端にする書き込み（`observe_terminal`、`request_cancel` で終端のコマンドを suppressed にする場合）で期限を書く。

### D4. Sandbox 操作はコマンドと一緒に期限を付ける

`sandbox_operations` は終端の状態を持たず、`is_dispatch_allowed` と `list_for_stop` が参照する。コマンドとは `accept_intent`（`service_operations.py:30-149`）で同時に作られる。コマンドに期限を書く transaction で、同じ操作 ID の操作にも同じ期限を書く。コマンドが作られなかった操作は期限を付けない（件数は少なく、運用者がバックフィルの出力で確認する）。

### D5. Worker の登録

heartbeat の書き込みで `expire_at = heartbeat_at + worker_days` を書く。生きている Worker は 20 秒ごとに延びるので消えない。`register` の書き込み（新しい incarnation の `set`）も同じ期限を書く。`workers/{id}/incarnations/*` は実在するドキュメントではなく（`reservations` の親のパスとしてだけ使われている）、TTL の対象にならない。クラッシュした Worker に残る reservations の掃除は、この change では扱わない（2026-10-18 に確認して対象外にした）。

### D6. Agent Platform Sessions の期限の延長

`AgentPlatformSessions` に `extend(session_id)` を加え、`update_mask=ttl`（または `expire_time`）で期限を「今 + ttl_seconds」に更新する。呼ぶのは入力を SDK に渡す直前で、残りが `ttl_seconds / 2` を下回るときだけにする（毎回の呼び出しを避けるため、残りの期間は `get` の `expire_time` をプロセス内に記録して判断する）。対象は会話の SDK セッション（`c-...`）と、単一プロセス構成の管理用セッション（`m-...`）の両方。

実機で確認済み（2026-10-18、`docs/distributed-operations-runbook.md` 付録 A）: 延長できる。呼び出しは SDK の `sessions._update(name=..., config={"ttl": "<ttl_seconds>s", "update_mask": "ttl", "wait_for_completion": True})` で、`expire_time` は更新時刻 + ttl になる。`expire_time` の mask も通るが、`ttl` を使う。REST の PATCH を直接呼ぶ必要はない。イベントの追記では期限は延びない。`ttl` の下限は 24 時間なので、`ttl_seconds` が 86400 未満の設定は作成と延長の両方で拒否される（設定の検証で弾く）。

### D7. 履歴を失った会話の再開

再開の前に、SDK セッションの保存先があるかを `list_raw_events`（None なら存在しない）で確認する。ない場合は、`_fresh` と同じく新しい SDK セッションで始め、会話の `sdk_session_id` を新しいものにし、`history_reset_reason=expired` を会話の管理情報に記録する。その turn の最初の出力の前に「以前の会話の履歴は保存期間を過ぎたため引き継げませんでした。作業領域のファイルは残っています。」に相当する通知を、通常の出力と同じ経路（出力のガードと outbox）で送る。

追記で 404 が返った場合は、`_known` から外して作り直し、追記を 1 回だけ再試行し、同じ記録を残す。

- 却下した案: SDK の resume が失敗してから検出する案。SDK のエラーの形に依存し、失敗した turn を作り直す処理が要る。

### D8. Terraform

`terraform/variables.tf` に `firestore_prefix`（既定 `silk-agent`）を追加し、既存の索引のコレクション名も `"${var.firestore_prefix}_commands"` などに置き換える（値は変わらないので plan の差分は出ない）。TTL は `google_firestore_field` で、`inputs`、`controls`（コレクショングループ）、`<prefix>_outbox`、`<prefix>_commands`、`<prefix>_sandbox_operations`、`<prefix>_quiesce_requests`、`<prefix>_slack_receipts`、`<prefix>_control_queue`、`<prefix>_workers` の `expire_at` に設定する。`expire_at` は索引から外す（`index_config {}`）。

### D9. 既存の記録へのバックフィル

`silk-agent retention backfill [--apply] [--limit N]` を追加する。種類ごとに D1 の規則を当て、`expire_at` がない終端の記録に期限を書く。終端時刻が分からない記録は、実行時刻を起点にする（すぐには消えない側に倒す）。既定では件数だけを表示し、`--apply` のときだけ 1 件ずつ transaction で「状態がまだ終端で、`expire_at` がない」ことを確認してから書く。

## Risks / Trade-offs

- [TTL で消した記録を後で参照する処理がある] → 実装の前に、各コレクションの読み出し（get / query）を洗い出し、終端の記録を読む処理がないか確認する（tasks 1.2）。重複排除は受信記録に依存するので、受信記録の期間は Slack の再送より十分長くする。
- [状態が終端から戻る遷移で `expire_at` を消し忘れる] → D1 の規則を通さない書き込みを禁止し、戻る遷移のテストで `expire_at` が消えることを確認する。
- [TTL の削除は期限から最大 24 時間ほど遅れる] → 期限を過ぎて残っている記録も終端なので、読み出しの結果は変わらない。
- [Agent Platform の更新 API が非公開・変更される] → 呼び出しを `AgentPlatformSessions` に閉じ込め、失敗しても turn を止めない。D6 の確認結果を `docs/` に記録する。
- [履歴を失った通知が既存の出力のガードで止まる] → 固定の文言なので、出力のガードの試験に含める。

## Migration Plan

1. コードを配置する（新しい記録から期限が付く）。この時点では TTL ポリシーは `inputs` だけなので、他はまだ消えない。
2. `terraform apply` で TTL ポリシーを追加する。TTL ポリシーの有効化には時間がかかる（数分〜）ので、`gcloud firestore fields ttls list` で状態を確認する。
3. `silk-agent retention backfill` で件数を確認してから `--apply` を実行する。
4. 戻す場合: TTL ポリシーを外す（`terraform apply` で該当の field を削除）。既に消えた記録は戻らないので、手順 3 の前にバックアップ（`gcloud firestore export`）を取る。
