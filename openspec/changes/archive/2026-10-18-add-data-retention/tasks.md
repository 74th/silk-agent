# Tasks

実装者向け: 1 章で Agent Platform の期限の延長ができるかを先に確認する。延長できないと分かったら、design の D6 のとおりユーザに確認し、spec と 5 章を更新してから進める。Firestore 側（2〜4 章）は 1.1 の結果に関係なく進めてよい。実 DB への書き込み（バックフィルの `--apply`）は 6 章まで行わない。

## 1. 事前確認

- [x] 1.1 試験用の Agent Platform セッションを 1 つ作り（ttl 1 時間）、SDK の `_update` または REST の PATCH で `ttl` / `expire_time` を更新して、`get` の `expire_time` が延びるかを確認する。`update_mask` に通る値と、イベントの追記で延びないことも記録する。終わったらセッションを削除する。検証: 結果を `docs/distributed-operations-runbook.md` の付録として記録し、design の D6 を確定値（使う mask と呼び出し方）で更新する。
- [x] 1.2 `stores/firestore/` と `gke_session_sandbox/firestore_repository.py` の、対象コレクションへの get / query をすべて洗い出し、終端の記録を読む処理がないか確認する。検証: 結果の表を `docs/firestore-index-plan.md` の TTL の節に追加し、削除して困る読み出しがあれば design を更新する。

## 2. 保持期限の規則と設定（D1〜D3）

- [x] 2.1 `config.py` の `DistributedExecutionConfig` に `retention.terminal_days`（30）、`receipt_days`（7）、`worker_days`（7）を追加し、0 以下を拒否する。検証: `tests/test_config.py` に既定値と不正値のテストを追加して通す。
- [x] 2.2 `retention.py` を、種類（入力、制御要求、outbox、コマンド、quiesce、受信記録、Worker）ごとに状態と終端時刻から `expire_at` か None を返す関数群に広げる。コマンドは終了と通知の遅い方から数える。検証: `tests/test_firestore_retention.py` で design の表の全状態を網羅し、書かない状態では None になることを確認する。

## 3. 各ストアで期限を書く

- [x] 3.1 `inbox.py` の入力の状態遷移（`advance_input`、`rebind_after_reset`、`recover_dispatching`、`record_sender_enrichment`）で、終端になる書き込みに `completed_at` と `expire_at` を、終端から戻る書き込みに `expire_at` の削除を加える。検証: emulator の `tests/integration/test_firestore_inbox_emulator.py` で、完了時に Timestamp の `expire_at` が付き、uncertain / held には付かないことを確認する。
- [x] 3.2 制御要求と `control_queue` の handled / filtered / stale への遷移で `expire_at` を書く。受信の transaction で受信記録に `received_at` と `expire_at` を書く。検証: emulator のテストで両方を確認し、受信記録の期限内の再送が重複として扱われることを既存のテストで確認する。
- [x] 3.3 `outbox.py` の `mark_delivery` で delivered / suppressed に `expire_at` を書き、uncertain や pending に戻す書き込みでは消す。検証: outbox の単体・emulator テストを追加して通す。
- [x] 3.4 `command_monitor.py` で、コマンドが終端かつ通知が済んだ書き込み（`observe_terminal` で none / suppressed、`request_cancel`、`set_notification_state`）に `expire_at` を書き、同じ transaction で同じ操作 ID の `sandbox_operations` にも書く（D4）。さらに `inbox.advance_input` が `command_finished` の入力を COMPLETED にする transaction で、コマンドを `notification_state=delivered` にして同じ期限を書く（D3 の追記）。検証: `tests/test_command_monitor.py` と emulator のテストで、通知が pending / held の間は期限が付かず、delivered になったら両方に付き、held から pending に戻したら期限が消えることを確認する。
- [x] 3.5 `quiesce.py` の answered への遷移で `expire_at` を書く。検証: 単体テストを追加して通す。
- [x] 3.6 `workers.py` の登録と heartbeat で、Worker のドキュメントに `expire_at = heartbeat_at + worker_days` を書く（D5。incarnations は実在しないので書かない）。検証: `tests/test_worker_heartbeat.py` と emulator のテストで、heartbeat ごとに期限が延びることを確認する。
- [x] 3.7 上の変更で書く `expire_at`、`completed_at`、`received_at` が文字列ではなく Timestamp で保存されることを確認する。検証: emulator のテストで、各コレクションの値の型を確認する。

## 4. Terraform（D8）

- [x] 4.1 `terraform/variables.tf` に `firestore_prefix` を追加し、既存の索引のコレクション名を変数で書き直す。検証: `terraform plan` で既存の索引に差分が出ない。
- [x] 4.2 D8 の対象コレクションの `expire_at` に `google_firestore_field` の TTL を追加する。`expires_at` には付けない。検証: `terraform validate` が成功し、plan に TTL の追加だけが出る。

## 5. Agent Platform Sessions（D6、D7）

- [x] 5.1 `AgentPlatformSessions` に期限の取得と延長を追加し、残りが `ttl_seconds / 2` を下回るときだけ延長する。失敗はログに残して turn を続ける。検証: `tests/test_agent_platform_store.py` の fake で、期限が十分なら呼ばれない、少なければ呼ばれる、失敗しても例外にならないことを確認する。
- [x] 5.2 入力を SDK に渡す直前（分散構成の Worker と単一プロセスの両方）で 5.1 を呼ぶ。管理用セッション（`m-...`）も対象にする。検証: Worker と単一プロセスの経路のテストで、延長が呼ばれることを確認する。
- [x] 5.3 再開の前に保存先の有無を確認し、ない場合は新しい SDK セッションで始め、会話の `sdk_session_id` を更新し、`history_reset_reason` を記録し、履歴を引き継げなかった通知を通常の出力の経路で送る（D7）。検証: fake で保存先がない会話を再開し、応答と通知が同じスレッドに出て、入力が completed になることを確認する。
- [x] 5.4 追記が 404 で失敗したら `_known` から外して作り直し、1 回だけ再試行し、5.3 と同じ記録を残す。検証: fake で 404 を 1 回返し、追記が成功することを確認する。
- [x] 5.5 5.3 の通知の文言が出力のガードを通ることを確認する。検証: 出力ガードのテストに文言を追加して通す。

## 6. バックフィルと実環境での確認（D9）

- [x] 6.1 `silk-agent retention backfill [--apply] [--limit N]` を追加する。既定は種類ごとの対象件数と除外件数の表示だけ。`--apply` では transaction で状態と `expire_at` の有無を確かめてから書く。検証: emulator のテストで、既定では書かれないこと、2 回目の対象が 0 件になることを確認する。
- [x] 6.2 実環境で `gcloud firestore export` でバックアップを取り、コードを配置し、`terraform apply` で TTL を有効にする。検証: `gcloud firestore fields ttls list` で全対象が ACTIVE になる。
- [x] 6.3 `retention backfill` を件数表示だけで実行して結果を確認し、`--apply` を実行する。検証: 2 回目の件数表示で対象が 0 件。翌日、outbox と inputs の件数が減り、uncertain・held の件数が変わっていないことを確認する。
- [x] 6.4 `docs/firestore-index-plan.md` の TTL の節と、`docs/distributed-operations-runbook.md`（保持期間の設定、バックフィル、TTL の確認方法、履歴を失った会話の扱い）を更新する。検証: runbook の手順で 6.2〜6.3 を再現できる。
