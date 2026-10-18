# Proposal

## Why

分散構成の Firestore は、処理済みの記録を消す仕組みがなく増え続ける。Terraform には `inputs.expire_at` の TTL ポリシーがあるが、`expire_at` を書く処理がない（`retention.terminal_expiry()` は呼ばれていない）。2026-10-18 時点で、実 DB の入力 118 件のうち `expire_at` が付いたものは 0 件、outbox は 755 件、古い Worker の登録は約 65 件ある。一方、会話の履歴を持つ Agent Platform Sessions は作成時に 30 日の TTL を設定し、更新しても延びない。このため、30 日以上続く会話は活動中でも履歴を失う。期限切れの後の復旧処理もない。

## What Changes

- 終端状態になった Firestore の記録（入力、制御要求、outbox、コマンドと Sandbox 操作、quiesce 要求、Slack の受信記録、古い Worker の登録）に、保持期限のフィールドを書く。処理中、未確定（uncertain）、保留（held）、通知が済んでいないものには書かない。
- Terraform で、それらのコレクションに TTL ポリシーを設定する。保持期間は設定で変えられるようにする。
- 既存の終端状態の記録に保持期限を付ける CLI（既定は件数の表示だけで、書き込みは明示的に指定したときだけ）を追加する。
- 会話で新しい入力を処理するときに、Agent Platform Sessions の期限を延ばす。延長できることを最初に実機で確認する。
- 履歴が期限切れなどで失われていた会話を再開するとき、新しいセッションで続け、以前の履歴を引き継げなかったことをユーザに知らせる。

## Capabilities

### New Capabilities

なし。

### Modified Capabilities

- `distributed-execution`: 処理済みの共有記録を保持期間の後に削除し、処理中・未確定の記録は残すことを追加する。
- `session-persistence`: 活動中の会話の履歴を期限で失わないこと、履歴が失われた会話を再開できることを追加する。

## Impact

- `src/silk_agent/stores/firestore/`（`retention.py`、`inbox.py`、`outbox.py`、`command_monitor.py`、`service_operations.py`、`quiesce.py`、`workers.py`）、`src/gke_session_sandbox/firestore_repository.py` は対象外（会話ごとの記録は残す）。
- `src/silk_agent/stores/agent_platform.py`、`core/session.py` と `agent/engine.py` の再開処理、`config.py`（保持期間の設定）、`cli.py`（保持期限の付与コマンド）。
- `terraform/main.tf` と `variables.tf`（TTL ポリシー、`firestore_prefix` の変数化）。TTL の削除は Firestore の課金対象の削除操作になる。
- `docs/firestore-index-plan.md`、`docs/distributed-operations-runbook.md`。
- 会話そのもの（`conversations`、`ownership`、`sandboxes`、`resource_leases` の会話ごとの記録）の削除と、試験用の会話（`acceptance:*`、`loadtest:*`）の掃除は扱わない。
