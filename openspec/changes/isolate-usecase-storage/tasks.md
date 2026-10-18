# Tasks

## 1. バックアップ専用のバケット

- [ ] 1.1 `terraform/variables.tf` に `firestore_backup_bucket_name` と `firestore_backup_retention_days`（正の値の検証付き）を追加し、`main.tf` に専用バケット（公開禁止、ライフサイクル削除、`force_destroy = false`）を、ワークロードの IAM なしで追加する。`terraform validate` が通り、`terraform plan` にワークロードの主体への IAM がないことを確認する
- [ ] 1.2 `terraform apply`（dev）の後、既存のバックアップを新しいバケットへ移す。`gcloud storage ls -r` でオブジェクト数とサイズが一致することを確認してから、元の `firestore-backups/` を削除する
- [ ] 1.3 Worker の Workload Identity の権限（`gcloud storage objects list` を、その主体で実行する、または `gcloud storage buckets get-iam-policy` で確認する）で、バックアップのバケットに権限がないことを確認する。運用者の権限で `gcloud firestore export` を新しいバケットへ実行し、`SUCCESSFUL` を確認する

## 2. ユースケースごとのコマンド出力のバケット

- [ ] 2.1 `usecases.<name>.command_output_bucket`（任意）を追加し、`google_storage_bucket.command_output` を `for_each = var.usecases` にして、`moved` で既存のバケットを `["silk-agent"]` へ移す。IAM（Worker の読み取り、Sandbox Service の読み書き、`legacy_bot_access` の SA）を、ユースケース自身のバケットだけに付ける。`terraform plan` に、既存のバケットの destroy / replace がないことを確認する
- [ ] 2.2 `terraform apply`（dev）。`bigquery-analysis` のバケットができ、`bigquery-analysis` の主体が `silk-agent` のバケットへの権限を失い、`silk-agent` の主体が `bigquery-analysis` のバケットに権限を持たないことを、`gcloud storage buckets get-iam-policy` で確認する
- [ ] 2.3 `usecases/bigquery-analysis` の `silk-agent-distributed.yaml` の `command_output_bucket` を新しいバケットに変更し、適用して Worker と Sandbox Service が新しいバケットを使うことを確認する（`kubectl` のログ、またはコマンドを1件実行して出力が新しいバケットに保存されること）

## 3. 文書

- [ ] 3.1 `docs/usecase-guide.md`、`docs/distributed-operations-runbook.md`（§11.3 のバックアップの保存先）、`usecases/bigquery-analysis/OPERATIONS.md`、`usecases/template` の設定の説明を更新し、分離できない共有部分（Firestore のデータベース、Agent Platform Runtime）を制限として明記する。記述が実装と一致することを読み合わせて確認する

## 4. 仕上げ

- [ ] 4.1 `uv run pytest`（Terraform の定義を検査するテストを含む）、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
