# Design

## Context

- `terraform/main.tf` に、コマンド出力のバケット `google_storage_bucket.command_output` が1つある（30日で削除）。IAM は `for_each = local.gke_principals`（全ユースケース）で、Worker に `objectViewer`、Sandbox Service に `objectAdmin`。単一プロセスの Bot 用の SA（`legacy_bot_access` のユースケースだけ）にも `objectAdmin`。
- ユースケースごとの設定ファイルは `distributed.command_output_bucket` で保存先を指す。現状は、`silk-agent` と `bigquery-analysis` が同じバケットを指す。
- Firestore のエクスポートは `gcloud firestore export gs://<bucket>/...` で運用者が取る（runbook §11.3）。2026-10-18 のバックアップは、コマンド出力のバケットの `firestore-backups/` にある。

## Goals / Non-Goals

**Goals:**
- バックアップをワークロードの権限の外に置く。
- コマンド出力を、ユースケースごとの権限の範囲に閉じる。

**Non-Goals:**
- Firestore のデータベース全体への権限（`roles/datastore.user` はプロジェクト単位）の分離。接頭辞単位の IAM 条件は、コレクションのパスに対する条件がなく実現できない。制限として文書に明記する。
- Agent Platform の Runtime（会話履歴）の分離。Runtime はユースケースごとに作れるが、別の change で扱う。
- `bigquery-analysis` の過去のコマンド出力の移行（運用の期間が短い）。

## Decisions

1. **バックアップ用のバケットは、Terraform で作り、ワークロードの IAM を一切付けない。** 名前は変数 `firestore_backup_bucket_name`（既定 `<project>-silk-agent-firestore-backup`）。保持は変数 `firestore_backup_retention_days`（既定 90）のライフサイクルの削除。`force_destroy = false`、`public_access_prevention = enforced`、`uniform_bucket_level_access = true`。エクスポートは運用者の権限で取る（運用者に `roles/storage.admin` がある前提は、runbook に書く）。Firestore のサービスエージェントがこのバケットへ書けることは、`terraform apply` の後に、実際に1回エクスポートして確認する（足りなければ、そのサービスエージェントだけに、このバケットの `objectAdmin` を足す）。代案の「コマンド出力のバケットに、IAM 条件（プレフィックス）で Worker を拒否する」は、deny は IAM Deny ポリシーが要り、バケットを共有するかぎり設定ミスの影響が大きいので採らない。
2. **コマンド出力のバケットは `for_each = var.usecases` で作る。** `usecases.<name>.command_output_bucket` を任意の項目として足す。省略時は `<project_id>-<name>-command-output`。既存の `silk-agent` は、`var.command_output_bucket_name`（既定の現在のバケット）を、その項目に既定値として入れる。`moved` ブロックで、既存のバケットと IAM を `["silk-agent"]` のキーへ移す（再作成しない）。IAM は、ユースケース自身のバケットへ、自身の主体だけ。`legacy_bot_access` の SA も、そのユースケースのバケットだけ。
3. **`bigquery-analysis` の設定のバケット名を、新しいバケットに変える。** ユースケースの k8s の設定（`silk-agent-distributed.yaml`）と、テンプレートの例の説明を直す。Worker の更新（ConfigMap の変更で rollout）が必要。
4. **既存のバックアップは、`gcloud storage cp -r` で新しいバケットへ移し、オブジェクト数とサイズを突き合わせてから、元の `firestore-backups/` を削除する。** 削除は、突き合わせが一致した後だけ。
5. **文書**: `docs/usecase-guide.md` と runbook に、バックアップの保存先の変更、ユースケースごとのバケット、分離できない共有部分（Firestore のデータベース、Agent Platform Runtime）を書く。

## Risks / Trade-offs

- [`moved` の誤りで、既存のバケットや IAM が再作成される（バケットの破棄）] → `force_destroy = false` なので、中身があると破棄は失敗する。`terraform plan` で `destroy` が出ないことを、適用の前に確認する（runbook の既存の手順）。
- [`bigquery-analysis` の Worker が、新しいバケットが空のうちに、過去の出力の URI（古いバケットを指す）を読もうとして失敗する] → 運用の期間が短く、実害は小さいと判断した。起動時の確認で、`command_output_bucket` が存在しアクセスできることを確かめる（既存の実装があれば流用）。
- [エクスポートが、サービスエージェントの権限不足で失敗する] → Decision 1 のとおり、適用後に実際に取って確認する。
- [90日より長く持つべきか] → 変数で変えられる。バックアップの運用方針（頻度、保持）は、別に決める。

## Migration Plan

1. `terraform plan` で、作成されるバケットと IAM の差分に destroy / replace がないことを確認する。
2. `terraform apply`（dev）。バケット `...-firestore-backup` と `...-bigquery-analysis-command-output` ができ、IAM が付く。
3. 既存のバックアップを新しいバケットへ移し、突き合わせ、元を削除する。
4. `bigquery-analysis` の設定を直して適用し、Worker が新しいバケットを使うことを確認する。
5. ロールバック: 設定を元のバケットに戻す。IAM は Terraform で戻す（バケットの中身は、元のバケットに残してあるので失われない）。
