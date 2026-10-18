# Proposal

## Why

`bigquery-analysis` のエージェントは、Sandbox から任意の SQL を BigQuery に実行でき、費用の上限がない（`xprivate/product-review-20261018.md` P2-23）。現状のプロジェクトの日次クエリ量は既定の 200 TiB で、ユーザごとの上限は無制限。SQL はエージェント自身のコード（`google-cloud-bigquery`）で実行されるので、クエリごとの `maximum_bytes_billed` は強制できない。メタデータサーバから GSA のトークンを取得することもできるので、トークンが外へ出ても被害を抑える、サーバ側の上限が要る。GSA の権限は `roles/bigquery.jobUser` だけ（データセットの読み取り権限はない）で、これは維持する。

## What Changes

- ユースケースの Terraform に、BigQuery の日次クエリ量のカスタムクォータ（プロジェクト単位とユーザ単位）を追加する。上限は変数 `daily_query_limit_gib`（既定 100 GiB）。上限を超えたクエリは、BigQuery が `quotaExceeded` で拒否する。
- 超過したときの失敗が、既存の要件（実行失敗を結果と区別する）で、結果として報告されないことをテストで確認する。
- 運用の文書に、上限の値、変え方、超過時の挙動、プロジェクト単位のクォータである（同じプロジェクトの他の BigQuery の利用にも効く）ことを書く。依頼できる人を絞る `allowed_users` は、Slack のユーザ ID が運用者の判断なので、設定の手順だけを書く。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `bigquery-analysis-usecase`: BigQuery のクエリ量に日次の上限があり、超過は失敗として扱われる要件を加える。

## Impact

- `usecases/bigquery-analysis/terraform/main.tf`、`variables.tf`、`terraform.tfvars.example`
- `usecases/bigquery-analysis/OPERATIONS.md`、`usecases/bigquery-analysis/tests`
- dev の GCP（クォータの上書き）。プロジェクト `nnyn-dev` 全体の BigQuery の日次クエリ量に効く。
