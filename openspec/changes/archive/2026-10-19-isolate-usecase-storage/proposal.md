# Proposal

## Why

ストレージの権限が、ユースケースの分離を破っている（`xprivate/product-review-20261018.md` P2-17）。

- Firestore のバックアップ（`gs://nnyn-dev-silk-agent-command-output/firestore-backups/`）が、コマンド出力のバケットの中にある。このバケットは、すべてのユースケースの Worker が `objectViewer`、Sandbox Service が `objectAdmin` を持つので、どのユースケースの Worker も、全ユースケースのバックアップ（会話の入力、出力、コマンドを含む）を読める。さらに、バケットのライフサイクルで30日後に削除されるので、バックアップとしても不適切。
- コマンド出力のバケットが全ユースケースで共有されている。設定ファイルの `command_output_bucket` は、既定で同じバケットを指しており、`bigquery-analysis` も `silk-agent` と同じバケットを使う。あるユースケースの Worker が、別のユースケースのコマンド出力を読める。

## What Changes

- Firestore のバックアップ用に専用のバケットを作る。Worker、Sandbox Service、Gateway、Control の Workload Identity には、このバケットへの権限を一切与えない。保持期間は、コマンド出力とは別に、長めに設定する。
- コマンド出力のバケットを、ユースケースごとに作る。ユースケースの役割（Worker は読み取り、Sandbox Service は読み書き）には、そのユースケース自身のバケットだけを許す。既存の `silk-agent` は、今のバケットをそのまま使う（データの移行なし）。
- 既存のバックアップのオブジェクトを、専用のバケットへ移し、コマンド出力のバケットからは削除する。
- `bigquery-analysis` の設定を、自分のバケットに向ける。
- Terraform の `usecases` に、コマンド出力のバケット名を足す（省略時はプロジェクトとユースケース名から決める）。
- 分離できない共有部分（Firestore のデータベース、Agent Platform の Runtime）を、制限として文書に明記する。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `gke-infrastructure`: バックアップ専用の保存先（ワークロードから読めない）と、ユースケースごとのコマンド出力の保存先（自分のものだけ読み書きできる）を要件に加える。

## Impact

- `terraform/main.tf`、`terraform/variables.tf`、`terraform/outputs.tf`、`terraform/terraform.tfvars.example`
- `usecases/bigquery-analysis/k8s/distributed/steady/config/silk-agent-distributed.yaml`、`usecases/template/...`、`docs/usecase-guide.md`、`docs/distributed-operations-runbook.md`、`usecases/bigquery-analysis/OPERATIONS.md`
- dev の GCP（バケットの作成、IAM の変更、バックアップの移動）。コードの変更はない。
- **BREAKING（運用）**: `bigquery-analysis` の Worker は、`silk-agent` のバケットにある過去のコマンド出力を読めなくなる（まだ運用が始まったばかりで、移行しない）。
