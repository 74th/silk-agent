# Proposal

## Why

silk-agent の検証用 GKE は別プロジェクトの Terraform に依存していたが、その検証環境は破棄中である。silk-agent 自身が必要なクラスタを再作成できるようにし、リージョナルクラスタで生じていた検証用途には過剰なノード数と費用を抑える。

## What Changes

- このリポジトリの `terraform/` に、単一ゾーンの GKE Standard クラスタを定義する。
- 通常ワークロード用の `system` と Sandbox 用の gVisor 有効な `gvisor` の 2 ノードプールを作り、各プールの定常ノード数を 1 にする。
- クラスタに必要な VPC、サブネットと Pod/Service のセカンダリレンジ、Cloud NAT、ノード ID、Artifact Registry、API 有効化を、このリポジトリ内の専用 Terraform root にまとめる。
- 新クラスタの出力、構築・破棄手順、Kubernetes 側の事前準備、および旧クラスタ名・レジストリを参照するデプロイ設定を更新する。

## Capabilities

### New Capabilities

- `gke-infrastructure`: silk-agent の検証用 GKE とそれに必要なクラウド基盤を、このリポジトリから再現可能に構築する。

### Modified Capabilities

なし。既存の `agent-runtime` は Sandbox での実行を規定しているが、クラスタの所有・構成要件は規定していない。

## Impact

- `terraform/gke/` に、既存のアプリ用 `terraform/` と state を分けた GKE 関連リソースを追加する。
- `k8s/`、`scripts/`、`README.md` の検証用クラスタ名、イメージ参照先、セットアップ手順に影響する。
- 旧クラスタの Terraform state や残存ディスクは取り込まない。新クラスタへの Kubernetes リソースの適用とイメージの push は別途必要になる。
