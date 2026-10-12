# Spec Delta

## Purpose

会話別 Sandbox が必要な場合に限り、専用の Kubernetes 身元と宛先を明示して Google Cloud API にアクセスできる構成を提供する。

## ADDED Requirements

### Requirement: Sandbox のクラウド認証を分離する
BigQuery ユースケースは、Terraform で専用の Google Cloud Service Account、BigQuery ジョブ実行権限、専用 Kubernetes ServiceAccount だけに与える Workload Identity のなりすまし許可を管理しなければならない (SHALL)。Sandbox Pod はその Kubernetes ServiceAccount を使用し、Bot の認証情報を共有してはならない (SHALL NOT)。権限のない別の Sandbox に、その権限を適用してはならない (SHALL NOT)。静的なサービスアカウント鍵をイメージやワークスペースへ含めてはならない (SHALL NOT)。

#### Scenario: BigQuery 用 Sandbox を起動する
- **WHEN** Terraform で BigQuery 用 GSA と IAM 設定を適用し、対応する KSA を Kubernetes に適用してから会話別 Sandbox を作成する
- **THEN** Pod はその KSA で起動し、Workload Identity を通じて BigQuery ジョブ実行権限を持つ GSA として API を呼べる

#### Scenario: KSA と GSA の関連付けを確認する
- **WHEN** 管理者が Terraform の出力と Kubernetes に適用された KSA を確認する
- **THEN** KSA の GSA 注釈と Terraform が許可した namespace・KSA 名が一致する

### Requirement: 外向き通信を明示的に許可する
Sandbox の既定の外向き通信制限を維持し、BigQuery API と認証に必要な宛先だけをユースケース設定で許可できなければならない (SHALL)。メタデータサーバへのアクセスは明示的な設定がある Sandbox に限り許可しなければならない (SHALL)。

#### Scenario: BigQuery ユースケースの通信
- **WHEN** 管理者が BigQuery API とメタデータサーバへの送信を明示的に許可する
- **THEN** その会話の Sandbox から必要な宛先へ通信でき、それ以外の宛先は従来どおり拒否される

#### Scenario: 既定の構成
- **WHEN** メタデータサーバへの送信許可を設定しない
- **THEN** 生成される NetworkPolicy はその宛先への送信を許可しない
