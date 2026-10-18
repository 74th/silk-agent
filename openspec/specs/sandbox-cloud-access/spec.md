# Sandbox Cloud Access Specification

## Purpose

会話別 Sandbox が必要な場合に限り、専用の Kubernetes 身元と宛先を明示して Google Cloud API にアクセスできる構成を提供する。

## Requirements

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

### Requirement: 既定の通信制限の下で名前解決できる
Sandbox は、外向き通信を既定で拒否したまま、クラスタ DNS で名前を解決できなければならない (SHALL)。クラスタがノード上の DNS キャッシュ（NodeLocal DNSCache）を使う場合も、使わない場合も、名前解決が成功しなければならない (SHALL)。DNS のために許可する宛先は、管理者が設定したクラスタ DNS の応答元の 53 番ポート（UDP/TCP）に限らなければならない (SHALL)。DNS の許可によって、他の Pod・ポート・外部宛先への通信を許可してはならない (SHALL NOT)。

#### Scenario: NodeLocal DNSCache が有効なクラスタ
- **WHEN** NodeLocal DNSCache が有効なクラスタで、既定の通信設定の Sandbox を作成し、Sandbox 内でクラスタ内外の名前を解決する
- **THEN** 名前解決は成功する

#### Scenario: NodeLocal DNSCache が無効なクラスタ
- **WHEN** NodeLocal DNSCache が無効なクラスタで、既定の通信設定の Sandbox を作成し、名前を解決する
- **THEN** 名前解決は成功する

#### Scenario: DNS 以外の通信
- **WHEN** 許可リストが空の Sandbox から、名前解決できた外部宛先へ TCP 443 で接続する、または DNS の応答元へ 53 番以外のポートで接続する
- **THEN** 接続は拒否される

#### Scenario: FQDN 許可リストと組み合わせる
- **WHEN** 管理者が `allowed_fqdns` に宛先を 1 つ設定して Sandbox を作成する
- **THEN** その宛先は名前解決でき、設定したポートへ接続できる。許可していない宛先への接続は拒否される

### Requirement: 通信設定の変更を既存の Sandbox に反映する
システムは、Sandbox を再開するとき、その会話の通信制限（NetworkPolicy と FQDN の許可）を現在の設定から作り直した内容に合わせなければならない (SHALL)。運用者は、稼働中の Sandbox にも、再起動せずに現在の設定を再適用できなければならない (SHALL)。再適用は、その会話・世代が所有する policy だけを変更しなければならない (SHALL)。他の会話や、所有者の一致しない policy を変更してはならない (SHALL NOT)。

#### Scenario: 修正前に作成された Sandbox を再開する
- **WHEN** DNS の許可が古い policy で作成され一時停止している Sandbox を、修正後の Sandbox Service で再開する
- **THEN** 再開後の Sandbox の policy は現在の設定と一致し、名前解決が成功する

#### Scenario: 稼働中の Sandbox に再適用する
- **WHEN** 運用者が稼働中の Sandbox に通信設定の再適用を実行する
- **THEN** その Sandbox の policy が現在の設定に更新され、Pod は再起動されない

#### Scenario: 所有者が一致しない policy
- **WHEN** 同じ名前の policy が別の会話または世代の所有になっている
- **THEN** 再適用はその policy を変更せず、失敗として報告する

### Requirement: 名前解決と通信制限を検証できる
システムは、運用者が指定した Sandbox の中で、名前解決、許可した宛先への接続、許可していない宛先の遮断を確認し、それぞれの成否を表示する手段を提供しなければならない (SHALL)。検証は Sandbox の作業領域や会話の状態を変更してはならない (SHALL NOT)。

#### Scenario: 名前解決ができない Sandbox を検証する
- **WHEN** 運用者が名前解決できない Sandbox に対して検証を実行する
- **THEN** 名前解決の失敗が、使用した DNS サーバの情報とともに表示され、検証は失敗として終了する

#### Scenario: 正常な Sandbox を検証する
- **WHEN** 運用者が正常な Sandbox に対して、許可した宛先と許可していない宛先を指定して検証を実行する
- **THEN** 名前解決と許可した宛先への接続が成功し、許可していない宛先は遮断されたと表示される
