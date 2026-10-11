# Spec Delta

## Purpose

silk-agent の検証用 GKE をこのリポジトリの管理下で再現し、会話用 Sandbox と Bot を動かすための最小構成および運用者が接続するための情報を提供する。

## ADDED Requirements

### Requirement: 単一ゾーンの検証用クラスタ
システムは、指定した Google Cloud プロジェクトの指定した 1 ゾーンに GKE Standard クラスタを構築できなければならない (SHALL)。クラスタをリージョナル構成にしてはならない (SHALL NOT)。

#### Scenario: 既定の検証環境を計画する
- **WHEN** 運用者が既定の入力で Terraform の plan を作成する
- **THEN** クラスタの location はリージョン内の 1 ゾーンになり、リージョナルクラスタや複数ゾーンに分散したノードプールは計画されない

### Requirement: 用途別に 1 ノードずつ配置する
システムは、通常ワークロード用の `system` と gVisor Sandbox 用の `gvisor` の 2 ノードプールを用意し、それぞれの定常ノード数を 1 としなければならない (SHALL)。`gvisor` プールでは Sandbox Pod が gVisor で実行できなければならない (SHALL)。

#### Scenario: クラスタを作成する
- **WHEN** Terraform によりクラスタとノードプールを作成する
- **THEN** `system` と `gvisor` が同じ指定ゾーンに存在し、各プールの目標ノード数は 1 で、`gvisor` RuntimeClass を使用できる

### Requirement: Sandbox の稼働に必要なクラウド基盤を提供する
システムは、クラスタ用ネットワーク、Pod と Service の IP 範囲、プライベートノードの外向き通信、永続ディスクを使うための機能、ノードのイメージ pull 権限、およびイメージの保存先を同じ Terraform 構成で提供しなければならない (SHALL)。クラスタ制御面へのアクセスは設定された CIDR に制限しなければならない (SHALL)。

#### Scenario: Bot と Sandbox のイメージを利用する
- **WHEN** 運用者が新しい保存先へイメージを push し、クラスタへ必要な Kubernetes リソースを適用する
- **THEN** ノードはそのイメージを pull でき、Bot と gVisor Sandbox は永続ボリュームを持つ Pod として起動できる

#### Scenario: 許可された端末から接続する
- **WHEN** 運用者が許可 CIDR 内からクラスタ認証情報を取得する
- **THEN** 制御面へ接続でき、許可 CIDR 外からの接続は拒否される

### Requirement: 自分のクラスタを識別して運用できる
システムは、クラスタ名、ゾーン、ノードプール名、イメージ保存先、クラスタ認証情報の取得方法を運用者に提示しなければならない (SHALL)。デプロイ手順は、このクラスタを対象として識別し、旧検証クラスタの名前やイメージ保存先を前提としてはならない (SHALL NOT)。

#### Scenario: 新しいクラスタへデプロイする
- **WHEN** 運用者が構築結果に従ってクラスタ認証情報を取得し、デプロイ手順を実行する
- **THEN** 対象クラスタが確認され、新しい保存先のイメージを参照する設定でデプロイできる

### Requirement: 既存のクラウドリソースと分離して破棄できる
システムは、旧検証プロジェクトおよび silk-agent の既存アプリ用 Terraform state を取り込まず、GKE 関連リソースを独立した state で管理しなければならない (SHALL)。破棄手順は、保持対象の Kubernetes 永続ディスクへの影響を明示しなければならない (SHALL)。

#### Scenario: 破棄計画を確認する
- **WHEN** 運用者が GKE 関連リソースの破棄を計画する
- **THEN** 旧検証クラスタと既存のアプリ用リソースは対象にならず、残存ディスクの扱いを確認できる
