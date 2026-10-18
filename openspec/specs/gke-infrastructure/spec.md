# GKE Infrastructure Specification

## Purpose

silk-agent の検証用 GKE をこのリポジトリの管理下で再現し、会話用 Sandbox と Bot を動かすための最小構成および運用者が接続するための情報を提供する。

## Requirements

### Requirement: 単一ゾーンの検証用クラスタ
システムは、指定した Google Cloud プロジェクトの指定した 1 ゾーンに GKE Standard クラスタを構築できなければならない (SHALL)。クラスタをリージョナル構成にしてはならない (SHALL NOT)。

#### Scenario: 既定の検証環境を計画する
- **WHEN** 運用者が既定の入力で Terraform の plan を作成する
- **THEN** クラスタの location はリージョン内の 1 ゾーンになり、リージョナルクラスタや複数ゾーンに分散したノードプールは計画されない

### Requirement: 用途別のノードプールを自動調整する
システムは、通常ワークロード用の `system` と gVisor Sandbox 用の `gvisor` の 2 ノードプールを用意し、それぞれ最小 1 ノード、最大 10 ノードで自動調整しなければならない (SHALL)。`gvisor` プールでは Sandbox Pod が gVisor で実行できなければならない (SHALL)。

#### Scenario: クラスタを作成する
- **WHEN** Terraform によりクラスタとノードプールを作成する
- **THEN** `system` と `gvisor` が同じ指定ゾーンに存在し、各プールで 1～10 ノードの自動調整が有効になり、`gvisor` RuntimeClass を使用できる

#### Scenario: ワークロードの需要が減る
- **WHEN** ワークロードの Pod を各プールのより少ないノードへ収容できるようになる
- **THEN** 各プールは自動的にスケールインし、1 ノードより少なくはならない

### Requirement: Sandbox の稼働に必要なクラウド基盤を提供する
システムは、クラスタ用ネットワーク、Pod と Service の IP 範囲、プライベートノードの外向き通信、永続ディスクを使うための機能、ノードのイメージ pull 権限、およびイメージの保存先を同じ Terraform 構成で提供しなければならない (SHALL)。クラスタ制御面へのアクセスは設定された CIDR に制限しなければならない (SHALL)。

#### Scenario: Bot と Sandbox のイメージを利用する
- **WHEN** 運用者が新しい保存先へイメージを push し、クラスタへ必要な Kubernetes リソースを適用する
- **THEN** ノードはそのイメージを pull でき、分離されたサービスは管理用共有PVCなしで起動でき、gVisor Sandbox は作業用永続ボリュームを持つ Pod として起動できる

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

### Requirement: 共有 Worker の分離配置
システムは、Slack受信、共有Worker、割当・復旧・送信、Sandbox管理を独立して起動できなければならない (SHALL)。分散配置はBotのSQLite用共有PVCを必要としてはならない (SHALL NOT)。SDKとSandboxの通信・権限境界を維持しなければならない (SHALL)。

#### Scenario: Workerの増設
- **WHEN** 運用者がWorker数を1から2へ増やす
- **THEN** 同じ共有状態を参照して会話を分担し、SandboxへのEgress許可やSDK権限を広げない

### Requirement: 共有管理ストアの構築と移行
システムは、Firestoreの設定、必要な権限・索引、既存データの移行・検証手順を提供しなければならない (SHALL)。既存DBを無断で置換したり、旧系と新系を同じ会話の正本として同時稼働させたりしてはならない (SHALL NOT)。

#### Scenario: 既存環境の切替
- **WHEN** 旧Botと管理処理を停止して移行する
- **THEN** 会話キー、履歴参照、Sandbox世代、Pod/PVC、開始者が維持され、照合結果を確認してから新系を起動できる

### Requirement: コマンドを止めない計画的縮退
システムは、Workerの新規割当を停止し、会話を解放してからPod数を減らす手順と、その進行状況を提供しなければならない (SHALL)。Workerの縮退を理由にSandboxのコマンドを取り消してはならない (SHALL NOT)。

#### Scenario: バックグラウンド実行中の縮退
- **WHEN** Workerをdrainし、SDK turnが完了した状態でPodを削除する
- **THEN** 同じSandboxとcommand_idでコマンドが継続し、別Workerが後の入力・完了通知を処理する

#### Scenario: 通常の縮退が完了しない
- **WHEN** 担当会話が実行中でdrainを完了できず、期限が指定されていない
- **THEN** Pod削除を待機し、運用者は残る会話数を確認できる

### Requirement: Sandbox の一時停止と期限削除を Sandbox 管理サービスが実行する
Sandbox 管理サービスは、使われていない Sandbox の一時停止と、保持期間を過ぎた Sandbox の削除を、API 要求の寿命から独立して継続的に実行しなければならない (SHALL)。一時停止は設定 `lifecycle.idle_timeout_seconds`（既定300秒）、削除は一時停止時刻から `lifecycle.retention_seconds`（既定7日）を基準とし、期限は管理 DB の `expires_at` と Sandbox リソースの `spec.shutdownTime`（`shutdownPolicy: Delete`）に記録しなければならない (SHALL)。判定の間隔は `lifecycle.reconcile_interval_seconds`（既定60秒）とする。実行中のコマンドを持つ Sandbox を、idle を理由に停止してはならない (SHALL NOT)。複数の Sandbox 管理サービスが動いても、同じ Sandbox を二重に停止・削除してはならない (SHALL NOT)。この処理が起動していない構成を、正常な配置として許してはならない (SHALL NOT)。

#### Scenario: 使われなくなった Sandbox
- **WHEN** Sandbox が `idle_timeout_seconds` を超えて使われず、実行中のコマンドもない
- **THEN** 一時停止され（Pod は削除、PVC は保持）、`expires_at` と `spec.shutdownTime` が設定され、次の依頼で同じ PVC から再開できる

#### Scenario: 実行中のコマンドがある
- **WHEN** Sandbox が `idle_timeout_seconds` を超えて使われていないが、コマンドが実行中である
- **THEN** 停止されず、コマンドは同じ command_id で継続する

#### Scenario: 保持期間の経過
- **WHEN** 一時停止から `retention_seconds` が経過する
- **THEN** Sandbox と PVC が削除され、管理 DB の記録が期限切れとして整合する

#### Scenario: 処理が起動していない
- **WHEN** Sandbox 管理サービスが reconciler を起動していない設定（`run_reconciler: false`）で配置される
- **THEN** 配置の検査（manifest の試験）がこれを検出し、Sandbox と PVC が回収されないまま増え続ける構成を受け入れない

### Requirement: クラスタ DNS の構成を宣言して管理する
システムは、クラスタの NodeLocal DNSCache の有効・無効を Terraform の入力で宣言し、適用できなければならない (SHALL)。Terraform の構成と実際のクラスタの DNS 構成に差分を残してはならない (SHALL NOT)。Sandbox の DNS 許可の設定は、宣言したクラスタ DNS の構成で名前解決できる内容でなければならない (SHALL)。

#### Scenario: 現在のクラスタを計画する
- **WHEN** 運用者が NodeLocal DNSCache を有効にした入力で、既存のクラスタに対して Terraform の plan を作成する
- **THEN** DNS キャッシュの設定に差分は出ない

#### Scenario: 設定を切り替える
- **WHEN** 運用者が NodeLocal DNSCache の入力を変更して plan を作成する
- **THEN** plan にはクラスタの DNS キャッシュ設定の変更が示され、適用後も既定の通信設定の Sandbox は名前解決できる
