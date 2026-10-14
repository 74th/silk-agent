# Spec Delta

## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Sandbox の稼働に必要なクラウド基盤を提供する
システムは、クラスタ用ネットワーク、Pod と Service の IP 範囲、プライベートノードの外向き通信、永続ディスクを使うための機能、ノードのイメージ pull 権限、およびイメージの保存先を同じ Terraform 構成で提供しなければならない (SHALL)。クラスタ制御面へのアクセスは設定された CIDR に制限しなければならない (SHALL)。

#### Scenario: Bot と Sandbox のイメージを利用する
- **WHEN** 運用者が新しい保存先へイメージを push し、クラスタへ必要な Kubernetes リソースを適用する
- **THEN** ノードはそのイメージを pull でき、分離されたサービスは管理用共有PVCなしで起動でき、gVisor Sandbox は作業用永続ボリュームを持つ Pod として起動できる

#### Scenario: 許可された端末から接続する
- **WHEN** 運用者が許可 CIDR 内からクラスタ認証情報を取得する
- **THEN** 制御面へ接続でき、許可 CIDR 外からの接続は拒否される
