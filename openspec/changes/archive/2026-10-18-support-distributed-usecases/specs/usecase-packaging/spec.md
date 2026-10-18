# Spec Delta

## MODIFIED Requirements

### Requirement: 適用手順の文書
システムは、ユースケースへの適用手順（Sandboxイメージの作成、`startup.sh`、Skillsとプロンプト、フック、設定、ビルドとデプロイ）を文書として提供し、手順どおりのテンプレートパッケージを含めなければならない (SHALL)。文書とテンプレートは、単一プロセスの配置に加えて、分散構成（Gateway、Worker、Control、Sandbox Service）への配置を扱わなければならない (SHALL)。分散構成の手順は、クラウド資源（Terraform）、イメージ、設定、Kubernetes への適用、リリースと切り戻し、同じクラスタでの複数ユースケースの運用を含まなければならない (SHALL)。

#### Scenario: テンプレートからの適用
- **WHEN** テンプレートパッケージをコピーし、文書の手順に従って名前と設定を書き換えてビルドする
- **THEN** 本体のコードを変更せずに、そのユースケースのBotをデプロイできる

#### Scenario: テンプレートから分散構成で適用する
- **WHEN** テンプレートパッケージをコピーし、文書の分散構成の手順に従って名前・環境の値を書き換え、Terraform の適用、イメージのビルド、Kubernetes への適用を行う
- **THEN** 本体のコードを変更せずに、そのユースケースの Gateway、Worker、Control、Sandbox Service が起動し、Slack から会話できる

## ADDED Requirements

### Requirement: 分散構成でユースケースを配置できる
システムは、環境やユースケースに固有の値を持たない分散構成の共通の配置定義を提供しなければならない (SHALL)。ユースケースは、その共通の定義を参照し、自分の namespace、イメージ、設定、Slack の認証情報だけを指定して配置できなければならない (SHALL)。共通の定義に、特定のプロジェクト、イメージ、Firestore の接頭辞、バケットを書いてはならない (SHALL NOT)。ユースケースの配置で必要な値が指定されていない場合、配置の生成または起動は失敗し、足りない項目を示さなければならない (SHALL)。

#### Scenario: 共通の定義を確認する
- **WHEN** 運用者が共通の配置定義を検索する
- **THEN** 特定のプロジェクト ID、イメージのダイジェスト、Firestore の接頭辞、バケット名が含まれていない

#### Scenario: 値を書き忘れる
- **WHEN** ユースケースの設定で `firestore_prefix` を書かずに配置する
- **THEN** 役割のプロセスは起動せず、`firestore_prefix` が必要であることがエラーとして出る

#### Scenario: 開発環境の既存の配置
- **WHEN** 開発環境の配置を新しい共通の定義から生成する
- **THEN** 生成されたリソースの namespace、名前、Firestore の接頭辞は変更前と同じで、既存の会話をそのまま続けられる

### Requirement: 複数のユースケースを同じクラスタで分離して動かす
システムは、同じクラスタと同じ Firestore の database で、複数のユースケースを互いに分離して動かせなければならない (SHALL)。ユースケースごとに、管理用の namespace、Sandbox 用の namespace、Firestore の接頭辞、Slack アプリの認証情報が分かれていなければならない (SHALL)。あるユースケースの Gateway が受けた入力を、別のユースケースの Worker が処理してはならない (SHALL NOT)。あるユースケースの Worker が、別のユースケースの Sandbox Service や Sandbox に接続できてはならない (SHALL NOT)。あるユースケースの役割の権限で、別のユースケースの Slack の認証情報を読めてはならない (SHALL NOT)。

#### Scenario: 2 つのユースケースを動かす
- **WHEN** ユースケース A と B を同じクラスタに配置し、それぞれの Slack アプリへメッセージを送る
- **THEN** A の会話は A の Worker と A の Sandbox 用 namespace の Sandbox で処理され、B の会話は B の側で処理される。どちらの Firestore の記録にも他方の会話は現れない

#### Scenario: 別のユースケースの Sandbox Service へ接続する
- **WHEN** ユースケース A の Worker の権限で、ユースケース B の Sandbox Service へ要求を送る
- **THEN** 要求は通信の制限または認証で拒否される

#### Scenario: 別のユースケースの認証情報を読む
- **WHEN** ユースケース A の Gateway の権限で、ユースケース B の Slack の認証情報を読もうとする
- **THEN** 読み取りは拒否される

### Requirement: ユースケースごとのクラウド資源を宣言できる
システムは、ユースケースの一覧（名前、namespace、Firestore の接頭辞、Slack の認証情報の保存先）を Terraform の入力として受け取り、ユースケースごとの役割の権限、Slack の認証情報の保存先、Firestore の索引と保持期限の設定を作らなければならない (SHALL)。役割の権限は、そのユースケースの namespace の ServiceAccount だけに与えなければならない (SHALL)。ユースケースを一覧に追加・削除しても、他のユースケースの資源を変更してはならない (SHALL NOT)。

#### Scenario: ユースケースを追加する
- **WHEN** 運用者がユースケースの一覧に 1 件を追加して Terraform の plan を作成する
- **THEN** plan には追加したユースケースの資源の作成だけが出て、既存のユースケースの資源に変更は出ない

#### Scenario: 権限の範囲を確認する
- **WHEN** 運用者が Terraform の出力でユースケース A の Worker の権限を確認する
- **THEN** 権限の主体は A の管理用 namespace の Worker の ServiceAccount だけである
