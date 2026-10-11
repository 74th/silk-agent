# Spec Delta

## Purpose

silk-agent本体を変更・フォークせずに、ユースケースごとのシステムプロンプト、Skills、入力ガードの基準、フック、Sandboxイメージ、設定を別のパッケージとして用意し、組み合わせてデプロイできるようにする。

## ADDED Requirements

### Requirement: ユースケース固有のものを本体の外に置ける
システムは、システムプロンプト、入力ガードの基準、MCPの設定、Skills、フック、Sandboxのテンプレートとイメージを、すべて設定で指定した場所から読み込まなければならない (SHALL)。設定がないときに本体が使う既定は、ユースケースに依存しない汎用のもの（システムプロンプトと入力ガードの基準）に限らなければならない (SHALL)。本体のパッケージとBotのベースイメージには、ユースケース固有のファイル（MCPの設定、Skills、デプロイ先の設定）を含めてはならない (SHALL NOT)。

#### Scenario: 別パッケージのプロンプトを使う
- **WHEN** 設定の`agent.system_prompt_file`にユースケースのパッケージ内の`prompts/agent-system.md`を指定して起動する
- **THEN** エージェントはそのファイルの内容をシステムプロンプトとして使う

#### Scenario: プロンプトファイルがない
- **WHEN** `agent.system_prompt_file`に存在しないファイルを指定して起動する
- **THEN** Botは起動せず、見つからないファイルのパスをエラーとして出す

### Requirement: 設定ファイル内の相対パスは設定ファイルの場所を基準にする
システムは、設定ファイルに書かれた相対パス（システムプロンプト、入力ガードの基準、MCPの設定、ローカルのSkills、Sandboxの設定ファイル）を、その設定ファイルのあるディレクトリを基準に解決しなければならない (SHALL)。絶対パスはそのまま使わなければならない (SHALL)。

#### Scenario: 相対パスの解決
- **WHEN** `/config/silk-agent.yaml`に`input_guard.policy_file: policies/input-guard.md`と書いて起動する
- **THEN** `/config/policies/input-guard.md`が入力ガードの基準として読まれる

### Requirement: 環境固有の値に既定値を持たない
システムは、GCPプロジェクト、Agent Platform Sessionsのインスタンス、Model Armorのテンプレートなど、デプロイ先の環境に固有の値に既定値を持ってはならない (SHALL NOT)。それらを使う機能が有効なのに値が設定されていない場合、Botは起動せず、足りない設定項目の名前をエラーとして出さなければならない (SHALL)。

#### Scenario: プロジェクトの指定漏れ
- **WHEN** `agent.project`を書かずにBotを起動する
- **THEN** Botは起動せず、`agent.project`が必要であることがエラーとして出る

### Requirement: フックを設定で差し込める
システムは、設定の`hooks`に`パッケージ.モジュール:ファクトリ`の形式で書いたフックを、起動時に読み込んで使わなければならない (SHALL)。読み込めない場合は起動してはならない (SHALL NOT)。

#### Scenario: フックの読み込み
- **WHEN** 設定に`hooks: ["my_usecase.hooks:create"]`と書き、そのパッケージをインストールした状態で起動する
- **THEN** `my_usecase.hooks.create`が返したフックにイベントが渡される

#### Scenario: フックのモジュールがない
- **WHEN** インストールされていないモジュールを`hooks`に書いて起動する
- **THEN** Botは起動せず、読み込めなかったフックの名前がエラーとして出る

### Requirement: パッケージのコマンドで起動する
システムは、silk-agentをインストールした環境で、リポジトリのファイル配置に依存せずに、Slack Botを`silk-agent slack`コマンドで起動できなければならない (SHALL)。設定ファイルは環境変数`SILK_AGENT_CONFIG`またはコマンドの引数で指定できなければならない (SHALL)。

#### Scenario: ユースケースのイメージから起動
- **WHEN** silk-agentとユースケースのパッケージをインストールしたイメージで、`SILK_AGENT_CONFIG=/config/silk-agent.yaml`として`silk-agent slack`を実行する
- **THEN** Slack Botがその設定で起動する

### Requirement: 拡張できるベースイメージ
システムは、BotとSandbox Podのそれぞれについて、ユースケースのイメージが`FROM`で拡張できるベースイメージを提供しなければならない (SHALL)。Sandbox Podのベースイメージは、ユースケースがツールを追加し`startup.sh`を置いても、sandboxdとコマンドの管理がそのまま動かなければならない (SHALL)。

#### Scenario: ツールを追加したSandboxイメージ
- **WHEN** Sandbox Podのベースイメージに`git`をインストールし`startup.sh`を置いたイメージを作り、Sandboxの設定に指定する
- **THEN** そのイメージのSandboxで、エージェントが`git --version`を実行できる

### Requirement: 適用手順の文書
システムは、ユースケースへの適用手順（Sandboxイメージの作成、`startup.sh`、Skillsとプロンプト、フック、設定、ビルドとデプロイ）を文書として提供し、手順どおりのテンプレートパッケージを含めなければならない (SHALL)。

#### Scenario: テンプレートからの適用
- **WHEN** テンプレートパッケージをコピーし、文書の手順に従って名前と設定を書き換えてビルドする
- **THEN** 本体のコードを変更せずに、そのユースケースのBotをデプロイできる
