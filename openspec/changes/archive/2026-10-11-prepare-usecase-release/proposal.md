# Proposal

## Why

silk-agentを実際のユースケースに使うには、そのユースケース専用のSandboxイメージ（必要なツール入り）、Skillsとシステムプロンプト、監査用のイベントログ送信を用意する必要がある。しかし現状は、プロンプトのパス（`policies/agent-system.md`）、GCPプロジェクト（`nnyn-dev`）の既定値、Botイメージへ同梱する設定などがこのリポジトリに直接埋め込まれており、ユースケースごとにフォークして書き換えるしかない。リリースして使い回せるよう、本体とユースケースへの適用部分を分け、適用手順をまとめる。あわせて、Slackで応答する相手を絞る設定と、Sandboxの起動時に任意の処理を差し込む仕組みを加える。

## What Changes

- **ユースケース適用パッケージの分離**: silk-agent本体をライブラリ兼ベースイメージとして使えるようにし、ユースケース固有のもの（Sandboxイメージ、`startup.sh`、Skills、システムプロンプト、入力ガードの基準、フックの実装、設定、k8sマニフェスト）を別のPythonパッケージ（テンプレートを`usecases/template/`に置く）にまとめられるようにする。
  - 設定に`agent.system_prompt_file`と`hooks`（`module:factory`の形式）を追加する。
  - 設定ファイル内の相対パスは、**設定ファイルのあるディレクトリ**を基準に解決する。**BREAKING**（従来はリポジトリのルート基準。`policy_file: policies/input-guard.md`などを書き換える）
  - GCPプロジェクトなど環境固有の値（`nnyn-dev`）の既定値をなくし、必須の設定にする。**BREAKING**
  - 起動を`apps/slack_bot.py`から、パッケージのコマンド`silk-agent slack`（`python -m silk_agent slack`）に移す。`apps/`のスクリプトはこれを呼ぶだけにする。
  - Skillsは、Botイメージに同梱したローカルディレクトリからも読めることを仕様として明記する。
- **イベントフック**: プロンプトの入力（フィルタ・ガードの結果を含む）、turnの開始と終了、ツールの実行（入力と結果）、エージェントの返答、停止、ガードによるブロックを、ユースケースが実装するフック関数のセットに渡す。フックは観測専用で、失敗・遅延がturnに影響しない。標準でJSON Linesをログに出すフックを同梱する。
- **Sandboxの起動スクリプト**: `gke_session_sandbox.supervisor`が、コンテナ起動時（作成・再開のたび）に、イメージ内の`/opt/session-runtime/startup.sh`があれば`AGENT_USER_EMAIL`などのコンテナの環境変数を引き継いで実行し、終わってからsandboxdを起動する。失敗時の扱い、上限時間を設定できる。Sandboxの準備完了を待つ時間を設定できるようにする。
- **Sandboxに渡すメールアドレス**: 環境変数名を`USER_EMAIL`から`AGENT_USER_EMAIL`に変える。**BREAKING**（Sandbox内でこの変数を使うもの）。値は会話を始めたユーザに固定し、`startup.sh`とエージェントのコマンド（sandboxd経由。バックグラウンド、再開後を含む）の両方から参照できることを仕様にする。
- **Slackの応答範囲の設定**:
  - 既存の`filter.allowed_users`（ユーザIDのリスト）と`filter.allowed_channels`（チャンネルIDのリスト）を仕様として明確にする。`allowed_channels`を設定しても、DMを受け付けるかは`filter.allow_direct_messages`（既定`true`）で別に決める。
  - `filter.deny_guests`（ゲスト = マルチチャンネル／シングルチャンネルゲスト）と`filter.deny_external_users`（Slackコネクトなど別のworkspaceのユーザ）を追加する。既定は`false`。
- **リファクタリング**（リリースの妨げになるものをこの変更で行う）:
  - 本番コードが`silk_agent.testing.fakes`から`SystemClock`、`InMemoryStateStore`、`AllowAllGuard`、`NoopSanitizer`を読み込んでいるのを、本番用のモジュールへ移す。
  - `slack_app.py`が`SlackChannel`の非公開属性（`_state_store`）を書き換えているのをやめ、正規の受け渡しにする。
  - `CommandTracker`が`wiring.py`と`app.py`で別々に作られているのを1つにする。
  - Sandbox Podイメージのビルドで、`gke_session_sandbox`から手で列挙したファイルだけをコピーしている部分と、イメージ内の`protobuf>=7`（本体は`<7`）の食い違いを解消し、ユースケースのイメージが`FROM`できるベースイメージとして公開できるようにする。
  - 今回は行わない改善候補はdesign.mdに挙げる。
- 適用手順を`docs/usecase-guide.md`にまとめ、READMEから参照する。

## Capabilities

### New Capabilities

- `usecase-packaging`: 本体とユースケース適用部分の分離（設定・プロンプト・Skills・フックの差し込み、ベースイメージ、相対パスの解決、起動コマンド）
- `event-hooks`: 入力・turn・ツール・返答などのイベントをユースケースのフック関数へ渡す
- `sandbox-startup`: Sandboxコンテナの起動時に`startup.sh`を実行する

### Modified Capabilities

- `input-guard`: ルールベースのフィルタに、DMの扱い、ゲスト・外部ユーザの除外を加える
- `agent-runtime`: Skillsの読み込み元にローカルディレクトリを加え、システムプロンプトを設定したファイルから読む

## Impact

- コード: `src/silk_agent/config.py`、`wiring.py`、`app.py`、`slack_app.py`、`channels/slack*.py`、`core/filters.py`、`core/events.py`、`core/orchestrator.py`、`agent/engine.py`、`testing/fakes.py`、新規`src/silk_agent/hooks.py`・`__main__.py`・`cli.py`、`src/gke_session_sandbox/supervisor.py`・`config.py`・`backend.py`
- イメージ・デプロイ: `Dockerfile`、`runtime/session-runtime.Dockerfile`、`scripts/build-session-runtime-image.sh`、`k8s/config/*.yaml`、`config/silk-agent.example.yaml`
- 新規: `usecases/template/`（テンプレートパッケージ）、`docs/usecase-guide.md`
- Slackアプリ: ゲスト・外部ユーザの判定に`users:read`（既存のスコープ）で`users.info`を使う
- 既存のデプロイは、設定ファイルの相対パスとGCPプロジェクトの指定を書き換える必要がある
