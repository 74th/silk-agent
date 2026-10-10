# Proposal

## Why

Slackから指示すると、Claude Agent SDKのエージェントが、GKE Agent Sandbox上のセッション専用作業環境でツールを実行するBotを作りたい。Sandboxの管理は並行開発中のライブラリ`gke-session-sandbox`が担うので、このリポジトリではその上の「Slack → Bot → Claude Agent SDK → ライブラリ」の部分を作る。入力・出力のガード、中断・割り込み、Botが落ちたことの可視化を最初から組み込む。

## What Changes

- 新規のPythonパッケージ`silk_agent`を作る。コアはportとadapterで構成し、Slack・Streamlit・テスト用のチャネルから同じコアを動かす。
- 会話ごとに`AgentSession`を1つだけ持つ`AgentSessionRegistry`を作り、処理中に追加の書き込みがあってもClaude Agent SDKのインスタンスが二重に起動しないようにする。
- Claude Agent SDK（`ClaudeSDKClient`）を、Agent Platform上のClaudeで動かす。組み込みのファイル・シェル系ツールは無効にし、`gke-session-sandbox`を呼ぶSDK MCPツールに置き換える。MCP設定ファイルと、GCSから同期したSkillsを読み込む。
- Claude Agent SDKの`SessionStore`をAgent Platform Sessionsで実装し、会話を永続化する。管理情報（run state、進捗メッセージの位置）もAgent Platform Sessionsのstateへ保存する。
- 入力は、ルールベースのフィルタの後、Agent Platform上の`gemini-3.8-flash`で指示違反を判定する。
- 出力は、Model Armorのテンプレート`projects/nnyn-dev/locations/us-central1/templates/simple`でsanitizeする。個人情報はマスクして表示し、それ以外のフィルタに該当した出力は表示しない。
- Slackでは、メッセージが「停止」「中止」「中断」「STOP」のいずれかなら実行中の処理を止める。実行中の追加指示は割り込みとして扱う。
- Slackの進捗メッセージに実行したツールの一覧を表示し、「最終更新 YYYY-MM-DD HH:MM」を1分ごとに更新する。Botの再起動後は、中断したturnをユーザに知らせて再開できるようにする。

前提（READMEで確認待ちのため、ここでは仮置きする）:

- 本番のClaudeモデルは設定で指定する（仮に`claude-opus-5-5`）。実モデルを使うテストは`claude-haiku-5-5`で行う。
- Slack Botは1インスタンスで動かす。プロセスをまたいだ排他は作らない。

対象外: README §11の提案機能（ツール結果の検査、入力へのModel Armor、承認フロー、予算、監査ログなど）。`gke-session-sandbox`ライブラリ自体の実装。

## Capabilities

### New Capabilities

- `conversation-core`: チャネルに依存しない会話処理。入力・出力イベント、`AgentSessionRegistry`による1会話1インスタンスの保証、停止・割り込み、turnの状態遷移
- `agent-runtime`: Claude Agent SDKの実行。Agent Platform上のClaude、Sandbox MCPツール、ツール実行イベントのhook、MCP設定、GCSからのSkills同期
- `session-persistence`: Agent Platform SessionsによるSessionStoreと管理情報の保存、Bot再起動後の中断turnの検出と再開
- `input-guard`: ルールベースのメッセージフィルタと、Geminiによる指示違反の判定
- `output-sanitizer`: Model Armorによる出力のsanitize（個人情報のマスクと差し止め）
- `slack-channel`: Slack adapter。反応する条件、停止語、進捗メッセージ（ツール一覧と1分ごとの最終更新）、応答の投稿、正常終了
- `dev-channels`: Streamlitのチャット画面と、ユニットテスト用のScriptedチャネル・fake実装

### Modified Capabilities

（なし。既存のspecはない）

## Impact

- 新規コード: `src/silk_agent/`、`apps/slack_bot.py`、`apps/streamlit_app.py`、`policies/`、`config/`、`tests/`
- 依存パッケージ: `claude-agent-sdk`、`slack-bolt`、`slack-sdk`、`google-genai`、`google-cloud-modelarmor`、Agent Platform Sessionsのクライアント（`google-cloud-agentplatform`または`google-cloud-aiplatform`）、`google-cloud-storage`、`streamlit`、`gke-session-sandbox`（ローカルパス参照）
- GCPリソース（nnyn-dev）: Agent Runtimeインスタンス、Agent Platform上のClaude・Geminiの利用、Model Armorテンプレート`simple`、Skills用のGCSバケット、Secret Manager（Slackトークン）
- Slackアプリ: Socket Mode、`app_mentions:read`、`channels:history`、`groups:history`、`im:history`、`chat:write`、`files:write`などのスコープ
