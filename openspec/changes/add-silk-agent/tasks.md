# Tasks

## 1. プロジェクトの準備

- [x] 1.1 `pyproject.toml`（Python 3.12以上、uv、依存パッケージ、`gke-session-sandbox`のpath依存、pytestの`integration` marker、ruff）と`src/silk_agent/`の骨組みを作り、`uv sync`と`uv run pytest`（0件）が成功することを確認する
- [x] 1.2 設定ファイルの型（Pydantic）と読み込み処理を作り、README §10の設定例を`config/silk-agent.example.yaml`として読み込めることをユニットテストで確認する

## 2. コア（conversation-core）

- [x] 2.1 入力・出力イベント型とport（`ChatChannel`、`AgentEngine`、`SandboxRuntime`、`SessionStateStore`、`InputGuard`、`OutputSanitizer`、`SkillSource`、`Clock`）を定義し、`ruff`と型チェックが通ることを確認する
- [x] 2.2 テスト用の代替実装（`ScriptedChannel`、`FakeAgentEngine`、`InMemoryRuntime`、in-memoryの`SessionStateStore`、`FakeClock`、`AllowAllGuard`、`NoopSanitizer`）を作り、シナリオどおりのイベント列が返ることをユニットテストで確認する
- [x] 2.3 `AgentSessionRegistry`と`AgentSession`（入力queue、worker task、状態遷移）を作り、「処理中の書き込みで新しいインスタンスが起動しない」「同時に届いた最初のメッセージでセッションが1つ」「同じ会話は順に処理」のテストが通ることを確認する
- [x] 2.4 停止と割り込み（interrupt / queueモード）、リセットを実装し、conversation-coreのspecの停止・割り込み・リセットのシナリオのテストが通ることを確認する
- [x] 2.5 turn実行中のheartbeatを実装し、`FakeClock`で150秒進めるとheartbeatが2回出て、完了後は出ないことをテストで確認する
- [x] 2.6 `ConversationOrchestrator`（フィルタ → 停止語 → 入力ガード → AgentSession → 出力sanitize → チャネル）を作り、メッセージからturn完了までのイベント順のテストが通ることを確認する

## 3. ガード（input-guard / output-sanitizer）

- [x] 3.1 `MessageFilter`チェーン（送信者、許可リスト、空・長さ上限、レート）を実装し、input-guardのspecのフィルタのシナリオのテストが通ることを確認する
- [x] 3.2 `policies/input-guard.md`と`GeminiPolicyGuard`（構造化出力、直前5メッセージの文脈、失敗時のblock / allow設定）を実装し、google-genaiをモックしたユニットテストで許可・ブロック・要確認・失敗時の扱いを確認する
- [x] 3.3 `ModelArmorSanitizer`（SDPだけ該当ならマスク、それ以外は差し止め、マスク後テキストなしは差し止め、長文の分割、失敗時の差し止め）を実装し、Model Armorのレスポンスを固定値にしたユニットテストで各分岐を確認する
- [x] 3.4 Gemini（`gemini-3.8-flash`、location `global`）とModel Armor（テンプレート`simple`）へ実際に接続するintegrationテストを作り、上書き要求の入力がブロックされ、メールアドレスを含む出力がマスクされることを`uv run pytest -m integration`で確認する

## 4. エージェント（agent-runtime）

- [x] 4.1 `ClaudeAgentEngine`（`ClaudeSDKClient`、Agent Platform用のenv、hookから出力イベントへの変換、interrupt → 読み切り → query）を実装し、SDKをモックしたユニットテストでイベント変換を確認する
- [x] 4.2 Sandbox MCPツール（bash、read_file、write_file、edit_file、list_files、glob、grep、command_status、command_output）と組み込みツールの無効化を実装し、`InMemoryRuntime`を使ったユニットテストで各ツールと「結果不明時に再送しない」動作を確認する
- [x] 4.3 `LocalProcessRuntime`（会話ごとの一時ディレクトリ、開発専用）を実装し、会話間でファイルが分離されることをテストで確認する
- [x] 4.4 MCP設定ファイルの読み込み（`strict_mcp_config=True`、`${ENV}`の展開、HTTP / SSEのみ許可）を実装し、作業ディレクトリの`.mcp.json`が読まれないことをテストで確認する
- [x] 4.5 `LocalDirSkillSource`と`GcsSkillSource`（世代ID、世代の切り替え、turn中は固定、cwdへのsymlink、Sandboxへのコピー）を実装し、GCSをモックしたテストで更新が次のturnから反映されることを確認する
- [x] 4.6 Agent Platform上の`claude-haiku-5-5`と`LocalProcessRuntime`でintegrationテストを作り、ツール呼出し、ツール実行イベント、interruptが動くことを確認する。あわせて、実行中に積んだメッセージの扱いをこのテストで調べ、結果をdesign.mdに記録する

## 5. 永続化（session-persistence）

- [x] 5.1 `AgentPlatformSessionStore`（`append` / `load`、UUIDからsession_idへの変換、`raw_event`、uuidでの重複除去、seq順、TTL）を実装し、client をモックしたユニットテストで重複除去と順序を確認する
- [x] 5.2 `run_session_store_conformance`をAgent Platform Sessionsに対して実行するintegrationテストを作って通し、1 eventのサイズ上限を測ってdesign.mdに記録する
- [x] 5.3 Agent Platform Sessions上の`SessionStateStore`（管理用session、追記ログ、`list_running`）を実装し、integrationテストで保存と列挙を確認する
- [x] 5.4 `bot_instance_id`、起動時の中断検出（`interrupted(crash)`）、SIGTERM時の猶予と中断保存（`interrupted(shutdown)`）を実装し、in-memoryのstoreと`FakeAgentEngine`で「クラッシュ後の起動で中断が通知される」「自動では再開しない」のテストが通ることを確認する
- [x] 5.5 中断した会話の再開（中断の通知とコマンドIDの文脈付与）を実装し、Haiku 5.5のintegrationテストで「再起動後に会話を継続できる」ことと、`tool_use`の途中で切れたtranscriptからresumeできるかを確認する（できなければ`load`での補完を実装する）
- [x] 5.6 `mirror_error`をturn完了の警告として出す処理を実装し、`append`が失敗し続けるstoreで警告が付くことをテストで確認する

## 6. Slack（slack-channel）

- [x] 6.1 Bolt（`AsyncApp` + Socket Mode）の`SlackChannel`（メンション・参加スレッド・DM、会話キー、event_idでの重複排除、bot投稿の除外）を実装し、Slackのイベントpayloadを使ったユニットテストで反応する条件を確認する
- [x] 6.2 停止語の判定（NFKC正規化、前後の空白、大文字化、完全一致、設定で変更可）を実装し、slack-channelのspecの停止語のシナリオのテストが通ることを確認する
- [x] 6.3 `ProgressView`と進捗メッセージ（投稿、1秒に1回までの`chat.update`、ツール一覧の最新20件と残り件数、MCPツールの表示、「最終更新」の1分更新、完了・中断の表示、更新失敗時の再投稿）を実装し、`FakeClock`とモックしたSlack clientで「5分のツール実行中に最終更新が1分ごとに変わる」ことをテストで確認する
- [x] 6.4 応答の投稿（テキストブロック単位、12,000文字超はファイルとしてアップロード）と、再起動後の「Botの停止により中断」の表示を実装し、モックしたSlack clientでテストする
- [ ] 6.5 `apps/slack_bot.py`（設定、Secret Managerからのトークン取得、起動時の中断検出、SIGTERMの処理）を作り、テスト用のworkspaceで「メンション → ツール実行の表示 → 応答」「停止」「Botの強制終了 → 最終更新が止まる → 再起動で中断表示」を手動で確認する

## 7. Streamlit（dev-channels）

- [x] 7.1 `StreamlitChannel`と`apps/streamlit_app.py`（応答、`st.status`でのツール表示、停止語）を作り、`uv run streamlit run apps/streamlit_app.py`で依頼と停止が動くことを手動で確認する

## 8. Sandbox（GKE）

- [x] 8.1 `GkeSessionSandboxRuntime`（`SandboxManager.ensure`、`USER_EMAIL`のenv、command / file APIへの対応、`pending_context_events`の反映とack）を実装し、`gke-session-sandbox`をモックしたユニットテストで通知が次のturnに反映されることを確認する
- [ ] 8.2 `gke-session-sandbox`のcommand / file APIの実装後に、検証用GKEクラスタでSlack（またはStreamlit）から「ファイル作成 → コマンド実行 → 別の会話から見えない」をE2Eで確認する

## 9. 仕上げ

- [x] 9.1 READMEに、セットアップ手順（GCPリソース、Slackアプリのスコープ、設定、起動方法、テストの実行方法）を追記し、手順どおりにStreamlitを起動できることを確認する
- [x] 9.2 `uv run pytest`（ユニット）、`uv run pytest -m integration`、`ruff check`がすべて通ることを確認する
