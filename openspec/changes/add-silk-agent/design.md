# Design

## Context

- リポジトリはまだ空（openspecの設定とREADMEだけ）。READMEが全体の仕様案で、このdesignはその実装方針を決める。
- Sandboxは`gke-session-sandbox`（`../test-google-cloud-agent-platform/20261010-gke-agent-sandbox-with-sandboxd`）を使う。現在は`SandboxManager.ensure / get / suspend / resume / terminate`まで実装済み。command / file API（CommandTracker）はまだ実装中。
- Claude Agent SDK（Python、`claude-agent-sdk` 0.2.165時点）には、`ClaudeSDKClient.interrupt()`、hook（PreToolUse / PostToolUse / PostToolUseFailure）、SDK MCPサーバ（`create_sdk_mcp_server`）、差し替え可能な`SessionStore`（`append` / `load`）がある。
- Agent Platform Sessionsは、Agent Runtimeインスタンスの下にsessionを作り、eventを`events.append`（`raw_event`で任意データを保存可）で追加し、`events.list`で読む。
- Slack Botは1インスタンスで動かす。

## Goals / Non-Goals

**Goals:**
- コアをSlack・GCP・Sandboxから切り離し、ユニットテストを外部接続なしで動かせるようにする。
- `gke-session-sandbox`のcommand APIが未完成でも、他の部分を先に作って動かせるようにする。

**Non-Goals:**
- 複数レプリカでの動作（会話のlease、イベントの転送）。
- 文字単位のストリーミング表示（`chat.startStream`）。
- README §11の提案機能。

## Decisions

### D1. Port / adapter構成

`src/silk_agent/core`にイベント型とport（`typing.Protocol`）を置き、外部とつながる実装は`channels/`、`agent/`、`guards/`、`stores/`、`skills/`、`sandbox/`に分ける。portは`ChatChannel`、`AgentEngine`、`SandboxRuntime`、`SessionStateStore`、`InputGuard`、`OutputSanitizer`、`SkillSource`、`Clock`。SDKの`SessionStore`はSDKのプロトコルをそのまま使う。

- 代替案: Slack Boltの上に直接書く → Streamlit・テストから再利用できないため不採用。

### D2. 1会話1インスタンス: `AgentSessionRegistry` + 会話ごとのworker task

- `AgentSessionRegistry`は`dict[conversation_key, AgentSession]`と`asyncio.Lock`を持ち、`get_or_create`だけを公開する。
- `AgentSession`は`asyncio.Queue`と1つのworker taskを持ち、入力はすべてqueueへ入れる。`ClaudeSDKClient`を作るのはworkerだけなので、二重起動が起きない。
- **停止と割り込みはqueueを経由しない。** 実行中のturnを止める必要があるため、`AgentSession.stop()` / `interrupt_with(message)`として直接呼び、workerに「中断」を通知する。
- アイドル（既定10分）になったら`ClaudeSDKClient.disconnect()`する。`AgentSession`自体は軽いので残す。
- 代替案: Slackのスレッドごとにロックする → Streamlitやテストで同じ仕組みを使えないため不採用。

### D3. 割り込みは「interrupt → 読み切り → query」

`ClaudeSDKClient.interrupt()`を呼び、`receive_response()`で`ResultMessage`まで読み捨ててから、新しいメッセージを`query()`する。SDKのストリーミング入力にメッセージを積む方式は、現在のturnに注入されるのかどうかが公式に明記されていないため、既定にはしない。`interrupt_mode=queue`の場合は、メッセージをqueueに積んで現在のturnの完了を待つ。

**検証結果（2026-10-10、`claude-haiku-5-5`、claude-agent-sdk 0.2.165）**: turn実行中（`bash`で`sleep 25`を実行中の8秒時点）に`query()`で2通目を送ると、2通目は**現在のturnに注入された**。ツールの完了後の次のモデル呼出しで取り込まれ、`ResultMessage`は1つだけ（2通目のための新しいturnは始まらない）で、応答には両方の指示への回答が含まれた。つまり、ストリーミング入力に積む方式は「割り込み」ではなく「実行中のturnへの追記」になる。実行中のツールは止まらないため、停止・方針変更の用途には向かない。既定は引き続き「interrupt → 読み切り → query」とする。追記の方式は、将来`interrupt_mode=append`として追加できる（今回は実装しない）。

### D4. AgentEngineのイベント変換

`ClaudeAgentEngine`はSDKのメッセージとhookを、コアの出力イベントに変換する。

- `AssistantMessage`の`TextBlock` → `AssistantText`（ブロック単位）
- PreToolUse hook → `ToolStarted`（`tool_use_id`、ツール名、ツールごとの要約関数）
- PostToolUse / PostToolUseFailure hook → `ToolFinished`（開始時刻との差分で所要時間を出す）
- `ResultMessage` → `TurnFinished`（usage、cost）
- `SystemMessage(mirror_error)` → turnに警告フラグを立てる

`FakeAgentEngine`は、シナリオ（`[Text("..."), Tool("bash", {...}, result=...), ...]`）を受け取り、同じイベント列を返す。中断にも対応する（ツールの途中で止められる）。

### D5. Claude on Agent Platform

`ClaudeAgentOptions.env`に`CLAUDE_CODE_USE_VERTEX=1`、`ANTHROPIC_VERTEX_PROJECT_ID`、`CLOUD_ML_REGION`を渡す。認証はADC（本番ではWorkload Identity）。モデルIDは設定から渡し、integrationテストでは`claude-haiku-5-5`に固定する。

### D6. Sandboxツール

`create_sdk_mcp_server("sandbox", tools=[bash, read_file, write_file, edit_file, list_files, glob, grep, command_status, command_output])`を作る。ツールは`SandboxRuntime` portだけに依存する。

- 実装は`GkeSessionSandboxRuntime`（`SandboxManager.ensure` → `session.commands` / `session.files`）、`LocalProcessRuntime`（会話ごとの一時ディレクトリでsubprocessを実行する。開発用で、隔離はしない）、`InMemoryRuntime`（テスト用）の3つ。
- 組み込みツールは`disallowed_tools`で無効にする（Pythonで`tools=[]`が効くかは未検証のため使わない）。
- `gke-session-sandbox`はpath依存（`uv`の`[tool.uv.sources]`）にし、`GkeSessionSandboxRuntime`は遅延importにする。ライブラリがなくても他の部分は動く。
- `pending_context_events`は、turn開始時にユーザメッセージの前へ`<system-notice>`として付け、`query()`が成功した後にackする。

### D7. SessionStoreの対応付け

`AgentPlatformSessionStore`:

- Claudeの`session_id`（UUID）を`c-` + UUIDの英小文字・数字・ハイフン（63文字以内）にして、Agent Platformの`session_id`に使う。sessionは最初の`append`で作る（作成時のconflictは既存のsessionを使う）。
- entry 1件を`raw_event={"uuid", "subpath", "seq", "entry"}`として1 eventで保存する。`author="claude-agent-sdk"`、`invocation_id`はturn ID。
- `load`は`events.list`を全件たどり、`subpath`で絞り込み、`uuid`で重複を除いて、`seq`（なければtimestamp）の順に並べる。
- TTLは30日（設定可）。
- 適合性テスト`run_session_store_conformance`をintegrationテストで実行する。

**検証結果（2026-10-10、`vertexai` 2.4.0、`us-central1`）**: `run_session_store_conformance`はAgent Platform Sessionsに対して全項目が通った。1 eventに保存できるサイズは、`raw_event`に4 MiBの文字列を入れた場合まで成功した（上限は未確認）。ただし書込みと読込の往復にかかる時間はサイズにほぼ比例し、64 KiBで約3秒、1 MiBで約5秒、4 MiBで約19秒だった。実用上の上限は保存サイズではなく、`append`の遅さ（SDKは約100msごとにバッチを送る）になる。このため、ツール結果が数MiBになる場合のGCS退避は、今回は実装せず、必要になった時点で追加する。clientは、`agentplatform.Client`には`agent_engines`がまだなかったため、`vertexai.Client`（非推奨の警告が出る）を使う。

### D8. 管理情報（SessionStateStore）

管理用のAgent Platform Session（`session_id="m-" + sha256(conversation_key)[:40]`、`user_id="silk-agent-control"`）を会話ごとに作り、管理情報を保存する。

- stateを直接更新するAPIがない場合は、`raw_event={"kind":"control","state":{...}}`のeventを追記し、最新のeventを現在の値とする（追記のみのログ）。
- 起動時に実行中の会話を探すため、`user_id="silk-agent-control"`のsessionを列挙して、それぞれの最新の管理eventを読む。会話数が増えて遅くなった場合は、Firestore実装に差し替える。
- `SessionStateStore` portは`get(key)` / `put(key, state)` / `list_running()`の3つだけにする。

### D9. 起動ID・heartbeat・中断の検出

- 起動ごとに`bot_instance_id`（UUID）を発行し、turn開始時の管理情報に記録する。
- heartbeatは`AgentSession`内のtaskで、turnの実行中だけ`Clock`の間隔（60秒）ごとに`Heartbeat`を出す。テストではfakeの`Clock`で時刻を進める。
- 起動時の処理: `list_running()`のうち`bot_instance_id`が今回と違うものを`interrupted(crash)`に変え、チャネルに`TurnInterrupted`を通知する。Slack adapterは保存済みの進捗メッセージの`ts`を使って表示を書き換える。
- SIGTERM: Socket Modeを閉じる → 実行中のturnを最大25秒待つ → 終わらなければ中断して`interrupted(shutdown)`を保存 → SessionStoreをflushして終了する。

### D10. Slackの進捗メッセージ

- turnの開始時に`chat.postMessage`で投稿し、`ts`を管理情報に保存する。
- 内容は`ProgressView`（状態、開始時刻、ツール一覧、最終更新時刻）から描画する。更新は`chat.update`で、変化があってもまとめて1秒に1回までにする。heartbeatでは「最終更新」だけが変わる。
- 停止語の判定: `unicodedata.normalize("NFKC", text).strip().upper()`を、同じ方法で正規化した停止語の集合と比較する。
- `chat.update`が失敗した場合（メッセージ削除など）は、新しい進捗メッセージを投稿し直す。

### D11. ガード

- `GeminiPolicyGuard`: `google-genai`の`Client(vertexai=True, project="nnyn-dev", location="global")`で`gemini-3.8-flash`を呼ぶ。`response_schema=InputVerdict`（Pydantic）で判定を受け取る。直前の5メッセージを文脈として渡す。
- `ModelArmorSanitizer`: `modelarmor_v1.ModelArmorClient`をregional endpointで使い、`sanitize_model_response`を呼ぶ。判定は次の通り。
  - `filter_match_state=NO_MATCH_FOUND` → そのまま表示
  - `sdp`だけが該当し、`deidentify_result.data.text`がある → マスク後のテキストを表示
  - それ以外 → 差し止め
- 長文は約8,000トークン（文字数で概算）ごとに、段落・コードブロックの境界で分割して検査する。
- 同期のSDKは`asyncio.to_thread`で呼ぶ（async版があれば使う）。

### D12. Skillsの同期

`GcsSkillSource`は、`gs://<bucket>/skills/`の一覧のgeneration番号のハッシュを世代IDとし、変わった場合だけ`skills-cache/<世代ID>/.claude/skills/`へダウンロードして、`current`の参照を切り替える。turn開始時に`current`を固定し、会話の`cwd/.claude/skills`をその世代へのsymlinkにする。Sandboxへは、`ensure`後とSkillsの世代が変わったturnの開始時に`/workspace/.skills/`へコピーする。

### D13. パッケージとツール

Python 3.12以上、`uv`、`pytest` + `pytest-asyncio`、`ruff`。integrationテストは`-m integration`で明示したときだけ実行する。

## Risks / Trade-offs

- [Agent Platform Sessionsのeventサイズ上限が小さい] → 大きなentry（長いtool結果）は、GCSに置いて参照だけ保存する分岐をSessionStoreに入れられるようにしておく。integrationテストで上限を測る。
- [管理情報を追記のみのeventで持つと、起動時の列挙が遅い] → `SessionStateStore` portで隠し、Firestoreへ差し替えられるようにする。
- [transcriptが`tool_use`の途中で切れたままresumeするとSDKがエラーになる可能性] → **検証済み（2026-10-10、`claude-haiku-5-5`）**: `bash`でtool_useが始まった直後にCLIを強制終了し、ローカルのtranscriptがない別のプロセス相当（`CLAUDE_CONFIG_DIR`を分けた新しいengine）からAgent Platform Sessionsのstoreだけでresumeしたところ、エラーにならず、最初に覚えさせた内容を答えられた。このため、`load`時の補完は実装しない。resume時は、中断されたことをシステム通知（`<system-notice>`）で伝える。
- [Model Armorを通すため、応答がテキストブロック単位でしか表示されず、体感の速さが落ちる] → 進捗メッセージで状態とツールを即時に見せて補う。
- [`LocalProcessRuntime`はホストでコマンドを実行する] → 開発専用とし、Slack Botの設定では選べないようにする。
- [Model Armorの1分あたりの呼出し上限（1,200 QPM）] → ツール要約は短く、更新をまとめて呼出し回数を抑える。
- [1インスタンスなので、Botの停止中は誰も使えない] → 進捗メッセージの「最終更新」でユーザに見えるようにし、再起動後に中断を通知する。

## Migration Plan

新規作成のため移行はない。デプロイは次の順に行う。

1. Agent Runtimeインスタンスを作る。
2. Secret ManagerにSlackのトークンを登録する。
3. Workload Identityで、Agent Platform（Claude、Gemini、Sessions）、Model Armor、GCSへの権限を付ける。
4. 1レプリカのDeploymentで動かす。

ロールバックはDeploymentを前のimageに戻す。保存済みの会話は形式を変えない限り引き継げる。

## Open Questions

- Agent Platform上のClaudeのモデルIDの表記（`@`付きのバージョン指定が要るか）と、提供リージョン（`CLOUD_ML_REGION`の値）。設定値で吸収できる。
- 本番で使うClaudeモデル。設定値で吸収できる。
- Agent Platform SessionsのPython clientを`agentplatform`と`vertexai`のどちらにするか。adapter内部だけの違い。

## 実装中に分かったこと

- **`gke-session-sandbox`の依存競合（解消済み）**: 以前は`gke-session-sandbox`が`protobuf>=7.35.1`を要求し、`google-cloud-aiplatform`の`protobuf<7`と競合していた。ライブラリ側が`protobuf>=6.31.1,<7`に変更されたため、`pyproject.toml`の依存（path指定）に追加でき、`uv lock`も通る。`GkeSessionSandboxRuntime`のimportは遅延のまま残している。
- **Model Armorのテンプレート**: 既存の`simple`はSDPが`basicConfig`で、メールアドレスを検出できず、マスク後テキストも返らない。マスク表示には、`advancedConfig`（DLPのinspect / deidentifyテンプレート）が必要（`terraform/main.tf`に定義。既存の`simple`は`terraform import`で取り込む）。
- **`ToolSearch`の表示**: Claude CodeのCLIは、SDK MCPツールを遅延ロードするため、最初のツール呼出しの前に組み込みの`ToolSearch`を呼ぶ。このツールはホストに影響しないため無効にしていないが、進捗メッセージのツール一覧に`ToolSearch`として現れる。
- **コマンドIDの永続化**: 「実行中のSandboxコマンドのID」を再開時に伝えるため、`SandboxRuntime.run_command`に`on_started`を追加し、`CommandTracker`が管理情報へ記録する（結果不明のコマンドは完了まで残す）。
- **Streamlit**: 画面は再描画のたびにイベントログから組み立てる。入力ガードの判定中もポーリングを続ける（`is_busy`）。
