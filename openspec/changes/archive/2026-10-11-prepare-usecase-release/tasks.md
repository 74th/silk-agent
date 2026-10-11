# Tasks

## 1. 本番コードからテスト用モジュールを切り離す

- [x] 1.1 `SystemClock`を`src/silk_agent/core/clock.py`、`InMemoryStateStore`を`src/silk_agent/stores/memory.py`、`AllowAllGuard`・`NoopSanitizer`を`src/silk_agent/guards/noop.py`へ移し、`testing/fakes.py`は再エクスポートにする。`grep -rn "testing.fakes" src apps`が`src/silk_agent/testing/`以外で0件になり、`uv run pytest`が通ること
- [x] 1.2 `SlackChannel.attach_state_store()`を追加して`SlackOutput`にも伝え、`slack_app.py`の`_state_store`の書き換えを削除する。`tests/test_slack_bot.py`が通ること
- [x] 1.3 `CommandTracker`を`build_services`で1つだけ作り、`build_engine_factory`の`state_store`引数を`tracker`引数に置き換える。`grep -n "CommandTracker(" src`が1件になり、`tests/test_background.py`・`tests/test_sandbox_tools.py`が通ること

## 2. 設定の読み込み

- [x] 2.1 `load_config`が設定ファイルのディレクトリを基準に相対パスを解決できるようにし（`base_dir`と`resolve()`）、`wiring.py`・`app.py`・`slack_app.py`・`apps/streamlit_app.py`の`root`を置き換える。`/tmp/x/silk-agent.yaml`の`policy_file: policies/a.md`が`/tmp/x/policies/a.md`になるテストを`tests/test_config.py`に追加して通ること
- [x] 2.2 環境固有の値（`agent.project`、`input_guard.project`、`output_sanitizer.template`、`session_store.project`・`agent_runtime`など）の既定値を削除し、有効な機能で欠けていれば項目名を出して検証エラーにする。`<id>`でメモリへ切り替える処理を削除する。欠けた場合・`provider: allow_all / noop / memory`で不要な場合のテストが通ること
- [x] 2.3 汎用のシステムプロンプトと入力ガードの基準を`src/silk_agent/resources/`へ移して`importlib.resources`で読み、`agent.system_prompt_file`を追加する。指定あり・指定なし・ファイルがない（起動エラー）のテストが通ること
- [x] 2.4 `config/silk-agent.example.yaml`と`k8s/config/silk-agent.yaml`を新しい形式に書き換える。`tests/test_example_files.py`で両方が`load_config`で読めること

## 3. 起動コマンドとベースイメージ（Bot）

- [x] 3.1 `src/silk_agent/cli.py`（`silk-agent slack`、`--config`と`SILK_AGENT_CONFIG`）と`src/silk_agent/__main__.py`を追加し、`[project.scripts]`に登録する。`apps/slack_bot.py`はこれを呼ぶだけにする。`uv run silk-agent --help`が動き、設定ファイルがない場合のエラーのテストが通ること
- [x] 3.2 `Dockerfile`から`policies/`と`config/mcp.json`のコピーをやめ、`ENTRYPOINT ["silk-agent", "slack"]`にする（派生イメージが`uv pip install`できるよう`uv`を残す）。`docker build .`が成功し、`docker run --rm <image> --help`が動くこと

## 4. Slackの応答範囲

- [x] 4.1 `UserMessage`に`sender: SenderInfo | None`と`is_direct`を追加し、`UserDirectory`を`users.info`の全情報（メール、`is_restricted`・`is_ultra_restricted`、`team_id`、`enterprise_id`）の10分キャッシュに変える。`SlackEventRouter`が送信者を解決して`UserMessage`に入れ、イベントの`user_team`・`auth.test`の`team_id`・`enterprise_id`から外部ユーザを判定する。ゲスト、Slackコネクトのユーザ、Enterprise Gridの別workspace、`users.info`の失敗（`None`）のテストを`tests/test_slack_events.py`に追加して通ること
- [x] 4.2 `FilterConfig`に`allow_direct_messages`（既定`true`）、`deny_guests`、`deny_external_users`（既定`false`）を追加し、`AllowListFilter`のチャンネル判定をDMに適用しないようにし、`DirectMessageFilter`と`SenderKindFilter`を`default_chain`に加える。`FilterDecision`に除外したフィルタ名（`reason`）を加える。input-guardのdelta specの各シナリオ（許可外ユーザの返信・停止語、チャンネル制限下のDM、DM拒否、ゲスト、外部、判定不能、設定なし）を`tests/test_orchestrator.py`などで確かめること

## 5. `AGENT_USER_EMAIL`を会話の開始者に固定する

- [x] 5.1 `ConversationState`に`owner_user`・`owner_email`を追加し（`stores/state.py`の保存形式も）、orchestratorが最初に受理したメッセージで保存してから`session.submit`する。保存・読み込みのテストが通ること
- [x] 5.2 `SlackBot._env`を`owner_email`から返すように変え、`_last_user`を削除する。`owner_email`のない既存の会話はenvを渡さない（`None`）。「別のユーザが返信した後の再開」と「既存の会話」のテストを`tests/test_slack_bot.py`に追加して通ること
- [x] 5.3 環境変数名を`USER_EMAIL`から`AGENT_USER_EMAIL`に変え（`silk_agent`の定数1か所）、`app.py`・`k8s/`のコメント、`tests/test_gke_runtime.py`、`docs/gke-session-sandbox/README.md`と`async-usage.md`の例を更新する（検証記録は変えない）。`grep -rn "[^_]USER_EMAIL" src k8s tests/test_gke_runtime.py docs/gke-session-sandbox/*.md`が0件になること
- [x] 5.4 trackerが`ProcessConfig.env_vars`にマーカーと呼出し元が指定した変数だけを渡すこと（コンテナの環境変数を上書きしないこと）を`tests/session_sandbox/test_tracker_runtime.py`で確かめ、gkeマーカーのテストに、フォアグラウンド・バックグラウンド・再開後のコマンドで`echo $AGENT_USER_EMAIL`が作成時の値になる確認を加えること

## 6. イベントフック

- [x] 6.1 `src/silk_agent/hooks.py`にイベントの型（`UserInput`、`TurnStart`、`TurnEnd`、`ToolStart`、`ToolEnd`、`AgentReply`、`SystemNotice`）、`Hooks`プロトコル、`BaseHooks`、`HookContext`、`HookDispatcher`（キュー上限、1件あたりの時間制限、例外のログ、古いものを捨てる、`aclose`）を実装する。順序、例外、遅いフック（`FakeClock`で10秒）、満杯時の破棄、`aclose`での配信のテストを`tests/test_hooks.py`に追加して通ること
- [x] 6.2 設定に`hooks`（文字列または`{factory, options}`）と配信の上限設定を追加し、`module:factory`を読み込む。読み込めない場合は起動エラーにする。`tests/test_hooks.py`で正常・モジュールなし・属性なしを確かめること
- [x] 6.3 orchestratorの入力の各分岐（除外、停止語、ブロック、要確認、ガード失敗、受理）と`emit`（turnの開始・終了・失敗・中断、返答の検査前の本文と検査の結果）、`AgentSession.notify`からイベントを出す。fakeのエンジンで「1回のturn」「ブロックされた入力」「除外された入力」のシナリオの順序をテストで確かめること
- [x] 6.4 `ClaudeAgentEngine`の`PreToolUse` / `PostToolUse` / `PostToolUseFailure`で`ToolStart`（`tool_input`）・`ToolEnd`（`tool_response`、状態、所要時間）を出す（`ClaudeEngineSettings.on_hook_event`）。`tests/test_claude_engine.py`でフックの入力を与えて確かめること
- [x] 6.5 標準のJSON Linesフック（`silk_agent.hooks:json_lines`、`options.include_content`既定`false`）を実装し、`Services.aclose`でorchestratorのshutdownの後に`dispatcher.aclose`を呼ぶ。標準出力のJSONと、正常終了時に中断のイベントが出てから`aclose`が呼ばれることをテストで確かめること

## 7. Sandboxの起動スクリプト（gke_session_sandbox）

- [x] 7.1 `supervisor.run`の最初に起動スクリプトの実行を入れる（`SANDBOX_STARTUP_SCRIPT`、`cwd=/workspace`（`SANDBOXD_ROOT_DIR`）、環境変数の引き継ぎ、`SANDBOX_STARTUP_FIRST_RUN`と`.session-runtime/startup-done`、`SANDBOX_STARTUP_TIMEOUT_SECONDS`、`SANDBOX_STARTUP_ON_ERROR`、実行中のSIGTERM）。あわせて`locals().get`の後始末を直す。スクリプトなし、成功、初回と2回目、失敗（`fail` / `continue`）、タイムアウト、`AGENT_USER_EMAIL`の引き継ぎを、一時ディレクトリと偽のsandboxd・trackerコマンドで`tests/session_sandbox/test_supervisor.py`に追加して通ること
- [x] 7.2 `RuntimeConfig`に`ready_timeout_seconds`（既定180）を追加し、`backend.wait_ready`へ渡す。`tests/session_sandbox/test_models_and_config.py`と`test_backend.py`で既定値と設定値を確かめること
- [x] 7.3 `docs/gke-session-sandbox/`に起動スクリプトと`ready_timeout_seconds`の説明を追加すること

## 8. Sandboxベースイメージ

- [x] 8.1 `src/gke_session_sandbox/_runtime_modules.py`に`RUNTIME_MODULES`を定義し、`scripts/build-session-runtime-image.sh`がそれを読んでコピーするようにする。一覧のモジュールが許可したパッケージ以外をimportしないことを確かめるテストを`tests/session_sandbox/test_package.py`に追加して通ること
- [x] 8.2 イメージ内の依存を`uv export`で`uv.lock`の版に固定し（`protobuf<7`の食い違いを解消）、タグを`<agent-sandboxの版>-session-runtime-<silk-agentの版>`にする。`/opt/session-runtime/`を作り、派生イメージが`USER root`でツールを追加できることをDockerfileのコメントに書く。ビルドが成功し、`tests/session_sandbox/test_proto_compatibility.py`が通ること

## 9. k8sマニフェストの分割

- [x] 9.1 `k8s/`を`k8s/base/`（Deployment、ServiceAccount、RBAC、PVC、StorageClass）と`k8s/overlays/dev/`（現在の設定、ConfigMap、イメージ名）に分け、`k8s/deploy.sh`を追従させる。`kubectl kustomize k8s/overlays/dev`の出力が分割前の`kubectl kustomize k8s`と（パス以外）同じであること

## 10. ユースケースのテンプレート

- [x] 10.1 `usecases/template/`に、`pyproject.toml`（本体へのpath依存）、`src/silk_usecase_template/hooks.py`（`BaseHooks`の例とマスクの例）、`prompts/agent-system.md`、`policies/input-guard.md`、`skills/example/SKILL.md`、`config/`（`silk-agent.yaml`、`sandbox-config.yaml`、`mcp.json`）を作る。`uv run --directory usecases/template python -c "import silk_usecase_template.hooks"`が動き、テンプレートの設定が`load_config`で読めるテストが通ること
- [x] 10.2 `usecases/template/sandbox/Dockerfile`（`FROM`ベースイメージ、ツールの追加、`startup.sh`のコピー）と`sandbox/startup.sh`（`AGENT_USER_EMAIL`と`SANDBOX_STARTUP_FIRST_RUN`を使う例）、`bot.Dockerfile`（`FROM`Botベースイメージ、パッケージのインストール、`/app/usecase/`へのコピー）、ビルドスクリプトを作る。両方のイメージのビルドが成功すること
- [x] 10.3 `usecases/template/k8s/`に`k8s/base/`を参照するkustomizationを作る。`kubectl kustomize usecases/template/k8s`が成功すること

## 11. 文書

- [x] 11.1 `docs/usecase-guide.md`に適用手順（テンプレートのコピーと名前の変更、Sandboxイメージと`startup.sh`（冪等に書く、`allowed_fqdns`、`ready_timeout_seconds`）、Skillsとプロンプト、フック（生の本文が渡ることとマスク）、Slackの応答範囲の設定、ビルドとデプロイ、本体の更新の取り込み、既存デプロイの設定の移行）を書くこと
- [x] 11.2 READMEを更新する（起動コマンド、設定の相対パス、応答範囲の設定、フック、`startup.sh`、`docs/usecase-guide.md`への参照、ディレクトリ構成）こと

## 12. 全体の確認

- [x] 12.1 `uv run pytest`、`uv run ruff check .`、`uv run ruff format --check .`、`uv run pyrefly check`がすべて通ること
- [ ] 12.2 テスト用クラスタに、新しいベースイメージと`k8s/overlays/dev`でデプロイし、既存のスレッドで会話が続くこと、`filter.deny_guests`を有効にしてゲストに反応しないことを確かめる
  - 2026-10-11: 新しいBot・Sandboxベースイメージをdev overlayでデプロイし、既存スレッドの継続を確認。`deny_guests: true`も適用済み。ゲストアカウントでの送信は未確認。
- [x] 12.3 テンプレートから作ったユースケースをデプロイし、`startup.sh`の中で`AGENT_USER_EMAIL`が見えること（ログ）、エージェントの`bash`で`echo $AGENT_USER_EMAIL`が会話を始めたユーザのメールアドレスになること、フックのJSONがCloud Loggingに出ることを確かめる
