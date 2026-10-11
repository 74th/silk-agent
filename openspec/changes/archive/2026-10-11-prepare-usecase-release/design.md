# Design

## Context

動機はproposal.mdのWhyを参照。現状の作りのうち、この設計に効くもの:

- 起動は`apps/slack_bot.py`がリポジトリのルート（`ROOT`）を基準に設定・ポリシーを探す。`wiring.build_engine_factory`は`root / "policies" / "agent-system.md"`を決め打ちで読む。Botイメージ（`Dockerfile`）は`policies/`と`config/mcp.json`を同梱する。
- `config.py`の既定値に`nnyn-dev`やModel Armorのテンプレート名が入っている。`app.build_state_store`は`agent_runtime`に`<id>`が含まれると黙ってメモリの保存先に切り替える。
- 本番コード（`app.py`、`wiring.py`、`slack_app.py`）が`silk_agent.testing.fakes`の`SystemClock`、`InMemoryStateStore`、`AllowAllGuard`、`NoopSanitizer`を使っている。
- `SlackBot`は`self.channel._state_store`と`self.channel.output._state_store`を外から書き換えている。
- `CommandTracker`が`wiring.build_engine_factory`（`tracker`）と`app.build_services`（`BackgroundCommands`用）で別々に作られる。
- `filter.allowed_users`と`filter.allowed_channels`はすでにあり、`AllowListFilter`で判定している。ただし`allowed_channels`を設定するとDM（チャンネルID`D…`）も除外される。ゲスト・外部ユーザの判定はない。`FilterChain.check`は同期で、Slack APIを呼べない。
- Sandboxに渡すメールアドレスは環境変数`USER_EMAIL`で、`SlackBot._last_user`（その会話で最後に書き込んだユーザ）から決めている。`USER_EMAIL`はコンテナの環境変数になり、sandboxd経由のコマンドからも参照できることをGKEで確かめてある（`docs/gke-session-sandbox/validation/session-manager-gke.md`。trackerは`ProcessConfig.env_vars`にマーカー用の変数だけを渡し、sandboxdはそれをコンテナの環境変数に重ねて子プロセスを起動する）。`gke_session_sandbox`は作成時のenvのフィンガープリントを保存し、ハンドルを持っていない状態（Bot再起動後など）で別のenvを渡すと`SessionConfigurationConflictError`になる。つまり、Bot再起動後に別のユーザが返信すると再開に失敗する。
- エンジンはClaude Agent SDKの`PreToolUse` / `PostToolUse` / `PostToolUseFailure`フックで`ToolStarted` / `ToolFinished`を作っている。ここではツールの生の入力と結果が手に入るが、外に出しているのは要約だけ。
- Sandbox Podは`gke_session_sandbox.supervisor`がsandboxdを起動し、`/v1/health`を待ってからtrackerを起動する。readinessProbeはtrackerの`/healthz`。管理側の`backend.wait_ready`は180秒の決め打ち。
- Sandbox Podイメージは、`scripts/build-session-runtime-image.sh`がagent-sandboxのソースに`gke_session_sandbox`の6ファイルと`_sandboxd_proto`を手でコピーして作る。イメージ内では`protobuf>=7.35.1,<8`・`grpcio>=1.84`を入れているが、本体（生成コードを作った側）は`protobuf>=6.31.1,<7`・`grpcio-tools==1.78.0`。

## Goals / Non-Goals

**Goals:**

- ユースケースの担当者が、本体を変更せずに、テンプレートをコピーしてSandboxイメージ・プロンプト・Skills・フック・設定を差し替え、デプロイできる。
- フックに、監査に足りる生の情報（入力本文、ツールの入力と結果、返答の検査前の本文）を渡す。
- Slackの応答範囲（ユーザ、チャンネル、DM、ゲスト、外部ユーザ）を設定だけで絞れる。

**Non-Goals:**

- PyPIやコンテナレジストリへの公開の自動化（CI）。バージョンのタグ付けとイメージのビルド手順までにする。
- 複数インスタンスのBot、マルチテナント（1つのBotで複数ユースケース）。1ユースケース = 1デプロイ。
- フックによる処理の変更（ブロック、書き換え）。観測専用にする。
- Sandboxからの認証付きダウンロードの仕組み（Workload Identityなど）。`startup.sh`の中でユースケースが用意する。
- Streamlitチャネルのユースケース対応。設定の読み方の変更にだけ追従する。

## Decisions

### 1. 本体 = ライブラリ + ベースイメージ、ユースケース = 別パッケージ + 派生イメージ

```
silk-agent（このリポジトリ）               ユースケース（usecases/template をコピー）
├─ Pythonパッケージ silk_agent             ├─ pyproject.toml（silk-agentに依存）
│   └─ resources/（汎用プロンプト・基準）   ├─ src/<pkg>/hooks.py
├─ Pythonパッケージ gke_session_sandbox    ├─ prompts/agent-system.md
├─ Botベースイメージ silk-agent:<ver>   ◀── ├─ policies/input-guard.md
└─ Sandboxベースイメージ                ◀── ├─ skills/<name>/SKILL.md
   session-runtime:<ver>                   ├─ bot.Dockerfile（FROM silk-agent）
                                           ├─ sandbox/Dockerfile（FROM session-runtime）
                                           ├─ sandbox/startup.sh
                                           ├─ config/（silk-agent.yaml, sandbox-config.yaml, mcp.json）
                                           └─ k8s/（kustomization: base + 設定）
```

- ユースケースのBotイメージは`FROM silk-agent:<ver>`に自分のパッケージを`uv pip install`し、`prompts/`・`policies/`・`skills/`を`/app/usecase/`へコピーする。設定はConfigMapで`/config/`に置き、イメージ内のファイルは絶対パス（`/app/usecase/...`）で指す。
- k8sのマニフェストは、ユースケースに依存しないもの（Deployment、ServiceAccount、RBAC、PVC、StorageClass）を`k8s/base/`に、このリポジトリの検証用デプロイの設定を`k8s/overlays/dev/`に分ける。テンプレートの`k8s/`は`k8s/base/`を参照するkustomizationで、イメージ名とConfigMapだけを差し替える。
- `kubectl kustomize`は、kustomizationのディレクトリの外にある設定ファイルを`configMapGenerator`で読めない（シンボリックリンクも不可）。そのためテンプレートでは、`config/`に`kustomization.yaml`（`k8s/base`とConfigMap）を置き、`k8s/kustomization.yaml`がそれを参照してイメージ名だけを差し替える。
- テンプレートは`usecases/template/`に置き、リポジトリ内では`[tool.uv.sources]`で本体をpath依存にする（CIでテンプレートがビルドできることを確かめられる）。外に出すときはgitのタグ依存に書き換える手順を文書に書く。
- 代替案: (a) 本体のリポジトリをフォークして書き換える → 本体の更新を取り込みにくいので不採用。(b) 設定ファイルとボリュームだけで差し替え、Pythonパッケージを作らない → フックのコードを入れる場所がないので不採用。(c) フックをHTTPのWebhookにする → イベントログ基盤ごとにSDKで送りたい場合が多く、コードで書けるほうが柔軟。Webhook送信はフックの実装例として扱う。

### 2. 設定の読み込み: 相対パスは設定ファイル基準、環境固有値は必須

- `load_config(path)`が`Config`と`base_dir = path.parent`を持つ`LoadedConfig`（または`Config`に除外フィールド`base_dir`）を返し、`resolve(p)`で相対パスを解決する。`root`引数を引き回している箇所（`wiring`、`app`、`slack_app`、Streamlit）は`base_dir`を使う。
- 汎用のシステムプロンプトと入力ガードの基準は`src/silk_agent/resources/`へ移し、`importlib.resources`で読む。`policies/`はテンプレートへ移す。
- `agent.project`、`input_guard.project`、`output_sanitizer.template`、`session_store.agent_runtime`などは既定値をなくす。pydanticの`model_validator`で「その機能が有効なら必須」を判定し、項目名を出して起動を止める（`input_guard.provider: allow_all`、`output_sanitizer.provider: noop`、`session_store.provider: memory`なら不要）。`<id>`を含む値で黙ってメモリに切り替える処理は削除する。
- 代替案: リポジトリのルートやカレントディレクトリ基準 → イメージ内の配置に依存し、ローカルとGKEで解決先が変わるので不採用。

### 3. 起動コマンド

`src/silk_agent/cli.py`に`main()`を置き、`[project.scripts] silk-agent = "silk_agent.cli:main"`と`python -m silk_agent`で呼べるようにする。サブコマンドは`slack`（Slack Bot）だけ。`apps/slack_bot.py`はこれを呼ぶだけの互換スクリプトにする。Botベースイメージの`ENTRYPOINT`は`["silk-agent", "slack"]`。

### 4. フック

- `src/silk_agent/hooks.py`に、イベントの型（`HookEvent`の共用体: `UserInput`、`TurnStart`、`TurnEnd`（完了・失敗・中断を`outcome`で区別）、`ToolStart`、`ToolEnd`、`AgentReply`、`SystemNotice`）と、`Hooks`プロトコル（`async on_event(event)`、`async aclose()`）を置く。種類ごとのメソッド（`on_user_input`、`on_tool_start`…）に振り分ける基底クラス`BaseHooks`も用意し、ユースケースは必要なメソッドだけを上書きする。
- 共通フィールド: `kind`、`conversation_key`、`turn_id | None`、`at`、`user`、`user_email`、`channel`（`"slack"`など）。`dataclasses.asdict`でJSONにできる形にする。
- 設定: `hooks: ["pkg.mod:factory"]`、または`{factory: "pkg.mod:factory", options: {...}}`。ファクトリは`HookContext`（`options`、`config`、`logger`）を受け取り`Hooks`を返す。標準のJSON Linesフックは`silk_agent.hooks:json_lines`。
- 配信: `HookDispatcher`が1本の`asyncio.Queue`（上限、既定10,000件）と1つのワーカータスクを持ち、イベントを順に各フックへ渡す。各フックの呼出しは`asyncio.wait_for`（既定10秒）で区切り、例外・タイムアウトはログに出して次へ進む。キューが満杯なら最も古いものを捨てて件数を数え、ログに出す。発生元は`dispatcher.publish(event)`（同期、待たない）だけを呼ぶので、遅いフックがturnを止めない。
  - 「1件10秒のフックでもturnが遅れない」は、ワーカーが別タスクであることで満たす。ワーカーは1本なので遅いフックはキューを溜めるが、上限で古いものから捨てる。
- 発生元:
  - `ConversationOrchestrator._handle_message`: フィルタ（除外したフィルタ名）、停止語、ガード（判定と理由）、受理の各分岐で`UserInput`を出す。フィルタは`FilterDecision`に`reason`（フィルタ名）を加える。
  - `ConversationOrchestrator.emit`: `TurnStarted` / `TurnFinished` / `TurnFailed` / `TurnInterrupted` → `TurnStart` / `TurnEnd`、`AssistantText` → `AgentReply`（検査前の本文と検査の結果）。
  - `ClaudeAgentEngine`の`PreToolUse` / `PostToolUse` / `PostToolUseFailure`: `ToolStart`（`tool_input`）、`ToolEnd`（`tool_response`、状態、所要時間）。エンジンへは`ClaudeEngineSettings.on_hook_event`として渡す。
  - `AgentSession.notify`: `SystemNotice`。
- ユーザ情報: ディスパッチャが会話キー → 直近の入力の送信者（ユーザID、メール）を覚え、送信者を持たないイベント（ツール、turn）に付ける。
- 終了: `Services.aclose`で、orchestratorのshutdown（中断の記録）の後に`dispatcher.aclose(timeout)`（キューを上限時間内で流し、各フックの`aclose`を呼ぶ）。
- 本文はマスクしない（監査のため）。出力の検査結果は`AgentReply.sanitize_status`として付ける。ログ基盤へ出すかどうか・マスクするかはユースケースのフックが決める。
- 代替案: 既存の`OutboundEvent`をそのまま渡す → ツールの生の入力・結果と入力の判定結果が含まれないので、フック専用の型を作る。OpenTelemetryのspanにする → 依存が増え、イベントログ基盤への送信という目的に対して重い。

### 5. Slackの応答範囲

- `UserMessage`に`sender: SenderInfo | None`（`email`、`is_guest: bool | None`、`is_external: bool | None`、`team`）と`is_direct: bool`を加える。`None`は「判定できない」。
- `SlackEventRouter._emit`の前に、`UserDirectory`（既存。`users.info`の結果を10分間キャッシュするよう変更）で送信者を解決する。ゲストは`is_restricted`または`is_ultra_restricted`。外部ユーザは、イベントの`user_team`（なければ`users.info`の`team_id`）がBotをインストールしたworkspace（`auth.test`の`team_id`）と異なる場合。Enterprise Gridでは`enterprise_id`が同じなら外部としない。
  - `users.info`は、メールアドレスのために従来も呼んでいたので、呼出しの回数は増えない（キャッシュの単位をユーザごとの全情報にする）。
- `AllowListFilter`: チャンネルの許可リストを`is_direct`のときは適用しない。新しい`DirectMessageFilter`（`allow_direct_messages`）、`SenderKindFilter`（`deny_guests`、`deny_external_users`。`None`は除外）を`default_chain`に加える。いずれも返信しない（`reply=None`）。
- 停止語はフィルタの後に判定しているので、許可外のユーザの停止語は既に無視される（仕様で明記するだけ）。
- 代替案: `FilterChain`を非同期にしてフィルタの中で`users.info`を呼ぶ → コアがSlackに依存するので不採用。チャネルが送信者の情報を埋め、コアはそれを判定するだけにする。

### 6. `AGENT_USER_EMAIL`を会話を始めたユーザに固定する

- `ConversationState`に`owner_user`と`owner_email`を加える。最初のメッセージを受理したときに保存し、以後は変えない。
- `SlackBot._env(key)`は`ConversationState.owner_email`を返す（`_last_user`は削除）。管理情報がない（最初のturnの前に`pop_notices`がSandboxを作る）場合に備え、orchestratorは受理時に状態を保存してから`session.submit`する。
- 代替案: envを渡さずに作成後だけ渡す → ライブラリの仕組み（作成時のenvのフィンガープリント）を変えることになるので不採用。
- 変数名は`USER_EMAIL`から`AGENT_USER_EMAIL`に変える。`USER_EMAIL`は一般的な名前で、イメージに入れたツールや`startup.sh`の中の変数と衝突しやすいため。名前は`silk_agent`の定数1か所に置く。`gke_session_sandbox`は任意のenvを受け取るだけなので、ライブラリのコードは変えない（文書の例だけ`AGENT_USER_EMAIL`にする。検証記録は当時のまま残す）。

### 6a. エージェントのコマンドへの受け渡し

- `AGENT_USER_EMAIL`はコンテナの環境変数なので、supervisor → `startup.sh`、supervisor → sandboxd → コマンドの両方に引き継がれる。現在の仕組みのままで満たせるので、新しい受け渡しは作らない。
- ただし、引き継ぎに依存していることを壊さないよう次を加える。
  - trackerが`ProcessConfig.env_vars`に渡す変数を、マーカーと呼出し元が指定したものだけに保つ（コンテナの環境変数を上書きしない）ことをユニットテストで確かめる。
  - `bash -lc`（ログインシェル）で実行するので、ベースイメージの`/etc/profile`などで環境変数を消さないことをDockerfileのコメントと文書に書く。
  - GKEの検証（`tests/session_sandbox`のgkeマーカーのテスト）で、フォアグラウンド・バックグラウンドのコマンドと再開後のコマンドから`AGENT_USER_EMAIL`を読めることを確かめる。
- 代替案: trackerがコマンドごとに`AGENT_USER_EMAIL`を明示的に`env_vars`へ入れる → trackerがsilk-agent固有の変数名を知ることになるので不採用。

### 7. Sandboxの起動スクリプト

- `supervisor.run`の最初、sandboxdの前に`_run_startup()`を入れる。`SANDBOX_STARTUP_SCRIPT`（既定`/opt/session-runtime/startup.sh`）がなければ何もしない。実行は`asyncio.create_subprocess_exec(script)`（実行権限がなければ`/bin/sh script`）、`cwd=/workspace`、`env=os.environ + SANDBOX_STARTUP_FIRST_RUN`、標準出力・標準エラーはそのままコンテナへ継承する。
- 初回の判定は`/workspace/.session-runtime/startup-done`の有無。成功したときだけ作る。ワークスペース（PVC）が削除されて作り直された場合は再び初回になる。
- 上限時間は`SANDBOX_STARTUP_TIMEOUT_SECONDS`（既定300）。超えたらプロセスグループごとSIGTERM → 5秒後SIGKILL。失敗時は`SANDBOX_STARTUP_ON_ERROR`（`fail`既定 / `continue`）。`fail`ならsupervisorは終了コード1で終わり、Podは再起動を繰り返す（管理側は準備完了の待ち時間を超えてエラーになり、エージェントにはSandboxの準備に失敗したことが伝わる）。
- 起動スクリプトの実行中もSIGTERMを受け付け、スクリプトを止めて終了する。
- これらの環境変数は`SESSION_SANDBOX_`（予約）を使わず、テンプレートの`env`またはイメージの`ENV`で設定する。
- 準備完了の待ち時間: `gke_session_sandbox`の`RuntimeConfig`に`ready_timeout_seconds`（既定180）を加え、`backend.wait_ready`に渡す。
- あわせて`supervisor.run`の`locals().get(...)`による後始末を、タスクを先に`None`で初期化する形に直す。
- 代替案: sandboxdの起動後にtrackerと並行して実行 → エージェントのコマンドがセットアップ前に走りうるので不採用。Kubernetesの`initContainer`で実行 → テンプレートの変更が必要で、`AGENT_USER_EMAIL`をinitContainerへも渡す必要があり、ユースケースから見て手順が増えるので不採用。

### 8. Sandboxベースイメージ

- ランタイムに入れるモジュールの一覧を`src/gke_session_sandbox/_runtime_modules.py`の定数`RUNTIME_MODULES`に一本化し、ビルドスクリプトはそれを読んでコピーする。ユニットテストで、一覧のモジュールが標準ライブラリ・`aiohttp`・`grpc`・`google.protobuf`・一覧内のモジュール以外をimportしないことを確かめる。
- イメージ内の依存は`uv export`で`uv.lock`から`aiohttp`・`grpcio`・`protobuf`の版を固定した`requirements`を作って入れる（本体と同じ版になる）。
- タグを`<agent-sandboxの版>-session-runtime-<silk-agentの版>`にし、ユースケースはdigestで`FROM`する。

### 9. リファクタリング（この変更で行うもの）

- `SystemClock`は`silk_agent/core/clock.py`、`InMemoryStateStore`は`stores/memory.py`、`AllowAllGuard`と`NoopSanitizer`は`guards/noop.py`へ移す。`testing/fakes.py`はそれらを再エクスポートしてテストを壊さない。
- `SlackChannel`に`attach_state_store(store)`を加え、`SlackOutput`にも伝える。`SlackBot`は非公開属性を触らない。
- `CommandTracker`は`build_services`で1つ作り、`build_engine_factory`と`BackgroundCommands`の両方に渡す（`build_engine_factory`の`state_store`引数は`tracker`引数に置き換える）。

### 10. 今回は行わないリファクタリングの候補

| 候補 | 理由・内容 |
| --- | --- |
| `slack.stop_words`・`slack.interrupt_mode`を`conversation:`へ移す | チャネル非依存の設定がSlackの下にある。設定の互換性を壊すので、次に設定を変えるときにまとめる |
| `sandbox/gke.py`の`type(e).__name__ == "..."`による例外判定 | `gke_session_sandbox`の例外クラスを遅延importして`isinstance`で判定する |
| `app.build_services` / `wiring.build_engine_factory`の分担 | 引数が多く、どちらに何があるかわかりにくい。組み立てを1つのビルダーにまとめる |
| `gke_session_sandbox`の依存（`kubernetes-asyncio`など）を任意のextraにする | ローカル開発やStreamlitだけなら不要 |
| `UserDirectory`のキャッシュを会話の管理情報と共有 | 再起動のたびに`users.info`を呼び直している |
| Bot・Sandboxイメージのビルドと公開のCI | 現在は手元のスクリプト |
| 再起動時のバックグラウンドコマンドの終了通知 | READMEの既知の制限。再起動後に`running_command_ids`から監視を再開する |

## Risks / Trade-offs

- [設定の相対パスの基準が変わり、既存のデプロイが起動しなくなる] → `k8s/config/silk-agent.yaml`と`config/silk-agent.example.yaml`を同じ変更で書き換え、移行手順を文書に書く。プロンプトや基準のファイルが見つからなければ、パスを出して起動を止める（気づかずに汎用のものへ戻らない）。
- [フックに生の入力・ツールの結果が渡り、認証情報や個人情報がログ基盤へ流れる] → 文書に明記し、テンプレートのフックにマスクの例を入れる。標準のJSON Linesフックは本文を含めるかを`options.include_content`（既定`false`）で選ぶ。
- [フックのキューの上限で、負荷が高いと監査イベントが欠ける] → 捨てた件数をログに出す。上限と1件あたりの時間は`hooks`の設定で変えられるようにする。確実な配信が必要な場合は、フックの中で永続的なキュー（Pub/Subなど）へ渡す実装を勧める。
- [ゲスト・外部ユーザの判定に`users.info`を使うので、Slack APIのレート制限や障害で正しいユーザも除外されうる] → 判定できないときは除外（安全側）。キャッシュ（10分）でAPI呼出しを減らす。設定が無効なら判定しない。
- [Enterprise Gridでの「外部」の判定が組織の運用と合わない可能性] → `enterprise_id`が同じなら内部とする規則を文書に書く。必要なら`allowed_teams`と組み合わせる。
- [`startup.sh`の失敗でPodが再起動を繰り返し、会話が使えなくなる] → 既定は`fail`だが、ログに出し、管理側の待ち時間で打ち切られてエージェントに伝わる。開発中は`continue`を使える。
- [`startup.sh`が再開のたびに走り、再開が遅くなる] → `SANDBOX_STARTUP_FIRST_RUN`で初回だけの処理を分けられるようにし、文書で冪等に書くことを求める。
- [Sandboxは外部通信がDNS以外止まっているので、`startup.sh`のダウンロードが失敗する] → 文書で`network.allowed_fqdns`への追加を手順に含める。
- [変数名の変更と開始者への固定で、既存の会話のSandbox（`_last_user`由来の`USER_EMAIL`で作成）とフィンガープリントが合わない] → 既存の会話の管理情報に`owner_email`がない場合は、既存のSandboxを再利用する（envを渡さない＝`None`）ようにし、新しい会話から`AGENT_USER_EMAIL`で固定する。既存の会話のSandboxでは、削除（7日）されるまで`USER_EMAIL`のまま残る。

## Migration Plan

1. 本体の変更（設定、フック、フィルタ、`AGENT_USER_EMAIL`、リファクタリング）をマージし、ユニットテストを通す。
2. Sandboxベースイメージを新しいタグでビルドし、`k8s/config/sandbox-config.yaml`の`runtime.image`を更新する（`startup.sh`がないイメージは従来どおり動く）。
3. `k8s/config/silk-agent.yaml`を新しい形式（絶対パス、必須項目、`hooks`）に書き換え、Botベースイメージでデプロイし、既存のスレッドで会話が続くことを確かめる。
4. `usecases/template/`をビルドしてテスト用のクラスタへデプロイし、文書の手順どおりに動くことを確かめる（`startup.sh`で`AGENT_USER_EMAIL`が見えること、フックのJSONが出ること、`deny_guests`の動作）。
5. 戻すときは、Botのイメージと設定のConfigMapを前の版に戻す。Sandboxのイメージは前のdigestに戻す（PVCの作業ファイルはそのまま）。

## Open Questions

- Botベースイメージ・Sandboxベースイメージを置くレジストリ（現在の`research-gke-agent-sandbox-with-sandboxd-images`をそのまま使うか、公開用に分けるか）。手順の書き方が変わるだけで、設計は変わらない。
