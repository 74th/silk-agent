# ユースケースへの適用手順

silk-agent本体を変更・フォークせずに、ユースケース（用途）ごとの**システムプロンプト、入力ガードの基準、Skills、MCP、フック、Sandboxのイメージ、設定**を別のパッケージとして用意し、組み合わせてデプロイする手順。1ユースケース = 1デプロイ（1つのBotと、そのBotのSandbox）。本番の配置は分散構成（[4章](#4-分散構成での適用推奨)）を使う。

## 1. 構成

```text
silk-agent（このリポジトリ）               ユースケース（usecases/template をコピー）
├─ Pythonパッケージ silk_agent             ├─ pyproject.toml（silk-agentに依存）
│   └─ resources/（汎用プロンプト・基準）   ├─ src/<パッケージ名>/hooks.py
├─ Pythonパッケージ gke_session_sandbox    ├─ prompts/agent-system.md
├─ Botベースイメージ silk-agent:<版>   ◀── ├─ policies/input-guard.md
└─ Sandboxベースイメージ                ◀── ├─ skills/<名前>/SKILL.md
   session-runtime:<版>                    ├─ bot.Dockerfile       （FROM Botベースイメージ）
                                           ├─ sandbox/Dockerfile   （FROM Sandboxベースイメージ）
                                           ├─ sandbox/startup.sh
                                           ├─ config/（silk-agent.yaml, sandbox-config.yaml, mcp.json, kustomization.yaml）
                                           ├─ k8s/kustomization.yaml（本体の k8s/base + 設定 + イメージ名）
                                           └─ build.sh
```

- Botのイメージ: 本体のBotベースイメージ（`Dockerfile`。`ENTRYPOINT`は`silk-agent slack`）に、ユースケースのパッケージを`uv pip install`し、`prompts/`・`policies/`・`skills/`を`/app/usecase/`へ置く。設定はConfigMapで`/config/`に置き、イメージ内のファイルは絶対パス（`/app/usecase/...`）で指す。
- Sandboxのイメージ: 本体のSandboxベースイメージ（`runtime/session-runtime.Dockerfile`）に、ツールと`startup.sh`を足す。
- 本体のパッケージとBotベースイメージには、ユースケース固有のファイル（MCPの設定、Skills、デプロイ先の設定）を含めない。設定がないときに本体が使う既定は、汎用のシステムプロンプトと入力ガードの基準（`src/silk_agent/resources/`）だけ。GCPプロジェクト、Sessionsのインスタンス、Model Armorのテンプレートなど環境に固有の値には既定値がなく、足りなければBotは起動せず、項目名（例: `agent.project`）をエラーとして出す。

## 2. 手順

### 2.1 テンプレートをコピーして名前を変える

```bash
cp -r usecases/template usecases/my-usecase
cd usecases/my-usecase
mv src/silk_usecase_template src/my_usecase      # パッケージ名（コピー直後で未追跡なので git mv ではなく mv）
```

書き換える場所:

| 場所 | 内容 |
| --- | --- |
| `pyproject.toml` | `name`、`[tool.uv.sources]`（下の「本体を別リポジトリから使う」） |
| `config/silk-agent.yaml` | `hooks`の`factory`（`my_usecase.hooks:create`）、`your-*`の値（プロジェクト、Sessionsのインスタンス、Model Armorのテンプレート、`allowed_teams`） |
| `config/sandbox-config.yaml` | `runtime.image`（§2.2でビルドしたSandboxイメージのdigest） |
| `k8s/kustomization.yaml` | `images`のBotイメージ名とタグ |
| `build.sh`の実行時 | `USECASE_NAME=my-usecase`（イメージ名`<名前>-bot` / `<名前>-sandbox`） |

**本体を別リポジトリから使う**: ユースケースを自分のリポジトリに置く場合は、本体を版（gitのタグ）で指す。

```toml
[tool.uv.sources]
silk-agent = { git = "https://github.com/<org>/silk-agent", tag = "v0.1.0" }
```

`config/kustomization.yaml`の`resources`も、相対パスの代わりにタグで指す（`github.com/<org>/silk-agent//k8s/base?ref=v0.1.0`）。Botのイメージは`bot.Dockerfile`の`SILK_AGENT_IMAGE`で、その版のBotベースイメージを指定する。

### 2.2 Sandboxのイメージと`startup.sh`

`sandbox/Dockerfile`は、Sandboxのベースイメージ（`scripts/build-session-runtime-image.sh`で作る。タグは`<agent-sandboxの版>-session-runtime-<silk-agentの版>`、利用時はdigestで`FROM`する）にツールを足す。

```dockerfile
FROM <ベースイメージ>@sha256:...
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git jq \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=0755 startup.sh /opt/session-runtime/startup.sh
USER 1000:1000
```

`ENTRYPOINT`と`/opt/runtime`（supervisor、tracker）、`/usr/local/bin/sandboxd`は変えない。コマンドは`bash -lc`（ログインシェル）で実行されるので、`/etc/profile`や`/etc/profile.d`で環境変数を消さない（`AGENT_USER_EMAIL`が届かなくなる）。

`startup.sh`は、Sandboxのコンテナが起動するたび（作成時と、停止からの再開時）に、sandboxdを起動する**前**に実行される。ファイルのダウンロード、初期設定などを、エージェントの最初のコマンドより前に済ませる。

- 環境変数: `AGENT_USER_EMAIL`（その会話を始めたSlackユーザのメールアドレス）、`SANDBOX_STARTUP_FIRST_RUN`（最初の起動は`1`、再開以降は`0`）。作業ディレクトリは`/workspace`。標準出力・標準エラーはコンテナのログに出る。
- **冪等に書く**: 再開のたびに実行される。初回だけの処理は`SANDBOX_STARTUP_FIRST_RUN`で分ける。
- 失敗（0以外の終了、または`SANDBOX_STARTUP_TIMEOUT_SECONDS`（既定300秒）超過）すると、既定ではSandboxは起動せず、Podは再起動を繰り返す（エージェントにはSandboxの準備に失敗したことが伝わる）。開発中は`SANDBOX_STARTUP_ON_ERROR=continue`で失敗を無視できる。この2つの変数は、`sandbox-config.yaml`が使うテンプレート（`templates/sandbox.yaml`）の`env`、またはイメージの`ENV`で設定する。
- **ダウンロード先を許可する**: Sandboxは外部へ通信できない（DNS以外）ので、`startup.sh`が使うホストを`sandbox-config.yaml`の`network.allowed_fqdns`に足す。
- **準備完了の待ち時間**: `startup.sh`が長いときは、`sandbox-config.yaml`の`runtime.ready_timeout_seconds`（既定180）を増やす。これを超えるとSandboxの作成・再開が失敗する。

詳細は[docs/gke-session-sandbox/README.md](gke-session-sandbox/README.md)の「起動スクリプト」。

### 2.3 Skillsとプロンプト

- **システムプロンプト**: `prompts/agent-system.md`を、`agent.system_prompt_file`で指す。本体の汎用のプロンプト（`src/silk_agent/resources/agent-system.md`）をコピーして役割を書き足すのが安全。Sandboxのツールの使い方、`<system-notice>`の扱い、バックグラウンド実行の扱いの指示は残す。指定したファイルがなければBotは起動しない（気づかずに汎用のものへ戻らない）。指定しなければ汎用のプロンプトを使う。
- **入力ガードの基準**: `policies/input-guard.md`を`input_guard.policy_file`で指す。同様に汎用の基準に足す。
- **Skills**: `skills/<名前>/SKILL.md`（と付属のファイル）。`skills.source`にBotのイメージ内のディレクトリ（`/app/usecase/skills`）かGCS（`gs://...`）を指定する。GCSなら、同期間隔（既定5分）以内に、以後に始まるturnへ反映される。イメージに含めたSkillsは、イメージを作り直すまで変わらない。Skillに付属のファイル（スクリプトなど）は、Sandboxの`/workspace/.skills/<Skill名>/`にコピーされる。エージェントに使わせるときは、`SKILL.md`にこの絶対パスで書く（コマンドの作業ディレクトリはSkillの場所ではない）。Sandboxのイメージに同じファイルを入れて、別のパス（`/app/usecase/skills/...`など）で使わせてもよい（bigquery-analysisはこの方式）。
  - **分散構成の上限**: Skills全体で**1000ファイル、合計8MiB**まで（超えるとSandboxへの同期が拒否され、turnが始まらない）。
  - **Skillsを減らす・空にする**: 同期のたびに、Sandboxの`.skills/`から、今回の世代にないファイルを削除する。ソースを空にすると、以後のturnはSkillsなしで動く（古い世代は使わない）。削除できないファイルがあれば同期は失敗として扱う。
- **MCP**: `config/mcp.json`（HTTP / SSEのサーバのみ、`${ENV}`を展開）。

### 2.4 フック

フックは、会話の出来事を**観測する**関数で、監査ログやイベントログ基盤へ送るために使う。`src/<パッケージ>/hooks.py`に`BaseHooks`を継承したクラスと、`HookContext`（`options`、`config`、`logger`）を受け取って`Hooks`を返すファクトリを書き、`config/silk-agent.yaml`で指す。

```yaml
hooks:
  - factory: my_usecase.hooks:create
    options: {include_content: false}
  - silk_agent.hooks:json_lines       # 標準: 1イベント1行のJSONを標準出力へ（Cloud Loggingが集める）
```

| イベント | 内容 |
| --- | --- |
| `user_input` | ユーザの入力（本文、送信者）と判定（`accepted` / `filtered`（除外したフィルタ名）/ `stop_word` / `blocked` / `review` / `guard_failed`）。ブロック・除外された入力もここで見える |
| `turn_start` / `turn_end` | turnの開始と、終了（`completed` / `failed` / `interrupted`と理由、使用量と費用） |
| `tool_start` / `tool_end` | ツール名と**生の入力**、状態と**生の結果**と所要時間 |
| `agent_reply` | エージェントの返答（**出力の検査の前の本文**と、検査の結果） |
| `system_notice` | バックグラウンドのコマンドの終了などの通知 |

全イベントに、会話キー、turn ID、時刻、ユーザID、メールアドレス、チャネル（`slack`）が付く。

- **生の内容が渡る**: 入力の本文、ツールの入力と結果、検査前の返答には、認証情報や個人情報が含まれうる。ログ基盤へ出す前にマスクするかどうかは、フックで決める（テンプレートの`mask()`が例）。標準の`json_lines`は、本文を出すかを`options.include_content`で選ぶ（既定`false`）。
- **観測専用**: フックは非同期に呼ばれ、例外や遅さがturnやSlackの表示に影響しない。失敗はBotのログに出る。処理の書き換えやブロックはできない。
- **欠けうる**: 配信待ちのキューには上限（`hook_delivery.queue_size`、既定10,000件）があり、超えると古いものから捨てて、件数をログに出す。1件あたりの時間は`hook_delivery.timeout_seconds`（既定10秒）で打ち切る。確実に届けたいときは、フックの中でPub/Subなどの永続的なキューへ渡す。
- 正常終了（SIGTERM）では、実行中のturnの中断を記録した後に、配信待ちを`hook_delivery.shutdown_timeout_seconds`（既定10秒）まで流し、フックの`aclose()`を呼ぶ。
- 読み込めないフック（モジュールや関数がない、ファクトリが失敗する）があると、Botは起動しない。

### 2.5 Slackの応答範囲

`config/silk-agent.yaml`の`filter`で、Botが反応する相手を絞る。除外したメッセージはエージェントに渡さず、返信もしない（フックには`filtered`として渡る）。

| 設定 | 内容 |
| --- | --- |
| `allowed_teams` / `allowed_channels` / `allowed_users` | 許可するworkspace・チャンネル・ユーザ。空なら制限しない。ユーザの許可リストは、メンションだけでなく、スレッドへの返信、DM、停止語にも効く |
| `allow_direct_messages`（既定`true`） | `false`でDMを受け付けない。チャンネルの許可リストはDMには適用しない |
| `deny_guests`（既定`false`） | `true`でゲスト（マルチチャンネル・シングルチャンネル）を除外する |
| `deny_external_users`（既定`false`） | `true`でBotをインストールしたworkspace以外のユーザ（Slackコネクトの共有チャンネルなど）を除外する |

- ゲスト・外部ユーザの判定には`users.info`（`users:read`、メールアドレスは`users:read.email`）を使い、結果は10分間キャッシュする。**判定できなかった場合は、その設定が有効なら除外する**。
- Enterprise Gridでは、`enterprise_id`が同じ組織のユーザは外部としない。組織の運用と合わない場合は`allowed_teams`と組み合わせる。

#### 検証時にガードを一時的に無効にする

Sandboxへ渡した環境変数の値をエージェントのコマンドで照合するような検証では、入力ガードが依頼をブロックしたり、出力サニタイザが返答をマスクしたりすることがある。検証用の設定だけで次のように切り替えられる。

```yaml
input_guard:
  provider: allow_all
output_sanitizer:
  provider: noop
```

検証後は元の`input_guard`と`output_sanitizer`の設定を戻してデプロイする。

### 2.6 ビルドとデプロイ

```bash
# 1. 本体のベースイメージ（本体のリポジトリで。タグ付けしたものを使う）
./k8s/build-image.sh                                  # Botベースイメージ → SILK_AGENT_IMAGE=<repo>@<digest>
GKE_PROJECT_ID=... GKE_LOCATION=... SANDBOXD_IMAGE_REPOSITORY=... \
  ./scripts/build-session-runtime-image.sh           # Sandboxベースイメージ → SESSION_RUNTIME_IMAGE=<repo>@<digest>

# 2. ユースケースのイメージ
cd usecases/my-usecase
USECASE_NAME=my-usecase IMAGE_REPOSITORY=<repo> PUSH=1 \
  SILK_AGENT_IMAGE=<Botベースイメージ@digest> SESSION_RUNTIME_IMAGE=<Sandboxベースイメージ@digest> \
  ./build.sh                                          # <名前>-bot と <名前>-sandbox をビルドしてpush
```

3. `config/sandbox-config.yaml`の`runtime.image`に、Sandboxイメージのdigestを書く。`k8s/kustomization.yaml`の`images`にBotイメージ（タグまたはdigest）を書く。
4. デプロイする（本体のリポジトリから）。

```bash
KUSTOMIZE_DIR=usecases/my-usecase/k8s ./k8s/deploy.sh   # Slackトークンの Secret、apply、rollout待ち
```

**注意**: この単一Pod構成のテンプレートは、本体の`k8s/base/`と同じ名前（Deployment `silk-agent`、namespace `session-sandbox-system`）を使う。開発用のBot（`k8s/overlays/dev`）と同じクラスタに適用すると、それを**上書き**する。本番の配置は分散構成（4章）を使う。

`kubectl kustomize usecases/my-usecase/k8s`で、適用前にマニフェストを確認できる。Deploymentの構成（レプリカ1、`Recreate`、PVC、ServiceAccountとRBAC）は本体の`k8s/base/`のままで、ユースケースは設定（ConfigMap）とイメージだけを差し替える。

### 2.7 本体の更新の取り込み

1. 本体の新しい版（タグ）を確認し、ベースイメージ（Bot、Sandbox）をビルドする。
2. `pyproject.toml`の本体のタグ、`build.sh`に渡すベースイメージのdigestを新しい版にして、ユースケースのイメージをビルドし直す。
3. 本体の変更点（設定の互換性を含む）を確認し、`config/`を直す。
4. `config/sandbox-config.yaml`のSandboxイメージのdigest、`k8s/kustomization.yaml`のBotイメージを更新してデプロイする。戻すときは、Botのイメージと設定を前の版に戻す。Sandboxのイメージは前のdigestに戻す（PVCの作業ファイルはそのまま）。

Sandboxのイメージを変えても、すでにある会話のSandboxは、次に作り直されるまで古いイメージで動く（停止から7日で削除）。

## 3. 既存のデプロイの設定の移行

この変更より前の設定で動かしていたデプロイは、次を直す。

- **相対パス**: 設定ファイル内の相対パスは、**設定ファイルのあるディレクトリ**が基準になった（以前はリポジトリのルート）。`/config/silk-agent.yaml`のConfigMapなら、`/config/...`か絶対パスで書く。
- **環境固有の値は必須**: `agent.project`、`input_guard.project`（`provider: gemini`のとき）、`output_sanitizer.template`（`model_armor`のとき）、`session_store.project`・`agent_runtime`（`agent_platform`のとき）、`sandbox.config_file`（`gke_session_sandbox`のとき）。既定値はなくなった。`agent_runtime`に`<id>`を含めると黙ってメモリの保存先になる動作はなくなった（`provider: memory`で明示する）。
- **プロンプトと基準**: Botのイメージの`policies/`はなくなった。`policies/agent-system.md`と`policies/input-guard.md`は`src/silk_agent/resources/`に移り、設定がなければそれを使う。内容を変えていた場合は、ファイルを自分のイメージ（またはConfigMap）に置いて`agent.system_prompt_file`・`input_guard.policy_file`で指す。
- **MCPの設定**: Botのイメージに入らなくなった。`mcp.config_file`をConfigMapのファイルで指す（本体の`k8s/overlays/dev`は`/config/mcp.json`）。
- **起動コマンド**: `silk-agent slack`（設定は`--config`または`SILK_AGENT_CONFIG`）。`apps/slack_bot.py`は同じものを呼ぶ互換スクリプトで残る。
- **環境変数`USER_EMAIL`→`AGENT_USER_EMAIL`**: Sandboxに渡すメールアドレスの変数名を変えた。値は、**その会話を始めたユーザ**のメールアドレスに固定される（以前は最後に書き込んだユーザ。スレッドに別のユーザが書き込んでも変わらない）。変更前に始まった会話は、管理情報に開始者がないため、環境変数を渡さずに既存のSandboxをそのまま使う（削除の7日後までは`USER_EMAIL`のまま）。新しい会話から`AGENT_USER_EMAIL`になる。イメージ内のスクリプトで`USER_EMAIL`を読んでいる場合は`AGENT_USER_EMAIL`に直す。
- **k8s**: `k8s/`は`k8s/base/`と`k8s/overlays/dev/`に分かれた（設定は`k8s/overlays/dev/config/`）。`k8s/deploy.sh`は既定で`k8s/overlays/dev`を使う。
- **Sandboxのイメージ**: タグが`<agent-sandboxの版>-session-runtime-<silk-agentの版>`になり、中のPythonパッケージは`uv.lock`の版に固定される。`startup.sh`のないイメージは従来どおり動く。

## 4. 分散構成での適用（推奨）

ここまでの手順（`k8s/base`、`k8s/deploy.sh`の既定）は単一プロセスのBot向けで、開発・小規模な確認用に残す。本番のユースケースは、分散構成（Gateway / Worker / Control / Sandbox Service）に置く。1ユースケース = 4つの役割とSandboxのひとまとまりで、同じクラスタに複数のユースケースを分離して置ける。

### 4.1 構成と分離の単位

| 役割 | 働き | 持つ認証情報 |
|---|---|---|
| Gateway | Slack（Socket Mode）から受信し、Firestoreに保存してからACKする | Slackのbot / appトークン |
| Worker | 保存された入力を処理する（Agent SDK、Sandbox呼び出し）。複数台 | なし（Slackのトークンを持たない） |
| Control | 割り当て、STOP・リセットの認可、Slackへの送信 | Slackのbotトークンだけ |
| Sandbox Service | Sandboxの作成・管理、コマンドの出力の保管 | なし（Kubernetes APIとFirestoreの権限） |

分離の単位はユースケースごとの次の4つ。

- **namespace**: 管理用の`<ユースケース>-system`（4役割）とSandbox用の`<ユースケース>-sandboxes`。
- **Firestoreの接頭辞**: 同じdatabaseの中で`<ユースケース>_commands`のようにコレクションが分かれる。`distributed.firestore_prefix`に既定値はなく、書かなければ起動しない。
- **Slackアプリ**: ユースケースごとに別のアプリ（トークン）を用意する。Secret Managerの`<ユースケース>-slack-bot-token`、`<ユースケース>-slack-app-token`に入れる。
- **Workload Identityの権限**: 各役割のServiceAccount（`<ユースケース>-system`の`silk-agent-<役割>`）に、その役割に必要な権限だけを付ける。

各役割は起動時に`POD_NAMESPACE`（Downward APIで渡される）と設定の値を照合し、食い違えば起動しない（`sandbox_service_url`のnamespace、Sandboxの`control_namespace`、`sandbox_service_allowed_ksas`のnamespace）。別のユースケースの設定をコピーしたまま配置しても、そのユースケースのService・ストアには接続しない。ローカル実行など、Kubernetesの外では`distributed.namespace_check: false`で無効にできる（起動ログに出る）。

### 4.2 手順

1. **クラウド資源（Terraform）**: リポジトリ直下の`terraform/`の`usecases`にユースケースを1件追加する（namespace、`firestore_prefix`、Slackのシークレット名。例は`usecases/bigquery-analysis/OPERATIONS.md`）。namespace（system / sandbox）とSlackのSecret IDは、全ユースケースを通して重複できない（重複するとplanで拒否される）。`terraform plan`で、追加したユースケースの資源の作成だけが出て既存の資源に変更・削除がないことを確認してからapplyする。役割の権限、Slackの Secret、Firestoreの索引とTTLが作られる。`terraform output usecases`で各役割の権限の主体を確認できる。ユースケース固有の資源（BigQueryのサービスアカウントなど）は`usecases/<名前>/terraform/`に置く。Slackのトークンの値は`gcloud secrets versions add`で登録する。
2. **テンプレートをコピーする**: `usecases/template/`を`usecases/<名前>/`にコピーし、2.1〜2.5（名前、Sandboxイメージ、Skills・プロンプト、フック、Slackの応答範囲）を行う。
3. **イメージ**: 2.6の1と2のとおりBot（4役割で共通）とSandboxのイメージをビルドしてpushする。
4. **設定と配置の定義**: `k8s/distributed/steady/`の次を書き換える（`your-...`を実際の値に）。
   - `config/silk-agent-distributed.yaml`: 4役割で共通の設定。`firestore_project`、`firestore_prefix`（Terraformの値と同じ）、`sandbox_service_url`（`silk-agent-sandbox-service.<名前>-system.svc`）、`sandbox_service_allowed_ksas`（`system:serviceaccount:<名前>-system:silk-agent-worker`）、Slackのシークレット、`command_output_bucket`。
   - `config/sandbox-service-config.yaml`: Sandbox Serviceの設定。`namespace`（`<名前>-sandboxes`）、`control_namespace`（`<名前>-system`）、Sandboxのイメージのdigest。SQLiteのパス（`repository_database_path`）は書かない（書くと起動しない）。
   - `kustomization.yaml`: 2つのnamespace（`silk-agent-namespaces`のリテラル）、Botのイメージ、ClusterRoleBindingの名前の接尾辞。
   共通の定義（`k8s/distributed/base/`、`k8s/distributed/platform/`）にはプロジェクト、イメージ、接頭辞、バケットの値を書かない。変更が必要なら本体のリポジトリで直す。
5. **配置（bootstrap → 起動）**: 本体のチェックアウトから`deploy.sh`を分散構成のモードで実行する。初回はbootstrap（3つの役割のレプリカが0）を適用し、運用runbook（`docs/distributed-operations-runbook.md` §2）の順序で役割を起動し、確認後にsteadyを適用する。

**MCPを足すとき（分散構成）**: `config/mcp.json`の`${ENV}`は**Workerの環境変数**から展開する。Workerの Deployment（`silk-agent-worker`）にその環境変数（Secretから）を足すパッチを`steady/kustomization.yaml`に書く。Workerのegressは既定で443だけなので、MCPサーバが443以外のポートなら、そのPod宛てのNetworkPolicy（egress）を足す。`usecases/bigquery-analysis/k8s/distributed/steady/echo-mcp.yaml`が、ingress・Workerのegress・サーバ自身のegress禁止の3つを持つ例。

Slackのトークンは、Secret Manager（Terraformが作った`<名前>-slack-bot-token`、`<名前>-slack-app-token`）に先に登録する。`.env`も、k8sのSecretも使わない。

```bash
printf '%s' "$SLACK_BOT_TOKEN" | gcloud secrets versions add <名前>-slack-bot-token --data-file=-
printf '%s' "$SLACK_APP_TOKEN" | gcloud secrets versions add <名前>-slack-app-token --data-file=-
DISTRIBUTED_DIR=usecases/<名前>/k8s/distributed/bootstrap SYSTEM_NAMESPACE=<名前>-system k8s/deploy.sh
DISTRIBUTED_DIR=usecases/<名前>/k8s/distributed/steady    SYSTEM_NAMESPACE=<名前>-system k8s/deploy.sh
```

`kubectl kustomize usecases/<名前>/k8s/distributed/steady`で、適用前にマニフェストを確認できる。`deploy.sh`は対象のnamespaceに`silk-agent-sandbox`の Secretを作り、4役割のrolloutを待つ（Slackのトークンは作らない。GatewayとControlが、設定の`slack.bot_token_secret`/`app_token_secret`から、自分のWorkload Identityで読む。Gatewayは両方、Controlはbot tokenだけ、WorkerとSandbox Serviceは読めない）。トークンのローテーションは、Secret Managerに新しいバージョンを登録して、GatewayとControlを再起動する（`kubectl -n <名前>-system rollout restart deploy/silk-agent-gateway deploy/silk-agent-control`）。トークンの指定漏れで開発用の`.env`に切り替わることはない（分散構成の`deploy.sh`は`.env`を読まない）。

### 4.3 運用

- **Workerの確認**: `silk-agent worker status-all`（稼働中のWorkerの一覧）、`drain-all` / `undrain-all`（受付の停止・再開）。コマンドの詳細は[運用runbook](distributed-operations-runbook.md)。ユースケースごとに設定ファイル（`--config`）が違うので、対象のユースケースの設定で実行する。
- **リリースと切り戻し**: ユースケースのイメージ（Bot、Sandbox）のdigestを`steady/kustomization.yaml`と`sandbox-service-config.yaml`に書いて適用する。戻すときは前のdigestに戻して再適用する。Workerは`maxUnavailable: 0`のローリング更新で、処理中の会話は引き継がれる。
- **削除**: namespaceを削除すれば4役割とSandboxが消える。`kubectl delete -k`で消えるクラスタ共通のリソースはユースケースごとのClusterRoleBindingだけで、TokenReview用のClusterRole（`silk-agent-sandbox-service-token-review`）は全ユースケース共通で`k8s/platform`に置くので、他のユースケースには影響しない（クラスタの初回構築では`kubectl kustomize k8s/platform | kubectl apply -f -`を先に適用する）。その後`usecases`から外してTerraformをapplyする（Firestoreの記録はTTLで消える）。

### 4.4 分離の限界

**Slack アプリはユースケースごとに分ける**: 複数のユースケースの Gateway が同じ Slack アプリのトークンで Socket Mode に接続すると、Slack は各イベントを**接続のうち1つにだけ**送る。メッセージごとに、どのユースケースの Gateway に届くかが決まらず、会話の途中で別のユースケースが応答する（履歴がないと答える、スレッドへの返信が無視される）。Secret Manager のシークレットはユースケースごとに分かれているので、それぞれに別の Slack アプリのトークンを登録する。同じワークスペースの同じチャンネルに複数のアプリを招待するときも、メンションする相手が分かるようにする。


複数のユースケースは同じFirestore databaseと同じクラスタを使う。FirestoreのIAMはコレクション単位に絞れないため、Workerの`roles/datastore.user`は、技術的には他のユースケースの接頭辞のコレクションも読み書きできる。接頭辞と起動時の照合は**取り違え**を防ぐもので、侵害された役割に対する強い分離ではない。強い分離が必要なユースケースは、Firestoreのdatabase（`distributed.firestore_database`）またはGoogle Cloudのプロジェクトを分ける。Slackの認証情報は、Secret Managerの権限がユースケースごとの役割に分かれているので、別のユースケースの役割からは読めない。コマンド出力のバケットも、ユースケースごとに別のバケット（Terraformの`usecases.<名前>.command_output_bucket`、設定ファイルの`distributed.command_output_bucket`と同じ値）で、そのユースケースの役割だけが読み書きできる。Agent PlatformのRuntime（会話履歴）は、現在は共有で、分離していない。Firestoreのバックアップは、どのユースケースの役割にも権限がない専用のバケット（Terraformの出力`firestore_backup_bucket`）に置く。
