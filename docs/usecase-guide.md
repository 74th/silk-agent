# ユースケースへの適用手順

silk-agent本体を変更・フォークせずに、ユースケース（用途）ごとの**システムプロンプト、入力ガードの基準、Skills、MCP、フック、Sandboxのイメージ、設定**を別のパッケージとして用意し、組み合わせてデプロイする手順。1ユースケース = 1デプロイ（1つのBotと、そのBotのSandbox）。

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
git mv src/silk_usecase_template src/my_usecase      # パッケージ名
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
- **Skills**: `skills/<名前>/SKILL.md`（と付属のファイル）。`skills.source`にBotのイメージ内のディレクトリ（`/app/usecase/skills`）かGCS（`gs://...`）を指定する。GCSなら、同期間隔（既定5分）以内に、以後に始まるturnへ反映される。イメージに含めたSkillsは、イメージを作り直すまで変わらない。
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
