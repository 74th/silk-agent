# sandbox-startup Specification

## Purpose
Sandboxコンテナの起動時に、ユースケースがイメージ内に置いた`startup.sh`を実行し、ファイルのダウンロードや初期設定などを、エージェントがコマンドを実行する前に済ませられるようにする。

## Requirements
### Requirement: 起動時にstartup.shを実行する
Sandboxのsupervisorは、コンテナの起動のたび（Sandboxの作成時と、停止からの再開時）に、イメージ内の起動スクリプト（既定`/opt/session-runtime/startup.sh`、環境変数`SANDBOX_STARTUP_SCRIPT`で変更可）が存在すれば、sandboxdを起動する前に実行しなければならない (SHALL)。スクリプトが存在しなければ、何もせずに起動を続けなければならない (SHALL)。スクリプトが終わるまで、Sandboxを準備完了にしてはならない (SHALL NOT)。

#### Scenario: Sandboxの作成時
- **WHEN** `startup.sh`が`/workspace/data.csv`をダウンロードするイメージで、新しい会話のSandboxが作られる
- **THEN** エージェントの最初のコマンドの時点で`/workspace/data.csv`が存在する

#### Scenario: 再開時
- **WHEN** 停止したSandboxが再開される
- **THEN** `startup.sh`がもう一度実行される

#### Scenario: スクリプトがない
- **WHEN** `startup.sh`を置いていないイメージでSandboxが作られる
- **THEN** Sandboxは従来どおり起動する

### Requirement: 起動スクリプトの実行環境
システムは、起動スクリプトを、Sandboxのコンテナと同じユーザ、作業ディレクトリ`/workspace`、コンテナの環境変数（Sandboxの作成時に渡した`AGENT_USER_EMAIL`を含む）で実行しなければならない (SHALL)。初回の起動かどうかを示す環境変数`SANDBOX_STARTUP_FIRST_RUN`（初回は`1`、以後は`0`）を加えなければならない (SHALL)。スクリプトの標準出力と標準エラーは、コンテナのログに出さなければならない (SHALL)。

#### Scenario: メールアドレスの受け渡し
- **WHEN** Slackユーザ`alice@example.com`の会話でSandboxが作られる
- **THEN** 起動スクリプトの中で`$AGENT_USER_EMAIL`が`alice@example.com`になる

#### Scenario: 初回の判定
- **WHEN** Sandboxが作られ、停止し、再開される
- **THEN** 作成時の実行では`SANDBOX_STARTUP_FIRST_RUN=1`、再開時の実行では`0`になる

### Requirement: Sandboxに渡すメールアドレスは会話を始めたユーザのもの
システムは、Sandboxの作成時に渡す`AGENT_USER_EMAIL`を、その会話を始めたユーザのメールアドレスにしなければならない (SHALL)。会話を始めたユーザは会話の管理情報に保存し、Botの再起動後も同じ値を使わなければならない (SHALL)。スレッドにほかのユーザが書き込んでも、`AGENT_USER_EMAIL`を変えてはならない (SHALL NOT)。

#### Scenario: 別のユーザが返信した後の再開
- **WHEN** `alice@example.com`がメンションして始めた会話で、Sandboxが停止し、Botが再起動した後に、`bob@example.com`がスレッドに返信する
- **THEN** Sandboxはエラーにならずに再開し、`AGENT_USER_EMAIL`は`alice@example.com`のまま

### Requirement: エージェントのコマンドにもメールアドレスを渡す
システムは、エージェントがSandboxで実行するコマンド（sandboxd経由で起動するもの。フォアグラウンドとバックグラウンドの両方）の環境変数に、Sandboxの作成時に渡した`AGENT_USER_EMAIL`を含めなければならない (SHALL)。停止からの再開後も同じ値でなければならない (SHALL)。

#### Scenario: コマンドからの参照
- **WHEN** `alice@example.com`が始めた会話で、エージェントが`bash`で`echo $AGENT_USER_EMAIL`を実行する
- **THEN** 出力は`alice@example.com`になる

#### Scenario: バックグラウンドのコマンド
- **WHEN** 同じ会話で、`sleep 30; echo $AGENT_USER_EMAIL`をバックグラウンドで実行する
- **THEN** 終了の通知に含まれる出力は`alice@example.com`になる

#### Scenario: 再開後のコマンド
- **WHEN** Sandboxが停止し、再開した後に`echo $AGENT_USER_EMAIL`を実行する
- **THEN** 出力は`alice@example.com`のまま

### Requirement: 起動スクリプトの失敗と上限時間
システムは、起動スクリプトが0以外で終了した場合、または上限時間（既定300秒、環境変数`SANDBOX_STARTUP_TIMEOUT_SECONDS`）を超えた場合、既定ではsandboxdを起動せずにコンテナを異常終了させなければならない (SHALL)。環境変数`SANDBOX_STARTUP_ON_ERROR=continue`が設定されていれば、失敗をログに出したうえで起動を続けなければならない (SHALL)。

#### Scenario: スクリプトの失敗
- **WHEN** 起動スクリプトが終了コード1で終わる
- **THEN** sandboxdは起動せず、コンテナのログにスクリプトの失敗と終了コードが出る

#### Scenario: 失敗しても続ける設定
- **WHEN** `SANDBOX_STARTUP_ON_ERROR=continue`で、起動スクリプトが終了コード1で終わる
- **THEN** 失敗がログに出て、Sandboxは起動する

### Requirement: 準備完了を待つ時間を設定できる
システムは、Sandboxの作成・再開で準備完了を待つ上限時間を、Sandboxの設定で変えられなければならない (SHALL)。設定しない場合は従来どおり180秒でなければならない (SHALL)。

#### Scenario: 長い起動スクリプト
- **WHEN** 起動スクリプトに約4分かかるイメージで、準備完了を待つ上限時間を600秒に設定する
- **THEN** Sandboxの作成は失敗せず、準備完了の後にエージェントのコマンドが実行される
