# Spec Delta

## Purpose

エージェント（Claude Agent SDK）を、Agent Platform上のClaudeで動かし、ツールを会話専用のSandboxで実行する。MCPサーバとGCS上のSkillsを読み込み、ツールの実行を出力イベントとして通知する。

## ADDED Requirements

### Requirement: Agent Platform上のClaudeを使う
システムは、エージェントのモデル呼出しを、Agent Platform（プロジェクト`nnyn-dev`）上のClaudeで行わなければならない (SHALL)。モデルは設定で指定できなければならない (SHALL)。Anthropic APIキーを必要としてはならない (SHALL NOT)。

#### Scenario: 設定したモデルで実行
- **WHEN** 設定のモデルが`claude-haiku-5-5`でturnを実行する
- **THEN** Agent Platform上の`claude-haiku-5-5`でturnが実行される

### Requirement: ツールは会話専用のSandboxで実行する
システムは、エージェントのシェル実行とファイル操作を、会話ごとに割り当てたSandboxの中で行わなければならない (SHALL)。Botのプロセスが動くホスト上で、エージェントのシェルコマンドを実行したりファイルを操作したりしてはならない (SHALL NOT)。

#### Scenario: シェルコマンドの実行
- **WHEN** エージェントが会話`K`で`ls`を実行するツールを呼ぶ
- **THEN** コマンドは会話`K`のSandboxで実行され、その出力と終了コードがエージェントに返る

#### Scenario: 会話間の分離
- **WHEN** 会話`K1`でファイル`a.txt`を書き、会話`K2`で`a.txt`を読む
- **THEN** 会話`K2`からは`K1`の`a.txt`が見えない

#### Scenario: 組み込みツールは使えない
- **WHEN** エージェントが利用可能なツールの一覧を受け取る
- **THEN** ホスト上で実行される組み込みのシェル・ファイル操作ツールは含まれない

### Requirement: 結果が不明なコマンド
システムは、Sandboxのコマンドの結果が通信断などで不明になった場合、コマンドを自動で再実行してはならない (SHALL NOT)。コマンドIDをエージェントへ返し、後からそのIDで状態と出力を確認できるツールを提供しなければならない (SHALL)。

#### Scenario: 実行中に接続が切れた
- **WHEN** コマンドの送信後に接続が切れ、結果が分からない
- **THEN** エージェントには「結果不明」とコマンドIDが返り、同じコマンドは再送されない

### Requirement: Sandboxからの通知を会話へ反映する
システムは、Sandboxライブラリから作業環境の消失などの通知を受け取った場合、次のturnでエージェントへその内容を伝えてから、通知を処理済みにしなければならない (SHALL)。

#### Scenario: 作業環境が保持期限で削除された
- **WHEN** 会話`K`の作業環境が保持期限で削除された後、会話`K`に新しいメッセージが届く
- **THEN** そのturnのエージェントには、前回の作業ファイルは利用できないことが伝えられる

### Requirement: ツール実行の通知
システムは、エージェントがツールを呼ぶたびに、開始時にツール名と引数の要約を、終了時に成功・失敗と所要時間を出力イベントとして返さなければならない (SHALL)。MCPツールでも同じように通知しなければならない (SHALL)。

#### Scenario: ツールの成功
- **WHEN** エージェントがツール`bash`を呼び、3秒で成功する
- **THEN** ツール開始（名前`bash`、コマンドの要約）と、ツール終了（成功、3秒）が返る

#### Scenario: ツールの失敗
- **WHEN** エージェントが呼んだツールがエラーになる
- **THEN** ツール終了（失敗）が返る

### Requirement: MCPサーバの読み込み
システムは、設定ファイルに書いたMCPサーバだけをエージェントに提供しなければならない (SHALL)。設定ファイル以外（作業ディレクトリやユーザ設定）にあるMCP設定を読み込んではならない (SHALL NOT)。

#### Scenario: 設定したMCPサーバ
- **WHEN** 設定ファイルにMCPサーバ`docs`を書いて起動する
- **THEN** エージェントは`docs`のツールを使える

#### Scenario: 設定外のMCP
- **WHEN** 作業ディレクトリに`.mcp.json`があり、そこにMCPサーバ`other`が書かれている
- **THEN** エージェントは`other`のツールを使えない

### Requirement: GCS上のSkillsの読み込み
システムは、設定したGCSの場所にあるSkillsをエージェントに読み込ませなければならない (SHALL)。GCS上のSkillsが更新された場合、同期間隔（既定5分）以内に、以後に始まるturnへ反映しなければならない (SHALL)。実行中のturnが参照するSkillsを、turnの途中で入れ替えてはならない (SHALL NOT)。Skillに付属するファイルは、エージェントのSandboxからも参照できなければならない (SHALL)。

#### Scenario: Skillの利用
- **WHEN** GCSに`report`というSkillがある状態でturnを実行する
- **THEN** エージェントは`report`を使え、そのSkillに付属するスクリプトをSandboxで実行できる

#### Scenario: Skillの更新
- **WHEN** GCS上の`report`を更新し、同期間隔が経過した後に新しいturnを始める
- **THEN** 新しいturnでは更新後の`report`が使われる
