# Echo MCP Service Specification

## Purpose

Kubernetes 上の独立した MCP サーバと silk-agent の接続を、認証の成功と拒否を含む Echo ツールの呼び出しで検証できるようにする。

## Requirements

### Requirement: Echo MCP を提供する
ユースケースは、HTTP で接続できる MCP サーバを Kubernetes 上に配備し、受け取った文字列をそのまま返す Echo ツールを提供しなければならない (SHALL)。Bot はユースケースの MCP 設定からそのツールを利用できなければならない (SHALL)。

#### Scenario: エージェントから Echo を呼ぶ
- **WHEN** 利用者が指定した文字列の Echo を依頼し、Bot と MCP サーバの接続が有効である
- **THEN** エージェントは外部 MCP の Echo ツールを呼び、同じ文字列を返す

### Requirement: X_API_KEY で全 MCP 要求を認証する
MCP サーバは `X_API_KEY` ヘッダーの値を設定済みの鍵と照合し、有効な鍵を持つ要求だけを処理しなければならない (SHALL)。欠落または不一致の要求はツール実行前に拒否しなければならない (SHALL)。鍵は Kubernetes Secret から供給し、リポジトリのファイル、応答、通常のログへ出してはならない (SHALL NOT)。鍵が設定されていないサーバは起動してはならない (SHALL NOT)。

#### Scenario: 正しい鍵を使う
- **WHEN** Bot が Secret 由来の正しい `X_API_KEY` を付けて MCP 要求を送る
- **THEN** MCP 接続と Echo 呼び出しが成功する

#### Scenario: 鍵がない、または誤っている
- **WHEN** `X_API_KEY` がない、または設定値と異なる MCP 要求を送る
- **THEN** サーバは認証エラーを返し、Echo ツールは実行されない

### Requirement: 疎通検証手順を提供する
ユースケースは、配備後に Bot を介した Echo 呼び出しと、鍵なし・誤った鍵の拒否を確認する手順を提供しなければならない (SHALL)。

#### Scenario: 配備を検証する
- **WHEN** 運用者が文書の疎通検証を実行する
- **THEN** 成功時に入力と同じ Echo 結果を、拒否時に認証エラーを確認できる
