# Proposal

## Why

既存のユースケーステンプレートには、実データを分析し、外部 MCP ツールを呼び出す一連の動作を確認できる例がない。BigQuery の公開データを使う分析エージェントと認証付き Echo MCP を用意し、Skill、Sandbox 内のツール、MCP、GKE 配備が連携することを検証できるようにする。

## What Changes

- `usecases/` に BigQuery 分析用パッケージを追加し、専用プロンプト、Skill、SQL 実行スクリプト、設定、イメージとデプロイ手順を含める。
- 指定された Google Trends の SQL を会話別 Sandbox で実行し、結果の日付・順位・検索語を根拠付きで日本語で説明できるようにする。
- FastMCP に Echo ツールを持つ HTTP サーバを追加し、Kubernetes に配備する。`X_API_KEY` ヘッダーを必須とし、Bot から Secret 経由で接続する。
- Terraform で BigQuery 用の Google Cloud Service Account と必要な IAM 権限を作成し、専用の Kubernetes ServiceAccount を Workload Identity で関連付ける。Sandbox の外向き通信は必要な宛先に限定する。
- 単体検証に加え、GKE 上のエージェント経由で SQL 実行と Echo 呼び出しを確認する手順と判定基準を追加する。

## Capabilities

### New Capabilities

- `bigquery-analysis-usecase`: Skill に従う公開 BigQuery データ分析と結果の提示。
- `echo-mcp-service`: `X_API_KEY` 認証付き Echo MCP の配備とエージェントからの利用。
- `sandbox-cloud-access`: 会話別 Sandbox に限定したクラウド認証と送信先の設定。

### Modified Capabilities

なし。既存のエージェントとユースケースの読み込み契約はそのまま利用する。

## Impact

`usecases/` にサンプルパッケージ、Echo サーバ、ユースケース専用 Terraform、Kubernetes リソースを追加する。`gke_session_sandbox` のネットワーク設定・Policy 生成にはメタデータサーバへの明示的な送信許可を追加する。Terraform は BigQuery 用 GSA、ジョブ実行権限、KSA のなりすまし許可を管理し、Kubernetes 側はその GSA を指す KSA を適用する。Kubernetes Secret、Artifact Registry、GKE 上の配備設定も必要になる。実際のプロジェクト ID、イメージ digest、API キーはリポジトリへ固定しない。
