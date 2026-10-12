# Design

## Context

`usecases/template` は Bot と Sandbox の派生イメージ、`skills/`、`prompts/`、`config/mcp.json`、Kustomize の構成を既に備える。外部 MCP は `src/silk_agent/agent/mcp_config.py` が HTTP/SSE と `${ENV}` のヘッダー展開に対応し、エージェントは設定済みサーバだけを読む。会話別 Sandbox は `templates/sandbox.yaml` で ServiceAccount トークンを自動マウントせず、`src/gke_session_sandbox/network.py` が既定拒否、DNS、設定された FQDN の Policy を会話ごとに生成する。現在のリポジトリには BigQuery の実行ツールや SQL ファイルはないため、依頼文の SQL からサンプルに新規作成する。

## Goals / Non-Goals

**Goals:**

- 本体の汎用エージェントを変更せず、独立したユースケースで Skill とスクリプトを用いる分析経路を示す。
- 認証された外部 MCP 呼び出しを、Bot から Kubernetes 内のサービスまで通して確認する。
- 会話別 Sandbox だけに BigQuery の実行権限と必要な送信先を与える。

**Non-Goals:**

- 任意の SQL を実行する汎用分析基盤、検索語の将来予測、BigQuery データの永続保存。
- 外部公開する MCP エンドポイントや利用者ごとに異なる BigQuery 権限。

## Decisions

### 1. サンプルを `usecases/bigquery-analysis` に独立して配置する

`usecases/template` を基に Bot/Sandbox の Dockerfile、設定、Skill、プロンプト、Kustomize を一体として置く。SQL は Skill の付属ファイル、実行スクリプトは同じ Skill の `scripts/` に置き、Sandbox にコピーされる既存の Skill 経路から呼び出す。スクリプトは固定 SQL のみを実行し、ジョブ用プロジェクトを環境変数で受け、行を機械的に読める形式で出力する。SQL 自体は依頼文の SELECT/GROUP BY/ORDER BY/LIMIT と東京日付の条件を保つ。

Python の BigQuery クライアントを Sandbox イメージに入れ、Application Default Credentials を使う。`bq` CLI の導入も可能だが、実行環境への依存が増え、スクリプトのエラー分類と出力形式を安定させにくい。Skill はスクリプトの呼び方、結果の読み方、失敗時の扱いを記し、プロンプトは分析依頼で Skill を参照するよう指示する。

### 2. Sandbox に専用の Workload Identity を与える

`usecases/bigquery-analysis/terraform` を独立した Terraform root として置き、既存の `terraform/`（本体アプリ）と `terraform/gke/`（クラスタ）の state にこのユースケース固有の IAM を混ぜない。入力として GCP プロジェクト、KSA の namespace と名前、GSA の ID を受け取る。Terraform は BigQuery API、専用 GSA、ジョブ実行プロジェクトの `roles/bigquery.jobUser`、GSA に対する KSA の `roles/iam.workloadIdentityUser` を管理し、GSA メールアドレスを出力する。なりすまし許可の member は GKE のプール名と `namespace/KSA` の組み合わせに絞る。KSA principal に直接 BigQuery 権限を付ける案は、ユーザが指定した Terraform 管理の GSA を経由しないため採らない。

Kubernetes 適用手順は、Terraform の GSA メールアドレス出力を読み、`iam.gke.io/gcp-service-account` 注釈付きの専用 KSA を先に適用する。その後、ユースケース設定の `template_overrides` で Sandbox Pod の `serviceAccountName` をこの KSA に固定し、Bot を配備する。適用前に Terraform の namespace・KSA 名と Kubernetes manifest が一致することを検証する。KSA の Kubernetes API トークンの自動マウントは無効のままにする。サービスアカウント鍵を Secret として配る案は、長寿命の鍵が会話別 Sandbox に残るため採らない。

既定拒否を保ちつつ、`gke_session_sandbox` の `network` 設定に GKE メタデータサーバへの明示的な許可を追加する。許可時だけ、生成する会話別 NetworkPolicy に `169.254.169.254/32` の TCP 80 を加える。BigQuery API への TCP 443 は既存の `allowed_fqdns` に `bigquery.googleapis.com` を指定する。IP を広く許可する案や Namespace 全体へ静的 Policy を追加する案は、他の会話・ユースケースへ権限が広がるため採らない。GKE の実環境で gVisor とメタデータサーバの疎通を検証し、追加の認証先が必要ならその FQDN を明示して検証する。

### 3. Echo MCP を Kubernetes 内部の HTTP サービスとして実装する

FastMCP の Echo ツールを含む別コンテナを `Deployment` と `ClusterIP Service` として配備し、Bot の `mcp.json` からサービス DNS の `/mcp` へ接続する。ASGI の認証層で MCP の全要求について `X_API_KEY` を照合し、ツールに到達する前に不正な要求を拒否する。空の鍵ではサーバを起動しない。依存版を固定し、実装時に選んだ FastMCP 版で HTTP と認証の統合テストを行う。

同一の Kubernetes Secret から Bot の `ECHO_MCP_API_KEY` と MCP サーバの鍵環境変数を供給する。`mcp.json` は `${ECHO_MCP_API_KEY}` を展開したヘッダーを送る。URL と鍵をプロンプトや Skill に書かない。`X_API_KEY` の下線を維持するため外部 Ingress は通さず、クラスター内部で接続する。MCP 側の Ingress は Bot の Pod からの TCP 接続だけに限定する。

### 4. 実動作を二段階で検証する

ローカルでは Terraform の format/validate/plan、GSA 出力と KSA 注釈の対応、SQL の固定性と出力・失敗処理、Skill/プロンプト/設定の参照整合性、認証なし・誤った鍵・正しい鍵の MCP 要求、Kustomize のレンダリング、ネットワーク Policy の opt-in を検証する。GKE では KSA と GSA の関連付けおよび Sandbox 内の ADC を確認して指定 SQL を実行し、Slack からの分析依頼と Echo 依頼で、エージェントがそれぞれ Skill と外部 MCP を使ったことをログと応答で確認する。公開データの内容は日々変わるため、特定の検索語を成功条件にしない。

## Risks / Trade-offs

- [GKE のメタデータ通信と NetworkPolicy の実装差] → gVisor 上の ADC 疎通を実クラスタで確認し、必要な宛先だけを設定・文書化する。
- [公開データの更新遅延や空結果] → 0行を正常な結果として扱い、実データの語句ではなく列・件数・並びを検証する。
- [API キーが Bot のプロセス環境に存在する] → Secret からのみ注入し、ログへ出さず、Echo サーバを ClusterIP と限定した Ingress で閉じる。
- [SQL ジョブの課金] → `LIMIT 10` は走査量を制限しないため、検証前に課金プロジェクトと想定走査量を確認し、必要に応じてジョブの最大課金バイト数を設定する。

## Migration Plan

既存の Bot 配備は変更せず、ユースケース専用 Terraform を apply して GSA と IAM を作成する。出力された GSA メールアドレスで注釈付き KSA を先に適用し、API キー Secret を準備してから Echo と BigQuery ユースケースのイメージをビルド・配備する。Kustomize の出力、KSA 注釈、MCP、Sandbox の ADC と SQL 実行、Slack 経由の両経路を順に検証する。戻す場合は先にユースケースの Bot と Sandbox を停止し、Deployment、Service、KSA、Secret を削除してから専用 Terraform root を destroy する。既定のネットワーク設定は変わらない。
