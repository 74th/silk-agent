# Tasks

## 1. Sandbox のクラウドアクセス

- [x] 1.1 `gke_session_sandbox` に GKE メタデータサーバへの明示的な送信許可設定を追加し、無指定では既定拒否、指定時は会話別 Policy に `169.254.169.254/32:80` のみが加わることをテストで確認する。
- [x] 1.2 ユースケース専用 Terraform root に GSA、BigQuery API、`roles/bigquery.jobUser`、対象 KSA だけの `roles/iam.workloadIdentityUser`、GSA メールアドレスの output を定義し、`terraform fmt -check`、`terraform validate`、`terraform plan` で確認する。
- [x] 1.3 Terraform 出力を使って `iam.gke.io/gcp-service-account` 注釈付き KSA を Bot より先に Kubernetes へ適用する手順を作り、Terraform の namespace・KSA 名との一致と、KSA 注釈をレンダリング結果で確認する。
- [x] 1.4 BigQuery ユースケースの Sandbox 設定に専用 KSA の `serviceAccountName`、`bigquery.googleapis.com:443` とメタデータサーバの許可を加え、Pod と会話別 Policy の生成結果で確認する。

## 2. BigQuery 分析ユースケース

- [x] 2.1 `usecases/bigquery-analysis` をテンプレートから構成し、Bot/Sandbox イメージ、プロンプト、Skill、設定の参照先が存在することを構成テストで確認する。
- [x] 2.2 依頼文の SQL を Skill の付属ファイルに置き、東京日付、Japan、列、重複排除、並び、最大10行の条件が一致することをテストで確認する。
- [x] 2.3 Sandbox イメージに BigQuery クライアントを含め、固定 SQL を ADC で実行するスクリプトを作り、成功・0行・権限不足・通信失敗の出力と終了状態をテストで確認する。
- [x] 2.4 分析時にスクリプトを使う Skill とシステムプロンプトを作り、結果の根拠を示す応答と失敗時に結果を作らない指示が読み込まれることをテストで確認する。

## 3. 認証付き Echo MCP

- [x] 3.1 FastMCP の HTTP Echo サーバと `X_API_KEY` 認証層を実装し、正しい鍵で同一文字列が返り、鍵なし・誤った鍵ではツールが実行されないことを統合テストで確認する。
- [x] 3.2 空の鍵で起動失敗し、鍵が応答・通常ログに現れないことをテストで確認する。
- [x] 3.3 Echo の Dockerfile、Deployment、ClusterIP Service、Bot からの接続だけを許す NetworkPolicy、共通 Secret の参照を加え、Kustomize レンダリングで接続先と Secret の配線を確認する。
- [x] 3.4 ユースケースの `mcp.json` に HTTP URL と `${ECHO_MCP_API_KEY}` ヘッダーを設定し、既存の MCP 設定ローダーで URL と `X_API_KEY` が正しく解決されることを確認する。

## 4. 配備と実動作の確認

- [x] 4.1 Terraform apply と output、注釈付き KSA の先行適用、イメージのビルド、Secret の作成、Kustomize 適用、Terraform destroy を含むロールバック、課金プロジェクトの確認を記した運用手順を作り、手順のコマンドと参照パスを確認する。
- [x] 4.2 GKE で KSA 注釈と GSA の IAM 関連付けを確認し、gVisor Sandbox 内で ADC と指定 SQL を実行して返却列・最大10行・並び、または0行を記録する。失敗時は認証と送信許可の原因を特定して修正する。
- [x] 4.3 GKE 上で正しい鍵による MCP 接続と Echo、鍵なし・誤った鍵の拒否を確認し、検証結果を記録する。
- [x] 4.4 Slack から分析依頼と Echo 依頼を送り、エージェントのツール呼び出しと応答が各 Skill・MCP の要件を満たすことを確認して結果を記録する。
