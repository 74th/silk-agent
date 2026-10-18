# BigQuery Analysis の配備と確認

## 前提と課金確認

- GKE クラスタでは Workload Identity と GKE Dataplane V2 / FQDN Network Policy が有効であること。
- Sandbox CRD、`gvisor` RuntimeClass が用意されていること。namespace（`bigquery-analysis-system` / `bigquery-analysis-sandboxes`）は `deploy.sh` が作る。
- BigQuery ジョブを実行する Google Cloud project を決め、請求先とクエリ料金を確認すること。公開データの保存料はかからないが、クエリのスキャン量に応じてジョブ実行 project に課金される。`LIMIT 10` はスキャン量を制限しない。
- Sandbox の成功条件は特定の検索語ではなく、`refresh_date`、`rank`、`term` の列、最大10行、日付降順・順位昇順、または0行であること。

まず実際の請求先 project とクエリ料金を確認し、`terraform/terraform.tfvars.example` を `terraform/terraform.tfvars` にコピーして `project_id` を設定する。Terraform state はこのユースケース専用 root で管理する。

```bash
cp usecases/bigquery-analysis/terraform/terraform.tfvars.example \
  usecases/bigquery-analysis/terraform/terraform.tfvars
```

```bash
terraform -chdir=usecases/bigquery-analysis/terraform init
terraform -chdir=usecases/bigquery-analysis/terraform fmt -check
terraform -chdir=usecases/bigquery-analysis/terraform validate
terraform -chdir=usecases/bigquery-analysis/terraform plan -var-file=terraform.tfvars
```

計画に表示される project と4つのリソースを確認してから適用する。

```bash
terraform -chdir=usecases/bigquery-analysis/terraform apply -var-file=terraform.tfvars
terraform -chdir=usecases/bigquery-analysis/terraform output -raw gsa_email
```

## ユースケースの一覧に追加する（Terraform）

bigquery-analysis は分散構成（Gateway / Worker / Control / Sandbox Service）で動かす。`terraform/`（リポジトリ直下）の `usecases` に次の1件を追加して apply し、役割の Workload Identity の権限、Slack の Secret、Firestore の索引と TTL を作る（既存のユースケースの資源は変わらない）。

```hcl
usecases = {
  # ...既存の silk-agent...
  "bigquery-analysis" = {
    system_namespace  = "bigquery-analysis-system"
    sandbox_namespace = "bigquery-analysis-sandboxes"
    firestore_prefix  = "bigquery-analysis"
    slack_secret_ids = {
      bot_token = "bigquery-analysis-slack-bot-token"
      app_token = "bigquery-analysis-slack-app-token"
    }
  }
}
```

`terraform -chdir=terraform plan` で作成だけが出る（既存の資源に変更・削除が出ない）ことを確認してから apply する。Slack の Secret の値は bigquery-analysis 用の Slack アプリのトークンを `gcloud secrets versions add` で登録する。

## KSA を先に適用する

Terraform の `ksa_namespace` / `ksa_name` は `k8s/distributed/steady/config/sandbox-service-config.yaml` の `namespace` / `template_overrides.spec.podTemplate.spec.serviceAccountName` と `k8s/distributed/steady/bigquery-ksa.yaml` の namespace / name に一致させる（`bigquery-analysis-sandboxes` / `bigquery-analysis`）。値を変更した場合は3箇所を揃える。KSA の namespace は `deploy.sh` が作るので、先に bootstrap で namespace を作ってから適用する（次の節の1回目）。

```bash
GSA_EMAIL="$(terraform -chdir=usecases/bigquery-analysis/terraform output -raw gsa_email)"
export GSA_EMAIL
envsubst < usecases/bigquery-analysis/k8s/distributed/steady/bigquery-ksa.yaml | kubectl apply -f -
kubectl get serviceaccount bigquery-analysis -n bigquery-analysis-sandboxes \
  -o jsonpath='{.metadata.annotations.iam\.gke\.io/gcp-service-account}{"\n"}'
```

出力が Terraform の `gsa_email` と同じであることを確認する。Sandbox を作る前に KSA を適用する。Terraform の Workload Identity member は `serviceAccount:<project-id>.svc.id.goog[bigquery-analysis-sandboxes/bigquery-analysis]` のみを許可する。

## イメージ、Secret、Kubernetes リソース

`k8s/distributed/steady/config/silk-agent-distributed.yaml`、`k8s/distributed/steady/config/sandbox-service-config.yaml`、`k8s/distributed/steady/kustomization.yaml` の `your-*` と環境の値を実環境に合わせ、Bot と Sandbox のイメージを digest で固定する。Echo イメージも同じ Artifact Registry repository に push する。

```bash
cd usecases/bigquery-analysis
USECASE_NAME=bigquery-analysis IMAGE_REPOSITORY=REGION-docker.pkg.dev/PROJECT/REPOSITORY \
  TAG=0.1.0 PUSH=1 SILK_AGENT_IMAGE=BOT_BASE@sha256:DIGEST \
  SESSION_RUNTIME_IMAGE=SANDBOX_BASE@sha256:DIGEST ./build.sh
```

`build.sh` は Bot、Sandbox、Echo MCP の3イメージをビルドして push する。出力された Sandbox image digest を `k8s/distributed/steady/config/sandbox-service-config.yaml` の `runtime.image` に、Bot と Echo のイメージ名・digest を `k8s/distributed/steady/kustomization.yaml` に設定する。

Echo の鍵を安全な乱数で生成し、値をファイルやリポジトリへ保存せずに Secret を作成する。Worker と Echo サーバは同じ Secret の同じキーを参照する。Secret は管理用 namespace に作る（namespace は次の `deploy.sh` が作るので、先に `kubectl create namespace bigquery-analysis-system` でもよい）。

```bash
kubectl create namespace bigquery-analysis-system --dry-run=client -o yaml | kubectl apply -f -
read -r -s -p 'Echo MCP API key: ' ECHO_MCP_API_KEY
printf '\n'
secret_file="$(mktemp)"
chmod 600 "$secret_file"
printf 'ECHO_MCP_API_KEY=%s\n' "$ECHO_MCP_API_KEY" > "$secret_file"
kubectl create secret generic bigquery-analysis-secrets \
  --namespace bigquery-analysis-system --from-env-file="$secret_file" \
  --dry-run=client -o yaml | kubectl apply -f -
rm -f "$secret_file"
unset ECHO_MCP_API_KEY secret_file
```

配備は silk-agent のチェックアウトから `deploy.sh` の分散構成のモードで行う。bigquery-analysis 用の Slack アプリのトークンを入れた `.env`（`SLACK_BOT_TOKEN` / `SLACK_APP_TOKEN`）を `SILK_AGENT_ENV_FILE` に指定する。1回目は bootstrap（replicas 0）、起動は運用 runbook（`docs/distributed-operations-runbook.md` §2）の順序に従い、確認後に steady を適用する。

```bash
kubectl kustomize usecases/bigquery-analysis/k8s/distributed/steady
export SILK_AGENT_ENV_FILE=/path/to/bigquery-analysis.env
DISTRIBUTED_DIR=usecases/bigquery-analysis/k8s/distributed/bootstrap \
  SYSTEM_NAMESPACE=bigquery-analysis-system k8s/deploy.sh
DISTRIBUTED_DIR=usecases/bigquery-analysis/k8s/distributed/steady \
  SYSTEM_NAMESPACE=bigquery-analysis-system k8s/deploy.sh
kubectl rollout status deployment/echo-mcp -n bigquery-analysis-system
```

Worker の状態は `silk-agent worker status-all` で確認する。

## 実動作の確認

### 2026-10-12 の配備確認

- `nnyn-dev` で BigQuery API を有効化し、専用 GSA に `roles/bigquery.jobUser`、対象 KSA `session-sandboxes/bigquery-analysis` に限定して `roles/iam.workloadIdentityUser` を付与した。KSA の GSA 注釈も一致する。
- Bot、Sandbox、Echo MCP の digest 固定イメージを Artifact Registry へ push し、GKE に配備した。更新した `.env` の Slack Bot/App token を `silk-agent-slack` Secret に反映し、Bot の Socket Mode 接続ログを確認した。
- 初回の Slack 依頼はイベントで届いたが、許可 Workspace ID が Bot token の `auth.test` 結果と異なり、`AllowListFilter` により除外された。許可 Workspace ID を `auth.test` の `T8W42FC4U` に修正して再配備した。
- Bot Pod から Echo MCP を確認し、鍵なし・誤った鍵はともに HTTP 401、正しい鍵で `echo-check-123` を送ると同じ文字列が返った。
- Slack で `echo-check-123` の依頼を送り、`mcp__echo__echo` が成功して同じ文字列が返信されることを確認した。
- Sandbox の固定 SQL を Sandbox イメージへ含めるよう Dockerfile を修正し、更新イメージを push して Bot 設定を再配備した。
- 許可 Workspace ID 修正後の Slack 依頼は受理され、`google-trends` Skill と `mcp__sandbox__bash` が成功した。gVisor Pod は `bigquery-analysis` KSA / 専用 GSA を使い、会話別 policy に `bigquery.googleapis.com:443` と metadata `169.254.169.254:80` が設定された。Slack へは正常終了・0行が返った。

### KSA / GSA と BigQuery

```bash
kubectl get serviceaccount bigquery-analysis -n bigquery-analysis-sandboxes -o yaml
gcloud iam service-accounts get-iam-policy "$(terraform -chdir=usecases/bigquery-analysis/terraform output -raw gsa_email)" \
  --project="$(awk -F'"' '/^project_id/ {print $2}' usecases/bigquery-analysis/terraform/terraform.tfvars)"
```

Slack で「日本の直近2日の急上昇検索語を調べ、日付・順位・検索語を教えて」と依頼する。監査ログのツール呼び出しに `mcp__sandbox__bash` と query script があり、返答の列・件数・順序がスクリプト出力に一致することを確認する。0行なら0行と明示する。失敗ならスクリプトのエラーを確認し、KSA annotation、GSA IAM binding、`bigquery.googleapis.com:443` と `169.254.169.254:80` の会話別 Policy を調べる。

BigQuery の jobUser は設定済み GSA にだけ付く。Sandbox Pod がこの KSA を使い、Bot の ServiceAccount や静的なサービスアカウント鍵を使っていないことも確認する。

### Echo MCP

Slack で「`echo-check-123` を Echo して」と依頼し、MCP `echo` ツールの呼び出しと、同じ文字列の返却を確認する。Worker Pod から認証なし・誤った鍵を送ると HTTP 401 になり、正しい鍵による Echo は Slack 経由で成功する。

```bash
kubectl exec -n bigquery-analysis-system deploy/silk-agent-worker -- python -c \
  'import http.client; c=http.client.HTTPConnection("echo-mcp",8000); c.request("GET","/mcp"); assert c.getresponse().status==401; c=http.client.HTTPConnection("echo-mcp",8000); c.request("GET","/mcp",headers={"X_API_KEY":"wrong"}); assert c.getresponse().status==401'
```

Bot の hook ログに鍵の値がなく、Echo サーバの通常ログにも鍵や認証ヘッダーが出ていないことを確認する。Echo サービスは ClusterIP のみで、NetworkPolicy は `silk-agent-worker` Pod からの TCP 8000 ingress だけを許可する（Worker 側は `echo-mcp-egress-from-worker` で許可する）。

## ロールバック

ユースケースは専用の namespace に閉じているので、namespace ごと削除すれば Bot（4 役割）、Sandbox、Echo、KSA、Secret が消える。その後専用 Terraform root のリソースを destroy し、リポジトリ直下の `usecases` から bigquery-analysis を外して apply する（索引・権限・Slack の Secret が消える。`usecases` から外すと TTL ポリシーも消え、期限のない記録もあるので、Firestore の記録は外す前にこのユースケースの接頭辞のデータを削除しておく）。

```bash
kubectl delete -k usecases/bigquery-analysis/k8s/distributed/steady   # 共通のClusterRole（k8s/platform）は消えない
kubectl delete namespace bigquery-analysis-system bigquery-analysis-sandboxes
terraform -chdir=usecases/bigquery-analysis/terraform destroy -var-file=terraform.tfvars
```

この Terraform root は `disable_on_destroy = false` で BigQuery API を無効化しない。API の有効状態は他の利用者にも影響しうるため、別途利用状況を確認する。

## コマンド出力の保存先

このユースケースのコマンド出力は、専用のバケット `nnyn-dev-bigquery-analysis-command-output`（リポジトリ直下の Terraform の `usecases.bigquery-analysis.command_output_bucket`）に保存する。設定ファイル `k8s/distributed/steady/config/silk-agent-distributed.yaml` の `distributed.command_output_bucket` と同じ値にする。他のユースケースのバケットには権限がない。2026-10-19 より前のコマンド出力は `silk-agent` のバケットにあり、このユースケースの Worker からは読めない（移行していない）。
