# BigQuery Analysis の配備と確認

## 前提と課金確認

- GKE クラスタでは Workload Identity と GKE Dataplane V2 / FQDN Network Policy が有効であること。
- `session-sandboxes` と `session-sandbox-system` namespace、Sandbox CRD、`gvisor` RuntimeClass が用意されていること。
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

## KSA を先に適用する

Terraform の `ksa_namespace` / `ksa_name` は `config/sandbox-config.yaml` の `namespace` / `template_overrides.spec.podTemplate.spec.serviceAccountName` と `k8s/bigquery-ksa.yaml` の namespace / name に一致させる。値を変更した場合は3箇所を揃える。

```bash
GSA_EMAIL="$(terraform -chdir=usecases/bigquery-analysis/terraform output -raw gsa_email)"
export GSA_EMAIL
envsubst < usecases/bigquery-analysis/k8s/bigquery-ksa.yaml | kubectl apply -f -
kubectl get serviceaccount bigquery-analysis -n session-sandboxes \
  -o jsonpath='{.metadata.annotations.iam\.gke\.io/gcp-service-account}{"\n"}'
```

出力が Terraform の `gsa_email` と同じであることを確認する。Bot を起動する前に KSA を適用する。Terraform の Workload Identity member は `serviceAccount:<project-id>.svc.id.goog[session-sandboxes/bigquery-analysis]` のみを許可する。

## イメージ、Secret、Kubernetes リソース

`config/silk-agent.yaml`、`config/sandbox-config.yaml`、`k8s/kustomization.yaml` の `your-*` を実環境に合わせ、Bot と Sandbox のイメージを digest で固定する。Echo イメージも同じ Artifact Registry repository に push する。

```bash
cd usecases/bigquery-analysis
USECASE_NAME=bigquery-analysis IMAGE_REPOSITORY=REGION-docker.pkg.dev/PROJECT/REPOSITORY \
  TAG=0.1.0 PUSH=1 SILK_AGENT_IMAGE=BOT_BASE@sha256:DIGEST \
  SESSION_RUNTIME_IMAGE=SANDBOX_BASE@sha256:DIGEST ./build.sh
```

`build.sh` は Bot、Sandbox、Echo MCP の3イメージをビルドして push する。出力された Sandbox image digest を `config/sandbox-config.yaml` の `runtime.image` に、Bot と Echo のイメージ名・tag を `k8s/kustomization.yaml` に設定する。

Echo の鍵を安全な乱数で生成し、値をファイルやリポジトリへ保存せずに Secret を作成する。Bot と Echo サーバは同じ Secret の同じキーを参照する。

```bash
read -r -s -p 'Echo MCP API key: ' ECHO_MCP_API_KEY
printf '\n'
secret_file="$(mktemp)"
chmod 600 "$secret_file"
printf 'ECHO_MCP_API_KEY=%s\n' "$ECHO_MCP_API_KEY" > "$secret_file"
kubectl create secret generic bigquery-analysis-secrets \
  --namespace session-sandbox-system --from-env-file="$secret_file" \
  --dry-run=client -o yaml | kubectl apply -f -
rm -f "$secret_file"
unset ECHO_MCP_API_KEY secret_file
```

Kustomize の結果を確認し、KSA と Secret が存在する状態で配備する。

```bash
kubectl kustomize usecases/bigquery-analysis/k8s
kubectl apply -k usecases/bigquery-analysis/k8s
kubectl rollout status deployment/echo-mcp -n session-sandbox-system
kubectl rollout status deployment/silk-agent -n session-sandbox-system
```

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
kubectl get serviceaccount bigquery-analysis -n session-sandboxes -o yaml
gcloud iam service-accounts get-iam-policy "$(terraform -chdir=usecases/bigquery-analysis/terraform output -raw gsa_email)" \
  --project="$(awk -F'"' '/^project_id/ {print $2}' usecases/bigquery-analysis/terraform/terraform.tfvars)"
```

Slack で「日本の直近2日の急上昇検索語を調べ、日付・順位・検索語を教えて」と依頼する。監査ログのツール呼び出しに `mcp__sandbox__bash` と query script があり、返答の列・件数・順序がスクリプト出力に一致することを確認する。0行なら0行と明示する。失敗ならスクリプトのエラーを確認し、KSA annotation、GSA IAM binding、`bigquery.googleapis.com:443` と `169.254.169.254:80` の会話別 Policy を調べる。

BigQuery の jobUser は設定済み GSA にだけ付く。Sandbox Pod がこの KSA を使い、Bot の ServiceAccount や静的なサービスアカウント鍵を使っていないことも確認する。

### Echo MCP

Slack で「`echo-check-123` を Echo して」と依頼し、MCP `echo` ツールの呼び出しと、同じ文字列の返却を確認する。Bot Pod から認証なし・誤った鍵を送ると HTTP 401 になり、正しい鍵による Echo は Slack 経由で成功する。

```bash
kubectl exec -n session-sandbox-system deploy/silk-agent -- python -c \
  'import http.client; c=http.client.HTTPConnection("echo-mcp",8000); c.request("GET","/mcp"); assert c.getresponse().status==401; c=http.client.HTTPConnection("echo-mcp",8000); c.request("GET","/mcp",headers={"X_API_KEY":"wrong"}); assert c.getresponse().status==401'
```

Bot の hook ログに鍵の値がなく、Echo サーバの通常ログにも鍵や認証ヘッダーが出ていないことを確認する。Echo サービスは ClusterIP のみで、NetworkPolicy は `silk-agent` Pod からの TCP 8000 ingress だけを許可する。

## ロールバック

先に Bot と Sandbox を停止し、ユースケースの Deployment、Service、NetworkPolicy、KSA、Secret を削除する。その後専用 Terraform root のリソースを destroy する。

```bash
kubectl delete -k usecases/bigquery-analysis/k8s
kubectl delete serviceaccount bigquery-analysis -n session-sandboxes
kubectl delete secret bigquery-analysis-secrets -n session-sandbox-system
terraform -chdir=usecases/bigquery-analysis/terraform destroy -var-file=terraform.tfvars
```

この Terraform root は `disable_on_destroy = false` で BigQuery API を無効化しない。API の有効状態は他の利用者にも影響しうるため、別途利用状況を確認する。
