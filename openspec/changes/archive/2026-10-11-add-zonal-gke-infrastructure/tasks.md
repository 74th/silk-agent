# Tasks

## 1. GKE 用 Terraform root

- [x] 1.1 `terraform/gke/` に既存のアプリ用 `terraform/` と独立した provider、version、state 設定と入力例を作る。`terraform init -backend=false` と `terraform validate` がこの root だけで通ることを確認する。
- [x] 1.2 GKE 専用 VPC、サブネットと Pod/Service の範囲、Cloud Router/NAT、必要な API とノードサービスアカウント・最小権限を定義する。`terraform validate` と plan の resource 一覧で、旧 state や既存アプリ用リソースを参照しないことを確認する。
- [x] 1.3 `location` を既定 `us-central1-a` のゾーンにした Standard クラスタを定義し、プライベートノード、制御面 CIDR 制限、PD CSI、Workload Identity、advanced datapath、FQDNNetworkPolicy を設定する。`terraform plan` で zonal location と必要な設定を確認する。
- [x] 1.4 デフォルトプールを削除し、同じゾーンに autoscaling なし・各 `node_count = 1` の `system` / `gvisor` を定義する。`gvisor` の Sandbox 設定を含め、`terraform plan` でノードプール 2 個、目標ノード計 2 台を確認する。
- [x] 1.5 Artifact Registry とクラスタ名・zone・プール名・Registry URL・認証情報取得コマンドの出力を追加する。`terraform output` 定義と plan を見て、旧レジストリ名が含まれないことを確認する。

## 2. Kubernetes とデプロイ設定

- [x] 2.1 参照元から Agent Sandbox controller v1.0.6 と必要な platform manifest を `k8s/` に移し、Bot の base と重複するリソースを除く。`kubectl kustomize` 等で生成した manifest に controller、namespace、Sandbox 管理 RBAC、workspace StorageClass があることを確認する。
- [x] 2.2 `k8s/deploy.sh` の context 照合、`k8s/build-image.sh` の Registry 既定値、dev overlay と Sandbox runtime image の設定を新クラスタ用に更新する。`rg` で実行用ファイルから旧クラスタ・Registry の固定参照が消え、`kubectl kustomize k8s/overlays/dev` が成功することを確認する。
- [x] 2.3 `scripts/build-session-runtime-image.sh` と `k8s/grant-iam.sh` が新 root の project、zone、Registry 出力を使う手順と整合するように調整する。シェル構文確認と設定値の照合で、旧環境への暗黙の push / 権限付与がないことを確認する。

## 3. 運用手順と検証

- [x] 3.1 README または専用 runbook に `terraform/gke/` の入力、init・plan・apply、controller/platform 適用順、イメージ push、Bot デプロイ、preflight、独立 state の destroy plan と残存 PV / ディスク確認を記す。文書中のコマンドが新しい cluster / zone / Registry 出力と一致することを確認する。
- [x] 3.2 `.gitignore` で `terraform/gke/` の state、plan、ローカル tfvars と認証情報を追跡しないようにし、`git status --short --ignored terraform/gke` で意図したファイルだけが無視されることを確認する。
- [x] 3.3 `terraform fmt -check -recursive`、`terraform validate`、可能なら認証済み環境で保存済み `terraform plan` を実行し、ゾーンが 1 つ、2 プール各 1 ノード、既存アプリ用リソースの変更なしと確認する。クラスタを作成する場合の受け入れ手順として `gke-session-sandbox preflight` と Sandbox の作成・停止を文書に記載し、実行結果がなければ未実施と明記する。
