# Design

## Context

`terraform/` は現在、Agent Runtime、Model Armor、GCS、Secret Manager、Bot 用 Google サービスアカウントなどを管理し、GKE は定義していない。参照元の Terraform は `location = var.region` のリージョナル Standard クラスタと `system` / `gvisor` プールを作る。各プールの `node_count` の既定値は 1 だが、リージョナルクラスタではゾーンごとに配置される。参照元には VPC、Cloud NAT、Artifact Registry とノード用権限もある。

`k8s/deploy.sh`、`k8s/build-image.sh`、`k8s/overlays/dev/` と Sandbox のイメージ設定は旧クラスタ・旧レジストリを参照する。Bot のベース manifest は `session-sandbox-system` / `session-sandboxes` namespace と Sandbox 管理 Role を前提としており、Agent Sandbox controller とこれらのプラットフォーム manifest は参照元に残っている。既存の作業ツリーには別変更が多数あるため、適用時はその差分を保持して編集する。

## Goals / Non-Goals

**Goals:**

- GKE だけを独立して plan / apply / destroy でき、既存のアプリ用 Terraform state を巻き込まない。
- GKE 作成後に、このリポジトリだけの定義と手順で Agent Sandbox controller、プラットフォーム前提、Bot を準備できる。
- 既存の gVisor、FQDNNetworkPolicy、PVC、Workload Identity の動作条件を維持する。

**Non-Goals:**

- 旧クラスタの Terraform state や PVC の移行、既存の会話ワークスペースの復元。
- 検証用クラスタの高可用性、ノード自動スケール、クラスタへの実際の apply。
- アプリ用 `terraform/` の既存リソースを GKE 用 state に移すこと。

## Decisions

### 1. `terraform/gke/` を独立した Terraform root にする

既存の `terraform/` と別の state、provider 設定、変数、出力を持たせる。専用 VPC、サブネット、NAT、Artifact Registry、ノードサービスアカウントを GKE とともに所有する。既存 root の API や IAM resource を参照せず、GKE root 内の API resource は `disable_on_destroy = false` にする。旧検証 state の import や remote-state 参照は行わない。代替は既存 root への追加だが、検証終了時の `terraform destroy` がアプリ用リソースにも及ぶため採用しない。

### 2. ゾーンを明示してプールを固定する

既定ゾーンを `us-central1-a` とし、region は zone から整合性を保って指定する。クラスタと両ノードプールの `location` は zone、追加の `node_locations` は指定しない。クラスタのデフォルトプールは削除し、`system` / `gvisor` の `node_count = 1`、autoscaling なしとする。`gvisor` 側だけ `sandbox_config = GVISOR`、両側とも参照元と同じ `e2-standard-2` を初期値にする。小さい VM へ一律変更する案は controller、Bot、同時 Sandbox の余裕を未測定のため採用しない。単一ゾーン化と 2 ノード化を先に検証する。

### 3. 参照元の必要な GKE 機能とネットワーク境界を持ち込む

VPC-native のプライベートノード、Pod/Service セカンダリレンジ、Cloud NAT、PD CSI、Workload Identity、advanced datapath、FQDN network policy、intranode visibility を維持する。公開制御面は明示入力の運用者 CIDR に限定し、空や広すぎる設定を避ける入力検証を用意する。CIDR は参照元と衝突しない例を示すが、実際のプロジェクト内の既存範囲を plan 前に確認する。ノード用サービスアカウントにはログ・監視・Artifact Registry の pull に必要な権限だけ付ける。Bot の Google API 権限は既存の `k8s/grant-iam.sh` が KSA に直接付与する方式を維持する。より簡単な public node / default VPC 案は既存の Sandbox ネットワーク境界と異なるため採用しない。

### 4. Kubernetes の前提をリポジトリ内で完結させる

参照元の Agent Sandbox controller v1.0.6 manifest と、本番利用する namespace、Sandbox 管理 Role / RoleBinding、workspace StorageClass を `k8s/` に移し、適用順序を文書化する。検証専用 fixture や旧 worker の PVC / ServiceAccount は移さない。Bot の `k8s/base/` と重複するリソースは作らない。`k8s/deploy.sh` は新しいクラスタの context を確認し、`k8s/build-image.sh`、dev overlay、Sandbox runtime 設定は新しい Registry を参照する。参照元リポジトリへの操作手順だけを残す案では、その破棄後に新クラスタを使えないため採用しない。

### 5. Terraform 出力と手順を運用の接点にする

クラスタ名、project、zone、プール名、Registry URL、`gcloud container clusters get-credentials ... --zone ...` を出力する。README または専用 runbook に init、format、validate、plan、apply、controller/platform 適用、イメージ push、Bot デプロイ、preflight、destroy の順序を書く。破棄前に PVC / PV と `Retain` ディスクを確認する。新旧クラスタの識別子が混ざらないよう、デプロイ時の context 照合を維持する。

## Risks / Trade-offs

- [単一ゾーンまたは 1 ノードの障害で Bot / Sandbox が停止する] → 検証用途の可用性として明記し、Pod/PVC の復旧を preflight で確認する。
- [同時 Sandbox が 1 台の gVisor ノードに収まらない] → 既定のマシンタイプを保ち、実測してからサイズやノード数を変更する。
- [既存 VPC と CIDR が重複する、またはゾーンの割当量が不足する] → CIDR と zone を変数化し、適用前に計画・割当量を確認する。
- [旧レジストリの digest を新レジストリで参照できない] → runtime と Bot のイメージを新 Registry へ再 push し、新 digest を設定する。
- [controller または platform 前提の移管漏れで Sandbox が作成できない] → manifest の適用順と `gke-session-sandbox preflight`、最小の Sandbox 作成・停止確認を受け入れ手順に含める。
- [Retain の PV が Terraform の破棄後もディスクを残す] → state 外の Kubernetes ストレージを破棄前後に記録し、手動削除の対象を明示する。

## Migration Plan

1. 旧検証環境の destroy 結果と残存ディスクを確認する。新 GKE root の project、zone、CIDR、運用者 CIDR を決める。
2. `terraform/gke/` の plan を確認して apply し、出力された cluster / zone / Registry を記録する。
3. 新クラスタの認証情報を取得し、context を照合して controller と platform 前提を適用する。KSA の Google API 権限を付与する。
4. runtime と Bot のイメージを新 Registry へ push し、digest を dev overlay と Sandbox 設定へ反映する。Bot をデプロイし、preflight と最小の Sandbox ライフサイクルを確認する。
5. 切り戻しが必要なら新 Bot のデプロイを止め、使える旧クラスタが残る間は旧 context とイメージ設定に戻す。旧クラスタの destroy 完了後は旧環境へは戻せない。新 GKE の破棄は独立した state の destroy plan を確認して行う。
