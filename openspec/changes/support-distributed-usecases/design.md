# Design

## Context

- 単一プロセスの配置: `usecases/<name>/config/kustomization.yaml` が `k8s/base` を参照し、ConfigMap `silk-agent-config`（namespace `session-sandbox-system`）を作る。`KUSTOMIZE_DIR=usecases/<name>/k8s ./k8s/deploy.sh` で適用する。テンプレートには `silk-agent-distributed.yaml` があるが、ConfigMap に入っておらず配置されない。テンプレートの `sandbox-config.yaml` は SQLite のパスを持ち、分散構成では拒否される（`config.py` の `validate_distributed_sandbox_config`）。
- 分散構成の配置: `k8s/distributed/` は 1 つの ConfigMap（`silk-agent-sandbox-service-config`）を 4 役割で共有し、`roles.yaml`（SA、Deployment、NetworkPolicy、Role）と `sandbox-service.yaml`（SA、ClusterRole / ClusterRoleBinding `silk-agent-sandbox-service-token-review`、`session-sandboxes` への RoleBinding、NetworkPolicy、Service、Deployment）を持つ。image と設定は開発環境の値で固定されている。
- 固定の名前（衝突・混線の原因）: Secret `silk-agent-slack` / `silk-agent-sandbox`、ClusterRole / ClusterRoleBinding、`sandbox_service_url` の `session-sandbox-system`、`sandbox_service_allowed_ksas`、Sandbox Service の NetworkPolicy の egress 先の namespace、`k8s/platform/base.yaml` の 2 つの namespace と Role `sandbox-management`、`k8s/grant-iam.sh` の主体、Terraform の `locals.gke_principal_prefix`（`ns/session-sandbox-system/sa`）と Slack の Secret の名前、索引のコレクション名（`silk-agent_commands`、`silk-agent_outbox`）。
- Firestore のコレクション名はすべて `{firestore_prefix}_<name>` で、Sandbox の管理情報（`firestore_repository.py`）も同じ接頭辞を使う。`inputs` / `controls` はサブコレクションなので、接頭辞を分ければ会話ごと分かれる。コレクショングループの索引（`inputs`、`controls`）は接頭辞に関係なく共有される。
- Gateway / Worker / Control にはユースケースの概念がなく、接頭辞・Service の URL・KSA で分かれる。Slack のトークンは環境変数（Secret）か Secret Manager から読む。
- 分離の単位はユーザと決めた: ユースケースごとに namespace を分け、Firestore は同じ database で接頭辞を分ける。

## Goals / Non-Goals

**Goals:**
- ユースケースが、共通の配置定義と少しの値だけで分散構成に配置できる。
- 2 つ以上のユースケースを同じクラスタで動かしても、会話・Worker・Sandbox・認証情報が混ざらない。
- 開発環境の今の配置を、状態の移行なしに新しい構造へ移す。

**Non-Goals:**
- 1 つの Worker のプールで複数のユースケースを処理すること（Worker はユースケース専用）。
- ユースケースごとの Firestore の database、ユースケースごとのクラスタ。
- 単一プロセスの配置（`k8s/base`）の廃止。ドキュメントでは分散構成を推奨とし、単一プロセスは開発用として残す。
- 既存の bigquery-analysis の会話の移行（新しい namespace と接頭辞で始める）。

## Decisions

### D1. 名前の付け方

ユースケース名 `<u>`（DNS ラベル）から次を決める。

| 対象 | 値 |
|---|---|
| 管理用 namespace | `<u>-system` |
| Sandbox 用 namespace | `<u>-sandboxes` |
| Firestore の接頭辞 | `<u>` |
| Slack の Secret Manager の Secret | `<u>-slack-bot-token`、`<u>-slack-app-token` |
| Kubernetes の Secret | `silk-agent-slack`、`silk-agent-sandbox`（namespace が違うので名前は共通のまま） |

namespace の中のリソース名（SA、Deployment、Service、NetworkPolicy、ConfigMap）は今の名前のままにし、namespace で分ける。これにより、NetworkPolicy のラベルや Service の名前を書き換える必要がない。ただし、クラスタスコープの ClusterRoleBinding だけは `silk-agent-sandbox-service-token-review-<u>` にする（ClusterRole は共通の 1 つ）。

開発環境の既存の配置は例外として `session-sandbox-system` / `session-sandboxes` / `silk-agent` のまま overlay で指定する（D3）。

- 却下した案: kustomize の `namePrefix` で同じ namespace に置く案（ユーザと相談の上で却下）。

### D2. 共通の base と overlay の構成

```
k8s/distributed/
  base/                 # 4 役割 + Sandbox Service。namespace・image・設定なし
  platform/             # ユースケースごとの namespace 2 つと Role sandbox-management の component
  overlays/dev/         # 開発環境（既存の namespace・接頭辞・image・設定）
  overlays/dev-bootstrap/  # replicas 0（今の distributed-bootstrap）
usecases/<u>/k8s/distributed/
  kustomization.yaml    # base + platform component + 設定 + image
  bootstrap/kustomization.yaml
  config/silk-agent-distributed.yaml, sandbox-service-config.yaml, mcp.json
```

- 設定の中の namespace に依存する値（`sandbox_service_url`、`sandbox_service_allowed_ksas`、Sandbox の `namespace` / `control_namespace`）は overlay の設定ファイルに書き、D4 の起動時の検査で食い違いを止める。kustomize の replacements で自動生成する案は、YAML の文字列の一部の置換が読みにくくなるので採らない。
- Sandbox Service の NetworkPolicy の egress 先の namespace は、platform component の中で Sandbox 用 namespace を指すよう patch する。
- `k8s/platform/base.yaml` の固定の namespace と Role は、開発環境の overlay に移す。StorageClass などクラスタで 1 つのものは `k8s/platform/` に残す。
- 今の `k8s/distributed/` のパスを使っている手順（README、runbook、`tests/test_distributed_manifest.py`）は新しいパスに更新する。

### D3. 開発環境は同じ値で移す

`overlays/dev` は、今の `k8s/distributed/` と同じ namespace、名前、接頭辞、image を出力しなければならない。移行の前後で `kustomize build` の出力を比較し、差分がリソースの並び以外にないことを確認してから切り替える。ClusterRoleBinding の名前だけは変わる可能性があるので、dev では今の名前を維持する patch を入れる。

### D4. 起動時の照合

各役割は `POD_NAMESPACE`（Downward API で渡す）を読み、次を照合する。

- Worker / Control / Gateway: `sandbox_service_url` のホスト名が `<service>.<POD_NAMESPACE>.svc` の形であること。
- Sandbox Service: Sandbox の `control_namespace` が `POD_NAMESPACE` と一致すること。`sandbox_service_allowed_ksas` の各要素の namespace が `POD_NAMESPACE` であること。
- 照合を無効にするのは `distributed.namespace_check: false` だけで、既定は有効。`POD_NAMESPACE` がない（Kubernetes の外）のに有効なら起動しない（明示的に無効にさせる）。

分散構成では `firestore_prefix` の既定値（今は `silk-agent`）をなくし、書かれていなければ起動しない（usecase-packaging の「環境固有の値に既定値を持たない」と同じ扱い）。書き忘れたユースケースが開発環境の接頭辞に混ざるのを防ぐため。開発環境とテンプレートの設定はすでに明示しているので、配置の変更はいらない。単一プロセスの構成は Firestore を使わないので影響しない。

Firestore の接頭辞と namespace の対応はコードでは照合しない（開発環境が `silk-agent` と `session-sandbox-system` で一致しないため）。代わりに Terraform のユースケースの一覧が正本になり、Worker の IAM が他の接頭辞を読めないわけではない点はリスクに書く。

### D5. Terraform のユースケースの一覧

`terraform/variables.tf` に `usecases`（map: キーがユースケース名、値が `system_namespace`、`sandbox_namespace`、`firestore_prefix`、`slack_secret_ids`）を追加する。既定には開発環境の 1 件（`session-sandbox-system`、`session-sandboxes`、`silk-agent`、今の Secret の名前）を入れ、今の資源のアドレスを `moved` ブロックで移して plan に再作成が出ないようにする。

- `for_each` で、役割ごとの Workload Identity の権限（主体は `ns/<system_namespace>/sa/silk-agent-<role>`）、Slack の Secret と Gateway / Control への読み取り権限、`<firestore_prefix>_commands` と `<firestore_prefix>_outbox` の索引、`add-data-retention` の TTL（接頭辞つきのコレクション）を作る。
- `add-data-retention` が先に入っていれば、その `firestore_prefix` 変数をこの一覧に置き換える。後に入る場合は、その change の D8 をこの一覧で書く。
- ユースケース固有のクラウド資源（bigquery-analysis の GSA など）は今のとおり `usecases/<u>/terraform/` に置き、`ksa_namespace` の既定を `<u>-sandboxes` にする。

### D6. 配置スクリプト

`k8s/deploy.sh` に分散構成のモード（`DISTRIBUTED_DIR`）を追加する。kube context の確認、`.env` からの Slack の Secret の作成（対象の namespace に作る）、`silk-agent-sandbox` の Secret の作成、`kubectl apply -k`、4 役割の rollout の待機を行う。初回は bootstrap の overlay（replicas 0）を適用し、runbook の手順で役割を起動する。`k8s/grant-iam.sh` は Terraform の出力を使うように直すか、Terraform に置き換えて削除する。

## Risks / Trade-offs

- [同じ Firestore の database なので、Worker の IAM（datastore.user）は他の接頭辞のコレクションも読み書きできる] → Firestore の IAM はコレクション単位で絞れない。D4 の照合と設定の分離で取り違えを防ぎ、ドキュメントで「強い分離が必要なら database かプロジェクトを分ける」と明記する。
- [開発環境の移行で名前が変わり、既存の Sandbox や会話が見えなくなる] → D3 で `kustomize build` の出力を比較してから適用する。
- [Terraform の `moved` を誤り、Firestore の索引や IAM が作り直される] → plan に destroy が出ないことを確認するまで apply しない。索引の作り直しは数分から数十分かかるので、出た場合は止める。
- [ユースケースが増えると Gateway / Control / Sandbox Service が重複して資源を使う] → 初期の対象は数件のユースケースとし、Worker 以外は replicas 1 を既定にする。

## Migration Plan

1. base / overlay の構成を作り、`overlays/dev` の出力が今の `k8s/distributed/` と同じであることを確認する。
2. Terraform の `usecases` を開発環境の 1 件で入れ、`moved` で plan に差分が出ないことを確認して apply する。
3. 開発環境を `overlays/dev` で適用する（出力が同じなので Pod は再作成されない）。
4. bigquery-analysis を一覧に追加して Terraform を apply し、`bigquery-analysis-system` / `bigquery-analysis-sandboxes` に配置する。Slack アプリは bigquery-analysis 用のものを使う。
5. 切り戻し: 4 は namespace ごと削除できる。1〜3 は前の `k8s/distributed/` に戻して適用する（出力が同じなので影響はない）。
