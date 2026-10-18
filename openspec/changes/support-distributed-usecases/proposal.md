# Proposal

## Why

ユースケースのパッケージ（`usecases/<name>/`）は、単一プロセスの Bot（`k8s/base`、`deploy.sh`）向けの配置手段しか持たない。分散構成（Gateway / Worker / Control / Sandbox Service）の `k8s/distributed/` は開発環境（`nnyn-dev`）の値と、固定の名前（Secret、ServiceAccount、ClusterRole、Service の URL、`allowed_ksas`、Terraform の IAM の主体、`silk-agent_` 接頭辞の索引）で書かれている。そのため、ユースケースを分散構成で配置できず、同じクラスタに 2 つ置くと会話・Worker・Sandbox が混ざる。`docs/usecase-guide.md` にも分散構成の手順がない（「分散」「terraform」の語が 0 回）。

## What Changes

- 分離の単位を決める: ユースケースごとに管理用の namespace（`<usecase>-system`）と Sandbox 用の namespace（`<usecase>-sandboxes`）を分け、Firestore は同じ database のまま `firestore_prefix` をユースケースごとに分ける。Worker・Gateway・Control・Sandbox Service はユースケース専用とし、Slack アプリもユースケースごとに用意する。
- `k8s/distributed/` を、環境の値を持たない共通の base と、環境・ユースケースごとの overlay に分ける。開発環境の現在の配置は、今の namespace と接頭辞のまま 1 つの overlay にする（状態の移行は不要）。
- ユースケースのテンプレートに分散構成用の overlay、設定（`silk-agent-distributed.yaml` と Sandbox Service の設定）、配置スクリプトを追加する。bigquery-analysis を新しい形に移す。
- Terraform で、ユースケースの一覧から namespace ごとの Workload Identity の権限、Slack の Secret、`firestore_prefix` ごとの索引と TTL を作る。
- 各役割の起動時に、自分の namespace と設定（Sandbox の namespace、管理 namespace、許可する KSA、Service の URL）が食い違っていないかを検査し、他のユースケースへの接続を防ぐ。
- `docs/usecase-guide.md` に分散構成の組み込み手順（Terraform、イメージ、設定、k8s、リリース、複数ユースケースの運用）を追記する。

## Capabilities

### New Capabilities

なし。

### Modified Capabilities

- `usecase-packaging`: 適用手順の文書とテンプレートに分散構成を含める。ユースケースを分散構成で配置できること、複数のユースケースを同じクラスタで分離して動かせること、ユースケースごとのクラウド資源を宣言できることを追加する。
- `distributed-execution`: 各役割が起動時に、自分の配置と設定の食い違い（他のユースケースの namespace・ストア・Service への接続）を検出して起動しないことを追加する。

## Impact

- `k8s/distributed/`、`k8s/distributed-bootstrap/`、`k8s/platform/`（namespace と Role を固定値で持っている）、`k8s/deploy.sh`、`k8s/grant-iam.sh`。
- `usecases/template/`、`usecases/bigquery-analysis/`（k8s、config、terraform、`OPERATIONS.md`）。bigquery-analysis は新しい namespace と接頭辞で動かすので、既存の会話は引き継がない。
- `terraform/main.tf`、`variables.tf`（ユースケースの一覧、role IAM、Slack の Secret、索引と TTL のループ）。
- `src/silk_agent/config.py`、各役割の起動処理（`gateway_runtime.py`、`worker_role.py`、`control_runtime.py`、`sandbox/service_runtime.py`）に起動時の検査を追加する。
- `docs/usecase-guide.md`、`k8s/distributed/README.md`、`docs/distributed-operations-runbook.md`、`tests/test_distributed_manifest.py`。
- `add-data-retention` が導入する Terraform の `firestore_prefix`（1 つ）を、この change でユースケースの一覧に広げる。
