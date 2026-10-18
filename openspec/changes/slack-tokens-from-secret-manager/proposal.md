# Proposal

## Why

分散構成の Slack トークンは、Terraform の Secret Manager と IAM（Gateway は両方、Control は bot token だけ）が用意されているのに、実際には `k8s/deploy.sh` が `.env` から作った k8s Secret（`silk-agent-slack`）の環境変数で動いている（`xprivate/product-review-20261018.md` P2-16）。

- 環境変数が Secret Manager に優先するので、Secret Manager のローテーション、ユースケース間の分離（ガイド §4.4 の根拠）が効かない。
- `deploy.sh` は `SILK_AGENT_ENV_FILE` を指定し忘れると、開発用の `.env` に黙って切り替わる。別のユースケースに開発用のトークンを入れ、Socket Mode で同じトークンを使うと、イベントが別のユースケースへ流れる。

## What Changes

- 分散構成の Gateway と Control は、設定の `slack.bot_token_secret` / `slack.app_token_secret`（Secret Manager のリソース名）から、Workload Identity でトークンを読む。マニフェストから `SLACK_*` の環境変数と `silk-agent-slack` の参照を外す。
- `k8s/deploy.sh` の分散の経路は、`.env` を読まず、k8s の Slack Secret を作らない（`silk-agent-sandbox` のフィンガープリント鍵の Secret は従来どおり）。
- dev の `silk-agent-slack-*` の Secret Manager に、現在のトークンを登録する。デプロイ後、k8s の `silk-agent-slack` を削除する。
- 文書（ユースケースガイド、runbook）を、Secret Manager への登録とローテーションの手順に直す。
- 単一 Pod 構成は、これまでどおり `.env` と k8s Secret を使う（変更しない）。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `distributed-execution`: 分散構成の Slack の認証情報は Secret Manager から、それを読む役割だけが読むことを要件に加える。

## Impact

- `k8s/distributed/base/roles.yaml`、`k8s/distributed/overlays/dev/config/silk-agent-distributed.yaml`、各ユースケースの分散用の設定、`k8s/deploy.sh`
- `tests/test_distributed_manifest.py`、`docs/usecase-guide.md`、`docs/distributed-operations-runbook.md`
- dev の GCP（Secret Manager のバージョン追加）と k8s（Secret の削除、ロールアウト）
