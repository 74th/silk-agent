# Design

## Context

- `slack_app.load_tokens` / `load_bot_token` は、環境変数 `SLACK_*` があればそれを使い、なければ設定の `slack.bot_token_secret` / `app_token_secret`（`projects/<p>/secrets/<id>`、`/versions/latest` を補う）を Secret Manager から読む。Gateway（`gateway_runtime`）と Control（`control_runtime`）は、すでにこの関数を使っている。
- Terraform は、ユースケースごとの Secret（`slack_secret_ids`）と IAM（Gateway: 両方、Control: bot token）を作っている。`bigquery-analysis` の設定とテンプレートの例には、`bot_token_secret` / `app_token_secret` が書かれている。dev の設定（`k8s/distributed/overlays/dev/config/silk-agent-distributed.yaml`）にはない。
- `k8s/distributed/base/roles.yaml` の Gateway と Control が、`silk-agent-slack` Secret の `SLACK_*` を環境変数で渡す。`deploy.sh` の分散の経路は、`SILK_AGENT_ENV_FILE`（既定は `.env`）から Secret を作る。

## Goals / Non-Goals

**Goals:**
- 環境変数をやめ、Secret Manager を唯一の経路にする。
- 指定の漏れで、別のトークンへ切り替わらないようにする。

**Non-Goals:**
- 単一 Pod 構成の変更（`.env` と k8s Secret のまま）。
- Secret Manager のバージョンの自動ローテーション。

## Decisions

1. **マニフェストから `SLACK_*` の環境変数を外し、設定で Secret Manager を指す。** 環境変数が優先される実装は、そのままにする（単一 Pod とローカル実行が使う）。分散のマニフェストに環境変数がなければ、Secret Manager が使われる。
2. **`deploy.sh` の分散の経路から、`.env` の確認と `silk-agent-slack` の作成を削除する。** 代わりに、Secret Manager の2つの Secret に有効なバージョンがあるかを `gcloud secrets versions list` で確認する経路は、`gcloud` と設定のプロジェクトが前提になるので入れず、起動時に、読めなければ Gateway / Control が `RuntimeError` で起動しない（既存の挙動）ことに任せる。運用者には、手順（文書）で、先に登録することを示す。
3. **移行**: dev の `silk-agent-slack-*` に、現在の k8s Secret の値を、値を表示せずにコピーする。デプロイ後、Gateway が Socket Mode に接続していること（ログ）を確認し、k8s の `silk-agent-slack` を削除する。`bigquery-analysis` は、Secret Manager にすでに値がある（1バージョン）ので、値の同一性は確認せず、そのまま切り替える。

## Risks / Trade-offs

- [Secret Manager に値がない、または IAM が足りないと、Gateway / Control が起動しない] → 起動前に、dev で手順どおり登録して確認する。起動しない場合は、すぐに分かる（`RuntimeError`）。
- [ローテーションに再起動が要る] → トークンは起動時に読む。文書に書く。
- [dev の Bot が、切り替えの間（数十秒）止まる] → dev なので許容する。

## Migration Plan

1. Secret Manager に値を登録（dev の `silk-agent-slack-*`）。
2. 設定とマニフェストを更新して適用。
3. Gateway と Control のログで、Secret Manager からの読み取りと Slack への接続を確認する。
4. k8s の `silk-agent-slack` を削除する。
5. ロールバック: マニフェストを戻して適用し、`silk-agent-slack` を `.env` から作り直す。
