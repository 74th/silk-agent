# Tasks

## 1. 設定とマニフェスト

- [x] 1.1 `k8s/distributed/base/roles.yaml` の Gateway と Control から `SLACK_*` の環境変数（`silk-agent-slack` の参照）を外し、dev の分散の設定に `slack.bot_token_secret` / `app_token_secret` を追加する。`tests/test_distributed_manifest.py` を、Slack のトークンの環境変数と Secret の参照がどの役割にもなく、設定に Secret Manager のリソース名があることの検査に直し、通ることを確認する
- [x] 1.2 `k8s/deploy.sh` の分散の経路から `.env` の確認と `silk-agent-slack` の作成を外す（単一 Pod の経路は変えない）。`bash -n` と `shellcheck`（あれば）が通り、分散の経路に `.env` への参照がないことを確認する

## 2. dev への適用

- [x] 2.1 dev の `silk-agent-slack-bot-token` / `app-token` に、現在の k8s Secret の値を、値を表示せずに登録する。`gcloud secrets versions list` で、バージョンができたことを確認する
- [x] 2.2 dev（`silk-agent`）と `bigquery-analysis` に適用し、Gateway と Control が起動して Slack に接続していること（ログ）を確認する。k8s の `silk-agent-slack` を削除する
- [x] 2.3 Pod の中から、Worker の Workload Identity では Slack の Secret を読めないこと、ユースケース間で読めないことを、`access_secret_version` の結果で確認する

## 3. 文書

- [x] 3.1 `docs/usecase-guide.md`（§4.2）と `docs/distributed-operations-runbook.md`（§1 の手順 3）を、Secret Manager への登録とローテーション（新しいバージョンの登録と再起動）に直す。記述が実装と一致することを読み合わせて確認する

## 4. 仕上げ

- [x] 4.1 `uv run pytest`、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
