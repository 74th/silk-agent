# Tasks

## 1. クォータの上限

- [x] 1.1 `usecases/bigquery-analysis/terraform` に変数 `daily_query_limit_gib`（正の値の検証付き、既定 100）と、2つのクォータの上書きを追加する。`terraform validate` と `plan` が通る
- [x] 1.2 `terraform apply`（dev）の後、`serviceusage` の `consumerQuotaMetrics` で、`1/d/{project}` と `1/d/{project}/{user}` の実効値が、設定した値（MiB）になっていることを確認する

## 2. 超過時の挙動

- [x] 2.1 クエリが `quotaExceeded` で拒否されたとき、Skill のスクリプトが、結果と区別した実行失敗として報告することを、`usecases/bigquery-analysis/tests` のテストで確認する

## 3. 文書

- [x] 3.1 `usecases/bigquery-analysis/OPERATIONS.md` に、上限の値、変え方、超過時の挙動、プロジェクト単位のクォータであること、`allowed_users` の設定手順を書く。記述が実装と一致することを読み合わせて確認する

## 4. 仕上げ

- [x] 4.1 `uv run pytest`、`ruff check`、`ruff format --check`、`pyrefly check` が通ることを確認する
