# Design

## Context

- 現在のクォータ（`serviceusage` の `consumerQuotaMetrics`）: 指標 `bigquery.googleapis.com/quota/query/usage` の、`1/d/{project}` が 209,715,200 MiB（200 TiB）、`1/d/{project}/{user}` が無制限。
- SQL は Sandbox のエージェントが `google-cloud-bigquery` で実行する。クライアントの `maximum_bytes_billed` は、エージェント自身が設定するコードなので、強制にならない。強制できるのは、サーバ側（プロジェクトのクォータ）だけ。

## Goals / Non-Goals

**Goals:**
- 1日のクエリ量に、サーバ側で上限を付ける。

**Non-Goals:**
- クエリごとの上限（エージェントのコードの側）。Skill に `maximum_bytes_billed` を書くのは助言にすぎず、強制しない。
- 依頼できる人の制限（`allowed_users`）の値の決定。Slack のユーザ ID は運用者の判断。

## Decisions

1. **`google_service_usage_consumer_quota_override` で、2つのリミットを上書きする。** `%2Fd%2Fproject` と `%2Fd%2Fproject%2Fuser`。値は MiB で、`daily_query_limit_gib * 1024`。既定 100 GiB。`force = true`（現在の使用量を下回る上書きも許す）。ユースケースの Terraform の root に置く（BigQuery 関連のリソースを管理している）。
2. **クォータはプロジェクト単位なので、同じプロジェクトの他の BigQuery の利用にも効く。** 検証用のプロジェクトでは許容し、OPERATIONS.md に明記する。本番で他の用途と同居させるなら、プロジェクトを分ける。
3. **超過の失敗は、既存の仕組みに乗せる。** BigQuery クライアントの例外（`Forbidden`、reason `quotaExceeded`）は、既存の Skill の実行失敗の分類で、結果と区別して報告される。追加のコードは入れず、テストで確認する。

## Risks / Trade-offs

- [上限が小さすぎて、正当な分析が止まる] → 変数で変えられる。超過は、結果と区別された失敗として、利用者に分かる。
- [クォータの上書きが反映されるまで数分かかる] → 適用後に、`consumerQuotaMetrics` で値を確認する。
