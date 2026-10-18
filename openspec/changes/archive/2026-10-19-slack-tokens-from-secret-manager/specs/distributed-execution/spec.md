## ADDED Requirements

### Requirement: 分散構成の Slack の認証情報を Secret Manager から読む
システムは、分散構成の Slack の認証情報（bot token、app token）を、環境変数や k8s の Secret ではなく、ユースケースごとの Secret Manager から、Workload Identity で読まなければならない (SHALL)。Gateway は両方、Control は bot token だけを読み、Worker と Sandbox Service には読む権限を与えてはならない (SHALL NOT)。デプロイの手順は、指定の漏れによって、別の環境のトークンへ黙って切り替わってはならない (SHALL NOT)。

#### Scenario: トークンのローテーション
- **WHEN** 運用者が Secret Manager に新しいバージョンを登録し、Gateway と Control を再起動する
- **THEN** 新しいトークンで Slack に接続する

#### Scenario: 別のユースケースの認証情報
- **WHEN** あるユースケースの Worker または Control が、別のユースケースの Slack の Secret を読もうとする
- **THEN** 権限エラーになる

#### Scenario: Worker に認証情報がない
- **WHEN** Worker の Pod の環境変数とマウントを調べる
- **THEN** Slack のトークンは含まれない
