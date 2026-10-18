# Spec Delta

## ADDED Requirements

### Requirement: 配置と設定の食い違いを起動時に検出する
各役割（Gateway、Worker、Control、Sandbox Service）は、起動時に、自分が動いている namespace と設定の値を照合しなければならない (SHALL)。Sandbox 管理の制御側の namespace、Sandbox Service の URL が指す namespace、Sandbox Service が受け入れる ServiceAccount の namespace が、自分の namespace と一致しない場合、その役割は起動してはならない (SHALL NOT)。起動しない場合は、食い違っている設定項目と、期待した値・実際の値をエラーとして出さなければならない (SHALL)。Kubernetes の外（ローカル開発など）で動かす場合は、照合を明示的に無効にできなければならない (SHALL)。

#### Scenario: 別のユースケースの設定で起動する
- **WHEN** ユースケース A の namespace で、ユースケース B の Sandbox Service の URL を書いた設定の Worker を起動する
- **THEN** Worker は起動せず、`sandbox_service_url` の namespace が自分の namespace と一致しないことがエラーとして出る

#### Scenario: 正しい設定で起動する
- **WHEN** ユースケース A の namespace で、A の値だけを書いた設定の役割を起動する
- **THEN** 照合は成功し、役割は通常どおり起動する

#### Scenario: ローカルで起動する
- **WHEN** 運用者が照合を無効にする指定をして、Kubernetes の外で役割を起動する
- **THEN** 照合は行われず、無効にしたことが起動時のログに出る
