# Spec Delta

## ADDED Requirements

### Requirement: クラスタ DNS の構成を宣言して管理する
システムは、クラスタの NodeLocal DNSCache の有効・無効を Terraform の入力で宣言し、適用できなければならない (SHALL)。Terraform の構成と実際のクラスタの DNS 構成に差分を残してはならない (SHALL NOT)。Sandbox の DNS 許可の設定は、宣言したクラスタ DNS の構成で名前解決できる内容でなければならない (SHALL)。

#### Scenario: 現在のクラスタを計画する
- **WHEN** 運用者が NodeLocal DNSCache を有効にした入力で、既存のクラスタに対して Terraform の plan を作成する
- **THEN** DNS キャッシュの設定に差分は出ない

#### Scenario: 設定を切り替える
- **WHEN** 運用者が NodeLocal DNSCache の入力を変更して plan を作成する
- **THEN** plan にはクラスタの DNS キャッシュ設定の変更が示され、適用後も既定の通信設定の Sandbox は名前解決できる
