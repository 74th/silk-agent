## ADDED Requirements

### Requirement: コマンドの監視は Sandbox を起動しない
システムは、通知が済んでいないコマンドの状態と出力を監視で読むとき、停止中の Sandbox を再開してはならず (SHALL NOT)、失われた Sandbox の代わりに新しい Sandbox を作ってはならない (SHALL NOT)。Sandbox が失われたことが確定したコマンドは、結果不明の終端として記録し、監視の再試行を止めなければならない (SHALL)。

#### Scenario: 停止中の Sandbox
- **WHEN** idle 停止している Sandbox に、通知が済んでいないコマンドの記録がある
- **THEN** 監視は Sandbox を再開せず、idle 停止はそのまま維持される

#### Scenario: 失われた Sandbox
- **WHEN** コマンドを実行していた Sandbox が失われている（世代が変わった、または終了している）
- **THEN** そのコマンドは結果不明の終端として記録され、新しい Sandbox は作られず、監視は再試行しない
