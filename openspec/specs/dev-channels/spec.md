# dev-channels Specification

## Purpose

Slackに接続せずにエージェントを動かせるよう、Streamlitのチャット画面と、ユニットテスト用のチャネル・外部サービスの代替実装を提供する。

## Requirements

### Requirement: Streamlitのチャット画面
システムは、Slackと同じ会話処理（フィルタ、ガード、エージェント、出力検査）を使うStreamlitのチャット画面を提供しなければならない (SHALL)。画面では、エージェントの応答、実行したツール、turnの状態を表示し、停止語で実行中のturnを止められなければならない (SHALL)。

#### Scenario: 画面からの指示
- **WHEN** Streamlitの画面で依頼を入力する
- **THEN** エージェントの応答と実行したツールが画面に表示される

#### Scenario: 画面からの停止
- **WHEN** turnの実行中に画面で「停止」と入力する
- **THEN** turnが中断され、中断したことが画面に表示される

### Requirement: 外部サービスなしのユニットテスト
システムは、Slack、Agent Platform（Claude、Gemini、Sessions）、Model Armor、GCS、GKEに接続せずに会話処理を実行できる、テスト用のチャネルと代替実装を提供しなければならない (SHALL)。テスト用のエージェントは、決められたシナリオどおりにテキストとツール呼出しを返さなければならない (SHALL)。時刻はテストから進められなければならない (SHALL)。

#### Scenario: シナリオでの実行
- **WHEN** 「ツール`bash`を呼んでからテキストを返す」シナリオのテスト用エージェントで、テスト用チャネルからメッセージを送る
- **THEN** ネットワークに接続せずに、ツール開始・ツール終了・テキスト・turn完了の出力イベントが得られる

#### Scenario: 時刻を進めるテスト
- **WHEN** テストで、実行中のturnがある状態で時刻を3分進める
- **THEN** heartbeatの出力イベントが3回得られる

### Requirement: 実モデルでのテストはHaiku 5.5を使う
システムは、実際のモデルを呼ぶintegrationテストで、Agent Platform上の`claude-haiku-5-5`を使わなければならない (SHALL)。integrationテストは、明示的に指定したときだけ実行され、通常のユニットテストの実行では外部に接続してはならない (SHALL NOT)。

#### Scenario: 通常のテスト実行
- **WHEN** 指定なしでユニットテストを実行する
- **THEN** 外部サービスへの接続は発生しない

#### Scenario: integrationテスト
- **WHEN** integrationテストを明示的に実行する
- **THEN** エージェントはAgent Platform上の`claude-haiku-5-5`で動く
