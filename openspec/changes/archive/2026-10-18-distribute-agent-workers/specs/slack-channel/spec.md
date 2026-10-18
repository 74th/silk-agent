# Spec Delta

## MODIFIED Requirements

### Requirement: Slackイベントの重複を処理しない
システムは、Slackから同じイベントが再送された場合、2回目以降を処理してはならない (SHALL NOT)。

重複排除は受信プロセスを越えて永続化しなければならない (SHALL)。同じ投稿が app_mention と message で届く場合も一入力として扱わなければならない (SHALL)。

#### Scenario: 再送
- **WHEN** 同じイベントIDのメッセージが2回届く
- **THEN** turnは1回だけ実行される

#### Scenario: 複数 Gateway とイベント種別
- **WHEN** 同じ投稿が別Gatewayへmentionとmessageとして届く
- **THEN** 永続入力は一件だけ作られ、二重にSDKへ渡さない

## ADDED Requirements

### Requirement: 永続化後の受信確認
システムは、処理対象イベントを永続化してから Slack に受信確認を返さなければならない (SHALL)。保存に失敗したイベントを受付成功として確認してはならない (SHALL NOT)。重複済みと確認できたイベントは再処理せず受信確認しなければならない (SHALL)。

#### Scenario: ACK直後の再起動
- **WHEN** 受信確認を返した直後にGatewayが停止する
- **THEN** 永続入力から別プロセスが処理を継続できる

#### Scenario: 保存先の障害
- **WHEN** 入力の永続化に失敗する
- **THEN** 受信成功のACKを返さず、受付障害を観測可能にする

### Requirement: 出力配送の永続化と不明状態
システムは、検査済み出力と送信状態を永続化し、会話内の出力順序と最新の進捗状態を維持しなければならない (SHALL)。新規投稿の成否が不明な場合、無条件に再投稿してはならない (SHALL NOT)。

#### Scenario: 送信後の記録失敗
- **WHEN** Slackが投稿を受理した可能性があり、送信済み記録が残っていない
- **THEN** 配送不明として照合対象にし、同じ返信を無条件に追加投稿しない

#### Scenario: 古いheartbeat
- **WHEN** turn完了の後に遅延したheartbeatが配送対象になる
- **THEN** 進捗表示を実行中へ戻さない

### Requirement: エージェント中断とコマンド継続の表示
システムは、Worker停止でSDKのturnが中断してもSandboxのコマンドが継続する場合、その違いをユーザへ伝えなければならない (SHALL)。

#### Scenario: Worker停止後の完了
- **WHEN** SDK中断後、継続中のコマンドが完了し、ユーザは再開していない
- **THEN** 検査済みの完了状況を通知し、Agentの作業続行にはユーザの再開が必要であることを示す
