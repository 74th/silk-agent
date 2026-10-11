# Spec Delta

## MODIFIED Requirements

### Requirement: ルールベースのフィルタ
システムは、Geminiによる判定より前に、次のメッセージを処理対象から除外しなければならない (SHALL): Bot自身やほかのBotの投稿、編集・参加通知などのシステムイベント、許可リストにないworkspace・チャンネル・ユーザからの投稿、DMを受け付けない設定のときのDM、除外する設定のときのゲストと外部ユーザの投稿、空のメッセージ、設定した長さの上限を超えるメッセージ。除外したメッセージはエージェントに渡してはならない (SHALL NOT)。

許可リストは次のとおりでなければならない (SHALL)。

- workspace・チャンネル・ユーザの許可リスト（`filter.allowed_teams`、`filter.allowed_channels`、`filter.allowed_users`）は、空なら制限せず、設定があればそこに含まれるIDの投稿だけを処理する。
- チャンネルの許可リストはDMには適用しない。DMを処理するかどうかは`filter.allow_direct_messages`（既定`true`）で決める。
- ユーザの許可リストは、会話を始めるメンションだけでなく、スレッドへの返信、DM、停止語にも適用する。
- `filter.deny_guests`が`true`なら、ゲスト（マルチチャンネルゲスト、シングルチャンネルゲスト）の投稿を除外する。
- `filter.deny_external_users`が`true`なら、Botをインストールしたworkspace以外に所属するユーザ（Slackコネクトの共有チャンネルなど）の投稿を除外する。
- ゲストか外部ユーザかを判定できなかった場合、その設定が有効なら除外する。

許可リスト、DM、ゲスト、外部ユーザの設定で除外したときは、返信しない。

#### Scenario: Botの投稿
- **WHEN** 別のBotがスレッドに投稿する
- **THEN** そのメッセージは処理されず、返信もされない

#### Scenario: 許可されていないチャンネル
- **WHEN** 許可リストにないチャンネルでBotがメンションされる
- **THEN** そのメッセージはエージェントに渡されない

#### Scenario: 長すぎるメッセージ
- **WHEN** 上限（既定8,000文字）を超えるメッセージが届く
- **THEN** エージェントに渡されず、長すぎることが返信される

#### Scenario: 許可されていないユーザの返信
- **WHEN** `filter.allowed_users`に`U1`だけを設定し、`U1`が始めたスレッドに`U2`が返信する
- **THEN** `U2`のメッセージはエージェントに渡されず、返信もされない

#### Scenario: 許可されていないユーザの停止語
- **WHEN** `filter.allowed_users`に`U1`だけを設定し、`U1`のturnの実行中に`U2`が「停止」と書き込む
- **THEN** turnは止まらない

#### Scenario: チャンネルを制限してもDMは受け付ける
- **WHEN** `filter.allowed_channels`に`C1`だけを設定し、`filter.allow_direct_messages`は既定のまま、BotにDMを送る
- **THEN** そのDMはエージェントに渡される

#### Scenario: DMを受け付けない
- **WHEN** `filter.allow_direct_messages: false`でBotにDMを送る
- **THEN** そのDMはエージェントに渡されず、返信もされない

#### Scenario: ゲストの除外
- **WHEN** `filter.deny_guests: true`で、シングルチャンネルゲストがBotをメンションする
- **THEN** そのメッセージはエージェントに渡されず、返信もされない

#### Scenario: 外部ユーザの除外
- **WHEN** `filter.deny_external_users: true`で、Slackコネクトの共有チャンネルにいる別のworkspaceのユーザがBotをメンションする
- **THEN** そのメッセージはエージェントに渡されず、返信もされない

#### Scenario: 判定できないユーザ
- **WHEN** `filter.deny_guests: true`で、ユーザ情報の取得に失敗したユーザがBotをメンションする
- **THEN** そのメッセージはエージェントに渡されない

#### Scenario: 設定がなければゲストも受け付ける
- **WHEN** `filter.deny_guests`と`filter.deny_external_users`を設定せず、ゲストがBotをメンションする
- **THEN** そのメッセージは入力ガードの判定へ進む
