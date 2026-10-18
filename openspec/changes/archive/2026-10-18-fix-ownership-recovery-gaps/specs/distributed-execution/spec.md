## ADDED Requirements

### Requirement: コンテナの再起動後の所有権を自動で回復する
システムは、所有権を持つ Worker の incarnation が、同じ Worker ID（同じ Pod UID）のより新しい incarnation に置き換わったことを確認できたとき、旧 incarnation のプロセスは存在しないものとして、その所有権を自動で解放しなければならない (SHALL)。Worker の予約数と SDK の稼働数は、記録された incarnation が解放する所有権の incarnation と一致するときだけ減らさなければならない (SHALL)。手動の解除でも同じである。

#### Scenario: コンテナだけが再起動した
- **WHEN** Worker の Pod は同じままコンテナが再起動し、新しい incarnation が登録され、旧 incarnation の所有権の lease が失効している
- **THEN** 旧 incarnation の停止確認の応答を待たずに、その所有権は解放され、会話は次の入力で別の（または同じ）Worker に割り当てられる

#### Scenario: 手動で解除しても新しい incarnation の予約数は変わらない
- **WHEN** 旧 incarnation の所有権を手動で解除する
- **THEN** 新しい incarnation の予約数と SDK の稼働数は変わらず、容量を超えた割り当ては起きない

### Requirement: Worker の記録がなくても所有権を解放できる
システムは、所有権の Worker の記録が保持期間（TTL）で消えていても、その所有権を自動回復と手動の解除で解放できなければならない (SHALL)。記録がないときは、予約数の更新は行わない。

#### Scenario: Worker の記録が TTL で消えた
- **WHEN** 停止した Worker の記録が削除された後に、その Worker の所有権を解放する
- **THEN** 所有権は解放され、会話は回復待ちから抜ける
