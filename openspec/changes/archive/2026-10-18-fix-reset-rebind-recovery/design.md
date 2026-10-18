# Design

## Context

- `DistributedResetCoordinator.reset`（`reset.py`）は、`begin_reset`（`reset_pending` を立てる）→ 停止 → `complete_reset`（世代 +1、`reset_pending` を消す）→ `rebind_after_reset`（`reset_seq` より後の、旧世代の未完了の入力を、入力ごとのトランザクションで新しい世代に書き換える）の順に進む。
- `complete_reset` は `reset_pending` が真のときだけ成功する。世代の更新後に再実行しても `None` を返し、`rebind_after_reset` は呼ばれない。`reset_seq` は `complete_reset` で消える。
- 旧世代の入力は、`list_unfinished` と制御の読み取りに現れ続け、そのたびに prepare が走る。

## Goals / Non-Goals

**Goals:**
- 再バインドを、再試行できる（冪等で、必要な情報が残る）処理にする。

**Non-Goals:**
- 世代の更新と再バインドを一つのトランザクションにすること。入力の件数に上限がなく、Firestore のトランザクションの上限を超えうる。

## Decisions

1. **`complete_reset` が、会話の記録に `rebind_pending: {previous_generation, reset_seq}` を書く。** 世代の更新と同じトランザクションなので、世代が更新されたのに記録がない状態は存在しない。
2. **`rebind_after_reset` は、最後まで走った（ページが空になった）ときに、トランザクションで `rebind_pending` を消す。** 入力ごとの書き換えは、既存の条件（旧世代かつ `seq > reset_seq`）で冪等。
3. **actor の起動時に、`rebind_pending` があれば `rebind_after_reset` を呼ぶ。** 起動は `actor.start()`（所有権の取得後）で、所有権の世代（epoch）の照合は既存の `ownership_matches` に任せる。新しい Worker でも、同じ Worker の再起動でも、同じ経路で完了する。
4. **旧世代の入力の無限な prepare は、再バインドの完了で解消する。** 追加の入力の読み取りの変更はしない（完了していない間は、入力の読み取りは従来どおり）。

## Risks / Trade-offs

- [`rebind_pending` を持つ会話の記録が増える] → 会話の記録の1フィールドで、完了で消える。
- [再バインドが永久に失敗する（入力が壊れている）] → 起動のたびに再試行し、失敗は構造化ログに残る。会話は、再バインドが終わるまで新しい入力を処理しない（旧世代の入力が残る状態よりは、誤りが明確）。
