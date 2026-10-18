# Proposal

## Why

分散構成のリセットは、世代の更新（`complete_reset`）と、リセット後に届いた入力の再バインド（`rebind_after_reset`）が別のトランザクションなので、間で失敗すると会話が詰まる（`xprivate/product-review-20261018.md` P2-8）。世代が更新された後は、`reset_pending` が消えるため、再試行しても再バインドが実行されない。旧世代のままの入力が残り、その入力の prepare（入力ガード、レート制限）が、会話の入力読み取りのたびに無限に繰り返される。

## What Changes

- `complete_reset` が、世代の更新と同じトランザクションで、再バインドが必要であること（直前の世代と `reset_seq`）を会話の記録に残す。
- 再バインドは、完了したら（残りがなくなったら）この記録を消す。再バインドの途中で失敗しても、記録が残る。
- actor の起動時（所有権の取得後と、Worker の再割り当て後）に、再バインドが残っていれば先に完了させる。
- 回帰テストを追加する（再バインドの途中で失敗した後、actor の起動で再バインドが完了すること）。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `distributed-execution`: リセットが途中で失敗しても、リセット後の入力が新しい世代に再バインドされ、会話が詰まらないことを要件に加える。

## Impact

- `src/silk_agent/stores/firestore/conversations.py`（`complete_reset`）、`src/silk_agent/stores/firestore/inbox.py`（`rebind_after_reset`）、`src/silk_agent/distributed/reset.py`、`src/silk_agent/distributed/actor.py`
- `tests/` の unit と `tests/integration/`（emulator）
