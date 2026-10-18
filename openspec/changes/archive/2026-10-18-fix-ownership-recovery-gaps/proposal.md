# Proposal

## Why

分散構成では、Worker が止まった会話の所有権の回復に、自動では抜けられない経路が2つある（`xprivate/product-review-20261018.md` P1-6、P1-7）。どちらも会話が `recovery_blocked` のまま利用者に返答しなくなり、手動の解除（`recovery confirm-stopped`）も効かないか、効くと別の Worker の予約数を狂わせる。

- **P1-6**: OOM などでコンテナだけが再起動すると、Pod 名と UID は同じで incarnation だけが変わる。旧 incarnation 宛ての quiesce には誰も答えず、Pod probe は「Pod が存在する」と判定するので、自動では解除されない。手動で解除すると、旧 incarnation の予約を消すときに、新しい incarnation の `reserved` / `active_sdk` を減らし、容量を超えて割り当ててしまう。
- **P1-7**: Worker ドキュメント（`workers/{id}`）は7日の TTL で消える。その後は、その Worker の所有権を `release` / `confirm_stopped` が False で返すだけで、自動回復も手動解除も通らない。

## What Changes

- 同じ Worker ID で、より新しい incarnation が登録済みなら、旧 incarnation のコンテナは存在しないので、停止の証拠として自動で解除する（同じ Pod UID に限る）。
- Worker のカウンタは、記録されている incarnation が、解放する所有権の incarnation と一致するときだけ減らす。手動解除でも同じ。
- Worker ドキュメントがすでに存在しないときは、カウンタの更新を省いて所有権を解放する（`release`、`release_unactivated`、`confirm_stopped`）。
- runbook §5 の「Worker が戻れば自動で解除する」を、この場合に合わせて書き直す。
- 回帰テストを追加する（Firestore emulator を含む）。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `distributed-execution`: 所有権の回復の停止の証拠に「同じ Pod での新しい incarnation の登録」を加え、予約数の更新を incarnation で守る要件、Worker ドキュメントがないときの解放の要件を加える。

## Impact

- `src/silk_agent/stores/firestore/ownership.py`、`src/silk_agent/stores/firestore/workers.py`、`src/silk_agent/distributed/control_runtime.py`、`src/silk_agent/distributed/pod_probe.py`
- `docs/distributed-operations-runbook.md`、`docs/firestore-index-plan.md`（読み取りの監査）
- `tests/` の unit と `tests/integration/`（emulator）
