# Design

## Context

- incarnation は `[pod_uid, uuid]` の JSON で、プロセス（コンテナ）ごとに新しくなる。`FirestoreWorkerDirectory.register` は、incarnation が違うと Worker ドキュメントを丸ごと置き換える（カウンタも0になる）。予約は `workers/{id}/incarnations/{incarnation}/reservations/{key}` に incarnation ごとに置かれる。
- 回復は `RecoveryLoop`（`control_runtime.py`）が、失効した所有権について quiesce を要求し、応答がなければ `_released_for_missing_pod` で Kubernetes に Pod の不在を確認する。Pod が存在すると、解放されない。
- `confirm_stopped` と `release*` は、Worker ドキュメントが存在しないと False を返す。`confirm_stopped` は、Worker ドキュメントの incarnation を照合せずにカウンタを減らす。

## Goals / Non-Goals

**Goals:**
- コンテナの再起動と Worker 記録の消失で、回復が止まらないようにする。
- 手動の解除が、他の incarnation の予約数を壊さないようにする。

**Non-Goals:**
- Pod の存在判定そのもの（`KubernetesPodProbe`）の変更。
- 別の Pod の置き換え（UID の変更）の扱い。これは既存の判定で解放される。

## Decisions

1. **停止の証拠に「同じ Pod UID の新しい incarnation の登録」を加える。** コンテナの再起動では、kubelet が旧コンテナのプロセスを終了してから新しいコンテナを起動するので、同じ Pod の新しい incarnation が登録されたことは、旧 incarnation の停止の証拠になる。証拠の ID は `auto:incarnation-replaced:<worker_id>`。判定は `RecoveryLoop` に `incarnation_replaced(token)` として注入し、Worker の記録を `FirestoreWorkerDirectory` から読む。代案として、Pod probe で `restartCount` を見る案は、restartCount の増加が incarnation の特定にならず、API の権限も増えるので採らない。
2. **カウンタの更新は、Worker ドキュメントの incarnation が所有権のものと一致するときだけにする。** `release` は既に一致を検査している。`confirm_stopped`（`release_after_stop`）に同じ検査を加える。不一致のときは予約（reservation）の削除と所有権の解放だけを行う。
3. **Worker ドキュメントがないときは、カウンタを省いて解放する。** 3つの関数（`release`、`release_unactivated`、`confirm_stopped`）で、`worker_snapshot.exists` を解放の条件から外す。予約が残っていれば削除する。Worker の記録がない状態で他の会話の予約数が壊れることはない（記録がないので、減らす対象がない）。代案の「所有権が残っている Worker には `expire_at` を付けない」は、`workers` の更新経路が増えるので採らない。
4. **読み取りの監査を更新する。** `docs/firestore-index-plan.md` の読み取り一覧に、`incarnation_replaced` が行う Worker ドキュメントの読み取りを加える。

## Risks / Trade-offs

- [コンテナの再起動を、旧プロセスがまだ動いているのに停止と誤認する] → 同じ Pod UID の新しい incarnation が登録されるのは、新しいコンテナの起動後で、同じコンテナ内に2つのプロセスは存在しない。lease の失効（`auto_after` 相当の待ち）を、Pod 不在の判定と同じに適用する。
- [Worker の記録が消えた所有権を解放しても、旧 SDK が残っている] → Worker の記録の保持期間（7日）は lease より十分に長い。この場合は、既存の Pod の不在の確認と併用し、記録がないことだけを根拠にはしない（手動の解除は運営者の判断）。
