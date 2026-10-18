# Design

## Context

- `ServiceCommandRuntime._handle` は `manager.get(key)` で handle を得る（作成も再開もしない）。しかし `handle.commands` は、リクエストごとに `manager.ensure(key)` で接続先を解決する `CommandTrackerClient` を返す。`ensure` は、`SUSPENDED` を再開し、`LOST` / `TERMINATED` なら次の世代を作る。
- 監視は30秒ごとに未通知のコマンドを読む。結果が `unknown` のときは、終端にせず次回に再試行する。

## Goals / Non-Goals

**Goals:**
- 監視が Sandbox のライフサイクルに触れないようにする。

**Non-Goals:**
- コマンドの開始・取消の経路（これらは `ensure` が必要）の変更。

## Decisions

1. **`SessionHandle` に、`ensure` を呼ばない読み取り専用の `observe_commands` を追加する。** `resolve_endpoint` は、`manager.get(key)` が返す記録が `READY` で、世代が handle と一致し、`sandbox_ref` があるときだけ `runtime_endpoint` を返す。それ以外は、状態に応じた例外（`SandboxNotReady`、`StaleGenerationError`）を投げる。既存の `commands`（`ensure` 付き）は変えない。
2. **`ServiceCommandRuntime` は `observe_commands` を使い、例外を分類する。** `SandboxNotReady`（停止中や再開中）は `unknown` のまま、従来どおり次回に再試行する（再開しない）。`StaleGenerationError`、または記録が `LOST` / `TERMINATED` は、終端の「失われた」として、`CommandResult(outcome="unknown")` に終端のマークを付けて返す。
3. **終端の確定は、既存の `observe_terminal` の経路を使う。** `outcome == "unknown"` かつ終端のマークなら、`CommandMonitor` は `UNKNOWN` を終端として記録し、通知を1回作る。再試行は止まる。

## Risks / Trade-offs

- [停止中の Sandbox のコマンドを、終了後も永久に確認できない] → 停止中は、`idle` 停止の条件（実行中のコマンドがない）を満たして停止したので、実行中のコマンドはない。`active_commands` が残ったまま停止することはない（gate の再確認）。残るのは、記録だけが古い場合で、これは P1-9 の修正（起動失敗の終端化）で減らしている。
- [`LOST` の判定が一時的な誤りである] → `manager.get` の記録は永続した状態で、`LOST` / `TERMINATED` は復帰しない。
