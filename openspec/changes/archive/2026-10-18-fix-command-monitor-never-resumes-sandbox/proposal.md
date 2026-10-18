# Proposal

## Why

分散構成のコマンド監視（`ServiceCommandRuntime`）は、docstring で「Sandbox を起動しない」としているのに、コマンドの状態を読むたびに Sandbox を再開し、失われた Sandbox は次の世代を作ってしまう（`xprivate/product-review-20261018.md` P1-10）。`handle.commands` の接続先の解決が `SandboxManager.ensure()` を呼ぶためで、`SUSPENDED` の Sandbox は再開され、`LOST` / `TERMINATED` のときは新しい Sandbox が作られる。結果は `unknown` になり、記録されずに30秒ごとに再試行される。INTENT のまま残った記録や行方不明のコマンドが一つでもあると、idle 停止しても再開されるループになり、Sandbox が止まらない。

## What Changes

- 監視用の読み取り（状態、出力）は、Sandbox が `READY` のときだけ、現在の世代の実行時エンドポイントへ直接問い合わせる。`ensure` は呼ばない。
- Sandbox が停止中（`SUSPENDED`）なら、コマンドは終わっているはずがなく、状態を「実行中を確認できない」として記録を保持する（再開しない）。
- 世代が違う、または `LOST` / `TERMINATED` なら、そのコマンドを終端の `unknown`（Sandbox が失われた）として確定させ、再試行を止める。
- 回帰テストを追加する。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `distributed-execution`: コマンド監視が Sandbox を起動・再開・作成しないこと、失われた Sandbox のコマンドを終端として確定することを要件に加える。

## Impact

- `src/silk_agent/sandbox/service_monitor.py`、`src/gke_session_sandbox/manager.py`（読み取り専用のクライアント）、`src/gke_session_sandbox/commands.py`
- `tests/test_sandbox_service_monitor.py`、`tests/session_sandbox/test_manager.py`
