# Proposal

## Why

単一 Pod 構成（GKE runtime）では、「停止」や `cancel_command` を実行しても、Sandbox で動くバックグラウンドのコマンドが止まらない（`xprivate/product-review-20261018.md` P1-1）。asyncio の task を cancel するだけで、リモートのプロセスには何も送らないのに「止めました」と表示するため、利用者は停止できたと誤解する。仕様（conversation-core「停止」）の「取消が確認できないコマンドを停止成功と表示してはならない」に違反している。

## What Changes

- `BackgroundCommands` のモニタなしの経路でも、取消を runtime へ command_id 単位で送る。
- GKE runtime に command_id 単位の取消 `cancel_command` を追加し、結果を `confirmed` / `unknown` として返す（LocalProcess は task の cancel でプロセスが止まるので従来どおり）。
- 取消を確認できなかった（`unknown`）コマンドは、tracker の ID を残す。再起動後の再開通知の対象になり続ける。
- `cancel_all`（停止要求の経路）は件数ではなく取消の内訳（confirmed / unknown など）を返し、確認できない分は「停止を確認できません」と表示する。
- `cancel_command` ツールは、リモートの停止を確認できないときに成功を返さない。
- 回帰テストを、実際の GKE runtime（fake の tracker クライアント）で追加する。

## Capabilities

### New Capabilities

なし

### Modified Capabilities

- `conversation-core`: 単一 Pod 構成のバックグラウンドのコマンドの取消について、リモートへの取消の送信、確認できない場合の扱い（unknown、ID の保持）を要件に加える。

## Impact

- `src/silk_agent/core/background.py`、`src/silk_agent/sandbox/gke.py`、`src/silk_agent/agent/sandbox_tools.py`（表示のみ）
- `tests/test_background.py`、`tests/test_gke_runtime.py`
- 分散構成（`command_monitor` あり）の取消の経路は変えない。
