# 分散配置の移行・ロールバック

この手順は停止を伴う移行用です。2026-10-15 時点では Worker 本番 factory、Control の送信・復旧配線、4役割 Deployment が未完了です。新系への実 Slack 受付切替は、それらと受け入れ検証が完了してから行ってください。旧管理 PVC は移行成功の確認が終わるまで保持します。

## 停止前の保全

旧 Bot と reconciler の Deployment 名・replicas、イメージ digest、設定、SQLite 管理 DB と対応する WAL/SHM、Agent Platform Runtime、Sandbox/Pod/PVC の UID を記録します。SQLite の稼働中コピーには SQLite backup API を使います。`.db` だけをコピーして WAL を捨ててはいけません。

旧 Bot の shutdown はユーザ STOP と同じ取消処理を通る版があります。進行中コマンドを移行前から継続できるとは保証しません。業務コマンドを完了させるか、その版の終了挙動を確認してから停止してください。人の依頼を模した Slack 投稿を自動では行いません。

## 停止と export

旧受付と旧 SDK・管理 reconciler を停止します。新 Gateway もまだ起動しません。両方を同じ会話へ接続してはいけません。停止した SQLite と Agent Platform 管理 Sessions を読み取ります。

```sh
uv run silk-agent migration export \
  --config /path/to/legacy-silk-agent.yaml \
  --sqlite-db /path/to/stopped-sandbox.db \
  --output /path/to/migration-source.json
```

出力は新規作成のみ、mode 0600 です。SDK transcript 本文は含みませんが、owner email、識別子、Sandbox の完全管理レコードを含むため非公開で保管します。`report.clean` が false の場合は欠落・重複・command ID・世代・UID の差異を解消し、再 export します。古い summary-only export は import できません。file operation や Sandbox mutation の途中の snapshot も import を拒否します。

## 既存 command ID の照合

各既存 Sandbox の UID・generation が snapshot と一致することを確認し、tracker ポートへの port-forward を用意します。`tracker-endpoints.json` は会話キーからその tracker URL への JSON map です。到達しない Sandbox を ensure/start/resume して補わないでください。

```json
{"slack:T123:C456:123.4": "http://127.0.0.1:18081"}
```

```sh
uv run silk-agent migration reconcile-commands \
  --config /path/to/legacy-silk-agent.yaml \
  --snapshot /path/to/migration-source.json \
  --tracker-endpoints /path/to/tracker-endpoints.json \
  --output /path/to/migration-reconciled.json
```

既存 command ID への GET だけを行い、start/cancel は呼びません。running、finished、cancelled、unknown を記録します。接続失敗・404・曖昧な応答は unknown のまま残します。旧 snapshot を書き換えず、別の 0600 ファイルを作ります。

## dry-run と import

対象 project/database/prefix を新系 config で確認し、専用 namespace の Firestore に接続します。`--legacy-stopped` は実際の停止確認を表す operator の宣言です。

```sh
uv run silk-agent migration import \
  --config /path/to/distributed-silk-agent.yaml \
  --snapshot /path/to/migration-reconciled.json \
  --legacy-stopped

uv run silk-agent migration import \
  --config /path/to/distributed-silk-agent.yaml \
  --snapshot /path/to/migration-reconciled.json \
  --legacy-stopped --execute
```

最初のコマンドは検証と件数・source hash の表示だけで書き込み0件です。実行時は source version/hash 付き cutover marker を先に保存し、各 document を transaction で新規作成します。途中失敗後は**同じ snapshot と同じ対象**で再実行します。同じ内容は変更せず、異なる内容・すでに新系が更新した document は上書きを拒否します。完了 marker は全件の作成・一致確認後だけ保存します。

SDK session ID、owner、Sandbox generation、Pod/PVC UID、fingerprint、通知を引き継ぎます。旧 running turn は interrupted_shutdown とし、SDK の実行スタックを自動再開しません。command は同じ ID、orphaned/held として保存します。未照合 command は unknown です。新しいコマンドとして送り直してはいけません。

marker は `${prefix}_migrations/cutover` と `${prefix}_migrations/import-complete` です。`accepting_input: false` は import 時点の記録であり、Gateway の起動を自動的に防ぐ lock ではありません。受付切替は operator が Deployment の起動状態で管理します。完了 marker、件数、SDK ID、owner、Sandbox 世代と UID、command 分類を照合してから新系の受付を切り替えます。

## ロールバック

**新系が一件も入力を受け付けていない場合**は新系を停止したまま、source snapshot と旧管理 DB/PVC を照合し、旧 image/config/replicas へ戻せます。import によって旧 DB・Sessions・PVC は変更されません。新系の単なる Pod 未起動だけでなく、Inbox/receipts に新規受付がないことを確認します。

**新系が入力を受け付けた後**は、過去の SQLite snapshot へ即復帰しません。新受付を止め、全 Worker を drain して SDK 停止を確認し、Firestore の最新 conversations、inputs、controls、commands、operations、outbox、receipts、ownership、migration marker と Agent Platform Sessions を保全します。新しいユーザ入力・結果・uncertain 配送・継続コマンドを旧形式と照合する逆移行が必要です。現実装は自動逆移行を提供しないため、その照合を完了できなければ新系で復旧します。

unknown/uncertain を pending に戻して再実行することは rollback ではありません。Slack の既送信投稿、外部 MCP の副作用、Sandbox の既実行コマンドは巻き戻りません。再投稿や再実行は人が結果を照合して判断します。
