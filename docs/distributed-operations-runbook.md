# 分散構成の運用手順

Gateway / Worker / Control / Sandbox Service からなる分散構成の構築・増設・縮退・障害対応の手順。旧系（`silk-agent slack`、SQLite 管理 DB）からの切替とロールバックは [distributed-migration-runbook.md](distributed-migration-runbook.md) に分ける。この文書のコマンドは `silk-agent` CLI の引数と `k8s/distributed/` の resource 名に合わせてあり、`tests/test_distributed_runbook.py` が解析できることを確認している。

**旧系と新系を同時に起動する手順はない。** 同じ Slack App を同時に購読するだけでなく、同じ会話を二重に処理するため、切替は必ず旧系の停止を先にする。

## 0. 前提

- 実行は `execution_mode: distributed` の設定 (`SILK_AGENT_CONFIG`)。in-process の `silk-agent slack` は分散データと混在できない。
- 役割ごとのコマンド。Worker は管理サブコマンドと並ぶため `run` を付ける。

| 役割 | コマンド | Slack 資格情報 |
| --- | --- | --- |
| Gateway | `silk-agent gateway --config CONFIG` | bot + app token |
| Worker | `silk-agent worker run --config CONFIG` | なし |
| Control | `silk-agent control --config CONFIG` | bot token のみ |
| Sandbox Service | `silk-agent sandbox-service --config CONFIG` | なし |

- 各 Pod は `:9090` に `/healthz`（loop の停止検知）、`/readyz`（各 loop が一度動いたか）、`/metrics` を持つ（`distributed.health_port`）。Sandbox Service は従来どおり自身の `:8080` に同じ 3 経路を持つ。

## 1. 構築

1. Terraform で Firestore・index・IAM・KSA 用 principal を作る。`terraform -chdir=terraform plan -var-file=terraform.tfvars` の差分に replace / delete が無いことを確認してから apply する。役割別の GKE principal は `silk-agent-gateway` / `silk-agent-worker` / `silk-agent-control` / `silk-agent-sandbox-service`。
2. イメージを作って push する: `k8s/build-image.sh`（出力される digest を `k8s/distributed/kustomization.yaml` の `images[].digest` に入れる）。**digest は役割追加前のイメージのままだと Worker/Control/Gateway が起動しない。**
3. Slack Secret `silk-agent-slack` を作る（`k8s/deploy.sh` が `.env` から作る）。Gateway は両方、Control は `SLACK_BOT_TOKEN` だけを参照する。Worker には Secret を渡さない。
4. `kubectl kustomize k8s/distributed` を確認し、`kubectl apply -k k8s/distributed` で適用する。Gateway / Worker / Control の replicas は `0` で作られ、この時点では Slack に接続せず SDK も起動しない。
5. Sandbox Service の `rollout status` を確認する: `kubectl -n session-sandbox-system rollout status deployment/silk-agent-sandbox-service`。

## 2. 起動順序（切替）

旧系の停止とデータ移行は [移行手順](distributed-migration-runbook.md)。完了後に、次の順で replicas を 1 にする。

```sh
kubectl -n session-sandbox-system scale deployment/silk-agent-control --replicas=1
kubectl -n session-sandbox-system scale deployment/silk-agent-worker --replicas=1
kubectl -n session-sandbox-system rollout status deployment/silk-agent-worker
silk-agent worker status-all --config CONFIG
kubectl -n session-sandbox-system scale deployment/silk-agent-gateway --replicas=1
```

Worker が `status-all` に出てから Gateway を起動する。Gateway を最後にするのは、受信した入力が Inbox に残っても Control と Worker が処理できる状態にしてから Slack の流れを開くため。

## 3. 増設

1. `silk-agent worker status-all --config CONFIG` で `reserved` / `active_sdk` / `capacity` を見る。上限は `distributed.max_active_sdks_per_worker`（初期値 4、実測で調整する）。
2. `kubectl -n session-sandbox-system scale deployment/silk-agent-worker --replicas=2`。新 Pod は起動 UUID を含む新しい incarnation として登録される。
3. 割当待ちが解消したことを `/metrics` の `silk_agent_control_waiting_conversations` が 0 に戻ることで確認する。既に常駐している会話は再配置されない。新規・解放済み会話から新 Pod に割り当てられる。

Gateway と Control も replicas を増やせる。Gateway は受信を重複排除し、Control の割当・送信は lease と transaction で二重処理を防ぐ。

## 4. 計画停止と縮退

任意の Pod を直接削除する縮退は、担当中の会話の SDK を中断するため安全な縮退として扱わない。先に drain する。

```sh
silk-agent worker drain-all --config CONFIG --wait
```

- 新規割当を止めてから各 Worker を draining にし、`reserved=0` かつ `active_sdk=0` を待つ。担当中の会話への追記・停止は受け付け続ける。
- `--timeout SECONDS`（または `distributed.drain_timeout_seconds`）を付けると期限後に SDK の中断と履歴保存を行う。実行中の Sandbox コマンドは継続し、再送されない。期限が切れても drain 状態は維持される。
- 戻り値 `2` は期限切れ、`1` は Worker 不在または incarnation の変更。
- `drained` を確認してから replicas を変える。

```sh
kubectl -n session-sandbox-system scale deployment/silk-agent-worker --replicas=1
silk-agent worker undrain-all --config CONFIG
```

Worker を起動した後に `undrain-all` を実行する。drain 中に Slack から届いた入力は Inbox に残り、解除後に処理される。個別の Worker は `silk-agent worker drain WORKER_ID --incarnation INCARNATION`（解除は `undrain`、状態は `status`）で扱う。

Worker のイメージ更新も同じ手順を使う（RollingUpdate は新 Pod を先に起動する）。drain せずに更新しても、終了する Worker は先に自分を draining にして新規割当を止め、実行中の turn を最大 25 秒待ってから中断・保存して所有権を解放する（`terminationGracePeriodSeconds: 60`）。実 GKE で、turn 実行中の Pod 削除から約 30 秒で別 Worker へ引き継がれることを確認した。長い turn は中断される（再実行はされず、その入力は `uncertain`）。Gateway / Control の更新は drain を必要としない。

## 5. recovery_blocked の調査と解除

Worker が lease を失った会話は、旧 Worker の SDK 停止が確認できるまで新しい Worker に渡さない。Control は期限切れの所有権を検出し、旧 incarnation に停止要求を出して証拠を待つ。応答がなければ `recovery_blocked` のままになり、`silk_agent_control_recovery_pending` が 0 でなくなる。

```sh
silk-agent recovery list --config CONFIG
```

`state=recovery_blocked` の会話を調べる。

1. 旧 Worker Pod が生きているか確認する（`kubectl get pod`、Pod UID は incarnation に含まれる）。**Pod が見えない、または force delete しただけでは停止の証明にならない。** ノードに到達できない場合、プロセスは動き続けている可能性がある。
2. Worker が戻れば、Control が自動で再試行して解除する。何もしない選択が最も安全。
3. ノードの隔離・削除などで停止を確認できた場合だけ、手動で解除する。

```sh
silk-agent recovery confirm-stopped CONVERSATION_KEY --evidence "node gke-xxx deleted, ticket 123" --config CONFIG
```

`--evidence` は解除理由として保存される。lease が切れていない所有権は拒否される。解除すると `dispatching` / `accepted` だった入力は `uncertain` になり、実行中の foreground コマンドは `orphaned` になる（コマンドは取り消されず、同じ command_id が引き継がれる）。誤って生きている Worker に対して実行すると、同じ会話の SDK が二重に動く。

## 6. uncertain の調査

結果不明を自動で再実行・再投稿しない。調査は Firestore コンソール（または `gcloud firestore`）で、PREFIX は `distributed.firestore_prefix`。

| 対象 | 場所 | 見る項目 |
| --- | --- | --- |
| 入力 | `PREFIX_conversations/*/inputs` の `delivery_state == uncertain` | `dispatch_attempt_id`, `recovery_evidence_id`。SDK に渡ったか不明。会話履歴（Agent Platform Sessions）で応答の有無を確認する |
| 出力 | `PREFIX_outbox` の `status == uncertain` | `output_id`, `attempt_count`, Slack ref。Slack のスレッドで同じ投稿があるか確認する |
| コマンド | `PREFIX_commands` | `state`, `delivery_mode`, `command_id`。`command_status` で同じ ID を照会し、確認前に同じコマンドを再実行しない |

- 出力が `uncertain` の会話は、その後の出力の送信が止まる（順序を守るため）。投稿の有無を確認した上で、該当 document の `status` を `delivered`（投稿済み）または `pending`（未投稿）に手動で更新する。更新前に `output_id` と Slack のスレッドを突き合わせる。
- 入力が `uncertain` の会話は、ユーザが次の入力を送ると通常どおり処理される。必要なら Slack で再依頼を案内する。

## 7. 観測

`/metrics`（`:9090`）の主な指標。会話 ID・ユーザ・本文は label に使わない。

| 指標 | 意味 |
| --- | --- |
| `silk_agent_worker_active_sdk` / `worker_reserved` / `worker_capacity` | Worker の稼働 SDK、予約数、上限 |
| `silk_agent_worker_lease_renewal_failures_total{reason}` | lease 更新失敗（`error` は保存先エラー、`lost` は所有権喪失） |
| `silk_agent_input_dispatch_latency_seconds` | 受信から SDK 投入決定まで（追記・開始の遅延） |
| `silk_agent_control_latency_seconds{kind}` | stop / reset の受信から完了まで |
| `silk_agent_output_delivery_latency_seconds` | 出力の作成から Slack 配送まで（完了から返信まで） |
| `silk_agent_control_waiting_conversations` / `control_available_workers` | 割当待ちの会話数と利用可能な Worker 数 |
| `silk_agent_control_recovery_total{result}` / `control_recovery_pending` | 回復の結果 / 停止証拠待ちの数 |
| `silk_agent_outputs_total{result}` | 配送結果（`delivered` / `retry` / `uncertain`） |
| `silk_agent_inputs_uncertain_total` | uncertain になった入力 |
| `silk_agent_process_resident_memory_bytes` | プロセスのメモリ |

Firestore の操作数は Cloud Monitoring の `firestore.googleapis.com/api/request_count` を使う（アプリ内では数えない）。外部から scrape するには、NetworkPolicy に scraper からの ingress を追加する（既定は ingress なし）。

## 8. 既知の制限

- 実 Slack・2 台の本番 Worker・実負荷での受け入れは未実施。`replicas: 0` の既定は、その確認前に有効化しないため。
- 旧 Worker が応答不能な間は復旧しない（可用性より単一実行を優先する）。
- 出力の origin 判定は、その会話で最後に渡した入力の番号を使う。STOP 直後の旧 turn が、次の入力が既に渡された後に出した遅延イベントは配送され得る。
- Worker の新しい Pod が既存会話を自動で引き取ることはない。常駐会話の再配置は idle 解放（`distributed.idle_close_seconds`）か drain を経由する。
