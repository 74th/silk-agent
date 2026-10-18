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
2. イメージを作って push する: `k8s/build-image.sh`（出力される digest を `k8s/distributed/overlays/dev/kustomization.yaml` の `images[].digest`（ユースケースは `usecases/<name>/k8s/distributed/steady/kustomization.yaml`） に入れる）。**digest は役割追加前のイメージのままだと Worker/Control/Gateway が起動しない。**
3. Slack Secret `silk-agent-slack` を作る（`k8s/deploy.sh` が `.env` から作る）。Gateway は両方、Control は `SLACK_BOT_TOKEN` だけを参照する。Worker には Secret を渡さない。
4. `kubectl kustomize k8s/distributed/overlays/dev-bootstrap` を確認し、`kubectl apply -k k8s/distributed/overlays/dev-bootstrap` で適用する。bootstrap は `k8s/distributed/overlays/dev` と同じ resource で、Gateway / Worker / Control の replicas を `0` にしたもの。この時点では Slack に接続せず、SDK も起動しない。**`k8s/distributed/overlays/dev` を直接 apply すると、Gateway が Slack に接続し Worker が起動する**（定常状態の構成: Gateway 1 / Worker 2 / Control 1）ので、移行（§2）の前には使わない。
5. Sandbox Service の `rollout status` を確認する: `kubectl -n session-sandbox-system rollout status deployment/silk-agent-sandbox-service`。

## 2. 起動順序（切替）

旧系の停止とデータ移行は [移行手順](distributed-migration-runbook.md)。完了後に、次の順で replicas を 1 にする（このあいだは bootstrap を apply し続ける。bootstrap を再適用すると replicas が 0 に戻るため）。切替が完了したら `kubectl apply -k k8s/distributed/overlays/dev` で定常状態の宣言（Gateway 1 / Worker 2 / Control 1）に切り替える。

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

**回復は需要駆動。** Control は、失効した所有権について、その会話に未処理の入力（`pending` / `queued`）があるときだけ回復を試みる。誰も待っていない会話は 30 秒ごとに入力の有無を確認するだけで、旧 Worker への停止要求も Pod の確認も行わない。回復に失敗した（`recovery_blocked` のままの）場合は、5 秒から 5 分まで指数的に待ち間隔を延ばして再試行する（無限に高頻度で回り続けない）。実 GKE で、Pod を強制削除して 240 秒メッセージを送らない間は試行が 0 回で、メッセージを送ると 1 回の回復で引き継がれることを確認した。

**自動回復。** Control は、次の2つのうち、どちらかを確認できたとき、lease 失効から `auto_recover_after_seconds`（既定は lease の 2 倍＝120 秒。通信断で生きている旧 Worker が lease を失って自分で SDK を閉じるのに十分な時間）が過ぎた後に、自動で解放する。(1) **コンテナの再起動**: 同じ Worker 名（同じ Pod UID）で、より新しい incarnation が登録されている（OOM などでコンテナだけが再起動した場合。Pod は存在するが旧プロセスはない）。証拠は `auto:incarnation-replaced:<Pod 名>`。この判定は設定に依らず常に有効。(2) **Pod の不在**（`distributed.auto_recover_missing_pods: true`、開発環境の設定で有効）: `recovery_blocked` の担当について Kubernetes API で旧 Worker の Pod を引き、その UID の Pod が存在しない（404、または同名で UID が異なる）ことを確認できたとき。証拠は `auto:pod-absent:<Pod 名>`。API エラーなど判断できない場合は解放しない。実 GKE で、Pod の強制削除から約 174 秒で別 Worker へ引き継がれることを確認した。(2) は既定で無効で、無効の間と、Pod が存在して (1) にも当たらない場合は以下の手動手順になる。手動で解除しても、解除する旧 incarnation の予約だけを戻し、新しい incarnation の予約数は変えない。Worker の記録（`workers/{id}`、7 日で TTL 削除）がすでにない場合も、解放できる。残るリスクは、強制削除された Pod がノードの分断で実際には動き続けている場合で、状態・Outbox・Sandbox 操作は epoch で拒否されるが、外部 MCP の副作用と履歴への書込みは防げない。Control には `pods` の `get` 権限だけを与えている。自動回復を待つ間、会話には「担当の処理が途中で止まりました…」という通知を epoch ごとに 1 回だけ出す。

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

- 出力の送信が失敗したときの分類: Slack が `ok: false` で拒否した（投稿されていないことが確定）場合、一時的なコード（`internal_error`、`service_unavailable`、`fatal_error`、`request_timeout`）は待ちを置いて最大10回まで再試行し、それ以外（`channel_not_found`、`is_archived`、`msg_too_long` など）と、送信前の検証の失敗は、その出力だけを `failed`（終端）にして、後続の出力は送る。`failed` は `outputs_total{result="failed"}` とログ（`not posted and is given up`）で観測でき、保持期間の後に削除される。接続断やタイムアウトなど、投稿されたかどうか分からない場合は、従来どおり `uncertain` になる。
- 出力が `uncertain` の会話は、その会話の後続の出力の送信が止まる（順序を守るため）。他の会話の送信は止まらない。投稿の有無を確認した上で、該当 document の `status` を `delivered`（投稿済み）または `pending`（未投稿）に手動で更新する。更新前に `output_id` と Slack のスレッドを突き合わせる。
- 入力が `uncertain` の会話は、ユーザが次の入力を送ると通常どおり処理される。必要なら Slack で再依頼を案内する。

**入力の終端状態（会話が詰まらないための取り決め）**

| 状況 | 入力の状態 |
| --- | --- |
| ユーザの STOP / リセットで中断された turn の入力（停止境界以下） | `cancelled` |
| 新しいメッセージで turn が置き換えられた入力（`interrupt` モード） | `cancelled` |
| 停止境界より後なのに、停止される turn に渡ってしまった入力 | `uncertain`（実際に処理されたか不明。再送しない） |
| shutdown / drain / lease 喪失で中断された turn の入力 | `uncertain` |
| 結果も中断の理由も記録されずに SDK のストリームが終わった turn の入力 | `uncertain` |
| query の直前の記録（session_id・入力・attempt の関連）が拒否された、または Firestore のエラーで保存できなかった入力 | `uncertain`（プロンプトは SDK に送られていない。再送しない。ユーザには失敗が表示される） |
| SDK へ渡した後に「accepted」の記録が拒否された入力 | `uncertain`（再送・再試行しない） |
| 準備（ガード、履歴、開始者の確定）が12回失敗し続けた入力 | `blocked`（後続の入力が進めるように） |
| 送信者を確定的に解決できない入力（`user_not_found`、`user_not_visible`。3回） | `blocked`。Gateway がスレッドに「送信者を確認できなかったため…」と投稿する（Outbox を通さない直接の投稿で、失敗しても再試行しない） |
| Slack の一時的な障害・レート制限、および `missing_scope` / `account_inactive` / `invalid_auth`（トークン・アプリ側の問題）による送信者の解決失敗 | 待ち続ける（打ち切らない。復旧・設定修正後に処理される）。全メッセージが解決待ちで止まったら Gateway のログ「Slack user lookup is misconfigured」とトークンの `users:read` / `users:read.email` スコープを確認する |

停止が認可されてから Worker がそれを適用するまでの1〜2秒に届いた入力は、停止の適用が終わるまで SDK に渡さず（最大60秒。適用できない制御に対しては一度待ちきったら以降の入力は待たない。完了通知の入力も同じ）、停止後の新しい turn で処理する。制御の処理が失敗し続ける場合は、同じ制御を再試行する間隔を指数的に伸ばす（上限30秒）。新しい Worker は、起動時に前の所有者が残した `dispatching` / `accepted` の入力を `uncertain` にする。

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

- 旧 Worker が応答不能な間の扱いは設定で変わる。`distributed.auto_recover_missing_pods` が無効なら、旧 Worker の停止証拠が得られるまで復旧しない（可用性より単一実行を優先する厳密なモード）。**開発環境の配置では有効**で、旧 Worker の Pod が Kubernetes API 上で存在しない（404、または同名で UID が異なる）ことを確認でき、lease 失効から120秒が経過すると、旧 Worker の停止証拠なしに自動で解放する（§5）。この場合、強制削除された Pod が分断されたノードで動き続けていると、外部 MCP の副作用と Agent Platform 履歴への書込みは防げない。厳密さを優先する運用では、この設定を無効にする。
- 出力の origin 判定は、その会話で最後に渡した入力の番号を使う。STOP 直後の旧 turn が、次の入力が既に渡された後に出した遅延イベントは配送され得る。
- Worker の新しい Pod が既存会話を自動で引き取ることはない。常駐会話の再配置は idle 解放（`distributed.idle_close_seconds`）か drain を経由する。

## 9. 負荷測定と初期上限（2026-10-17）

`scripts/acceptance_load.py [会話数] [コマンド秒数]` で、実 Firestore・実 SDK・実 Sandbox を使って N 会話を同時に動かし、2回（1回目は Sandbox の初回起動を含む、2回目はウォーム）の完了時間と Worker の CPU/メモリを測る。試験会話の出力は配送済みにして Slack へ流さない。

測定条件: Worker 2台、1台あたり SDK 上限 4（合計8スロット）、8会話同時、`sleep 15`。

| | 完了までの時間 (最小/中央値/最大) | Worker のピーク (CPU / メモリ) |
| --- | --- | --- |
| 1回目（Sandbox 初回起動を含む） | 49 / 170 / 716 秒 | 232m・788Mi / 145m・637Mi |
| 2回目（ウォーム） | 23 / 26 / 649 秒 | 176m・698Mi / 101m・647Mi |

- メモリは SDK 4並列で650〜790Mi（1 SDK あたり150〜200Mi）。Worker の limit 2Gi に約2.5倍の余裕があるため、resource と SDK 上限は据え置いた。上限を4より上げる試験は行っていない。
- 最大値の長い遅延は、Sandbox の起動ではなく担当の割当待ちだった（`silk_agent_input_dispatch_latency_seconds` が平均約160秒）。使われなくなった会話がスロットを `distributed.idle_close_seconds` の間占有するため、並列度の実質の上限が下がっていた。この値を 600 秒から **180 秒**に短縮した。代わりに、次のメッセージでその会話のコールド起動（SDK の開始と履歴の再開）が増える。
- 一時停止中の Sandbox へ入力すると、再開に約70秒かかる（再開後の次の入力は約13秒）。Sandbox は `lifecycle.idle_timeout_seconds`（300秒）で一時停止し、一時停止の7日後に削除される（Sandbox の `spec.shutdownTime`、`shutdownPolicy: Delete`）。これは Sandbox Service が起動する `LifecycleReconciler`（`sandbox.run_reconciler: true`）の仕事で、無効だと Sandbox と PVC が回収されず増え続ける。
- Firestore の操作数はアプリ内では数えず、Cloud Monitoring の `firestore.googleapis.com/api/request_count`（`database_id=silk-agent`）で測った。同じ構成（Gateway 1 / Control 1 / Worker 2 / Sandbox Service 1）で、負荷試験の27分間（2026-10-17 08:03〜08:30 UTC、8会話×2回）は 74,003 リクエスト、試験の後のトラフィックがない27分間（09:00〜09:27 UTC）は 23,385 リクエストだった。つまり、何も起きていなくても毎分約870リクエスト（毎秒約14、1日で約125万）が、各役割の polling（Control の割当・認可・送信・回復の各 loop、Worker の登録更新と lease 更新、actor の入力読出し）で発生する。負荷による増分は約5.1万リクエストで、16 turn あたり約3,200（試験の turn は `sleep` だけの軽い依頼）。ベースラインの polling が主要な費用になるため、Firestore の費用を抑えるなら polling 間隔（既定は Control の各 loop が1〜5秒、actor が1秒）を延ばすことを検討する。
- 自動縮退（HPA など）は、drain との整合を確認していないため有効にしていない。

## 10. 実環境の受け入れ記録（OpenSpec 14.7、2026-10-15〜18）

「確認済み」と「未実施」を分けて記録する。時刻は UTC（JST は +9 時間）。Slack の操作は人間（74th）が実施し、結果を報告した。API 経由は、実 Firestore へ入力と制御を直接投入した検証（Slack を使わない）。

**環境**: GKE `gke_nnyn-dev_us-central1-a_silk-agent-gke`、namespace `session-sandbox-system`（Sandbox は `session-sandboxes`）、Firestore `nnyn-dev` / `silk-agent`、Slack ワークスペース `T8W42FC4U` のチャンネル `C0C8ETHFKS8`（#test-agent）。Gateway 1 / Control 1 / Worker 2 / Sandbox Service 1。commit `f988f3c`、イメージ `sha256:ae932f60023d6098b5c5b9d04ebe9abaa729d4d0b0319c354b47d62f511a51fb`（2026-10-18 時点）。下記「query 前の記録」の再確認だけは commit `d1e307e`、イメージ `sha256:bfb6d7e125cad25c95efabd5b66544aa58f3debcef243d35c9efe2815281a6e2` で実施した。

| 確認項目 | 方法 | 結果 | 記録 |
| --- | --- | --- | --- |
| 質問への応答、進捗表示 | Slack | 確認済み | 操作者の報告（2026-10-15〜17） |
| 追記（実行中の turn への追加指示） | Slack | 確認済み | 操作者の報告（2026-10-18）。command_id は記録していない |
| STOP（実行中の turn、background コマンド） | Slack、API | 確認済み | Slack は操作者の報告。API は約5秒で中断、後続の入力も処理 |
| background コマンドの完了返信 | Slack | 確認済み | 2026-10-17 07:17 の command_id `843a35286c7f4e9594097c4344296ac7`、`6066d77ff9f44f1d97312b007688365f`（いずれも finished / background） |
| 複数スレッドの同時実行（2 Worker） | Slack、API | 確認済み | Slack は操作者の報告。API は8会話を2 Worker に分散（負荷測定 §9） |
| リセット | Slack、API | 確認済み | 世代が2に進み、通知が出て、文脈が消えることを確認（2026-10-17） |
| 再デプロイをまたぐ追加指示 | Slack、API | 確認済み | Worker 入替え後の追加指示に文脈つきで返答（API は約12秒） |
| Pod の強制削除からの回復 | Slack、API | 確認済み | 強制削除の約1分後に回復待ちの通知が1回、約2分後に別 Worker へ引継ぎ（2026-10-17 16:53〜16:56 JST）。旧 turn の入力は uncertain で再実行されない |
| drain | Slack + CLI | 確認済み | 下記 |
| query 前の記録（tasks 8.5）と STOP、再デプロイ | API（`scripts/acceptance_stop.py`、`acceptance_rollout.py`） | 確認済み | 2026-10-18 05:22〜05:25 UTC、commit `d1e307e`。STOP した入力は `cancelled`、直後と後続の入力は `completed`、未終端なし（停止後9秒）。すべての入力に `sdk_session_id` と `query_started_at` が保存された。Worker の rollout（28秒）をまたいで同じ `sdk_session_id` で再開し、追加指示に文脈つきで12秒で返答した。Worker のログに `refused` / ERROR なし |
| 許可外ユーザの STOP | — | **未実施** | 許可外のユーザがいないため。単体テストでの確認のみ |

**drain（2026-10-18 00:49〜00:58 UTC、会話 `…1792284522.078759`）**: 8 回の `sleep 8` の実行中（00:49:20 開始）に、`silk-agent worker drain-all` を 00:49:42 に実行（担当 Worker は `draining=true`、全体の割当停止も有効）。turn は中断されず最後まで完了し（command_id `264149e69a384fbfa1245c55e10d1ca9` から `d2ed4545d12d4f13ba026425a64ff6b3` までの8件がすべて finished / foreground）、「完了」の返信が出て、00:50:47 に担当が解放された（`reserved=0`）。drain 中の 00:56 に送った追加メッセージは pending のまま待機し（会話は waiting、担当なし）、`undrain-all` の 00:57:22 以降に処理されて 00:58:00 に完了した（command_id `fe19bcabea1e40788ea09c4c1ef5c781`）。

**この受け入れで未確認、または近似になっている点**
- 許可外ユーザの STOP は未実施。
- Worker の異常終了は、実プロセスへの SIGKILL ではなく `kubectl delete pod --grace-period=0 --force` で再現した。
- Gateway と Control は1台で運用しており、2台構成での受信の重複排除と割当の競合は、Firestore Emulator の試験までの確認。
- `drain-all --wait` は、死んだ Pod の古い登録が残っていると完了しなかった（2026-10-18 に修正: heartbeat が lease の期間内にある Worker だけを待つ。古い登録は `status-all` に `stale=true` と表示される。実クラスタで確認済み）。


## 11. データの保持期間とバックフィル（`add-data-retention`）

処理が済んだ Firestore の記録は、終端状態に変わる書き込みで `expire_at`（Timestamp）が付き、Firestore の TTL が保持期間の後に削除する。処理中・未確定（uncertain）・保留（held）・完了通知が済んでいないコマンドには付けない。所有権と lease の期限 `expires_at` には TTL を付けない（名前が似ているので注意）。

### 11.1 保持期間の設定

`distributed.retention`（単位は日。0 以下は設定エラー）。

| 設定 | 既定 | 対象 |
| --- | --- | --- |
| `terminal_days` | 30 | 入力（completed / blocked / cancelled / suppressed）、制御要求と `control_queue`（handled / filtered / stale）、outbox（delivered / suppressed）、コマンドと `sandbox_operations`、quiesce 要求（answered） |
| `receipt_days` | 7 | Slack の受信記録（受信時刻から。Slack の再送は数時間以内） |
| `worker_days` | 7 | Worker の登録（最後の heartbeat から。生きている Worker は 20 秒ごとに延びる） |

コマンドは「終了」と「通知が済んだ」の遅い方から数える。バックグラウンドコマンドの通知は、その `command_finished` 入力が completed になった時点で delivered になる（STOP などで取り消された入力は suppressed）。入力が uncertain のまま残ったコマンドは、運用者が確認するまで消えない。

削除してはならないもの: 会話ごとの管理情報（`conversations`、`ownership`、`sandboxes`、`resource_leases`、`user_rate_limits`）。会話の入力がすべて消えても会話は再開できる。試験用の会話（`acceptance:*`、`loadtest:*`）の掃除はこの仕組みの対象外。

TTL による削除は、期限から最大24時間ほど遅れる。TTL の削除は Firestore の課金対象の削除操作になる。

### 11.2 Terraform（TTL ポリシー）

`terraform/main.tf` が、対象のコレクションの `expire_at` に TTL ポリシーを設定する（コレクショングループ: `inputs`、`controls`。接頭辞つき: `outbox`、`commands`、`sandbox_operations`、`quiesce_requests`、`slack_receipts`、`control_queue`、`workers`）。`expire_at` は索引から外してある。コレクション名の接頭辞は `firestore_prefix`（既定 `silk-agent`）で、設定ファイルの `distributed.firestore_prefix` と一致させる。

```sh
cd terraform
terraform plan   # TTL の追加だけが出ること（既存の索引に差分がないこと）
terraform apply
gcloud firestore fields ttls list --database=silk-agent --project=nnyn-dev \
  --format="table(name.scope(collectionGroups),ttlConfig.state)"   # 9 件が ACTIVE になるまで数分〜
```

`inputs` の TTL は `moved` で新しいリソース名に引き継がれる（再作成されない）。

### 11.3 既存の記録へのバックフィル

保持期限の仕組みを入れる前に終端になった記録には `expire_at` がない。次の手順で付ける。

1. バックアップを取る（TTL の削除は戻せない）: `gcloud firestore export gs://<bucket>/retention-$(date +%Y%m%d) --database=silk-agent --project=nnyn-dev`。`gcloud firestore operations describe` で `operationState: SUCCESSFUL` を確認する。
2. 新しいコードを配置する（新しい記録から期限が付く）。その後 `terraform apply` で TTL を有効にする（§11.2）。
3. 件数だけを確認する（書き込まない）:
   ```sh
   silk-agent retention backfill --config CONFIG
   ```
   種類ごとの調べた件数（examined）、対象（eligible）、除外（excluded と理由）が出る。`state uncertain` / `state held` / `notice pending` などの除外は想定どおりか確認する。
4. 書き込む:
   ```sh
   silk-agent retention backfill --apply --config CONFIG
   ```
   件数を絞るなら `--limit 100` のように指定する（種類ごとの上限）。1 件ずつ transaction で「状態がまだ終端で、`expire_at` がない」ことを確認してから書く。終端時刻が保存されていない記録（受信記録など）は実行時刻から数える。保存されている時刻（outbox の `updated_at`、Worker の `heartbeat_at` など）がある記録は、その時刻から数えるので、保持期間より古いものは次の TTL の実行で消える。
5. もう一度手順 3 を実行し、対象（eligible）が全種類で 0 になることを確認する。翌日以降、outbox と inputs の件数が減り、uncertain・held の件数が変わらないことを確認する。

`notice pending` のコマンド（この change の前に終了して通知が pending のまま残ったもの）、`state requested` のまま応答のない quiesce 要求、`pending` のまま配送されない outbox は、期限が付かない（除外の表で件数を確認できる）。件数が少なければそのまま残し、増えるようなら原因を調べる。

2026-10-18 の実績（dev、`silk-agent` DB）: バックアップは `gs://nnyn-dev-silk-agent-command-output/firestore-backups/`（このバケットは 30 日で削除される）、TTL は 9 件すべて ACTIVE。バックフィルの件数表示は、inputs 108 / controls 16 / control_queue 16 / outbox 818 / commands 99 / sandbox_operations 99 / slack_receipts 71 / workers 92 が対象で、uncertain の入力 30、通知 pending のコマンド 13、pending の outbox 29・uncertain の outbox 3、応答のない quiesce 要求 23 が除外。

戻す場合: Terraform で該当の `google_firestore_field` を削除して TTL を外す。既に消えた記録はバックアップからしか戻せない。

### 11.4 履歴を失った会話

会話の履歴（Agent Platform Sessions）は作成から30日（`session_store.ttl_seconds`、下限は24時間）で消える。入力を処理するたびに、残りが半分を下回っていれば期限を延ばすので、使われている会話の履歴は消えない。

- 長く使われなかった会話の履歴が消えていた場合、新しい履歴で続け、スレッドに「以前の会話の履歴は保存期間を過ぎたため引き継げませんでした。作業領域のファイルは残っています。」と表示する。入力は失敗にならない。会話の管理情報に `history_reset_reason=expired` が残り、`sdk_session_id` は新しいものになる。会話をリセットすると `history_reset_reason` は消える。
- 処理中に履歴が消えた場合（追記が「存在しない」で失敗）は、保存先を作り直して追記を続け、同じ `history_reset_reason` を記録する（その turn は SDK のメモリの履歴で続く）。ユーザへの通知は出ない。
- 延長に失敗しても turn は止まらない（ログ `could not extend session` を確認する。次の入力で再試行する）。
- 履歴の調査: 会話のドキュメントの `history_reset_reason` と `sdk_session_id` を見る。作業領域（Sandbox のファイル）は履歴と別に残っている。

## 付録 A. Agent Platform Sessions の期限延長の実機確認（2026-10-18、`add-data-retention` 1.1）

dev の Agent Platform（`reasoningEngines/868017700884971520`）に試験用セッションを作り、SDK の `client.agent_engines.sessions._update` で期限を更新できるかを確認した。試験用セッションは確認後に削除した。

| 確認 | 結果 |
|---|---|
| `ttl` の下限 | **24 時間**。`ttl=3600s` での作成は `400 INVALID_ARGUMENT: ttl must be at least 24 hours` で拒否される |
| `_update(name=, config={"ttl": "172800s", "update_mask": "ttl", "wait_for_completion": True})` | 成功。`expire_time` は **更新時刻 + ttl** になった（作成時の 24 時間後 → 更新時の 48 時間後） |
| `_update(name=, config={"expire_time": <datetime>, "update_mask": "expire_time", "wait_for_completion": True})` | 成功。`expire_time` が指定した時刻になった |
| イベントの追記（`events.append`） | `update_time` は進むが `expire_time` は延びない |
| 公開の `update` | ない（`_update` のみ）。REST の PATCH を直接呼ぶ必要はなく、`_update` で足りる |
| `delete` | `config` に `wait_for_completion` を渡すと検証エラーになる。`delete(name=)` だけで削除できる |

この change では `update_mask="ttl"` を使い、`ttl` は `{ttl_seconds}s` で渡す。

## 12. 複数ユースケースの分離の確認（`support-distributed-usecases` 6.4、2026-10-18）

dev（`session-sandbox-system`）と bigquery-analysis（`bigquery-analysis-system`）を同じクラスタに置いた状態で確認した。

| 確認 | 結果 |
|---|---|
| bigquery-analysis の Worker から dev の Sandbox Service（`silk-agent-sandbox-service.session-sandbox-system.svc:8080/healthz`）へ接続 | タイムアウトで拒否（NetworkPolicy。Worker の egress は自分の namespace の Sandbox Service だけ） |
| 同じ Worker から自分の Sandbox Service（`...bigquery-analysis-system.svc:8080/healthz`） | 200 |
| `bigquery-analysis-system` に dev の設定（`overlays/dev/config`）で `silk-agent worker run` の Pod を作る | 起動せず終了コード 1。`ConfigError: configuration does not match the deployment: distributed.sandbox_service_url: expected namespace 'bigquery-analysis-system', got 'session-sandbox-system'` |
| `silk-agent worker status-all`（bigquery-analysis の Worker から） | bigquery-analysis の Worker 2 台だけが出る（接頭辞が違うので dev の Worker は見えない） |

確認用の Pod と ConfigMap は削除した。照合のコードは dev の現行イメージ（`sha256:00675bf4...`）には入っていないので、dev の Pod は照合を行わない。dev のイメージを更新したときに有効になる。

### 12.1 bigquery-analysis の実会話（`support-distributed-usecases` 6.3、2026-10-18）

dev と同じ Slack アプリ（試験用に同じトークン）に「日本の直近2日の急上昇検索語を…」と依頼した。bigquery-analysis 側で処理され、`google-trends` Skill と `bash`（`query.sh`）が成功して Slack に返信された（所要 53 秒）。

- 会話は `bigquery-analysis_conversations` に 1 件記録され、同じ会話 ID は dev の `silk-agent_conversations` にない。
- Sandbox は `bigquery-analysis-sandboxes` に作られた（`session-4d6cf1e092-g1`）。dev の `session-sandboxes` に新しい Sandbox はない。
- Sandbox の管理情報は `bigquery-analysis_sandbox_operations` に入った。
