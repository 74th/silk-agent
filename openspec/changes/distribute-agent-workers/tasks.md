# Tasks

実装者向け: 各節を番号順に進める。設計判断は `design.md` の D1～D11、振る舞いは `specs/` を正本とする。パスは推奨配置であり、責務を保って分割できる。各 checkbox は実装とその局所検証が終わってから完了にする。文書ができただけで実装済みにしない。後段の広域試験までは、実環境の replicas を増やさない。

既存の追記・interrupt・queue・20秒での background 化・即時 background 指定を維持する。変えるのは shutdown 時の remote cancel と全他起動IDのクラッシュ判定。API/SDK と DB の原子性を仮定せず、結果不明は uncertain にする。

## 1. 型・設定・依存の基礎（D1～D3）

- [x] 1.1 `core/distributed_models.py` に InputEnvelope、ControlEnvelope、OwnershipToken、WorkerRecord を定義する。会話キー、seq、入力ID、会話世代、Worker incarnation、epoch、期限、schema_version を明示する。検証: serialization round-trip と未知の enum/不正な世代/naive datetime の拒否をユニットテストする。
- [x] 1.2 同モジュールに入力配送状態、コマンド実行状態、通知状態、出力配送状態を独立した enum として定義する。accepted と completed、detach と cancel、実行状態と notification_state を混同しない。検証: design D3/D7 の全状態を表現でき、禁止遷移（completed→pending 等）を拒否するテストを追加する。
- [x] 1.3 `core/distributed_ports.py` に InboxStore、OwnershipStore、CommandStore、OutboxStore、WorkerDirectory の Protocol を追加する。単なる全体 put ではなく、受付採番、世代付き更新、terminal+通知入力の原子的記録、予約/解放を操作として表す。検証: fake 実装と型検査で呼出し契約が一致する。
- [x] 1.4 `config.py` に実行モード、Firestore project/database/prefix、Service URL、lease60秒/更新20秒/余裕5秒、SDK上限4、idle600秒、drain期限（既定なし）を追加する。負の値、更新間隔が期限以上、分散モードでSQLite設定を拒否する。検証: `tests/test_config.py` で旧設定互換と不正設定を確認する。
- [x] 1.5 `pyproject.toml` と lock に Firestore クライアントを追加し、環境の ADC と emulator 接続を分ける factory を作る。emulator host を明示指定した試験が本番へ接続しないようにする。検証: `uv sync`、factory の endpoint 選択テスト、型検査が成功する。
- [x] 1.6 `tests/fakes` 相当の場所に shared memory store、fake clock、明示 barrier 付き fake SDK/remote command を用意する。複数 Worker が同じ store を参照できるようにする。検証: 2 consumer の競合を sleep 頼りにせず再現できる基盤テストを実行する。

## 2. Firestore の入力・状態・watch（1完了後、D3～D4）

- [x] 2.1 `stores/firestore/` に共通クライアント、document ID hash、schema codec、エラー変換を実装する。本文/認証情報を例外ログへ出さない。検証: 同じ会話キーが安定したIDになり、異なるworkspaceの同一tsが衝突しないことを確認する。
- [x] 2.2 受信 transaction を実装する。event receipt と message receipt を読み、未処理なら会話seq採番・input/control・receipt・割当待ちを一緒に保存する。検証: emulator で同時2受信、同一event再送、mention/message二重配送を行い、入力が一件だけになる。
- [x] 2.3 入力状態を expected state + owner epoch で進める API を実装する。dispatch_attempt_id を送信前に保存し、再起動時に dispatching を uncertain へ分類できるようにする。検証: 古い世代の更新拒否、append不可時のpending復帰、送信結果不明時の再送抑止をテストする。
- [x] 2.4 会話管理情報を部分更新する API を実装する。turn 状態、owner_user/email、sdk_session_id、停止境界を区別し、開始者は最初の承認済み入力で一度だけ確定する。検証: 進捗更新とコマンド更新が同時でも履歴参照や開始者が消えない。
- [x] 2.5 watch adapter を作り、callback thread から asyncio loop へ signal を渡す。unsubscribe、再接続、未処理の再照会、重複signalの集約を実装する。検証: 切断中の新着を回収し、同じ入力を二度SDKへ渡さず、終了後にtask/threadが残らない。
- [x] 2.6 実際の query に対応する composite index と本文フィールドの index exemption を定義する。処理済みのみTTL対象にし、子コレクションの削除方法も定義する。検証: query一覧とindex定義の対応表を作り、pending/uncertain が保持期限で消えないテストを行う。

## 3. 所有権と割当（2完了後、D5）

- [x] 3.1 Worker 登録/heartbeat を実装し、Pod UID と起動 UUID で incarnation を識別する。draining、capacity、reserved、active_sdk を保存する。検証: 同じPod内のプロセス再起動が別incarnationとして認識され、旧heartbeatを受け入れない。
- [x] 3.2 Control allocator の役割leaseと割当transactionを実装する。draining/期限切れWorkerを除外し、会話ownershipとWorker予約数を同時更新する。検証: allocator二つが競合しても二重割当・capacity超過にならず、満杯時は入力が待機する。
- [x] 3.3 Worker の ownership activation/renewal/release を実装する。assignedをactiveにしてからSDKを起動し、期限前に更新不能なら新規実行を止める。検証: fake clockで期限境界を進め、旧tokenでSDK新規入力・状態保存が拒否される。
- [x] 3.4 epoch付きの状態更新・Outbox作成・Service操作受理を共通の不変条件として実装する。検証と保存を別々のget/putにしない。検証: 所有権が読取直後に変更される競合を注入し、旧操作がcommitされない。
- [x] 3.5 idle解放と新着入力の調停を実装する。SDK保存/closeを先に完了し、残入力の有無と割当待ち更新をtransactionで確定する。検証: 解放直前・直後の新着が旧SDKへ消えることなく、同じ/次Workerで一度処理される。

## 4. Sandbox Repository の分散対応（1～2完了後、D8）

- [x] 4.1 `gke_session_sandbox/repository_port.py` を追加し、manager/reconciler が使う get/get_by_hash/create/CAS/lease/reserve_command/event ack/operation 更新を抽象化する。SQLite具体型への依存を factory へ移す。検証: 既存 `tests/session_sandbox/test_manager.py` と `test_sqlite_repository.py` が通る。
- [x] 4.2 `firestore_repository.py` に既存 SessionRecord の codec と revision付きCASを実装する。会話hash、generation、Pod/PVC UID、環境fingerprint、pending notice を保持する。検証: SQLite fixtureをFirestoreへ往復し、情報の欠落がない。
- [x] 4.3 Sandbox操作用resource leaseを実装し、create/CAS/reserve_command/event ackで所有者と世代を検査する。検証: 二つのManagerによる同時ensure/suspendでリソースが一つになり、失効tokenの更新は拒否される。
- [x] 4.4 file operation をIDと実行主体・状態で記録する方式へ変更する。起動時のactive_file_operations一括ゼロ化を削除し、他プロセスの稼働を維持する。検証: Manager Aの操作中にBを再起動してもSandboxがidle停止されない。
- [x] 4.5 reconcilerの候補検索を状態/期限とページングに変更する。desired stateとKubernetes実体をUID/generationで照合し、途中再起動から収束させる。検証: create応答喪失、suspend途中、delete途中、実行状態unknownでのケースをfake backendで確認する。

## 5. Sandbox Service API と remote adapter（3～4完了後、D7/D11）

- [x] 5.1 `sandbox/service.py` にaiohttp起動、health/readiness、認証middleware、構造化エラーを実装する。専用audienceのprojected KSA tokenをTokenReviewで検証し、許可KSAだけ受理する。検証: 無token、期限切れ、別audience、未許可KSAを拒否するテストを追加する。
- [x] 5.2 session ensure/status/notices/ack APIと `sandbox/remote.py` の対応を実装する。readiness照会はSandboxの起動・idle更新を起こさない。検証: 現行 `test_notice_reaches_the_next_turn_and_is_acked_afterwards` と準備中表示の契約をremote経路でも確認する。
- [x] 5.3 command start/status/output/cancel/detach のrequest/response型を定義する。request_id、会話/owner世代、turn、origin_seqを必須にする。開始intentを外部送信前に保存する。検証: 同じID+payloadが同一コマンドになり、同じID+異なるpayloadはconflictとなる。
- [x] 5.4 trackerへのstart応答喪失からID照会する経路を実装する。404や世代不一致を即再実行へ変換しない。検証: startを受理後に接続を落とし、remote startの呼出し回数が1回のままunknownまたは既存結果へ回復する。
- [x] 5.5 ファイルread/write/list、Skills同期のremote APIを実装する。Serviceで会話とSandbox世代を解決し、任意の別会話endpointを呼出し側に選ばせない。検証: 会話間のファイル分離、サイズ上限、部分Skills同期失敗、旧epochの変更拒否を確認する。
- [x] 5.6 `GkeSessionSandboxRuntime.run_command` とremote adapterで、waitのCancelledErrorから自動cancelを除く。明示cancelは別操作で実行する。検証: `tests/test_gke_runtime.py` のwait取消テストを新仕様へ変更し、wait取消後も同じIDがrunning、明示cancel時だけ停止となる。
- [x] 5.7 mutation受理とstop/generation更新の競合を処理する。受理済みintentを台帳に残し、後から停止対象として回収できるようにする。検証: barrierの直前/直後でstartを止める試験を行い、無追跡のコマンドや停止後の旧turnの新規実行が残らない。

## 6. 永続コマンド監視（5完了後、D7）

- [x] 6.1 `sandbox/monitor.py` に非終端コマンドのclaim、状態照会、再試行時刻、lease引継ぎを実装する。状態不変でのFirestore書込みを避ける。検証: Monitor A停止後にBが同じIDを監視し、実行を再送しない。
- [x] 6.2 foreground/background/orphaned と terminal 状態を分け、detachと完了のtransactionを実装する。検証: 同時完了/20秒境界/明示即時backgroundで、ツール結果と後続通知が重複せず正しい一方になる。
- [x] 6.3 terminal保存と完了input作成を一transactionにする。通知IDはSandbox世代+command_idから決める。検証: terminal検出を二回処理し途中でクラッシュさせても、未通知のterminalや重複inputが残らない。
- [x] 6.4 cancel_requested、通知suppressed、取消確認を永続化し、user stop/指定cancel/resetから利用する。検証: 完了直前stop、通知保存後stop、cancel応答不明で、通常完了通知の抑止と停止成否の表示が正しい。
- [x] 6.5 大きな出力をCloud Storageへ保存し、Firestoreは参照と末尾だけ保持する。output cursor、期限、保存失敗を扱う。検証: Firestoreのdocument上限を越える出力でも状態を保存でき、出力保存失敗を成功扱いしない。
- [x] 6.6 `core/background.py` と `agent/sandbox_tools.py` の分散経路を新台帳へ接続する。既定20秒/明示trueの即時返却、bulk file guard、timeout指定、command_status/output/cancelを維持する。検証: `tests/test_background.py` と `tests/test_sandbox_tools.py` の契約をローカル・remote両adapterで確認する。

## 7. Gateway の永続受信（2完了後、D4）

- [x] 7.1 `distributed/gateway.py` にSocket envelope handlerを実装し、commit後だけSocketModeResponseを返す。Bolt自動ACKとの二重経路を作らない。検証: transport fakeでcommitより先にACKが呼ばれず、保存例外時はACKが0回になる。
- [x] 7.2 `slack_events.py` のpayload正規化とメンション/スレッド/DM判定を再利用可能に分ける。共有参加状態を使い、未参加messageが後続mentionをdedupで消さないようにする。検証: `tests/test_slack_events.py` の既存条件と両イベントの逆順到着を確認する。
- [x] 7.3 ユーザ情報取得をACK後の独立した処理へ移し、通常入力と停止候補の情報解決が互いを待たないようにする。Slack credentialはGateway/Control側に限定し、Workerは検証済みsender情報を読む。検証: users.info遅延中でもACK済み、別制御のlookupが実行可能、未知ユーザの許可扱いがない。
- [x] 7.4 sender情報解決待ちも永続状態にし、Gateway再起動後にControlが未解決分を回収できるようにする。停止の認可前にはstop_seqを進めない。検証: 未許可ユーザのSTOPでturnが止まらず、再起動後の許可ユーザのSTOPは回収される。
- [x] 7.5 Gatewayの終了時は新規Socket受信を閉じ、進行中commit/ACKの成否を確定する。検証: ACK直前/直後の停止を再現し、保存済み再送が二重inputを作らない。

## 8. Worker の入力・SDK 制御（3、5～7完了後、D6）

- [x] 8.1 `distributed/worker.py` に自分の割当watchと会話actorの生成/closeを実装する。Registryは有効なownershipの内側でのみ使用する。検証: 同会話の同時signalでEngineが一つ、別会話で異なるEngineが生成される。
- [x] 8.2 `dispatcher.py` に通常入力reader、順序付きguard task、control reader、turn taskを分離する。dispatcher内でturn/ensure/guard全体をawaitしない。検証: 永久待機するfake tool/guard/ensureの各条件で停止readerが進む。
- [x] 8.3 `ConversationOrchestrator` のfilter/guard/owner確定を永続入力へ接続し、直前5件のguard履歴を復元する。ユーザごとのrate limitもWorkerローカルだけにしない。検証: Worker交代後も履歴・開始者・制限が維持され、guard完了順で入力が逆転しない。
- [x] 8.4 `AgentSession` への投入結果を永続入力状態へ結びつける。メモリqueue待ち、append受理、次turn実行を区別し、未実行queueを失わない。検証: queueモードで未実行入力がqueuedとして残り、再割当後に再投入できる。
- [x] 8.5 Engineの新規session_idをquery前に保存する経路と、入力ID/dispatch attemptの関連を追加する。`_sent/_echoed/_closing` の意味を維持する。検証: `test_prompt_appended_after_the_cli_turn_ended_is_still_answered` と初回query直後の障害試験が通る。
- [x] 8.6 append拒否時は未送信を確認して次turnへ残し、queryの結果不明ならuncertainにする。検証: append窓終了/送信例外/正常echoの3ケースで再送の有無とturn数を確認する。
- [x] 8.7 append/interrupt/queueの設定を分散actorに接続する。interruptモードのnew_messageではバックグラウンドコマンドを取消さない。検証: append/interrupt/queueを永続入力付きActorで再実行し、共有Inboxの2 Worker競合で同じ入力を一度だけ投入する。

## 9. 停止・reset・完了通知（8完了後、D6～D7）

- [x] 9.1 認可済みcontrolからstop_seqを確定し、先行pending/guard中入力を即時barrierで無効化する。個別cancelled書込みはページングする。検証: A→STOP→BでA破棄/B保持、遅いAのguardが完了してもSDK投入されない。
- [x] 9.2 SDK interruptと永続台帳のcommand cancelを並行開始し、両結果を集計する。停止中に旧turnが新ツールを受理されないようにする。検証: 遅いinterruptとcancelの並行開始、remote unknown表示、Firestore commandページ検索とGCP index READYを確認。
- [x] 9.3 turnがidleでbackgroundのみ動く場合にもstopを処理する。担当なしの会話は制御処理用に割り当て、SDKを不要に起動しない。検証: `control_only` ownershipをSDK slotを消費せず起動し、Control actorだけが動くemulator/Worker testを追加。
- [x] 9.4 resetをstop完了→会話世代更新→session_id/history消去の順に実装する。resetより後のユーザ入力は新世代へ移し、旧system noticeは抑止する。検証: reset中に届いた新依頼が失われず、旧完了通知が新会話に注入されない。
- [x] 9.5 完了inputを `AgentSession.notify` に接続し、runningならappend、正常idleなら通知turn、中断中ならheld+機械的状態通知とする。検証: running/idle/owner解放済み/interruptedの4ケースでSDK数と返信を確認する。
- [x] 9.6 通知配送直前と出力作成時に取消・世代・停止境界を再検証する。検証: terminal保存後stop、append直後stop、reset直後通知の競合で不要な後続turn・未送信返信が抑止される。

## 10. 出力 Outbox と Sender（2、8～9完了後、D9）

- [x] 10.1 `orchestrator.emit` の分散sinkをOutboxへ変更し、sanitizer済みの本文/ツール要約/ファイルだけ配送可能にする。監査hooksは既存契約を維持する。検証: mask/block/errorが現行表示どおりで、生本文がSenderに配送されない。
- [x] 10.2 `distributed/sender.py` に会話単位の送信claim、sequence順、attempt記録、Slack ref保存を実装する。検証: Sender二つで同じready出力を同時送信せず、別会話の送信が独立する。
- [x] 10.3 `slack_output.py` / `slack_progress.py` の進捗状態を永続snapshotとSlack tsから復元する。heartbeatを集約し、終端状態を古い更新で戻さない。検証: Sender再起動後も既存進捗を更新し、turn完了後のheartbeatを無視する。
- [x] 10.4 投稿結果不明をuncertainにし、既知tsへのupdateと新規postを区別する。429はnext_attempt_atとRetry-Afterで再試行する。検証: 投稿受理後の接続切断で二重postせず、429中に他会話とSDK停止が進む。
- [x] 10.5 長文ファイルアップロード、添付の出力検査、Sandbox準備中、追加指示件数、中断とコマンド継続の表示を移植する。検証: `tests/test_slack_output.py` の既存ケースと継続中コマンド付き中断表示が通る。分散Outbox経由でサニタイズ後の長文がSlack file uploadへ進むこと、Sandbox準備中・追加指示件数・background command付きdrain中断の表示も確認。

## 11. 復旧・drain・縮退（3、6、8～10完了後、D5/D10）

- [x] 11.1 `core/lifecycle.py` の全他起動ID中断を分散経路から除去し、期限切れownershipだけをrecoveringにする。検証: Worker追加起動が正常な別Workerのturnを中断しない。
- [x] 11.2 quiesce要求とSDK停止確認応答を実装し、旧incarnationの停止証拠を保存する。Pod消失だけで確認済みにしない。検証: 通信断で旧Workerが生存するケースでは新SDKを起動せずrecovery_blockedになる。
- [x] 11.3 旧Worker停止確認後にepochを進め、未送信input/uncertain/held通知/既存commandを分類する。外部MCPの結果不明を自動再送しない。検証: 旧Worker復帰時の書込み拒否、ユーザresumeで既存command_idの状態が引き継がれる。
- [x] 11.4 `AgentSession.shutdown` の終了理由をuser_stop/resetとdrain/shutdown/lease_lostに分ける。後者でcancel_commandsを呼ばず、foreground待機をorphanedにする。検証: graceful shutdown・期限切れ・lease失効でremote cancel回数0、ユーザstopではcancelされる。
- [x] 11.5 admin CLIにWorker/全Workerのdrain開始・状態・解除を追加する。drain中は新規割当を止め、担当中の追記/停止を維持する。検証: active/reserved=0までの遷移と、backgroundのみなら解放可能なことを確認する。
- [x] 11.6 drain既定無期限と明示期限を実装する。期限後はSDK中断と保存を行い、コマンドは継続する。SIGTERM fallback25秒と保存失敗時のrecovery移行を実装する。検証: fake clockで期限前/後/保存失敗の状態がdesign D10どおりになる。
- [x] 11.7 Deployment全体drain→0担当確認→replicas変更→解除の手順とCLI出力を作る。任意Podを削除しても安全な計画的縮退を保証したと説明しない。検証: 手順の途中の新着がInboxに残り、解除後に処理されるharnessを実行する。

## 12. 起動配線とデプロイ設定（11完了後、D1/D11）

- [x] 12.1 `cli.py` / `app.py` を役割別factoryへ分け、gateway/worker/control/sandbox-serviceコマンドを追加する。不要な役割のcredentialやSDKを初期化しない。検証: `tests/test_cli.py` と役割別起動fakeで生成依存を確認する。 実装済み: `gateway` / `control` / `worker run`（`worker` は管理サブコマンドと並ぶため `run` を付けた。design の `worker --config` からの変更）/ `sandbox-service`。Workerは`worker_role.py`、ControlはallocatorにControlAuthorizer・OutboxSenderLoop・RecoveryLoopを加え、役割ごとにSlack資格情報を限定する（Workerは不要、Controlはbot tokenのみ）。`tests/test_cli.py`、`tests/test_worker_runtime.py`、`tests/integration/test_worker_role_emulator.py`（Firestore Emulator + fake SDK/Sandbox）で割当→turn→Outbox→STOP→RESET→回復を確認。
- [x] 12.2 in-process slack/Streamlit経路を既存adapterで維持し、分散データとの混在をconfigで拒否する。検証: `tests/test_streamlit_channel.py`、`test_slack_bot.py`、ローカルruntimeの回帰が通る。
- [x] 12.3 `k8s/base/` とoverlayに4役割のDeployment、Sandbox Service、KSA/RBAC、NetworkPolicy、probe、resource limitを追加する。WorkerとSandboxを別Podにし、Sandboxの既存Egressを維持する。検証: kustomize出力にSQLite volumeがなく、WorkerへSlack secretが配布されない。 `k8s/distributed/roles.yaml` にGateway/Worker/Controlの3 Deployment、KSA、NetworkPolicy、probe、resource limitを追加（Sandbox Serviceは既存）。3つとも `replicas: 0`。`tests/test_distributed_manifest.py` と `kubectl kustomize` で、SQLite/PVCが無いこと、WorkerにSlack Secretが渡らないこと、Controlはbot tokenだけであること、Worker KSAがSandbox Serviceの許可/ingressと一致することを確認。実クラスタへのapplyは未実施（Terraformの役割別IAMも `plan` のみで10 add / 0 change / 0 destroy）。
- [x] 12.4 `terraform/` にFirestoreのcreate/use-existing/import設定、API有効化、index、役割別IAMを追加する。GKE独立stateと既存DBを破壊しない。検証: terraform fmt/validateとレビュー用planで意図しないreplace/deleteがないことを記録する。
- [x] 12.5 `config/silk-agent.example.yaml`、`k8s/overlays/dev/config/`、`usecases/template/`、`usecases/bigquery-analysis/` の起動・設定例を更新する。環境fingerprint用secretを維持する。検証: config/example/usecase設定をPydanticで読み込み、distributedと各usecaseのkustomize buildが通る。
- [x] 12.6 request IDと状態を含む構造化ログ、design D11の指標、liveness/readinessを実装する。会話IDをmetric labelにせず秘密情報を出さない。検証: failpointでlease失敗/uncertain/待機数が観測でき、ログに本文・tokenが含まれない。 `distributed/metrics.py`・`health.py`・`logging.py`。各役割の `:9090` に `/healthz`（loop停止）、`/readyz`、`/metrics`。lease更新失敗・uncertain・割当待ち・稼働SDK・受信→投入/stop/配送の遅延・メモリを会話IDなしで出力。`tests/test_distributed_health.py` が各failpointの観測、label数上限、ログに本文/tokenが出ないことを確認。Firestore操作数はCloud Monitoringの指標を使う（アプリ内では数えない）。

## 13. 移行ツールと運用文書（12完了後）

- [x] 13.1 `distributed/migration.py` にSQLite/旧管理Sessionsのread-only exportとdry-run照合を実装する。件数、会話キー、SDK ID、owner、Sandbox世代、Pod/PVC UIDを出力する。検証: fixtureで欠落・重複・generation不一致を検出し、dry-runで書込みが0件となる。
- [x] 13.2 停止した旧系からFirestoreへのimportを実装する。source hash/versionで冪等にし、不一致を上書きせず止める。検証: 途中失敗後の再実行で二重記録がなく、完了済みデータを変更しない。`test_migration_import.py` とemulatorでsource hash衝突拒否、dry-run書込み0、UID/世代を復元できることを確認。
- [x] 13.3 進行中コマンドを同じIDで照合する移行処理を実装する。旧Bot停止時のcancel挙動を考慮し、無停止移行とは説明しない。検証: running/finished/cancelled/unknown/照会例外のfixtureで再startがなく、引継ぎ分類が正しい。`migration reconcile-commands` は既存tracker IDへのGETだけを行う。
- [x] 13.4 `docs/distribution-plan.md` とrunbookに構築、切替、増設、全Worker drain、縮退、recovery_blocked解除、uncertain調査を記載する。検証: コマンドの引数・resource名が実装と一致し、旧系/新系を同時起動する手順がない。 `docs/distributed-operations-runbook.md` に構築・起動順序・増設・全Worker drain・縮退・recovery_blocked解除・uncertain調査・観測・既知の制限を記載し、`distribution-plan.md` §20 に実装との対応を追記。`tests/test_distributed_runbook.py` が全 `silk-agent` コマンドを実CLIで解析し、`deployment/...` 名をmanifestと照合し、旧系/新系を同時起動する手順が無いことを確認。
- [x] 13.5 rollback手順を切替前と入力受付後に分ける。後者では古いsnapshotへ即復帰せず、最新状態の保全と整合を必要とする。管理PVCは移行成功後まで削除しない。検証: fixtureでsource保全と切替markerを確認し、READMEの既知制限を更新した。`docs/distributed-migration-runbook.md` に入力受付前/後と自動逆移行未提供の境界を記録。

## 14. 統合・障害試験とリリース条件（全実装後）

- [x] 14.1 emulator + fake SDK/Slack/Sandboxで2Gateway・2Worker・2Monitorの統合harnessを作る。同一会話同時入力、異なる会話の分散、watch再接続を検証する。完了条件: 一会話の有効SDK最大1、input/noticeの重複なし。検証: `test_two_gateways_and_two_workers_route_one_event_without_slack_or_sdk` がreceipt dedup、別会話の別Worker割当、Actor一会話一つ、watch再接続、Monitor一claimとcompletion一件を確認する。
- [x] 14.2 追記の通常時/終了直前、append拒否、interrupt/queue、guard/ensure/tool待ち中のSTOP、A→STOP→B、reset競合を統合試験する。完了条件: 既存の会話操作が保たれ、停止が長い通常処理の完了を待たない。検証: Firestore EmulatorとAgentSession/fake engineで append・append窓終了後queue・interrupt・queue、guard中/長いtool待ち中のSTOP、reset中のtool中断と後続入力rebindを確認。`tests/test_session.py::test_stop_while_sandbox_is_preparing_skips_engine_query` は準備callback停止後にengine queryが開始しないことを確認する。
- [x] 14.3 background即時/20秒/完了競合、idleとrunningへの通知、Worker解放後の完了、Monitor再起動、通知作成後STOPを統合試験する。完了条件: 正常会話には最初の返答と完了後の返答が届き、取消済み通知は届かない。検証: 即時backgroundと20秒既定値、短いforeground期限後の完了通知、detach/terminal競合の排他、idle時の新turn開始とrunning時のappend、旧Worker拒否後に新Workerが保留completionを配送すること、Monitor lease takeover/Firestore Emulatorでcompletion inputが一件だけ作られること、通知作成後STOPによる配送抑止を確認。
- [x] 14.4 ACK前後、SDK送信直後、start応答喪失、Slack受理直後、lease更新不能をfailpointで検証する。完了条件: 未送信とuncertainを区別し、unknownコマンド/投稿を無条件に再送しない。検証: GatewayのInbox commit後ACKとpersist失敗時のACKなし・重複receipt、復旧時のdispatching→uncertain、`test_lost_start_response_queries_by_id_without_resending`、SenderのSlack受理後応答喪失→uncertain/再送なし、Worker lease更新失敗→Actor closeを確認。
- [x] 14.5 Worker drain/期限切れ/SIGKILLと旧Worker復帰を検証する。完了条件: 同じSandbox generation/command_idが継続、古いepochは拒否、旧実行停止を確認できない間は新SDKが動かない。検証: drain確認後だけWorker assignmentをrelease、expired leaseはmatching SDK-closed evidenceまでblocked、evidence timeout（SIGKILL相当）中は再割当なし、確認後は同じsandbox_generation=1とcommand_idのbackground commandを引継ぎ、新epochは旧ownerの入力更新を拒否することをfake/Firestore Emulatorで確認。processへの実SIGKILLは未実施。
- [x] 14.6 `uv run pytest`、`uv run ruff check .`、`uv run ruff format --check .`、`uv run pyrefly check` とemulator suiteを実行し結果を記録する。既存テストを新仕様に合わせる変更は理由を残し、失敗の隠蔽や無関係なskipを追加しない。
- [ ] 14.7 明示した検証環境で実SDK/Sandbox/Slackの受け入れ手順を実行し、2Worker、追記、STOP、背景完了、drainを確認する。費用・外部投稿を伴う実行は適用時の承認範囲を確認して行う。完了条件: 環境/実行時刻/command_id/観測結果を記録し、未実施を成功扱いしない。
- [ ] 14.8 実負荷でSDKメモリ、受信→append/stop、完了→返信、Firestore操作量を測り初期上限を調整する。完了条件: 測定値と設定理由をrunbookへ残し、未確認の自動縮退を有効化せずにchangeの受け入れ結果をまとめる。
