# sandboxd実行経路の確認

## 固定した成果物

- Agent Sandbox: v1.0.6、tag commit `cdeac1ba4f4d29a20c8426a76cff25fcf326d5ea`
- 公式ソースtarball SHA256: `8a262b9773036b5a253a0c6d304461cc6bd7e6a68fb0ffb2ad77073d9f1b4a67`
- sandboxd image: `us-central1-docker.pkg.dev/nnyn-dev/research-gke-agent-sandbox-with-sandboxd-images/sandboxd@sha256:28ce219266fe3586b489e1edf9355722eb2951bef8c73db7ff06ec5541ae5d4a`
- Python runner image: `us-central1-docker.pkg.dev/nnyn-dev/research-gke-agent-sandbox-with-sandboxd-images/runtime-smoke@sha256:28c5fa74e6e5545ab8ca83361ec4cd563d064270e722ea5eda58c18a05e40dcd`
- Python session runtime image: `us-central1-docker.pkg.dev/nnyn-dev/research-gke-agent-sandbox-with-sandboxd-images/session-runtime@sha256:11eaa6651ef23b9a7e19a1138277a8e6b30b2400df461530a8016e043bc1baf3`

session runtime imageはPython製のtracker / supervisorを含み、公式v1.0.6のsandboxdだけを上流ソースからGoでビルドする。runnerもPythonで実装し、同じ`ProcessService` protobufを使う。Artifact Registryへのビルドとpushは`build-session-runtime-image.sh`、`build-runtime-smoke-runner.sh`で再実行できる。

## GKEで確認したこと

対象は`gke_research-gke-agent-sandbox-with-sandboxd_us-central1`。Sandbox `session-sandboxes/sandboxd-smoke`はgVisor RuntimeClass、`session-workspace-rwo`の1Gi PVC、headless Serviceを使ってReadyになった。

管理runnerはService DNS `sandboxd-smoke.session-sandboxes.svc.cluster.local`とPod IP `192.168.35.4`の両方へ接続した。それぞれについて、REST `PUT /v1/files/management-runner.txt`がHTTP 204で成功し、その後gRPC `process.v1.ProcessService/Execute`で同じPodの`/workspace/management-runner.txt`を読み、書き込んだ内容と一致することを確認した。両経路の実行結果は次のとおり。

```text
rest-write-and-grpc-read-ok target=sandboxd-smoke.session-sandboxes.svc.cluster.local
rest-write-and-grpc-read-ok target=192.168.35.4
gvisor-process-identity-and-stream-disconnect-ok target=sandboxd-smoke.session-sandboxes.svc.cluster.local
```

同じPod内で、固有env markerからprocessを探し、`/proc/<pid>/stat`のPID / PGID / starttimeと`boot_id`を取得できた。PGIDは独立process groupとしてPIDと一致した。同じPID・PGIDでもstarttimeが違うidentityを別processとして判定する比較も確認した（実OSで同一PIDが再利用されるまで回す負荷試験ではない）。gVisorでも`/proc`照合に必要な値は読める。

検証runnerがgRPC `Start` streamを切るとprocessが5秒以内に消えることも確認した。これはsandboxd v1.0.6がstream context cancellation時にprocess groupへSIGKILLを送り、残存descendantも掃除する挙動と一致する。採用する契約は、呼出元が切断してもtrackerが生存する間はcommandを追跡し続け、tracker自体の停止でsandboxd接続が失われたときはcommand終了を許容するものとする。再起動後、process group不在を確認できた場合はterminalのfinishedとして実行中件数から除き、exit code / 最終出力が得られない結果は不明・出力不完全と記録する。groupの生死を確認できなければunknownのまま停止を保留する。PID照合は終了済みgroupの誤採用防止に使い、切断を越えてprocessを維持する目的には使わない。

テスト用NetworkPolicyはsandboxd-smoke Podに対して、`session-sandbox-system` namespaceの`smoke-runner`だけにTCP 8080/9090を許可する。これはTask 4.6の本番用default-deny policyとは別のfixtureである。

CommandTrackerの基礎imageは`build-session-runtime-image.sh`で作る。container PID 1のPython supervisorが公式sandboxdとPython trackerを同じcontainer内で起動し、sandboxdのREST healthを確認してからtrackerを起動する。trackerはsandboxd gRPCへの接続を確認し、`/healthz`にsandboxd接続状態とworkspaceへ保存した起動世代を返す。trackerが停止した場合はsupervisorがsandboxdも停止するため、保持中のStart streamは切断されて実行中groupが終了する。sandboxd自体が停止した場合もtrackerを一緒に終了する。SIGTERMではtrackerを先に止め、次にsandboxdを止める。

```bash
GKE_PROJECT_ID=... GKE_LOCATION=... SANDBOXD_IMAGE_REPOSITORY=... \
  scripts/build-session-runtime-image.sh
```

tracker起動世代は`/workspace/.session-runtime/tracker-generation`で単調増加する。generation stateが破損している場合は0へ戻して起動せず、明示エラーで止まる。command受付はID・引数・env・cwdを`/workspace/.session-runtime/commands.json`へatomic renameで永続化してから応答する。同じIDと同じ要求は保存済みrecordを返し、異なる要求でのID再利用は409で拒否する。HTTP応答にenv値を含めない。`POST /v1/commands`はsandboxd `Start` streamを起動し、init / stdout / stderr / exitをjournalへ保存する。`GET /v1/commands/{id}`で状態・終了要約を取得し、`GET /v1/commands/{id}/output?stream=stdout&cursor=0&limit=65536`で出力をbyte cursor付きpageとして読む。出力本体は`/workspace/.session-runtime/command-output/`へ保存する。受付gateはjournalと同じatomic fileに保存し、閉じている間は新規IDを423で拒否する。stdin / EOF、signal、PTY resize、timeout、cancelもtracker経由で操作する。

2026-10-10のgVisor実GKE runでは`/healthz`が`{"status":"ok","tracker_generation":1,"sandboxd":"connected"}`を返した。trackerへSIGTERMを送り、supervisorがsandboxdも停止してcontainerが再起動することを確認した。PVC上のgeneration fileを維持したまま新trackerは`tracker_generation: 2`でsandboxdへ再接続し、Pod Readyへ戻った。Command ID受付では新規202、同一要求200、異なる要求409を確認した。実行commandのstdout / stderr、非ゼロexit code 7、重複要求でも実行が一度だけであることを確認した。無出力のsleep commandはrunningとして計上され、並列実行はruntime unit testで確認した。stdin / EOF、PTY resize、signal、timeout、cancelもgVisor Podで確認した。受付gateを閉じると未登録IDは423で拒否され、実行中IDはactiveとして計上され、既存IDの同一要求は200で取得できた。gateとjournalは永続化され、tracker再起動後も完了済み結果を照会できた。最新のSessionManager runではPython command / file clientを管理namespace内のJobから使い、受付後にclientを閉じたcommandを別clientから同じIDで照会し、stdout全体をcursorで再取得できること、buffered read / write、stream read / write、list / stat / exists、再帰delete、workspace外へ向くsymlinkの拒否を確認した。sandbox UIDは`fe4263e0-8975-4730-ae1c-8dd86d25963d`だった。

`SessionHandle.commands`は各新規commandの前にsessionをensureし、現在のgenerationのService DNS endpointを引き直す。結果待ちの通信が切れたときは同じcommandを再送せず、`ExecutionOutcomeUnknown`にcommand IDを含めて返す。呼出側はそのIDで`get()`または`wait()`を使って状態を再照会できる。実GKEでは管理namespaceからこのPython clientでcommandを実行し、stdoutとexit codeを取得した。Pod内のdetached processとしてcommandから切り離す実行形態はサポートしない。tracker停止でsandboxd streamが切れた場合は子process groupも終了する。

## Python SDKとの境界

公式Python SDK v1.0.6には`SandboxdInClusterConnectionConfig`があり、Pod IP経由またはService DNS経由でREST 8080とgRPC 9090へ接続できる。接続を閉じる`close_connection()`はローカル接続資源を閉じ、Kubernetes Sandboxは残す。`terminate()`は接続を閉じてclaimを削除する。SDK clientの`close()`はclientの接続とKubernetes API clientを閉じる。

ただし、高水準の`SandboxClient.get_sandbox()` / `AsyncSandboxClient.get_sandbox()`は既存SandboxClaimを引数に取り、そのstatusからcore Sandbox名を解決する。この変更はWarmPool/Claimを使わずcore Sandboxを直接管理するため、SDKの高水準lifecycle handleを使うと目的と異なるClaim依存を追加することになる。SDKの非公開constructorや内部helperも使わない。

そのため管理面はcore SandboxをKubernetes APIで管理し、データ面は公式sandboxdのREST/gRPC公開protocolをadapterから利用する。SDKの接続先選択と切断・terminateの意味はv1.0.6の公開README / API docで照合し、同じPodへPod IPとService DNS経由でprotocol疎通を実測した。SDK clientそのものからcore Sandboxへattachする公開APIはv1.0.6にないため、この組み合わせを使わない理由として記録する。

## 再実行

```bash
GKE_PROJECT_ID=... GKE_LOCATION=... SANDBOXD_IMAGE_REPOSITORY=... \
  scripts/build-sandboxd-image.sh
GKE_PROJECT_ID=... GKE_LOCATION=... SANDBOXD_IMAGE_REPOSITORY=... \
  scripts/build-runtime-smoke-runner.sh
GKE_PROJECT_ID=... GKE_LOCATION=... GKE_CLUSTER_NAME=... \
  SANDBOXD_IMAGE=... RUNTIME_SMOKE_IMAGE=... scripts/validate-sandboxd-gke.sh
```

検証は既存`session-sandboxes/sandboxd-smoke`を再利用する。Jobは完了後5分でTTL cleanupされる。Sandbox、PVC、NetworkPolicyはTask 2.6以降のlifecycle確認用に残す。
