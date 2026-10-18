# Tasks

実装者向け: 1 章の結果で design の D2 と D3 のどちらで進めるかが決まる。1 章が終わるまで 2 章以降に入らない。実クラスタでの操作は試験用の Sandbox だけで行い、利用中の会話の Sandbox の policy は 4 章の再適用コマンドができるまで手で変えない。

## 1. 原因の確定（D1）

- [x] 1.1 試験用の会話で Sandbox を作り、`network_probe.py dns` で名前解決が失敗することを再確認する。`resolv.conf`、NetworkPolicy の一覧、同じノードの `node-local-dns` Pod の IP を記録する。検証: 失敗の出力と記録を `docs/gke-session-sandbox/validation/` の新しい記録ファイルに残す。
- [x] 1.2 その Sandbox の `sandbox-dns-egress` の `to` に `kube-system` の `k8s-app=node-local-dns` を一時的に足し、名前解決を再試行する。検証: `dns-ok` が出るかを記録する。出たら原因は NodeLocal DNSCache で確定とし、D2 で進める。
- [x] 1.3 1.2 で失敗した場合だけ、同じノードの runc の Pod に同じ policy を付けて比較し、gVisor に固有かを切り分ける。D3 に切り替えるときは、design.md の D3 を確定値で更新してから 2 章に進む。検証: 比較結果を 1.1 の記録に追記し、design.md が更新されている。(不要: 1.2 で成功したため実施せず)
- [x] 1.4 試験用の policy の変更を元に戻し、試験用の Sandbox を削除する。検証: `kubectl get networkpolicy -n session-sandboxes` に一時的な変更が残っていない。

## 2. DNS の許可を設定可能にする（D2）

- [x] 2.1 `src/gke_session_sandbox/config.py` の `NetworkSettings` に `dns_pod_selectors` を追加し、既定を kube-dns と node-local-dns の 2 つにする。単数の `dns_pod_selector` は 1 要素として受け付け、両方の指定と空の配列はエラーにする。検証: `tests/session_sandbox/test_models_and_config.py` に既定値、単数の互換、同時指定と空配列の拒否のテストを追加して通す。
- [x] 2.2 `network.py` の `sandbox-dns-egress` を、selector ごとの `to` の要素を並べる形に変える。ポートは 53 の UDP/TCP のまま。検証: `tests/session_sandbox/test_network.py` の既存テストを更新し、両方の selector が出ること、53 以外のポートや他の namespace が許可されないことを確認する。
- [x] 2.3 （D3 を選んだ場合だけ）`templates/sandbox.yaml` に `dnsPolicy: None` と `dnsConfig` を追加し、nameserver を設定で変えられるようにする。検証: `tests/session_sandbox/test_templates.py` で生成された Pod spec の `dnsConfig` を確認する。D2 だけで済んだ場合は「不要」と書いて完了にする。(不要: D2 だけで解決したため実施せず)

## 3. policy の再適用（D4）

- [x] 3.1 `backend.py` に、policy を作成するか、所有者（ownerReference の UID と、会話キーの hash・世代のラベル）を確認してから spec を置き換える処理と、不要になった policy を所有者を確認してから削除する処理を追加する。NetworkPolicy と FQDNNetworkPolicy の両方に対応する。検証: `tests/session_sandbox/test_backend.py` で、作成、内容の置き換え、所有者が違う場合の拒否、不要な FQDN policy の削除をテストする。
- [x] 3.2 `manager.py` の作成と再開（`_resume_locked`）で、現在の設定から作った policy を 3.1 の処理で適用する。検証: `tests/session_sandbox/test_manager.py` で、古い DNS policy を持つ一時停止中の Sandbox を再開すると policy が新しい内容になることをテストする。
- [x] 3.3 会話キーを指定する再適用と、全 Sandbox への再適用の CLI を追加する。会話の lease を取ってから適用し、Pod は再起動しない。結果は会話ごとに 1 行で表示し、失敗が 1 件でもあれば非 0 で終了する。検証: CLI のテストで、lease を取れない会話がスキップとして報告され、所有者違いが失敗になることを確認する。

## 4. 検証コマンド（D5）

- [x] 4.1 `network_probe.py` に、`resolv.conf` の nameserver の表示と、許可宛先・遮断宛先をまとめて確認するモードを追加する。検証: 単体テストで、遮断されるべき宛先へつながった場合に失敗になることを確認する。
- [x] 4.2 指定した Sandbox の中で 4.1 を実行する CLI を追加する。作業領域には書き込まない。検証: テストで、実行するコマンドが `/workspace` に書き込まないこと、失敗時に非 0 で終了することを確認する。

## 5. Terraform（D6）

- [x] 5.1 `terraform/gke/variables.tf` に `enable_node_local_dns_cache`（既定 `true`）を追加し、`main.tf` の `addons_config` に `dns_cache_config` を追加する。`outputs.tf` に有効・無効を出力する。検証: `terraform -chdir=terraform/gke validate` が成功し、`terraform plan` で DNS キャッシュに差分が出ない。

## 6. 実クラスタでの確認とドキュメント

- [x] 6.1 Sandbox Service のイメージを作り直して配置し、全 Sandbox への再適用を実行する。検証: 再適用の出力がすべて成功で、稼働中の Sandbox の Pod の再起動回数が増えていない。
- [x] 6.2 新しく作った Sandbox と、一時停止から再開した Sandbox の両方で 4.2 の検証を実行する。検証: どちらも名前解決が成功し、許可リストが空なので外部の TCP 443 は遮断される。結果を 1.1 の記録に追記する。
- [x] 6.3 試験用の設定で `allowed_fqdns` に 1 件（例 `github.com` の TCP 443）を足した Sandbox を作り、許可宛先への接続と許可外の遮断を確認する。検証: 4.2 の出力を記録に追記し、試験用の設定を戻す。
- [x] 6.4 `docs/gke-session-sandbox/README.md` の DNS の前提、`preflight-and-operations.md` のトラブルシュート（NodeLocal DNSCache と Dataplane V2 での評価のされ方、ラベルの確認方法、再適用と検証のコマンド）、`validation/acceptance-matrix.md` の DNS の行を更新する。検証: 記載したコマンドを実行して同じ結果が得られる。
