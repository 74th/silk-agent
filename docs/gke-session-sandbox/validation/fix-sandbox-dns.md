# Sandbox DNS 障害の原因確定と修正の検証

## 原因の確定（2026-10-18、`silk-agent-gke`）

クラスタは `addonsConfig.dnsCacheConfig.enabled: true`（NodeLocal DNSCache）、`datapathProvider: ADVANCED_DATAPATH`。`node-local-dns` Pod は hostNetwork ではなく Pod IP を持つ（gVisor ノードの Pod は `192.168.40.2`、ラベル `k8s-app=node-local-dns`）。

利用中の会話の Sandbox には触れず、gVisor ノード上に専用ラベル（`dns-probe-test=true`）の試験用 Pod と、`sandbox-deny` / `sandbox-dns-egress` と同じ内容の NetworkPolicy を作って切り分けた（Pod と policy は検証後に削除済み）。

| 手順 | 結果 |
| --- | --- |
| `/etc/resolv.conf` | `nameserver 192.168.64.10`（kube-dns の ClusterIP）、`ndots:5` |
| DNS egress が `k8s-app=kube-dns` のみ | `socket.getaddrinfo("github.com")` が `[Errno -3] Temporary failure in name resolution` |
| DNS egress に `k8s-app=node-local-dns` を追加 | `dns-ok ['140.82.114.3']` |

policy に `node-local-dns` を足しただけで名前解決が成功したため、原因は「NodeLocal DNSCache への転送先 Pod が `sandbox-dns-egress` で許可されていないこと」で確定。design の D2 で進め、D3（gVisor 固有の回避）は不要。runc との比較（tasks 1.3）も不要。

## 修正後の実機確認（2026-10-18）

Sandbox Service のイメージを更新して配置し（`k8s/distributed/kustomization.yaml` のダイジェストも更新）、全会話に `silk-agent sandbox-network reapply --all` を実行した。

- 再適用: 一時停止中の 24 会話が `applied`、終了済みは `skipped`。利用中の会話は初回に `session lease is held` で `skipped` になり、再実行で `applied`。再実行後の `sandbox-dns-egress` の `to` は `kube-dns` と `node-local-dns` の 2 件。稼働中の Pod の再起動回数は 0 のまま。
- 新しく作った Sandbox（`silk-agent sandbox-network verify --conversation fixdns:probe-new --ephemeral`、終了コード 0）:

  ```
  nameservers ['192.168.64.10']
  dns-ok name=kubernetes.default.svc.cluster.local addresses=['192.168.64.1']
  dns-ok name=example.com addresses=['104.20.23.154', '172.66.147.243']
  deny-ok target=example.com:443 blocked=timed out
  ```

- 一時停止から再開した Sandbox（既存の `loadtest:05`、同じ検証、終了コード 0）: 上と同じ出力。この時点の検証コマンドは通常のコマンド経路で会話を暗黙に再開していたが、レビュー指摘を受けて、既存の会話を再開せず、READY でなければ失敗する診断用の経路に変更した。許可リストが空なので外部の TCP 443 は遮断される。
- `allowed_fqdns` に `github.com` の TCP 443 を足した試験用の設定（Pod 内の `/tmp` の設定ファイルを `-c` で指定、検証後に削除）で、試験用の会話に対して `verify --allow github.com:443 --deny example.com:443 --name github.com`（終了コード 0）:

  ```
  dns-ok name=github.com addresses=['140.82.112.3']
  allow-ok target=github.com:443
  deny-ok target=example.com:443 blocked=timed out
  ```

試験用の会話の Sandbox は `--ephemeral` で終了させ、NetworkPolicy / FQDNNetworkPolicy も残っていないことを確認した。

## 実装中に分かったこと

- 分散構成の Sandbox Service は Firestore に session を持つため、`gke-session-sandbox reapply-network` / `verify-network`（SQLite の設定向け）では実行できない。同じ処理を `silk-agent sandbox-network` として足し、Sandbox Service Pod の中で実行する。
- sandboxd は、`cwd=/tmp` を指定したコマンドを `fork/exec ...: no such file or directory` で起動できない。検証コマンドは cwd を指定せず、`bash -lc` 経由で `python3 -c` を実行する（ディスクへ書き込まない）。
- `terraform plan` は DNS キャッシュに差分を出さない（出力 `node_local_dns_cache_enabled` の追加のみ）。

## レビュー指摘への対応

- 再開が `RESUMING` で止まった場合の復旧（`ensure()` と reconciler）でも、Sandbox を起動する前に現在の policy を適用する。適用に失敗し続ける間は `RESUMING` のままで、古い許可のまま READY にならない。
- 検証コマンドは `SandboxManager.run_diagnostic_command` を使う。READY でない会話は再開せずに失敗し、リポジトリには何も記録しない。

## 再レビューへの対応

診断を tracker 経由で実行すると、journal と出力が `/workspace/.session-runtime` に保存され、診断中に reconciler が走ると tracker の active command によってアイドル時刻が更新されていた。診断を Kubernetes の pod exec（`CoreSandboxBackend.exec_diagnostic`）へ移し、tracker を通らないようにした。Sandbox Service の ServiceAccount だけに `pods/exec` の get・create を付与する Role（`sandbox-diagnostics`）を追加した。
