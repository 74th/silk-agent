# Design

## Context

- Sandbox Pod は `templates/sandbox.yaml` から作られ、`runtimeClassName: gvisor`、`dnsPolicy` は未指定（`ClusterFirst`）。`resolv.conf` の nameserver は kube-dns の ClusterIP `192.168.64.10`。
- 会話ごとの policy は `src/gke_session_sandbox/network.py` の `build_session_network_policies` が生成する。`sandbox-dns-egress` は `network.dns_namespace`（既定 `kube-system`）の `network.dns_pod_selector`（既定 `k8s-app=kube-dns`）の Pod への 53 番だけを許可する。
- policy は Sandbox の作成時だけ作られる（`manager.py:488-497` → `backend.create_with_policies`）。`backend.create_policies` は 409 のとき既存の policy を読んで所有者を確認するだけで、内容を更新しない。再開（`manager.resume`）では policy に触れない。
- 実クラスタ（2026-10-18 確認）:
  - `gcloud container clusters describe`: `addonsConfig.dnsCacheConfig.enabled: true`、`datapathProvider: ADVANCED_DATAPATH`。
  - `terraform/gke/main.tf` の `addons_config` には `dns_cache_config` がなく、NodeLocal DNSCache は Terraform の外で有効になっている（DaemonSet の作成は 2026-10-14T00:31:27Z）。
  - `node-local-dns` Pod は hostNetwork ではなく、各ノードで Pod IP（例 `192.168.40.2`、`192.168.34.17`）を持ち、ラベルは `k8s-app=node-local-dns`。
- 2026-10-10 の検証（旧クラスタ、NodeLocal DNSCache なし）では、同じ policy で名前解決と FQDN の許可・遮断が成功していた（`docs/gke-session-sandbox/validation/python-sandbox-backend.md:15`）。
- 名前解決の確認用に `network_probe.py` の `dns` モードがある。

## Goals / Non-Goals

**Goals:**
- 原因を実機で確定し、NodeLocal DNSCache の有無にかかわらず Sandbox が名前解決できるようにする。
- 既存の Sandbox（一時停止中を含む）を作り直さずに直す。
- クラスタの DNS 構成を Terraform に戻し、構成のずれをなくす。

**Non-Goals:**
- 起動時スクリプトでのコード取得、私有リポジトリの認証情報の受け渡し（別の change）。
- `allowed_fqdns` の既定値の変更や、ユースケースごとの許可リストの追加。
- Sandbox 以外（Gateway / Worker / Control）の NetworkPolicy の変更。

## Decisions

### D1. 最初に原因を確定し、結果で D2 と D3 を選ぶ

試験用 Sandbox の `sandbox-dns-egress` に `k8s-app=node-local-dns` の Pod を一時的に足し、`getaddrinfo` が成功するかを確認する。

- 成功する: 原因は NodeLocal DNSCache への転送が policy で拒否されていること。D2 で直す（本命）。
- 失敗する: gVisor の netstack 側の問題の可能性がある。同じノード上の runc の Pod で同じ policy を試し、gVisor に固有かを切り分けてから D3 に進む。

根拠: Dataplane V2 は Service の転送後の宛先 Pod で NetworkPolicy を評価する。そのため、ClusterIP への `ipBlock` が効かなかったことと、kube-dns の Pod IP へは直接届くことが、どちらも説明できる。旧クラスタとの違いは NodeLocal DNSCache の有無だけである。

### D2. DNS の応答元を複数指定できるようにし、既定で NodeLocal DNSCache を含める

`NetworkSettings` に `dns_pod_selectors`（ラベル selector の配列）を追加する。既定値は `[{"k8s-app": "kube-dns"}, {"k8s-app": "node-local-dns"}]`。`sandbox-dns-egress` は、`dns_namespace` の中で各 selector に一致する Pod への 53 番（UDP/TCP）を、1 つの policy の `to` に並べて許可する。

- 互換: 既存の `dns_pod_selector`（単数）は、その 1 つだけを使う指定として受け付ける。単数と複数を同時に指定したら設定エラーにする。現在の設定ファイルはどちらも上書きしていないので、配置の変更は不要。
- 却下した案:
  - `dnsPolicy: None` と `dnsConfig` で kube-dns の Pod IP を直接指定する案: Pod IP は変わるので保守できない。
  - NodeLocal DNSCache を無効にする案: 原因は避けられるが、クラスタ全体の DNS 性能を落とし、再び有効にすると同じ障害が起きる。
  - ClusterIP や `169.254.20.10` を `ipBlock` で許可する案: Dataplane V2 では転送後の Pod で評価されるため効かない（実測済み）。

### D3. D1 で gVisor が原因と分かった場合の代替

NodeLocal DNSCache のリンクローカル IP `169.254.20.10` を、`template_overrides` ではなく Sandbox のテンプレートの `dnsConfig`（`dnsPolicy: None`、nameserver `169.254.20.10`、`search` と `ndots` は ClusterFirst と同じ値）で指定する。そのうえで、`node-local-dns` の Pod を D2 と同じく許可する。この場合は D2 の設定項目に `dns_nameserver` を加える。D3 を使うときは、tasks の 2 章の前に design のこの節を確定値で更新する。

### D4. 作成時と再開時に policy を現在の設定へ合わせる

`backend` に、policy を作成するか、既存の policy の所有者（ownerReference の UID と、会話キーの hash・世代のラベル）を確認してから spec を置き換える処理を追加する。`manager` は Sandbox の作成時と再開時にこれを呼ぶ。現在の設定で不要になった policy（例: `allowed_fqdns` を空にした後の FQDN policy）は、同じ所有者確認の後に削除する。

- 稼働中の Sandbox 向けに、`gke-session-sandbox`（または `silk-agent sandbox-service` 経由）の CLI に、会話キーを指定して再適用するコマンドと、全 Sandbox に再適用するコマンドを追加する。再適用は会話の lease を取ってから行い、ライフサイクル処理と競合しないようにする。
- 却下した案: Sandbox を削除して作り直す案は、作業領域の PVC が残っても Pod が再起動し、実行中のコマンドを止めるので採らない。

### D5. 検証コマンド

`network_probe.py` を Sandbox の中で実行し、次を順に確認して結果を 1 行ずつ表示するコマンドを CLI に追加する: `resolv.conf` の nameserver、名前解決（クラスタ内の名前と外部の名前）、指定した許可宛先への TCP 接続、指定した許可外の宛先が遮断されること。失敗したら非 0 で終了する。作業領域には書き込まない（一時ファイルは `/tmp`）。

### D6. Terraform で NodeLocal DNSCache を宣言する

`terraform/gke/variables.tf` に `enable_node_local_dns_cache`（既定 `true`、現在のクラスタに合わせる）を追加し、`addons_config.dns_cache_config.enabled` に渡す。既定値を `true` にすることで、適用しても今のクラスタは変わらない。

## Risks / Trade-offs

- [D1 で原因が NodeLocal DNSCache ではなかった] → D3 に切り替える。tasks の 1 章を終えるまで、2 章以降の実装に入らない。
- [`node-local-dns` のラベルは GKE 側の実装なので変わる可能性がある] → 設定で変えられるようにし、検証コマンドで定期的に確認できるようにする。ドキュメントにラベルの確認方法を書く。
- [policy の置き換え中に、一時的に許可が狭くなる] → DNS 用の policy を置き換える前に、新しい許可を含む policy を作る順序にはしない（名前が同じため）。代わりに `replace` 1 回で spec を差し替え、Kubernetes 側で一瞬で切り替わるようにする。`sandbox-deny` には触れない。
- [再適用で他の会話の policy を書き換える] → 所有者の UID とラベルの両方を確認し、一致しなければ変更せずにエラーにする（spec の「所有者が一致しない policy」）。
- [NodeLocal DNSCache を無効にする入力を誤って適用する] → 既定値を現状の `true` にし、plan で差分が出ないことを tasks で確認する。

## Migration Plan

1. 実装後、Sandbox Service のイメージを更新して配置する。新しく作る Sandbox と、再開する Sandbox は自動で直る。
2. 稼働中の Sandbox には、全 Sandbox への再適用コマンドを 1 回実行する。
3. 検証コマンドで名前解決を確認した後、FQDN の許可リストを試験用の設定で 1 件試す。
4. 戻す場合は、前のイメージに戻す。D2 の既定値は許可を足すだけなので、戻しても既存の policy はそのまま動く（名前解決は再び失敗する）。

## Open Questions

- NodeLocal DNSCache を 2026-10-14 に有効にしたのは誰か（GKE の自動の変更か、手動の操作か）。D6 の既定値には影響しないが、ドキュメントの記録のために確認する。
