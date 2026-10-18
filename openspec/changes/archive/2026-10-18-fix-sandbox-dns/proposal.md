# Proposal

## Why

現在の GKE（`silk-agent-gke`）では Sandbox Pod 内の名前解決が失敗する。このため、FQDN の許可リスト（`network.allowed_fqdns`）と、起動時スクリプトでのコード取得（`git clone`）が動かない。2026-10-14 にクラスタの NodeLocal DNSCache（`addonsConfig.dnsCacheConfig.enabled: true`、`node-local-dns` DaemonSet）が Terraform の外で有効になった。Dataplane V2 では kube-dns の ClusterIP への問い合わせがノード上の `node-local-dns` Pod（`k8s-app=node-local-dns`、hostNetwork ではない通常の Pod IP）へ転送される。一方、`sandbox-dns-egress` は `k8s-app=kube-dns` の Pod だけを許可している。これが最有力の原因で、ClusterIP への `ipBlock` を足しても効かなかった観測とも合う（Dataplane V2 は転送後の宛先 Pod で NetworkPolicy を評価する）。

## What Changes

- 原因を実機で確定する。試験用 Sandbox の DNS egress に `node-local-dns` Pod を足して名前解決できるかを確認し、だめなら gVisor 側の切り分けへ進む。
- Sandbox の DNS egress で、クラスタ DNS の応答元を複数許可できるようにする。既定では kube-dns と NodeLocal DNSCache の両方を許可する。宛先ポートは引き続き 53（UDP/TCP）に限り、他の通信は広げない。
- Sandbox の作成時だけでなく再開時にも、その会話の NetworkPolicy / FQDNNetworkPolicy を現在の設定へ合わせる。既存の一時停止中・稼働中の Sandbox も、再開または明示的な再適用で修正済みの policy になる。
- Sandbox 内の名前解決と FQDN 許可・遮断を運用者が確認できるコマンドと手順を用意する。
- Terraform（`terraform/gke`）で NodeLocal DNSCache の有効・無効を明示的に管理し、現在の構成とのずれを解消する。

## Capabilities

### New Capabilities

なし。

### Modified Capabilities

- `sandbox-cloud-access`: 既定の通信制限の下でもクラスタ DNS で名前解決できること、通信設定の変更が既存 Sandbox に反映されること、名前解決と許可・遮断を検証できることを追加する。
- `gke-infrastructure`: クラスタ DNS（NodeLocal DNSCache）の構成を Terraform で宣言し、構成のずれを残さないことを追加する。

## Impact

- `src/gke_session_sandbox/network.py`（DNS egress の生成）、`config.py`（`NetworkSettings`）、`manager.py` と `backend.py`（policy の再適用）、`network_probe.py` / `cli.py`（検証コマンド）と、`tests/session_sandbox/` の関連テスト。
- `terraform/gke/main.tf` と `variables.tf`（`dns_cache_config`）。
- `docs/gke-session-sandbox/`（DNS の前提、検証記録、トラブルシュート）。
- 起動時スクリプトの `git clone` と、私有リポジトリの認証は別の課題とし、この change では扱わない。この change は、それらの前提になる名前解決を直す。
