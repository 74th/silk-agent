# Preflightと運用

SessionManagerを使う前に、接続先contextと必要なKubernetes機能・権限を確認する。

```bash
kubectl config current-context
uv run gke-session-sandbox preflight --config examples/sandbox-config.yaml
```

`--context`を指定するとkubeconfig内のcontextを選べる。in-cluster実行ではPodのServiceAccountを使う。preflightはSandbox CRDの`v1beta1`、設定したFQDN policy用CRD、`gvisor` RuntimeClass、workspace StorageClassとprovisioner、管理namespace / sandbox namespace、現在の接続主体の必要RBACを確認する。不足項目は一括して表示し、作成前に非ゼロ終了する。FQDN allowを設定しない場合、FQDN policy用CRDとそのRBACは必須ではない。

初回は専用検証clusterの正しいcontextを確認してからplatform manifestを適用する。controller、Bot / Sandbox namespace、Sandbox管理Role、RWO workspace StorageClassの適用と確認は[ゾーンクラスタrunbook](zonal-cluster-runbook.md)を参照。

```bash
kubectl kustomize k8s/platform | kubectl apply -f -
kubectl -n agent-sandbox-system rollout status deployment/agent-sandbox-controller --timeout=300s
kubectl get crd sandboxes.agents.x-k8s.io
kubectl get runtimeclass gvisor
```

workerにはsandbox resourceを作るnamespaceとsession recordを置くnamespaceだけにRoleBindingを作る。GKEの`FQDNNetworkPolicy`を使う場合は`networking.gke.io/fqdnnetworkpolicies`権限も必要になる。Pod自体のServiceAccount token mountは無効にし、管理workerの認証情報をsandboxへ渡さない。

sessionのresourceを手で調査する場合、同じnamespace内のsession labelでSandbox、Pod、PVC、NetworkPolicy、FQDNNetworkPolicyを探す。所有判定には名前やlabelだけを使わず、Sandbox UIDとPVCのcontroller ownerReference UIDを照合する。置き換わったUIDや別sessionのresourceは削除しない。Sandbox削除後にPVC / PVのGC完了を確認する。

```bash
kubectl get sandbox,pod,pvc,networkpolicy,fqdnnetworkpolicy -n session-sandboxes \
  -l session-sandbox.example.com/managed=true -o wide
kubectl describe sandbox -n session-sandboxes <sandbox-name>
kubectl get sandbox -n session-sandboxes <sandbox-name> -o yaml
```

`describe`ではReady condition、reason、eventを確認する。YAMLでは`metadata.uid`、session / generation label、ownerReference UID、Pod template、operatingModeを確認する。作成待ちで止まった場合はPod event、PVC event、StorageClass provisioner、runtimeClassを確認する。FQDN通信の問題は、policy selectorがPod labelと一致するか、namespace内により広いNetworkPolicyの許可がないか、DNSから得たIPが許可domainと共有されていないかを確認する。GKE FQDN policyはIP / port単位で適用され、同じIPを共有する別domainは区別できない。

sandbox用resourceはSandbox UIDのownerReferenceを付け、generation単位で分離する。専用workspace PVCだけを削除対象とし、共有readOnly fixtureは削除しない。通常運用での終了は`SandboxManager.terminate()`を使う。手動cleanupではUIDとownerReferenceを照合してから削除し、共有volumeや別generationを巻き込まない。

## DNSの確認とnetwork policyの再適用

Sandbox内で名前解決が`Temporary failure in name resolution`になる場合は、`/etc/resolv.conf`のnameserver（kube-dnsのClusterIP）が使えているかを先に見る。GKE Dataplane V2はNetworkPolicyを、Serviceの転送後のPodで評価する。NodeLocal DNSCache（`addonsConfig.dnsCacheConfig.enabled`）が有効だと転送先は同じノードの`node-local-dns` Podになるので、`sandbox-dns-egress`が`k8s-app=kube-dns`だけを許可していると名前解決できない（2026-10-18に実機で確認、`validation/fix-sandbox-dns.md`）。ClusterIPや`169.254.20.10`への`ipBlock`は転送後のPodで評価されるため効かない。

```bash
# クラスタのNodeLocal DNSCacheとlabelを確認する（labelはGKE側の実装なので変わりうる）
gcloud container clusters describe silk-agent-gke --zone us-central1-a --project nnyn-dev --format='value(addonsConfig.dnsCacheConfig.enabled)'
kubectl -n kube-system get pods -l k8s-app=node-local-dns -o wide --show-labels
# 会話のpolicyのDNS許可先を確認する
kubectl -n session-sandboxes get networkpolicy -l session-sandbox.example.com/managed=true \
  -o custom-columns=N:.metadata.name,SEL:.spec.egress[*].to[*].podSelector.matchLabels | grep dns-egress
```

許可先のlabelが変わった場合は`network.dns_pod_selectors`を設定で直す。設定を変えても、既存のSandboxのpolicyは自動では変わらない。再開時に作り直した内容へ置き換わる。稼働中や一時停止中のSandboxには、Pod（Sandbox）を再起動せずに再適用できる。

```bash
# 分散構成（Sandbox ServiceはFirestoreにsessionを持つので、Sandbox Service Podの中で実行する）
SVC="kubectl -n session-sandbox-system exec deploy/silk-agent-sandbox-service --"
$SVC silk-agent sandbox-network reapply --all                          # 全会話
$SVC silk-agent sandbox-network reapply --conversation slack:T123:C123.456
# SQLite構成のgke-session-sandboxでは: gke-session-sandbox reapply-network --config <config> --all
```

結果は会話ごとに1行（`applied` / `skipped` / `failed`）で表示する。lifecycle処理がleaseを持つ会話は`skipped`になるので、少し待って再実行する。policyの所有者（ownerReferenceのUIDとlabel）が一致しない場合は変更せず`failed`にして、終了コードは1になる。終了済みで残っていないSandboxは`skipped`になる。

Sandbox内からの確認は次のコマンドで行う（作業領域へ書き込まない）。名前解決の成否とnameserver、`--allow`の宛先への接続、`--deny`の宛先の遮断を1行ずつ表示し、1つでも期待と違えば終了コードは1になる。`--deny`を省略すると`example.com:443`を遮断対象にする。既存の会話は再開も再作成もせず、一時停止中などREADYでなければ失敗にする（会話の状態、アイドル判定、保持期限は変えない。一時停止中の会話は、再開時に最新のpolicyが適用される）。コマンドはKubernetesのpod exec APIで実行し、Pod内のcommand trackerを通らないので、`/workspace/.session-runtime`のjournalや出力保存は作られず、trackerのactive commandとして数えられない。そのため実行するPrincipalにはSandbox Podへの`pods/exec`のget・create権限（WebSocketのexecはgetも使う）が要る（分散構成では`k8s/distributed/sandbox-service.yaml`の`sandbox-diagnostics` Roleで、Sandbox ServiceのServiceAccountだけに付与している）。会話がなければ`--ephemeral`で試験用に作り、確認後に終了する。

```bash
$SVC silk-agent sandbox-network verify --conversation fixdns:probe --ephemeral
$SVC silk-agent sandbox-network verify --conversation slack:T123:C123.456 --allow github.com:443 --deny example.com:443
```

`allowed_fqdns`の確認は、許可先を設定した別の設定ファイル（Pod内の`/tmp`に置く）を`-c`で渡して試験用の会話に対して実行する。
