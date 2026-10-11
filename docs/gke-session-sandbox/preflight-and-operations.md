# Preflightと運用

SessionManagerを使う前に、接続先contextと必要なKubernetes機能・権限を確認する。

```bash
kubectl config current-context
uv run gke-session-sandbox preflight --config examples/sandbox-config.yaml
```

`--context`を指定するとkubeconfig内のcontextを選べる。in-cluster実行ではPodのServiceAccountを使う。preflightはSandbox CRDの`v1beta1`、設定したFQDN policy用CRD、`gvisor` RuntimeClass、workspace StorageClassとprovisioner、管理namespace / sandbox namespace、現在の接続主体の必要RBACを確認する。不足項目は一括して表示し、作成前に非ゼロ終了する。FQDN allowを設定しない場合、FQDN policy用CRDとそのRBACは必須ではない。

初回は専用検証clusterの正しいcontextを確認してからplatform manifestを適用する。manifestは管理用namespace、sandbox用namespace、worker ServiceAccount / RoleBinding、RWO workspace StorageClass、ROM StorageClassを作る。

```bash
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
scripts/bootstrap-platform.sh
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
