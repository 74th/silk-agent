# Python Sandbox backendの実GKE確認

2026-10-10、`gke_nnyn-dev_us-central1_research-gke-agent-sandbox-with-sandboxd`上で、Pythonの`CoreSandboxBackend`からcore `agents.x-k8s.io/v1beta1` Sandboxを作成・開始・停止・再開・削除した。Kubernetes async clientは`kubernetes-asyncio 35.0.1`、controllerはv1.0.6を使用した。

検証Sandbox UIDは`da1cdd72-de9d-437f-8c9b-e95a151085ed`、専用PVC UIDは`86e4f131-521e-4ee6-a980-2696723a864f`。Suspendedで作成した後、Runningへpatchし、controller Ready condition、`observedGeneration`、Ready Pod、Pod内sandboxd readiness probe（`GET /v1/health`）を待った。Suspended完了後はPodが消え、再開後はPod UIDが変わる一方PVC UIDは同じだった。削除後はownerReference GCでPVCが消え、Delete reclaim policyによりPVも消えるまで確認した。

この記録は初期のConfigMap Repository prototypeをGKE APIで検証した履歴です。session Lease取得、record ConfigMap作成、`resourceVersion` CAS、読戻し、Lease / ConfigMap cleanupがasync Kubernetes API経由で成功し、dict patchが400になる点を発見してJSON Patchの`test` / `replace` operationへ変更しました。現在のSessionRepositoryはAPI専用PVC上のSQLiteを使うため、このConfigMap操作は本番経路ではありません。

PVC管理はSandbox UIDのcontroller ownerReferenceとPVC UIDの両方を照合する。再確認時のSandbox UIDは`915bc6c0-aa2b-4683-a5c9-58eef3ad1101`、専用PVC UIDは`a37f6bfb-462b-4271-ba49-ba7478358f95`。検証用共有PVC `session-sandbox-fixtures/runtime-code-rom`のUIDが実行前後で同一で、Sandbox / 専用PVC / PV削除後も残ることを確認した。共有PVCをSandboxへmountするhelperはvolumeとvolumeMountの両方で`readOnly: true`を設定する。

`SandboxManager`も別run IDで確認した。同一keyの同時`ensure`はSandbox 1つへ収束し、get、suspend、resume、manager close後の再接続、terminateまで通った。検証Sandbox UIDは`73762625-5d53-47eb-972a-ca36c7e6eec7`。`close()`後もSandboxは存在し、2つ目のManagerが同じUIDへ再接続した。専用workspaceだけが削除され、共有fixtureは保持された。

当時のSandboxとLease / ConfigMap更新はJSON Patchで行いました。Sandbox mutation前にUID、Repository CAS前に`metadata.resourceVersion`を`test`し、置換済みリソースや古い読取からの書込みを拒否しました。より高水準の`create_with_policies()`はSandboxをSuspendedで作り、渡されたNetworkPolicyを作成してからRunningへ切り替えます。

2026-10-10のSessionManager検証では、session単位のdefault-deny、DNS、`api.anthropic.com:443`のFQDN allowを作成し、sandbox Pod内からDNSと許可先TCP接続が成功し、未許可の`neverssl.com:443`が拒否されることを確認した。管理namespaceからREST `8080`でファイルを作成でき、直接gRPC `9090`の実行要求は拒否された。sandbox Podの`automountServiceAccountToken`が無効で、service-account token volumeがないことも確認した。Policyとsandbox Podはsession / generationラベルで選択し、Sandbox UID ownerReferenceによりsession終了時に一緒に削除される。

同日、`gke-session-sandbox preflight --config examples/sandbox-config.yaml`を実クラスタへ接続して実行し、namespace、Sandbox CRD、gVisor RuntimeClass、workspace StorageClassと現在の認証主体RBACを確認した。`api.anthropic.com:443`を設定したケースでもFQDNNetworkPolicy CRDとRBACを含む40項目が通った。platform bootstrap再適用後、worker ServiceAccountにFQDNNetworkPolicyのcreate / delete権限があることも`kubectl auth can-i --as=...`で確認した。

再実行には明示した現在contextと認証が必要。Sandbox、PVC、PVは検証scriptがcleanupし、PVC / PVの削除完了まで待つ。

```bash
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
uv run python scripts/validate-core-sandbox-backend-gke.py --timeout 300
```

SessionManagerのend-to-end確認は次で再実行できる。

```bash
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
uv run python scripts/validate-session-manager-gke.py --timeout 300
```

通常unitは`uv run pytest tests/unit`。controller Ready conditionがtimeoutした場合は最後に確認したreason / observedGenerationを`SandboxReadyTimeout`へ含める。停止・削除中にUIDが変わった場合は、同名の置換Sandboxを操作しない。
