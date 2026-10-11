# Sandbox停止・再開・削除の実測

2026-10-10に、`session-sandboxes/sandboxd-smoke`でAgent Sandbox v1.0.6のcore Sandbox lifecycleを確認した。実行前のPodはgVisor pool上の`us-central1-a`にあり、workspace PVCは`session-workspace-rwo`、PV reclaim policyは`Delete`だった。PVCのownerReferenceはSandbox UID `dc80cdf5-770b-4e6f-bed2-4f3d61816a1f`を指していた。

停止時はSandboxの`operatingMode`を`Suspended`へ変更した。controllerがSuspended conditionを報告しPodを削除した後も、workspace PVC UID `f8b850f1-1422-4bb4-bf93-c7c026706214`とownerReferenceは維持された。

Runningへ戻すとSandbox UIDとPVC UIDは同じまま、新しいPodが作成された。Pod UIDは`beb41107-ddba-494c-8725-d9c8be5c82aa`から`e53308a5-50b6-4f1b-91d8-a9b5c3bcc75a`へ変わった。停止前にRESTで書き込んだ`/workspace/management-runner.txt`を新しいPodから読み、内容が保持されたことを確認した。

Sandboxを削除するとownerReferenceのGCでPVCが削除され、PVも削除された。PVのCSI volumeHandleで特定したCompute Engine PD `pvc-f8b850f1-1422-4bb4-bf93-c7c026706214`もGoogle Cloud API上から消えた。Task 2.3で作った共有ROM PV / diskはこの削除の対象外で、残っている。

再実行はまず[sandboxd経路検証](sandboxd-runtime.md)を実行してfixture Sandboxとworkspaceデータを作り、その後このcontext確認付きscriptを実行する。

```sh
GKE_PROJECT_ID=... GKE_LOCATION=... GKE_CLUSTER_NAME=... \
  ./scripts/validate-sandbox-lifecycle-gke.sh
```

ScriptはSandbox / PVC owner UID、Delete reclaim policy、smoke dataを事前確認し、想定と違うリソースなら破壊操作を始めない。
