# Tasks

実装者向け: 開発環境の配置は、`kustomize build` の出力と Terraform の plan が変更前と同じであることを確認してから切り替える（design の D3、D5）。実クラスタと実 Terraform への適用は 6 章まで行わない。`add-data-retention` との順序は design の D5 に従う。

## 1. 起動時の照合（D4）

- [x] 1.1 `config.py` に `distributed.namespace_check`（既定 true）を追加し、`POD_NAMESPACE` と設定の値（`sandbox_service_url` のホスト、Sandbox の `control_namespace`、`sandbox_service_allowed_ksas` の namespace）を照合する関数を作る。食い違いは項目名・期待値・実際の値を含むエラーにする。検証: `tests/test_config.py` に、一致、URL の食い違い、KSA の食い違い、`POD_NAMESPACE` がない場合、無効化した場合のテストを追加して通す。
- [x] 1.2 分散構成で `distributed.firestore_prefix` の既定値をなくし、未設定ならその項目名のエラーで起動しないようにする。検証: `tests/test_config.py` に未設定の拒否を追加し、`k8s/` と `usecases/` の分散構成の設定ファイルがすべて値を明示していることを確認するテストを追加する。
- [x] 1.3 Gateway / Worker / Control / Sandbox Service の起動処理で 1.1 を呼び、無効化したときは起動ログに出す。検証: 各役割の起動のテストで、食い違いの設定では起動しないことを確認する。
- [x] 1.4 `roles.yaml` と `sandbox-service.yaml` の各 Deployment に Downward API で `POD_NAMESPACE` を渡す。検証: `tests/test_distributed_manifest.py` で 4 役割すべてに `POD_NAMESPACE` があることを確認する。

## 2. 共通の base と開発環境の overlay（D1〜D3）

- [x] 2.1 変更前の `kubectl kustomize k8s/distributed` と `k8s/distributed-bootstrap` の出力を保存する。検証: 出力ファイルが scratch に保存されている。
- [x] 2.2 `k8s/distributed/base/` に、namespace・image・設定を持たない 4 役割と Sandbox Service の定義を移す。ClusterRoleBinding は名前に接尾辞を付けられる形にする。検証: `grep` で base に `nnyn-dev`、イメージのダイジェスト、`silk-agent_`、`session-sandbox` が含まれないことを確認するテストを `tests/test_distributed_manifest.py` に追加する。
- [x] 2.3 ユースケースごとの namespace 2 つ、Role `sandbox-management`、Sandbox Service の RoleBinding と NetworkPolicy の egress 先の patch を、kustomize の component（`k8s/distributed/platform/`）にする。検証: component を使った最小の overlay を `kubectl kustomize` で生成でき、namespace が指定どおりになる。
- [x] 2.4 `k8s/distributed/overlays/dev` と `overlays/dev-bootstrap` を作り、今の設定・image・namespace を指定する。`k8s/platform/base.yaml` の固定の namespace と Role を dev の overlay へ移す。検証: 2.1 の出力と比べ、差分がリソースの並び以外にないことを `diff` で確認する（比較を `tests/test_distributed_manifest.py` に固定値として残す）。
- [x] 2.5 `k8s/distributed/README.md`、`docs/distributed-operations-runbook.md`、テストの中の古いパスを新しいパスに更新する。検証: `grep -r "k8s/distributed-bootstrap\|apply -k k8s/distributed$"` で古いパスが残っていない、`tests/test_distributed_runbook.py` が通る。

## 3. Terraform のユースケースの一覧（D5）

- [x] 3.1 `terraform/variables.tf` に `usecases` を追加し、既定に開発環境の 1 件を入れる。役割の IAM、Slack の Secret とその権限、`_commands` / `_outbox` の索引を `for_each` にし、今の資源のアドレスを `moved` で移す。検証: `terraform plan` に変更・再作成・削除が出ない。
- [x] 3.2 Terraform の出力に、ユースケースごとの namespace、接頭辞、各役割の権限の主体を追加する。検証: `terraform output` に一覧が出る。
- [x] 3.3 `add-data-retention` の TTL の設定がある場合は、`firestore_prefix` 変数をこの一覧に置き換える。ない場合は「未適用」と書いて完了にし、その change の D8 に一覧を使うことを追記する。検証: plan に差分が出ない、または design の追記がある。
- [x] 3.4 `k8s/grant-iam.sh` の固定の主体をやめ、Terraform で同じ権限が管理されていることを確認してからスクリプトを削除するか、出力を使う形に直す。検証: スクリプトに `session-sandbox-system` の固定値が残っていない。

## 4. テンプレートと配置スクリプト（D2、D6）

- [x] 4.1 `usecases/template/k8s/distributed/`（overlay、bootstrap、`config/` の 3 ファイル）を作る。Sandbox の設定は SQLite のパスを持たない分散構成用にし、環境固有の値は `your-...` の置き換え用の値にする。検証: テンプレートを一時ディレクトリにコピーして値を置き換え、`kubectl kustomize` で生成でき、`silk-agent` の設定の読み込み（`validate_required` を含む）が通るテストを追加する。
- [x] 4.2 `k8s/deploy.sh` に分散構成のモード（`DISTRIBUTED_DIR`）を追加する。対象の namespace に Slack と `silk-agent-sandbox` の Secret を作り、4 役割の rollout を待つ。検証: `bash -n` と、`kubectl` を差し替えたテストで、作成する Secret の namespace と待機する Deployment を確認する。
- [x] 4.3 `usecases/bigquery-analysis` を新しい形に移す。`bot-echo-secret-patch.yaml` を Worker の Deployment に当て直し、echo-mcp を `bigquery-analysis-system` に置き、`bigquery-ksa.yaml` と `terraform/` の `ksa_namespace` を `bigquery-analysis-sandboxes` にする。検証: `tests/test_bigquery_analysis_usecase.py` を更新して通し、`kubectl kustomize` の出力で全リソースが 2 つの namespace に入っている。

## 5. ドキュメント

- [x] 5.1 `docs/usecase-guide.md` に分散構成の章を追加する: 4 役割の構成、分離の単位（namespace と接頭辞、Slack アプリ）、Terraform の `usecases` への追加とユースケース固有の Terraform、イメージ、`silk-agent-distributed.yaml` と Sandbox Service の設定、配置（bootstrap → 起動）、Worker の CLI（`silk-agent worker status-all` など）、リリースと切り戻し、同じ database を共有する場合の分離の限界。検証: `grep` で「分散」「terraform」が含まれ、`tests/` の文書のテストがあれば通る。
- [ ] 5.2 `usecases/bigquery-analysis/OPERATIONS.md` を新しい namespace と手順に更新する。検証: 記載のコマンドが 6.3 で実際に使える。

## 6. 実環境での確認

- [x] 6.1 Terraform の plan を確認して apply する（開発環境の 1 件）。検証: plan に変更が出ず、apply 後の `terraform plan` も差分なし。
- [x] 6.2 開発環境を `overlays/dev` で適用する。検証: Pod が 1 回ローリング再起動する（`POD_NAMESPACE` の追加で Pod テンプレートが変わるため）が、`silk-agent worker status-all` で Worker が稼働中、既存の会話に Slack から返信できる。
- [ ] 6.3 bigquery-analysis を Terraform の一覧に追加して apply し、`deploy.sh` の分散構成のモードで配置して、bigquery-analysis の Slack アプリから会話する。検証: 会話が `bigquery-analysis_conversations` に記録され、Sandbox が `bigquery-analysis-sandboxes` に作られ、開発環境の Firestore の記録と Sandbox に現れない。
- [ ] 6.4 分離を確認する: bigquery-analysis の Worker の Pod から開発環境の Sandbox Service へ接続を試す。開発環境の設定を bigquery-analysis の namespace で起動するよう 1 つの Pod を作り、起動しないことを確かめる。検証: 接続が拒否され、Pod が照合のエラーで終了する。結果を `docs/distributed-operations-runbook.md` に記録する。
