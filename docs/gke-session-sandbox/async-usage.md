# Async利用例

`SandboxManager`はsession単位の作業環境を取得するasync APIです。設定とSandbox templateの確認はクラスタ接続なしで実行でき、ensure / get / suspend / resume / terminate / list / closeと、追跡command・workspace file APIを提供しています。

```bash
uv run python examples/render_template.py
```

このコマンドは`examples/sandbox-config.yaml`を検証し、`templates/sandbox.yaml`へ設定値を型付きで挿入したmanifestとdigestを標準出力へ表示します。image参照は利用するruntime imageへ置き換えてください。通常の運用では再現性のためdigestで固定します。

実クラスタへ接続し、1 sessionの作成・停止・再開を行う例です。設定のimage参照を実際のsandboxd入りruntime imageへ置換してから実行します。

```bash
GKE_PROJECT_ID=... GKE_LOCATION=... GKE_CLUSTER_NAME=... \
  uv run python examples/async_lifecycle.py
```

command APIを含む最終的な利用契約は次のとおりです。

```python
from gke_session_sandbox import SandboxManager, SessionKey


async def run_agent_turn() -> None:
    async with SandboxManager.from_config("examples/sandbox-config.yaml") as manager:
        session = await manager.ensure(
            SessionKey(tenant_id="workspace-id", session_id="channel/thread"),
            env={"USER_EMAIL": "user@example.com"},
        )
        async with session.commands as commands:
            result = await commands.run(
                ["python3", "-c", "print('hello')"],
                cwd=".",
                timeout_seconds=30,
            )
        async with session.files as files:
            await files.write("result.txt", result.stdout)
            print((await files.read("result.txt")).decode())
```

clientを閉じた後もcommand IDで状態を確認し、stdoutをcursorで取り直せます。pageを読むAPIは1回あたり最大1MiBで、stdoutとstderrを別々に取得します。

```python
async with session.commands as commands:
    accepted = await commands.start(["python3", "-c", "print('saved output')"])
command_id = accepted["command_id"]

async with session.commands as commands:
    result = await commands.wait(command_id)
    cursor = 0
    output = bytearray()
    while True:
        page = await commands.read_output(command_id, stream="stdout", cursor=cursor)
        output.extend(page.data)
        cursor = page.cursor
        if page.complete:
            break
    print(output.decode())
```

通常の`wait()`はstdout / stderr各8MiBまでをbuffered結果として返します。超過時は例外になり、`read_output()`で保存済み範囲を取得できます。保存上限はcommand合計64MiB / session合計256MiB、既定保持期間は終了後604800秒（7日）です。`output_complete=False`なら保存済みpageがあっても一部欠落しており、`OutputIncompleteError`または容量に対応する例外で原因を確認します。出力本体はPVC上に保存し、出力の期限切れ後もcommandの終了要約は残ります。

`ensure`へ渡すenvはそのgenerationのPod環境へ反映されます。同じgenerationへの再接続でenvを省略すると記録済み設定を使い、異なる値を渡すと競合エラーになります。commandの`env`はそのcommandだけへ適用されます。managerの終了はSandbox削除を意味しません。長寿命API processではmanagerのlifespan中にreconciler taskを動かし、idle停止と期限処理を続けます。process再起動後は永続Repositoryから監視を再開します。

APIサーバーのlifespanへは次のように組み込みます。reconciler taskはAPIと同じprocessで動き、独立したDeploymentは必要ありません。

```python
from contextlib import asynccontextmanager

from gke_session_sandbox import LifecycleReconciler, SandboxManager


@asynccontextmanager
async def lifespan(app):
    manager = await SandboxManager.from_config("sandbox-config.yaml")
    app.state.sandbox_manager = manager
    try:
        async with LifecycleReconciler(manager).lifespan():
            yield
    finally:
        await manager.close()
```

`gke-session-sandbox reconcile --config sandbox-config.yaml`を運用時の常駐実行に使えます。`--once`を付けると一回だけ状態を照合して終了します。APIと別processで常駐させる場合も、二重実行時の更新はSQLiteのsession Leaseで直列化されます。

workspace消失の通知は、Bot側で会話storeへevent ID付きで追記してからackします。ack前にBotが停止した場合は同じeventが再取得されるため、会話storeの`append_once`で重複を防ぎます。

```python
session = await manager.ensure(key)
for event in session.pending_context_events:
    await conversation_store.append_once(event.event_id, event.context_message)
    await manager.ack_context_event(key, event.event_id)
```

`workspace_cleared` / `workspace_lost`のmessageは、以前のworkspace filesが残っている前提で回答しないようAgentへ伝えます。共有コードPVCは引き続きread-onlyで利用できます。会話本文はSessionRepositoryには保存しません。

公開契約とcluster接続例は[README](README.md)を参照してください。実GKEでのcommand/file client確認は[sandboxd runtime検証記録](validation/sandboxd-runtime.md)にあります。設定描画例と`examples/async_lifecycle.py`は現行packageに合わせています。

永続Repositoryは`repository_database_path`に指定したSQLite databaseへrecordとsession leaseを保存します。GKEでAPI processを動かすときは、`examples/sandbox-config.yaml`の既定値`/var/lib/session-sandbox/sessions.sqlite3`を管理API専用PVCへmountしてください。`repository_require_mount: true`なら、mountが無い状態で一時ディスクへ誤って書く前に起動を失敗させます。PVCはReadWriteOnceで、API Podはreplicas=1にします。SQLite transactionとversion条件付き更新で同時書込みを保護し、process / Pod再起動後も同じPVCからsession一覧と状態を復元します。ConfigMapの1 MiB制限はありませんが、未ack eventは既定100件に制限します。

GKE bootstrapは`session-manager-state-rwo` StorageClassと10Gi PVCを用意します。StorageClassのreclaim policyは`Retain`なので、PVCを削除してもディスク上のデータは残ります。不要になったディスクは内容を確認してから手動で削除してください。管理API Deployment側では同じvolume名をcontainer mountとPVCへ結び、mount先をdatabase pathの親directoryにします。

```yaml
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: api
          volumeMounts:
            - name: session-state
              mountPath: /var/lib/session-sandbox
      volumes:
        - name: session-state
          persistentVolumeClaim:
            claimName: session-manager-state
```

ローカル開発などmount外のdirectoryを明示的に使う場合だけ、`repository_require_mount: false`を設定します。

generation、Sandbox / PVC UID、operation ID / phase、実行前に確保したcommand ID、event ID / ack時刻は同じrecordへ保存されます。Kubernetes APIが404を返した場合だけrecord不在として扱い、接続障害はそのまま呼び出し元へ返します。watchのresourceVersionが期限切れ（410）になった場合は一覧を取り直してwatchを再開します。

Sandboxのcore API backendは[実GKE検証記録](validation/python-sandbox-backend.md)にあります。cluster接続には`kubernetes-asyncio` 35系を使い、API操作は非同期で行います。
