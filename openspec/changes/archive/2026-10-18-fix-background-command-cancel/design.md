# Design

## Context

- `BackgroundCommands`（`core/background.py`）には2つの経路がある。分散構成は `command_monitor` 経由で `cancel_sender`（`RemoteSandboxRuntime.cancel_command`）へ取消を送る。モニタなしの経路（単一 Pod）は `_cancel` で asyncio の task を cancel するだけ。
- LocalProcess runtime は wait が cancel されるとプロセスを止める。GKE runtime（`sandbox/gke.py`）は、wait を cancel しても handle を残すだけでリモートには何も送らない。command_id 単位の取消の入口もなく、`cancel_commands(conversation_key)` だけがある。これは turn の停止（`session._interrupt`）からしか呼ばれない。
- tracker は task の終了で `finished` を呼び、ID を消す。`_run` の `finally` は `unknown` / `running` のときだけ ID を残す。

## Goals / Non-Goals

**Goals:**
- モニタなしの経路の取消を、runtime が command_id 単位で受けられるようにする。
- 確認できないときに成功を返さず、ID を残す。

**Non-Goals:**
- 分散構成（モニタあり）の取消の経路の変更。
- 取消の再試行や、停止の確認のポーリング。

## Decisions

1. **runtime に任意の `cancel_command(conversation_key, command_id, request_id=None) -> str` を持たせる。** 既存の `cancel_sender`（RemoteSandboxRuntime の同名メソッド）と同じシグネチャにして、`BackgroundCommands` が `getattr(runtime, "cancel_command")` で拾う既存の仕組みをそのまま使う。持たない runtime（LocalProcess）は従来どおり task の cancel で止める。代案として `SandboxRuntime` の Protocol に必須で加える案は、LocalProcess に空実装が要るので採らない。
2. **GKE runtime の `cancel_command` は、tracker が終了を報告したときだけ `confirmed` とする。** tracker の cancel API は SIGTERM を送って HTTP 202 を返すだけで、終了を待たない（SIGKILL は5秒後）。そのため、cancel の後に `get` で record が `finished` になるのを `cancel_confirm_seconds`（既定10秒）まで確認する。終了を確認できなければ `accepted`、cancel や `get` の失敗は `unknown` とし、どちらもローカルの handle と tracker の ID を残す。（当初は「cancel API の成功を `confirmed`」としていたが、SIGTERM を無視するコマンドで誤って停止成功になるため改めた。）
3. **`BackgroundCommands._cancel` は、sender があり command_id があるとき、先に sender で取消を送り、その結果を返す。** task は結果にかかわらず cancel する（待ち続けない）。結果が `unknown` のときは、`_run` の `finally` で tracker の ID を消さない。そのため、`entry.result` を `cancelled` ではなく `unknown` にする。
4. **`cancel_all` は `CommandCancelReport` を返す。** orchestrator の `report_stop` はすでにこの型を扱え、`unknown` があれば「停止を確認できません」と表示する。件数だけを返す今の経路は、`int` の分岐として残る（互換）。

## Risks / Trade-offs

- [取消が `confirmed` と返ったが、実際には止まっていない] → tracker の cancel は SIGKILL まで送る。確認できない失敗は `unknown` に倒す。
- [`unknown` の ID が残り続ける] → 単一 Pod 構成の既知の制限（再起動後の再開通知）。終端化は別の change（review の P1-9 / 過去レビュー「残2」）で扱う。
