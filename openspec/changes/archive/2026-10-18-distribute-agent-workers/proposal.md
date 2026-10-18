# Proposal

## Why

現在は Slack 受信、会話の SDK、バックグラウンド通知、SQLite による Sandbox 管理が一つの Bot Pod に集まり、複数 Pod へ増設できない。実行中の追記・停止・完了後の再返信を維持し、Worker の縮退で Sandbox のコマンドを取り消さずに運用できる構成へ移行する。

## What Changes

- GKE 上に Gateway、共有 Agent Worker、Control（割当・復旧・送信）、Sandbox Service（API・監視・ライフサイクル）を分離する。Worker は複数会話を保持し、Sandbox は会話ごとの別 Pod を維持する。
- Firestore に受信箱、制御要求、会話所有権、コマンド台帳、Sandbox 管理状態、出力 Outbox を保存する。SDK transcript は既存の Agent Platform Sessions に残す。
- ACK 前の入力永続化、会話内の順序、複数 Gateway 間の重複排除を実装する。
- 所有権の世代・期限、旧実行の停止確認、停止境界により、古い Worker の新規操作を防ぐ。SDK の追記窓と echo 追跡を維持する。
- 実行中でも制御要求を処理し、停止で先行する未処理入力とコマンドを取り消す。長いコマンドは従来どおり明示指定または既定20秒でバックグラウンド化する。
- 永続コマンド監視で Worker 交代後も完了を届ける。待機の解除とコマンド取消を分離し、計画的縮退では turn 完了後に引き渡す。
- **BREAKING**: 分散 GKE 配置では SQLite の共有管理と単一 Bot Deployment を廃止し、状態移行と新設定を必要とする。正常 shutdown はユーザ停止とは区別し、Sandbox コマンドを取り消さない。
- 初期は手動の増設・drain 運用とし、負荷指標を実装する。自動 HPA 縮退、SDK のライブ移送、外部副作用の exactly-once は対象外。

## Capabilities

### New Capabilities

- `distributed-execution`: 永続 Inbox/Outbox、共有 Worker 所有権、失効時の排他、再割当、Firestore による Sandbox 管理、独立したサービス配置と運用。

### Modified Capabilities

- `conversation-core`: 分散環境での単一 SDK、永続受付順、停止境界、中断会話に対する通知の保留。
- `agent-runtime`: コマンド監視の永続化、detach と cancel の分離、Worker 交代時のコマンド継続と完了通知。
- `session-persistence`: 管理情報の Firestore 化、lease に基づく中断検出、drain、履歴保存失敗・配送不明時の復旧。
- `slack-channel`: 永続化後の ACK、共有重複排除、出力配送の復旧と不明状態、SDK 中断とコマンド継続の表示。
- `gke-infrastructure`: 分離 Deployment、Firestore 設定・権限、管理 PVC の移行、手動 drain を含む運用。

## Impact

- `src/silk_agent/core/`、`agent/`、`channels/`、`stores/`、`sandbox/`、`app.py`、`slack_app.py`、`cli.py`、`config.py` と関連テスト。
- `src/gke_session_sandbox/` の Repository 抽象化、Firestore 実装、reconciler の複数プロセス対応。Sandbox 内の CommandTracker は継続利用する。
- `pyproject.toml` の Firestore クライアント、`terraform/` の DB/IAM/index、`k8s/` とユースケース配置、移行 CLI・runbook。
- Streamlit・ローカル開発は既存の in-process adapter を維持する。入力・出力ガード、Skills、会話開始者、外部 MCP 設定、Sandbox Egress 分離を保つ。
- 詳細な検討元は [distribution-plan.md](../../../docs/distribution-plan.md)。同一 Pod と会話ごとの Worker/Sandbox ペアは採用しない。
