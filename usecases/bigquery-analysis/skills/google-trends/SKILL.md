---
name: google-trends
description: 日本の直近2日の急上昇検索語を BigQuery から取得して説明する。
---

# Google Trends の急上昇検索語を調べる

1. `bash /app/usecase/skills/google-trends/scripts/query.sh` を実行します。
2. 終了コードが 0 のときだけ JSON の `rows` を分析します。`rows` が空なら対象期間にデータがなかったと伝えます。
3. 終了コードが 0 以外なら標準エラーの内容を伝え、検索語や分析結果を作らないでください。`daily quota failure` は、BigQuery の1日のクエリ量の上限に達したことを示します。今日は同じクエリを再実行せず、上限に達したと伝えてください。`rate limit failure` は、短い間のリクエストが多すぎることを示します（日次の上限ではありません）。しばらく待ってからもう一度試してよいと伝えてください。
4. 成功時は各行の `refresh_date`、`rank`、`term` を保ち、取得条件（Japan、東京日付で直近2日、最大10行）を短く示してください。
5. このデータから確認できない検索理由や社会的背景は断定せず、必要なら推測と明確に区別してください。
