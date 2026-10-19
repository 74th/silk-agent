---
name: example
description: Skillの例。ユーザが「Skillの例を見せて」と頼んだときに使う。
---

# example

Skill は、エージェントが使う手順書です。この `skills/` ディレクトリは、Bot のイメージの
`/app/usecase/skills` に入り、`config/silk-agent.yaml` の `skills.source` で読み込まれます
（GCS の `gs://...` を指定すれば、イメージを作り直さずに更新できます）。

Skill に付属するファイル（スクリプトなど）は、エージェントの Sandbox の
`/workspace/.skills/<Skill 名>/` にコピーされます（この Skill なら `/workspace/.skills/example/`）。
`scripts/hello.sh` のように置き、ここから `bash /workspace/.skills/example/scripts/hello.sh`
のように、**この絶対パスで**使わせます（コマンドの作業ディレクトリは Skill の場所ではありません）。

この例では、`hello` という文字列を一言返せば十分です。
