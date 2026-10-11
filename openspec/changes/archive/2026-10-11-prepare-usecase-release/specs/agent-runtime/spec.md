# Spec Delta

## RENAMED Requirements

- FROM: `### Requirement: GCS上のSkillsの読み込み`
- TO: `### Requirement: Skillsの読み込み`

## MODIFIED Requirements

### Requirement: Skillsの読み込み
システムは、設定した場所（GCS、またはBotのイメージに含めたローカルのディレクトリ）にあるSkillsをエージェントに読み込ませなければならない (SHALL)。読み込み元のSkillsが更新された場合、同期間隔（既定5分）以内に、以後に始まるturnへ反映しなければならない (SHALL)。実行中のturnが参照するSkillsを、turnの途中で入れ替えてはならない (SHALL NOT)。Skillに付属するファイルは、エージェントのSandboxからも参照できなければならない (SHALL)。

#### Scenario: Skillの利用
- **WHEN** GCSに`report`というSkillがある状態でturnを実行する
- **THEN** エージェントは`report`を使え、そのSkillに付属するスクリプトをSandboxで実行できる

#### Scenario: Skillの更新
- **WHEN** GCS上の`report`を更新し、同期間隔が経過した後に新しいturnを始める
- **THEN** 新しいturnでは更新後の`report`が使われる

#### Scenario: イメージに含めたSkill
- **WHEN** 設定の`skills.source`に、Botのイメージ内のディレクトリ`/app/usecase/skills`を指定し、そこに`report`というSkillがある状態でturnを実行する
- **THEN** エージェントは`report`を使え、そのSkillに付属するスクリプトをSandboxで実行できる

## ADDED Requirements

### Requirement: システムプロンプトを設定したファイルから読む
システムは、エージェントのシステムプロンプトを、設定の`agent.system_prompt_file`に指定したファイルから読まなければならない (SHALL)。指定がない場合は、本体に含まれる汎用のシステムプロンプト（Sandboxツールの使い方、`<system-notice>`の扱い、バックグラウンド実行の扱いを含む）を使わなければならない (SHALL)。

#### Scenario: ユースケースのプロンプト
- **WHEN** `agent.system_prompt_file`に「回答は英語で書く」と書いたファイルを指定してturnを実行する
- **THEN** エージェントはそのファイルの内容をシステムプロンプトとして受け取る

#### Scenario: 指定がない
- **WHEN** `agent.system_prompt_file`を設定せずにturnを実行する
- **THEN** エージェントは本体の汎用のシステムプロンプトを受け取る
