---
docType: agent-contract
scope: repo
status: current
authoritative: true
owner: skills
language: zh-CN
whenToUse: "Before editing the reusable skills repository."
whenToUpdate: "When skill creation workflow, validation commands, docpact config, marketplace metadata, or repo documentation rules change."
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - _docs/**
lastReviewedAt: 2026-04-29
lastReviewedCommit: 7bcc1db8d066fa546ffa6e5c9c4b0def46c81ca1
---

# Tiangong AI Skills Agent Contract

本仓库负责可复用 agent skills。workspace 根仓负责子模块集成与交付治理；
skill 内容、skill 规范、marketplace 元数据和本仓文档治理属于本仓库。

## Required Load Order

1. 阅读本文件。
2. 阅读 `.docpact/config.yaml`。
3. 对计划修改的路径，在本仓根目录运行
   `docpact route --root . --paths <target-paths> --format json`。
4. 阅读 `_docs/contracts/**`、`_docs/architecture/**`、`_docs/runbooks/**`
   中与 route 结果相关的文档。
5. 如果任务涉及新建或修改 skill，继续按下方现有 `skill-creator` 强制规则执行。

## Source Of Truth

- `.docpact/config.yaml`：机器可读治理规则、route alias、coverage、
  doc inventory 与 freshness 策略。
- `README.md` / `README.zh-CN.md`：安装、更新、环境变量和使用说明。
- `_docs/contracts/repo-contract.md`：本仓边界、skill 规范与完成条件。
- `_docs/architecture/repo-architecture.md`：skill 仓库结构和分发拓扑。
- `_docs/runbooks/development.md`：创建、校验、生成 agent 配置和交付流程。

## Hard Boundaries

- 不要把 workspace 子模块策略、分支策略或集成完成规则复制进本仓。
- 不要绕过 `skill-creator` 规范手写不合规 skill。
- 不要提交真实 API key、账号密码或用户私有数据到 skill 资源中。
- 修改 skill 触发条件、脚本、引用资料、agent 配置或 marketplace 元数据时，
  同步检查本仓 docs 和 docpact route 结果。

## Completion Criteria

- 修改前已查看相关 `docpact route` 输出。
- route 命中的文档已 reviewed 或 updated。
- 治理变更后 `docpact validate-config --root . --strict` 通过。
- skill 变更按 `skill-creator` 流程运行对应校验。

## Skill-Creator Workflow

For new or modified skills, load
`$CODEX_HOME/skills/.system/skill-creator/SKILL.md`, or
`~/.codex/skills/.system/skill-creator/SKILL.md` when `CODEX_HOME` is unset.
Use that skill's scripts by path: `scripts/init_skill.py`,
`scripts/generate_openai_yaml.py`, and `scripts/quick_validate.py <skill-path>`.
