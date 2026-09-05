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
  - .dockerignore
  - Dockerfile.clean-test
  - .github/workflows/docpact.yml
  - scripts/**
  - _docs/**
lastReviewedAt: 2026-09-05
lastReviewedCommit: 24c56953e23f88f5fffa29c5c1447eb4feb8ba3f
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
- `_docs/architecture/atomic-data-capabilities.md`：原子数据 Skill 与 CLI/Research
  的目标边界。
- `_docs/runbooks/atomic-data-skill-migration.md`：数据 Skill 分类、批次、PR 依赖和
  迁移验收流程。

## Hard Boundaries

- 不要把 workspace 子模块策略、分支策略或集成完成规则复制进本仓。
- 不要绕过 `skill-creator` 规范手写不合规 skill。
- 不要提交真实 API key、账号密码或用户私有数据到 skill 资源中。
- 修改 skill 触发条件、脚本、引用资料、agent 配置或 marketplace 元数据时，
  同步检查本仓 docs 和 docpact route 结果。
- 原子数据机器契约、connector、Schema、凭证和运行回执属于 Tiangong CLI；
  数据 Skill 只保留语义入口和可检验的 capability/operation contract-major requirement。
  实际 CLI package 由调用方或 workspace runtime lock 负责，不下沉到每个 Skill。

## Completion Criteria

- 修改前已查看相关 `docpact route` 输出。
- route 命中的文档已 reviewed 或 updated。
- 治理变更后 `docpact validate-config --root . --strict` 通过。
- skill 变更按 `skill-creator` 流程运行对应校验。
- 数据 Skill 迁移还必须验证稳定 requirement，并用仓库级 migration provenance 对
  exact CLI capability/operation/Schema 做一次发布资格审计；只有对应 CLI 正式版本
  可安装后才能移除旧执行脚本。普通兼容 CLI 发布不要求重写全部 Skill requirement。
- `tsinghua-graduate-thesis` 的 PDF renderer 或视觉 QA 变更必须用仓库内隐私安全的真实二进制 PDF fixture，在其 targeted clean-container 中先观察 RED、再在新容器中转 GREEN；PR 前还要运行全仓 cold gate。
- Auto Research 及其直接 evidence wrapper 的变更必须先在
  `scripts/test-clean-container.sh` 创建的独立、无宿主 HOME/全局
  Skill/CLI/runtime cache 的容器中观察回归测试失败，再在另一个新容器中转绿；
  构建可复用输入未变化的 Docker layer，宿主测试不能替代。
- 该容器必须运行 `academic-paper-download/scripts/tests` 的完整 unittest
  discovery；测试镜像只安装 `requirements.lock` 中哈希锁定的核心依赖，绝不安装
  可选 CloakBrowser 运行时或浏览器二进制。
- 修改 `.dockerignore`、`Dockerfile.clean-test`、依赖输入，或准备 PR/发布时，
  必须额外运行 `scripts/test-clean-container.sh --cold-build` 验证冷构建路径。

## Skill-Creator Workflow

For new or modified skills, load
`$CODEX_HOME/skills/.system/skill-creator/SKILL.md`, or
`~/.codex/skills/.system/skill-creator/SKILL.md` when `CODEX_HOME` is unset.
Use that skill's scripts by path: `scripts/init_skill.py`,
`scripts/generate_openai_yaml.py`, and `scripts/quick_validate.py <skill-path>`.

## User Feedback

Read `CONTRIBUTING.md` when changing reporting forms or assisting a reporter.
The installed Auto Research reporting reference and GitHub form core fields
must agree; reporting does not require research setup or paid diagnostics.
