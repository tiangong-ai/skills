---
docType: guide
scope: repo
status: current
authoritative: true
owner: skills
language: zh-CN
whenToUse: "安装、更新或使用 Tiangong AI 可复用 skills 仓库时。"
whenToUpdate: "当安装命令、目标 agent、安装范围、环境变量或 skill 可用性说明变化时。"
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - README.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-08-09
lastReviewedCommit: 3dbc264d97573060b5c2e5fa08a4ab8242db87dc
---

# 天工 AI Skills

仓库地址: https://github.com/tiangong-ai/skills

请使用 https://github.com/vercel-labs/skills 提供的 `skills` CLI 来安装、更新和管理这些 skills。

## 安装 CLI
```bash
npm i skills -g
```

## 安装
- 仅列出可用技能（不安装）:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --list
  ```
- 安装全部技能（默认项目级）:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills
  ```
- 安装指定技能:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-kb-sci-search
  ```

## 目标 agent 与作用域
- 指定 agent:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -a codex -a claude-code
  ```
- 全局安装（用户级）:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -g
  ```
- 作用域说明:
  - Codex 是 universal agent：项目级使用 `./.agents/skills`，全局使用
    `$HOME/.agents/skills`；`CODEX_HOME` 不会改变 `skills@1.5.22` 的全局目标。
  - Claude Code 项目级使用 `./.claude/skills`；全局优先使用
    `$CLAUDE_CONFIG_DIR/skills`，否则使用 `$HOME/.claude/skills`。
  - 其他 agent 有各自目录；应查看精确锁定的 `skills` CLI 返回路径，不要按
    `~/<agent>/skills` 类推。

## 安装方式
- 交互式安装可选:
  - Symlink (recommended)
  - Copy

## 更新与确认
- 列出已安装技能:
  ```bash
  npx skills list
  ```
- 检查更新:
  ```bash
  npx skills check
  ```
- 更新全部技能:
  ```bash
  npx skills update
  ```

## 环境变量

环境变量要求由各 skill 自己维护。使用会调用外部服务的 skill 前，优先阅读该
skill 的 `references/env.md`（如存在）。

## Auto Research 外部 Skill 配置

普通 Skill 管理可以直接使用 `npx skills`。Auto Research workspace 应优先使用
CLI 的防呆 setup 层：底层仍调用精确锁定的 `skills` CLI，同时额外绑定来源
commit、Skill tree hash、目标目录、许可证选择、凭据名称和审计状态。先查看只读
目录，或启动交互式 Wizard：

```bash
npx --yes @tiangong-ai/cli@0.0.26 research setup catalog \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.26 research setup \
  --workspace /absolute/path/to/workspace
```

目录包含 Brave 互联网证据能力、可选的 Tiangong SCI/文档解析/论文获取 companion，
以及可选的 Anthropic 或 PPT Master 闭环后创作 Skills。所有条目都是外生、独立
授权、精确锁定且由用户选择；研究 package 不会捆绑或安装它们。完整流程见
`tiangong-auto-research/references/setup.md` 和 `external-skills.md`。
创建 PPT 时首选 PPT Master；Anthropic PPTX 仍是兼容的按场景选项，需要时可在
同一显式计划中一起选择。
