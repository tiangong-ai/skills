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
lastReviewedAt: 2026-09-03
lastReviewedCommit: 246f93abffc19ff7efa6aad37f9cf2515b4fc82b
---

# 天工 AI Skills

仓库地址: https://github.com/tiangong-ai/skills

请使用 https://github.com/vercel-labs/skills 提供的 `skills` CLI 来安装、更新和管理这些 skills。

## 反馈问题或建议能力

使用[反馈表单](https://github.com/tiangong-ai/skills/issues/new/choose)和
[提交指南](CONTRIBUTING.md)，支持中文和英文。CLI 运行时问题或归属不确定时，
使用 [CLI 表单](https://github.com/tiangong-ai/cli/issues/new/choose)。也可以让
已安装的 Auto Research 按随包提供的反馈规范生成统一格式的 Issue 草稿。

## 原子数据 Skills

当前本地候选分支中的 AirNow Hourly Observations、EPA EIS Records、Federal Register Documents、NASA FIRMS Active Fire、
OpenAQ Air Quality、Regulations.gov Comments、Regulations.gov Comment Details、Regulations.gov Attachments、
USBR Project Records、USBR RISE、USGS Water IV、三个 Open-Meteo 来源，以及 GDELT DOC、Events、GKG、Mentions
加上 Bluesky Cascades、YouTube Video Search 与 YouTube Comments，共二十一个 Skill 已
收敛为 Tiangong CLI TypeScript 7 数据运行时之上的薄语义候选。每个候选只在
`references/tiangong-data-requirement.json` 中记录稳定的 capability/operation
contract major 要求，以及该 Skill 确实依赖的 operation feature；实际 CLI build 由调用方或 workspace runtime lock 选择。Agent
使用同一已解析 CLI 运行 `data describe` 与 `data run`。这些 Skills 不再保留第二份
provider connector 运行时，也不保存各自的 package lock。

权威迁移源 `main@ac19289b4876d8a90595a0270721ef3f5ee7ced8` 共有 21 个
`source-fetch` Skill，当前 21 项均已完成源语义复核、与本地 CLI 候选的 requirement
校验，以及统一 copy/symlink 安装和数据专项门禁。本次迁移验收使用的 CLI 版本与精确
manifest/Schema digest 只集中保存在
`scripts/data-skill-migration-provenance.json`，它是发布证据而不是安装后的 Skill
运行依赖。仓库全量 cold gate 在叠加独立范围的上游文件系统时钟修复后同样通过；该
前置修复有意不进入本数据迁移分支。

所有权边界、候选清单、迁移源审计更正、分批迁移顺序和发布门槛见
`_docs/architecture/atomic-data-capabilities.md` 与
`_docs/runbooks/atomic-data-skill-migration.md`。已经审计的 RSS/fulltext、Figshare 下载、
论文下载、Tiangong/KB 和私有邮箱候选继续保持其内容、artifact、Research、产品或安全
边界，不会为了数量对等而缩减为无状态 data connector。

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
- 安装清华研究生学位论文 LaTeX 工作流:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tsinghua-graduate-thesis
  ```
- 安装天工 KB 导入工作流:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-kb-ingest
  ```
- WorkBuddy/CodeBuddy 作为 producer 时，在 canonical orchestrator 旁安装薄适配 Skill:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-auto-research-workbuddy
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
`npx skills add` 只安装或链接 Skill 文件，不会配置语言运行时或执行安装后 hook。
如果 Skill 提供锁定运行时的 bootstrap 与 smoke 命令，应按该 Skill 的说明显式
执行这些步骤。

## 天工 KB 导入兼容性

`tiangong-kb-ingest` 使用经过验证的精确 CLI 0.0.48 发行版。安装或更新 Skill
后，运行不需要凭据的兼容性冒烟：

```bash
npx --yes --package "@tiangong-ai/cli@0.0.48" -- tiangong-ai --version
npx --yes --package "@tiangong-ai/cli@0.0.48" -- tiangong-ai kb --help
```

版本命令必须输出 `0.0.48`；KB 帮助必须列出 bulk scan、metadata dry-run、
collection list/schema、ingest 和 status 命令。上述检查不会调用或修改 KB 后端。

## Auto Research 外部 Skill 配置

普通 Skill 管理可以直接使用 `npx skills`。Auto Research workspace 应优先使用
CLI 的防呆 setup 层：底层仍调用精确锁定的 `skills` CLI，同时额外绑定来源
commit、Skill tree hash、目标目录、许可证选择、安全凭据绑定和审计状态。普通用户
可在 Wizard 中隐藏输入每个已选 Key；命名环境变量和有界 stdin/密码管理器输入仍是
显式可选方式。先查看只读目录，或启动交互式 Wizard：

```bash
REVIEWED_BOOTSTRAP_CLI_VERSION=X.Y.Z # 替换为已审阅的精确稳定版本
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup catalog \
  --workspace /absolute/path/to/workspace --json
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup \
  --workspace /absolute/path/to/workspace
```

需要可重复、非交互式配置时，先运行 `research setup init`。它会以不覆盖已有
文件的方式创建 schema v2 `.tiangong-research/setup.yaml` 模板和
`setup.env.example`。YAML 会显式列出当前 catalog 中的全部 Skill、凭据和设置，
包括处于关闭状态的可选项；env example 同样列出全部凭据变量及空占位。用户审阅
启用状态、许可证、模型、价格、检查和确认项后，普通 `research setup` 只检测该
workspace 内的固定 YAML，并跳过 Wizard。可选的真实 `setup.env` 必须仅 owner
可读写；禁用凭据必须保持空值，非空的禁用凭据会直接报错。旧 v1、无效或不完整声明会失败，
不会回退到交互流程，也不会向父目录搜索。只有所有已选依赖、provider live check、
Policy 兼容性检查和独立 reviewer smoke 都完全就绪，setup 才返回成功。需要明确
选择交互流程时使用 `research setup wizard`。详见
`tiangong-auto-research/references/setup.md` 和
`tiangong-auto-research/references/env.md`。

bootstrap 版本是新 workspace 的显式选择，不得使用 `latest`、tag 或 range。
apply 创建 `runtime-lock.json` 后，已安装 orchestrator 的内置 resolver 会让
所有 workspace 操作只运行该锁定版本。

资料较多时，orchestrator 使用 CLI 的只读文件大小预检、角色覆盖预测、有界原子
批量登记与显式科学评审执行。兼容的锁定 CLI 支持在分析前于同一项目修订获取结果，
仅在需要新来源时显式重开 discovery，并复用已有回执和文件；设计变化或分析后的
修订仍走正式后继流程。原始要求、当前批准范围、原生宿主检查、独立评审和可移植
审计保持关联，不增加固定付费评审轮次；流程闭环与任务完成分别汇报。详见
`tiangong-auto-research/references/execution-assurance.md`。不修改冻结历史，也不为
通过门禁而偷偷降低科学要求。

兼容的锁定 CLI 还支持在原项目追加兑现预声明的待补科学对象，区分原始请求原文、
解释与重建，并把计算验收绑定到实际原生计算回执。完整工件可经精确绑定的按需读取
通道访问，不再因人为的总上下文长度门槛而阻断。只有命令自报时仍标为未验证执行；
文件哈希与读取回执不等于科学结论正确。机器契约与观察器属于 CLI，Skill 不维护
第二套执行器。

orchestrator 会在 setup 或任何工具调用之前先检查研究问题。预设结论或排除反证的
请求会被暂停，并给出一个可检验的改写版本；只有用户明确确认后才继续。争议性主题和
方向性假设只要仍允许零结果、替代解释和反面证据，就不会因为“有立场”而被拒绝；
伪造或隐瞒证据的请求会被拒绝。

目录还提供 `tiangong-auto-research` 工作流 orchestrator、默认基线 Brave
互联网证据能力、可选的 Tiangong SCI/文档解析/论文获取 companion，以及可选的
Anthropic 或 PPT Master 闭环后创作 Skills。workspace 可以是用户指定的任意目录。
所有条目都是外生、独立授权、精确锁定且经用户明确确认或选择；研究 package
不会捆绑或安装它们。完整流程见
`tiangong-auto-research/references/setup.md` 和 `external-skills.md`。
CLI 内置数据 connector 不属于上述外生 Skill：native discover packet 会动态投影当前
data catalog，Auto Research 通过 Research evidence 命令进程内调用同一 TypeScript
runtime。Skill 不复制 provider adapter，也不维护固定 connector 清单；Research 只在
不改变核心结果的前提下增加预算、owner-only credential、不可变 receipt/ledger、artifact
和 review 绑定。
当结果大于一次 Agent context 时，Auto Research 仍完整保存不可变证据，并提供与 receipt
绑定的续读游标。需要逐行完整审阅时必须读到游标为空；摘要任务可以提前停止，但必须披露
已呈现比例。provider 缺口、operation 限制和 Agent context 投影始终分开表达。
`tiangong-auto-research-workbuddy` 只负责沙箱 IDE 路由，会回到 canonical
orchestrator 及其签名 reviewer bridge 流程，不维护第二套研究协议。
创建 PPT 时首选 PPT Master；Anthropic PPTX 仍是兼容的按场景选项，需要时可在
同一显式计划中一起选择。

当目标是顶刊论文时，orchestrator 提供保守的 Research Policy 默认模板，覆盖文章
类型、学科、期刊类别、项目 publication brief，以及四个独立终稿审阅角色。CLI
Policy Wizard 会把所选 Markdown 复制到研究 workspace 供人类审阅；仍在使用通用
默认内容时会明确提示，审批必须绑定当前内容的精确哈希，任何后续修改或过期都会使
审批失效。只有依据当前官方指南做出实质性人工定制，才可能达到精确目标期刊的就绪
上限。这套门禁只能产出可复核的投稿候选稿，不能承诺编辑接受。详见
`tiangong-auto-research/references/publication-policy.md`。

在 discovery 之前，当前原生 Codex、Claude、WorkBuddy 或 CodeBuddy host 还必须给出封闭、目标特定的科学
设计。冻结的模型实现和环境锁必须先通过 `research scientific object register`
进入 workspace；Skill 不会要求用户手工把文件写入 `.tiangong-research`。CLI 只负责
验证和冻结设计：discovery 前进行独立 `research-design` 审查；acquisition 完成后、
analysis 前依次进行真实记录 `evidence-construct` 和 `pilot-methods` 审查。
acquisition 后还必须完成逐文件拆解、精确 evidence atom、typed-content snapshot、
passing inference snapshot、可复现分析和机械生成的 Claim-Evidence Graph。投稿冻结
要求完整论文章节及显式的 cover/title/checklist/data/code/source-data 文件；四个终稿
审阅必须使用与原生 producer 不同的已配置 agent family 和全新 session。CLI 会为
全部早期审查、终稿审查和一次修订预留预算，每个权威恢复世代都必须重新审批，并只
在语义复核完整冻结链后导出包含正式证据原文而不是宿主本地路径的可移植审计目录。
CLI 仍是确定性控制平面，不会启动嵌套 producer 来替用户发明科学设计。详见
`tiangong-auto-research/references/scientific-design.md`。
