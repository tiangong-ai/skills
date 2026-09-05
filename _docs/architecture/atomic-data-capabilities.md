---
docType: architecture
scope: repo
status: current
authoritative: true
owner: skills
language: zh-CN
whenToUse: "规划或修改原子数据 Skill、CLI 数据能力绑定或 Auto Research 数据能力入口时。"
whenToUpdate: "当仓库边界、薄 Skill 结构、兼容绑定、迁移范围或 CLI/Research 交接变化时。"
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - _docs/architecture/repo-architecture.md
  - _docs/contracts/repo-contract.md
  - _docs/runbooks/atomic-data-skill-migration.md
  - "*-fetch/**"
  - "*-search/**"
  - "*-download/**"
  - tiangong-auto-research/**
lastReviewedAt: 2026-09-05
lastReviewedCommit: 24c5695ca3129bbe021e4b80d830742841e9011e
---

# 原子数据 Skill 目标架构

## 决策

现有数据 fetch/search/download Skills 中可复用的 provider 业务逻辑将按评审结果重写到
Tiangong CLI 的 TypeScript 7.x 数据运行时。Skills 仓库继续拥有面向 agent 的语义
入口，但不再维护第二份 Python/JavaScript connector、HTTP/认证/分页/重试实现或闭合
机器 Schema。

EcoCouncil 仓库已提交的 `main@ac19289b4876d8a90595a0270721ef3f5ee7ced8`
是本次迁移范围和源行为的权威输入；其中 21 个 `skills/source-fetch` Skill 必须逐项形成
迁移或明确保留/退役决定。Skills 仓库现有脚本只作为目标仓库集成与兼容性参考，不得
覆盖 EcoCouncil 的源清单，也不得据此静默遗漏 EcoCouncil 独有能力。

EcoCouncil 工作树 `codex/pluggable-harness-migration` 的未提交内容属于已废弃实验，混合
了 council runtime、报告链、数据路线纪律和部分 fetch 脚本改动；它不属于本次迁移输入，
不得复制、择取或用于推断源语义。迁移不合并旧 Git 历史，不保留 Python 兼容层，也不
迁入 OpenClaw harness、议会/多 agent 编排、跨 round/case 数据库或案例工作流。

CLI 仓库中的 `docs/agents/data-runtime-architecture.md` 是命令、manifest、Schema、错误、
回执、凭证和执行行为的权威目标契约；本文件只规定 Skills 如何消费它。

## 所有权

| 内容                                 | 所有者        | Skills 侧规则                                |
| ------------------------------------ | ------------- | -------------------------------------------- |
| 用户意图、任务选择、结果使用边界     | Skills        | 写入 `SKILL.md`，为 agent 提供语义入口       |
| 数据源客观说明、覆盖范围、许可、限制 | CLI           | 由 Discovery Metadata 统一发布，Skill 不复制 |
| capability/operation 的客观说明      | CLI           | 由 catalog/describe 发布三层发现语义         |
| capability/operation 兼容要求        | Skills        | 保存稳定、机器可检验的 contract major 与必要 feature |
| exact CLI package/integrity          | 调用方/Workspace | 由 runtime lock 统一负责，不分散进 Skill   |
| connector、Schema、错误码、回执      | CLI           | Skills 不复制定义                            |
| 受控下载与本地 artifact transaction  | CLI           | Skill 只选择显式目录并解释结果边界           |
| HTTP、认证、分页、限流、缓存         | CLI           | Skill 不再直接执行网络业务代码               |
| 多源选择、证据准入、研究持久化       | Auto Research | 不下沉到原子 Skill                           |
| 旧 Python/OpenClaw 实现              | 只读迁移输入  | 正式路径不得依赖                             |

CLI 是先确认、先实现、先发布的基座。Skills 计划可以与 CLI 计划同步评审，但生产 Skill
不能先合并一份指向尚不存在命令或未发布 contract 的 requirement。

## 标准薄 Skill

迁移后的原子数据 Skill 预期只包含：

```text
<skill>/
├── SKILL.md
├── agents/openai.yaml
└── references/
    └── tiangong-data-requirement.json
```

具体 Skill 可在确有需要时保留不属于 CLI Discovery Metadata 的任务型 reference/asset，
但默认移除：

- provider fetch Python/JavaScript 脚本和依赖；
- `config.example.env` 中由 CLI 负责的运行时配置；
- OpenClaw/eco-council chaining 模板和 raw artifact 路径约定；
- 重复的数据源说明、覆盖范围、许可、限制和 provider 文档列表；
- 重复的输入/输出字段表、重试算法、HTTP 参数构造和 provider 响应校验代码。

`SKILL.md` 应说明用户表达什么意图时选择该能力、何时改选其他工作流、如何通过
`catalog`/`describe` 获取当前客观来源事实、如何用文件/stdin 调用调用方已解析的 CLI
operation，以及结果可以进入哪些上层任务。凭证只以逻辑需求呈现，真实值由 CLI
解析；示例不得把 secret 放进命令行或 JSON。

发现语义分为三层：数据源说明外部数据集由谁维护及覆盖什么；capability 说明 CLI
开放了其中哪一部分能力；operation 说明一次调用执行什么动作。三层客观事实均由
CLI Discovery Metadata 发布。Skill 只增加自然语言意图路由和上层工作流语义，不
维护另一份可能漂移的来源目录。

## 稳定 requirement 与迁移 provenance

每个迁移后的 Skill 只保存
`references/tiangong-data-requirement.json`，声明：

- requirement schema 版本与 `skillName`；
- `capabilityId` 和兼容的 capability contract major；
- 每个允许 operation 的 `operationId` 与兼容 contract major。
- 当 Skill 依赖某项同 major 内新增的行为时，声明该 operation 的
  `requiredFeatures`；不依赖额外行为的 requirement 继续使用 v1。

requirement 不包含 CLI package version、manifest digest 或输入/输出 Schema digest，
因此无关模块变更、普通修复和兼容扩展不会触发 21 个 Skill 锁步更新。调用方负责选择
实际 CLI；managed Auto Research 必须使用 workspace `runtime-lock.json`，独立 Skill
则使用调用方已经解析的稳定 CLI。两种路径都先用同一运行时的 `data describe` 检查
requirement，并从返回值取得本次 `DataRunRequest` 所需的精确 capability/operation
版本。contract major 变化、operation 消失或所需 feature 不再发布才要求更新 Skill。

删除旧 provider runtime 仍需一次精确迁移资格证据。它集中保存在仓库级
`scripts/data-skill-migration-provenance.json`，记录本次验收使用的 CLI 版本、manifest
和 operation Schema digests。该文件不随单个 Skill 安装，不参与日常运行，也不因后续
兼容 CLI 发布而重生成；它只在重新执行迁移/发布资格审计时更新。Discovery Metadata
及其 digest 始终由 `catalog`/`describe` 即时提供，不进入 requirement 或 execution
provenance。

## 调用边界

薄 Skill 通过公共命令族调用一次原子 operation：

```text
tiangong-ai data catalog
tiangong-ai data describe <capability-id>
tiangong-ai data doctor <capability-id> [--live]
tiangong-ai data run <capability-id> <operation-id> --input <path|-> [--artifact-dir <absolute-existing-directory>]
```

这些命令已进入 CLI 主分支。生产 Skill 只要求包含该 contract 的正式 npm 包；Skill 不
自行发现其他来源、不跨来源 fan-out、不解释研究结论。
只有 Execution Manifest 声明 `artifactOutput` 的 operation 才能接收显式
`--artifact-dir`；绝对路径是 out-of-band 执行参数，不进入 request/result/receipt。
一个来源内部有界的分页或多文件窗口仍可以是一个 operation；跨来源组合必须由 Auto
Research 或显式上层调用者完成。

Auto Research 已接入同一 CLI 内部数据服务：native discover packet 从 registry 动态投影
`data:<capability-id>:<operation-id>`，当前十九个 connector 产生二十三个 operation，
不在 orchestrator 中维护静态 provider 表。原子 Skill、独立 data 调用与 Research 共享
相同核心结果/回执；Research 只额外负责 runtime/manifest binding、owner-only credential
map、预算、来源/证据准入、永久 evidence/artifact、journal、handoff 和 review。这些状态
不得回流到薄 Skill；新增 connector/operation 也不要求新增 Auto Research provider 代码。
大结果的完整 core result 始终进入不可变 evidence store；Agent context 只做有界投影，并
通过 receipt-bound opaque cursor 从同一对象续读，不重复访问 provider。Auto Research 必须
分别解释 provider coverage、operation/runtime limit coverage 与 context view coverage：前两者
决定数据本身是否存在缺口，后者只决定 Agent 当前看到了多少。需要逐行审阅时续读至游标
为空；允许摘要或自适应停止时必须披露已呈现/总量比例。

## 原子性与独立性

不要求每个能力有独立进程或 npm 包，但要求：

- 每个 Skill 只绑定一个清晰 provider/capability 语义；
- 每个 operation 可单独 catalog、describe、doctor、run 和测试；
- 一个 connector 的凭证、失败、缓存或 partial 结果不污染其他能力；
- Skill 不调用另一个 Skill 来完成自己的核心 fetch；
- 通用 contract 只统一运行和 provenance，不把异构来源强压成巨型业务 Schema。

## 迁移分类

目录名包含 fetch/search/download 不等于都应迁入通用 data runtime。所有候选先归入：

1. **原子 provider connector**：有稳定官方 endpoint、闭合参数/结果和明确许可，适合
   CLI `data`。
2. **内容获取/媒体工作流**：涉及正文解析、浏览器、下载产物或平台特定语义，需要单独
   判断是否仍是 connector。
3. **Tiangong/KB/Research 产品能力**：已有明确产品命令或研究边界，不因名称含
   `search`/`fetch` 而迁移。
4. **私有账户/本地输入能力**：例如 IMAP，需独立安全评审，不自动进入公共数据面。
5. **退役/合并**：价值、许可、API 稳定性或维护成本不足，不追求旧能力数量对等。

具体目录清单、批次和完成门槛见
`_docs/runbooks/atomic-data-skill-migration.md`。

## TypeScript 7 边界

用户要求的 TypeScript 7 重写发生在 CLI 仓库。Skills 侧不再保留数据业务实现，因此
不为 connector 新增另一套 TypeScript 脚本。CLI 的 TypeScript 7 工具链升级先作为
独立门槛通过，随后才实现公共合同和 connector；Skill 迁移等候已发布的兼容 CLI。

## 完成定义

一次原子 Skill 迁移完成必须同时满足：

- CLI 中已有已发布、TypeScript 7 实现的 capability/operation 及闭合测试；
- Skill 只保留意图入口、上层使用边界和可检验 contract-major/required-feature requirement；
- 客观来源/覆盖/限制通过 CLI Discovery Metadata 获取，不在 Skill 重复维护；
- 无 Python/旧 harness/OpenClaw 执行依赖，无重复机器 Schema；
- requirement/provenance、skill-creator、安装 smoke 和 docpact 门禁通过；
- Research 如需该来源，通过 CLI adapter 复用核心结果，而不是继续执行 Skill 脚本。

本地候选分支已为 EcoCouncil 21 项权威清单建立逐项薄 Skill。三个 Regulations.gov
入口中，comments 与 comment detail 共享 `regulations-gov.comments` capability 并分别只绑定
search 与 fetch-details；attachments 独立绑定声明 artifact output 的
`regulations-gov.attachments/download`。两个 YouTube 入口共享一个 capability 并分别绑定
search-videos 与 fetch-comments，四个 GDELT 入口分别绑定独立 capability。本地候选分支可先
完成薄化和测试，但只有在对应 CLI 正式版本发布、仓库级 migration provenance 复验、隔离安装 smoke 和
回退路径验证后才达到生产完成。RSS/fulltext、Figshare、论文下载、Tiangong/KB 与邮箱
候选的边界审计也已完成：它们保留专用内容、artifact、Research、产品或私有账户实现，
不作为尚未完成的 data runtime 迁移项。
