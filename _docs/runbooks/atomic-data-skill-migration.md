---
docType: runbook
scope: repo
status: current
authoritative: true
owner: skills
language: zh-CN
whenToUse: "盘点、选择、薄化和验证数据 fetch/search/download Skills 时。"
whenToUpdate: "当迁移候选、批次、CLI 版本依赖、PR 顺序、删除范围或验收命令变化时。"
checkPaths:
  - AGENTS.md
  - _docs/architecture/atomic-data-capabilities.md
  - _docs/contracts/repo-contract.md
  - README.md
  - README.zh-CN.md
  - "*-fetch/**"
  - "*-search/**"
  - "*-download/**"
  - tiangong-auto-research/**
lastReviewedAt: 2026-09-05
lastReviewedCommit: 24c5695ca3129bbe021e4b80d830742841e9011e
---

# 原子数据 Skill 迁移实施计划

## 当前基线

- 计划基线：`origin/main` at
  `4104e527facd09ecc242dad7a1e9645adf9d21f0`。
- 计划分支：`codex/atomic-data-plan`，使用独立干净 worktree。
- EcoCouncil 迁移源固定为 `https://github.com/fpddmw/EcoCouncil.git` 的已提交
  `main@ac19289b4876d8a90595a0270721ef3f5ee7ced8`；其 21 个
  `skills/source-fetch` Skill 是迁移范围的权威清单。
- EcoCouncil 工作树当前分支 `codex/pluggable-harness-migration` 的未提交改动被明确
  归档为废弃实验，不属于迁移输入。该工作树混合 council runtime、报告链、数据路线
  纪律和部分 fetch 脚本试验；不得从中复制、择取实现或用其修正 `ac19289` 的源语义。
- 原 Skills checkout、现有 `codex/atomic-environment-data-skills` worktree 及 CLI 的
  未提交变更都保持不动，不 stash/reset/rebase/clean。
- 首批实现继续使用该独立 worktree；兼容 CLI 包、requirement/provenance 和全部门禁通过后才提交 PR。

## 2026-08-31 迁移源审计更正

- 早期计划把 Skills 目标仓库中的 38 个 fetch/search/download 目录作为总候选清单，
  这是目标目录盘点，不是 EcoCouncil 迁移源清单，不能证明迁移完整性。
- 当前 21 个薄 Skill 与 19 个 CLI capability 已逐项对照 EcoCouncil 权威源完成本地
  迁移与源语义复核；21 项均有明确 CLI/Skill 目标，不再存在结构性缺项。
- 下方矩阵保留逐项审计结论。所有稳定 requirement、仓库级 migration provenance、统一安装与数据专项门禁已通过；
  仓库全量 cold-container 门禁在叠加独立前置修复 `576a6cb` 后通过。正式 CLI 版本发布后
  仍须用正式 CLI 验证 21 个 requirement，并重生一次仓库级 migration provenance，才能合并 Skills PR。
- EcoCouncil 功能分支已提交点 `32d38e5172ebe8703c61a7031b7055c766ac9028`
  与 `ac19289` 的 `skills/source-fetch` 内容一致；本次仍只引用远端可复现的
  `main@ac19289`，不引用未提交工作树状态。

### EcoCouncil 21 项迁移控制矩阵

下表按 `ac19289:skills/source-fetch` 的字典序逐项列出迁移目标。21 项均已完成源
`SKILL.md`、脚本外部行为、CLI Discovery/Execution contract、thin Skill、stable requirement 与 migration provenance
对照，并通过本地数据专项门禁及含独立前置修复的集成 cold gate；表中的“待正式复验”
只表示仍需用正式 CLI 复验稳定 requirement 并更新仓库级 provenance，不表示仍有业务实现缺口。

| # | EcoCouncil 权威源 Skill | Skills 目标 | CLI capability / operation 或保留边界 | 当前状态 |
| -: | ------------------------ | ----------- | ---------------------------------------- | -------- |
| 1 | `fetch-airnow-hourly-observations` | `airnow-hourly-obs-fetch` | `airnow.hourly-observations/fetch-hourly` | 完成；本地门禁通过；待正式复验 |
| 2 | `fetch-bluesky-cascade` | `bluesky-cascade-fetch` | `bluesky.public-posts/fetch-cascades` | 完成；本地门禁通过；待正式复验 |
| 3 | `fetch-epa-eis-records` | `epa-eis-records-fetch` | `epa.eis-records/search`；只解析官方 EIS 结果表，不作法律或政策判断 | 完成；本地门禁通过；待正式复验 |
| 4 | `fetch-federal-register-documents` | `federal-register-doc-fetch` | `federal-register.documents/search` | 完成；本地门禁通过；待正式复验 |
| 5 | `fetch-gdelt-doc-search` | `gdelt-doc-search` | `gdelt.doc-search/search` | 完成；本地门禁通过；待正式复验 |
| 6 | `fetch-gdelt-events` | `gdelt-events-fetch` | `gdelt.events/fetch` | 完成；本地门禁通过；待正式复验 |
| 7 | `fetch-gdelt-gkg` | `gdelt-gkg-fetch` | `gdelt.gkg/fetch` | 完成；本地门禁通过；待正式复验 |
| 8 | `fetch-gdelt-mentions` | `gdelt-mentions-fetch` | `gdelt.mentions/fetch` | 完成；本地门禁通过；待正式复验 |
| 9 | `fetch-nasa-firms-fire` | `nasa-firms-fire-fetch` | `nasa-firms.active-fire/fetch-area` | 完成；本地门禁通过；待正式复验 |
| 10 | `fetch-open-meteo-air-quality` | `open-meteo-air-quality-fetch` | `open-meteo.air-quality/fetch-hourly` | 完成；本地门禁通过；待正式复验 |
| 11 | `fetch-open-meteo-flood` | `open-meteo-flood-fetch` | `open-meteo.flood/fetch-daily` | 完成；本地门禁通过；待正式复验 |
| 12 | `fetch-open-meteo-historical` | `open-meteo-historical-fetch` | `open-meteo.historical-weather/fetch` | 完成；本地门禁通过；待正式复验 |
| 13 | `fetch-openaq` | `openaq-data-fetch` | `openaq.air-quality/search-locations` 与 `fetch-sensor-measurements`；S3 archive 明确移出该 atomic capability | 完成；本地门禁通过；待正式复验 |
| 14 | `fetch-regulationsgov-attachments` | `regulationsgov-attachments-fetch` | `regulations-gov.attachments/download`；固定官方 origin、SHA-256、relative manifest 与事务型本地 artifact | 完成；本地门禁通过；待正式复验 |
| 15 | `fetch-regulationsgov-comment-detail` | `regulationsgov-comment-detail-fetch` | `regulations-gov.comments/fetch-details`，只含 attachment metadata | 完成；本地门禁通过；待正式复验 |
| 16 | `fetch-regulationsgov-comments` | `regulationsgov-comments-fetch` | `regulations-gov.comments/search` | 完成；本地门禁通过；待正式复验 |
| 17 | `fetch-usbr-project-records` | `usbr-project-records-fetch` | `usbr.project-records/fetch`；只取显式官方 URL 与同站链接清单 | 完成；本地门禁通过；待正式复验 |
| 18 | `fetch-usbr-rise` | `usbr-rise-fetch` | `usbr.rise/discover-items` 与 `fetch-results` | 完成；本地门禁通过；待正式复验 |
| 19 | `fetch-usgs-water-iv` | `usgs-water-iv-fetch` | `usgs.water-instantaneous-values/fetch` | 完成；本地门禁通过；待正式复验 |
| 20 | `fetch-youtube-comments` | `youtube-comments-fetch` | `youtube.public-content/fetch-comments` | 完成；本地门禁通过；待正式复验 |
| 21 | `fetch-youtube-video-search` | `youtube-video-search` | `youtube.public-content/search-videos` | 完成；本地门禁通过；待正式复验 |

矩阵完成定义同时要求：CLI connector 或明确保留边界已批准、TypeScript 7 业务实现与
fixture/conformance 通过、Skill 只保留意图与上层组合语义、stable requirement 与同一
候选/正式 CLI 包的仓库级 provenance 相互验证、旧重复 connector 逻辑已移除，并且两仓治理与 clean-container
门禁通过。数量对上但任一条件缺失，仍不得宣称迁移完成。

AirNow 复核以固定提交中的 Python 归一化行为为准，补回了源行时间无效时使用所属
HourlyAQObs 文件小时的回退、空 AQSID 的保留，以及单个污染物数值无效时按缺失值处理而
不丢弃同一记录的其他有效字段；这些边界已有 TypeScript 回归测试，requirement 与仓库级 provenance 已用修正后的
本地 CLI 候选重新验证。

Bluesky 复核补回了固定源所声明的公共 AppView `403` 备用主机、关闭服务端时间过滤但继续
执行客户端 UTC 窗口的历史覆盖诊断，以及对坏 seed/feed/thread 节点的局部隔离；同时保留
`hitsTotal`、坏记录计数和级联拓扑校验。当前 capability 明确保持无凭证公共读取边界，
不迁入源脚本可选的账号/app-password 会话创建、私有或 viewer-specific 状态；这属于认证
账户能力而非本公共数据执行契约，若未来需要应单独设计短期会话凭据合同。

Federal Register 复核移除了源实现不存在的“必须同时提供日期与过滤器”限制，恢复了受公共
runtime 页数/记录数约束的 newest listing，并补回 citation、comments-close、raw-text、XML、
JSON 与 Regulations.gov 等提供方链接字段。单条稀疏记录和可选 count/total-pages 元数据现在
按源脚本的容错语义归一化，不再使整页结果失败；正文仍未被获取，法律解释边界不变。

GDELT DOC 复核补回 query lint（拒绝 `site:`/`inurl:` 与未加括号的 `OR`）、重复
`domain:`/`domainis:` 的有界拆分合并、可选失败批次保留、provider 默认时间窗、长绝对
timeline 窗口与 `tonechart` 归一化；稀疏 article 字段不再使整批失败。CLI 继续只发布
结构化 JSON 数据面，不迁入源脚本的 HTML/RSS/CSV 展示或任意 `--param` 透传，这些不是稳定
机器执行契约；article body、图像和页面内容仍属于单独治理的内容获取边界。

GDELT Events 复核恢复了源 `max-files` 作为“大窗口内最多选择 N 个”的语义，允许非 15 分钟
对齐的 UTC 边界并从首个落入窗口的快照开始；坏 UTF-8/列数行会记录问题并局部隔离，有效行
继续返回，每个 ZIP 增加 SHA-256。CLI 仍以确定性 HTTPS 15 分钟路径替代体量无界的
`masterfilelist.txt` 流式扫描，并返回命名列而不落地原始 ZIP；这是经 receipt/partial 补偿的
执行边界，不宣称创建了源脚本的本地归档镜像或 quarantine 文件。

GDELT GKG 复核确认了固定源的 `.gkg.csv.zip`、27 列、UTC 范围内按时间升序取前
`max-files` 和 ZIP/UTF-8/列数检查语义；它复用已修正的文件 feed 核心，非对齐下界从首个
落入范围的 15 分钟快照开始，并在文件上限截断时显式报告。坏行按统一 runtime 规则局部
隔离，SHA-256、CRC 和行级问题随结果保留；CLI 返回命名 GKG 字段而不创建源脚本式 ZIP
归档、masterfilelist、preview 或 quarantine artifact，这一边界已写入 thin Skill。

GDELT Mentions 复核确认了固定源的 `.mentions.CSV.zip`、16 列、UTC 范围内按时间升序取前
`max-files` 和 ZIP/UTF-8/列数检查语义；它与 Events/GKG 共用同一有界实现，但保留独立
capability、闭合字段 Schema 和 mention-level discovery。非对齐范围、截断、坏行隔离、
SHA-256/CRC/行级问题处理与另外两个 feed 一致；CLI 不创建 ZIP 归档、masterfilelist、
preview 或 quarantine artifact，也不把 mention row 解释为独立文章、认可或已验证事件。

NASA FIRMS 复核确认了固定源的 8 个 source、31 个闭合 UTC 日期、5 日分片、area 交易估算、
可选 availability 校验、MAP_KEY 路径注入和共同 active-fire 字段；TypeScript 连接器补回了
源实现的重复 CSV header 诊断，并在保留可用 detection 的同时显式报告行/字段 partial。
CLI 将 bbox 进一步限制为非跨日界线且非全球扫描，并发布闭合的跨传感器公共字段；源脚本
的 standalone MAP_KEY transaction-status 探针、任意 sensor raw columns、raw JSON/log
artifact 继续作为明确边界，不由 thin Skill 复制。

Open-Meteo Air Quality 复核确认了固定源的最多 10 个坐标、92 个闭合日期、最多 16 个
hourly variables、domain/cell selection、多坐标顺序和 nullable aligned arrays。TypeScript
连接器补回 GMT 下每个闭合日期严格 24 个小时、时间严格递增以及 provider 零 UTC offset
校验；短时间轴、乱序、timezone drift、缺变量/单位或坏数值现在显式 partial。CLI 继续固定
公共 non-commercial endpoint 与 GMT，不迁入任意 timezone、optional customer API key、
endpoint override 或 raw artifact 参数。

Open-Meteo Flood 复核确认了固定源的最多 10 个坐标、366 个闭合日期、7 个官方 discharge
variables、cell selection、多坐标响应、严格递增日轴与可选 ensemble member 校验。TypeScript
连接器补回 provider GMT/零 UTC offset 校验，并将 member 识别从固定两位后缀恢复为源脚本的
`river_discharge_memberN` 数字后缀，同时拒绝不能安全表示的 member ID；输出 Schema 与 migration provenance
已同步。CLI 仍要求 ensemble 与 `river_discharge` 同时请求，并固定公共 non-commercial
endpoint/GMT，不迁入任意 timezone、optional API key 或 raw artifact 参数。

Open-Meteo Historical Weather 复核确认了固定源的最多 10 个坐标、366 个闭合日期、小时/日
变量上限、多坐标响应、完整日轴、严格递增和数组/单位校验。TypeScript 连接器将小时轴从仅
拒绝“超过窗口”修正为 GMT 下必须严格 `24 × 日期数`，并补 provider GMT/零 UTC offset
校验；短小时轴或 timezone drift 现在保留可用 section 并显式 partial。CLI 继续使用单一
受控 model、curated numeric variables 和固定 Celsius/kmh/mm，不迁入源脚本的 multi-model、
任意 timezone/unit、optional API key 或 raw artifact 调参。

## 2026-08-31 实现状态

- CLI PR #71 已合并到 `tiangong-ai/cli` 主分支，merge commit 为 `832e302`；
  TypeScript 7、data runtime、AirNow、Federal Register 以及 Execution Manifest /
  Discovery Metadata 分层均已进入源码主线。
- 当前公共 `@tiangong-ai/cli@0.0.54` 尚不包含 `data` 命令，因此仍未达到删除
  Skill 旧执行脚本和提交稳定 requirement/迁移 provenance 的门槛。
- Skills 仓库已增加 capability requirement/迁移 provenance 生成校验器及离线兼容测试。
  AirNow、EPA EIS、Federal Register、USGS Water IV、Open-Meteo Air Quality、Open-Meteo Flood、
  Open-Meteo Historical Weather、NASA FIRMS、OpenAQ、Regulations.gov Comments、
  Regulations.gov Comment Details、Regulations.gov Attachments、USBR Project Records、USBR RISE，以及 GDELT DOC、Events、GKG、Mentions 已在本地
  候选分支薄化；Bluesky Cascades、YouTube Video Search 与 YouTube Comments 也已在
  完成逐项语义/许可/安全审计后薄化。comments/detail 两个 Regulations.gov Skill 分别绑定同一
  `regulations-gov.comments` capability 的 search 与 fetch-details operation；attachments
  Skill 独立绑定 `regulations-gov.attachments/download` 与 artifact-output contract；四个
  GDELT Skill 分别绑定独立 capability；两个 YouTube Skill 分别绑定同一
  `youtube.public-content` capability 的 search-videos 与 fetch-comments operation。
  二十一项旧 Python connector 与重复 provider references 已移出候选 Skill，并共同纳入
  copy/symlink 安装 smoke。
- 当前本地候选包 `0.0.55` 只用于分支内兼容验证，不代表 npm 正式发布。PR 前必须用
  实际包含全部十九个 capability 的正式版本验证二十一个 requirement、重生一次仓库级
  provenance，并用该 npm 包重跑全部门禁。

## 2026-08-31 本地候选自洽性审计（不代表 EcoCouncil 迁移完成）

- Skills 目标仓库中实际存在 42 个 fetch/search/download 候选：21 个原子 provider
  Skill 已薄化，其余 21 个落入下文记录的内容/文件、产品或私有账户保留边界。该统计
  只说明目标仓库目录已分类，不覆盖 EcoCouncil 独有 Skill，也不是迁移完成证据。
- CLI 候选使用 Node 24、TypeScript 7.0.2 和版本 `0.0.55`，发布 19 个 capability；
  21 个薄 Skill 的 requirement 均不含 package version；仓库级 migration provenance 记录候选 `0.0.55`。
- 每个薄 Skill 只含 `SKILL.md`、`agents/openai.yaml` 和
  `references/tiangong-data-requirement.json`；原 Python connector、provider 配置、重复 API
  notes 和 OpenClaw 模板均不在生产 Skill 路径中。
- 21 个 requirement 已逐项验证 capability/operation contract major；仓库级 migration provenance 已对照当前候选的 execution manifest 与 operation 输入/输出 Schema
  digest；copy/symlink 隔离安装 smoke 已对当前全部 21 项、copy/symlink 两种模式和同一
  本地 CLI 候选包通过，且未携带 provider 凭证。EPA EIS 已通过
  独立 `quick_validate.py`、requirement verify 和 thin-skill contract；USBR Project Records
  已完成同样的独立验证；Regulations.gov Attachments 已完成本地 TypeScript artifact
  transaction、`quick_validate.py`、薄 Skill requirement contract 与 provenance verify。正式 npm
  包可用后仍必须对全部 21 项重跑同一 smoke，且不访问真实 provider。
- CLI 的 lint、typecheck、543 项全量测试、3 项 platform contract、coverage、npm pack、
  immutable setup pin audit 和 docpact 均通过；21 个薄 Skill 与 21 个明确保留 Skill 的
  `quick_validate.py` 均通过，requirement/provenance contract 与 docpact 也通过。
- 当前 19 capability/21 thin Skill 的最终候选已重跑门禁：CLI 在新 clean container 中
  通过 543 项测试，statement coverage 为 85.13%；21 项 requirement verify 与 1 份精确 migration provenance verify、独立
  `quick_validate.py` 以及 copy/symlink 两种模式共 42 次 provider-offline 安装 smoke 均
  通过。
- Skills 数据分支相对 `upstream/main` 对 `academic-paper-download` 为零差异；其全量 cold
  gate 与干净 `upstream/main@4104e52` 均可复现同一基线失败：browser snapshot 使用进程
  墙钟，与容器文件系统 `mtime` 不可比较，导致论文下载 discovery 76 项中 1 failure、
  4 errors。该问题已拆到独立本地前置分支 `codex/academic-paper-filesystem-clock@576a6cb`，
  在另一新 cold container 中以 77 项通过、1 项显式网络 smoke 跳过转 GREEN；含该前置
  修复的数据迁移集成态 `codex/atomic-data-integration@bb53a1b` 也已在第三个新 cold
  container 中通过全部 Node、Python 与 shell suites。
- CLI Research adapter 与必要的 Auto Research 变更已作为独立后续工作包实现：动态
  registry 投影、进程内 data execution、core receipt parity、Research receipt/ledger/
  budget/credential/artifact 绑定和 native packet 入口均有单独 clean-room TDD；这份证据
  仍与此前 21 个原子 Skill 的迁移完成证据分离。
- 后续可靠性补强把 provider 缺口、显式运行限制和 Agent context 投影拆成独立维度；完整
  core result 仍一次持久化，Auto Research 通过 receipt-bound cursor 本地续读，不重复消耗
  provider request。YouTube comments 显式区分 `top-level-only` 与 `all-visible`，并报告
  request budget、已完整展开 thread 与未展开 thread ID。Regulations.gov 本轮暂缓且不改动。
- 当前只有本地分支提交；在维护者统一审阅并明确确认前，不推送实现分支、不创建 PR。

## 与 CLI 的同步顺序

1. CLI 与 Skills 计划 PR 同时开放，先确认两仓所有权、命令候选、迁移范围和验收。
2. CLI 独立完成 TypeScript 7 基线 PR。
3. CLI 完成空 data runtime/机器 contract PR，再实现首批 connectors。
4. CLI 发布候选版本，导出 canonical manifest/Schema digest。
5. Skills 首批迁移 PR 使用候选包做验证；正式 CLI 发布后验证 requirement、更新仓库级 provenance 并合并。
6. CLI 接入 Research adapter；Auto Research Skill 同步遵循其强制 clean-room RED/GREEN
   门禁。状态：本地实现与文档已完成，待两个仓库各自全量门禁和维护者审阅。

因此不是“Skills 先合并、CLI 后跟进”。Skills 可以提前评审语义和迁移 diff，但 CLI
机器基座必须先确定、先可测试、先可发布。

## 全量候选清单

以下清单用于分类，不代表全部批准迁移。

### A. 原子 provider connector 候选

- `airnow-hourly-obs-fetch`
- `federal-register-doc-fetch`
- `usgs-water-iv-fetch`
- `nasa-firms-fire-fetch`
- `open-meteo-air-quality-fetch`
- `open-meteo-flood-fetch`
- `open-meteo-historical-fetch`
- `openaq-data-fetch`
- `regulationsgov-comments-fetch`
- `regulationsgov-comment-detail-fetch`
- `regulationsgov-attachments-fetch`
- `epa-eis-records-fetch`
- `usbr-project-records-fetch`
- `usbr-rise-fetch`
- `gdelt-doc-search`
- `gdelt-events-fetch`
- `gdelt-gkg-fetch`
- `gdelt-mentions-fetch`
- `bluesky-cascade-fetch`
- `youtube-video-search`
- `youtube-comments-fetch`

### B. 审计后保留专用内容/媒体/下载边界

- `ai-tech-rss-fetch`
- `ai-tech-fulltext-fetch`
- `eceee-news-fulltext-fetch`
- `sustainability-rss-fetch`
- `sustainability-fulltext-fetch`
- `figshare-data-download`
- `academic-paper-download`

RSS Skill 的核心包括任意 feed/OPML intake、SQLite subscription state、dedupe 与
incremental sync；fulltext Skill 的核心包括 HTML/body acquisition、正文解析、retry queue
与持久化。Figshare 的交付物是浏览器获取的本地文件 artifact；论文下载是带合法开放获取
路径、浏览器 handoff、PDF/hash/manifest/provenance 的 Research companion。把这些行为
替换成无状态 JSON connector 会实质丢失功能并引入不同的动态 URL、内容安全、文件与状态
合同，因此它们不是待办迁移，而是经审计后继续保持现有专用边界。

### C. 保持现有产品边界，默认不迁入 data runtime

- `tiangong-kb-course-search`
- `tiangong-kb-course-fulltext-fetch`
- `tiangong-kb-edu-search`
- `tiangong-kb-esg-search`
- `tiangong-kb-patent-search`
- `tiangong-kb-report-search`
- `tiangong-kb-sci-search`
- `tiangong-kb-textbook-search`
- `dify-knowledge-base-search`
- `fetch-abstract-to-kb`
- `fetch-meta-from-kb`
- `fetch-meta-to-kb`

它们属于 Tiangong KB、Dify 或现有 ingest/search 产品面，应在各自命令边界演进。

### D. 单独安全评审

- `email-imap-fetch`
- `email-imap-full-fetch`

私有邮箱、账户凭证和正文处理不作为公共数据 connector 的默认迁移对象。

每次清单更新都要记录：官方来源、许可/ToS、认证、operation、输入/输出、分页/分块、
大小和速率限制、partial 语义、现有 fixture、真实使用者、维护成本、目标分类和决定。

## 首批：AirNow + Federal Register

### 选择理由

- AirNow Hourly Obs：无需凭证，覆盖有界 UTC 窗口、多文件 CSV、空间/污染物过滤、来源
  文件 lineage 和 partial file failure。
- Federal Register Documents：无需凭证，覆盖稳定 query 编码、分页 JSON、记录上限、
  官方政策元数据和“只取搜索结果、不抓正文”的边界。
- 两者共同验证两种有代表性的 transport/shape，同时避免首批评审被真实 secret 和
  provider 账户状态阻塞。
- credential/redaction 在 CLI foundation 由 synthetic connector 强制测试；NASA
  FIRMS 在下一批验证真实 logical credential 路径。

### 迁移前提

- CLI TypeScript 7 和基础 data contract 已合并。
- 两个 CLI connector 的 manifest、Schema、fixtures、错误和 receipt 测试已通过。
- 可安装的 CLI 候选包能导出 canonical describe contract。
- 现有 Skill 的外部行为、来源说明和限制已盘点；旧 Python 只读。

### Skill 变更

每个首批 Skill：

1. 用 `skill-creator` 工作流更新 `SKILL.md` 和生成的 `agents/openai.yaml`。
2. 新增由 CLI describe 生成的 `references/tiangong-data-requirement.json`。
3. 删除已由 CLI Discovery Metadata 发布的数据源/API/覆盖/许可/限制副本；只在确有
   任务型选择语义时保留非重复 reference。凭证解析转由 CLI 时删除重复配置。
4. 将 Python fetch 脚本、OpenClaw chaining 模板和旧 raw artifact 约定从生产路径移除。
5. 示例只使用精确 capability/operation 和文件/stdin 输入，不放 secret 到 argv/JSON。
6. README/marketplace 只有在可用性、安装或发现面实际变化时才同步更新。

### 旧实现行为对照

首批迁移按已评审的 capability v1 边界重写，不把旧 Python 命令行逐参数兼容层带入
Skill。以下差异必须明确，不能被误写成无损命令替换：

| Skill                          | 保留到 CLI capability 的核心行为                                                                                                                                                       | 有意收敛或替代的旧行为                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AirNow                         | UTC 小时文件规划、bbox/time/pollutant 过滤、站点小时记录、逐文件 lineage、缺失/坏文件 partial                                                                                          | 任意 endpoint/path/user-agent/重试调参、Skill `check-config`、dry-run、日志文件和 raw artifact 写入被 CLI manifest、`data doctor`、统一 limits、run-result/receipt 取代；非整点输入不再静默截断而是拒绝                                                                                                                                                    |
| EPA EIS Records                 | 四种官方 common search、显式官方 UI search URL、`submissionsTable` 的 title/CEQ/unique ID/type/date/agency/state/detail/download cues、跨搜索去重和 later-search partial | 删除任意 base URL、http URL、重试/节流/user-agent/log/dry-run/output 与 raw artifact 写入；显式 URL 只允许官方 HTTPS origin/path，HTML 漂移 fail closed。CLI 不下载链接文件，也不判断 NEPA/EIS adequacy、legal sufficiency、environmental effects、compliance 或 policy responsibility |
| Federal Register               | publication date、term、agency、type、topic、docket、RIN、稳定查询编码、有界分页/记录、空结果/截断/later-page partial                                                                  | `section`、`significant` 输入过滤、任意 `fields` 投影和 `executive_order_number` 排序不属于首个 capability v1；Skill 不再宣称支持。每次调用的 page/record 上限改用 run-request 顶层 `limits` 且只能降低 manifest 上限；dry-run、日志和 raw artifact 路径不再保留                                                                                           |
| USGS Water IV                  | bbox 或最多 100 个 sites、严格正 ISO duration 或显式 window、最多 8 个参数及 site type/status/agency 过滤、WaterML series/value 归一化、qualifier/provisional、no-data 过滤和坏 row/series partial | 旧脚本允许本地 env/argv 覆盖 endpoint、重试、节流、上限、user-agent、日志和 `file://` fixture，并提供 `check-config`、dry-run、raw artifact 写入；这些改由 CLI endpoint policy、manifest limits、static doctor、fixture tests 和 run-result/receipt 取代。官方 legacy 上限把旧 Skill 的 200 sites 收紧为 100；零 duration、悬空 `T` 与 week/其他 component 混用 fail closed。明确部分未质控运行数据的 120-day 限制、2026-H2 可能 degradation/blackout 及 2027-Q1 decommission 风险 |
| Open-Meteo Air Quality         | 最多 10 个坐标、92 个闭合日期、16 个官方 hourly variables、domain/cell selection、单次多坐标响应、GMT 下严格 24 小时/日且递增、nullable aligned arrays 和坐标/变量 partial                                          | timezone 固定为 GMT 并校验 provider 零 UTC offset；删除任意 timezone、endpoint、API key、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证；商业 customer endpoint/API key 需要独立 capability 评审。输出改为 location-hour 列式结果和统一 run-result/receipt，不兼容旧 snake_case payload                                        |
| Open-Meteo Flood               | 最多 10 个坐标、366 个闭合日期、7 个官方 daily discharge variables、cell selection、严格递增完整 GMT 日轴、variable-width `memberN` ensemble fields、nullable aligned arrays 和坐标/变量/member partial                      | timezone 固定为 GMT 并校验 provider 零 UTC offset，ensemble 必须同时请求 `river_discharge`；删除任意 timezone、endpoint、API key、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证。输出按 location-day 计数并显式区分 requested/river-grid coordinate，不把 GloFAS simulated discharge 误写成 gauge observation、告警或严重度   |
| Open-Meteo Historical Weather  | 最多 10 个坐标、366 个闭合日期、一个受控 model、12 个 curated hourly 与 12 个 curated numeric daily variables、GMT 下严格 24 小时/日与完整日轴、多坐标 nullable aligned arrays 和坐标/section/变量 partial         | timezone 与单位固定为 GMT/摄氏度/km/h/mm 并校验 provider 零 UTC offset；两个变量数组显式传入且至少一方非空。删除任意 timezone、endpoint、API key、multi-model、任意 units、重试、节流、user-agent、日志、dry-run 和 raw artifact 调参。公开 endpoint 明确为 non-commercial 且无凭证；输出按 location 内 hourly 后 daily 的时间行计数，明确 reanalysis/model grid 并提示长期趋势使用 ERA5 或 ERA5-Land |
| NASA FIRMS Active Fire         | 一个 reviewed source、非跨日界线 bbox、最多 31 个闭合 UTC 日期、可选 availability probe、五天分片、transaction/record cap、重复/不一致 header 与 row validation、公共 MODIS/VIIRS/Landsat detection 字段和 chunk/row partial | `NASA_FIRMS_MAP_KEY` 只由 CLI 从进程环境解析并作为受保护 path segment 注入；删除 Skill-local env 文件、standalone MAP_KEY status probe、endpoint/retry/throttle/user-agent/log/dry-run/raw artifact 调参、任意 sensor raw columns 和 OpenClaw fan-out。输出改为统一 run-result/receipt，不把 thermal anomaly 误写成 fire perimeter、burned area、incident、cause 或 alert                                             |
| OpenAQ Air Quality             | 有界 location discovery、provider/owner/sensor/parameter/license/coverage metadata，以及单 sensor、最长 366 天的 raw/hourly/daily measurements、显式 flag/null/coverage interval、local-day daily 语义、稳定 ID 分页和 later-page partial          | 任意 v3 path/query、standalone 全局 metadata catalog、API/S3 自动路由、S3 prefix/list/download、Skill-local region/bucket/endpoint 配置和 Python client/router 被移除；`OPENAQ_API_KEY` 只由 CLI header 注入。批量 archive 文件转入单独 content/download 审计；输出不提供 AQI、健康/监管判断、跨 sensor 聚合、单位转换或来源归因                                                             |
| Regulations.gov Comments       | posted 或 last-modified 二选一的最长 366 天窗口、agency/comment-on/search-term 收窄、稳定 JSON:API 分页、comment ID 与 provider 可用的日期/标题/withdrawal metadata、显式 null 和 later-page partial | `REGGOV_API_KEY` 只由 CLI header 注入；last-modified filter 是 provider 标注的 beta 能力。移除旧 `docketId`/`documentType`/`subtype` filter、任意 sort expression、`candidate_corpus_summary` heuristic、endpoint、重试/节流/log/dry-run、JSONL 写入和 quarantine。结果明确不是代表性公众意见、投票或统计 sentiment，不提供 comment post/modify、detail body 或 attachment download |
| Regulations.gov Comment Detail | 最多 100 个显式 comment ID、caller 顺序、comment/docket/document linkage、日期、withdrawal/restriction、组织上下文、duplicate count、可选且可为 null 的 modify/attachment metadata 和 per-ID partial | 将旧 300-ID batch cap 收紧到 100，并删除本地文件 ID 解析、任意 endpoint、重试/节流/log/dry-run、raw/JSONL/quarantine 写入；CLI 以 allowlist 排除姓名、邮箱、电话、地址与 locality 等个人 profile 字段，只返回 attachment metadata/link，不下载 bytes；缺失 modify/format/link/size 保留为 null，不解释为 false/zero，也不提供法律判断或代表性 sentiment |
| Regulations.gov Attachments    | 最多 20 个显式 comment ID、可选 exact attachment allowlist、官方 comment-detail attachment metadata、固定 download origin、有界 files/bytes、SHA-256、相对 manifest 与 partial file coverage | 删除旧脚本的任意 base/file URL、当前 OpenAPI 未定义的独立 attachment endpoint、Skill-local retry/log/output 与直接文件写入；`REGGOV_API_KEY` 只发往 API endpoint，CLI 通过显式 `--artifact-dir` 事务暂存、校验后 no-overwrite commit，绝对路径不进入结果/receipt。文件仍是不可信 bytes，不提供 malware scan、打开、OCR、text extraction、stance、法律或 evidence 结论 |
| USBR Project Records            | 显式 `www.usbr.gov` 页面、title/meta description、响应 digest/bytes/safe headers、同 origin 链接去重、文档顺序/type 与 later-page partial                                      | 删除 URL 文件读取、任意/非 HTTPS/外部 host、重试/日志/dry-run/output 和 raw artifact 写入；CLI 不做站内搜索、递归 crawling 或链接下载，不把链接线索当作已审阅证据，也不推断法律、政策、运行、环境影响或治理责任                                                                                  |
| USBR RISE                      | 有界 provider-page catalog discovery、client-side terms/location/parameter/source 过滤、显式 item ID result fetch、可选 UTC/location/parameter/order/item-metadata、item/page partial | 删除任意 endpoint、Skill-local env/retry/throttle/log/dry-run/output、operator-supplied metadata override 和 raw artifact 写入；CLI 返回统一 result/receipt，保留 unit/timestep/transformation/source/disclaimer，且不把 scan order 当排名、缺失行当物理不存在，或推断 shortage、compliance、causality 与 governance responsibility |
| GDELT DOC                      | 有界 relative/absolute window、受控 article-list/timeline modes、GDELT query syntax、模式化 JSON 结果和 provider truncation/空结果语义                                               | 删除任意 DOC mode/format/额外 query 参数、endpoint/retry/throttle/log 配置和 raw artifact 写入；只返回文章 metadata/link 或聚合时间线，不下载正文，也不把自动 tone/count/ranking 解释为代表性、事实或因果证据                                                                                                                                                |
| GDELT Events                   | latest 或任意秒级 inclusive UTC range（从首个落入窗口的 15 分钟快照开始）、最多 20 个 source files、ZIP/MD5/CRC/UTF-8/61-column 校验、机器编码 event rows 与来源 lineage                                                        | 删除 masterfilelist 下载、dry-run、任意 expected-columns、output/quarantine/log 路径和持久 ZIP；CLI 返回有界内存归一化 rows 与统一 receipt，不把 coded event 当作验证事实、唯一事件或正文                                                                                                                                                                      |
| GDELT GKG                      | latest 或任意秒级 inclusive UTC range（从首个落入窗口的 15 分钟快照开始）、最多 20 个 source files、ZIP/MD5/CRC/UTF-8/27-column 校验、GKG annotations 与 document lineage                                                       | 删除 masterfilelist 下载、dry-run、任意 expected-columns、output/quarantine/log 路径和持久 ZIP；CLI 返回有界内存归一化 rows，不把 machine-extracted themes/entities/locations/tone 当作已验证知识、正文或 sentiment ground truth                                                                                                                              |
| GDELT Mentions                 | latest 或任意秒级 inclusive UTC range（从首个落入窗口的 15 分钟快照开始）、最多 20 个 source files、ZIP/MD5/CRC/UTF-8/16-column 校验、mention-level provenance/confidence/source linkage                                       | 删除 masterfilelist 下载、dry-run、任意 expected-columns、output/quarantine/log 路径和持久 ZIP；CLI 返回有界内存归一化 rows，不把 mention 当作独立 endorsement、唯一文章、验证事件或正文                                                                                                                                                                    |
| Bluesky Cascades               | public search/author/custom/list seed source、optional UTC window、seed post normalization、visible `getPostThread` reply topology、blocked/not-found node 与 per-thread partial                               | 删除 optional auth/base URL fallback、Skill-local env、retry/throttle/log/dry-run、JSON/JSONL artifact 和 OpenClaw 配置；CLI 统一 bounded HTTP/receipt，只开放 public AppView，把 ranking/feed/indexing/moderation/counters 明确为可变快照，不宣称 archive completeness、代表性、事实、身份、sentiment 或 causal diffusion evidence                              |
| YouTube Video Search           | query/channel/published/order/region/language/safe-search、十种 video filter、最多 10 个 search pages / 250 个候选、search rank/page/position、`videos.list` 必选 detail enrichment、丰富 snippet/content/status/live fields、public comment/view threshold、缺失 detail partial | `YOUTUBE_API_KEY` 只经 CLI `X-Goog-Api-Key` header 注入；删除 query-string key、endpoint/retry/throttle/log/dry-run、JSONL/quarantine/artifact 输出。`publishedBefore` 按当前 provider 语义为 inclusive；删除只适用于 channel search 的 `videoCount` order，并收紧为严格 RFC 3339 UTC。CLI 预留 detail request budget，不下载 media/caption/transcript/thumbnail，只返回 thumbnail URL metadata；独立 Search Queries quota 和 2026-08-24 `viewCount` 口径断点显式提示，不把 ranking 或统计当作代表性、endorsement、truth 或 sentiment |
| YouTube Comments               | 1–50 个严格 11 位显式 video IDs、成对 UTC half-open `[start,end)` window 与 published/updated 选择、thread order、只作用于 top-level 的 search terms、`comments.list` 完整可见 replies 分页、parent/video linkage validation、per-video partial | 删除本地 txt/json/jsonl ID 文件解析、HTML text mode、endpoint/retry/throttle/log/dry-run、JSONL/quarantine/artifact 输出；plainText 固定，API key 只由 CLI header 注入。per-video thread 与 per-thread reply cap 只截断本地范围，operation-wide request/record cap 才停止全局；embedded replies 不作为完整数据，reply 关联错位显式 partial；评论不被解释为代表性 opinion、身份、事实、人口属性或 sentiment ground truth |

新结果是 `tiangong.data.run-result.v1`，字段命名和审计结构以 operation output Schema
及 core receipt 为准，不承诺旧 Python payload 的 snake_case/raw-artifact 兼容。需要旧版
已收敛过滤或输出字段的消费者必须先推动新的 capability/operation 版本评审，不能绕回
已删除脚本。

### 首批验收

- 每个 requirement 的 capability/operation contract major 与候选/正式包兼容。
- exact package 与 integrity 由调用方/workspace runtime lock 负责；Execution Manifest
  与 operation Schema digest 只出现在仓库级 migration provenance。Discovery Metadata
  文案变化和普通兼容 CLI 发布不得触发全部 Skill 锁步更新。
- `quick_validate.py <skill-path>` 和生成 agent metadata 校验通过。
- 离线 requirement 测试覆盖缺失 capability/operation 与 contract-major 漂移；
  migration provenance 测试单独覆盖 exact manifest/Schema digest 漂移。
- copy/symlink 安装 smoke 使用临时 project/HOME，只运行 version、catalog、describe、
  静态 doctor 和 fixture/local dry contract；不访问真实 provider。
- 仓库中不再存在首批 provider 的第二份可执行业务逻辑。

## 后续批次

### 批次 2：时序/空间与凭证

USGS Water IV、Open-Meteo Air Quality、Open-Meteo Flood、Open-Meteo Historical
Weather、NASA FIRMS、OpenAQ，以及 Regulations.gov comments/detail/attachments 三个语义入口已在
本地完成 CLI connector 与 Skill 薄化。comments/detail 两个 Skill 共享一个 capability；
attachments 使用独立 capability 和受控本地 artifact contract，三者保持独立意图入口与
单 operation requirement。

USGS 已验证 100-site/25-square-degree/8-parameter 边界、严格正 duration、WaterML
series/value、qualifier/provisional、部分运行数据 120-day 限制，以及 2026-H2 可能
degradation/blackout 与 2027-Q1 退役语义；Open-Meteo
Air Quality 已验证模型网格、GMT 列式多变量数据、public/commercial endpoint 分离和
attribution 语义；Flood 已验证 GloFAS river-grid、forecast-only statistics、ensemble
members 与非 gauge/alert 边界；Historical 已验证受控单模型、hourly/daily 双粒度、
reanalysis 与 station observation 区分，以及跨年代模型一致性提示；NASA FIRMS 已验证
path-segment logical credential、quota estimate、CSV chunk 与 hotspot/non-perimeter 边界；
OpenAQ 已验证 header credential、官方 `monitor`/`mobile` filter、显式 ID 排序、location
discovery、单 sensor raw/hourly/daily、provider 合法 null/flag/coverage interval、daily
local-day 边界、双 operation requirement、source-specific attribution 和 S3 download 分层；
旧版 standalone metadata catalog 与任意 path/query 被明确收敛，S3 archive 进入独立
content/download 能力 backlog，二者均不再被误报为当前 atomic Skill 能力或未完成迁移项；Regulations.gov
已验证 provider-auth、JSON:API pagination、beta last-modified filter 的 Eastern wall-clock
转换、provider 合法稀疏 search/detail/attachment metadata 的显式 null、个人字段 allowlist、
attachment metadata 与 per-ID partial；旧 search filters、任意 sort 与 heuristic summary
均作为有意收敛边界记录。每个 connector 单独批准，不因共享 provider
品牌而把多个 operation 合成一个巨型 Skill。

Regulations.gov Attachments 已完成 `regulations-gov.attachments/download` 与
`regulationsgov-attachments-fetch`：metadata 只通过官方 comment detail 的
`include=attachments` 获取，文件只从精确 `downloads.regulations.gov` origin 下载；CLI
要求显式绝对现有目录、隐藏暂存、校验后 no-overwrite commit，并只返回相对名称、digest、
bytes 和 manifest。Skill 只负责 comment/attachment ID 选择、目录纪律、不可信文件提示与
下游安全/提取工作流分流。

USBR RISE 已作为下一项独立 capability 完成：`discover-items` 保留 provider page scan
order 并应用 client-side filter，`fetch-results` 只接受 grounded item IDs；两者共享官方
keyless API scope，但拥有独立闭合 Schema。对应 `usbr-rise-fetch` 只保留 item 选择、
缺口解释和上层判断边界，不再携带 Python connector、artifact 输出或 endpoint 调参。
CLI clean container 492 项全过，Skill requirement/provenance、`quick_validate.py` 与 copy/symlink 安装
smoke 已通过；正式 migration provenance 仍须等待包含最终 21 项迁移结果的精确 CLI 发布版本。

EPA EIS 已完成 `epa.eis-records/search` 与 `epa-eis-records-fetch`：保留四种官方
common search 和 UI-created search URL，后者只接受官方 HTTPS origin/path。CLI 使用
TypeScript HTML tokenizer 解析 result table，保留 CEQ/provider ID、类型、日期、机构、
州、detail/download cues，明确区分 0-item 与 markup drift，并对 record cap 和后续搜索
失败给出 truncation/partial。clean container 499 项全过；Skill 不复制 parser，只负责
搜索选择、空结果解释、文档未下载边界和上层判断纪律。

USBR Project Records 已完成 `usbr.project-records/fetch` 与
`usbr-project-records-fetch`：只接受显式 `https://www.usbr.gov` 页面，保留页面响应
provenance 和同 origin 链接清单，不跟随或下载链接；page/global record/per-page link cap
与后续页面失败均显式报告。CLI clean container 505 项全过；Skill 只负责页面选择、链接
线索解释、与 RISE 的能力分流及上层判断边界。

### 批次 3：GDELT 与内容/社交来源

GDELT 已决定保持四个独立 capability：DOC 搜索的 API/模式化聚合语义与三个文件 feed
不同；Events、GKG、Mentions 共享 CLI 内部有界 ZIP/TSV 核心，但分别拥有闭合输出 Schema
和独立发现语义。四个 Skill 已在本地候选分支完成薄化、stable requirement 和统一
安装 smoke 接入。

Bluesky 与 YouTube 的审计结论是：它们的公开、闭合、只读 API operation 适合 data
runtime。CLI 已完成 `bluesky.public-posts/fetch-cascades` 以及
`youtube.public-content/search-videos|fetch-comments`；三个 Skill 已薄化、与同一本地候选包完成兼容复验并进入统一安装 smoke。YouTube key 仅经 `X-Goog-Api-Key` header 注入；comments
operation 使用 `comments.list` 展开 replies，不信任 embedded reply sample。固定 EcoCouncil
提交的 17 个原有候选已全部完成逐项源语义复核；连同补齐的 4 个缺失能力，21 项迁移主体
均已落到本地候选树并通过最终统一安装、数据专项门禁及含独立前置修复的集成 cold gate。
合并前仅剩正式 CLI 版本发布后的 requirement 复验与仓库级 provenance 重生。

RSS/fulltext、Figshare 与 academic paper 的审计结论相反：它们分别拥有持久订阅/正文队列、
浏览器文件 artifact 或 Research acquisition/provenance 核心，因此继续保持现有专用实现，
不是未完成的原子迁移。

### 不迁移/退役

没有真实消费者、API/许可不稳定、无法获得可维护 fixture、长期失败或与既有 Tiangong
产品命令重叠的能力，可以保留、合并或退役。该决定单独记录，不以数量对等为目标。

## PR 拆分与依赖

建议序列：

1. Skills plan PR：本文、架构、docpact 路由；不改 Skill。
2. CLI plan PR：与 Skills plan 同步评审。
3. CLI TS7、foundation、pilot PRs：先合并并产出候选/正式包。
4. Skills pilot PR：可在候选包出现后以 draft 开放，正式包发布后验证 requirement、更新仓库级 provenance 并合并。
5. CLI Research adapter PR 与必要的 Auto Research Skill PR：单独 clean-room TDD。

若维护者要求每个仓库只保留一个实现 PR，仍遵守同一依赖：两边同时审阅，CLI 先合并/
发布，Skills 后合并。不得使用分支 commit 作为长期生产 pin，也不得让 CLI 发布审计依赖
尚未合并的 Skills commit。

## 验证流程

计划/治理变更：

```bash
docpact validate-config --root . --strict
docpact lint --root . --worktree --mode enforce
```

实际 Skill 变更还必须：

- 按 `AGENTS.md` 完整读取并使用 `skill-creator`；
- 对每个目标运行 `scripts/quick_validate.py <skill-path>`；
- 运行新增的 requirement/provenance contract 和隔离安装 smoke；
- 运行受影响脚本/引用/agent metadata 测试；
- Auto Research 或直接 evidence wrapper 变更必须在相互独立的 clean container 中先
  观察 RED、再转 GREEN，PR 前运行 cold gate。

requirement/provenance 工具的离线契约测试：

```bash
node --test scripts/tests/data-skill-binding.test.mjs
```

隔离 copy/symlink 安装 smoke：

```bash
TIANGONG_DATA_SKILLS_RUN_INSTALL_SMOKE=1 \
TIANGONG_DATA_CLI_VERSION=X.Y.Z \
TIANGONG_DATA_CLI_PACKAGE=@tiangong-ai/cli@X.Y.Z \
node --test scripts/tests/data-skill-install-smoke.test.mjs
```

它会清除 provider 凭证，只运行 version、catalog、describe、static doctor 和本地
结构化阻断请求，不访问 AirNow、Bluesky、EPA EIS、FederalRegister.gov、GDELT、NASA
FIRMS、OpenAQ、Open-Meteo、Regulations.gov、USBR、USGS WaterServices 或 YouTube。

稳定 requirement 只在 Skill 所需的 capability/operation contract major 或
`requiredFeatures` 改变时生成；
普通 CLI patch/minor 发布无需改写 21 个文件：

```bash
node scripts/data-skill-binding.mjs generate \
  --skill airnow-hourly-obs-fetch \
  --capability airnow.hourly-observations \
  --operations fetch-hourly \
  --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify \
  --requirement airnow-hourly-obs-fetch/references/tiangong-data-requirement.json \
  --cli-version X.Y.Z
```

若 Skill 依赖同一 contract major 中的特定行为，用分号分隔 operation、逗号分隔 feature：

```bash
node scripts/data-skill-binding.mjs generate \
  --skill youtube-comments-fetch \
  --capability youtube.public-content \
  --operations fetch-comments \
  --required-features fetch-comments=youtube.reply-strategy \
  --cli-version X.Y.Z
```

本次删除 21 个旧 provider runtime 的 exact release 资格只集中生成、验证一次：

```bash
node scripts/data-skill-binding.mjs generate-provenance \
  --root . --cli-version X.Y.Z
node scripts/data-skill-binding.mjs verify-provenance \
  --root . --cli-version X.Y.Z
```

`generate-provenance` 会读取全部 per-Skill requirement，并把 package version、manifest
与 operation Schema digests 写入 `scripts/data-skill-migration-provenance.json`。该文件
是迁移/发布验证证据，不随单个 Skill 安装，也不参与日常 runtime compatibility；实际
workspace build 继续由各自的 runtime lock 和 integrity 负责。

## 回退和删除纪律

- 计划 PR 不删除业务文件。
- Skill 实现 PR 只有在正式 CLI 版本可安装、requirement/provenance 已验证且迁移 smoke 通过后才删除
  旧脚本。
- CLI connector 若发布后回退，调用方 runtime lock 回到仍受支持的正式 CLI；Skill requirement 仅在 contract major 改变时更新；不能静默
  指向 branch 或本地 checkout。
- 不清理旧仓库、旧 worktree 或用户未提交内容；归档/删除需要另一次明确授权。

## 准备完成定义

准备完成是指：最新主分支、干净 worktree、两仓权威计划、候选清单、首批范围、TS7
依赖、PR 顺序、验证和回退门槛均已持久化并通过治理检查。准备完成后停止，等待下一次
实现授权。
