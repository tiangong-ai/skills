# AGENTS.md

适用范围：本仓库根目录及全部子目录。

## 强制规则：使用 Codex 创建或更新 Skill

1. 必须参考 Codex 内置 `skill-creator` 指南并按其要求执行。
2. 默认参考路径：
   - `/root/.codex/skills/.system/skill-creator/SKILL.md`
   - 若运行环境不同，使用等价的 `$CODEX_HOME/skills/.system/skill-creator/SKILL.md`
3. 当任务是“新建 skill / 修改 skill / 规范化 skill”时，优先触发并遵循 `skill-creator` 流程。
4. 如本文件与 `skill-creator` 细则存在冲突，以 `skill-creator` 为准。

## Skill 实施流程（必须遵守）

1. 明确 skill 的触发场景与典型用例。
2. 规划可复用资源（`scripts/`、`references/`、`assets/`）。
3. 新 skill 优先使用 `init_skill.py` 初始化目录与模板。
4. 按规范填写/更新 `SKILL.md` 与资源文件。
5. 生成或更新 `agents/openai.yaml`（使用官方脚本与 `--interface` 参数）。
6. 运行 `quick_validate.py <skill-path>`，修复后直到通过。

## Skill 文件规范

1. Skill 目录名仅使用小写字母、数字、连字符（hyphen-case），且应小于 64 字符。
2. 每个 skill 至少包含 `SKILL.md`。
3. `SKILL.md` 的 YAML frontmatter 仅允许：
   - `name`
   - `description`
4. `description` 必须写清楚“做什么 + 何时使用（触发条件）”。
5. 仅在确有必要时创建 `scripts/`、`references/`、`assets/`，避免冗余文件。

## 交付前检查

1. 校验通过：`quick_validate.py` 返回通过结果。
2. 若新增脚本：至少运行一次代表性测试，确认可执行与输出合理。
3. 变更说明中明确列出：新增/修改的 skill 文件与校验结果。
