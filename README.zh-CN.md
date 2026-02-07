# 天工 LCA Skills

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
  npx skills add https://github.com/tiangong-ai/skills --skill flow-hybrid-search --skill process-hybrid-search
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
  - 项目级安装到 `./<agent>/skills/`.
  - 全局安装到 `~/<agent>/skills/`.

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
- `sci-journals-hybrid-search`
  - `TIANGONG_AI_APIKEY`: 必填。用于 Supabase edge function 的 `x-api-key`。
- `dify-knowledge-base-search`
  - `DIFY_API_BASE_URL`: 必填。示例 `https://api.dify.ai/v1`。
  - `DIFY_DATASET_ID`: 必填。Dify 数据集（知识库）ID。
  - `DIFY_API_KEY`: 必填。用于 `Authorization: Bearer <DIFY_API_KEY>`。
