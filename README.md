# Tiangong AI Skills

Repository: https://github.com/tiangong-ai/skills

Use the `skills` CLI from https://github.com/vercel-labs/skills to install, update, and manage these skills.

## Install the CLI
```bash
npm i skills -g
```

## Install
- List available skills (no install):
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --list
  ```
- Install all skills (project scope by default):
  ```bash
  npx skills add https://github.com/tiangong-ai/skills
  ```
- Install specific skills:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill flow-hybrid-search --skill process-hybrid-search
  ```

## Target agents and scope
- Target specific agents:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -a codex -a claude-code
  ```
- Install globally (user scope):
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -g
  ```
- Scope notes:
  - Project scope installs into `./<agent>/skills/`.
  - Global scope installs into `~/<agent>/skills/`.

## Install method
- Interactive installs let you choose:
  - Symlink (recommended)
  - Copy

## Update and verify
- List installed skills:
  ```bash
  npx skills list
  ```
- Check for updates:
  ```bash
  npx skills check
  ```
- Update all skills:
  ```bash
  npx skills update
  ```

## Environment variables
- `sci-journals-hybrid-search`
  - `TIANGONG_AI_APIKEY`: required. Used as `x-api-key` for the Supabase edge function.
- `dify-knowledge-base-search`
  - `DIFY_API_BASE_URL`: required. Example `https://api.dify.ai/v1`.
  - `DIFY_DATASET_ID`: required. Dify dataset (knowledge base) ID.
  - `DIFY_API_KEY`: required. Used as `Authorization: Bearer <DIFY_API_KEY>`.
