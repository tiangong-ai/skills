---
docType: guide
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When installing, updating, or using the Tiangong AI reusable skills repository."
whenToUpdate: "When install commands, target agents, scope behavior, environment variables, or available skill guidance changes."
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - README.zh-CN.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-04-29
lastReviewedCommit: 7bcc1db8d066fa546ffa6e5c9c4b0def46c81ca1
---

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
  npx skills add https://github.com/tiangong-ai/skills --skill sci-journals-hybrid-search --skill dify-knowledge-base-search
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

## Environment Variables

Environment requirements live with each skill. Before using a skill that calls
an external service, read that skill's `references/env.md` when present.
