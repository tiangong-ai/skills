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
lastReviewedAt: 2026-08-09
lastReviewedCommit: 3dbc264d97573060b5c2e5fa08a4ab8242db87dc
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
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-kb-sci-search
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
  - Codex is a universal agent: project scope uses `./.agents/skills`, and
    global scope uses `$HOME/.agents/skills`. `CODEX_HOME` does not change the
    `skills@1.5.22` global destination.
  - Claude Code project scope uses `./.claude/skills`; global scope uses
    `$CLAUDE_CONFIG_DIR/skills` when set, otherwise `$HOME/.claude/skills`.
  - Other agents have their own directories; inspect the exact path reported by
    the pinned `skills` CLI rather than deriving `~/<agent>/skills` by analogy.

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

## Auto Research External Skill Setup

Use `npx skills` directly for ordinary Skill management. For an Auto Research
workspace, prefer the CLI's guarded setup layer: it still uses the exact pinned
`skills` CLI underneath, while also binding source commits, tree hashes,
destinations, license choices, credential names, and audit state. Start with the
read-only catalog or the guided Wizard:

```bash
npx --yes @tiangong-ai/cli@0.0.26 research setup catalog \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.26 research setup \
  --workspace /absolute/path/to/workspace
```

The catalog includes Brave internet evidence, optional Tiangong SCI/document/
paper companions, and optional Anthropic or PPT Master post-closure authoring
Skills. Every entry is external, separately licensed, pinned, and user-selected;
nothing is bundled or installed by a research package. See
`tiangong-auto-research/references/setup.md` and `external-skills.md`.
For PPT creation, prefer PPT Master; Anthropic PPTX remains compatible and may
be selected alongside it when its workflow fits the task.
