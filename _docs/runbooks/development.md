---
docType: runbook
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When creating, updating, validating, or publishing reusable skills."
whenToUpdate: "When skill creation, generated agent config, validation, install, or marketplace update workflow changes."
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-04-29
lastReviewedCommit: 7bcc1db8d066fa546ffa6e5c9c4b0def46c81ca1
---

# Skills Development Runbook

## Before Editing A Skill

1. Read `AGENTS.md`.
2. Read the Codex `skill-creator` guidance required by `AGENTS.md`.
3. Run docpact route for the target skill paths.
4. Inspect existing skill resources before changing structure.

## New Skill Or Major Skill Update

Use the `skill-creator` workflow. Prefer the official initializer when creating
new skills, then fill in `SKILL.md`, optional `scripts/`, `references/`,
`assets/`, and generated `agents/**` files as required.

## Validation

Run:

```bash
docpact validate-config --root . --strict
```

For skill changes, run the validator required by `AGENTS.md` from the
`skill-creator` scripts directory, such as:

```bash
scripts/quick_validate.py <skill-path>
```

Run representative script tests when a skill script changes.

## README And Marketplace Updates

Update `README.md` and `README.zh-CN.md` when installation, environment
variables, target agents, or user-facing skill availability changes. Update
`.claude-plugin/marketplace.json` when marketplace discovery metadata changes.
