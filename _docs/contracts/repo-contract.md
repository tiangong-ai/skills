---
docType: contract
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When deciding whether a change belongs in the skills repository."
whenToUpdate: "When ownership, skill format rules, generated agent config requirements, marketplace metadata, or completion criteria change."
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .claude-plugin/**
  - .docpact/config.yaml
  - "*/SKILL.md"
lastReviewedAt: 2026-04-29
lastReviewedCommit: 7bcc1db8d066fa546ffa6e5c9c4b0def46c81ca1
---

# Skills Repository Contract

## Ownership

This repository owns reusable agent skills, per-skill scripts, references,
assets, generated agent configuration files, README files, and curated
marketplace grouping metadata.

## Boundaries

- Project-level vendored skills under a consuming repository's `.agents/**`
  belong to that consuming repository.
- Root workspace governance, branch policy, and submodule integration remain in
  the workspace repository.
- Runtime credentials and user-private data do not belong in skill assets,
  references, or scripts.

## Skill Surface

Each skill directory must follow the repository `AGENTS.md` rules and the
Codex `skill-creator` guidance. Changes to `SKILL.md`, scripts, references,
assets, generated `agents/**` files, or marketplace metadata require review of:

- `AGENTS.md`
- `README.md`
- `README.zh-CN.md`
- `_docs/runbooks/development.md`
- `_docs/standards/documentation-standards.md`

## Completion Criteria

- Run `docpact route` before editing governed files.
- Run `docpact validate-config --root . --strict` after governance changes.
- For skill changes, run the applicable `skill-creator` validation workflow,
  including `scripts/quick_validate.py <skill-path>` from the `skill-creator`
  skill when available.
- Regenerate or update agent config files when the skill workflow requires it.
- Do not leave install, validation, or trigger facts only in chat.

`.claude-plugin/marketplace.json` is curated marketplace grouping metadata. It
may be a subset of installable skill directories unless the marketplace file is
explicitly updated to include every skill.
