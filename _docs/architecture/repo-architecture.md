---
docType: architecture
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When changing skill directories, generated agent configs, repository README files, or marketplace metadata."
whenToUpdate: "When skill layout, install flow, validation workflow, or discovery metadata changes."
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-04-29
lastReviewedCommit: 7bcc1db8d066fa546ffa6e5c9c4b0def46c81ca1
---

# Skills Repository Architecture

## Overview

The repository is a collection of reusable agent skills. Each skill is a
directory with a required `SKILL.md` and optional `scripts/`, `references/`,
`assets/`, and generated `agents/` resources.

## Key Paths

- `AGENTS.md`: mandatory repository-level skill creation and validation rules.
- `README.md` and `README.zh-CN.md`: install, update, target agent, and
  environment variable instructions.
- `.claude-plugin/marketplace.json`: curated marketplace grouping metadata; it
  may be a subset of installable skill directories.
- `*/SKILL.md`: individual skill entrypoint and trigger description.
- `*/scripts/**`: executable helpers used by skills.
- `*/references/**`: supporting reference material.
- `*/assets/**`: reusable skill assets or templates.
- `*/agents/**`: generated agent configuration files.

## Runtime Shape

Skills are consumed by external agent runtimes through the `skills` CLI or by
copy/symlink installation. Some skills require environment variables for
external APIs; those requirements belong in the relevant skill docs and the
repository README when broadly useful.

## Integration Points

- The root workspace pins this repository as a submodule.
- Consumers install skills into project or user agent directories.
- Marketplace metadata influences discovery and install ordering for the subset
  it lists.
