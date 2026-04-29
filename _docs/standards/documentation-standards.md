---
docType: standard
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When creating, moving, or reviewing skills repository documentation."
whenToUpdate: "When documentation layers, metadata rules, skill doc requirements, or source-of-truth boundaries change."
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - .github/workflows/docpact.yml
  - _docs/**
  - "*/SKILL.md"
lastReviewedAt: 2026-04-29
lastReviewedCommit: 7bcc1db8d066fa546ffa6e5c9c4b0def46c81ca1
---

# Skills Documentation Standards

## Layers

- `AGENTS.md`: mandatory repository entry guidance and skill creation rules.
- `.docpact/config.yaml`: machine-readable governance, routing, coverage, and
  document inventory.
- `.github/workflows/docpact.yml`: CI enforcement for config validation and PR
  documentation lint.
- `_docs/contracts/**`: current constraints and ownership rules.
- `_docs/architecture/**`: current repository topology and integration facts.
- `_docs/runbooks/**`: executable procedures.
- `_docs/standards/**`: repo-local documentation and engineering standards.
- `*/SKILL.md`: skill-specific trigger and usage entrypoint.

## Rules

- Keep deterministic governance facts in `.docpact/config.yaml`.
- Keep skill trigger semantics in each skill's `SKILL.md`.
- Keep install, update, and broad environment variable guidance in repository
  README files.
- Keep executable creation and validation workflow in `_docs/runbooks/**`.
- Do not duplicate root workspace branch policy or submodule integration policy
  in this repository.
- Do not include real credentials, user-private data, or large generated
  artifacts in skill docs or assets.
