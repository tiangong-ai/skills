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
lastReviewedAt: 2026-09-05
lastReviewedCommit: 24c56953e23f88f5fffa29c5c1447eb4feb8ba3f
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
- Architecture/runbook documents must label unavailable commands and future
  behavior as proposed. For atomic data, CLI documents are authoritative
  for machine contracts; Skills documents cover semantic entrypoints,
  capability requirements, release provenance, inventory, and migration
  workflow without copying closed schemas.

User-facing contribution guides and issue forms implement the shared workspace
reporting policy. Keep required core labels/IDs stable, accept Chinese and
English content, and ship the complete agent templates inside Auto Research.
