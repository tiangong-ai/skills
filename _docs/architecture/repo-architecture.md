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
lastReviewedAt: 2026-09-05
lastReviewedCommit: 24c5695ca3129bbe021e4b80d830742841e9011e
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

`tiangong-kb-ingest` is a thin orchestration Skill over an exact published
Tiangong CLI. Its offline contract rejects stale or inconsistent CLI literals;
its explicit networked install smoke exercises both copy and symlink installs,
then verifies the exact CLI version, KB help surface, and a credential-free
local bulk scan without contacting the backend.

`tiangong-auto-research` documents both the interactive setup Wizard and the
CLI-owned declarative path. Its references explain fixed workspace-local YAML
discovery, complete explicit materialization of every current catalog Skill,
credential, and setting, owner-only env input with empty disabled options, no
interactive fallback after a declaration error, and complete-readiness gating.
The Skill does not duplicate the closed YAML schema or parse configuration; the
CLI-generated template and validator remain authoritative.

The canonical Skill also owns the native-host research-question gate. It runs
before setup or tool use, pauses conclusion-presupposing or
counterevidence-excluding requests with one testable rewrite, and waits for the
user. The CLI may install and verify host routing instructions, but it does not
implement a model-driven bias classifier.

`tiangong-auto-research/assets/research-policy/defaults/**` is the versioned,
generic source pack for top-journal Policy initialization. It is immutable
source material, not a user Policy or journal endorsement. The CLI copies a
selected stack into the user-selected research workspace, where a human may
customize and explicitly approve the exact resolved content.

`tiangong-auto-research/references/scientific-design.md` defines the Skill-side
native workflow for a closed project-specific design, explicit public
pre-admission registration of raw model/environment objects, frozen-versus-
pending null semantics, exact portable review-blob promotion, three early independent
scientific review gates, post-acquisition decomposition/evidence-atom/content
freeze, evidence-construct canary binding, inference snapshot, reproducible
analysis and Claim-Evidence Graph, role-complete submission packaging,
Policy-owned future freeze obligations for models, environment locks, and
source-derived uncertainty states, authoritative recovery generations, and
portable audit handoff. The Skill supplies instructions and conservative
defaults only. The CLI owns schemas, hashing, stage admission, mechanical
evaluation, lifecycle reservations, other-family reviewer isolation, and
semantic audit verification; the configured native Codex, Claude, WorkBuddy,
or CodeBuddy host remains the scientific producer.

Large evidence sets use the CLI's optional pre-freeze role forecast and bounded
atomic decomposition/atom batches. File-size preflight distinguishes an
artifact ceiling from aggregate output limits. Prepared scientific reviews use
the configured isolated transport through an explicit cost-confirmed execution
command; recovery reuses exact discovery/artifact bytes in a reviewed successor.
These recipes do not duplicate schema evaluators or add per-record verification
loops to the Skill.

`references/execution-assurance.md` routes the canonical orchestrator through
CLI-owned original/current task contracts, exact scope confirmation, native
check intake, pre-analysis same-project evidence revision and task-aware
review/audit. Compatible runtimes may explicitly reopen discovery for new
sources without changing project identity. The Skill reports task coverage
separately from workflow/publication status and keeps all producer work native;
it neither implements a second state machine nor adds fixed paid reviewer rounds.

The same reference guides original-request provenance, same-project fulfillment
of explicitly pending model/environment/parameter slots, actual native calculation
observation and acceptance, and packet-only on-demand artifact reads. The CLI
owns closed schemas and runtime/audit relationships. The Skill keeps reasoning
native, preserves original assumptions and human scope approval, distinguishes
unverified reports from observed processes, and never treats byte/read identity
as scientific or environment attestation. It does not impose a second aggregate
context-length gate or reproduce the on-demand server in Skill code.

`tiangong-auto-research-workbuddy` is a thin sandboxed-IDE adapter. It routes
WorkBuddy/CodeBuddy native producer tasks back to the canonical orchestrator and
its `sandboxed-ide.md` reference. It owns no duplicate research schema or
control-plane behavior. Independent review remains a CLI-owned Codex/Claude
route through either the native platform capsule or the signed sidecar bridge.

`tsinghua-graduate-thesis/scripts/render-pdf.mjs` is the thesis visual-QA
renderer boundary. It probes a known nonblank page from the actual PDF, rejects
Poppler language-pack/font failures even when the child process exits zero, and
may fall through to another explicit or discovered `pdftoppm` candidate. Its
clean-container suite uses a privacy-safe embedded CID Type 0C Adobe-GB1 PDF and
a real fault-injected Poppler library; it does not mock renderer stderr.

## Atomic Data Skills

The architecture and staged inventory are documented in
`_docs/architecture/atomic-data-capabilities.md` and
`_docs/runbooks/atomic-data-skill-migration.md`.

An atomic data Skill is a thin semantic entrypoint over a compatible published
Tiangong CLI capability. It keeps source guidance, limitations, agent
instructions, and a machine-checkable contract-major/required-feature requirement. The caller or
workspace runtime lock owns the exact package; connector logic, schemas,
credentials, retries, and core receipts live only in the CLI's
TypeScript 7 runtime. Auto Research reuses its workspace-locked runtime and adds
its own evidence admission and persistence instead of executing a second Skill
script.

Twenty-one local candidate Skills now use this shape: AirNow, Federal Register,
USGS Water IV, three Open-Meteo sources, NASA FIRMS, OpenAQ, EPA EIS, USBR RISE,
USBR Project Records, three Regulations.gov semantic entrypoints, separate
GDELT DOC, Events, GKG, and Mentions entrypoints, Bluesky Cascades, and separate
YouTube video-search/comment entrypoints. Regulations.gov search and detail
bind different operations of one capability, while attachments uses its own
capability; the two YouTube Skills likewise bind different operations of one
capability; the GDELT Skills bind four independent capabilities with one
operation each. Each directory has only
`SKILL.md`, generated agent metadata, and a package-independent capability
requirement; its
former Python connector and duplicate provider references are absent. These
become production migrations only after a compatible CLI package is published
and the repository-level migration provenance/install smoke qualify that
release. Ordinary compatible CLI releases do not rewrite every Skill. RSS/full-text,
Figshare, academic-paper, Tiangong/KB, and private-email
candidates have completed their boundary audit and retain their specialized
runtimes rather than losing core content, artifact, product, research, or
account-security semantics.

## Integration Points

- The root workspace pins this repository as a submodule.
- Consumers install skills into project or user agent directories.
- Marketplace metadata influences discovery and install ordering for the subset
  it lists.

## User Feedback

`CONTRIBUTING.md` and `.github/ISSUE_TEMPLATE/**` expose the shared workspace
reporting contract. The canonical Auto Research Skill routes feedback before
research setup or scientific-question gating and includes complete offline
Markdown templates in `references/issue-reporting.md`. The WorkBuddy adapter
uses that same route, including when setup is absent. Form labels and agent
headings stay aligned; reporting gathers existing evidence without running
research or implicitly submitting externally.
