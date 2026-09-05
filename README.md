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
lastReviewedAt: 2026-09-03
lastReviewedCommit: 246f93abffc19ff7efa6aad37f9cf2515b4fc82b
---

# Tiangong AI Skills

Repository: https://github.com/tiangong-ai/skills

Use the `skills` CLI from https://github.com/vercel-labs/skills to install, update, and manage these skills.

## Report a problem or suggest a capability

Use the [feedback forms](https://github.com/tiangong-ai/skills/issues/new/choose)
and [contribution guide](CONTRIBUTING.md). Chinese and English reports are welcome.
For CLI runtime problems or uncertain ownership, use the
[CLI forms](https://github.com/tiangong-ai/cli/issues/new/choose). Auto Research
can also prepare the same structured report from its installed reporting reference.

## Atomic data skills

Twenty-one local candidate Skills—AirNow Hourly Observations, EPA EIS Records, Federal Register Documents, NASA
FIRMS Active Fire, OpenAQ Air Quality, Regulations.gov Comments,
Regulations.gov Comment Details, Regulations.gov Attachments, USBR Project Records, USBR RISE, USGS Water IV, three Open-Meteo sources, and
GDELT DOC, Events, GKG, and Mentions, plus Bluesky Cascades, YouTube Video
Search, and YouTube Comments—are thin semantic Skills over the Tiangong CLI
TypeScript 7 data runtime. Each candidate records its stable capability and
operation contract-major requirements plus any operation feature it actually depends on in
`references/tiangong-data-requirement.json`; the caller or workspace runtime
lock chooses the actual CLI build. The agent uses that same resolved CLI for
`data describe` and `data run`. These Skills contain no second provider
connector runtime and no per-Skill package lock.

This candidate set covers every item in the authoritative EcoCouncil source
baseline, `main@ac19289b4876d8a90595a0270721ef3f5ee7ced8`, which contains 21
`source-fetch` Skills. All 21 have been revalidated against the source
semantics, their requirements have been verified against the local CLI
candidate, and the unified copy/symlink installation and data-specific gates
pass. The qualifying CLI version and exact manifest/Schema digests live once in
`scripts/data-skill-migration-provenance.json`; they are release evidence, not
an installed Skill runtime lock. The repository-wide cold
gate also passes with a separately scoped upstream filesystem-clock fix; that
prerequisite is intentionally absent from this data migration branch.

See `_docs/architecture/atomic-data-capabilities.md` and
`_docs/runbooks/atomic-data-skill-migration.md` for the ownership boundary,
candidate inventory, source audit correction, staged migration order, and
release gates. The audited
RSS/full-text, Figshare-download, academic-paper, Tiangong/KB, and private-email
candidates retain their existing content, artifact, product, research, or
security boundaries instead of being narrowed into stateless data connectors.

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
- Install the Tsinghua graduate thesis LaTeX workflow:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tsinghua-graduate-thesis
  ```
- Install the Tiangong KB ingest workflow:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-kb-ingest
  ```
- For a WorkBuddy/CodeBuddy producer, install the thin adapter beside the
  canonical orchestrator:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-auto-research-workbuddy
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
`npx skills add` installs or links skill files; it does not provision language
runtimes or execute post-install hooks. When a skill provides a locked runtime
bootstrap and smoke command, run those explicit steps from its own instructions.

## Tiangong KB Ingest Compatibility

`tiangong-kb-ingest` uses the exact reviewed CLI 0.0.48 distribution. After
installing or updating the Skill, run the credential-free compatibility smoke:

```bash
npx --yes --package "@tiangong-ai/cli@0.0.48" -- tiangong-ai --version
npx --yes --package "@tiangong-ai/cli@0.0.48" -- tiangong-ai kb --help
```

The version command must print `0.0.48`; KB help must list the bulk scan,
metadata dry-run, collection list/schema, ingest, and status surfaces. These
checks do not call or mutate the KB backend.

## Auto Research External Skill Setup

Use `npx skills` directly for ordinary Skill management. For an Auto Research
workspace, prefer the CLI's guarded setup layer: it still uses the exact pinned
`skills` CLI underneath, while also binding source commits, tree hashes,
destinations, license choices, safe credential bindings, and audit state. The
Wizard lets ordinary users enter each selected key with hidden TTY input; named
environment variables and bounded stdin/password-manager input remain explicit
alternatives. Start with the read-only catalog or the guided Wizard:

```bash
REVIEWED_BOOTSTRAP_CLI_VERSION=X.Y.Z # replace with one reviewed exact stable release
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup catalog \
  --workspace /absolute/path/to/workspace --json
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup \
  --workspace /absolute/path/to/workspace
```

For repeatable non-interactive provisioning, run `research setup init` first.
It creates a no-overwrite schema-v2 `.tiangong-research/setup.yaml` template
plus `setup.env.example`. The YAML explicitly lists every current catalog Skill,
credential, and setting, including disabled optional entries; the env example
lists every credential variable with an empty placeholder. After the user
reviews the current catalog, enabled states, licenses, models, pricing, checks,
and confirmations, bare `research setup` detects only that workspace-local YAML
and bypasses the Wizard. The optional real `setup.env` must be owner-only;
disabled credentials stay empty and a non-empty disabled value is rejected.
The removed v1 declaration and all invalid or incomplete declarations fail
closed; they never trigger an
interactive fallback or parent-directory search. Setup returns success only
after every selected dependency, provider live check, Policy compatibility
check, and independent reviewer smoke reaches complete readiness. Use explicit
`research setup wizard` to choose the interactive path. See
`tiangong-auto-research/references/setup.md` and
`tiangong-auto-research/references/env.md`.

The bootstrap version is an explicit new-workspace choice, never `latest`, a
tag, or a range. After apply creates `runtime-lock.json`, the installed
orchestrator's bundled resolver runs exactly that locked version for all
workspace operations.

For acquisition-heavy work, the orchestrator uses the CLI's read-only artifact
size and role-coverage forecasts, bounded atomic content batches, and explicit
prepared scientific-review execution. Recovery reuses verified discovery and
downloaded artifacts. A compatible locked runtime can revise acquisition in the
same project before analysis, explicitly reopening discovery only for new sources;
design changes and later revisions retain the formal successor workflow. Original
and currently approved task requirements, native check records, independent review
and portable audit stay linked without extra fixed reviewer rounds. Workflow
closure is reported separately from task completion. See
`tiangong-auto-research/references/execution-assurance.md`; frozen history and
scientific requirements are never silently rewritten to make a gate pass.

Compatible runtimes also fulfill predeclared pending scientific objects in place,
preserve original-request provenance, and bind computational acceptance to an
observed ordinary native calculation. Complete packet artifacts are available
on demand without an arbitrary total context-length gate. A command report alone
remains unverified execution; file hashes and read receipts do not certify
scientific validity. These are CLI-owned contracts, not a second Skill executor.

The orchestrator gates a research question before setup or any tool call. A
request that presupposes its conclusion or excludes contrary evidence is
paused with one testable rewrite and resumes only after explicit user
confirmation. Controversial topics and directional hypotheses remain allowed
when null results, alternatives, and counterevidence are testable; evidence
fabrication or concealment is refused.

The catalog also offers the `tiangong-auto-research` workflow orchestrator,
default-baseline Brave internet evidence, optional Tiangong SCI/document/paper
companions, and optional Anthropic or PPT Master post-closure authoring Skills.
The workspace can be any user-selected directory. Every entry is external,
separately licensed, pinned, and explicitly confirmed/selected; nothing is
bundled or installed by a research package. See
`tiangong-auto-research/references/setup.md` and `external-skills.md`.
Built-in CLI data connectors are different: the native discover packet projects
their current catalog dynamically, and Auto Research invokes the shared
TypeScript runtime through its Research evidence command. The Skill does not
duplicate provider adapters or keep a fixed connector list; Research adds only
budget, owner-only credential, immutable receipt/ledger, artifact, and review
bindings around the unchanged core result.
When that result is larger than one Agent context, Auto Research keeps the full
immutable evidence and exposes receipt-bound continuation cursors. Exhaustive
row review follows those local pages to completion; summary work may stop early
only with the presented fraction disclosed. Provider gaps, operation limits,
and Agent-context projection remain separate signals.
`tiangong-auto-research-workbuddy` is only a sandboxed-IDE adapter. It routes
back to the canonical orchestrator and its signed reviewer-bridge reference;
it does not define a second research workflow.
For PPT creation, prefer PPT Master; Anthropic PPTX remains compatible and may
be selected alongside it when its workflow fits the task.

For a top-journal goal, the orchestrator includes a conservative Research
Policy template pack for article type, field, journal class, project brief, and
four independent final-review roles. The CLI Policy Wizard copies the selected
Markdown into the research workspace for human review; it reports when generic
defaults remain, requires explicit approval of the exact content hash, and
invalidates approval after any edit or expiry. Exact-journal readiness requires
current official guidance and substantive human customization. These gates can
produce a reviewable submission candidate, never a promise of editorial
acceptance.

Before discovery, the current native Codex or Claude host must also provide a
closed, target-specific scientific design. The CLI validates and freezes the
design. Frozen model implementations and environment locks must first enter the
workspace through `research scientific object register`; the Skill never asks
the user to hand-copy them into `.tiangong-research`. The CLI requires independent
`research-design` review before discovery, then real-record `evidence-construct`
and `pilot-methods` reviews after acquisition and before analysis. After acquisition it also requires exact
decomposition records, evidence atoms, a typed-content snapshot, a passing
inference snapshot, a reproduced analysis, and a mechanically generated
Claim-Evidence Graph. Publication freeze requires the complete manuscript
sections and explicit cover/title/checklist/availability/source-data files;
four fresh reviews must use the configured agent family that differs from the
native producer. It reserves the complete early/final-review and revision
lifecycle, requires reapproval for every authoritative recovery generation,
and exports a semantic-chain-verified portable audit directory containing exact
formal evidence rather than host-local pointers. The CLI remains the
deterministic control plane; it never launches a nested producer to invent the
science. See
`tiangong-auto-research/references/publication-policy.md` and
`tiangong-auto-research/references/scientific-design.md`.
