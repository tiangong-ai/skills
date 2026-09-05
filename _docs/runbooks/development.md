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
  - .dockerignore
  - Dockerfile.clean-test
  - .github/workflows/docpact.yml
  - scripts/**
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-09-05
lastReviewedCommit: 24c5695ca3129bbe021e4b80d830742841e9011e
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

## Atomic Data Skill Migration

Before changing a fetch/search/download Skill as part of the atomic
data refactor, read `_docs/architecture/atomic-data-capabilities.md` and follow
`_docs/runbooks/atomic-data-skill-migration.md`. Do not remove a provider
runtime until the CLI's TypeScript 7 connector is accepted, a compatible
package is installable, the Skill's stable capability requirement passes, and
the exact migration package/digests are recorded once in repository-level
provenance. Planning changes alone do not authorize Skill rewrites or file
deletion.

Run the requirement/provenance contract with:

```bash
node --test scripts/tests/data-skill-binding.test.mjs
```

For migrated Skills, run the explicit copy/symlink install smoke against the
same exact published CLI package whose version and contracts are recorded by
the repository migration provenance:

```bash
TIANGONG_DATA_SKILLS_RUN_INSTALL_SMOKE=1 \
TIANGONG_DATA_CLI_VERSION=X.Y.Z \
TIANGONG_DATA_CLI_PACKAGE=@tiangong-ai/cli@X.Y.Z \
node --test scripts/tests/data-skill-install-smoke.test.mjs
```

The smoke runs version, catalog, describe, static doctor, and an intentionally
blocked local request with provider credentials removed. It must not contact a
provider.

Use `scripts/data-skill-binding.mjs generate` and `verify` for stable per-Skill
requirements. These checks compare capability and operation contract majors plus
declared `requiredFeatures`; they do not compare package versions or digests. Use `generate-provenance` and
`verify-provenance` only to qualify the exact CLI release used by this migration;
the resulting repository artifact is not installed with each Skill.

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

For a `tiangong-kb-ingest` exact CLI pin change, run the offline stale-pin
contract and then the explicit networked copy/symlink install smoke:

```bash
python3 -m unittest discover -s tiangong-kb-ingest/scripts/tests -v
TIANGONG_KB_INGEST_RUN_INSTALL_SMOKE=1 \
  python3 -m unittest discover -s tiangong-kb-ingest/scripts/tests -v
```

The install smoke uses a temporary project and HOME, runs the repository-
standard exact `skills` installer, verifies the installed Skill files, and
invokes only CLI version/help plus a local bulk scan. It must not receive KB
credentials or contact the backend.

For `tsinghua-graduate-thesis` PDF renderer or visual-QA behavior, write the
real-PDF regression first and run each phase through the targeted entrypoint:

```bash
tsinghua-graduate-thesis/scripts/test-clean-container.sh
```

The test fixture is a privacy-safe binary PDF with an embedded open-license CID
Type 0C font, `Identity-H`, `Adobe-GB1`, and no `ToUnicode`. The failure path
loads a real Poppler library whose compiled data directory is fault-injected;
do not replace it with mocked stderr or a text-only assertion. Record the RED,
turn the suite GREEN in a new container, and run both the targeted and full
repository gates with `--cold-build` before PR delivery whenever their
Dockerfiles or dependency inputs change.

Run representative script tests when a skill script changes.

For `tiangong-auto-research` or any direct evidence wrapper, including
`academic-paper-download`, use
test-driven development exclusively through the mandatory clean-room entrypoint
for the red and green cycles:

```bash
scripts/test-clean-container.sh
```

It builds from the digest-pinned Node 24 image, copies only the secret-filtered
repository context, and runs the routing, resolver, agent wrapper, Research
Policy/scientific-design/native-execution contract, generated agent metadata,
the Tsinghua real-PDF renderer regression, SCI/report/patent wrapper suites,
and the complete `academic-paper-download` unittest discovery as a non-root user
with isolated HOME and runtime networking disabled. Do not mount host agent
directories, CLIs, runtime caches, credentials, browser profiles, or source
worktrees into the running container.

The test image installs only `academic-paper-download/requirements.lock` with
hash verification. It never installs the optional CloakBrowser requirements or
downloads a browser binary; those remain explicit integration checks outside
the network-disabled unit gate.

The default entrypoint may reuse Docker layers whose declared inputs still
match, while every test run uses a new container. Run the explicit cold mode
after changing `.dockerignore`, `Dockerfile.clean-test`, or dependency inputs,
and before PR or release delivery:

```bash
scripts/test-clean-container.sh --cold-build
```

Cold mode adds `--no-cache`; neither mode uses `--pull`, so the base image can
change only through a reviewed digest update.

When changing sandboxed-IDE routing, keep its deterministic markers in
`tiangong-auto-research/scripts/test-routing-contract.mjs` and require the thin
adapter, canonical reference, exact structured bridge errors, and no-bypass
language to pass in the clean container.

Task-assurance recipe tests copy the Skill to an isolated directory and execute
its single-command shell examples against a capture-only Node stand-in. Check
resolver paths, argv and exact scope-hash confirmation there; this is not a
substitute for behavioral evaluation. Evaluate realistic recovery/completion
decisions independently without supplying the expected answer, and run the
corresponding protocol against the exact candidate CLI outside the repository.
Keep native case data, captures and generated research outside Git.

Use that deterministic layer for each small correction with a mock reviewer.
For changes to task meaning, scientific-object fulfillment, execution/read trust
boundaries or the end-to-end workflow, also use a fresh external native case and
a real independent reviewer before claiming major-behavior qualification. Do
not pay for another research run after every documentation or formatting edit;
record the tier decision and exact validation evidence in the tracked task.
The workspace testing standard remains authoritative for cross-repo gate policy.

## README And Marketplace Updates

Update `README.md` and `README.zh-CN.md` when installation, environment
variables, target agents, or user-facing skill availability changes. Update
`.claude-plugin/marketplace.json` when marketplace discovery metadata changes.

## Reporting Contract

When changing contribution guidance, GitHub forms, or the installed reporting
reference, review the shared workspace reporting policy and both repositories'
core field IDs, labels, ordering, and required flags together. Repository-only
fields remain optional. Keep reporting instructions available in an isolated
Skill install; never link local templates outside the installed Skill tree.
`node --test scripts/tests/issue-reporting.test.mjs` checks isolated installation
and form/agent heading parity, and runs in the existing clean-container suite.
Inspect both YAML forms with a YAML parser before delivery.
