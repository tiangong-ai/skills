# External Skill setup

Use this reference to configure Tiangong Auto Research in a clean directory or
to inspect, resume, update, or replace an existing setup generation.

## Safety model

Setup is a separate owner operation, not part of a research package. No
recommended Skill is bundled with the CLI or installed without an explicit
Wizard confirmation or plan selection. The CLI:

1. shows the complete recommendation catalog without writing files;
2. records exact source commits, Skill tree hashes, installer version and npm
   integrity, destinations, settings, safe credential bindings, licenses,
   checks, and mutations in an immutable plan; secret values never enter it;
3. checks and persists supplied credentials to an owner-only store before it
   downloads an installer or source;
4. copies only the selected pinned trees, verifies the installed bytes, and
   configures their declared execution role;
5. stores broker and adapter credentials in separate owner-only stores without
   printing values;
6. writes a sanitized doctor report and append-only journal events.

Project-local copy mode is the default. Global installation needs a separate
confirmation and exact targets are frozen into the plan. Setup never installs
system packages or Python packages, never runs `pip install`, never follows a
Skill destination symlink, and never performs a floating update.

## Declarative clean-directory setup

Use the declarative path for reviewed, repeatable provisioning without a TTY.
The workspace may still be any user-selected absolute directory; the CLI never
derives a product default from a sample or repository path.

Generate no-overwrite examples with the exact reviewed bootstrap CLI:

```bash
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup init \
  --workspace /absolute/path/to/research-workspace --json
```

This creates a closed `schemaVersion: 2` `.tiangong-research/setup.yaml`,
`.tiangong-research/setup.env.example`, and a control-directory `.gitignore`
that excludes `setup.env`. The YAML contains only non-secret choices and
materializes all current catalog Skills under `selection.skills`, all
credentials, and all settings. Every Skill includes an explicit `enabled`
state and its catalog license ID. Every credential and setting includes its
catalog- and current-selection-derived `requirement`, catalog `appliesTo`, and
explicit enabled state; optional
omission is not a state. The generated template deliberately does not accept
licenses, network writes, global mutation, or paid smoke cost for the user.
Review the current catalog, complete those fields explicitly, and treat the
generated CLI template—not this reference—as the authoritative closed schema.
The same template makes `reviewerExecution.transport` explicit. Use
`native-direct` when the current host can create the platform capsule, or
`sandbox-bridge` for an outer-sandboxed IDE with an owner-started sidecar. Read
[sandboxed-ide.md](sandboxed-ide.md) before selecting the bridge.
The removed v1 declaration is rejected; regenerate the v2 template instead of
hand-migrating an old file.

The closed schema requires exactly all current catalog entries. Missing or
extra entries, changed license/requirement/applicability metadata, an incomplete
Brave profile combination, a disabled currently required entry, or an
enabled setting without a value fails before network access. Optional entries
remain visible with `enabled: false`; set one to true only after intentionally
selecting its applicable Skill and reviewing the corresponding license,
dependency, provider, and cost.

When file-based credentials are useful, copy the env example locally. It lists
all catalog credentials, including disabled optional entries. Fill only values
whose `credentials.<id>.enabled` choice is true, leave the rest empty, and make
the file owner-only:

```bash
cp .tiangong-research/setup.env.example .tiangong-research/setup.env
chmod 600 .tiangong-research/setup.env
```

Then run ordinary setup. Bare setup detects only the fixed workspace-local
`.tiangong-research/setup.yaml`; it never scans a parent directory. Absolute
`--config` and `--env-file` are explicit alternatives. If a declaration exists,
the command is non-interactive and does not fall back to the Wizard after a
YAML, schema, permission, credential, install, provider, reviewer, or readiness
failure. Use `research setup wizard` only when the user explicitly chooses the
interactive path.

```bash
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup \
  --workspace /absolute/path/to/research-workspace --json
```

Declarative setup requires live provider checks and the independent reviewer
CLI agent smoke with explicit cost authorization. It runs them through the
same apply/doctor pipeline as the Wizard and succeeds only when
`overallReadiness=READY`. Missing dependencies, skipped checks, provider
failure, reviewer failure, and warnings from any selected component remain a
non-zero incomplete result. The current native producer is never launched as a
child process.

The CLI binds the semantic YAML hash to the immutable plan. An unchanged file
reuses the same plan and reruns verification. A changed declaration stops; only
after reviewing the complete change may the owner set
`replaceExistingPlan: true`. The CLI then archives the prior immutable plan and
declaration binding before creating the replacement. Never edit a plan or
binding file directly.

Read [env.md](env.md) before creating `setup.env`. Secret values are imported
into the existing owner-only stores before downloads and are excluded from the
YAML, plan, binding, stdout, stderr, report, and journal.

## Interactive clean-directory Wizard

The workspace may be any ordinary user-selected directory. Example paths below
are placeholders, not defaults or repository requirements.

Prerequisites are Node.js 24, `git`, `npx`, an agent-compatible sandbox
(`/usr/bin/sandbox-exec` on macOS or `bwrap` on Linux) for independent review,
the current interactive Codex or Claude Code host, and normal authentication for
the other-family reviewer CLI. Create or choose the
directory, review one exact stable CLI release, replace `X.Y.Z` below, and
start the Wizard; ordinary users do not need to export keys. `latest`, tags,
ranges, paths, and command fragments are not bootstrap versions:

```bash
mkdir -p /absolute/path/to/research-workspace
cd /absolute/path/to/research-workspace

REVIEWED_BOOTSTRAP_CLI_VERSION=X.Y.Z
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai --version
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup wizard \
  --workspace /absolute/path/to/research-workspace
```

For every selected credential, the Wizard displays the logical ID, provider,
and official acquisition/configuration URL, then offers:

1. enter securely now (recommended for ordinary users; TTY input is hidden);
2. read from a named owner environment variable;
3. consume a value preloaded from stdin/password manager;
4. skip for now.

The stdin path is explicit and bounded. List logical IDs in the same order as
their one-line values; `--yes` prevents `npx` from asking an install question on
the secret pipe:

```bash
op read 'op://Research/Brave/api-key' | \
  npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
    tiangong-ai research setup \
    --credential-stdin brave.search.api-key \
    --workspace /absolute/path/to/research-workspace

{
  op read 'op://Research/Brave/api-key'
  op read 'op://Research/Tiangong SCI/api-key'
} | npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup \
  --credential-stdin brave.search.api-key,tiangong.sci.api-key \
  --workspace /absolute/path/to/research-workspace
```

The pipe is read before the remaining questions, which continue on the
controlling terminal. A stdin value is used only if the matching option is
chosen; unselected/preloaded logical IDs are rejected rather than ignored.
Do not paste a value into the environment-variable-name prompt.

The Wizard then guides the user through:

- smoke-test versus production mode;
- a Brave web/news public-internet baseline by default; bounded context and
  media profiles are visibly marked subscription-dependent;
- explicit project-local installation of the `tiangong-auto-research`
  orchestrator (recommended) so normal research requests enter this workflow;
- project-scoped host routing bound into the reviewed plan: one managed Codex
  block in the workspace-root `AGENTS.md` and one dedicated Claude rule under
  `.claude/rules/`, without replacing owner instructions;
- optional Tiangong companions and post-closure authoring Skills; for PPT
  creation it presents PPT Master as preferred while keeping Anthropic PPTX as
  a compatible situational choice;
- the current native research host (Codex or interactive Claude Code), the
  other-family reviewer CLI, matching Skill targets, and project/global scope;
- non-secret endpoints/settings;
- a source choice for every selected required or optional credential;
- each pinned source/license notice and explicit acceptance;
- optional model IDs and reviewed token prices;
- live provider checks, a separately authorized synthetic Unstructure upload,
  and separately authorized paid agent smoke checks;
- a full plan preview, network confirmation, immutable plan creation, and
  optional apply.

Project instruction routing is installed only when the orchestrator and
project scope are selected. Conflicting, modified, linked, or otherwise unsafe
targets stop before mutation. A replacement plan removes only bytes still
proven to be setup-owned. Global Skill scope does not write project instruction
files. After apply, start a new native-host session: Codex and Claude Code load
project instructions at session startup, while WorkBuddy/CodeBuddy continue to
use the separately installed thin adapter Skill rather than an assumed private
instruction filename.

Apply defaults to yes after all required values are available. Secure/stdin
values live only for that apply, are persisted to the existing owner-only store
before downloads, and are then discarded from Wizard memory. If the user saves
only the plan, those transient values are discarded and the result supplies
safe `credential set --prompt` recovery commands. If a required key is skipped,
apply stops before any network download. Configure the logical ID and resume
the exact plan; do not recreate the plan merely to bypass preflight.

## Read-only inspection and automation

The catalog command never creates workspace files:

```bash
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup catalog \
  --workspace /absolute/path/to/research-workspace \
  --scope project --agents codex --json
```

For non-interactive automation, first write JSON files containing only
non-secret mappings/settings. For example, `credential-env.json` contains
environment variable names, not values:

```json
{
  "brave.search.api-key": "BRAVE_API_KEY"
}
```

Create and review a minimal production plan, then apply its exact path:

```bash
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup plan \
  --workspace /absolute/path/to/research-workspace \
  --mode production-research \
  --evidence-profile brave-baseline \
  --skills tiangong.auto-research \
  --agents codex --scope project \
  --credential-env /absolute/path/to/credential-env.json \
  --accept-license brave-search-skills:MIT,tiangong-ai-skills:MIT \
  --confirm-network-downloads --json

npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup apply \
  --plan /absolute/path/to/research-workspace/.tiangong-research/setup-plan.json \
  --json
```

Add `--skills` IDs, settings, credential mappings, and matching license IDs only
after reviewing `setup catalog`. Do not manufacture pins or hashes from this
document; the CLI catalog is authoritative.

## Status, doctor, and recovery

After credentials are safely stored and before external source checkout, apply
creates a small project-scoped `tiangong-auto-research-recovery` Skill when the
full orchestrator was selected. Its immutable-plan-bound instructions allow only
`research context inspect`, `research setup status`, and the exact
`setup.next.retryCommand`; they prohibit research execution, standalone evidence,
credential access, global Skill fallback, and ambient CLI fallback. This closes
the bootstrap gap if source checkout or installation fails. Once the full
external orchestrator matches its reviewed tree hash, the CLI verifies the
generated files byte-for-byte and removes only that recovery directory. A
changed, symlinked, or ambiguous recovery directory blocks cleanup and is never
overwritten or deleted automatically.

After the immutable plan exists, resolve every ordinary command through the bundled
Skill helper. During a partial installation it uses the reviewed plan version;
after apply creates the runtime lock it requires the matching locked version.
Set `AUTO_RESEARCH_CLI` to the absolute path of this selected Skill, not a
guessed global location:

```bash
AUTO_RESEARCH_CLI=/absolute/path/to/installed/tiangong-auto-research/scripts/research_cli.mjs
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/research-workspace -- \
  research setup status \
  --workspace /absolute/path/to/research-workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/research-workspace -- \
  research setup doctor \
  --workspace /absolute/path/to/research-workspace --json
```

Static doctor checks installed tree hashes, required settings, owner-only
credential stores, Node/git/npx, the reviewer sandbox/CLI, Python, and pinned
Python requirements. It also verifies each planned project instruction target;
structural success is reported separately from the required new-session
activation. `--live` uses provider network/quota. A synthetic document
upload also requires `--allow-synthetic-unstructure-upload`; reviewer smoke
requires `--agent-smoke --confirm-agent-smoke-cost`. Doctor does not launch the
native producer. It probes a required capability only once and skips the paid
reviewer smoke when an earlier blocking prerequisite fails.

Setup commands return success only when `overallReadiness` is `READY`; this is
the complete selected-configuration result. Read `researchReadiness` for later
research-core admission. `preprocessingReadiness`,
`acquisitionReadiness`, and `authoringReadiness` describe optional domains;
`overallReadiness` may be `PARTIALLY_READY` while unrelated research remains
ready. An optional component becomes blocking only when the current project or
operation explicitly requires its exact catalog ID. After the reviewer smoke
succeeds, ordinary workspace doctor calls may reuse its unexpired hash-bound
attestation after checking the current reviewer runtime fingerprint. Explicit
smoke flags refresh it; expiry or drift remains blocking.

Status reports credential persistence separately from overall readiness and
includes the effective CLI package/version/root plus the selected orchestrator
path and install status. A stored broker or adapter credential is not an
ambient shell credential, and a readiness blocker must not be reported as a
missing ambient key.

On a blocked apply, use the exact-version-pinned `retryCommand` in setup state.
For a failed step this command is `research setup retry --step <recorded-step>`,
not a read-only status or doctor command. Clear a stale
lock only with both stale-lock flags after confirming no setup process owns it.
Do not delete plan/state/source-cache directories or overwrite installed trees.

### Replace the current immutable selection

Do not edit a hash-bound plan. To change an evidence profile, companion, or
orchestrator selection, run the exact CLI again and choose its explicit
replacement action:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/research-workspace -- \
  research setup \
  --workspace /absolute/path/to/research-workspace
```

For automation, rerun the reviewed `research setup plan` inputs with
`--replace-plan`. Replacement reconciles the complete setup-managed capability
set and both credential stores: deselected Brave/Sci declarations and lock
records are removed, custom capability declarations are preserved, and no
installed Skill directory is deleted. A provider `OPTION_NOT_IN_PLAN` result
must lead to an explicit operator choice between a baseline replacement and a
subscription change; setup never switches profiles silently.

## Update without version drift

Separate the installed CLI's supported commands from release availability.
The locked resolver deliberately keeps selecting the old CLI; a new document
or successful metadata query does not add commands to that installation.
Inspect its help once. With support available, `research setup update --check`
is a local catalog comparison; `--candidate-version X.Y.Z` checks one explicitly
selected exact stable release. An unavailable query means unknown, not no update.
Do not select `latest`, infer a candidate, or rewrite the runtime lock.

When an upgrade-capable exact candidate has been reviewed and authorized, invoke
that candidate to plan the transition. In a regular host, use a reviewed Node.js
24/npx environment:

```bash
REVIEWED_UPGRADE_CLI_VERSION=X.Y.Z # replace with the reviewed exact stable release
npx --yes --registry=https://registry.npmjs.org \
  --@tiangong-ai:registry=https://registry.npmjs.org --strict-ssl=true \
  --package "@tiangong-ai/cli@$REVIEWED_UPGRADE_CLI_VERSION" -- tiangong-ai \
  research setup upgrade --plan --confirm-upgrade \
  --workspace /absolute/path/to/research-workspace --json
```

A sandboxed IDE retains its approved Node/adapter and reviewer transport;
unsupported candidate invocation requires a reviewed compatible path, not an
ambient-runtime or permission bypass. See [sandboxed-ide.md](sandboxed-ide.md).

Planning leaves the active generation intact. Review the returned `planPath`,
ownership/changes and license choices, then execute its exact `applyCommand`.
The updater verifies prior-owned bytes, stages changed trees and reuses verified
unchanged material. Owner modifications, linked targets and ambiguous ownership
remain conflicts; do not delete or overwrite directories to make Doctor pass.
Preserve models, pricing, custom launchers, credentials, evidence and budgets.
Unchanged accepted choices carry forward; additional consent applies to actual
new licenses, downloads, global writes or costs.

For an interrupted transition, inspect status and use the same candidate's
returned apply/recovery command or `rollbackCommand`. Do not reconstruct the
previous generation or repair a mixed plan/lock by hand. Ordinary research
resumes only after coherent activation and the required readiness checks. If a
paid check started but its result was lost, inspect the recorded attempt before
requesting another; old readiness never certifies changed bytes. Return to the
locked resolver after activation and start a new native session when routing
instructions changed. Retained private recovery preimages are not public audit
exports or permission to discard owner work.

## Run selected companion adapters

Companions execute outside immutable research packages with a minimal child
environment and verified Skill tree. Document output uses a unique temporary
file and a no-overwrite atomic commit:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/research-workspace -- \
  research setup companion run \
  --id tiangong.document-granular-decompose \
  --input /absolute/path/to/source.pdf \
  --output /absolute/path/to/source.fulltext.md \
  --workspace /absolute/path/to/research-workspace --json
```

Paper acquisition binds only the precise adapter result and manifest; it never
chooses the newest PDF in a directory:

```bash
mkdir -p /absolute/path/to/papers
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/research-workspace -- \
  research setup companion run \
  --id tiangong.academic-paper-download \
  --doi 10.1234/example --out /absolute/path/to/papers \
  --workspace /absolute/path/to/research-workspace --json
```

The adapter must complete its PDF header/EOF/pypdf/size/SHA-256/atomic manifest
pipeline before the CLI reports success. If automatic OA sources are exhausted,
the result is `browser-handoff-required` with no committed artifact. Follow the
installed Skill's browser-handoff reference manually; setup never launches a
browser or turns a browser into an evidence capability.
