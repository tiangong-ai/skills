---
name: tiangong-auto-research
description: Orchestrate open-ended, multi-source, evidence-backed research in the current native Codex, Claude Code, WorkBuddy, or CodeBuddy host, especially when an ancestor directory contains `.tiangong-research` or the user asks to research, investigate, build on prior outputs, compare evidence, form a conclusion, or produce a reviewed research artifact. Also use for setup, preflight, native producer execution, recovery, independent review, closure, and requests to report Auto Research bugs or suggest capabilities (反馈问题、提交 Issue). In an Auto Research workspace this Skill takes precedence over individual web, news, SCI, report, patent, download, or document Skills unless the user explicitly requests one isolated standalone operation outside the research workflow. Covers Chinese requests such as “研究一下”, “朝这个方向做一做”, “结合已有成果继续研究”, “查资料并形成结论”, and “系统梳理证据”.
---

# Tiangong Auto Research

## Route feedback before research

For requests to report a problem, suggest a capability, or prepare an Issue,
read [references/issue-reporting.md](references/issue-reporting.md) and follow
that reporting workflow. Reporting does not require a valid research workspace,
setup, or scientific-question approval. Return the report without entering the
research workflow unless the user also requests research execution.

## Gate the research question before acting

Before any CLI, browser, search, database, or file operation, inspect the
user's research request. Continue when it leaves the result open to evidence.
Pause and require a rewrite when it assumes the conclusion, asks to prove a
predetermined position, requests only supporting evidence, excludes contrary
evidence, or treats an unsupported causal or normative judgment as fact.

When paused:

1. Do not call tools or begin setup.
2. Identify the problematic assumption in one concise sentence.
3. Offer one testable rewrite that preserves the intended topic, scope,
   population, geography, and time period.
4. Wait for the user's explicit confirmation or edited question.

Do not reject a controversial topic or directional hypothesis merely because
it has a position. It may proceed when null results, alternative explanations,
and counterevidence remain testable. Refuse requests to fabricate, conceal, or
misrepresent evidence.

For an existing managed directory, use the bundled resolver for every CLI operation.
Resolve `AUTO_RESEARCH_CLI` from this loaded Skill's absolute directory; do not
guess a global Skill path. The resolver accepts only the CLI package and exact
stable version from the regular non-symlink runtime lock, or from the immutable
setup plan while installation is still pending or blocked:

```bash
AUTO_RESEARCH_CLI=/absolute/path/to/installed/tiangong-auto-research/scripts/research_cli.mjs
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research --help
```

For a clean directory without a runtime lock or setup plan, read
[references/setup.md](references/setup.md) and ask the user to choose one
reviewed exact bootstrap CLI version. Never substitute `latest`, a range, a
tag, a path, or a command fragment. After setup creates the runtime lock, use
the resolver above.

If that clean directory contains the fixed workspace-local
`.tiangong-research/setup.yaml`, bare `research setup` must use its declarative
path without a TTY. It never scans parent directories and must not fall back to
the Wizard after a declaration error. Use `research setup init` to generate the
no-overwrite YAML and env examples. The YAML and env example explicitly expose
all current catalog Skills, credentials, and settings, including disabled
optional entries. Only declaration schema v2 is accepted; omission is invalid
and a disabled credential must remain empty. Use explicit
`research setup wizard` when the user chooses the
interactive path. Follow [references/setup.md](references/setup.md) and
[references/env.md](references/env.md); the generated CLI template is the only
authoritative declaration schema.

The CLI is the deterministic control plane: it owns setup, locks, brokered
evidence, schemas, coverage, budgets, admission, the independent review process,
the journal, and closure. The current interactive Codex, Claude Code,
WorkBuddy, or CodeBuddy session owns producer reasoning. The CLI must never
launch a nested producer process.
Do not reproduce control-plane contracts or edit control files by hand.

During an accepted apply, the CLI may temporarily create the project Skill
`tiangong-auto-research-recovery`. That generated recovery-only entry can inspect
context and setup status and execute only the exact pinned retry command. It must
never perform research or standalone evidence search. The CLI removes only its
own exact plan-bound recovery bytes after this full external orchestrator matches
the reviewed tree hash; follow [references/setup.md](references/setup.md) for the
detailed stop and recovery rules.

## Route to the right reference

- Read [references/setup.md](references/setup.md) for a new or clean directory,
  the guided Wizard, non-interactive setup, updates, or recovery.
- Read [references/external-skills.md](references/external-skills.md) before
  selecting a recommended external Skill, provider, license, or execution role.
- Read [references/env.md](references/env.md) before configuring credentials,
  agent authentication, provider checks, or wrappers.
- Read [references/production-research.md](references/production-research.md)
  before production preflight, execution, recovery, or closure.
- Read [references/execution-assurance.md](references/execution-assurance.md)
  before original-task intake, pre-analysis evidence correction, scope changes,
  planned-object fulfillment, observed native calculations, packet-only artifact
  reads, or completion reporting. Prefer supported same-project operations for
  an unchanged study; preserve original/current task completion separately.
- Read [references/evidence-pipeline.md](references/evidence-pipeline.md) before
  discovery, acquisition, evidence refresh, or an addendum.
- Read [references/evidence-exhaustion.md](references/evidence-exhaustion.md)
  before declaring a material evidence gap exhausted, requesting paid or
  authorized access, waiting for an external response, or narrowing scope.
- Read [references/native-execution.md](references/native-execution.md) before
  preparing or submitting discover, acquire, analyze, or synthesize stages.
- Read [references/sandboxed-ide.md](references/sandboxed-ide.md) when the
  producer runs inside WorkBuddy, CodeBuddy, or another outer sandbox, or when
  native reviewer isolation returns a nested-sandbox error.
- Read [references/publication-policy.md](references/publication-policy.md)
  before a top-journal project, Policy approval, final manuscript freeze,
  four-role publication review, or readiness closure.
- Read [references/scientific-design.md](references/scientific-design.md) before
  top-journal admission, any scope change, the three early scientific gates,
  a target-specific recovery generation, or portable audit export.

## Choose the mode before spending budget

- `smoke-test` is for deterministic mocks, routing/eval checks, workflow demos,
  and explicitly accepted low-cost canaries.
- `production-research` is for conclusions a user may rely on. It requires a
  locked independent public-internet evidence profile. Local evidence and an
  owner database can supplement that profile but cannot replace it.

## Inspect before mutation

The workspace may be any user-selected directory; example paths are
placeholders, never defaults. Resolve paths to absolute paths, then inspect the
directory:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research context inspect \
  --path /absolute/path/to/workspace --json
```

Follow `role`, `allowedOperations`, and the structured `setup` preflight.
For `setup` or a blocked `workspace`, execute only its exact-version-pinned
`next.retryCommand`; never fall through to a standalone provider wrapper.
Stop on `invalid`; never repair immutable
state, locks, journal events, evidence objects, receipts, or outputs manually.

For a clean directory, show the read-only ecosystem catalog and run the guided
setup only after the user asks to configure it:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup catalog \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup \
  --workspace /absolute/path/to/workspace --json
```

When the user asks for repeatable non-interactive configuration, first create
the safe template with `research setup init --workspace
/absolute/path/to/workspace --json`, then let the user review and complete it.
Never pre-accept licenses, downloads, global writes, quota, or agent-smoke cost.
Setup is complete only when the resulting `overallReadiness` is `READY`; a
planned, skipped-check, partially ready, or blocked result is not success.

The user must explicitly confirm the recommended project-local
`tiangong-auto-research` orchestrator and every external source, then accept its
pinned license. The Wizard defaults evidence to Brave web/news; context and
media remain visibly subscription-dependent choices. For each selected
credential it offers hidden input, an owner environment variable, preloaded
stdin/password-manager input, or an explicit skip; read
[references/env.md](references/env.md) for the exact safe paths. It may create a
plan without applying it. Never silently install a Skill, write globally,
substitute a provider, downgrade a profile, or accept a license. Missing
required credentials must block before any source download.

## Preserve execution boundaries

- Brave and owner-whitelisted SCI/report/patent sources are
  `evidence-capability` Skills only after their exact capabilities are locked.
  Discovery calls them only through the scoped broker and locked manifest
  method. Never execute their standalone shell examples from a research
  workflow or expose broker credentials to an agent.
- Built-in structured data capabilities are native CLI capabilities, not
  external Skills. During discover, use only the packet's dynamic data catalog:
  choose from its source/capability/operation summaries, inspect the selected
  input contract through the workspace-locked resolver, and invoke the packet's
  `runDataCapability` command. That command promotes the shared TypeScript
  runtime result into Research evidence. Never use standalone `data run` for a
  project, copy provider logic into this Skill, or assume a fixed capability
  list. Suspended capabilities are not projected into this packet, even though
  standalone catalog and describe output keep them discoverable for diagnosis.

  `runDataCapability.executionKind=workspace-cli-relative-argv` means every
  packet command is relative to the workspace runtime lock; always
  prefix its resolver-relative `argv`, `readArgv`, or `describeArgv` arguments with
  `node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace --`; never
  execute a packet token as a PATH-resolved global `tiangong-ai` binary.

  Review the returned `providerCoverage`, `limitCoverage`, and `contextView`
  independently. Provider gaps, intentional operation bounds, and the smaller
  Agent-facing projection are not interchangeable. When
  `contextView.nextCursor` is present and the question requires exhaustive
  row-level review, invoke the packet's exact `runDataCapability.readArgv` with
  the returned receipt and cursor until `nextCursor is null`. This reads the
  immutable local evidence and does not consume another provider call. For a
  summary or adaptive task, stopping earlier is allowed only when the presented
  fraction is recorded as a limitation; never imply that every returned row was
  reviewed.

Use the same runtime lock for contract inspection and execution:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  data describe <capability-id> --json
```
- `document-granular-decompose` is an `input-preprocessor`. Run it explicitly
  through `research setup companion run`, then admit the exact hash-bound output
  as a project input. Its output is not evidence merely because parsing worked.
- `academic-paper-download` is an `acquisition-adapter`. Its automatic OA order
  remains Unpaywall, Semantic Scholar OA, then arXiv. If all are exhausted, the
  CLI reports an explicit browser handoff; it never launches or chooses a
  browser automatically.
- Document/PDF/spreadsheet/presentation Skills are `post-closure-authoring`.
  They may format a closed report but cannot produce or alter admitted evidence,
  analysis, review, or closure.
- For PPT creation, prefer `hugohe3.ppt-master`. Keep `anthropic.pptx` as a
  compatible situational option; both may be selected explicitly in one plan.
- Selected optional preprocessors, acquisition adapters, and authoring Skills
  remain visible diagnostics. They become hard gates only when the current
  project lists them in `requiredCompanionIds` or explicitly invokes that
  operation. Their failure never authorizes a standalone evidence downgrade.

## Preflight and initialize a project

For `--goal top-journal`, first use the project-scoped Policy Wizard and stop
unless its exact reviewed hash is approved. Generic defaults are bounded
feasibility guidance, not journal endorsement. Follow
[references/publication-policy.md](references/publication-policy.md); never
invent a target-journal policy or bypass its status gate.

Before top-journal preflight, the current native host must write the closed
`scientific-design` contract. It must distinguish observation, validation,
cross-model comparison, scenario, and accounting roles; freeze units,
denominators, independent clusters, thresholds, baselines, evidence roles,
closest-work requirements, known gaps, and handoff conditions. Pass the same
exact file to preflight and init with the native producer's opaque session ID.
Before referencing a frozen model implementation or environment lock, use
`research scientific object register` and copy its returned raw-byte hash and
safe locator into the design; never hand-copy objects into the control store.
The design must map every required evidence role to its lawful acquisition
routes. All plan-bound lawful agent routes must have immutable terminal events
before an evidence-exhausted handoff is valid.
Follow [references/scientific-design.md](references/scientific-design.md); never
let the CLI invent a study design or describe resampling as new independent data.

Prepare evidence requirements with `dimensions`, `sourceTypes`,
`requiredCapabilityIds`, `requiredCompanionIds`, `requiredDiscoveryScopes`,
`minSources`, `minFullTextSources`, `minDatedSources`, and nullable date bounds.
Require each owner database that the question must actually exercise; Brave or
local files cannot mask an undeclared report, patent, or other whitelisted
capability. For large local sources, create an immutable input plan with bounded
context files or ranges.
Use a projected data operation's exact `data:<capability-id>:<operation-id>` ID
in `requiredCapabilityIds` only when that source is mandatory for the reviewed
question. Do not require all built-in data operations merely because they are
available.

First require the current setup generation and its real production checks to
pass. Doctor probes required capabilities once, then runs only the independent
reviewer CLI smoke after all blocking zero/low-cost checks pass. It reuses an
unexpired attestation and never smoke-tests the current native producer as a
child process:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup status \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup doctor \
  --workspace /absolute/path/to/workspace --live \
  --agent-smoke --confirm-agent-smoke-cost --json
```

Stop unless `researchReadiness` is `READY` and every companion explicitly
required by this project or operation is `READY`. Keep degraded optional domains
visible, but do not block unrelated research or weaken evidence coverage.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project preflight \
  --workspace /absolute/path/to/workspace \
  --goal top-journal --policy-project PROJECT \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json \
  --design /absolute/path/to/scientific-design.json --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project init PROJECT \
  --workspace /absolute/path/to/workspace \
  --goal top-journal \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json \
  --design /absolute/path/to/scientific-design.json \
  --design-producer-agent codex \
  --design-producer-session OPAQUE_NATIVE_SESSION \
  --confirm-budget --json
```

Stop when capability/input coverage is insufficient or the projected cost has
not been accepted. Use the same requirements and input plan for preflight and
initialization. For an explicitly selected evidence-report goal, omit the
top-journal Policy/design options; do not silently downgrade a requested
top-journal study to make admission pass.

For a new project on a compatible locked runtime, register the original-task
checklist immediately after init and before the first review or producer stage,
following [execution-assurance.md](references/execution-assurance.md). Use a few
meaningful user requirements and existing design/coverage IDs, not a duplicate
workflow or one requirement per tool call. Never invent historical acceptance
for a project that lacks those records.

## Validate, run, and recover

Production requires explicit models/prices, different producer and reviewer
agent families, a current setup doctor report, and real provider/reviewer
attestations. After preflight, use dry-run, then let the control plane identify
the next action:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run \
  --workspace /absolute/path/to/workspace --project PROJECT --dry-run --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run \
  --workspace /absolute/path/to/workspace --project PROJECT \
  --max-cycles 20 --progress-jsonl --json
```

For `native-stage-required`, do not keep calling `research run`. Prepare the
exact next stage, perform the returned prompt in this current app/session, and
submit its JSON through the CLI. Discovery evidence must be fetched through the
packet's broker command or its native `runDataCapability` command for a selected
structured source. Use the dynamic data catalog and the locked `data describe`
instead of memorizing operations. Follow `runDataCapability.readArgv` whenever
the first data view has a cursor and exhaustive row-level review is required;
otherwise disclose the exact presented fraction. Record discovery assessments incrementally instead of
returning all source metadata in the final stage output. Record native
Web/Browser activity, formalize useful leads through the broker, and bind every
network download to its exact download event before artifact registration.
After acquire, disposition every acquired content artifact, register exact
line-range/JSON-Pointer evidence atoms, and freeze typed content. A stopped
acquisition/content gate still preserves these frozen results but prohibits
inference. Only then proceed through inference, analyze, the mechanically
generated Claim-Evidence Graph, and synthesize. Only after that may
`research run` launch the configured independent reviewer and perform
mechanical closure. Follow
[references/native-execution.md](references/native-execution.md) for the exact
commands and recovery rules.

At idle stage boundaries, record the checks actually performed against each
current requirement before launching the existing independent review. Preserve
failed, not-run and inconclusive outcomes; a supported negative finding is not
automatically a failure. Bind computational acceptance to its actual native run
when the locked CLI supports observation; a self-reported command remains
unverified execution. Read large bound artifacts on demand instead of dropping
requirements or changing frozen evidence to fit an arbitrary context limit.

For a top-journal project, `research status` may require `research-design`,
`evidence-construct`, or `pilot-methods` review before it exposes the next
native stage. Produce the bounded assessment in this native host, then use a
fresh configured reviewer session through the CLI. A real-record construct
canary occurs only after acquisition and typed-content snapshots are frozen,
and an outcome-blind methods pilot occurs after that canary and before analysis.
The construct assessment may reference only source IDs, exact atoms, and
full-text/date states from the frozen chain. Pass its exact, external JSON canary files through
`--canary-artifacts`; an unbound digest or invented source ID is a mechanical
failure. Reviewer prose cannot override these failures.

Proceed only when doctor reports ready. Discovery uses only locked broker or
native data evidence capabilities for formal retrieval. Acquisition uses its
packet-governed download, parsing, and artifact-registration operations;
analysis, synthesis, and independent review do not acquire additional evidence.
Doctor, preflight, dependency,
provider, and evidence-coverage failures must stop the workflow. Never silently
downgrade a systematic task to a standalone SCI, report, patent, web, or paper
operation; only the user may explicitly narrow the request to one isolated
standalone operation.

Inspect state with `research status`; the default list contains only
authoritative work. A recovery fork supersedes its source. Archive completed or
superseded history, and abandon unfinished history, instead of leaving ambiguous
project variants. Use the exact native stage `abort`, `research project retry`,
or `research project fork` only with explicit user direction; do not reset or
delete state.

When the next material step requires user authorization, login/MFA/challenge
completion, or an external institution's response, request the packet's durable
handoff and stop. Do not spend the remaining search budget on low-yield
substitutes. Resume only after the handoff is explicitly resolved. A complete
project has a passing independent review, `outputs/report.md`, and
`outputs/closure.json`. Return the permanent evidence locators, review-packet
binding, usage/cost, decision, and material limitations.

For a top-journal goal, that base closure is not publication closure. Author the
final paper and role-complete submission files in this current native host,
freeze their exact manifest with the assessment, inference chain,
Claim-Evidence Graph, and reproducibility record, obtain fresh configured
other-family evidence, methods/reproducibility, domain/novelty, and
journal-editor reviews, then mechanically close the publication generation.
Any Policy, manuscript, or submission-file change invalidates downstream
approval/review. Return
only the CLI-computed bounded readiness language; never promise acceptance.
Report workflow status, publication verdict, `task.originalScope` and
`task.currentScope` separately. A closed workflow or narrower completed scope
does not answer withdrawn or unresolved original requirements.
Export and independently verify a portable project audit bundle before external
handoff or archival; it must contain the formal evidence bytes and review
objects, not merely receipts or local-path references.
