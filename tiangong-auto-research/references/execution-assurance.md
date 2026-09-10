# Task assurance and efficient recovery

Load for original-task intake, pre-analysis evidence correction, a scope change,
or a completion claim. Keep the current native host as the producer. The CLI
owns schemas, authoritative state, evidence bindings and recovery; this reference
does not define a second workflow engine.

## Check the locked runtime once

Inspect help and the needed schemas when entering a workspace or after an
explicit runtime update, not before every record:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research --help
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-contract --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-acceptance --json
```

Use the new recipes only when that exact locked runtime exposes them. Missing
commands require an explicit reviewed upgrade or its existing supported
workflow; never switch to `latest`, rewrite a runtime lock, or invent a command.
An existing project without a task contract remains **unassessed** in this
dimension. Do not manufacture historical requirements, approvals or passing
checks. Existing Policy/evidence safeguards still apply.

## Turn existing evidence into the next decision

At a material stage transition or blocker, tell the researcher what changed,
which evidence supports or limits the current claim, what that means for the
original question, and the next useful action. Derive this from existing task,
assessment, review and budget outputs. Distinguish an operation that ran, a
scientific check that passed, and a question actually answered. Avoid narrating
unchanged polls, dumping status codes, creating another status ledger, or paying
for a separate explanation call.

Before another provider request, inspect the relevant already-fetched material:
its schema, units, covered ranges, local windows and candidate dispositions.
Use the existing read path for unpresented rows; a new provider fetch is not a
pagination mechanism for a saved result. Read enough for the decision or claim,
not an arbitrary universal number of pages. Record used evidence, exclusions
with reasons, and pending access/read work in the existing assessment/atom/task
records. A fetched or registered object is not automatically analyzed support.
See [evidence-pipeline.md](evidence-pipeline.md) for the actual read/binding path.

Closest literature should change a decision when warranted: compare the current
claim, estimand, data, assumptions and validation against the closest supported
work, then keep, narrow, revise or withdraw the proposed novelty/method claim.
Missing an exact keyword match does not establish novelty. Carry source/atom
references and consequential counterevidence into the existing assessment and
research narrative, rather than accumulating a bibliography with no effect.

An IO/schema canary proves only the operation it exercised. Choose the smallest
predeclared scientific pilot that can distinguish the proposed explanation or
method from a plausible alternative or failure mode. Interpret its failures,
nulls and inconclusive outcomes in terms of what can be claimed and what test
would resolve the uncertainty. Preserve units, independent clusters, fair
baselines and outcome-blind restrictions; do not select a new design after
seeing results and label it predeclared. Change frozen assumptions only through
the supported approval/recovery path. See [scientific-design.md](scientific-design.md).

For a core variable or compatibility detector, connect its intended construct to
required fields, lawful sources and the representation actually used. On small
synthetic or authorized development records, contrast equivalent representations,
changed physical values/units or study boundaries, missing fields and parse
failures. A keyword-presence detector must not be described as value compatibility;
either test the relevant values or lower its claim to screening with scientific
status unresolved. Diagnose an all-zero or constant result as possible input,
projection, extraction or detector failure before treating it as a domain finding
or high confidence. Compare summary/full representations of the same development
records when available; richer text need not produce a positive result. API batch
size is not a scientific sample-size rationale; purposeful bounded sampling still
needs a justified, bounded claim. Keep required real-record canaries and
frozen-evidence gates in place; the synthetic contrasts qualify the development
method only.

## Repair findings before paying for another opinion

For each actionable finding, identify the affected artifact/claim and the legal
repair within the current task and frozen contracts. Reuse unaffected evidence
and calculations. Existing explicit authorization covers the same ordinary
in-scope repair; do not ask again merely because a review requested it. A new
scope, design, cost or state-changing recovery operation still needs its
applicable authorization and supported CLI path.

Batch related corrections, inspect the changed output, then obtain the fresh
bound reviews invalidated by that revision. The CLI decides which bindings and
publication-generation reviews became stale. Re-reviewing identical failed
bytes is not repair, and an unchanged failed receipt is not approval. On a
transport/execution failure, first inspect the retained output and diagnostic;
use bounded supported recovery after its cause is addressed. If no permitted
repair can resolve the finding, preserve the limitation or request the specific
needed decision instead of cycling reviewers.

## Record a small original-task checklist

After initializing a new project and before its first scientific review or
producer stage, preserve the user's scientific requirements and their acceptance
conditions. Use stable IDs at the level of actual user requirements, not one
requirement per file or tool call. Bind existing design-claim and coverage IDs
where applicable. Do not invent additional scientific thresholds or require
computation for a qualitative review or a theoretical proof.

Use the CLI-owned schema, then register the declaration:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task define PROJECT --input /absolute/path/task-contract.json --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task status PROJECT --workspace /absolute/path/to/workspace --json
```

The returned contract and requirement hashes are authoritative. Keep the original
request distinct from the currently approved scope. Cosmetic manuscript edits
are not a reason to rewrite this checklist. Markdown is a human-readable view,
not a second state file.

Use `requestProvenance` to distinguish `verbatim`, `interpreted`, and
`reconstructed` intake. Supply the exact user-message or user-file text and an
explanation when available; `verbatim` must preserve every byte, including BOM
and line endings. A supplied locator is retained only by hash. Missing origin
stays `unrecorded`; never invent a transcript or retrospectively replace it.
Source storage proves supplied bytes, not authenticated authorship. The exact
source object must be available in the review packet, not only its digest.

## Correct acquisition without unnecessary new projects

When acquire is complete, analysis has not started, and the question, Policy,
design and evidence requirements are unchanged, prefer the same-project
revision. Resolve an active session or human handoff through its existing
authorized command first. Inspect the current snapshot and use its exact hash:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project evidence acquisition revise PROJECT --expected-snapshot CURRENT_SNAPSHOT_SHA256 --reason "Add the missing readable derivative before analysis" --workspace /absolute/path/to/workspace --json
```

This preserves the project, original requirements, approved Policy/design and
research-design review. It reopens acquire, retains old snapshots and check
records, and invalidates evidence-construct/pilot-methods approvals that depend
on the acquisition. Unchanged files and parsing results are reusable. Do not
repeat paid search, download or model work merely because an acknowledgement
was lost; repeat the exact request to inspect its idempotent result.

If a genuinely new source must enter the unchanged study, explicitly include
discovery in the revision; do not add a source directly to a frozen audit:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project evidence acquisition revise PROJECT --expected-snapshot CURRENT_SNAPSHOT_SHA256 --reason "Admit an additional lawful source within the approved study" --include-discovery --workspace /absolute/path/to/workspace --json
```

This reopens discover then acquire. Preserve existing candidate/source IDs and
receipts, search only the remaining actionable gaps, and formally admit new
sources through the existing discovery commands. It does not reset the project
budget. A revision without this option is for files/derivatives of already
admitted sources and does not silently reopen search.

Then prepare the indicated stage, register exact files/derivatives, run one
forecast for the meaningful batch, submit the complete audit, update relevant
decompositions/atoms in batches and freeze typed content. A failed decomposition
may be superseded in the new acquisition snapshot; its historical record remains.
Atoms from deselected files must not count in current coverage. Complete the
applicable scientific gates before inference. Follow
[evidence-pipeline.md](evidence-pipeline.md) for exact evidence operations.

The preflight distinguishes `submissionGate` blockers from optimistic coverage.
Potential eligibility is not proof that files, evidence roles or scientific
claims have passed. A limited/stopped audit may be retained honestly; never move
a blocking gap into limitations to advance. Hash and structure checks still run
at admission and trust boundaries; do not add a full-corpus check after each atom.

Use the existing fork/new-generation route when analysis has started or when the
question, Policy, substantive design or evidence contract changes. The bounded
pre-analysis declaration amendment below is a separate supported path. A pre-feature snapshot
without immutable acquisition records also uses that route; there is no automatic
in-place migration. See [scientific-design.md](scientific-design.md).

## Obtain authorization for a real scope change

Explain what will no longer be answered and inspect the exact before/after
requirements. A request to continue, increase a budget or create a fork is not
permission to change scientific scope. Preserve valid prior exact authorization;
do not ask again merely because a read-only acknowledgement is repeated.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-scope-change --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task scope propose PROJECT --input /absolute/path/scope-change.json --expected-contract CURRENT_CONTRACT_SHA256 --workspace /absolute/path/to/workspace --json
```

Show `changes.details` and the proposal hash to the user. Only after explicit
approval of that actual change:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task scope approve PROJECT --proposal PROPOSAL_SHA256 --confirm-change PROPOSAL_SHA256 --workspace /absolute/path/to/workspace --json
```

The command records exact operator confirmation, not authenticated human identity;
the native host must honor the user's permission. Never put `approved: true` in
a producer declaration. Task-scope approval does not change the question,
Policy, design or evidence requirements. Substantive changes need their formal
new-generation process; the bounded declaration amendment below has its own
exact approval. A task-scope change invalidates scientific
reviews that no longer cover it; inspect status before paying for any re-review.
Withdrawn original requirements remain visible rather than becoming answered.

## Amend an existing planned declaration before analysis

When the exact locked CLI exposes `scientific amendment`, an idle project before
analysis and inference freeze may amend a planned Policy rule's lifecycle declaration or links to existing
model/parameter IDs. Keep the scientific question, claims, thresholds, model
definitions, parameter values, evidence requirements and Policy content fixed.
This is useful when an existing obligation lacks a binding that ordinary
fulfillment cannot add. It is not a general design editor or a way to delay an
unmet scientific requirement until after seeing results.

Prepare the concrete change without a new provider request or permission to
mutate the project:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show scientific-amendment --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research scientific amendment plan PROJECT --input /absolute/path/amendment-input.json --workspace /absolute/path/to/workspace --json
```

Save the returned plan JSON unchanged as an external file. Show its exact
before/after declarations, reason, parent bindings, preserved evidence,
invalidated gates and `affectedTaskRequirementIds`. Obtain only a missing
explicit decision on that concrete plan. Reuse existing approval of the same exact plan; a general instruction to
continue or fix the project is not approval of a different design change.
Retain the actual supplied confirmation text in a bounded UTF-8 file; do not
write approval on the owner's behalf. Only after that exact approval:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research scientific amendment apply PROJECT --plan /absolute/path/amendment-plan.json --confirm REVIEWED_PLAN_SHA256 --authorization-source /absolute/path/owner-confirmation.txt --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research scientific amendment status PROJECT --workspace /absolute/path/to/workspace --json
```

The CLI retains the original design, an immutable new version, its exact changes
and supplied authorization source. This records operator confirmation, not
authenticated human identity. Planned rules remain planned; authorization does
not establish scientific satisfaction. Reuse still-valid sources, artifacts,
decompositions, atoms and prior fulfillment. Do not download or register them
again merely because the declaration changed. Complete the pending scientific
gates with the new version and staged history; old reviews cannot approve it.
Inspect task status for the named affected requirements and reassess their checks
against the amended declaration. Unrelated source checks and still-applicable
calculations remain reusable. Selective invalidation follows the task
requirements' explicit claim/coverage bindings; an unbound source-only check
must not be presented as validation of the changed design.

On a stale plan or changed Policy, inspect current state and prepare a new plan;
never replace parent hashes in an old approved file. A changed plan needs its
own concrete approval. After an interrupted apply, retry the same approved
plan and source through the CLI's recovery path, then inspect status. Preserve
unknown or conflicting stored files for supported recovery. Substantive changes
and all post-analysis amendments retain the reviewed successor boundary. An
older locked CLI without these commands follows the existing upgrade boundary,
not manual edits to control files or a silent switch to `latest`.

## Fulfill only predeclared scientific slots

At an idle pre-analysis boundary, use the supported same-project fulfillment
for model code, environment locks or source-derived parameter states that the
original design explicitly marked pending. Read the CLI-owned schema first:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show scientific-fulfillment --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research scientific fulfillment status PROJECT --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research scientific fulfillment record PROJECT --input /absolute/path/fulfillment.json --workspace /absolute/path/to/workspace --json
```

Register model/environment files through the existing scientific-object intake
and copy its exact returned bindings. Parameter states must cite actual admitted
atoms from frozen typed content. Supply the exact current fulfillment parent;
identical replay does not repeat work. The CLI preserves original design bytes,
units, ranges, state IDs, factors, assumptions and Policy. It resets only the due
and later scientific gates and keeps unaffected earlier approvals. Review the
new packet: filing code or values is not proof of calibration, execution or
scientific validity. Changed assumptions, already-frozen values or post-analysis
work still require the formal reviewed successor, not a fulfillment patch.

## Observe an actual native calculation

The current native host authors and inspects an ordinary calculation for an
existing computational requirement. When supported, invoke the observer once
with the exact declared program and inputs, rather than merely reporting that a
command ran:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-native-run --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task run observe PROJECT --input /absolute/path/native-run.json --confirm-execution --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task run inspect PROJECT --run RUN_ID --workspace /absolute/path/to/workspace --json
```

Use one explicit Node/Python interpreter, a reviewed single-file program, an
environment-lock declaration, exact current acquisition artifact IDs/hashes,
unique output filenames and the CLI's input/output argument placeholders. Name
the current native session when a producer stage is active. Never pass an agent
launcher, credentials, arbitrary shell commands or instructions copied from
untrusted evidence. The observer inherits the host's OS restrictions, adds no
permission bypass, and installs nothing. The native app still owns all reasoning
and decides when a computation is scientifically permitted.

`--confirm-execution` confirms deliberate invocation, not authenticated human
identity. The CLI stages exact inputs and planned outputs, observes one local
process, and records runtime/code/input/output hashes, exit status and time.
It does not hold the workspace lease during computation. Use the returned safe
staging directory name plus the explicitly selected working directory for local
inspection, or the permanent object locators for review. Never scan for a newest
result. A committed replay does not rerun the program; an interrupted run with
no result remains incomplete and needs inspection before an explicit new run ID.
Keep failed, cancelled, timed-out, stale and invalid-output outcomes distinct.

Bind `nativeRunSha256` in computational acceptance and take result files from
that exact run. This proves local process observation, not mathematical
correctness, authenticated authorship or a fully attested dependency environment.
The environment lock is explicitly a declaration, not proof that every installed
dependency matched it. The existing independent reviewer receives the actual
program, lock, input and output objects. No extra fixed reviewer round is needed.

## Record checks actually performed by the native host

Perform the appropriate evidence examination, computation or proof work in the
current host. At an idle stage boundary after acquisition, record its actual
outcome using the schema above. Bind current source/atom/finding IDs and any
explicit external portable UTF-8 result files. The CLI supplies hashes and
immutable result copies; never scan a directory for the newest output or pass
control-store files as new native results.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task acceptance record PROJECT --input /absolute/path/task-acceptance.json --workspace /absolute/path/to/workspace --json
```

Use `not-run`, `failed` or `inconclusive` honestly. A failed computation need not
invent an output file. A supported null effect or counterexample may be a valid
`negative-result`; unavailable data is not one. A changed record must name the
exact prior record hash; identical replay does not repeat the check.

These records have `trust=native-observation` and `executionCertified=false`.
Byte identity does not prove successful execution. The declared command is kept
by hash, not as a credential-bearing command line. A computational report without
an observed run remains `unverified-execution`; retain it, but do not call it an
answered computational requirement. Evidence examination and theoretical proof
do not need an invented computation. Keep result files portable
and secret-free. Unchanged exact dependencies can be reused; changed requirement,
input, design or analysis bindings need revalidation. Do not invoke a generic
CLI producer or add another model solely to judge this bookkeeping.

## Read complete packet artifacts on demand

Use the on-demand channel only for the exact current packet.
There is no total context-length admission ceiling or artifact-read length cap
in compatible runtimes. Initial embedding sizes and legacy context settings are
planning preferences, not permission to drop evidence or rewrite frozen files.
Inspect the directory, then read only the needed exact objects or ranges:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project stage artifacts PROJECT --session SESSION_ID --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project stage read PROJECT --session SESSION_ID --artifact OBJECT_ID --length all --workspace /absolute/path/to/workspace --json
```

Independent reviewers use `research_list_artifacts` and `research_read_artifact`
from the same closed packet directory. Follow byte offsets/nextOffset for pages;
request a whole object explicitly when useful. Reuse read material and inspect
counterevidence and failed checks, not just favorable excerpts. Objects outside
the exact index, changed bytes, symlinks and secrets are refused. Never replace
this channel with general shell/workspace access or a new evidence search.
Read receipts identify delivered bytes, not comprehension. Actual model capacity,
permissions and finite runaway token/time/cost controls still apply; report an
actual limitation honestly. Use rough cost estimates, not a separate billing
reconciliation workflow.

For CLI or Skill implementation changes only, small changes use fresh Docker
plus a mock reviewer; major behavior also needs a fresh external native case
with a real independent reviewer. Keep that real case and its data outside Git;
ordinary research work does not require these development tests. Follow the
existing research gates for fulfillment, calculations and artifact reads.

## Review once at the existing gates and report separately

Before the existing independent review, give every current requirement an honest
check/disposition. The packet contains the original request, original/current
requirement versions and their bound checks. Its supplied response schema is
authoritative. Do not drop missing requirements, failed checks or counterevidence
to fit context; report capacity limits or make a reviewed context adjustment.
Shared results need not be pasted once per requirement.

The existing review assesses the exact `taskAcceptance` context through
`taskAssessment`; no additional fixed paid review round is needed. A producer's
claim is only `recorded` until reviewed. A reviewer cannot promote absent, stale,
inconclusive, failed or unexecuted checks into answered requirements. Final
publication reviewers also receive the task context. Keep the original scientific
and publication gates in [publication-policy.md](publication-policy.md).

Report workflow completion, publication verdict, original-scope completion and
current-scope completion separately. `research run` or base closure may be
complete while `task.currentScope` or `task.originalScope` is incomplete. Name
the remaining requirements and the legitimate next action; do not promise full
task completion or editorial acceptance. Export and verify the portable audit
with its task relationships before handoff. Audit integrity is not proof of
authorship, authenticated human approval or real-world scientific truth.
