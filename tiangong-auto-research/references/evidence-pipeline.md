# Evidence-first research pipeline

Load this reference before discovery, acquisition, evidence refresh, or an
addendum. The operating sequence is:

```text
broad search -> strict admission -> gap filling -> evidence freeze -> inference
```

The sequence is a correctness boundary, not merely a preferred writing style.
Do not analyze while evidence is still mutable, and do not use analysis to
decide what the evidence ledger claims was retrieved.

Use these recipes through the workspace-locked runtime. If its help does not
expose a needed command, request the reviewed setup/CLI upgrade rather than
switching to a floating version or imitating the operation with control-file
edits.

## 1. Define the coverage contract

Before spending provider or model budget, describe the question as reviewed
evidence requirements: dimensions, source types, required capability IDs,
required discovery scopes, minimum source/full-text/dated counts, and date
boundaries. `research project preflight` is authoritative for capability,
context, reservation, and expected-cost gaps.

For scientific evidence roles, a flat `sourceTypeRequirements` array means
all-of, not alternatives. When scientifically justified, use the CLI schema's
explicit `allOf`, `anyOf`, and `atLeast` groups; every supplied group must hold.
Do not add a required type merely because a search capability is available,
or relabel a source to satisfy a missing type.

Production requires an independent public-internet capability. Add every
owner-whitelisted database whose contents matter to the question as an exact
required capability; a general web result cannot silently substitute for it.
For top-journal work, the frozen scientific design must also enumerate every
lawful, relevant route available in the configured environment: broker
capabilities, native Web/Browser channels, OA/download adapters, explicitly
authorized browsers, licensed or owner-provided material, external requests,
and field collection. Omitting an applicable route is a design/review defect,
not permission to stop early.

## 2. Search broadly in bounded batches

The discover packet contains a mechanically derived `discovery.plan` and live
progress. The working call budget scales with reviewed dimensions, source
types, required channels, minimum sources, and a separate gap-fill reserve. It
is no longer a fixed six-call allowance, but it never exceeds the workspace
hard ceiling of 256 in a new production workspace. Treat that ceiling as a
runaway guard, never a quota:

1. Exercise required first-pass capabilities.
2. Use broad, high-yield queries across distinct selected channels.
3. Inspect registered candidate IDs and coverage after each batch.
4. End the broad first pass when reviewed coverage is supportable; minimum
   counts alone do not establish task completion or scientific adequacy.
5. Spend the remaining calls only on explicit gaps, counterevidence, missing
   dates, missing source types, applicability limits, or full-text candidates.

Exact repeated requests reuse the project evidence cache without another
provider network call. Every returned bounded view still consumes one reviewed
broker-view slot and its context reservation, preventing cache/pagination from
bypassing package limits. Use pagination or bounded JSON Pointers instead of
placing a large response wholesale into model context. The permanent raw object
remains content-addressed even when only a bounded view reaches the producer.

Every formal network occurrence must pass through the packet's broker command.
The broker owns endpoint policy, credentials, retries, response bounds,
sanitization, immutable bytes, and receipts. A standalone search result can
help find a lead but cannot replace a required broker occurrence.

The packet also exposes built-in structured data capabilities dynamically. The
catalog distinguishes the external data source, the CLI capability, and each
atomic operation; use its summary, `provides`, `doesNotProvide`, and selection
hints to choose a source, then inspect only the selected operation through the
same workspace runtime lock used by the packet:

Only capabilities whose catalog availability is `available` are projected;
suspended capabilities are not projected into Auto Research. The standalone
catalog retains them so operators can inspect the suspension reason and resume
criteria without offering them to the Agent as executable evidence sources.

The packet declares
`runDataCapability.executionKind=workspace-cli-relative-argv`; always
prefix its resolver-relative `argv`, `readArgv`, and `describeArgv` with the locked
`node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace --` command.
Do not execute their first token as a PATH-resolved global CLI.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  data describe <capability-id> --json
```

Follow upstream/downstream
hints when they are useful, but do not turn them into a mandatory workflow.

Run the exact published `DataRunRequest` only through the packet's
`runDataCapability` command, whose underlying route is:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence data run PROJECT \
  --request /absolute/path/to/data-run-request.json \
  --workspace /absolute/path/to/workspace --json
```

This is an in-process Research adapter over the same TypeScript data runtime,
not a second connector implementation. It preserves the core result and core receipt digest,
then adds the Research call/item/byte/context budget,
namespaced owner-only credential mapping, immutable data-runtime receipt,
candidate, ledger, journal, and review bindings. A successful or explicit
partial result may be assessed with its stated completeness and missing ranges;
a blocked result is journaled but never admitted as evidence. Never call
standalone `data run` inside a project, because that intentionally creates no
Research receipt or ledger state. New registry operations appear without a
corresponding edit to this Skill.

Interpret the returned communication as three independent dimensions:

- `providerCoverage` reports whether the provider supplied the requested
  source ranges and identifies upstream gaps.
- `limitCoverage` reports whether explicit operation/runtime bounds stopped the
  run. A bounded result may still have complete provider coverage within that
  declared request.
- `contextView` reports only how much of the already persisted result is in the
  current Agent view. Projection is not acquisition loss.

If `contextView.nextCursor` is non-null, the packet's
`runDataCapability.readArgv` reads the next bounded page from the same immutable
evidence object. Substitute only the returned receipt ID and opaque cursor;
never decode, increment, or reuse a cursor for another receipt. Continue until
`nextCursor is null` before claiming exhaustive row-level review. This local
read does not consume another provider call or create a second evidence
occurrence. When a summary, sample, or adaptive decision genuinely needs no
further page, record the exact presented fraction and the unreviewed remainder
as a limitation. Never convert an Agent context projection into a provider
`partial`, and never convert a provider or limit gap into mere context
truncation.

## 3. Keep one immutable evidence ledger

Inputs, broker results, structured data results, and native-app discoveries are normalized into stable
candidate IDs and deduplicated by canonical public URL, DOI, or input hash.
The append-only hash-chained ledger records discovery occurrences, admission or
rejection judgments, artifact registration and assessment, snapshots, claim
use, reviewer binding, and supersession.

The current native Codex or Claude app may use its own Web/Browser experience
to find additional leads. Record every material search, navigation, download,
or file-inspection occurrence through the packet's `recordActivity` command.
For a top-journal project, pass the exact frozen route as
`acquisitionRouteId`; the control plane rejects unbound or mismatched activity.
The control plane persists only a sanitized input hash, channel, counts, status,
challenge class, and candidate IDs. Then register safe, non-secret candidate
metadata through `registerCandidate`. Such a lead remains
`supplemental-not-admitted` until the same URL/DOI has an immutable broker
occurrence. Never cite or admit a native-only candidate. Registered inputs are
already formal candidates under their own content-hash identity. The frozen
snapshot includes the verified activity summary so native work and formal
broker evidence remain one auditable ledger rather than two hidden work logs.
Each structured data result already has its own immutable data-runtime
occurrence; assess the returned candidate ID directly. Do not re-register it as
a native URL lead or pretend that a partial result is complete.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence candidate register PROJECT \
  --record /absolute/path/to/candidate.json \
  --workspace /absolute/path/to/workspace --json
```

Assess candidates as they are found in batches of at most 25 through the
packet's `recordAssessment` command. Each append-only batch may replace the
latest judgment for a candidate without repeating deterministic source
metadata. The model does not generate locators, hashes, retrieval dates, URLs,
or receipt identity; the control plane joins those fields from the ledger. The
final discover submission is therefore only a compact closeout containing one
status per reviewed dimension, limitations, and remaining gaps. Omitted
candidates remain available for a later gap-fill pass. Record an explicit
rejection only when it is meaningful and supportable.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence assessment record PROJECT \
  --record /absolute/path/to/assessment-batch.json \
  --workspace /absolute/path/to/workspace --json
```

## 4. Audit acquisition before inference

After provisional admission, the native `acquire` stage assesses every source
exactly once. Use selected external acquisition/document Skills or an explicitly
authorized browser. Before a large transfer with a known size, use the offline
preflight; it reads no file body and does not contact a provider:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence artifact preflight --bytes KNOWN_BYTE_COUNT \
  --workspace /absolute/path/to/workspace --json
```

The per-artifact limit and aggregate package-output limit are distinct. For a
local file use `--path /absolute/path/to/exact-file` instead of `--bytes`.
An oversized file requires a lawful provider-side subset/filter or an explicit
reviewed size decision, not a blind download, arbitrary chunking, or a skipped
hash/format check. Size eligibility alone never proves valid file content.

For every network file, capture the exact browser Download
object or equivalent transport completion, save it to a unique planned staging
path, and first bind that event through `bindDownload`. A failed or cancelled
event creates no successful binding and cannot register an artifact. Never scan
a directory for the newest file and never infer success from file existence.
For a top-journal project, the binding record must carry the exact
`acquisitionRouteId`. Broker fetches use the corresponding snake-case
`acquisition_route_id` argument.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence download bind PROJECT \
  --candidate CANDIDATE_ID --record /absolute/path/to/download.json \
  --workspace /absolute/path/to/workspace --json
```

Register the exact downloaded file with the returned binding ID. A text or
structured derivative must instead name the same-candidate parent artifact;
this preserves a mechanical chain back to the bound network file.

When a structured data operation produces files, the adapter preserves every
declared hash-bound artifact in the permanent evidence object store before its
temporary output directory is removed. Treat binary or provider-supplied files
as untrusted: during acquisition, register the exact staged file or a legitimate
readable derivative against the returned data candidate, then follow the same
decomposition and atom rules below. The data-runtime receipt proves retrieval
and byte identity; it does not by itself prove that a binary file was safely
parsed or visually reviewed.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence artifact register PROJECT \
  --candidate CANDIDATE_ID --path /absolute/path/to/exact-file.pdf \
  --download-binding DOWNLOAD_BINDING_ID \
  --source-url https://publisher.example/article \
  --workspace /absolute/path/to/workspace --json
```

The registry verifies size, SHA-256, exact event/file binding, and structure. PDF
registration requires a parseable non-empty document with an EOF marker. ZIP
and Office Open XML registration verifies the central directory, safe paths,
supported compression, declared uncompressed sizes, and CRC; encrypted, ZIP64,
or unsupported compression is rejected. XLSX also records declared sheet
names. Optional license, license URL, host type, and article version are stored
only when the source explicitly declares them.

Keep these distinctions exact:

- `registeredFullFile`: an exact full file is hash-bound for audit/review.
- `producerContextLevel=full-input`: the registered input itself is available
  through its reviewed input contract.
- `producerContextLevel=bounded-text-artifact`: a registered UTF-8 text, JSON,
  CSV, or Markdown derivative is selectively embedded or read through the exact
  on-demand packet channel. HTML stays
  metadata-only because an error or challenge page must not masquerade as
  acquired full text.
- `producerContextLevel=metadata-only`: no producer-readable full text was
  admitted. A raw PDF/DOCX/PPTX/XLSX alone remains here.
- `reviewerBoundFullFile`: the independent review packet binds the exact full
  file; this does not mean the reviewer model read all binary bytes.
- `visuallyVerified`: false unless a future explicit visual-verification event
  records otherwise.

An accepted source may remain metadata/abstract-only when the requirements
allow it, but the audit must state that limitation. Unresolved blocking gaps
do not erase successfully acquired evidence: acquisition freezes the complete
source/artifact/gap audit and marks its separate `inferenceGate=stop`.
Continue only far enough to decompose everything already acquired and freeze
the typed-content record; then request the exact access/scope handoff. Never
report the stopped snapshot as inference-ready.

Use `limitations` for nonblocking qualifications and an intentional outcome
seal that the current gate permits. `gaps` means unresolved blocking evidence
deficits; every entry stops inference. Never move a genuine deficit into
`limitations` just to pass. Later Policy obligations belong in the reviewed
design's due-gate dispositions, not a second informal to-do list.

Create and register all needed readable derivatives while acquire is active.
Before final acquisition submission, forecast the proposed audit, especially
for a top-journal design with role-specific floors:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence content forecast PROJECT \
  --input /absolute/path/to/acquisition-audit.json \
  --workspace /absolute/path/to/workspace --json
```

Inspect the exact known source/artifact gaps. A forecast pass means potential
eligibility, never that atoms, scientific relevance, or an independent review
already passed. Metadata-only entries cannot stand in for atom-eligible role
coverage. Run this once per meaningful completed acquisition batch and before
freezing, not after every small record. Keep acquiring only the explicit gaps
that remain lawfully actionable; retain an honest stopped audit and hand off
when the necessary evidence is unavailable.
The `submissionGate` distinguishes known submit blockers, such as an accepted
binary input without readable content, from potential eligibility. Keep honest
limited/stopped audits separate from permission to infer.

## 5. Freeze, then infer

Successful acquisition creates `outputs/evidence-snapshot.json` plus an
immutable project-local copy under `evidence/snapshots/`. The semantic snapshot
hash binds the question, evidence and acquisition records, ledger head,
receipts, selected artifacts, coverage, explicit gaps, the inference decision,
and parent/delta lineage. File existence is not readiness; inspect the semantic
hash and gate through `research status --json`.

Before any evidence-construct assessment or analysis, disposition every
acquired full-text/data artifact. Readable derivatives must already have been
registered during acquire and selected in its audit. After acquisition freezes,
record one decomposition object per source artifact; do not try to register new
files into the closed acquisition window:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence decomposition record PROJECT \
  --record /absolute/path/to/decomposition.json \
  --workspace /absolute/path/to/workspace --json
```

A complete decomposition names the parser/version, exact parent and output
artifact IDs, content classes, and limitations. A limited or failed
decomposition is an explicit disposition, not permission to omit the file.
Never claim that a PDF, workbook, archive, or HTML challenge page was read
merely because it was downloaded.

Register claim-usable evidence as exact atoms from a producer-readable
UTF-8/JSON/CSV/Markdown artifact. Each atom binds the admitted source and
candidate, exact artifact hash, a one-based line range or JSON Pointer, the
control-plane-extracted excerpt and excerpt hash, evidence role/dimension,
support/counterevidence/method/limitation function, scope, and limitations:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence atom register PROJECT \
  --record /absolute/path/to/evidence-atom.json \
  --workspace /absolute/path/to/workspace --json
```

Do not paste an invented excerpt or cite only a source-level ID when an exact
atom is required. For many records prefer the bounded atomic batch commands;
the CLI validates a shared snapshot/artifact view once instead of reloading it
for every record. Use the CLI-owned record schemas inside a
`{"schemaVersion":1,"records":[...]}` envelope and the limits returned by help:

Inspect `research schema show evidence-atom-batch --json` or
`research schema show artifact-decomposition-batch --json` through the same
locked resolver. The corresponding single-record schema names are
`evidence-atom` and `artifact-decomposition`. These schemas describe record
shape; real artifact, stage, lineage, and locator checks still run at admission.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence decomposition batch PROJECT \
  --record /absolute/path/to/decomposition-batch.json \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence atom batch PROJECT \
  --record /absolute/path/to/atom-batch.json \
  --workspace /absolute/path/to/workspace --json
```

One invalid item commits none of that batch. Correct that item and replay the
batch; an identical committed batch is idempotent. Single-record commands remain
appropriate for one-off additions. Do not launch a second validation loop per
item after a successful batch. Freeze the typed universe only after all acquired artifacts
have dispositions and all material claims have atoms:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence content freeze PROJECT \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research status --project PROJECT \
  --workspace /absolute/path/to/workspace --json
```

The `evidencePipeline` status reports acquisition gaps/gate, decomposition and
atom counts, typed role gaps, inference identity, and Claim-Evidence Graph
identity. A stopped acquisition or content gate prohibits inference. It does
not justify further low-yield substitute search after lawful routes are
exhausted.

For top-journal work, the real-record evidence-construct canary and its exact
content-addressed JSON artifacts are reviewed against both frozen acquisition
and typed-content snapshots before the outcome-blind methods pilot. Only
snapshot source IDs and exact atoms count; full-text and date states are
re-derived mechanically. After those gates pass, preparing `analyze` freezes an
immutable `inference-snapshot.json` containing the exact sources, atoms, design
claims/edges, policy/review bindings, input artifacts, implementations, and
environment locks. Analyze schema v2 must bind that snapshot, one reproduced
analysis run, and for every finding the admitted source IDs, exact atom IDs,
design claim IDs, uncertainty, and applicability. Successful submit generates
`claim-evidence-graph.json` mechanically; do not hand-author it. Synthesis and
review may use only this verified chain and cannot fetch, register, or silently
substitute new evidence.

The review packet binds the current acquisition/content/inference/graph chain,
selected exact artifacts, permanent broker/data objects, bounded excerpts, analysis,
and report. Claim and review bindings are appended to the ledger. Mechanical
closure re-verifies all hashes and refuses a missing, changed, or stale
snapshot, graph, packet, context, receipt, artifact, or source.

## 6. Refresh through an addendum

If acquire completed but analysis has not started, first use
[execution-assurance.md](execution-assurance.md) to choose the supported
same-project revision. An acquisition-only correction preserves the approved
research design while invalidating its dependent post-acquisition reviews.
Use explicit `--include-discovery` only when a new source needs formal admission;
reuse prior receipts and files, and do not reset the budget or repeat unchanged
queries. Rebuild the current typed-content view; older failed decompositions
and deselected atoms remain historical, not current evidence.

When the locked runtime or snapshot cannot support that revision, or the design
changes or analysis has started, use the formal recovery/new-generation route in
[scientific-design.md](scientific-design.md). `--resume-through discover` can
reuse verified discovery and files in a successor. Its target-specific
Policy/design approval remains required. Never hand-edit frozen history.

Never mutate a closed project. When material new evidence exists, create a new
addendum project:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project addendum CLOSED_PROJECT --to NEW_PROJECT \
  --workspace /absolute/path/to/workspace --json
```

The original closure remains byte-for-byte unchanged. The addendum inherits
the verified base ledger/evidence/audit/artifacts, starts again at discover,
freezes a child snapshot with a mechanical added/changed/removed/unchanged
delta, and reruns analysis, synthesis, independent review, and closure. The
superseded project becomes stale and is hidden from default `research status`;
use `research status --all` only for lineage audit. Recovery forks have the
same single-authority rule: the new fork supersedes its source immediately.
Use `research project archive` for complete/stale history and
`research project abandon` for unfinished history; never infer the latest
project from its name or version suffix.

Use the status response's `discovery`, `snapshot`, `evidencePipeline`,
`nativeStage`, `lineage`, and `recommendedAction` fields instead of inspecting
control files manually.
