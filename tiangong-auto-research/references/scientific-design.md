# Scientific design contract and early review gates

Load this reference before admitting a top-journal project, changing its scope,
recovering into a new generation, or crossing the discover, acquire, or analyze
boundaries. The current native Codex or Claude host performs the scientific
reasoning. The CLI validates, freezes, hashes, routes, and audits it; the CLI
does not create the study design or launch a nested producer.

## Design before search

Start with the approved Research Policy and inspect the closed schema:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show scientific-design --json
```

In the current native host, write one project-specific design JSON. It must
make the following explicit before evidence search can consume the main budget:

- article identity, central study kind, supporting components, bridge edges,
  target journals, contribution statement, and allowed claim verbs;
- estimand, claims, result classes, and a directed claim/evidence graph;
- endpoints and their truth roles, comparable endpoint pairs, units, quantities,
  denominators, permitted terms, and prohibited overclaims;
- calibration/validation datasets, exposure identifiers, shared upstream data,
  independent data-generating processes, original units, independent clusters,
  effective independent units, and the resampling unit;
- every model equation/coefficient set, safe object locator, raw-file SHA-256,
  executable entrypoint, implementation status, exact environment-lock status,
  baseline role, and the gate at which any pending object must be frozen;
- empirical, regulatory, scenario, analytic, and decision thresholds, including
  sensitivity plans for non-empirical thresholds;
- factor levels, uncertainty application/composition rules, frozen or
  source-pending parameter states, and an exact parameter-state binding for
  every declared joint sensitivity state;
- required evidence roles, closest-work full-text floors, counterevidence, known
  gaps, baseline fairness, context construction, stop/handoff dispositions, and
  one disposition for every resolved Research Policy rule.

A model output is not field truth. A discrepancy between two engineering models
is a cross-model result unless one endpoint is independently observed and
declared as such. Resampling iterations reduce Monte Carlo error; they never
increase the number of independent experimental or observational units. Gross
installed mixture is not automatically binder, virgin material, network demand,
or an observed effect. Encode those boundaries in quantities and claim verbs.

## Register frozen scientific objects

Before writing a design that marks an implementation `executable-frozen` or an
environment `exact-frozen`, register the external raw files through the public
workspace-scoped intake. This happens before project initialization, so it does
not take a project ID:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research scientific object register \
  --kind model-implementation \
  --path /absolute/path/to/model.py \
  --media-type text/x-python \
  --workspace /absolute/path/to/workspace --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research scientific object register \
  --kind environment-lock \
  --path /absolute/path/to/requirements.lock \
  --media-type text/plain \
  --workspace /absolute/path/to/workspace --json
```

Copy each returned `sha256` and `objectLocator` exactly into the matching design
field. The locator has the form `lineage/objects/<sha256>/blob`. The CLI accepts
only bounded regular non-symlink files outside `.tiangong-research`, validates
reviewable UTF-8 media, hashes raw bytes, writes the blob atomically, and creates
a deterministic record without the host source path. Registration is
idempotent. Use `research scientific object inspect --kind KIND --locator
LOCATOR` to revalidate an existing record and blob. Do not hand-copy files into
the control directory, reuse an object under the wrong kind, or put an external
absolute path in the design.

The research-design review promotes registered Python, R, JavaScript, text,
JSON, TOML, or YAML bytes as exact project-local portable blobs. It records
their media type, object kind, safe source locator, registration-record hash,
size, and raw-byte SHA-256 in `stageInputs`; it does not parse code or lock files
as JSON. Preflight and project admission revalidate every frozen binding and
report the exact model/artifact/reason when an object is missing, unsafe,
kind-mismatched, metadata-invalid, or hash-drifted.

## Executable model and uncertainty contracts

A digest proves byte identity, not scientific executability. For every model,
bind both the implementation and its environment lock with safe retrievable
locators and declare `artifactHashBasis: raw-file-bytes`. A model marked
`executable-frozen` and an environment marked `exact-frozen` must already bind
their exact registered bytes. A compatible runtime preserves the originally
declared deadline when a pending object is fulfilled before that gate. Otherwise declare
`pending-source-acquisition` or `pending-runtime-lock` and a later
`evidence-construct` or `pilot-methods` deadline. Specification prose,
unresolved coefficients, an unpinned runtime, or an empty dependency lock is
not executable merely because its bytes are hash-bound.

A pending implementation must set `implementationArtifactSha256`,
`implementationArtifactLocator`, and `implementationEntrypoint` to `null`. A
pending environment must set `environmentLockSha256` and
`environmentLockLocator` to `null`. The early review does not try to promote
those absent objects; their Policy-owned future obligation remains explicit and
becomes blocking at its declared due gate.

Every pending model deadline must be owned by a `planned` Policy disposition at
the same gate through `modelStructureIds`. Every source-pending uncertainty
parameter must likewise bind its `freezeBeforeGate` through
`uncertaintyParameterIds`. A disposition cannot be `satisfied-by-design` while
its required implementation, environment, or parameter states remain pending.

For uncertainty groups, list every `jointStateId` and give one
`jointStateBindings` entry that selects exactly one existing state from every
grouped parameter. Keep factor levels and uncertainty multipliers distinct;
declare their application point and composition rule and preserve factor-level
identity. A large bootstrap count, a label-only joint state, or a full-factorial
name without exact bindings is not an auditable state space.

The directed bridge graph must be endpoint-continuous from every central model
output through decision and accounting consequences. Each edge declares its
operator, aggregation rule, scale reconciliation, join keys, spatial/temporal
alignment, quantities, uncertainty inputs, and model identities. Do not bridge
a model result to an overlay, scenario, or material claim only in prose.

Run preflight and initialization with the same exact design:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project preflight \
  --workspace /absolute/path/to/workspace \
  --goal top-journal --policy-project PROJECT \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --design /absolute/path/to/scientific-design.json --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project init PROJECT \
  --workspace /absolute/path/to/workspace \
  --goal top-journal --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --design /absolute/path/to/scientific-design.json \
  --design-producer-agent codex \
  --design-producer-session OPAQUE_NATIVE_SESSION \
  --confirm-budget --json
```

The session identifier is opaque input for reuse detection. Only its SHA-256 is
persisted. Preflight reserves the base packages, three early scientific reviews,
four final publication reviews, and one bounded revision cycle. Admission stops
when the complete lifecycle does not fit the finite runaway ceiling.

The design may pass admission with later-gate objects explicitly pending. This
is not permission to compute from them. The research-design review packet lists
each such item under `mechanicalAssessment.futureGateObligations`, including its
error code, exact due gate, object IDs, and owning Policy rule IDs. At the due
gate the same condition becomes a blocking mechanical error unless the exact
predeclared slot has been fulfilled through the CLI, or a reviewed successor
provides the materially changed design. Follow
[execution-assurance.md](execution-assurance.md) for the closed fulfillment
schema, exact parent binding and no-reexecution behavior.

This list also exposes ordinary `planned` Policy rules with their exact due
gate. A rule deferred to evidence-construct is not already discharged because
research-design passed. Fulfillment discharges only the pending-object filing
blocker; the unchanged Policy rule still needs independent scientific judgement.
For only an existing planned lifecycle declaration or model/parameter link,
use the amendment path explicitly confirmed by the owner in
[execution-assurance.md](execution-assurance.md) before analysis or inference
freeze. Substantively different design content requires a reviewed successor
before the deadline. Do not mark it satisfied with an invented
assessment or wait until a frozen acquisition makes an avoidable gap costly.

## Three early scientific gates

`research status` returns the next gate and a safe command. Do not prepare a
native stage while an upstream gate is pending, under revision, stopped, or
hash-invalid.

1. `research-design` blocks discovery. It checks identity, observability,
   claims/edges, truth roles, quantity ontology, validation semantics, known-gap
   dispositions, and lifecycle feasibility.
2. `evidence-construct` runs only after acquisition has frozen
   `evidence-snapshot.json` and the producer has dispositioned every acquired
   artifact, registered exact evidence atoms, and frozen
   `content-snapshot.json`. It blocks analysis. The native producer must
   construct the central joins/edges on real records without inspecting result
   values, record the exact canary artifacts, demonstrate that each required
   evidence role reaches its full-text, atom, and independent-source floor,
   disposition closest work, and provide a complete packet/on-demand route to
   central evidence. A token estimate alone is not a context admission ceiling.

This real-record construct canary is a feasibility gate, not a result-producing
analysis. Discovery metadata or a binary file alone cannot satisfy it. Every
coverage ID must exist in the frozen acquisition/content chain, claim-usable
support must bind exact atoms, and full-text/date claims are checked against the
snapshots. Put canary artifact SHA-256 values in the assessment and supply their
absolute canonical paths in a separate owner-reviewed JSON array through
`--canary-artifacts`; the CLI promotes the exact non-symlink JSON bytes and
binds them into the packet without persisting the host source paths. 3. `pilot-methods` runs after the evidence-construct review and blocks analysis. It checks
leakage/circularity, endpoint compatibility, baseline fairness, units and
denominators, threshold types, decision-loss metrics, validation-plan
coverage, and the original/cluster/effective/resampling-unit audit.

For each gate, inspect its assessment schema, have the current native producer
write a bounded assessment from the exact frozen inputs, and prepare a fresh
independent reviewer packet:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show scientific-assessment-research-design --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project scientific review prepare PROJECT \
  --role research-design \
  --assessment /absolute/path/to/research-design-assessment.json \
  --reviewer-agent claude \
  --reviewer-session FRESH_OPAQUE_REVIEW_SESSION \
  --workspace /absolute/path/to/workspace --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show scientific-review-research-design --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project scientific review execute PROJECT \
  --role research-design --confirm-review-cost \
  --workspace /absolute/path/to/workspace --json
```

Obtain user approval for the bounded reviewer cost before adding
`--confirm-review-cost`. Execution uses the configured `native-direct` or
`sandbox-bridge` transport, validates the prepared packet, and records a
sanitized execution receipt before mechanical submission. It never starts a
nested producer. Inspect `research reviewer status` for that configured
transport; a native-direct workspace does not need a bridge sidecar.

An already saved successful execution can be replayed without another model
call. A failed attempt requires inspection and explicit `--retry` after the
cause is corrected; do not blindly repeat it. A mechanically nonpassing packet
can receive an independent stop/handoff verdict, but reviewer prose cannot
upgrade its evidence. The manual `scientific review submit --review` path
remains available for an independently produced, exactly bound review; never
fabricate reviewer output in the producer session.

Repeat with `evidence-construct`, adding
`--canary-artifacts /absolute/path/to/canary-paths.json`, and then with
`pilot-methods`. The reviewer must use the
configured other agent family and a session unused by the producer or any prior
review. The packet binds exact design, Policy, assessment, stage outputs, and
reviewer identity hashes. Reviewer prose cannot override a failed mechanical
canary, missing evidence role, inflated effective sample size, invalid
resampling unit, unverified validation plan, or missing decision-loss metric.
Revise the assessment/design/method and prepare a new packet with a new session.

Review the exact promoted `stageInputs`. Their `sha256` values are digests of
the raw file bytes at the portable `path`; `sourceLocator` records provenance,
while `purpose`, `ownerId`, `mediaType`, `objectKind`, and
`registrationRecordSha256` bind the bytes to the scientific object under
review. Model implementations and environment locks use an exact portable
`blob`, not a newest-file scan or JSON reinterpretation. `packetSha256` is the packet's logical identity (excluding its own
identity field); the portable audit manifest separately records the raw stored
packet-file digest. Do not interchange those two hash meanings.

## Scope change and authoritative generations

Never edit frozen bytes by hand. The CLI's bounded pre-analysis acquisition and
task-scope revisions are described in
[execution-assurance.md](execution-assurance.md); they do not rewrite the
scientific design. Compatible runtimes separately allow the owner to approve
amendments to existing planned lifecycle declarations and model/parameter links
before analysis or inference freeze, with immutable history and fresh affected
reviews/checks. Substantive design changes still require a new generation.
Explain and obtain exact authorization for changed task requirements; a generic
continue/fork instruction is not that authorization. A top-journal fork or
addendum requires an approved Policy and scientific design whose `projectId`
matches the new target generation, plus a fresh native producer session:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project fork OLD --to NEW \
  --design /absolute/path/to/new-scientific-design.json \
  --design-producer-agent codex \
  --design-producer-session FRESH_NATIVE_SESSION \
  --workspace /absolute/path/to/workspace --json
```

Inherited outputs remain evidence, not inherited scientific approval. The new
generation starts at the applicable pending gates; a later ready package cannot
bypass an earlier gate. The source becomes explicitly superseded, the default
status shows only the authoritative descendant, and `--all` remains the history
view.

Prefer the supported same-project acquisition revision before analysis when
the study is unchanged. It preserves the existing research-design approval and
reopens only the explicitly selected evidence stages. The following fork is the
fallback when that revision is unavailable or a new generation is required;
it preserves discovery and files rather than repeating search and download:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project fork OLD --to NEW --resume-through discover \
  --design /absolute/path/to/new-scientific-design.json \
  --design-producer-agent codex \
  --design-producer-session FRESH_NATIVE_SESSION \
  --workspace /absolute/path/to/workspace --json
```

First approve the target-specific Policy and design as required for every
top-journal successor. The fork retains source download provenance and exact
artifact bytes, reopens acquire, and keeps old snapshots/reviews immutable.
Use the successor's status and artifact IDs to select unchanged files in its
new acquisition audit, then obtain only the missing lawful evidence. Rerun
typed-content and scientific reviews for that successor; inherited evidence is
not inherited scientific approval. For an evidence-report project without a
scientific design, omit the design options. Neither path is an in-place edit of
the old generation.

Filling the original design's pending uncertainty values, model code or exact
environment lock is not automatically a material redesign. Use the compatible
CLI's append-only same-project fulfillment before analysis; keep original bytes,
units, state identities, assumptions and Policy unchanged. The due and later
reviews are reset and receive exact fulfillment objects, while unaffected
earlier approvals remain bound to their deadline-specific view. A changed
assumption or replacement of an already-frozen value still needs a successor
with target-specific reapproval. Never edit an old object or manually mark an
obligation complete.

## Portable audit handoff

At a milestone or after publication closure, export an immutable directory and
verify it independently:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project audit export PROJECT \
  --output /absolute/path/to/new-audit-directory \
  --workspace /absolute/path/to/workspace --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project audit verify \
  --bundle /absolute/path/to/new-audit-directory --json
```

Before copying, export semantically reloads every present acquisition,
typed-content, inference, Claim-Evidence Graph, and publication object; a stale
or internally rehashed-but-disconnected chain is not exportable. The manifest's
`researchChain` records their intrinsic IDs/hashes and the publication
generation/package binding. The bundle includes only the selected project,
portable input copies, formal evidence/artifact bytes, Policy/design/review
objects, outputs, environment fingerprints, and safe journal derivatives that
retain source event/payload hashes without operational session values. It omits
credentials, setup sources, browser profiles, active native state, ephemeral
capsules, and unrelated files. Export refuses an existing/symlink destination,
host-specific workspace paths, sensitive text, missing evidence bytes, semantic
drift, or any hash drift. Verification rejects every extra, missing, or changed
byte; it never scans a Downloads directory or substitutes a newer file.
With task tracking configured, `researchChain.task` binds the task history and
check context. Verification checks the original/current requirements,
authorization/check references, current dependencies and bound review as well
as bytes. It remains a portable integrity/relationship check, not independent
proof that a human approved the scope or that a computation actually ran.
