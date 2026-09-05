# Native producer execution

Discover, acquire, analyze, and synthesize are performed by the current
interactive Codex, Claude Code, WorkBuddy, or CodeBuddy session. The CLI is not
a producer-agent launcher. It prepares a hash-bound packet, brokers authorized
evidence, registers exact acquired artifacts, admits the result, launches the
other agent family only for independent review, and closes mechanically.

## Identify the next action

For a new project, record its original task before this first execution boundary.
For recovery or scope changes, read
[execution-assurance.md](execution-assurance.md). The returned task context is
part of the native-stage binding; do not rewrite it while a session is active.

Run the control plane after project initialization:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run --workspace /absolute/path/to/workspace \
  --project PROJECT --max-cycles 20 --progress-jsonl --json
```

`stopReason=native-stage-required` means the current host must perform the next
producer stage. It is not an error and must not trigger a nested `codex exec` or
`claude -p` call.

`scientific-review-required`, `scientific-revision-required`, and
`scientific-stopped` are different actions. Inspect the named gate and its
`recommendedAction`; do not loop on `research run` or prepare a producer stage
that a due scientific gate prohibits. A future gate does not prevent an
earlier allowed discover/acquire package.

For a top-journal project, `research status` can instead return a pending
`research-design`, `evidence-construct`, or `pilot-methods` gate. Complete that
gate before preparing a native stage. The current native host writes the
schema-bound assessment; only the configured other agent family performs the
fresh independent review. See [scientific-design.md](scientific-design.md).

The ordering is deliberate:

```text
design review → discover → acquire, register readable derivatives, forecast
→ freeze acquisition → record decompositions/atoms in batches → freeze typed content
→ real-record construct review → outcome-blind methods pilot review
→ freeze inference → analyze → freeze Claim-Evidence Graph → synthesize
```

The post-acquisition order is a correctness boundary: discovery metadata cannot
prove a full-text floor, and acquisition artifacts must be frozen before the
construct canary cites them. Do not replace the real-record canary with a
synthetic schema example, inspect outcome values while proving construction,
or use repeated cells/rows as independent resampling units. A later inherited
package cannot bypass an earlier gate.

Read `mechanicalAssessment.futureGateObligations` before continuing. Pending
source-derived parameter values, executable model bytes, and exact environment
locks are permitted only until their declared gate and only when an exact
planned Policy rule owns them. They are not usable results. Use the compatible
CLI's same-project fulfillment for only those predeclared slots, as described in
[execution-assurance.md](execution-assurance.md). Different assumptions or
already-frozen values still need a reviewed successor. At the due gate the CLI
stops if required objects remain unresolved.

The same early obligation list includes ordinary planned Policy rules, not just
model and uncertainty objects. Plan any genuinely required successor before its
due gate; an assessment or object-filing receipt cannot silently resolve a
scientific Policy rule or rewrite the frozen design.

For `evidence-construct`, write one or more bounded JSON canary artifacts outside
`.tiangong-research`. Put their exact SHA-256 values in the assessment and pass
an owner-reviewed JSON array of their absolute canonical paths with
`--canary-artifacts`. The CLI rejects symlinks, duplicates, oversized or
credential-like content, promotes exact bytes into content-addressed project
storage, and binds them into the review packet. Coverage IDs must exist in the
post-acquisition frozen snapshot; claimed full-text and publication-date states
are re-derived from that snapshot rather than trusted from producer JSON.

## Prepare the exact stage

Use the exact native host selected by the immutable setup plan. Valid host
identities are `codex`, `claude`, `workbuddy`, and `codebuddy`; never label one
host as another merely to pass admission:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage prepare PROJECT \
  --stage discover --host-agent workbuddy \
  --workspace /absolute/path/to/workspace --json
```

The returned packet binds the project, stage, inputs, prior outputs, runtime and
capability locks, schema, model, limits, prompt, and command argument arrays.
Use the packet verbatim. Do not edit `project.json`, the active session, locks,
journal, evidence store, or admitted outputs.

Preparation is idempotent while its exact session remains active. If the wrong
host, stage, model, project state, or hash is observed, stop on the structured
error.

Compatible packets expose `listArtifacts` and `readArtifact` for exact on-demand
reads. Initial embedded text is not the whole evidence corpus. Follow the exact
index and offsets, or request a whole object explicitly; do not drop original
requirements or counterevidence to fit a historical context threshold. A real
model-capacity or permission failure remains a limitation, not a passing review.

## Fetch discovery evidence

For each broker request, write one new bounded JSON object matching
`commands.fetchEvidence.requestSchema`. It contains logical capability and
credential IDs, never credential values. Invoke the returned argv, replacing
only the request-file placeholder:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence fetch PROJECT \
  --request /absolute/path/to/request.json \
  --workspace /absolute/path/to/workspace --json
```

Use only the returned bounded context and exact receipt fields. The broker
enforces the locked endpoint/method/Accept policy, injects owner credentials,
limits calls/bytes/items/tokens, persists content-addressed raw evidence, and
records sanitized events. Standalone web/search/database tools cannot replace
required broker receipts.

For structured data, inspect `providerCoverage`, `limitCoverage`, and
`contextView` separately. If `contextView.nextCursor` is present, use the exact
`runDataCapability.readArgv` from the packet with the returned receipt ID and
opaque cursor. Continue until `nextCursor is null` when a claim depends on
reviewing every returned row. Each continuation reads the same immutable local
evidence and does not consume another provider call. If the task only needs a
summary or an adaptive sample, it may stop earlier, but the stage assessment
must state the presented fraction and remaining unreviewed rows. A projected
context is not a provider gap; a provider gap or operation limit must remain
visible even after every persisted row has been read.

Native Web or Browser work must first use the packet's `recordActivity` argv.
Its search/navigation input is sanitized and persisted only by SHA-256; counts,
challenge class, status, and linked candidate IDs remain auditable. Register
useful results as supplemental leads with `registerCandidate`. They remain
inadmissible until a broker receipt formalizes the same canonical URL/DOI.
Registered inputs are already formal candidates under their own content-hash
identity.

After each useful discovery batch, write at most 25 candidate judgments through
the packet's `recordAssessment` argv. The ledger keeps the latest judgment per
candidate and the CLI joins all deterministic provenance fields. The final
discover output must contain only the compact closeout schema returned by the
packet; do not repeat a source-sized evidence array.

## Register exact acquisition artifacts

When the next stage is `acquire`, use the packet's `bindDownload` argv before
registering every file obtained from a network source. Bind the exact completed
Download object or equivalent to one unique planned staging path, safe final
URL, suggested filename, and available non-secret identifier. A cancelled or
failed download must be recorded as such and cannot be promoted. Then follow
`registerArtifact`, passing the candidate, exact absolute path, and returned
download binding. The registry accepts no directory and performs no “latest
download” selection.

For a producer-readable derivative, register the exact output with
`--derived-from-artifact` naming its same-candidate parent instead of claiming a
second network download. Add source/license metadata only when explicitly
declared by the source.

Binary registration makes the exact file review-bound, but PDF/DOCX/PPTX/XLSX
alone does not claim producer-readable full text. Register a separately derived
UTF-8 text/JSON/CSV/Markdown artifact when one was legitimately produced
and should be embedded within the bounded producer context.

Acquisition submit freezes the complete result even when it contains honest
blocking gaps. Before preparing `evidence-construct` or `analyze`, follow
[evidence-pipeline.md](evidence-pipeline.md): record a complete/limited/failed
decomposition for every acquired full-text or data artifact, register exact
line-range or JSON-Pointer evidence atoms, and run `research project evidence
content freeze`. Do not leave acquired PDFs, spreadsheets, archives, or
structured files as unexamined binary attachments. `research status` must show
the typed content as verified; a stopped content/acquisition gate requires a
scope/access handoff rather than analysis.

Use the artifact byte preflight and acquisition forecast described in
[evidence-pipeline.md](evidence-pipeline.md) before final submission. Register
readable derivatives during the active acquire stage; only decomposition/atom
records are added after that snapshot freezes. Prefer batch registration when
there are many records, without repeating full verification for each item.
For a missing derivative after acquire completed, prefer the compatible CLI's
same-project revision before analysis. A new source requires explicit discovery
reopening and formal admission, not a direct edit to acquisition JSON. Preserve
unchanged files/receipts and inspect the resulting current snapshot.

## Submit producer output

Save only the schema-conforming JSON object to a new regular non-symlink file.
Then use the packet's exact session and expected model:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage submit PROJECT \
  --session SESSION_ID \
  --output /absolute/path/to/stage-output.json \
  --confirm-model EXPECTED_MODEL \
  --workspace /absolute/path/to/workspace --json
```

File existence is not success. Submit rechecks the session and all bindings,
parses the authoritative schema, validates provenance and coverage, enforces
byte/token/wall/cost reservations, computes hashes, writes a run record, and
atomically promotes the output. Native-host usage is conservatively charged at
the reviewed package reservation because the host app does not expose trusted
per-stage usage telemetry to the CLI.

A rejected submission leaves the active session intact so the current host can
perform a bounded formatting correction or gather missing authorized evidence.
It does not silently retry research or invoke another model.

Login, MFA, CAPTCHA, Turnstile, paywall, security-warning, or authorization
activity cannot be submitted as ordinary completion. Record it as `blocked`
through `recordActivity`, create an `interactive-challenge`
`user-action-required` record with the packet's `requestHandoff` argv, and stop.
This immediate safety pause is not an evidence-exhausted claim, does not create
a terminal acquisition-route event, and must be resolved by the user before
that route can continue. When all
plan-bound lawful routes have terminal evidence but a required role still needs
licensed or owner material, follow
[evidence-exhaustion.md](evidence-exhaustion.md) and request an
`evidence-exhausted` handoff. Use `external-response-required` when an
institution or another third party must respond. Both states are durable and do
not burn the prepared attempt. Resume only after an operator explicitly runs
`research project handoff resolve` with a non-secret resolution note.

## Continue, review, or abort

After each successful submit, call `research run` again. Prepare/submit the
next native producer stage through discover, acquire, analyze, and synthesize,
performing the typed-content steps between acquire and analyze. Analyze prepare
freezes the exact inference snapshot; analyze submit mechanically freezes the
Claim-Evidence Graph. The same run command may then launch only the configured
independent reviewer CLI and, after a passing review, perform mechanical
closure.

Before the run that may launch review, record an honest check/disposition for
every current task requirement at an idle boundary. Computational work remains
in this native host. Use the compatible CLI observer for one explicitly declared
ordinary calculation, then bind its exact `nativeRunSha256` at acceptance. It
records process execution and exact inputs/outputs without inventing scientific
validity or fully attesting the declared environment. Unobserved computational
claims stay unverified, and failed runs cannot become positive checks. Follow
[execution-assurance.md](execution-assurance.md); do not add a separate paid
reviewer round or treat a missing check as a reason to repeat the research.

For a top-journal project, this is the base research closure, not the final
publication verdict. Continue in the same current native host to author and
freeze the manuscript, then use four fresh independent publication-review
sessions. Follow [publication-policy.md](publication-policy.md). Do not ask
`research run` to launch a producer for manuscript authoring.

To discard an active native session explicitly:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage abort PROJECT --session SESSION_ID \
  --workspace /absolute/path/to/workspace --json
```

Abort removes only that CLI-created runtime capsule and active session. It
consumes the prepared attempt and never deletes admitted evidence or outputs.

Use `research status` to follow the authoritative project. A recovery fork
supersedes its source and is the only default-runnable descendant. Use
`research status --all` for lineage audit, `research project archive` for
complete/stale history, and `research project abandon` for unfinished history.

Before an external handoff or archival milestone, export and verify the project
audit bundle described in [scientific-design.md](scientific-design.md). A local
manifest or receipt hash without the referenced evidence bytes is not a
portable audit package.
Read the task completion fields as well as workflow status. `status=complete`
for the workflow can coexist with inconclusive or withdrawn task requirements.
