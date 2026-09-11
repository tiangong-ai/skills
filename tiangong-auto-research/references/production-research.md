# Production research workflow

Load this reference for production initialization, a resumed/blocked project,
broker configuration, or recovery. The CLI remains authoritative when this
reference and runtime output differ.

Resolve every command through the existing workspace lock:

```bash
AUTO_RESEARCH_CLI=/absolute/path/to/installed/tiangong-auto-research/scripts/research_cli.mjs
```

## Production admission checklist

Start from the user-selected workspace's current immutable setup generation.
Run `research setup status`, then one explicitly cost-confirmed
`research setup doctor --live --agent-smoke --confirm-agent-smoke-cost` before
project preflight. That command probes each required capability once and starts
only the independent reviewer CLI smoke, after every blocking zero/low-cost
prerequisite passes. It never starts the current native producer as a child.
Do not repeat paid smokes without a reason. Continue when
`researchReadiness=READY`; optional domain degradation blocks only a project or
operation that explicitly requires the exact component.

Before `project init`, record and show the user:

1. The research question and evidence dimensions.
2. Required source types, exact capability IDs and discovery scopes that must
   be exercised, minimum sources, minimum full-text sources, and any
   date/applicability boundaries.
3. Available immutable inputs and locked capabilities that can satisfy each
   requirement, including the selected external internet profile and every
   owner-whitelisted database marked required for discovery.
4. Gaps that would block the post-discovery coverage gate.
5. Package token reservations, configured price basis, maximum cost, and
   whether the confirmation threshold is crossed.

For a top-journal goal, initialize and explicitly approve the applicable
Research Policy first. Use `research policy wizard PROJECT`; it reads the
verified project-installed orchestrator, warns when generic defaults remain,
and never treats them as exact-journal endorsement. See
[publication-policy.md](publication-policy.md) for policy states, verdict
ceilings, final manuscript freeze, and four-role review.

Then have the current native host create the closed scientific-design contract
and pass that same exact file to preflight and project init. Preflight evaluates
observable estimands, claim/edge feasibility, endpoint truth roles, quantity and
denominator scope, independent units, threshold types, evidence-role/full-text
requirements, known gaps, context fit, and baseline fairness. It also reserves
three early scientific reviews, four final publication reviews, and one bounded
revision cycle. See [scientific-design.md](scientific-design.md).

The scientific gate order is design review, discovery, acquisition and evidence
freeze, evidence-construct review, outcome-blind methods pilot, then inference.
This prevents discovery metadata from being misreported as acquired full text.
The evidence-construct packet binds owner-supplied canary JSON artifacts and
revalidates every referenced source ID, full-text state, and publication date
against the frozen snapshot.

Use `research project preflight`; do not calculate a competing checklist in the
skill. Production `project init` requires its evidence-requirements file and,
when the configured threshold is crossed, explicit `--confirm-budget`.

## Reproducible route configuration

Production mode requires different agent families, explicit model IDs, and
explicit pricing for producer and reviewer. `binary` must be the exact agent
name or an absolute executable path. Prefer the exact `codex` / `claude`
routes. If an explicit wrapper is required, set its absolute path in `binary`
and the underlying absolute agent path in `wrapperTargetBinary`; never use a
wrapper with an unpinned PATH lookup. The runtime separately records the target
binary, route launcher/wrapper, and internal adapter SHA-256 values, plus the
reported version, actual/configured model, OS, and architecture.
The producer route must declare `executionMode=native-host`; the reviewer route
must declare `executionMode=headless-cli`. `workspace doctor --agent-smoke`
executes only the reviewer in a real capsule and writes a 24-hour attestation
bound to its runtime fingerprints, workspace config, capability lock, and
doctor schema. Expiry or drift blocks review before invocation. During that
validity window, plain `workspace doctor` rechecks all bound hashes and the
resolved reviewer runtime before reuse. Use explicit smoke flags to refresh the
check; missing, expired, or drifted attestations never receive an implicit pass.

Within a reviewer package, a formatting repair may reuse the capsule-local agent
auth copy only when it remains a regular owner-only file whose SHA-256 matches
the owner source. Authentication drift stops the package; setup/runtime never
overwrite the capsule copy or silently choose another credential.

The budget includes:

- total tokens, cost, and wall time;
- a token reservation for every agent stage;
- maximum structured-output and repair tokens;
- output file count/bytes and attempts;
- broker calls, response bytes, context items, and estimated context tokens;
- the cost-confirmation threshold.

New production workspaces use finite runaway ceilings of 50,000,000 total
tokens and USD 5,000. Package ceilings are 12,000,000 for discovery, 2,000,000
for acquisition, 1,500,000 each for analysis and synthesis, and 2,500,000 for
review. Primary output is bounded at 32,000 tokens and repair at 16,000. The
legacy input-context setting is only an embedding/planning hint; compatible
runtimes have no total context-length or artifact-read admission ceiling.
These values are not a target spend:
coverage-driven working plans, early stop, three attempts per package, and
explicit confirmation above USD 10 control ordinary execution. Smoke-test
workspaces retain smaller low-cost defaults. Lower production ceilings only
when preflight proves every complete reservation still fits.

The lifecycle reserves up to 500,000 tokens for each early scientific review,
750,000 for each final publication review, and 4,000,000 for one revision,
with corresponding finite wall-time reserves. These are hard runaway ceilings,
not expected consumption. Do not start a call when its reservation plus all
remaining mandatory gates cannot fit the confirmed total.

The CLI will not prepare or launch a package unless its full token and
conservative price reservation fits. Native producer preparation reserves the
prompt, schema, admitted input/prior-stage context, bounded broker allowance,
and output ceiling. Because the current host app does not expose trusted
per-stage usage telemetry to this control plane, successful submit charges the
entire reviewed package reservation and records
`accountingMode=reserved-native-host`. The submit boundary still rejects an
oversized, invalid, stale, or over-budget result before promotion. Do not
describe native-host turns or output tokens as provider-side hard limits.

The independently launched reviewer retains pre-call prompt/schema/output and
repair reservations plus provider-side structured-output/turn controls where
the selected CLI supports them. Its formatting repair requests plain JSON in
one tool-free turn and passes through the same validators. Preflight and runtime
share the review-context reservation formula. The broker separately derives a
coverage-scaled working broker-view budget from dimensions, source types,
required channels, minimum sources, and a gap-fill reserve. It is not a fixed
six-call allowance and never exceeds the new-production hard ceiling of 256
bounded views, each with at most 32,000 context tokens. The packet returns live
progress and remaining working budget after each success; stop early once
coverage is supportable. Network/cache status is reported
separately; a cached view avoids a provider call but still consumes one view and
its context reservation. Candidate assessments are appended in batches of at
most 25, so the final discovery output remains a compact closeout instead of
growing with source count. Reserve enough output for every producer schema and
enough input context for prior-stage artifacts; preflight reports both gaps
mechanically.

## Bounded local evidence

Use the same absolute `--input-plan` for preflight and project initialization.
Each entry registers the exact full source plus evidence dimensions, source
type, full-text claim, publication date, and either:

- `contextPath`: a separate regular, non-symlink excerpt whose own SHA-256 is
  admitted; or
- `contextRanges`: sorted, non-overlapping, one-based inclusive line ranges
  from a UTF-8 source.

```json
{
  "schemaVersion": 1,
  "inputs": [
    {
      "path": "/absolute/path/to/source.txt",
      "contextRanges": [{ "startLine": 20, "endLine": 80 }],
      "role": "primary",
      "dimensions": ["energy", "water"],
      "sourceType": "peer-reviewed",
      "fullText": true,
      "publicationDate": "2024-01-15"
    }
  ]
}
```

The native producer packet contains only the derived bounded context. The full,
hash-verified source is withheld until independent review and is then listed in
the review packet. The CLI rejects symlinks, duplicate source content,
overlapping or out-of-range slices and changed hashes. Large aggregate context
uses its complete packet-bound read route, not a refusal at
`maxInputContextTokens`. Declaring `fullText: true` means the exact full file is
registered for review; it does not expose that entire file to discovery.

## Authoritative output contracts

Inspect, but never copy or fork, a current contract with:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show discover --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show analyze --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show review --json
```

Evidence sources include stable ID/title/locator/provenance, URL or DOI when
safe and available, source type, publication/retrieval dates, full-text status,
an excerpt or JSON Pointer, quality rationale, applicability, and covered
dimensions. The CLI verifies input locators and every broker object. Findings
may cite only admitted source IDs.

For discover, acquire, analyze, and synthesize,
`research project stage prepare` returns the authoritative schema and exact
prompt to the current native host. Save one JSON object and pass it to
`research project stage submit`; submit validates and materializes it
atomically. A rejected native submission preserves the bound session for an
explicit host correction and never launches a nested repair agent. For review,
Codex receives `--output-schema` or Claude receives
`--json-schema`; invalid reviewer syntax/schema or a mechanically diagnosed
binding gets one low-budget formatting-only repair with no broker or research
tools. The repair may not add facts. A second reviewer failure stops instead of
repeating research.

## Broker evidence

A `brokered-network` capability has exact hosts plus an HTTP policy:

```json
{
  "id": "method.public-source",
  "skillPath": "/absolute/path/to/public-source-skill",
  "permissions": ["project-read", "candidate-write", "brokered-network"],
  "allowedHosts": ["api.example.org"],
  "http": {
    "endpoint": "https://api.example.org/v1/search",
    "method": "GET",
    "accept": "application/json",
    "allowedContentTypes": ["application/json"],
    "staticHeaders": {},
    "maxRequestBytes": 0,
    "maxResponseBytes": 524288,
    "maxItems": 100
  },
  "coverage": {
    "dimensions": ["research-question"],
    "sourceTypes": ["primary"],
    "fullText": true,
    "publicationDates": true
  },
  "credentials": []
}
```

Each successful body is written to the immutable content-addressed evidence
store before its capsule is deleted. A receipt binds the response hash/size,
content type, hashed final URL, raw locator, bounded context locator, retrieval
time, and cache status. Review packets enumerate and hash these permanent
objects.

The review capsule deterministically distributes one global context budget
across registered local bounded contexts and every broker receipt. The
resulting excerpt bundle and the complete review packet are themselves stored
by content hash under the project's `review/contexts/` and `review/packets/`
directories. The packet enumerates raw broker objects, original per-receipt
bounded contexts, and full local-file hashes; its hash is schema-bound without
duplicating packet metadata in the model prompt. The reviewer initially receives
the excerpt bundle and artifact directory and can inspect needed complete objects
through the packet-only read channel. Mechanical closure checks that
the packet, bundle, broker objects, and local input/context hashes still exist
and match before it records their safe locators.

Use `json_pointer` and `max_items` for a bounded JSON view. The workspace's
`maxBrokerContextTokens` limit additionally caps the staged view using a
conservative `ceil(contextBytes / 3)` estimate while retaining the exact raw
object. For a collection, continue within that object by sending the returned
`contextNextOffset` as `item_offset`; each view has its own receipt and context
object, while the raw response is reused without another upstream fetch.
Follow upstream pagination only through its next admitted HTTPS URL so each
upstream response receives its own permanent receipt. Public requests default
to the persistent cache; pass `cache_mode=bypass` for a fresh request.
Credentialed requests always require bypass to avoid replaying scoped content
under changed authorization.

Capability health checks separately retain only a bounded sanitized provider
code/detail and safe request ID. In particular, `OPTION_NOT_IN_PLAN` requires an
explicit baseline replacement or provider subscription change; it is never an
automatic retry or fallback signal.

The locked manifest exposes the exact non-secret endpoint so discovery never
guesses a path. The broker sends the capability's exact `Accept`, checks the
initial target and every GET redirect against both the host and endpoint scope,
screens credential-like response material, and rejects undeclared content
types or oversized bodies. A `/` endpoint explicitly grants origin-wide paths;
other endpoint paths are exact. Non-2xx results include only status, a
sanitized short response, safe request ID, and parsed `Retry-After`.
The broker performs at most one inline retry for a `429` whose declared or
default delay is at most five seconds. A longer throttle remains a classified
failure and does not hold the discovery call open.

Capabilities may instead declare bounded JSON `POST`, an exact safe static
header map, and a positive `maxRequestBytes`. Discovery then supplies only the
documented non-secret `request_body`; credential-like keys are rejected, the
credential is injected by the broker, POST redirects are refused, and the
journal/cache metadata records only the canonical body SHA-256 rather than the
body. GET remains the default method inside an explicit HTTP endpoint policy.

## Coverage, retry, and recovery

Discovery output is promoted for diagnosis, then acquisition audits every
provisionally admitted source. The CLI freezes accepted/limited sources, their
explicitly selected artifacts, and honest unresolved gaps in one immutable
snapshot; the separate inference gate records whether the chain may continue.
It derives producer-readable full-text availability, source types, source
counts, dated counts, date range, per-dimension source IDs, and the
pass/insufficient decision mechanically. `partial` is usable but incomplete;
`missing`, an acquisition gap, or any unmet source/full-text/date minimum stops
inference after all acquired material has been decomposed and typed. Model-
provided qualitative gaps remain visible but cannot override those derived
fields. See
[evidence-pipeline.md](evidence-pipeline.md) for exact full-file versus
producer-readable semantics and addendum lineage.

Every manifest capability marked `requiredForDiscovery=true` must produce a
verified broker receipt. The gate distinguishes a capability that was never
called from one that was called but yielded no admissible receipt; the latter
reports only sanitized failure kinds such as `rate-limit`, never request URLs or
credentials.

Native discovery receives a packet containing the locked capability manifest
and each staged external Skill's top-level `SKILL.md`. The current host must use
the packet's `research project evidence fetch` argv for admitted network
evidence; standalone web/search/database tools cannot replace required broker
receipts. Broker results include the exact bounded view inline with the receipt.
Acquire receives the provisional evidence record and exact artifact registry
command, then produces a complete audit. Between acquire and analyze, the
native host records exact decomposition lineage and evidence atoms and freezes
the typed-content snapshot. Analyze receives only a passing immutable inference
snapshot and emits schema v2 with a reproduced analysis run; submit generates
the Claim-Evidence Graph. Synthesize receives that chain and analysis. Neither
later stage may gather new evidence. Review uses only the two packet-bound
artifact read tools, with initial context and exact immutable file references.
Preflight/runtime use rough initial-context and expected-read estimates, not the
whole corpus or an unbounded legacy context hint. There is no artificial aggregate
length refusal; real provider capacity and finite runaway time/token/cost budgets
remain. Claude supports the configured provider turn guard; Codex has no equivalent
CLI turn flag. Only formatting repair is tool-free. The complete packet, full
files, original contexts and raw objects stay hash-bound. Read receipts show which
exact bytes were delivered; never claim comprehension or inspection of unread
files. Run records, journal usage, and JSONL progress include sanitized
event/item counts, provider turns, tool calls, reasoning-token counts, and
bounded provider errors.

Failures are classified:

- configuration/authentication/deterministic 4xx, coverage, and review failures
  stop immediately;
- structured-output failures use only the repair path; synthesis mechanically
  converts literal `/n` or double-escaped `\\n` immediately before Markdown
  block structures into line feeds, records a content-free normalization event,
  and leaves URLs or unmatched text unchanged before independent review;
- a short 429 receives at most one broker-level bounded-delay retry; a remaining
  429 may schedule package recovery after its recorded delay;
- bounded transient/5xx failures may retry while attempts and budget remain.

Use an explicit management command instead of editing state:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project retry PROJECT \
  --package PACKAGE --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project fork SOURCE \
  --to TARGET --resume-through analyze \
  --workspace /absolute/path/to/workspace --json
```

Retry grants one auditable extra attempt and resets downstream scheduling while
preserving promoted files. Fork starts a new budget/accounting history and may
inherit only verified completed discovery/acquisition/analysis/synthesis
artifacts; review and closure always run again. A closed-project evidence
refresh uses `research project addendum`, not a fork or in-place mutation.

For a top-journal source, initialize and approve Policy for `TARGET`, create a
new target-specific design, and add `--design`, `--design-producer-agent`, and
`--design-producer-session` to fork/addendum. Old scientific reviews never carry
forward. The new generation re-enters the applicable early gates, even when it
inherits completed outputs; an inherited later package cannot bypass them.

The fork becomes authoritative immediately and its source becomes stale. The
default status/run path excludes superseded, archived, and abandoned projects;
`research status --all` is the audit view. Archive complete/stale history and
abandon unfinished history with an explicit reason.

When a portable handoff or audit is needed, use `research project audit export` followed by
`research project audit verify`. The portable bundle includes exact formal
evidence/artifact bytes and project review objects while excluding credentials,
host-specific paths, active native state, capsules, and unrelated projects. Reuse
existing exact bundles or artifact links when suitable; normal continuation or a
cosmetic revision does not require another complete delivery copy.

When the next material step cannot be performed autonomously, use one of two
durable states rather than another retry:

- `user-action-required`: authorized login, SSO/MFA, CAPTCHA, security warning,
  paywall/entitlement decision, VPN, or another explicit user operation;
- `external-response-required`: a government, institution, owner database, or
  other third party must provide non-public material or permission.

The handoff record states the non-secret reason, exact requested actions, and
remaining evidence gaps. `research run` returns `handoff-required` and does not
schedule more work. Resolve it explicitly only after the action or response has
been registered; never treat continued substitute searching as resolution.

For a material evidence ceiling, run the evidence access status command,
`research project access status PROJECT --workspace /absolute/path --json`
before requesting the handoff. An `evidence-exhausted` record must cite every
terminal event hash for all required plan-bound agent routes and must bind each
remaining purchase, subscription, authorization, or external request to a
required evidence role and exact resume criteria. Follow
[evidence-exhaustion.md](evidence-exhaustion.md). An interactive challenge is a
separate immediate pause and never asserts route exhaustion.

Run one recovered or canary project with an explicit scope:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run \
  --project PROJECT --workspace /absolute/path/to/workspace \
  --progress-jsonl --json
```

The top-level result and run-level JSONL events then carry that `projectId`, and
historical blocked siblings do not change its exit code. Omit `--project` only
when the operator intends to schedule and summarize the whole workspace.
`stopReason=native-stage-required` is the normal producer handoff: follow
[native-execution.md](native-execution.md) to prepare and submit discover,
acquire, analyze, and synthesize in the current host. Calling `research run`
after synthesis may launch only the independently configured reviewer CLI and
then close mechanically.

## Standard zero-cost eval before a real canary

Confirm the owning CLI release passed deterministic mock coverage for:

- a clean empty directory, pinned external installer plan, and explicit
  installed/configured/locked/credential/live statuses;
- missing external Skills, missing credentials, unavailable provider plans,
  symlinked or drifting trees, and actionable structured failures;
- explicit owner-database import, required-discovery receipt enforcement, and
  local-only production rejection;
- routing and smoke/production mode boundaries;
- discover → acquire → decompose/atomize → content freeze → inference freeze →
  analyze → Claim-Evidence Graph → synthesize → review → close;
- top-journal Policy approval, publication assessment, required manuscript
  sections, complete submission roles, content/inference/graph/reproducibility
  binding, four fresh configured other-family role-bound reviews, revision
  invalidation, and publication closure;
- public `research run` never launching a producer subprocess, with native
  prepare/fetch/register/submit advancing the four producer stages;
- native-only leads remaining supplemental until an immutable broker
  occurrence formalizes the same candidate;
- exact artifact registration, binary-only full-text semantics, immutable
  snapshot/delta lineage, and addendum supersession;
- permanent evidence and review-packet hash verification plus semantic
  pre-export validation of the named portable audit research chain;
- persistent bounded review-context verification and packet/context tamper
  rejection before closure;
- malformed JSON repair and a second-failure stop;
- invalid provenance/finding binding repair without repeating research;
- 429, deterministic 4xx/422, 5xx, redirect, oversized response, cache, and
  bounded offset extraction with raw-object reuse;
- reviewer capsule HOME/sandbox startup and evidence/journal recovery;
- bounded native producer context with full-source reviewer staging;
- explicit native broker discovery with embedded locked Skill documentation,
  no new evidence in analyze/synthesize, packet-only review reads, and tool-free repair;
- a broker result larger than the historical 64 KiB capture floor;
- capability drift and evidence tampering;
- insufficient evidence blocking closure;
- secret redaction across output, provider telemetry, errors, progress,
  journal, and manifests;
- project-scoped execution in a workspace containing historical blockers;
- research-core readiness surviving optional acquisition/preprocessing
  degradation, exact companion promotion, one-probe doctor behavior, and paid
  reviewer smoke suppression after static blockers.

Only then run a bounded real-model canary with reservations that pass preflight.
Do not infer production readiness from a Crossref-only or single-source smoke
test.
