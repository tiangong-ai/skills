# Top-journal Research Policy and final publication gate

Load this reference for a top-journal paper, target-journal readiness claim,
final manuscript review, or publication closure. The goal is a truthful,
reviewable submission candidate; no policy or reviewer can guarantee acceptance.

## Initialize Policy before project admission

Run the guided Policy Wizard only after project-scoped setup is `READY` and the
installed orchestrator matches the immutable setup plan:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy wizard PROJECT --workspace /absolute/path/to/workspace
```

The Wizard loads only that verified project installation. It selects one
article type, field, and journal class, copies conservative Markdown defaults,
completes the publication brief, and optionally creates an exact-journal
policy. It always warns that defaults are generic. Approval requires a separate
acknowledgement and binds the exact current content hash.

Use deterministic commands for automation or audit:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy catalog --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy status PROJECT --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy approve PROJECT --confirm --acknowledge-defaults \
  --workspace /absolute/path/to/workspace --json
```

Do not hand-copy a policy pack or use an ambient/global Skill as an implicit
source. `--source-root` is an explicit automation escape hatch and remains
subject to non-symlink validation; the Wizard never uses it.

## Human-maintained Markdown

The editable project stack is `research-policy/PROJECT/`. A human may update:

- `publication-brief.md`: central question, claim, outcome, contribution, and
  evidence, method, novelty, deliverable, stop, and handoff conditions;
- `article-type.md`, `field.md`, and `journal-class.md`: stricter applicable
  requirements for this study;
- `journal.md`, when present: current official scope, editorial threshold,
  evidence, methods, reproducibility, desk-reject, reviewer, and pivot rules;
- reviewer rubrics for evidence, methods/reproducibility, domain/novelty, and
  the journal editor.

An exact-journal policy cannot be approved by changing only its header. Name
the journal, cite a current official HTTPS guideline URL and retrieval date,
and replace every substantive generic section. Never infer journal rules from
memory or weaken the baseline with a less strict local rule.

Status is mechanical: `missing`, `default-unapproved`, `custom-draft`,
`default-approved`, `custom-approved`, `conflict`, `stale`, `changed`, or
`invalid`. Any content edit after approval yields `changed`; expiry yields
`stale`. Both stop project stages and publication review until re-approved.

Policy approval is necessary but not sufficient for project admission. The
current native host must also create a project-specific scientific design, and
fresh independent reviews must pass at research design, real-record evidence
construction, and outcome-blind methods pilot boundaries. Read
[scientific-design.md](scientific-design.md). These early gates are reverified
before final manuscript freeze; a manually changed status field or missing
review object cannot inherit publication approval.

## Verdict ceilings

Defaults support feasibility and planning but are not journal endorsement. The
CLI computes the maximum permitted language:

- generic stack: `top-journal-candidate`;
- human-customized article, field, journal class, and project brief:
  `top-journal-class-ready`;
- the above plus a complete exact-journal policy:
  `target-journal-submission-ready`.

Mechanical evidence or review failures can only lower the result. An editor
cannot raise a ceiling or override an unobserved central outcome, incomplete
recall, missing direct evidence, or unreproduced result.

## Author in the current native host

After the base research chain is complete, write the final manuscript,
assessment, and submission files in the current Codex app/session or
interactive Claude Code session. The CLI remains a control plane and must not
launch a nested producer. The Markdown/plain-text manuscript must contain
Abstract, Introduction, Methods (or Materials and Methods), Results,
Discussion, Data availability, Code availability, and References/Bibliography.

Before authoring the final materials, inspect the closed analysis identity and
the installed CLI's authoritative schema through the locked resolver:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication lineage PROJECT --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show publication-result-lineage --json
```

The lineage view verifies existing closed/reviewed core bindings and deliberately
returns an empty `files` template. Preserve that source identity while producing
the manuscript, assessment, figures, tables and source data. Record each actual
file's raw-byte hash and its source `analysisSha256`; hash binary figures as
bytes, not decoded text. Do not populate every parent with the
latest hash after the fact. A B artifact does not become an A result by relabeling
it. If the locked CLI lacks this view/schema, its older freeze does not establish
this binding; follow the reviewed upgrade procedure when this assurance is needed.

Create an owner-reviewed submission manifest outside `.tiangong-research`. It
uses `schemaVersion: 1` and distinct absolute canonical paths. Required roles
are `cover-letter`, `title-page`, `reporting-checklist`, `data-availability`,
`code-availability`, and `source-data`; optional roles are
`figure-table-index`, `extended-data`, and `supplementary-methods`. The following
is only the manifest's `files` array, not a complete manifest:

```json
[
    { "role": "cover-letter", "path": "/absolute/path/cover-letter.md" },
    { "role": "title-page", "path": "/absolute/path/title-page.md" },
    {
      "role": "reporting-checklist",
      "path": "/absolute/path/reporting-checklist.md"
    },
    {
      "role": "data-availability",
      "path": "/absolute/path/data-availability.md"
    },
    {
      "role": "code-availability",
      "path": "/absolute/path/code-availability.md"
    },
    { "role": "source-data", "path": "/absolute/path/source-data.csv" }
]
```

The root manifest must also contain the completed nested `resultLineage` object,
not the entire inspection response or an extra `analysisGenerationId` field,
conforming to the CLI schema:
its captured base identity and a prepared entry for `manuscript`, `assessment`,
every submission role and each `supplement-N` in one-based supplied order. Use a
deliberate unique supplement list so deduplication cannot obscure that order. Include
actual material figures/tables and their source inputs as submission files or
supplements; an index is an inventory, not those artifacts. Hash verification
establishes the declared bindings and bytes, not scientific derivation or complete
material coverage; the producer must supply that provenance for independent review.
Unchanged administrative files and demonstrably unaffected materials may be
reused with their provenance and applicability checked against the current
analysis; do not claim they were recomputed or rerendered.
Supply those extra files with `--supplements <absolute-json-file>` at freeze;
that file contains the ordered array of absolute material paths used for the
`supplement-N` entries. Do not omit the actual files after recording their hashes.

If freeze/status reports stale analysis, report or review-packet bindings, repair
the affected base research through supported CLI operations while retaining
applicable evidence. Do not edit control-store files, swap only a manifest hash,
or repeat publication reviews against the same stale base. A changed material
requires its actual source lineage and a coherent refreeze. Historical generations
without lineage remain limited evidence. Qualitative work keeps its honest
non-computational metadata; do not invent an execution to fill these fields.

Inspect the assessment schema, then freeze:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show publication-assessment --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication freeze PROJECT \
  --manuscript /absolute/path/to/final-manuscript.md \
  --assessment /absolute/path/to/publication-assessment.json \
  --submission /absolute/path/to/submission-package.json \
  --producer-agent codex --producer-session OPAQUE_NATIVE_SESSION \
  --workspace /absolute/path/to/workspace --json
```

Freeze requires the configured native producer family and content-addresses the
manuscript, assessment, supplements, submission files, approved Policy, final
acquisition/content/inference snapshots, mode-bound analysis, Claim-Evidence
Graph, reproducibility manifest, and base outputs. It validates every finding's
exact graph topology to atoms, sources, design claims, and the analysis run; a
graph with the right edge names but disconnected endpoints fails even if its
own hash was recomputed. It evaluates central claims, outcomes, result classes,
evidence composition, owner-input trust, recall, novelty, reproduction, and
pivots. File existence alone is never success.

## Figures and project-specific maps

Visual-quality claims require actual image delivery and inspection of the exact
rendered figure or page. File existence, render success, paths, dimensions, SVG
text and a verbal description are not proof that a reviewer saw the image.
Use the host's supported visual tools and the CLI's existing artifact/read
records where available; report what was delivered and inspected without
inventing receipt fields. Producer visual checks do not replace independent
review. The clean configured reviewer must actually support the delivered image
and retain the required other-family/session boundary. If delivery or capability
is missing, keep visual quality unverified, state the text-only review scope,
and obtain a supported capable route before claiming visual acceptance.

For a map, use the project's approved brief/Policy and source requirements for
boundary dataset/version, study extent, labels, projection and required insets.
Keep source/license/version and relevant presentation choices traceable in the
figure caption or existing reproducibility materials; check the actual rendered
map against them. Do not import a national/geopolitical default from another
project or add an inset merely to satisfy a remembered template. When a missing
choice materially affects interpretation, request that focused project decision.
Unavailable map bytes or metadata remain a gap, not a guessed verification.

## Four fresh independent final reviews

Prepare and submit exactly one review for each role: `evidence`,
`methods-reproducibility`, `domain-novelty`, and `journal-editor`.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication review prepare PROJECT --role evidence \
  --reviewer-agent claude --reviewer-session FRESH_OPAQUE_SESSION \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show publication-review-evidence --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication review submit PROJECT --role evidence \
  --review /absolute/path/to/review.json \
  --workspace /absolute/path/to/workspace --json
```

Repeat with fresh sessions. A reviewer uses the configured headless Codex or
Claude CLI family, which must differ from the native producer family. Merely
changing the session while keeping the producer family is not independent. It
must also differ from every prior project reviewer session. The append-only
journal stores only the session SHA-256 for reuse detection; deleting a mutable
cache does not permit reuse.

Each packet binds the exact Policy, evidence/content/inference snapshots,
Claim-Evidence Graph, reproducibility manifest, base closure, manuscript,
assessment, submission files, supplements, mechanical result, role, reviewer,
and schema. Review cannot add evidence. A manuscript or submission-file
revision creates a new generation and invalidates every old review; a Policy
change or expiry blocks publication.
When task tracking is configured, the frozen generation and each existing
reviewer packet also bind the original request and original/current requirement
check matrix. Read that context; a native check record is not a certified run.
Use the existing role's findings for missing task coverage, not an extra fixed
review round. See [execution-assurance.md](execution-assurance.md).

## Close and report bounded language

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication status PROJECT --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication close PROJECT --workspace /absolute/path/to/workspace --json
```

Closure re-verifies every hash and requires all four reviews. Report only the
returned verdict, bounded statement, limitations, and pivots.
Report original/current task completion alongside that publication verdict.
A narrower approved scope does not erase unanswered original requirements.
`target-journal-submission-ready` means the frozen artifact passed its declared
gates; it does not predict or guarantee editorial acceptance.

`publication status` exposes `submissionPackageSha256`, `submissionRoles`,
`contentSnapshotSha256`, `inferenceSnapshotSha256`, and
`claimEvidenceGraphSha256`; inspect these before launching a paid review. After
closure, export and verify the portable audit directory from
[scientific-design.md](scientific-design.md). Handing off only the manuscript,
receipt hashes, or a workspace-specific path is not sufficient for independent
editorial or reproducibility audit.
