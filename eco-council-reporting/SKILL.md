---
name: eco-council-reporting
description: Build stage-aware eco-council packets and deterministic drafts for data-readiness, moderator matching authorization, post-match expert reports, and moderator decisions. Use when eco-council-normalize has already produced canonical submissions, evidence-library views, matching artifacts, and round context JSON, and OpenClaw needs valid contract-shaped draft outputs without bypassing the audited readiness, authorization, matching, report, and decision flow.
---

# Eco Council Reporting

## Core Goal

- Keep normalization and deliberation separate.
- Turn canonical round artifacts into:
  - `data_readiness_packet.json` and `*_data_readiness_draft.json`
  - `matching_authorization_packet.json` and `matching_authorization_draft.json`
  - post-match `report_packet.json` and `*_report_draft.json`
  - `decision_packet.json` and `council_decision_draft.json`
- Produce valid draft objects that OpenClaw can revise or promote without skipping stages.

## Required Upstream State

- Start from one scaffolded eco-council run directory created by `$eco-council-data-contract`.
- Run `$eco-council-normalize` first so canonical submissions, active evidence-library views, and `context_*.json` already exist.
- Build readiness packets before moderator matching authorization.
- Build report packets only after matching/adjudication artifacts exist.
- Keep the final canonical files such as `data_readiness_report.json`, `matching_authorization.json`, `*_report.json`, and `council_decision.json` unchanged until review is complete.

## Workflow

1. Build expert data-readiness packets and deterministic drafts.

```bash
python3 scripts/eco_council_reporting.py build-data-readiness-packets \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

2. Build the moderator matching-authorization packet after both readiness reports exist.

```bash
python3 scripts/eco_council_reporting.py build-matching-authorization-packet \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

3. Build expert report packets and deterministic report drafts only after matching/adjudication has run.

```bash
python3 scripts/eco_council_reporting.py build-report-packets \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

4. Build the moderator decision packet and next-round task drafts.
- This step may happen after post-match reports or directly from readiness plus matching authorization when matching was deferred or denied.
- Sociologist and environmentalist next-round tasks keep `source-selection` in `expected_output_kinds` so audited source choice remains explicit before any future fetch stage.

```bash
python3 scripts/eco_council_reporting.py build-decision-packet \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --next-round-id round-002 \
  --prefer-draft-reports \
  --pretty
```

5. Or let the script build every stage-eligible packet in one call.

```bash
python3 scripts/eco_council_reporting.py build-all \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --prefer-draft-reports \
  --pretty
```

6. Render OpenClaw text prompts from the generated packets.

```bash
python3 scripts/eco_council_reporting.py render-openclaw-prompts \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001
```

7. After expert or moderator review, promote the approved drafts into canonical contract paths.

```bash
python3 scripts/eco_council_reporting.py promote-all \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

## What This Skill Writes

- Derived packets only. This skill does not overwrite canonical readiness, authorization, report, or decision files until a promote command is used.
- Each packet embeds:
  - relevant round context
  - contract-facing writing rules
  - validation hints
  - one valid draft object for direct editing or promotion
- Promotion commands validate the selected draft and then write canonical outputs only when safe to do so.

## Scope Decisions

- Do not fetch remote data here.
- Do not mutate raw, normalized, or shared canonical inputs.
- Do not silently invent IDs, coordinates, or time windows.
- Do not let post-match report generation run before matching/adjudication exists.
- Use deterministic heuristics for:
  - report finding seeds
  - gap-to-question conversion
  - missing evidence typing
  - next-round task seeding

## References

- `references/packet-format.md`
- `references/decision-heuristics.md`
- `references/openclaw-chaining-templates.md`

## Script

- `scripts/eco_council_reporting.py`

## OpenClaw Compatibility

- Call this skill after `$eco-council-normalize`.
- Feed readiness, authorization, report, or decision packets to OpenClaw instead of raw DB rows.
- Use `render-openclaw-prompts` when you want ready-to-paste role prompts that point at the packet files.
- Validate any promoted draft with `$eco-council-data-contract`.
