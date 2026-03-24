---
name: eco-council-data-contract
description: Define, validate, and scaffold shared data contracts for eco-council multi-agent runs, rounds, source governance, canonical submissions, readiness reports, moderator matching authorization/results, evidence-library entries, expert reports, and moderator decisions. Use when Codex needs to standardize staged data exchange between moderator, sociologist, environmentalist, and historian agents, create canonical JSON or SQLite schemas, or plan deterministic normalization and evidence-library persistence before OpenClaw orchestration.
---

# Eco Council Data Contract

## Core Goal

- Keep one shared contract for the eco-council control plane and evidence plane.
- Validate mission, round-task, source-selection, override-request, claim, observation, claim-submission, observation-submission, data-readiness-report, matching-authorization, matching-result, evidence-adjudication, isolated-entry, remand-entry, expert-report, and moderator-decision payloads before OpenClaw agents exchange them.
- Scaffold a repeatable run directory and initialize a canonical SQLite store for downstream normalization and linking.
- Expand audited `policy_profile` defaults into the effective mission caps and source-governance envelope used by the rest of the stack.

## Workflow

1. Inspect the supported object kinds.

```bash
python3 scripts/eco_council_contract.py list-kinds --pretty
```

2. Emit example payloads and adapt them before wiring OpenClaw.

```bash
python3 scripts/eco_council_contract.py write-example \
  --kind mission \
  --output /tmp/eco-mission.json \
  --pretty
```

3. Validate one object or a list of objects.

```bash
python3 scripts/eco_council_contract.py validate \
  --kind claim \
  --input /tmp/claims.json \
  --pretty
```

4. Scaffold one council run before connecting the moderator loop.

```bash
python3 scripts/eco_council_contract.py scaffold-run \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --run-id eco-20260320-chiangmai-smoke \
  --topic "Chiang Mai smoke verification" \
  --objective "Determine whether public smoke claims are supported by physical evidence." \
  --start-utc 2026-03-18T00:00:00Z \
  --end-utc 2026-03-19T23:59:59Z \
  --region-label "Chiang Mai, Thailand" \
  --point 18.7883,98.9853 \
  --pretty
```

5. Or scaffold the run directly from a fully authored `mission.json`.

```bash
python3 scripts/eco_council_contract.py scaffold-run-from-mission \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --mission-input ./configs/chiangmai-mission.json \
  --pretty
```

6. Scaffold the next round from `next_round_tasks` after moderator review.

```bash
python3 scripts/eco_council_contract.py scaffold-round \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --round-id round-002 \
  --tasks-input ./runs/20260320-chiangmai-smoke/round_001/moderator/next_round_tasks.json \
  --pretty
```

7. Initialize the canonical SQLite database used by the deterministic normalization layer.

```bash
python3 scripts/eco_council_contract.py init-db \
  --db ./data/eco-council.db \
  --pretty
```

8. Validate the scaffolded bundle after agents or post-processors write files.

```bash
python3 scripts/eco_council_contract.py validate-bundle \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --pretty
```

## Canonical Objects

- `mission`: moderator-owned run charter and shared window/region constraints.
- `round-task`: moderator-assigned work item for one expert role in one round.
- `source-selection`: expert-owned audited decision about whether any source is needed and which allowed sources may run.
- `override-request`: explicit request for an upstream human/bot to change a mission cap or source-governance boundary without letting moderator or experts self-apply it.
- `claim`: sociologist-produced public or policy assertion that may need physical validation.
- `claim-submission`: sociologist-owned compact claim candidate that is explicitly audited for worth-storing and representativeness.
- `observation`: environmentalist-produced normalized measurement or event summary from one physical source.
- `observation-submission`: environmentalist-owned compact observation candidate that is explicitly audited for worth-storing and representativeness.
- `data-readiness-report`: per-data-role sufficiency judgment over the current active evidence-library submissions.
- `matching-authorization`: moderator-owned gate that decides whether a matching pass may run.
- `matching-result`: deterministic claim-observation matching output constrained by moderator authorization.
- `evidence-adjudication`: moderator-facing result summary over cards, isolated entries, and remands after matching.
- `isolated-entry`: evidence-library object for claims or observations that remain reasonably unmatched.
- `remand-entry`: evidence-library object for claims or observations that require another round of collection before adjudication is stable.
- `evidence-card`: linked assessment between one claim and one or more observations.
- `expert-report`: per-role round report for sociologist, environmentalist, historian, or moderator.
- `council-decision`: moderator verdict for whether the round is sufficient or another round is required.

## Scope Decision

- Keep this skill focused on contracts, validation, scaffolding, and SQLite initialization.
- Do not fetch source data here.
- Do not perform geocoding, embedding, or RAG retrieval here.
- Do not let moderator or experts exchange raw skill payloads directly; normalize first, then pass canonical objects.
- Treat `shared/evidence-library/` as the persistent cross-round ledger surface for active submissions, cards, isolated entries, and remands.
- Treat `mission.policy_profile` plus optional mission overrides as the only authority for default caps; agents may only ask for changes through `override_requests`.

## References

- `references/contract-notes.md`
- `references/normalization-roadmap.md`

## Assets

- `assets/schemas/eco_council.schema.json`
- `assets/sqlite/eco_council.sql`
- `assets/examples/*.json`

## Script

- `scripts/eco_council_contract.py`

## OpenClaw Compatibility

- Let moderator, sociologist, environmentalist, and historian exchange only canonical files defined by this skill.
- Keep raw fetch outputs under `raw/` and canonical outputs under `normalized/` or `shared/`.
- Use moderator rounds externally in OpenClaw; this skill only scaffolds and validates round state.
- Scaffolded rounds now include placeholder `source_selection.json` files for sociologist and environmentalist so audited source choice has one canonical location.
- Scaffolded rounds also include an `evidence-library/` directory for active submissions, cards, isolated entries, remands, and the append-only ledger.
- Use `scaffold-round` after moderator approval instead of mutating earlier round folders in place.
