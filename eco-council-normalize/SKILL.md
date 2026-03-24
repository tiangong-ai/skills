---
name: eco-council-normalize
description: Normalize eco-council raw fetch outputs into separate public-signal and environment-signal staging databases, canonical submissions, shared claims and observations, evidence-library entries, evidence cards, and role-specific round context JSON. Use when sociologist and environmentalist agents have already collected raw JSON, JSONL, CSV, or ZIP artifacts and OpenClaw needs deterministic cleaning, deduplication, provenance tracking, and moderator-ready aggregation before data-readiness review, matching authorization, or report writing.
---

# Eco Council Normalize

## Core Goal

- Keep OpenClaw in the control plane and keep deterministic normalization in a separate data-plane step.
- Build two staging SQLite databases per run:
  - `analytics/public_signals.sqlite`
  - `analytics/environment_signals.sqlite`
- Convert raw artifacts into canonical `claim-submission`, `observation-submission`, active evidence-library views, `claim`, `observation`, `evidence-card`, `isolated-entry`, `remand-entry`, and role-specific context JSON compatible with `$eco-council-data-contract`.

## Required Upstream State

- Start from one scaffolded eco-council run directory created by `$eco-council-data-contract`.
- Keep raw fetch outputs under role-owned `raw/` directories.
- Use the mission geometry and mission window already stored in `mission.json`.

## Workflow

1. Initialize the normalization workbench for one run.

```bash
python3 scripts/eco_council_normalize.py init-run \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --pretty
```

2. Normalize public-signal artifacts into:
- `analytics/public_signals.sqlite`
- `round_001/sociologist/normalized/public_signals.jsonl`
- `round_001/shared/claims.json`

```bash
python3 scripts/eco_council_normalize.py normalize-public \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --round-id round-001 \
  --input gdelt-doc-search=./runs/20260320-chiangmai-smoke/round_001/sociologist/raw/gdelt-doc-search.json \
  --input gdelt-gkg-fetch=./runs/20260320-chiangmai-smoke/round_001/sociologist/raw/gdelt-gkg-fetch.json \
  --input bluesky-cascade-fetch=./runs/20260320-chiangmai-smoke/round_001/sociologist/raw/bluesky/seed_posts.json \
  --input youtube-comments-fetch=./runs/20260320-chiangmai-smoke/round_001/sociologist/raw/youtube-comments.jsonl \
  --pretty
```

3. Normalize environment artifacts into:
- `analytics/environment_signals.sqlite`
- `round_001/environmentalist/normalized/environment_signals.jsonl`
- `round_001/shared/observations.json`

```bash
python3 scripts/eco_council_normalize.py normalize-environment \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --round-id round-001 \
  --input airnow-hourly-obs-fetch=./runs/20260320-chiangmai-smoke/round_001/environmentalist/raw/airnow-hourly-obs-fetch.json \
  --input open-meteo-air-quality-fetch=./runs/20260320-chiangmai-smoke/round_001/environmentalist/raw/open-meteo-air-quality.json \
  --input nasa-firms-fire-fetch=./runs/20260320-chiangmai-smoke/round_001/environmentalist/raw/nasa-firms-fire.json \
  --pretty
```

4. Link canonical claims and observations into `matching-result`, `evidence-adjudication`, `evidence-card`, `isolated-entry`, and `remand-entry`.

```bash
python3 scripts/eco_council_normalize.py link-evidence \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

5. Build compact role contexts for report writing and moderator review.

```bash
python3 scripts/eco_council_normalize.py build-round-context \
  --run-dir ./runs/20260320-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

## Supported Sources

Public-signal normalization currently supports:
- `gdelt-doc-search`
- `youtube-video-search`
- `youtube-comments-fetch`
- `bluesky-cascade-fetch`
- `federal-register-doc-fetch`
- `regulationsgov-comments-fetch`
- `regulationsgov-comment-detail-fetch`
- ZIP-backed manifest ingestion for `gdelt-events-fetch`, `gdelt-mentions-fetch`, and `gdelt-gkg-fetch`
  - the canonical raw artifact is the manifest JSON
  - the normalizer reads the referenced ZIP sidecars and emits deterministic `table-coverage` plus sample row signals

Environment normalization currently supports:
- `airnow-hourly-obs-fetch`
- `usgs-water-iv-fetch`
- `open-meteo-historical-fetch`
- `open-meteo-air-quality-fetch`
- `open-meteo-flood-fetch`
- `nasa-firms-fire-fetch`
- `openaq-data-fetch` API JSON and CSV/CSV.GZ artifacts

## Scope Decision

- Do not fetch remote data here.
- Do not use an LLM here.
- Do not geocode place names here.
- Do not write final natural-language expert reports here.
- Keep raw artifacts immutable and preserve provenance into every canonical object.
- Keep `shared/evidence-library/` as the persistent cross-round surface for active submissions, cards, isolated entries, and remands.

## References

- `references/pipeline-layout.md`
- `references/source-mapping.md`
- `references/context-format.md`

## Assets

- `assets/sqlite/public_signals.sql`
- `assets/sqlite/environment_signals.sql`
- `assets/examples/public_signal.json`
- `assets/examples/environment_signal.json`
- `assets/examples/role_context.json`

## Script

- `scripts/eco_council_normalize.py`

## OpenClaw Compatibility

- Let OpenClaw call fetch skills first, then call this skill for deterministic normalization.
- Pass only canonical outputs or role contexts into moderator/expert prompts.
- Treat `analytics/*.sqlite` as staging workbenches and `shared/*.json` as exchange artifacts.
