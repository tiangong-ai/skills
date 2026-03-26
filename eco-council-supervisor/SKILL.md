---
name: eco-council-supervisor
description: Run an eco-council workflow through one stage-gated local supervisor, maintain a local SQLite case library of historical eco-council records, and aggregate a separate offline SQLite signal corpus of normalized public and environment signals across runs. Use when you want to bootstrap a run from mission JSON, provision fixed OpenClaw moderator/sociologist/environmentalist agents, require audited expert source-selection before any fetch stage, gate the later flow through data-readiness, moderator matching authorization, matching and adjudication, reports, and decisions, import agent JSON replies safely, advance rounds with minimal manual freedom, render a human-readable meeting record from the run directory, archive completed runs into a queryable local history database, or import signal-bearing run directories into a cross-run corpus for retrieval, evaluation, and training preparation.
---

# Eco Council Supervisor

Use this skill when the eco-council flow should be driven by one deterministic local controller instead of ad hoc shell usage.

## Core Workflow

1. Initialize one run with `init-run`.
   - Optionally attach one local case-library SQLite database so the moderator receives compact similar-case context automatically.
   - By default, `init-run` also provisions the fixed OpenClaw moderator/sociologist/environmentalist agents and writes their workspace guides.
   - By default, the supervisor also auto-archives into `runs/archives/eco_council_case_library.sqlite` and `runs/archives/eco_council_signal_corpus.sqlite`; use `--disable-auto-archive` only for special runs.
2. Use `provision-openclaw-agents` only to repair or recreate the three fixed agents later, or use `--no-provision-openclaw` at `init-run` time when you intentionally want a scaffolded run without live agents.
3. Let the moderator review or revise `tasks.json`.
4. Let the sociologist and environmentalist each return one audited `source-selection` object.
  - Moderator tasks should express evidence needs only.
  - Expert source selection should map those needs into governed source families, layers, anchors, and exact skills.
  - Each agent stays inside the active `policy_profile` envelope and uses `override_requests` only when an upstream human/bot must widen caps or governance.
5. Follow `RUN_DIR/supervisor/CURRENT_STEP.txt`.
6. At each shell stage, use `continue-run` and approve the step.
7. At each agent stage, import returned JSON with:
  - `import-task-review`
  - `import-source-selection`
  - `import-data-readiness`
  - `import-matching-authorization`
  - `import-report`
  - `import-decision`
8. If raw fetch is produced by an external runner instead of `continue-run`, import the canonical `fetch_execution.json` with `import-fetch-execution`.
9. After `run-data-plane`, the supervisor auto-imports normalized analytics into the offline signal corpus by default, and after `promote-all` it auto-imports the run snapshot into the case library by default.

## Command Surface

- `python3 scripts/eco_council_supervisor.py init-run --run-dir ... --mission-input ... [--workspace-root ...] [--history-db ... --history-top-k 3] [--case-library-db ...] [--signal-corpus-db ...] [--disable-auto-archive] [--no-provision-openclaw] [--yes] --pretty`
  - Calls `$eco-council-orchestrate bootstrap-run`.
  - Creates supervisor state plus role/session prompt files.
  - By default, also creates or reuses the three fixed OpenClaw agents; use `--no-provision-openclaw` to skip that on purpose.
  - When `--history-db` is set, moderator task-review and decision turns also receive a compact similar-case summary from the local history library.
  - By default, every run auto-archives into the repo-local case library and signal corpus under `runs/archives/`.
  - `--case-library-db` or `--signal-corpus-db` overrides those default archive paths.
  - `--disable-auto-archive` turns off both archive imports for special cases.
- `python3 scripts/eco_council_supervisor.py provision-openclaw-agents --run-dir ... --pretty`
  - Creates or reuses fixed OpenClaw agent ids for moderator, sociologist, and environmentalist.
  - Refreshes each agent workspace `IDENTITY.md` plus `OPENCLAW_AGENT_GUIDE.md`.
- `python3 scripts/eco_council_supervisor.py status --run-dir ... [--history-db ... --history-top-k 3] [--disable-history-context] [--case-library-db ...] [--signal-corpus-db ...] [--disable-auto-archive] --pretty`
  - Shows current round, stage, outbox prompts, `CURRENT_STEP.txt`, the current historical-context attachment state, and the current automatic archive attachment state for both the case library and signal corpus.
- `python3 scripts/eco_council_supervisor.py summarize-run --run-dir ... --lang zh --pretty`
  - Renders one human-readable Markdown meeting record under `RUN_DIR/reports/`.
  - Supports `--lang zh|en` for report language only; workflow payloads remain in English.
- `python3 scripts/eco_council_case_library.py init-db --db ... --pretty`
  - Initializes a local SQLite history database for eco-council records.
- `python3 scripts/eco_council_case_library.py import-run --db ... --run-dir ... --overwrite --pretty`
  - Imports one completed or in-progress run directory into the case library.
- `python3 scripts/eco_council_case_library.py import-runs-root --db ... --runs-root ... --overwrite --pretty`
  - Bulk-imports every run under one `runs/` root.
- `python3 scripts/eco_council_case_library.py list-cases --db ... --pretty`
  - Lists imported historical cases with final status and latest counts.
- `python3 scripts/eco_council_case_library.py search-cases --db ... --query ... [--region-label ...] --pretty`
  - Finds similar historical cases for human lookup or moderator context injection.
- `python3 scripts/eco_council_case_library.py show-case --db ... --case-id ... --pretty`
  - Shows one case plus per-round summaries; optional flags can include report, claim, and evidence summaries.
- `python3 scripts/eco_council_case_library.py export-case-markdown --db ... --case-id ... [--lang zh|en] --pretty`
  - Exports one human-readable Markdown case record for audit or replay review.
- `python3 scripts/eco_council_signal_corpus.py init-db --db ... --pretty`
  - Initializes a separate local SQLite corpus for normalized public/environment signals imported from run analytics databases.
- `python3 scripts/eco_council_signal_corpus.py import-run --db ... --run-dir ... [--overwrite] --pretty`
  - Imports one signal-bearing run after `run-data-plane`; reads analytics database paths from `run_manifest.json` when present and otherwise falls back to `RUN_DIR/analytics/public_signals.sqlite` plus `RUN_DIR/analytics/environment_signals.sqlite`.
- `python3 scripts/eco_council_signal_corpus.py import-runs-root --db ... --runs-root ... [--overwrite] --pretty`
  - Bulk-imports every eligible run under one `runs/` root into the offline signal corpus.
- `python3 scripts/eco_council_signal_corpus.py list-runs --db ... [--limit 50] --pretty`
  - Lists imported runs with current stage and normalized signal counts.
- `python3 scripts/eco_council_signal_corpus.py show-run --db ... --run-id ... --pretty`
  - Shows one imported run with per-round summaries, source breakdowns, claim types, metrics, and artifact inventory aggregates.
- `python3 scripts/eco_council_supervisor.py continue-run --run-dir ... --pretty`
  - Runs exactly one approved shell stage.
- `python3 scripts/eco_council_supervisor.py run-agent-step --run-dir ... --pretty`
  - Sends the current moderator/expert turn to OpenClaw, captures JSON, validates it, and imports it automatically.
  - Supports moderator task review, expert source selection, expert data-readiness drafting, moderator matching-authorization drafting, expert report drafting, and moderator decision drafting.
- `python3 scripts/eco_council_supervisor.py import-task-review ...`
- `python3 scripts/eco_council_supervisor.py import-source-selection ...`
- `python3 scripts/eco_council_supervisor.py import-data-readiness ...`
- `python3 scripts/eco_council_supervisor.py import-matching-authorization ...`
- `python3 scripts/eco_council_supervisor.py import-report ...`
- `python3 scripts/eco_council_supervisor.py import-decision ...`
- `python3 scripts/eco_council_supervisor.py import-fetch-execution --run-dir ... [--input ...] --pretty`
  - Accepts canonical `fetch_execution.json` produced by an external raw-data runner, validates it against the full current fetch plan under the shared `fetch.lock`, and advances the stage to `ready-to-run-data-plane`.

## Guardrails

- Keep shell execution inside the supervisor.
- Keep agents limited to JSON-only outputs.
- No source runs unless an expert selected it in `source_selection.json` within the active mission `source_governance` boundary.
- Moderator and experts may propose `override_requests`, but those requests are advisory only until an upstream human/supervisor edits `mission.json`; they never self-apply mission-envelope changes.
- Treat `RUN_DIR/supervisor/CURRENT_STEP.txt` as the human checklist.
- If raw artifacts come from an external runner or simulator, import only a canonical `fetch_execution.json` whose usable artifact paths already exist.
- If OpenClaw cannot load local repo skills directly, still use the generated prompt files as the source of truth.
- Treat the case-library SQLite database as historical record storage, not as a replacement for canonical per-run JSON artifacts.
- Treat the signal-corpus SQLite database as an offline cross-run aggregation and training-prep store built from normalized analytics DBs, not as a replacement for canonical per-run JSON artifacts or the live per-run analytics databases.
- Automatic signal-corpus import happens only after `run-data-plane` succeeds; it never replaces the live per-run analytics DBs that were just written.
- Automatic case-library import happens only after `promote-all` succeeds; it stores run/round/evidence summaries for retrieval and audit, not the live raw artifacts.
- Treat moderator historical context as planning-only retrieval help; it must not override current-run evidence.

## References

- `references/workflow.md`
- `references/openclaw-agents.md`
