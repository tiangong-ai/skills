# Supervisor Workflow

The supervisor keeps a strict split:

1. Local shell stages
   - `prepare-round`
   - `execute-fetch-plan`
   - `run-data-plane`
   - `promote-all`
   - `advance-round`
2. Agent stages
   - moderator task review
   - sociologist source selection
   - environmentalist source selection
   - sociologist report draft
   - environmentalist report draft
   - moderator decision draft

## Files the user should care about

- `RUN_DIR/supervisor/CURRENT_STEP.txt`
- `RUN_DIR/supervisor/state.json`
- `RUN_DIR/supervisor/sessions/*.txt`
- `RUN_DIR/supervisor/outbox/*.txt`
- `RUN_DIR/supervisor/responses/*`

## Stage Map

- `awaiting-moderator-task-review`
  - Prefer `run-agent-step`.
  - Manual fallback: send the moderator session prompt and task-review outbox prompt, then use `import-task-review`.
- `awaiting-source-selection`
  - Prefer `run-agent-step --role sociologist` and `run-agent-step --role environmentalist`.
  - Manual fallback: send the two expert source-selection outbox prompts, then use `import-source-selection`.
  - Moderator tasks should describe evidence needs through `task.inputs.evidence_requirements`; they should not prescribe concrete source skills.
  - Expert source-selection must use packet `governance` plus `family_memory` to choose `family_plans`, `layer_plans`, and exact `selected_sources`.
- `ready-to-prepare-round`
  - Run `continue-run`.
- `ready-to-execute-fetch-plan`
  - Run `continue-run`.
  - External/manual fallback: write the round `raw/` artifacts plus canonical `fetch_execution.json`, then use `import-fetch-execution`.
  - The imported execution record must cover every current-plan step, match current step ids and artifact paths, and contain only usable statuses for downstream normalization.
  - `import-fetch-execution` acquires the same round `fetch.lock` as live fetch execution so external runners and supervisor import stay serialized.
- `ready-to-run-data-plane`
  - Run `continue-run`.
- `awaiting-expert-reports`
  - Prefer `run-agent-step --role sociologist` and `run-agent-step --role environmentalist`.
  - Manual fallback: send the two expert outbox prompts, then use `import-report`.
- `awaiting-moderator-decision`
  - Prefer `run-agent-step`.
  - Manual fallback: send the moderator decision outbox prompt, then use `import-decision`.
- `ready-to-promote`
  - Run `continue-run`.
- `ready-to-advance-round`
  - Run `continue-run`.
- `completed`
  - Stop.

## OpenClaw Note

- `init-run` now provisions the three fixed OpenClaw agents by default unless `--no-provision-openclaw` is used.
- `provision-openclaw-agents` remains available as a repair/recreate command for the same three agents.
- Each provisioned workspace now contains `IDENTITY.md` and `OPENCLAW_AGENT_GUIDE.md`.
- The generated guide tells agents which commands are safe to inspect, which commands are reserved for the supervisor, and why.
- Provisioning does not force a chat channel. That keeps Feishu optional. You can talk to the agents through whatever OpenClaw surface you prefer.

## Offline Signal Corpus

- Import a run into the offline signal corpus only after `run-data-plane` has produced normalized analytics databases, or when the run directory already contains those databases from an earlier execution.
- `scripts/eco_council_signal_corpus.py` resolves analytics DB paths from `RUN_DIR/run_manifest.json` first and otherwise falls back to `RUN_DIR/analytics/public_signals.sqlite` plus `RUN_DIR/analytics/environment_signals.sqlite`.
- If supervisor state has `signal_corpus.db` configured, `continue-run` automatically imports the current run into that offline signal corpus immediately after a successful `run-data-plane` step.
- The offline signal corpus is a separate cross-run aggregation store for retrieval, evaluation, and training preparation. Keep canonical per-run JSON artifacts and per-run analytics DBs as the source of truth for an individual run.
- Replay fixtures or eval bundles that only materialize shared JSON outputs without analytics DB tables will not populate the offline signal corpus.
