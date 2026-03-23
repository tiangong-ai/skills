# OpenClaw Agents

Use this note when working on the fixed moderator, sociologist, and environmentalist agents created by the supervisor.

## What `init-run` now does

- `init-run` scaffolds the run, writes supervisor/session prompt files, and provisions the three fixed OpenClaw agents by default.
- Use `--no-provision-openclaw` only when you intentionally want a scaffolded run without live agents.

## Command ownership

- Supervisor or human only:
  - `init-run`
  - `provision-openclaw-agents`
  - `continue-run`
  - `run-agent-step`
  - `import-task-review`
  - `import-source-selection`
  - `import-report`
  - `import-decision`
  - `import-fetch-execution`
- Role agent safe commands:
  - `status`
  - The explicit schema-validation command printed inside the active prompt or packet

## Role-agent rule

- A role agent owns only the current JSON artifact named by the turn prompt.
- A role agent must not advance stages, import files, or trigger raw-data shell stages unless the human explicitly reassigns it to act as the supervisor operator.

## Workspace files

After provisioning, each OpenClaw workspace contains:

- `IDENTITY.md`
- `OPENCLAW_AGENT_GUIDE.md`

The generated guide includes run-specific command examples, safe inspection commands, and commands reserved for the supervisor.
