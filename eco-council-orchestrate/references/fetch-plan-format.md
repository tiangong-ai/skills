# Fetch Plan Format

`prepare-round` writes `round_xxx/moderator/derived/fetch_plan.json`.

## Top-Level Shape

```json
{
  "plan_kind": "eco-council-fetch-plan",
  "schema_version": "1.0.0",
  "generated_at_utc": "2026-03-21T08:00:00Z",
  "input_snapshot": {
    "tasks": {
      "path": "",
      "exists": true,
      "sha256": ""
    },
    "source_selections": {}
  },
  "run": {},
  "roles": {
    "sociologist": {
      "allowed_sources": [],
      "evidence_requirements": [],
      "governed_families": [],
      "source_selection_path": "",
      "source_selection_status": "",
      "selected_sources": []
    }
  },
  "steps": []
}
```

## Step Fields

Each step includes:

- `step_id`
- `role`
- `source_skill`
- `task_ids`
- `depends_on`
- `artifact_path`
- `stdout_path`
- `stderr_path`
- `cwd`
- `command`
- `notes`
- `skill_refs`
- `normalizer_input`

Some steps may also include:

- `artifact_capture`
  - `stdout-json` means the fetch command prints the canonical JSON artifact to stdout and the runner should materialize that JSON at `artifact_path`.
- `download_dir`
  - sidecar directory for downloaded files when the canonical artifact is only a manifest
- `quarantine_dir`
  - optional sidecar directory for structure-validation issue files

## Intended Usage

- `command` is the exact shell snippet the expert agent or local runner should execute.
- `artifact_path` is the contract path that downstream normalization expects.
- `input_snapshot` records the task list and source-selection files that `prepare-round` planned against; if they change, rerun `prepare-round`.
- `roles.<role>.evidence_requirements` records the moderator-side evidence gaps that motivated this role's selection work.
- `roles.<role>.governed_families` records which family/layer policy surface applied to that role.
- `roles.<role>.selected_sources` is the only set that may execute automatically.
- `steps` may be an empty list when experts decided no source is needed for the round.
- `depends_on` is used for chained steps such as:
  - `youtube-video-search` -> `youtube-comments-fetch` when both were explicitly selected
  - `regulationsgov-comments-fetch` -> `regulationsgov-comment-detail-fetch` when both were explicitly selected
- Raw GDELT table steps (`gdelt-events-fetch`, `gdelt-mentions-fetch`, `gdelt-gkg-fetch`) use:
  - `artifact_capture=stdout-json`
  - `artifact_path` for the manifest JSON
  - `download_dir` for ZIP sidecars referenced by `downloads[].output_path`
- `normalizer_input` can be passed directly to `$eco-council-normalize --input`.

## Editing Rule

Do not change artifact paths casually after `prepare-round`.

If the moderator changes task scope enough to require different sources or different raw paths, rerun `prepare-round` and let the new plan replace the old one.
