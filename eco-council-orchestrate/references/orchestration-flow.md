# Orchestration Flow

## Control Plane vs Data Plane

Keep these phases separate:

1. moderator review
   - mission is already fixed
   - moderator edits `tasks.json` only
   - moderator expresses evidence needs and claim focus; moderator does not prescribe source skills
2. expert source selection
   - sociologist and environmentalist decide whether any source is needed
   - they choose `family_plans`, `layer_plans`, anchors, and exact source skills under mission `source_governance`
   - only explicitly selected sources may run
   - canonical outputs live at `round_xxx/<role>/source_selection.json`
3. expert raw collection
   - sociologist and environmentalist execute fetch commands
   - only `raw/` artifacts are written
   - manifest-backed sources may also write sidecar downloads under `raw/<source>/`, for example GDELT table ZIP files referenced by a canonical manifest JSON
4. deterministic data plane
   - normalize
   - link evidence
   - build round context
   - build report/decision drafts
5. expert and moderator deliberation
   - experts revise report drafts
   - moderator revises decision draft
6. promotion and next-round scaffolding
   - promote approved drafts
   - scaffold `round_002`, `round_003`, ...

## Recommended Loop

For each round:

1. moderator reviews `tasks.json`
2. experts produce `source_selection.json`
3. `prepare-round`
4. expert agents run fetch prompts
5. `run-data-plane`
6. OpenClaw experts revise report drafts
7. OpenClaw moderator revises decision draft
8. `$eco-council-reporting promote-all`
9. if `council_decision.next_round_required=true`, run `advance-round`

## File Boundaries

- moderator input:
  - `mission.json`
  - `round_xxx/moderator/tasks.json`
- expert source-selection output:
  - `round_xxx/sociologist/source_selection.json`
  - `round_xxx/environmentalist/source_selection.json`
- expert raw output:
  - `round_xxx/<role>/raw/*`
- deterministic exchange:
  - `round_xxx/shared/claims.json`
  - `round_xxx/shared/observations.json`
  - `round_xxx/shared/evidence_cards.json`
- report handoff:
  - `round_xxx/<role>/derived/*.json`
  - `round_xxx/<role>/derived/openclaw_*.txt`
