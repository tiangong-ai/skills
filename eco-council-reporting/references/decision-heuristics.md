# Decision Heuristics

This skill uses deterministic heuristics to seed moderator decisions.

## Missing Evidence Typing

Current gap-to-type mappings:

- station corroboration gaps -> `station-air-quality`
- wildfire gaps without fire detections -> `fire-detection`
- wildfire gaps without weather context -> `meteorology-background`
- flood gaps -> `precipitation-hydrology`
- heat gaps -> `temperature-extremes`
- drought gaps -> `precipitation-soil-moisture`
- policy-reaction gaps -> `policy-comment-coverage`
- thin public coverage across channels -> `public-discussion-coverage`

## Task Seeding

Each missing evidence type maps to one default next-round task shape:

- `station-air-quality` -> `environmentalist` with one station-air-quality evidence requirement
- `fire-detection` -> `environmentalist` with one fire-detection evidence requirement
- `meteorology-background` -> `environmentalist` with one weather-context evidence requirement
- `precipitation-hydrology` -> `environmentalist` with one hydrometeorological evidence requirement
- `temperature-extremes` -> `environmentalist` with one temperature evidence requirement
- `precipitation-soil-moisture` -> `environmentalist` with one drought-corroboration evidence requirement
- `policy-comment-coverage` -> `sociologist` with one rulemaking-evidence requirement
- `public-discussion-coverage` -> `sociologist` with one broader public-discussion coverage requirement

The moderator side now seeds `task.inputs.evidence_requirements` instead of concrete source hints.
Experts later translate those requirements into governed source families and layers during source-selection.
When counting public-source diversity, `gdelt-doc-search` and the three raw GDELT table skills are treated as one GDELT family rather than four independent channels.

## Completion Logic

High-level decision rules:

- unresolved evidence plus remaining round budget -> `moderator_status=continue`
- fully resolved evidence and complete reports -> `moderator_status=complete`
- unresolved evidence after `max_rounds` is exhausted, or no usable round artifacts -> `moderator_status=blocked`

The generated `completion_score` is heuristic only. Moderator review may still revise it.
