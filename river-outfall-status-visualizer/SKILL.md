---
name: river-outfall-status-visualizer
description: Analyze river outfall Excel workbooks and build report-ready river outfall status visualizations that compare current, normal, 20-year, and 50-year water levels, distinguish left-bank and right-bank outfalls, and identify which outfalls are safe, partially submerged, or fully submerged. Use when Codex needs to turn a river longitudinal-profile or outfall inventory workbook into a briefing chart, status summary, mock dataset, or reusable HTML report.
---

# River Outfall Status Visualizer

## Core Goal

- Convert one Excel workbook into:
- A scenario-aware river outfall status summary.
- A standalone HTML chart suitable for briefing and review.
- A reusable mock workbook that follows the same template.
- Keep the chart vertically truthful. `Y` values must always use real elevation.
- Allow horizontal zoom without resizing outfall symbols. Only the outfall anchor positions move with the river layout.
- Distinguish left-bank and right-bank outfalls with side-specific offset, connector direction, label placement, and legend.

## Required Inputs

- Start from a `.xlsx` workbook.
- Prefer the mixed single-sheet format documented in [references/data-schema.md](references/data-schema.md):
- Outfall rows carry code, size, base elevation, mileage, bank side, and scenario water levels.
- Control-node rows carry reach or gate names, mileage, and scenario water levels.
- If `当前水位` is missing, ask for it or state clearly that the output only supports the available scenarios.
- If the workbook lacks riverbed or channel-bottom data, describe the output as a river outfall status chart, not a true riverbed longitudinal profile.

## Quick Start

1. Generate or inspect a workbook:

```bash
python3 scripts/generate_mock_example_excel.py \
  --output assets/examples/example-river-sample.xlsx
```

2. Compute scenario summaries:

```bash
python3 scripts/calc_submergence.py \
  --input assets/examples/example-river-sample.xlsx \
  --pretty
```

3. Render a standalone HTML report:

```bash
python3 scripts/render_status_report.py \
  --input assets/examples/example-river-sample.xlsx \
  --output assets/examples/example-river-sample-report.html
```

## Workflow

1. Read [references/data-schema.md](references/data-schema.md) before touching the workbook schema.
2. Read [references/status-rules.md](references/status-rules.md) before changing status logic.
3. Read [references/visual-spec.md](references/visual-spec.md) before changing the chart layout or legend.
4. Use `scripts/calc_submergence.py` first when the request is analytical.
5. Use `scripts/render_status_report.py` when the request needs a deliverable chart.
6. Use `scripts/generate_mock_example_excel.py` to create a regression fixture or explain the expected workbook shape.
7. Report validation warnings explicitly. Do not silently coerce missing elevations, missing sizes, or ambiguous bank-side values.

## Visual Rules

- Keep `Y` coordinates in true elevation. Do not vertically exaggerate.
- Use mileage for ordering and anchor positioning.
- Keep the page free of browser-level horizontal scrolling. Use wheel zoom in the chart plus the timeline overview for navigation.
- Allow horizontal zoom or compression for layout, but keep outfall symbol widths fixed during zoom.
- Anchor each outfall at its true mileage and base elevation.
- Draw the outfall crown from true geometry height when size data is available.
- Use shape to distinguish geometry type:
- Rectangle for box culverts or rectangular outfalls.
- Circle for circular pipes. Do not flatten circular pipes into ellipses.
- Use bank-side offset to distinguish left-bank and right-bank outfalls:
- Left-bank outfalls offset to the left of the river axis.
- Right-bank outfalls offset to the right of the river axis.
- Reinforce bank side with connector direction, label placement, and legend text.
- Use status as the dominant semantic encoding:
- `未受淹`
- `部分受淹`
- `完全淹没`
- Make the selected scenario visually dominant and keep non-selected scenarios visible but weaker.
- Prefer code plus size on the chart. Keep full outfall names in the detail table when the chart is dense enough that names would harm readability.

## Scripts

- `scripts/generate_mock_example_excel.py`
- Create an anonymized example workbook with mixed outfall and control-node rows.
- `scripts/calc_submergence.py`
- Read a workbook, normalize the rows, compute scenario statuses, and print or export a summary.
- `scripts/render_status_report.py`
- Generate a standalone HTML report with scenario switching and horizontal zoom.
- `scripts/river_outfall_status_lib.py`
- Shared parser, validator, geometry, summary, and `.xlsx` read/write helpers.

## References

- [references/data-schema.md](references/data-schema.md)
- Workbook shape, column aliases, and row classification rules.
- [references/status-rules.md](references/status-rules.md)
- Water-level interpretation, crown inference, and tri-state status logic.
- [references/visual-spec.md](references/visual-spec.md)
- Chart semantics, left/right bank encoding, and layout constraints.

## Output Checklist

- State which scenarios were actually present in the source workbook.
- Report total outfall count and counts by status for the highlighted scenario.
- List fully submerged outfall codes explicitly.
- Call out left-bank and right-bank counts when they matter to the request.
- Mention missing riverbed data when the chart is a status visualization rather than a true riverbed longitudinal profile.
