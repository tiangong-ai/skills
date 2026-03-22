# Visual Spec

## Chart Semantics

- Treat the chart as a river outfall status visualization for briefing, not as a design drawing.
- Keep the vertical axis in true elevation.
- Use mouse-wheel horizontal zoom inside the profile chart and a timeline-style overview strip below the chart for positioning.
- Do not rely on browser-level horizontal scrollbars for chart browsing.
- Do not let horizontal zoom resize outfall symbol widths.
- Keep every outfall anchored to its true mileage and base elevation.

## Left/Right Bank Encoding

- Offset left-bank outfalls to the left of the river axis.
- Offset right-bank outfalls to the right of the river axis.
- Let users toggle left-bank and right-bank visibility from the legend row.
- Use connector direction, label placement, and legend notes to reinforce bank side.
- Avoid using status color to encode bank side. Reserve status color for submergence state.

## Status Encoding

- Use one dominant scenario at a time.
- Keep the selected scenario water line strongest.
- Render water levels as continuous step lines: horizontal within a reach, vertical transition at gates or control nodes.
- Use outfall border and badge color for the selected scenario status.
- Show submerged depth inside the outfall symbol with a water-colored fill overlay when possible.
- Keep non-selected scenarios visible with thinner or lighter water lines.

## Label Rules

- Show concise outfall labeling on the chart without slanted leader lines.
- Keep the outfall identifier inside the symbol when space allows; otherwise place it immediately above the symbol.
- Show size text close to the outfall symbol.
- Prefer code plus size on the chart and keep full `排口名称` in the detail table when the view contains many outfalls.
- Add staggered label offsets only when local density makes overlap unavoidable.
- Use the detail table below the chart for full attributes rather than overloading the drawing.

## Summary Rules

- Show total outfall count.
- Show counts for `未受淹`, `部分受淹`, and `完全淹没`.
- Show the active scenario label clearly.
- Show fully submerged outfall codes in a dedicated summary block.
- If the workbook includes both left-bank and right-bank outfalls, report the bank counts alongside the status counts.
