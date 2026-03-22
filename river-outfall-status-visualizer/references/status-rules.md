# Status Rules

## Elevation Inputs

- Use `底高程` as the outfall invert elevation.
- Use `口顶高程` when it is present.
- If `口顶高程` is absent, infer it from geometry:
- Rectangular outfall: `口顶高程 = 底高程 + 高度`.
- Circular outfall: `口顶高程 = 底高程 + 直径`.

Interpret `尺寸` in millimeters:

- `2000*1000` means width `2000 mm`, height `1000 mm`.
- `1500` means diameter `1500 mm`.

## Scenario Status Logic

For each scenario water level:

- `未受淹`
- Water level `<= 底高程`
- `部分受淹`
- `底高程 < 水位 < 口顶高程`
- `完全淹没`
- `水位 >= 口顶高程`
- `待补充`
- Required water level, invert elevation, or crown elevation is missing.

## Reporting Rules

- Report counts for every available scenario.
- List fully submerged outfalls explicitly because that is usually the briefing-critical set.
- Keep partially submerged outfalls in the summary because they are the next likely risk set.
- When `当前水位` is absent, say so directly instead of pretending that `常水位` is current.

## Validation Rules

- Warn on non-numeric mileage.
- Warn on duplicate outfall codes.
- Warn on outfalls with unresolved crown elevation.
- Warn on scenarios that exist globally but are missing on specific rows.
