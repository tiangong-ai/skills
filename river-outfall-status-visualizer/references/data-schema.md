# Data Schema

## Supported Workbook Pattern

Use one mixed worksheet unless the user provides a cleaner multi-sheet template. The first non-empty worksheet is the input source.

The preferred columns are:

| Column | Meaning | Required |
| --- | --- | --- |
| 排河口编号 | Human-facing sequence id | Recommended |
| 排口名称 | Human-facing outfall name for table and optional labeling | Recommended |
| 代码 | Stable outfall code | Yes for outfall rows |
| 尺寸 | `宽*高` for rectangular outfalls or `直径` for circular outfalls, in millimeters | Yes unless 口顶高程 is present |
| 底高程 | Outfall invert elevation | Yes for outfall rows |
| 口顶高程 | Explicit crown elevation | Optional |
| 里程 | Along-river mileage | Yes |
| 位置 | `左岸` or `右岸` | Yes for left/right distinction |
| 所属河道名称 | River name | Recommended |
| 所属河段名称 | Reach name or control-node label | Required on control-node rows |
| 当前水位 / 所属河道当前水位 | Current water level | Recommended |
| 常水位 / 所属河道常水位 | Normal water level | Recommended |
| 20年一遇水位 / 所属河道20年一遇水位 / 所属河道20年一遇常水位 | 20-year water level | Recommended |
| 50年一遇水位 / 所属河道50年一遇水位 / 所属河道50年一遇常水位 | 50-year water level | Recommended |

## Row Classification

- Treat a row as a control node when `所属河段名称` is present and outfall-defining columns such as `代码`, `尺寸`, and `底高程` are empty.
- Treat a row as an outfall when at least one of `代码`, `排河口编号`, `尺寸`, or `底高程` is present.
- Keep water-level columns on both row types. Outfall rows can still contribute profile points for the water lines.

## Column Alias Rules

- Accept current-water aliases: `当前水位`, `所属河道当前水位`, `当前河道水位`.
- Accept normal-water aliases: `常水位`, `所属河道常水位`.
- Accept 20-year aliases: `20年一遇水位`, `20年一遇常水位`, `所属河道20年一遇水位`, `所属河道20年一遇常水位`.
- Accept 50-year aliases: `50年一遇水位`, `50年一遇常水位`, `所属河道50年一遇水位`, `所属河道50年一遇常水位`.
- Accept bank-side aliases: `位置`, `岸别`, `左右岸`, `所在岸`.
- Accept outfall-name aliases: `排口名称`, `排河口名称`, `名称`.

## Data Hygiene Rules

- Sort outfalls and control nodes by mileage before analysis or rendering.
- Keep empty numeric cells empty. Do not replace them with `0`.
- Warn when bank-side text cannot be normalized to `左岸` or `右岸`.
- Warn when size is missing and crown elevation cannot be inferred.
- Warn when scenario columns are missing, but continue with the scenarios that are present.
