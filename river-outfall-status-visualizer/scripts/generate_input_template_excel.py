from __future__ import annotations

import argparse
from pathlib import Path

from river_outfall_status_lib import write_workbook_xlsx


ENTRY_HEADERS = [
    "所属河道名称",
    "所属河段名称",
    "里程",
    "河底高程",
    "堤顶高程",
    "所属河道当前水位",
    "所属河道常水位",
    "所属河道20年一遇水位",
    "所属河道50年一遇水位",
    "位置",
    "排河口编号",
    "排口名称",
    "代码",
    "尺寸",
    "底高程",
    "口顶高程",
]

GUIDE_HEADERS = ["项目", "填写要求", "示例"]

GUIDE_ROWS = [
    {
        "项目": "工作表使用",
        "填写要求": "只在“录入模板”sheet 中录入正式数据；每一行只能表示一个控制节点或一个排口。",
        "示例": "控制节点行与排口行可混排，系统会按列内容自动识别。",
    },
    {
        "项目": "控制节点行",
        "填写要求": "填写 所属河道名称、所属河段名称、里程、河底高程、堤顶高程、各场景水位；位置和排口字段留空。",
        "示例": "所属河段名称=上游控制断面，里程=2350。",
    },
    {
        "项目": "排口行",
        "填写要求": "填写 位置、排河口编号/排口名称/代码、尺寸、底高程；可同时填写所属河段、水位、河底、堤顶，建议与所在河段保持一致。",
        "示例": "位置=左岸，代码=PK023，尺寸=1800，底高程=18.45。",
    },
    {
        "项目": "尺寸填写",
        "填写要求": "矩形排口填写“宽*高”，圆形排口直接填写直径，单位均为 mm。",
        "示例": "2200*1500 或 1800。",
    },
    {
        "项目": "口顶高程",
        "填写要求": "可选。若留空，系统会根据“底高程 + 尺寸高度/直径”自动推算。",
        "示例": "异形口门或已知实测口顶时可直接填写。",
    },
    {
        "项目": "位置",
        "填写要求": "仅填写“左岸”或“右岸”。",
        "示例": "左岸。",
    },
    {
        "项目": "里程",
        "填写要求": "沿河里程，建议统一使用 m。",
        "示例": "3560。",
    },
    {
        "项目": "河底/堤顶高程",
        "填写要求": "用于纵断图背景剖面绘制，建议至少在控制节点行完整填写；排口行可补充所在位置的对应高程。",
        "示例": "河底高程=15.20，堤顶高程=22.80。",
    },
    {
        "项目": "水位场景",
        "填写要求": "当前水位、常水位、20年一遇、50年一遇可按掌握情况填写；缺失场景不会参与对应分析。",
        "示例": "所属河道当前水位=19.35。",
    },
    {
        "项目": "代码唯一性",
        "填写要求": "正式排口代码建议唯一，便于汇总和筛选。",
        "示例": "PK001、PK002、PK003。",
    },
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a directly usable Excel input template for the river outfall status visualizer."
    )
    parser.add_argument(
        "--output",
        default="assets/templates/river-outfall-input-template.xlsx",
        help="Path to the output .xlsx template workbook.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output_path = write_workbook_xlsx(
        args.output,
        [
            {
                "name": "录入模板",
                "headers": ENTRY_HEADERS,
                "rows": [],
            },
            {
                "name": "填写说明",
                "headers": GUIDE_HEADERS,
                "rows": GUIDE_ROWS,
            },
        ],
    )
    print(f"Input template written to: {Path(output_path).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
