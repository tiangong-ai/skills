from __future__ import annotations

import argparse
from pathlib import Path

from river_outfall_status_lib import write_simple_xlsx


HEADERS = [
    "排河口编号",
    "排口名称",
    "代码",
    "尺寸",
    "底高程",
    "里程",
    "位置",
    "所属河道名称",
    "所属河段名称",
    "所属河道当前水位",
    "所属河道常水位",
    "所属河道20年一遇水位",
    "所属河道50年一遇水位",
]

CONTROL_NODES = [
    {"name": "样例起点", "mileage": 0, "current": 22.10, "normal": 21.70, "flood20": 23.05, "flood50": 23.85},
    {"name": "样例上游一", "mileage": 1500, "current": 21.15, "normal": 20.75, "flood20": 22.50, "flood50": 23.20},
    {"name": "样例上游二", "mileage": 2900, "current": 20.20, "normal": 19.75, "flood20": 21.55, "flood50": 22.35},
    {"name": "样例中上段", "mileage": 4200, "current": 19.20, "normal": 18.70, "flood20": 20.80, "flood50": 21.70},
    {"name": "样例中段", "mileage": 5600, "current": 18.25, "normal": 17.80, "flood20": 19.95, "flood50": 20.90},
    {"name": "样例中下段", "mileage": 7100, "current": 17.40, "normal": 16.95, "flood20": 18.95, "flood50": 20.05},
    {"name": "样例下游一", "mileage": 8600, "current": 16.55, "normal": 16.05, "flood20": 17.90, "flood50": 18.95},
    {"name": "样例下游二", "mileage": 10050, "current": 15.80, "normal": 15.30, "flood20": 17.10, "flood50": 18.25},
    {"name": "样例出口前", "mileage": 11250, "current": 15.10, "normal": 14.65, "flood20": 16.10, "flood50": 17.20},
    {"name": "样例出口", "mileage": 12000, "current": 14.90, "normal": 14.40, "flood20": 15.80, "flood50": 16.90},
]

OUTFALL_MILEAGES = [
    180, 430, 710, 980, 1260, 1580, 1870, 2140, 2430, 2720,
    3050, 3340, 3620, 3890, 4170, 4470, 4760, 5060, 5360, 5660,
    5960, 6260, 6560, 6860, 7160, 7460, 7760, 8060, 8360, 8660,
    8980, 9300, 9620, 9950, 10280, 10610, 10940, 11270, 11600, 11880,
]

CIRCLE_SIZES = ["600", "800", "1000", "1200", "1500", "1800", "2000", "2200"]
RECT_SIZES = [
    "1600*1200",
    "1800*1200",
    "2200*1500",
    "2400*1800",
    "3000*2000",
    "3600*2400",
    "4200*3000",
]
BASE_OFFSETS = [
    0.95, -0.25, 0.35, 1.35, -0.70, 0.15, 0.85, -1.05, 1.55, 0.55,
    -0.40, 0.05, 1.15, -0.85, 0.45, 1.75, -1.20, 0.25, 0.75, -0.55,
]


def make_row(
    *,
    number: str = "",
    name: str = "",
    code: str = "",
    size: str = "",
    base_elev: float | str = "",
    mileage: float | int,
    bank: str = "",
    river_name: str = "",
    reach_name: str = "",
    current_level: float | str = "",
    normal_level: float | str = "",
    flood20_level: float | str = "",
    flood50_level: float | str = "",
) -> dict[str, object]:
    return {
        "排河口编号": number,
        "排口名称": name,
        "代码": code,
        "尺寸": size,
        "底高程": base_elev,
        "里程": mileage,
        "位置": bank,
        "所属河道名称": river_name,
        "所属河段名称": reach_name,
        "所属河道当前水位": current_level,
        "所属河道常水位": normal_level,
        "所属河道20年一遇水位": flood20_level,
        "所属河道50年一遇水位": flood50_level,
    }


def segment_for_mileage(mileage: float) -> dict[str, object]:
    segment = CONTROL_NODES[0]
    for node in CONTROL_NODES:
        if mileage >= node["mileage"]:
            segment = node
        else:
            break
    return segment


def build_rows() -> list[dict[str, object]]:
    river = "样例河道A"
    rows: list[dict[str, object]] = []

    for node in CONTROL_NODES:
        rows.append(
            make_row(
                mileage=node["mileage"],
                reach_name=node["name"],
                current_level=node["current"],
                normal_level=node["normal"],
                flood20_level=node["flood20"],
                flood50_level=node["flood50"],
            )
        )

    for index, mileage in enumerate(OUTFALL_MILEAGES, start=1):
        segment = segment_for_mileage(mileage)
        bank = "左岸" if index % 2 else "右岸"
        if index % 5 == 0:
            bank = "右岸" if bank == "左岸" else "左岸"

        if index % 3 == 1:
            size = RECT_SIZES[(index - 1) % len(RECT_SIZES)]
        else:
            size = CIRCLE_SIZES[(index - 1) % len(CIRCLE_SIZES)]

        base_offset = BASE_OFFSETS[(index - 1) % len(BASE_OFFSETS)]
        base_elev = round(float(segment["normal"]) + base_offset, 2)

        rows.append(
            make_row(
                number=str(index),
                name=f"样例排口{index:02d}",
                code=f"A{index:04d}",
                size=size,
                base_elev=base_elev,
                mileage=mileage,
                bank=bank,
                river_name=river,
                current_level=segment["current"],
                normal_level=segment["normal"],
                flood20_level=segment["flood20"],
                flood50_level=segment["flood50"],
            )
        )

    rows.sort(key=lambda item: float(item["里程"]))
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a 12 km anonymized example river outfall workbook with 40 outfalls."
    )
    parser.add_argument(
        "--output",
        default="assets/examples/example-river-sample.xlsx",
        help="Path to the output .xlsx workbook.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output_path = write_simple_xlsx(
        args.output,
        HEADERS,
        build_rows(),
        sheet_name="Sheet1",
    )
    print(f"Mock workbook written to: {Path(output_path).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
