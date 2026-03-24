from __future__ import annotations

import argparse
import json
from pathlib import Path

from river_outfall_status_lib import load_workbook


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Calculate river outfall submergence status summaries from an Excel workbook."
    )
    parser.add_argument("--input", required=True, help="Path to the input .xlsx workbook.")
    parser.add_argument(
        "--output-json",
        help="Optional path to write the normalized analysis JSON.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Print a human-readable summary instead of raw JSON.",
    )
    return parser


def render_pretty_summary(report: dict[str, object]) -> str:
    lines = [
        f"河道: {report['river_name']}",
        f"排口总数: {len(report['outfalls'])}",
        (
            "河道背景: "
            f"河底{'已提供' if report.get('has_bed_profile') else '未提供'}, "
            f"堤顶{'已提供' if report.get('has_levee_profile') else '未提供'}"
        ),
        (
            "左右岸分布: "
            f"左岸 {report['bank_counts']['left']} 个, "
            f"右岸 {report['bank_counts']['right']} 个, "
            f"待确认 {report['bank_counts']['unknown']} 个"
        ),
        "",
    ]
    for scenario in report["scenarios"]:
        scenario_key = scenario["key"]
        summary = report["summary"][scenario_key]
        counts = summary["counts"]
        lines.append(f"[{summary['label']}]")
        lines.append(
            "  "
            f"未受淹 {counts['safe']} | "
            f"部分受淹 {counts['partial']} | "
            f"完全淹没 {counts['submerged']} | "
            f"待补充 {counts['unknown']}"
        )
        submerged_codes = "、".join(summary["submerged_codes"]) or "无"
        partial_codes = "、".join(summary["partial_codes"]) or "无"
        lines.append(f"  完全淹没: {submerged_codes}")
        lines.append(f"  部分受淹: {partial_codes}")
        lines.append("")
    if report["warnings"]:
        lines.append("Warnings:")
        for warning in report["warnings"]:
            lines.append(f"  - {warning}")
    return "\n".join(lines).strip()


def main() -> int:
    args = build_parser().parse_args()
    report = load_workbook(args.input)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if args.pretty:
        print(render_pretty_summary(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
