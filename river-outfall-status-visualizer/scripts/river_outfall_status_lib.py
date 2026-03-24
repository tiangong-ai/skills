from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import math
import re
import zipfile
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

XML_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CORE_NS = "http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
DC_NS = "http://purl.org/dc/elements/1.1/"
DCTERMS_NS = "http://purl.org/dc/terms/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

NS = {"a": XML_NS}

SCENARIOS = [
    {
        "key": "current",
        "label": "当前水位",
        "color": "#2563eb",
        "aliases": ("当前水位", "所属河道当前水位", "当前河道水位"),
    },
    {
        "key": "normal",
        "label": "常水位",
        "color": "#f59e0b",
        "aliases": ("常水位", "所属河道常水位"),
    },
    {
        "key": "flood20",
        "label": "20年一遇",
        "color": "#8b5cf6",
        "aliases": (
            "20年一遇水位",
            "20年一遇常水位",
            "所属河道20年一遇水位",
            "所属河道20年一遇常水位",
        ),
    },
    {
        "key": "flood50",
        "label": "50年一遇",
        "color": "#dc2626",
        "aliases": (
            "50年一遇水位",
            "50年一遇常水位",
            "所属河道50年一遇水位",
            "所属河道50年一遇常水位",
        ),
    },
]

FIELD_ALIASES = {
    "number": ("排河口编号", "编号"),
    "name": ("排口名称", "名称", "排河口名称"),
    "code": ("代码", "排口代码", "排口编码"),
    "size": ("尺寸", "孔径", "管径"),
    "base_elev": ("底高程", "底部高程", "管底高程"),
    "crown_elev": ("口顶高程", "顶高程"),
    "bed_elev": ("河底高程", "河床高程", "河底", "槽底高程"),
    "levee_elev": ("堤顶高程", "堤顶", "岸顶高程", "岸顶"),
    "mileage": ("里程", "桩号"),
    "bank": ("位置", "左右岸", "岸别", "所在岸"),
    "river_name": ("所属河道名称", "河道名称", "河道"),
    "reach_name": ("所属河段名称", "河段名称", "节点名称", "闸点名称"),
}

STATUS_META = {
    "safe": {"label": "未受淹", "color": "#15803d"},
    "partial": {"label": "部分受淹", "color": "#ea580c"},
    "submerged": {"label": "完全淹没", "color": "#b91c1c"},
    "unknown": {"label": "待补充", "color": "#64748b"},
}

BANK_META = {
    "left": {"label": "左岸", "accent": "#0f766e"},
    "right": {"label": "右岸", "accent": "#1d4ed8"},
    "unknown": {"label": "待确认", "accent": "#475569"},
}

SIZE_PATTERN = re.compile(r"[xX×*]")


def _letters_to_index(col_letters: str) -> int:
    index = 0
    for char in col_letters:
        if char.isalpha():
            index = index * 26 + (ord(char.upper()) - 64)
    return index


def _index_to_letters(index: int) -> str:
    letters = []
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("a:v", NS)
    if cell_type == "s" and value_node is not None:
        shared_index = int(value_node.text or "0")
        return shared_strings[shared_index] if shared_index < len(shared_strings) else ""
    if cell_type == "inlineStr":
        inline = cell.find("a:is", NS)
        if inline is None:
            return ""
        return "".join(text_node.text or "" for text_node in inline.iter(f"{{{XML_NS}}}t"))
    if value_node is None or value_node.text is None:
        return ""
    return value_node.text


def read_simple_xlsx(path: str | Path) -> list[dict[str, str]]:
    workbook_path = Path(path)
    with zipfile.ZipFile(workbook_path) as archive:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in shared_root:
                shared_strings.append(
                    "".join(text_node.text or "" for text_node in item.iter(f"{{{XML_NS}}}t"))
                )

        workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
        rels_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: f"xl/{rel.attrib['Target']}"
            for rel in rels_root
            if rel.attrib.get("Target")
        }
        sheet_nodes = workbook_root.find("a:sheets", NS)
        if sheet_nodes is None or not list(sheet_nodes):
            return []
        first_sheet = list(sheet_nodes)[0]
        rel_id = first_sheet.attrib.get(f"{{{REL_NS}}}id")
        if not rel_id or rel_id not in rel_map:
            return []
        sheet_root = ET.fromstring(archive.read(rel_map[rel_id]))
        sheet_data = sheet_root.find("a:sheetData", NS)
        if sheet_data is None:
            return []

        parsed_rows: list[dict[int, str]] = []
        for row_node in sheet_data.findall("a:row", NS):
            row_values: dict[int, str] = {}
            for cell in row_node.findall("a:c", NS):
                ref = cell.attrib.get("r", "")
                col_letters = "".join(char for char in ref if char.isalpha())
                if not col_letters:
                    continue
                row_values[_letters_to_index(col_letters)] = _cell_text(cell, shared_strings).strip()
            if row_values:
                parsed_rows.append(row_values)

    if not parsed_rows:
        return []

    header_row = parsed_rows[0]
    max_header_col = max(header_row)
    headers = []
    for column in range(1, max_header_col + 1):
        header = (header_row.get(column) or "").strip()
        headers.append(header or f"Column{column}")

    records: list[dict[str, str]] = []
    for row_values in parsed_rows[1:]:
        record = {
            header: (row_values.get(index) or "").strip()
            for index, header in enumerate(headers, start=1)
        }
        if any(value != "" for value in record.values()):
            records.append(record)
    return records


def _sheet_rows_xml(
    headers: list[str],
    rows: list[dict[str, object]],
    register_shared: callable,
) -> str:
    def is_numeric(value: object) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def format_numeric(value: float | int) -> str:
        return f"{value:.12g}"

    row_xml_parts: list[str] = []
    header_cells = []
    for column_index, header in enumerate(headers, start=1):
        ref = f"{_index_to_letters(column_index)}1"
        header_cells.append(f'<c r="{ref}" t="s"><v>{register_shared(str(header))}</v></c>')
    row_xml_parts.append(f'<row r="1">{"".join(header_cells)}</row>')

    for row_number, row in enumerate(rows, start=2):
        cells = []
        for column_index, header in enumerate(headers, start=1):
            value = row.get(header, "")
            if value is None or value == "":
                continue
            ref = f"{_index_to_letters(column_index)}{row_number}"
            if is_numeric(value):
                cells.append(f'<c r="{ref}"><v>{format_numeric(value)}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="s"><v>{register_shared(str(value))}</v></c>')
        row_xml_parts.append(f'<row r="{row_number}">{"".join(cells)}</row>')
    return "".join(row_xml_parts)


def write_workbook_xlsx(
    path: str | Path,
    sheets: list[dict[str, object]],
) -> Path:
    if not sheets:
        raise ValueError("Workbook must contain at least one sheet.")

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    shared_strings: list[str] = []
    shared_index: dict[str, int] = {}

    def register_shared(value: str) -> int:
        if value not in shared_index:
            shared_index[value] = len(shared_strings)
            shared_strings.append(value)
        return shared_index[value]

    worksheets: list[tuple[str, str]] = []
    workbook_sheet_nodes: list[str] = []
    workbook_rel_nodes: list[str] = []
    content_override_nodes: list[str] = []

    for sheet_index, sheet in enumerate(sheets, start=1):
        sheet_name = str(sheet.get("name", f"Sheet{sheet_index}")).strip() or f"Sheet{sheet_index}"
        headers = [str(item) for item in sheet.get("headers", [])]
        rows = list(sheet.get("rows", []))

        worksheet_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<worksheet xmlns="{XML_NS}" xmlns:r="{REL_NS}"><sheetData>'
            + _sheet_rows_xml(headers, rows, register_shared)
            + "</sheetData></worksheet>"
        )
        worksheets.append((f"xl/worksheets/sheet{sheet_index}.xml", worksheet_xml))
        workbook_sheet_nodes.append(
            f'<sheet name="{escape(sheet_name)}" sheetId="{sheet_index}" r:id="rId{sheet_index}"/>'
        )
        workbook_rel_nodes.append(
            '<Relationship '
            f'Id="rId{sheet_index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{sheet_index}.xml"/>'
        )
        content_override_nodes.append(
            '<Override '
            f'PartName="/xl/worksheets/sheet{sheet_index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )

    shared_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<sst xmlns="{XML_NS}" count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">'
        + "".join(
            f'<si><t xml:space="preserve">{escape(text)}</t></si>' for text in shared_strings
        )
        + "</sst>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<workbook xmlns="{XML_NS}" xmlns:r="{REL_NS}"><sheets>'
        + "".join(workbook_sheet_nodes)
        + "</sheets></workbook>"
    )
    styles_rel_id = len(sheets) + 1
    shared_rel_id = len(sheets) + 2
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Relationships xmlns="{PKG_REL_NS}">'
        + "".join(workbook_rel_nodes)
        + '<Relationship '
        f'Id="rId{styles_rel_id}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        + '<Relationship '
        f'Id="rId{shared_rel_id}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" '
        'Target="sharedStrings.xml"/>'
        + "</Relationships>"
    )
    package_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Relationships xmlns="{PKG_REL_NS}">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" '
        'Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" '
        'Target="docProps/app.xml"/>'
        "</Relationships>"
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<styleSheet xmlns="{XML_NS}">'
        "<fonts count=\"1\">"
        "<font><sz val=\"11\"/><color theme=\"1\"/><name val=\"Calibri\"/><family val=\"2\"/><scheme val=\"minor\"/></font>"
        "</fonts>"
        "<fills count=\"2\">"
        "<fill><patternFill patternType=\"none\"/></fill>"
        "<fill><patternFill patternType=\"gray125\"/></fill>"
        "</fills>"
        "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"
        "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
        "<cellXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/></cellXfs>"
        "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>"
        "</styleSheet>"
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        + "".join(content_override_nodes)
        + '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
        '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        "</Types>"
    )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    core_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<cp:coreProperties xmlns:cp="{CORE_NS}" xmlns:dc="{DC_NS}" '
        f'xmlns:dcterms="{DCTERMS_NS}" xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        f'xmlns:xsi="{XSI_NS}">'
        "<dc:creator>Codex</dc:creator>"
        "<cp:lastModifiedBy>Codex</cp:lastModifiedBy>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>'
        "</cp:coreProperties>"
    )
    app_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>Codex</Application>"
        "</Properties>"
    )

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types_xml)
        archive.writestr("_rels/.rels", package_rels_xml)
        archive.writestr("docProps/app.xml", app_xml)
        archive.writestr("docProps/core.xml", core_xml)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        archive.writestr("xl/styles.xml", styles_xml)
        archive.writestr("xl/sharedStrings.xml", shared_xml)
        for worksheet_path, worksheet_xml in worksheets:
            archive.writestr(worksheet_path, worksheet_xml)
    return output_path


def write_simple_xlsx(
    path: str | Path,
    headers: list[str],
    rows: list[dict[str, object]],
    *,
    sheet_name: str = "Sheet1",
) -> Path:
    return write_workbook_xlsx(
        path,
        [
            {
                "name": sheet_name,
                "headers": headers,
                "rows": rows,
            }
        ],
    )


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def find_header(headers: list[str], aliases: tuple[str, ...]) -> str | None:
    alias_map = {alias.strip().casefold(): alias for alias in aliases}
    for header in headers:
        if header.strip().casefold() in alias_map:
            return header
    return None


def parse_size(size_text: str) -> dict[str, object] | None:
    raw_text = str(size_text or "").strip()
    if not raw_text:
        return None
    normalized = SIZE_PATTERN.sub("*", raw_text)
    if "*" in normalized:
        width_text, height_text = normalized.split("*", 1)
        width_mm = parse_float(width_text)
        height_mm = parse_float(height_text)
        return {
            "kind": "rect",
            "raw": raw_text,
            "width_mm": width_mm,
            "height_mm": height_mm,
            "width_m": (width_mm / 1000.0) if width_mm is not None else None,
            "height_m": (height_mm / 1000.0) if height_mm is not None else None,
        }
    diameter_mm = parse_float(normalized)
    return {
        "kind": "circle",
        "raw": raw_text,
        "diameter_mm": diameter_mm,
        "width_m": (diameter_mm / 1000.0) if diameter_mm is not None else None,
        "height_m": (diameter_mm / 1000.0) if diameter_mm is not None else None,
    }


def normalize_bank(raw_value: object) -> str:
    text = str(raw_value or "").strip().casefold()
    if not text:
        return "unknown"
    if "左" in text or text.startswith("left") or text == "l":
        return "left"
    if "右" in text or text.startswith("right") or text == "r":
        return "right"
    return "unknown"


def classify_status(
    water_level: float | None,
    base_elev: float | None,
    crown_elev: float | None,
) -> tuple[str, float | None]:
    if water_level is None or base_elev is None or crown_elev is None:
        return "unknown", None
    if water_level <= base_elev:
        return "safe", 0.0
    covered_height = min(max(water_level - base_elev, 0.0), max(crown_elev - base_elev, 0.0))
    if water_level >= crown_elev:
        return "submerged", covered_height
    return "partial", covered_height


def _scenario_levels(row: dict[str, str], scenario_headers: list[dict[str, str]]) -> dict[str, float]:
    levels: dict[str, float] = {}
    for scenario in scenario_headers:
        value = parse_float(row.get(scenario["field"], ""))
        if value is not None:
            levels[scenario["key"]] = value
    return levels


def _format_summary_text(label: str, counts: dict[str, int], total: int) -> str:
    return (
        f"{label}下共 {total} 个排口，"
        f"未受淹 {counts['safe']} 个，"
        f"部分受淹 {counts['partial']} 个，"
        f"完全淹没 {counts['submerged']} 个，"
        f"待补充 {counts['unknown']} 个。"
    )


def normalize_workbook_rows(rows: list[dict[str, str]], *, source_name: str = "") -> dict[str, object]:
    if not rows:
        raise ValueError("Workbook has no data rows.")

    headers = list(rows[0].keys())
    resolved_fields = {
        key: find_header(headers, aliases)
        for key, aliases in FIELD_ALIASES.items()
    }
    mileage_header = resolved_fields["mileage"]
    if not mileage_header:
        raise ValueError("Workbook is missing a mileage column such as `里程`.")

    scenario_headers = []
    for scenario in SCENARIOS:
        header = find_header(headers, scenario["aliases"])
        if header:
            scenario_headers.append(
                {
                    "key": scenario["key"],
                    "label": scenario["label"],
                    "color": scenario["color"],
                    "field": header,
                }
            )
    if not scenario_headers:
        raise ValueError("Workbook is missing recognized scenario water-level columns.")

    warnings: list[str] = []
    seen_codes: set[str] = set()
    duplicate_codes: set[str] = set()
    outfalls: list[dict[str, object]] = []
    controls: list[dict[str, object]] = []
    profile_points: list[dict[str, object]] = []
    channel_points: list[dict[str, object]] = []
    river_name = ""
    has_bed_profile = False
    has_levee_profile = False

    for row_index, row in enumerate(rows, start=2):
        mileage = parse_float(row.get(mileage_header, ""))
        if mileage is None:
            if any(str(value).strip() for value in row.values()):
                warnings.append(f"第 {row_index} 行里程缺失或无法解析，已跳过。")
            continue

        levels = _scenario_levels(row, scenario_headers)
        if levels:
            profile_points.append({"mileage": mileage, "levels": levels})

        row_river_name = (
            str(row.get(resolved_fields["river_name"], "")).strip()
            if resolved_fields["river_name"]
            else ""
        )
        if row_river_name and not river_name:
            river_name = row_river_name

        code = str(row.get(resolved_fields["code"], "")).strip() if resolved_fields["code"] else ""
        name = (
            str(row.get(resolved_fields["name"], "")).strip()
            if resolved_fields["name"]
            else ""
        )
        number = (
            str(row.get(resolved_fields["number"], "")).strip()
            if resolved_fields["number"]
            else ""
        )
        size_text = (
            str(row.get(resolved_fields["size"], "")).strip()
            if resolved_fields["size"]
            else ""
        )
        base_elev = (
            parse_float(row.get(resolved_fields["base_elev"], ""))
            if resolved_fields["base_elev"]
            else None
        )
        crown_elev = (
            parse_float(row.get(resolved_fields["crown_elev"], ""))
            if resolved_fields["crown_elev"]
            else None
        )
        bed_elev = (
            parse_float(row.get(resolved_fields["bed_elev"], ""))
            if resolved_fields["bed_elev"]
            else None
        )
        levee_elev = (
            parse_float(row.get(resolved_fields["levee_elev"], ""))
            if resolved_fields["levee_elev"]
            else None
        )
        reach_name = (
            str(row.get(resolved_fields["reach_name"], "")).strip()
            if resolved_fields["reach_name"]
            else ""
        )
        bank_raw = (
            str(row.get(resolved_fields["bank"], "")).strip()
            if resolved_fields["bank"]
            else ""
        )
        if bed_elev is not None or levee_elev is not None:
            channel_points.append(
                {
                    "mileage": mileage,
                    "bed_elev": bed_elev,
                    "levee_elev": levee_elev,
                }
            )
            has_bed_profile = has_bed_profile or bed_elev is not None
            has_levee_profile = has_levee_profile or levee_elev is not None
            if bed_elev is not None and levee_elev is not None and levee_elev < bed_elev:
                warnings.append(f"第 {row_index} 行堤顶高程低于河底高程，请核对。")

        has_outfall_payload = any(
            [
                code,
                number,
                size_text,
                base_elev is not None,
                bank_raw,
            ]
        )
        if reach_name and not has_outfall_payload:
            controls.append(
                {
                    "name": reach_name,
                    "mileage": mileage,
                    "levels": levels,
                    "bed_elev": bed_elev,
                    "levee_elev": levee_elev,
                }
            )
            continue
        if not has_outfall_payload:
            continue

        size_info = parse_size(size_text)
        if crown_elev is None and base_elev is not None and size_info and size_info.get("height_m") is not None:
            crown_elev = base_elev + float(size_info["height_m"])

        if not code:
            code = f"ROW{row_index:03d}"
            warnings.append(f"第 {row_index} 行缺少排口代码，已临时使用 `{code}`。")
        if code in seen_codes and code not in duplicate_codes:
            duplicate_codes.add(code)
            warnings.append(f"排口代码 `{code}` 重复出现，请核对数据。")
        seen_codes.add(code)
        if base_elev is None:
            warnings.append(f"排口 `{code}` 缺少底高程，状态只能标记为待补充。")
        if crown_elev is None:
            warnings.append(f"排口 `{code}` 无法确定口顶高程，状态只能标记为待补充。")

        bank_key = normalize_bank(bank_raw)
        if bank_key == "unknown":
            warnings.append(
                f"排口 `{code}` 的岸别 `{bank_raw or '空值'}` 无法识别，图中会标记为待确认。"
            )

        statuses: dict[str, dict[str, object]] = {}
        for scenario in scenario_headers:
            level = levels.get(scenario["key"])
            if scenario["key"] not in levels:
                warnings.append(
                    f"排口 `{code}` 缺少 `{scenario['label']}`，该场景状态会标记为待补充。"
                )
            status_key, covered_height = classify_status(level, base_elev, crown_elev)
            statuses[scenario["key"]] = {
                "level": level,
                "status_key": status_key,
                "status_label": STATUS_META[status_key]["label"],
                "covered_height": covered_height,
            }

        outfalls.append(
            {
                "row_index": row_index,
                "number": number,
                "name": name,
                "display_name": name or code,
                "code": code,
                "size_text": size_text,
                "shape": (size_info or {}).get("kind", "rect"),
                "width_m": (size_info or {}).get("width_m"),
                "height_m": (size_info or {}).get("height_m"),
                "diameter_mm": (size_info or {}).get("diameter_mm"),
                "width_mm": (size_info or {}).get("width_mm"),
                "height_mm": (size_info or {}).get("height_mm"),
                "bank": bank_key,
                "bank_label": BANK_META[bank_key]["label"],
                "bank_accent": BANK_META[bank_key]["accent"],
                "mileage": mileage,
                "base_elev": base_elev,
                "crown_elev": crown_elev,
                "bed_elev": bed_elev,
                "levee_elev": levee_elev,
                "levels": levels,
                "statuses": statuses,
                "reach_name": reach_name,
                "raw": row,
            }
        )

    if not outfalls:
        raise ValueError("Workbook has no recognizable outfall rows.")
    if not profile_points:
        raise ValueError("Workbook has no usable profile points for water levels.")

    outfalls.sort(key=lambda item: (item["mileage"], item["code"]))
    controls.sort(key=lambda item: (item["mileage"], item["name"]))
    profile_points.sort(key=lambda item: (item["mileage"], len(item["levels"])))
    channel_points.sort(key=lambda item: item["mileage"])

    bank_counts = {
        key: sum(1 for outfall in outfalls if outfall["bank"] == key)
        for key in BANK_META
    }

    summary: dict[str, dict[str, object]] = {}
    for scenario in scenario_headers:
        counts = {"safe": 0, "partial": 0, "submerged": 0, "unknown": 0}
        submerged_codes: list[str] = []
        partial_codes: list[str] = []
        for outfall in outfalls:
            status_info = outfall["statuses"][scenario["key"]]
            status_key = str(status_info["status_key"])
            counts[status_key] += 1
            if status_key == "submerged":
                submerged_codes.append(str(outfall["code"]))
            if status_key == "partial":
                partial_codes.append(str(outfall["code"]))
        summary[scenario["key"]] = {
            "label": scenario["label"],
            "counts": counts,
            "submerged_codes": submerged_codes,
            "partial_codes": partial_codes,
            "text": _format_summary_text(scenario["label"], counts, len(outfalls)),
        }

    elevation_values: list[float] = []
    mileage_values: list[float] = []
    for outfall in outfalls:
        if outfall["base_elev"] is not None:
            elevation_values.append(float(outfall["base_elev"]))
        if outfall["crown_elev"] is not None:
            elevation_values.append(float(outfall["crown_elev"]))
        if outfall["bed_elev"] is not None:
            elevation_values.append(float(outfall["bed_elev"]))
        if outfall["levee_elev"] is not None:
            elevation_values.append(float(outfall["levee_elev"]))
        mileage_values.append(float(outfall["mileage"]))
    for point in profile_points:
        mileage_values.append(float(point["mileage"]))
        elevation_values.extend(float(level) for level in point["levels"].values())
    for point in channel_points:
        mileage_values.append(float(point["mileage"]))
        if point["bed_elev"] is not None:
            elevation_values.append(float(point["bed_elev"]))
        if point["levee_elev"] is not None:
            elevation_values.append(float(point["levee_elev"]))
    if controls:
        mileage_values.extend(float(control["mileage"]) for control in controls)

    min_elev = min(elevation_values)
    max_elev = max(elevation_values)
    elev_span = max(max_elev - min_elev, 1.0)
    padding = max(0.5, elev_span * 0.08)

    available_scenarios = [
        {
            "key": scenario["key"],
            "label": scenario["label"],
            "color": scenario["color"],
        }
        for scenario in scenario_headers
    ]
    default_scenario = "current" if any(s["key"] == "current" for s in available_scenarios) else available_scenarios[0]["key"]
    notes = [
        "Y轴按真实高程展示，不做垂向夸张。",
        (
            "工作簿已提供河底/堤顶高程，主图按真实高程叠加河底线、堤顶线；"
            "当前选中场景的河道水体以蓝色填充。"
            if has_bed_profile or has_levee_profile
            else "当前工作簿未提供河底/堤顶高程，主图仅显示水位线与排口状态。"
        ),
        "页面不依赖浏览器横向滚动条；在纵断图区域滚动鼠标滚轮可做横向缩放，排口符号宽度不随缩放变化。",
        "下方时间轴式总览条用于定位当前视窗范围。",
        "左岸排口向轴线左侧偏置，右岸排口向轴线右侧偏置，并可按岸别单独筛选。",
    ]

    return {
        "source_name": source_name,
        "river_name": river_name or source_name or "未命名河道",
        "outfalls": outfalls,
        "controls": controls,
        "profile_points": profile_points,
        "channel_points": channel_points,
        "scenarios": available_scenarios,
        "summary": summary,
        "warnings": warnings,
        "bank_counts": bank_counts,
        "status_meta": STATUS_META,
        "bank_meta": BANK_META,
        "has_bed_profile": has_bed_profile,
        "has_levee_profile": has_levee_profile,
        "bounds": {
            "min_elev": min_elev - padding,
            "max_elev": max_elev + padding,
            "min_mileage": min(mileage_values),
            "max_mileage": max(mileage_values),
        },
        "notes": notes,
        "default_scenario": default_scenario,
    }


def load_workbook(path: str | Path) -> dict[str, object]:
    workbook_path = Path(path)
    rows = read_simple_xlsx(workbook_path)
    return normalize_workbook_rows(rows, source_name=workbook_path.stem)


def nice_step(span: float, target_steps: int = 6) -> float:
    if span <= 0:
        return 1.0
    rough = span / max(target_steps, 1)
    exponent = math.floor(math.log10(rough))
    fraction = rough / (10 ** exponent)
    if fraction <= 1:
        nice_fraction = 1
    elif fraction <= 2:
        nice_fraction = 2
    elif fraction <= 5:
        nice_fraction = 5
    else:
        nice_fraction = 10
    return nice_fraction * (10 ** exponent)
