#!/usr/bin/env python3
"""Deterministic normalization pipeline for eco-council runs."""

from __future__ import annotations

import argparse
import csv
import os
import gzip
import hashlib
import importlib.util
import io
import json
import math
import re
import sqlite3
import statistics
import sys
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
ASSETS_DIR = SKILL_DIR / "assets"
PUBLIC_DDL_PATH = ASSETS_DIR / "sqlite" / "public_signals.sql"
ENVIRONMENT_DDL_PATH = ASSETS_DIR / "sqlite" / "environment_signals.sql"
CONTRACT_SCRIPT_PATH = SKILL_DIR.parent / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"

SCHEMA_VERSION = "1.0.0"
POINT_MATCH_EPSILON_DEGREES = 0.05
NORMALIZE_CACHE_VERSION = "v3"
MAX_CONTEXT_TASKS = 4
MAX_CONTEXT_CLAIMS = 4
MAX_CONTEXT_OBSERVATIONS = 8
MAX_CONTEXT_EVIDENCE = 4
GDELT_SCAN_ROW_LIMIT = 25000
GDELT_EXAMPLE_SIGNAL_LIMIT = 3
GDELT_MATCHED_ROW_STORE_LIMIT = 32
PHYSICAL_CLAIM_TYPES = {
    "wildfire",
    "smoke",
    "flood",
    "heat",
    "drought",
    "air-pollution",
    "water-pollution",
}
NON_CLAIM_PUBLIC_SIGNAL_KINDS = {
    "artifact-manifest",
    "table-coverage",
    "timeline-bin",
}
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "were",
    "with",
}
CLAIM_KEYWORDS = {
    "wildfire": ("wildfire", "fire", "burning", "burn", "forest fire", "bushfire"),
    "smoke": ("smoke", "haze", "smog", "ash"),
    "flood": ("flood", "flooding", "overflow", "inundation"),
    "heat": ("heat", "heatwave", "hot weather", "extreme heat"),
    "drought": ("drought", "dry spell", "water shortage", "dryness"),
    "air-pollution": ("air quality", "pm2.5", "pm10", "pollution", "dirty air", "aqi"),
    "water-pollution": ("water pollution", "contaminated water", "sewage", "toxic spill"),
    "policy-reaction": ("policy", "regulation", "rulemaking", "public comment", "epa", "agency"),
}
CLAIM_METRIC_RULES = {
    "smoke": {
        "support": {
            "pm2_5": 35.0,
            "pm10": 50.0,
            "us_aqi": 100.0,
            "fire_detection_count": 1.0,
        },
        "contradict": {
            "pm2_5": 12.0,
            "pm10": 20.0,
            "us_aqi": 50.0,
        },
    },
    "air-pollution": {
        "support": {
            "pm2_5": 35.0,
            "pm10": 50.0,
            "us_aqi": 100.0,
            "nitrogen_dioxide": 40.0,
            "ozone": 100.0,
        },
        "contradict": {
            "pm2_5": 12.0,
            "pm10": 20.0,
            "us_aqi": 50.0,
        },
    },
    "wildfire": {
        "support": {
            "fire_detection_count": 1.0,
            "temperature_2m": 30.0,
            "wind_speed_10m": 5.0,
        },
        "contradict": {
            "fire_detection_count": 0.0,
            "precipitation_sum": 20.0,
            "relative_humidity_2m": 70.0,
        },
    },
    "flood": {
        "support": {
            "precipitation_sum": 20.0,
            "precipitation": 10.0,
            "river_discharge": 100.0,
            "river_discharge_mean": 100.0,
            "river_discharge_max": 120.0,
            "river_discharge_p75": 100.0,
        },
        "contradict": {
            "precipitation_sum": 1.0,
            "river_discharge": 20.0,
            "river_discharge_mean": 20.0,
            "river_discharge_max": 25.0,
            "river_discharge_p75": 20.0,
        },
    },
    "heat": {
        "support": {
            "temperature_2m": 32.0,
        },
        "contradict": {
            "temperature_2m": 22.0,
        },
    },
    "drought": {
        "support": {
            "precipitation_sum": 2.0,
            "soil_moisture_0_to_7cm": 0.12,
        },
        "contradict": {
            "precipitation_sum": 10.0,
            "soil_moisture_0_to_7cm": 0.25,
        },
    },
}
OPENAQ_TIME_KEYS = (
    "datetime",
    "date",
    "observed_at",
    "observedAt",
    "timestamp",
    "utc",
)
OPENAQ_VALUE_KEYS = ("value", "measurement", "concentration")
OPENAQ_LAT_KEYS = ("latitude", "lat")
OPENAQ_LON_KEYS = ("longitude", "lon", "lng")
ENVIRONMENT_METRIC_ALIASES = {
    "pm25": "pm2_5",
    "pm2.5": "pm2_5",
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "o3": "ozone",
    "ozone": "ozone",
    "no2": "nitrogen_dioxide",
    "nitrogen_dioxide": "nitrogen_dioxide",
    "so2": "sulphur_dioxide",
    "sulphur_dioxide": "sulphur_dioxide",
    "co": "carbon_monoxide",
    "carbon_monoxide": "carbon_monoxide",
    "us_aqi": "us_aqi",
    "gage_height": "gage_height",
}
AIRNOW_PARAMETER_METRIC_MAP = {
    "PM25": "pm2_5",
    "PM10": "pm10",
    "OZONE": "ozone",
    "NO2": "nitrogen_dioxide",
    "CO": "carbon_monoxide",
    "SO2": "sulphur_dioxide",
}
USGS_PARAMETER_METRIC_MAP = {
    "00060": "river_discharge",
    "00065": "gage_height",
}
GDELT_EVENTS_INDEX = {
    "event_id": 0,
    "sql_date": 1,
    "actor1_name": 6,
    "actor2_name": 16,
    "event_code": 26,
    "event_base_code": 27,
    "event_root_code": 28,
    "goldstein": 30,
    "num_mentions": 31,
    "num_sources": 32,
    "num_articles": 33,
    "avg_tone": 34,
    "action_geo_name": 52,
    "action_geo_country": 53,
    "action_geo_lat": 56,
    "action_geo_lon": 57,
    "date_added": 59,
    "source_url": 60,
}
GDELT_MENTIONS_INDEX = {
    "event_id": 0,
    "event_time": 1,
    "mention_time": 2,
    "mention_type": 3,
    "source_name": 4,
    "identifier": 5,
    "confidence": 11,
    "doc_length": 12,
    "doc_tone": 13,
}
GDELT_GKG_INDEX = {
    "record_id": 0,
    "date": 1,
    "source_common_name": 3,
    "document_identifier": 4,
    "themes": 8,
    "locations": 10,
    "persons": 12,
    "organizations": 14,
    "tone": 15,
    "all_names": 23,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: Path) -> list[Any]:
    records: list[Any] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            records.append(json.loads(text))
    return records


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    atomic_write_text_file(path, pretty_json(payload, pretty=pretty) + "\n")


def atomic_write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record, ensure_ascii=True, sort_keys=True) for record in records]
    atomic_write_text_file(path, "\n".join(lines) + ("\n" if lines else ""))


def normalize_space(value: str) -> str:
    return " ".join(str(value).split())


def truncate_text(value: str, limit: int) -> str:
    text = normalize_space(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    text = normalize_space(str(value))
    return text


def maybe_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def canonical_environment_metric(value: Any) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    lowered = text.casefold()
    if lowered.endswith("_aqi"):
        base_metric = ENVIRONMENT_METRIC_ALIASES.get(lowered[:-4], lowered[:-4])
        return f"{base_metric}_aqi"
    return ENVIRONMENT_METRIC_ALIASES.get(lowered, text)


def parse_loose_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    text = normalize_space(str(value))
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for pattern in ("%Y%m%d%H%M%S", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, pattern)
            parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    return None


def to_rfc3339_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_path_payload(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return read_json(path)
    if suffix == ".jsonl":
        return read_jsonl(path)
    raise ValueError(f"Unsupported JSON payload path: {path}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def text_tokens(value: Any, *, minimum_length: int = 4) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", maybe_text(value).casefold())
    output: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if len(token) < minimum_length or token in STOPWORDS or token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output


def parse_round_components(round_id: str) -> tuple[str, int, int] | None:
    match = re.match(r"^(.*?)(\d+)$", maybe_text(round_id))
    if match is None:
        return None
    prefix, digits = match.groups()
    return prefix, int(digits), len(digits)


def round_sort_key(round_id: str) -> tuple[str, int, str]:
    components = parse_round_components(round_id)
    if components is None:
        return (round_id, 10**9, round_id)
    prefix, number, _width = components
    return (prefix, number, round_id)


def round_directory_name(round_id: str) -> str:
    return round_id.replace("-", "_")


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_directory_name(round_id)


def discover_round_ids(run_dir: Path) -> list[str]:
    round_ids: list[str] = []
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        if not child.name.startswith("round_"):
            continue
        round_ids.append(child.name.replace("_", "-"))
    return sorted(dict.fromkeys(round_ids), key=round_sort_key)


def previous_round_id(run_dir: Path, round_id: str) -> str | None:
    components = parse_round_components(round_id)
    if components is None:
        candidates = [item for item in discover_round_ids(run_dir) if item < round_id]
        return candidates[-1] if candidates else None
    prefix, number, _width = components
    candidates: list[str] = []
    for item in discover_round_ids(run_dir):
        item_components = parse_round_components(item)
        if item_components is None:
            continue
        item_prefix, item_number, _item_width = item_components
        if item_prefix == prefix and item_number < number:
            candidates.append(item)
    return candidates[-1] if candidates else None


def mission_path(run_dir: Path) -> Path:
    return run_dir / "mission.json"


def load_mission(run_dir: Path) -> dict[str, Any]:
    payload = read_json(mission_path(run_dir))
    if not isinstance(payload, dict):
        raise ValueError("mission.json must be an object.")
    return payload


def mission_run_id(mission: dict[str, Any]) -> str:
    run_id = mission.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("mission.json missing run_id.")
    return run_id


def mission_window(mission: dict[str, Any]) -> dict[str, str]:
    window = mission.get("window")
    if not isinstance(window, dict):
        raise ValueError("mission.json missing window.")
    start_utc = maybe_text(window.get("start_utc"))
    end_utc = maybe_text(window.get("end_utc"))
    if not start_utc or not end_utc:
        raise ValueError("mission.json window is incomplete.")
    return {"start_utc": start_utc, "end_utc": end_utc}


def mission_place_scope(mission: dict[str, Any]) -> dict[str, Any]:
    region = mission.get("region")
    if not isinstance(region, dict):
        raise ValueError("mission.json missing region.")
    label = maybe_text(region.get("label")) or "Mission region"
    geometry = region.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("mission.json region.geometry must be an object.")
    return {"label": label, "geometry": geometry}


def mission_constraints(mission: dict[str, Any]) -> dict[str, int]:
    if CONTRACT_MODULE is not None and hasattr(CONTRACT_MODULE, "effective_constraints"):
        values = CONTRACT_MODULE.effective_constraints(mission)
        if isinstance(values, dict):
            return {key: int(value) for key, value in values.items() if isinstance(value, int) and value > 0}
    constraints = mission.get("constraints")
    if not isinstance(constraints, dict):
        return {}
    output: dict[str, int] = {}
    for key in (
        "max_rounds",
        "max_claims_per_round",
        "max_tasks_per_round",
        "claim_target_per_round",
        "claim_hard_cap_per_round",
    ):
        value = constraints.get(key)
        if isinstance(value, int) and value > 0:
            output[key] = value
    return output


def geometry_to_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    kind = maybe_text(geometry.get("type"))
    if kind == "Point":
        lat = maybe_number(geometry.get("latitude"))
        lon = maybe_number(geometry.get("longitude"))
        if lat is None or lon is None:
            return None
        return (lon, lat, lon, lat)
    if kind == "BBox":
        west = maybe_number(geometry.get("west"))
        south = maybe_number(geometry.get("south"))
        east = maybe_number(geometry.get("east"))
        north = maybe_number(geometry.get("north"))
        if None in {west, south, east, north}:
            return None
        return (float(west), float(south), float(east), float(north))
    return None


def geometry_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_type = maybe_text(left.get("type"))
    right_type = maybe_text(right.get("type"))
    if left_type == "Point" and right_type == "Point":
        left_lat = maybe_number(left.get("latitude"))
        left_lon = maybe_number(left.get("longitude"))
        right_lat = maybe_number(right.get("latitude"))
        right_lon = maybe_number(right.get("longitude"))
        if None in {left_lat, left_lon, right_lat, right_lon}:
            return False
        assert left_lat is not None
        assert left_lon is not None
        assert right_lat is not None
        assert right_lon is not None
        return (
            abs(left_lat - right_lat) <= POINT_MATCH_EPSILON_DEGREES
            and abs(left_lon - right_lon) <= POINT_MATCH_EPSILON_DEGREES
        )
    if left_type == "Point" and right_type == "BBox":
        left_lat = maybe_number(left.get("latitude"))
        left_lon = maybe_number(left.get("longitude"))
        bbox = geometry_to_bbox(right)
        if None in {left_lat, left_lon} or bbox is None:
            return False
        assert left_lat is not None
        assert left_lon is not None
        west, south, east, north = bbox
        return west <= left_lon <= east and south <= left_lat <= north
    if left_type == "BBox" and right_type == "Point":
        return geometry_overlap(right, left)
    left_bbox = geometry_to_bbox(left)
    right_bbox = geometry_to_bbox(right)
    if left_bbox is None or right_bbox is None:
        return False
    left_west, left_south, left_east, left_north = left_bbox
    right_west, right_south, right_east, right_north = right_bbox
    return not (
        left_east < right_west
        or right_east < left_west
        or left_north < right_south
        or right_north < left_south
    )


def point_matches_geometry(latitude: float | None, longitude: float | None, geometry: dict[str, Any]) -> bool:
    if latitude is None or longitude is None:
        return False
    return geometry_overlap(
        {"type": "Point", "latitude": latitude, "longitude": longitude},
        geometry,
    )


def mission_region_tokens(mission: dict[str, Any]) -> list[str]:
    return text_tokens(mission_place_scope(mission).get("label"), minimum_length=3)


def mission_topic_tokens(mission: dict[str, Any]) -> list[str]:
    ignored = set(mission_region_tokens(mission))
    values: list[Any] = [mission.get("topic"), mission.get("objective")]
    hypotheses = mission.get("hypotheses")
    if isinstance(hypotheses, list):
        values.extend(item for item in hypotheses if item is not None)
    tokens: list[str] = []
    seen: set[str] = set()
    for value in values:
        for token in text_tokens(value, minimum_length=4):
            if token in ignored or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return tokens


def row_token_set(*parts: Any, minimum_length: int = 3) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        tokens.update(text_tokens(part, minimum_length=minimum_length))
    return tokens


def source_domain(value: str) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    parsed = urlparse(text)
    domain = parsed.netloc or parsed.path
    domain = domain.casefold()
    return domain[4:] if domain.startswith("www.") else domain


def top_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
    return items


def top_counter_text(counter: Counter[str], limit: int = 3) -> str:
    parts = [f"{item['value']} ({item['count']})" for item in top_counter_items(counter, limit=limit)]
    return ", ".join(parts)


def maybe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.fmean(values), 3)


def time_windows_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_start = parse_loose_datetime(left.get("start_utc"))
    left_end = parse_loose_datetime(left.get("end_utc"))
    right_start = parse_loose_datetime(right.get("start_utc"))
    right_end = parse_loose_datetime(right.get("end_utc"))
    if None in {left_start, left_end, right_start, right_end}:
        return False
    assert left_start is not None
    assert left_end is not None
    assert right_start is not None
    assert right_end is not None
    return max(left_start, right_start) <= min(left_end, right_end)


def default_public_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "public_signals.sqlite"


def default_environment_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "environment_signals.sqlite"


def default_context_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived"


def shared_claims_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "claims.json"


def shared_observations_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "observations.json"


def shared_evidence_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_cards.json"


def claim_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "claim_submissions.json"


def observation_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "observation_submissions.json"


def data_readiness_report_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "data_readiness_report.json"


def matching_authorization_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_authorization.json"


def matching_result_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "matching_result.json"


def evidence_adjudication_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_adjudication.json"


def evidence_library_dir(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library"


def evidence_library_ledger_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "ledger.jsonl"


def claims_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "claims_active.json"


def observations_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "observations_active.json"


def cards_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "cards_active.json"


def isolated_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "isolated_active.json"


def remands_open_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "remands_open.json"


def library_context_path(run_dir: Path, round_id: str, role: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / f"context_{role}.json"


def role_normalized_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "normalized"


def run_manifest_path(run_dir: Path) -> Path:
    return run_dir / "run_manifest.json"


def load_or_build_manifest(run_dir: Path, mission: dict[str, Any]) -> dict[str, Any]:
    manifest_file = run_manifest_path(run_dir)
    if manifest_file.exists():
        payload = read_json(manifest_file)
        if isinstance(payload, dict):
            return payload
    return {
        "run_id": mission_run_id(mission),
        "run_dir": str(run_dir),
        "analytics_backend": "sqlite",
        "databases": {
            "public_signals": str(default_public_db_path(run_dir)),
            "environment_signals": str(default_environment_db_path(run_dir)),
        },
    }


def normalize_cache_dir(run_dir: Path) -> Path:
    return run_dir / "analytics" / "normalize_cache"


def normalize_cache_path(
    run_dir: Path,
    *,
    domain: str,
    source_skill: str,
    run_id: str,
    round_id: str,
    artifact_sha256: str,
) -> Path:
    key = stable_hash(NORMALIZE_CACHE_VERSION, domain, source_skill, run_id, round_id, artifact_sha256)
    safe_domain = re.sub(r"[^a-z0-9_-]+", "-", domain.lower())
    safe_source = re.sub(r"[^a-z0-9_-]+", "-", source_skill.lower())
    return normalize_cache_dir(run_dir) / safe_domain / f"{safe_source}_{key[:16]}.json"


def read_cache_payload(path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(path)
    if not isinstance(payload, dict):
        return None
    return payload


def write_cache_payload(path: Path, payload: dict[str, Any]) -> None:
    write_json(path, payload, pretty=False)


def load_ddl(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def init_sqlite_db(path: Path, ddl_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ddl = load_ddl(ddl_path)
    with sqlite3.connect(path) as conn:
        conn.executescript(ddl)
        conn.commit()


def emit_row_id(prefix: str, index: int) -> str:
    return f"{prefix}-{index:03d}"


def percentile95(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    position = 0.95 * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    weight = position - lower
    return lower_value + (upper_value - lower_value) * weight


def artifact_ref(signal: dict[str, Any]) -> dict[str, Any]:
    ref = {
        "source_skill": signal["source_skill"],
        "artifact_path": signal["artifact_path"],
        "record_locator": signal["record_locator"],
    }
    if signal.get("external_id"):
        ref["external_id"] = signal["external_id"]
    if signal.get("sha256"):
        ref["sha256"] = signal["sha256"]
    return ref


def load_contract_module() -> Any | None:
    if not CONTRACT_SCRIPT_PATH.exists():
        return None
    module_name = "eco_council_contract"
    spec = importlib.util.spec_from_file_location(module_name, CONTRACT_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT_MODULE = load_contract_module()
if CONTRACT_MODULE is not None and hasattr(CONTRACT_MODULE, "SCHEMA_VERSION"):
    SCHEMA_VERSION = CONTRACT_MODULE.SCHEMA_VERSION


def validate_payload(kind: str, payload: Any) -> None:
    if CONTRACT_MODULE is None:
        return
    result = CONTRACT_MODULE.validate_payload(kind, payload)
    validation = result.get("validation", {})
    if validation.get("ok"):
        return
    issue_messages = []
    for issue in validation.get("issues", [])[:5]:
        issue_messages.append(f"{issue.get('path')}: {issue.get('message')}")
    raise ValueError(f"Generated invalid {kind}: {'; '.join(issue_messages)}")


def insert_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    data = list(rows)
    if not data:
        return
    conn.executemany(sql, data)
    conn.commit()


def parse_input_specs(values: list[str]) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Invalid --input value {raw!r}. Use source-skill=/path/to/artifact.")
        source_skill, path_text = raw.split("=", 1)
        source_skill = source_skill.strip()
        path_text = path_text.strip()
        if not source_skill or not path_text:
            raise ValueError(f"Invalid --input value {raw!r}.")
        path = Path(path_text).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Input artifact does not exist: {path}")
        parsed.append((source_skill, path))
    return parsed


def semantic_fingerprint(text: str) -> str:
    cleaned = []
    token = []
    for char in text.lower():
        if char.isalnum():
            token.append(char)
            continue
        if token:
            cleaned.append("".join(token))
            token = []
    if token:
        cleaned.append("".join(token))
    filtered = [item for item in cleaned if item and item not in STOPWORDS]
    return "-".join(filtered[:12])


def claim_type_from_text(text: str) -> str:
    lowered = text.lower()
    for claim_type, keywords in CLAIM_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return claim_type
    return "other"


def candidate_statement(title: str, text: str) -> str:
    if text:
        return truncate_text(text, 420)
    return truncate_text(title, 420)


def extract_value_for_metric(observation: dict[str, Any]) -> float | None:
    statistics_obj = observation.get("statistics")
    if isinstance(statistics_obj, dict):
        for key in ("mean", "max", "p95", "min"):
            value = maybe_number(statistics_obj.get(key))
            if value is not None:
                return value
    return maybe_number(observation.get("value"))


def make_public_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    signal_kind: str,
    external_id: str,
    title: str,
    text: str,
    url: str,
    author_name: str,
    channel_name: str,
    language: str,
    query_text: str,
    published_at_utc: str | None,
    engagement: dict[str, Any],
    metadata: dict[str, Any],
    artifact_path: Path,
    record_locator: str,
    sha256_value: str,
    raw_obj: Any,
) -> dict[str, Any]:
    identity = external_id or url or f"{signal_kind}:{record_locator}"
    signal_hash = stable_hash(source_skill, identity, maybe_text(title), maybe_text(text))
    return {
        "signal_id": f"pubsig-{signal_hash[:12]}",
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "external_id": external_id,
        "title": title,
        "text": text,
        "url": url,
        "author_name": author_name,
        "channel_name": channel_name,
        "language": language,
        "query_text": query_text,
        "published_at_utc": published_at_utc,
        "captured_at_utc": utc_now_iso(),
        "engagement": engagement,
        "metadata": metadata,
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "sha256": sha256_value,
        "raw_json": raw_obj,
    }


def collect_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("records", "items", "data", "results"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
    return []


def strip_simple_html(value: str) -> str:
    return normalize_space(re.sub(r"<[^>]+>", " ", value))


def normalize_public_from_youtube_videos(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(collect_records(payload)):
        video = record.get("video")
        if not isinstance(video, dict):
            continue
        video_id = maybe_text(record.get("video_id")) or maybe_text(video.get("id"))
        title = maybe_text(video.get("title"))
        description = maybe_text(video.get("description"))
        url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="youtube-video-search",
                signal_kind="video",
                external_id=video_id,
                title=title,
                text=description,
                url=url,
                author_name=maybe_text(video.get("channel_title")),
                channel_name=maybe_text(video.get("channel_title")),
                language=maybe_text(video.get("default_language") or video.get("default_audio_language")),
                query_text=maybe_text(record.get("query")),
                published_at_utc=to_rfc3339_z(parse_loose_datetime(video.get("published_at"))),
                engagement=video.get("statistics") if isinstance(video.get("statistics"), dict) else {},
                metadata={
                    "search_match": record.get("search_match"),
                    "content_details": video.get("content_details"),
                    "status": video.get("status"),
                },
                artifact_path=path,
                record_locator=f"$[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def normalize_public_from_youtube_comments(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(collect_records(payload)):
        comment_id = maybe_text(record.get("comment_id"))
        video_id = maybe_text(record.get("video_id"))
        text = maybe_text(record.get("text_original") or record.get("text_display"))
        url = ""
        if video_id and comment_id:
            url = f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="youtube-comments-fetch",
                signal_kind=maybe_text(record.get("comment_type")) or "comment",
                external_id=comment_id,
                title=truncate_text(text, 120),
                text=text,
                url=url,
                author_name=maybe_text(record.get("author_display_name")),
                channel_name=maybe_text(record.get("channel_id")),
                language="",
                query_text=maybe_text((record.get("source") or {}).get("search_terms")),
                published_at_utc=to_rfc3339_z(parse_loose_datetime(record.get("published_at"))),
                engagement={"like_count": maybe_number(record.get("like_count"))},
                metadata={
                    "video_id": video_id,
                    "thread_id": maybe_text(record.get("thread_id")),
                    "parent_comment_id": maybe_text(record.get("parent_comment_id")),
                    "source": record.get("source"),
                },
                artifact_path=path,
                record_locator=f"$[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def bluesky_uri_to_url(uri: str, author_handle: str) -> str:
    if not uri or not author_handle:
        return ""
    parts = uri.split("/")
    post_id = parts[-1] if parts else ""
    if not post_id:
        return ""
    return f"https://bsky.app/profile/{author_handle}/post/{post_id}"


def normalize_public_from_bluesky(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("seed_posts"), list):
            seeds.extend(item for item in payload["seed_posts"] if isinstance(item, dict))
        if isinstance(payload.get("threads"), list):
            for thread in payload["threads"]:
                if not isinstance(thread, dict):
                    continue
                nodes = thread.get("nodes")
                if isinstance(nodes, list):
                    seeds.extend(node for node in nodes if isinstance(node, dict))
    elif isinstance(payload, list):
        seeds.extend(item for item in payload if isinstance(item, dict))

    signals: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, record in enumerate(seeds):
        uri = maybe_text(record.get("uri"))
        if uri and uri in seen_ids:
            continue
        if uri:
            seen_ids.add(uri)
        author_handle = maybe_text(record.get("author_handle"))
        text = maybe_text(record.get("text"))
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="bluesky-cascade-fetch",
                signal_kind="reply" if maybe_text(record.get("reply_parent_uri")) else "post",
                external_id=uri or maybe_text(record.get("cid")),
                title=truncate_text(text, 120),
                text=text,
                url=bluesky_uri_to_url(uri, author_handle),
                author_name=author_handle,
                channel_name=maybe_text(record.get("author_did")),
                language=",".join(record.get("langs", [])) if isinstance(record.get("langs"), list) else "",
                query_text="",
                published_at_utc=maybe_text(record.get("timestamp_utc")) or to_rfc3339_z(parse_loose_datetime(record.get("created_at"))),
                engagement={
                    "reply_count": maybe_number(record.get("reply_count")),
                    "repost_count": maybe_number(record.get("repost_count")),
                    "like_count": maybe_number(record.get("like_count")),
                    "quote_count": maybe_number(record.get("quote_count")),
                },
                metadata={
                    "author_did": maybe_text(record.get("author_did")),
                    "cid": maybe_text(record.get("cid")),
                    "reply_root_uri": maybe_text(record.get("reply_root_uri")),
                    "timestamp_source": maybe_text(record.get("timestamp_source")),
                },
                artifact_path=path,
                record_locator=f"$[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def extract_reggov_resource(record: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if "detail" in record:
        detail = record.get("detail")
        if isinstance(detail, dict):
            resource = detail.get("data") if isinstance(detail.get("data"), dict) else detail.get("data")
            if isinstance(resource, dict):
                return resource, {"response_url": record.get("response_url"), "validation": record.get("validation")}
    return record if "attributes" in record else None, {}


def normalize_reggov_resource(
    path: Path,
    record: dict[str, Any],
    *,
    index: int,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> dict[str, Any] | None:
    resource, metadata = extract_reggov_resource(record)
    if not isinstance(resource, dict):
        return None
    attrs = resource.get("attributes") if isinstance(resource.get("attributes"), dict) else {}
    links = resource.get("links") if isinstance(resource.get("links"), dict) else {}
    text = maybe_text(
        attrs.get("comment")
        or attrs.get("commentText")
        or attrs.get("commentOn")
        or attrs.get("title")
        or attrs.get("organization")
    )
    title = maybe_text(attrs.get("title") or attrs.get("subject") or attrs.get("organization")) or truncate_text(text, 120)
    metadata.update(
        {
            "docket_id": maybe_text(attrs.get("docketId")),
            "document_type": maybe_text(attrs.get("documentType")),
            "posted_date": maybe_text(attrs.get("postedDate")),
            "last_modified_date": maybe_text(attrs.get("lastModifiedDate")),
        }
    )
    return make_public_signal(
        run_id=run_id,
        round_id=round_id,
        source_skill=source_skill,
        signal_kind="policy-comment",
        external_id=maybe_text(resource.get("id") or record.get("comment_id")),
        title=title,
        text=text,
        url=maybe_text(links.get("self") or metadata.get("response_url")),
        author_name=maybe_text(attrs.get("organization") or attrs.get("firstName")),
        channel_name=maybe_text(attrs.get("agencyId")),
        language="",
        query_text="",
        published_at_utc=to_rfc3339_z(
            parse_loose_datetime(attrs.get("postedDate") or attrs.get("lastModifiedDate"))
        ),
        engagement={},
        metadata=metadata,
        artifact_path=path,
        record_locator=f"$[{index}]",
        sha256_value=sha256_value,
        raw_obj=record,
    )


def normalize_public_from_reggov(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(collect_records(payload)):
        normalized = normalize_reggov_resource(
            path,
            record,
            index=index,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
        if normalized is not None:
            signals.append(normalized)
    return signals


def normalize_public_from_federal_register(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        raise ValueError(
            "Federal Register raw artifact must use the canonical federal-register-doc-fetch "
            "payload with a top-level records array."
        )
    query_text = ""
    if isinstance(payload, dict):
        request_obj = payload.get("request")
        if isinstance(request_obj, dict):
            query_text = maybe_text(request_obj.get("term"))
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        agencies = record.get("agencies") if isinstance(record.get("agencies"), list) else []
        agency_names = [
            maybe_text(item.get("name") or item.get("raw_name") or item.get("slug"))
            for item in agencies
            if isinstance(item, dict) and maybe_text(item.get("name") or item.get("raw_name") or item.get("slug"))
        ]
        agency_slugs = [
            maybe_text(item.get("slug"))
            for item in agencies
            if isinstance(item, dict) and maybe_text(item.get("slug"))
        ]
        title = maybe_text(record.get("title"))
        abstract = maybe_text(record.get("abstract"))
        excerpts = strip_simple_html(maybe_text(record.get("excerpts")))
        text = abstract or excerpts or title
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="policy-document",
                external_id=maybe_text(record.get("document_number") or record.get("html_url") or index),
                title=title or maybe_text(record.get("document_number")) or "Federal Register document",
                text=text,
                url=maybe_text(record.get("html_url") or record.get("pdf_url")),
                author_name="",
                channel_name=", ".join(agency_names),
                language="",
                query_text=query_text,
                published_at_utc=to_rfc3339_z(parse_loose_datetime(record.get("publication_date"))),
                engagement={},
                metadata={
                    "type": maybe_text(record.get("type")),
                    "document_number": maybe_text(record.get("document_number")),
                    "pdf_url": maybe_text(record.get("pdf_url")),
                    "public_inspection_pdf_url": maybe_text(record.get("public_inspection_pdf_url")),
                    "agencies": agency_names,
                    "agency_slugs": agency_slugs,
                    "topics": record.get("topics") if isinstance(record.get("topics"), list) else [],
                    "docket_ids": record.get("docket_ids") if isinstance(record.get("docket_ids"), list) else [],
                    "regulation_id_numbers": record.get("regulation_id_numbers") if isinstance(record.get("regulation_id_numbers"), list) else [],
                    "significant": record.get("significant"),
                    "comment_url": maybe_text(record.get("comment_url")),
                    "raw_text_url": maybe_text(record.get("raw_text_url")),
                    "source_page_number": record.get("source_page_number"),
                },
                artifact_path=path,
                record_locator=f"$.records[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def normalize_public_from_gdelt_doc(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return signals

    if isinstance(payload.get("articles"), list):
        records = payload["articles"]
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            title = maybe_text(item.get("title"))
            description = maybe_text(item.get("seendate") or item.get("domain"))
            signals.append(
                make_public_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="gdelt-doc-search",
                    signal_kind="article",
                    external_id=maybe_text(item.get("url") or item.get("title")),
                    title=title,
                    text=title or description,
                    url=maybe_text(item.get("url")),
                    author_name="",
                    channel_name=maybe_text(item.get("domain")),
                    language=maybe_text(item.get("language") or item.get("sourcelang")),
                    query_text="",
                    published_at_utc=to_rfc3339_z(
                        parse_loose_datetime(item.get("seendate") or item.get("date"))
                    ),
                    engagement={},
                    metadata=item,
                    artifact_path=path,
                    record_locator=f"$.articles[{index}]",
                    sha256_value=sha256_value,
                    raw_obj=item,
                )
            )
        return signals

    for key in ("timeline", "data", "records"):
        candidate = payload.get(key)
        if not isinstance(candidate, list):
            continue
        for index, item in enumerate(candidate):
            if not isinstance(item, dict):
                continue
            title = maybe_text(item.get("title")) or "GDELT timeline bin"
            text = title
            if maybe_text(item.get("value")):
                text = f"{title} value={item.get('value')}"
            signals.append(
                make_public_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="gdelt-doc-search",
                    signal_kind="timeline-bin",
                    external_id=maybe_text(item.get("date") or item.get("datetime") or index),
                    title=title,
                    text=text,
                    url=maybe_text(item.get("url")),
                    author_name="",
                    channel_name="",
                    language="",
                    query_text="",
                    published_at_utc=to_rfc3339_z(
                        parse_loose_datetime(item.get("date") or item.get("datetime"))
                    ),
                    engagement={},
                    metadata=item,
                    artifact_path=path,
                    record_locator=f"$.{key}[{index}]",
                    sha256_value=sha256_value,
                    raw_obj=item,
                )
            )
        if signals:
            return signals
    return signals


def gdelt_row_value(row: list[str], index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    return maybe_text(row[index])


def iter_zip_tsv_rows(path: Path, *, max_rows: int = GDELT_SCAN_ROW_LIMIT) -> tuple[str, list[list[str]], bool]:
    with zipfile.ZipFile(path) as archive:
        member_names = [name for name in archive.namelist() if not name.endswith("/")]
        if not member_names:
            return ("", [], True)
        member_name = member_names[0]
        rows: list[list[str]] = []
        scan_complete = True
        with archive.open(member_name, "r") as raw_handle:
            with io.TextIOWrapper(raw_handle, encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle, delimiter="\t")
                for row in reader:
                    if not row:
                        continue
                    if len(rows) >= max_rows:
                        scan_complete = False
                        break
                    rows.append([maybe_text(item) for item in row])
    return (member_name, rows, scan_complete)


def manifest_download_records(payload: Any) -> list[tuple[int, dict[str, Any]]]:
    if not isinstance(payload, dict):
        return []
    downloads = payload.get("downloads")
    if not isinstance(downloads, list):
        return []
    output: list[tuple[int, dict[str, Any]]] = []
    for index, item in enumerate(downloads):
        if isinstance(item, dict):
            output.append((index, item))
    return output


def manifest_latest_timestamp(payload: Any) -> str | None:
    latest: datetime | None = None
    for _index, item in manifest_download_records(payload):
        entry = item.get("entry") if isinstance(item.get("entry"), dict) else {}
        candidate = parse_loose_datetime(entry.get("timestamp_utc") or entry.get("timestamp_raw"))
        if candidate is None:
            continue
        if latest is None or candidate > latest:
            latest = candidate
    return to_rfc3339_z(latest)


def gdelt_theme_values(value: Any) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in maybe_text(value).split(";"):
        text = maybe_text(item)
        if not text:
            continue
        primary = re.split(r"[,#]", text, maxsplit=1)[0].replace("_", " ").strip()
        normalized = maybe_text(primary)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    return output


def gdelt_name_values(value: Any) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in maybe_text(value).split(";"):
        text = maybe_text(item)
        if not text:
            continue
        primary = re.split(r"[,#]", text, maxsplit=1)[0].strip()
        normalized = maybe_text(primary)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    return output


def gdelt_gkg_locations(value: Any) -> list[dict[str, Any]]:
    locations: list[dict[str, Any]] = []
    for item in maybe_text(value).split(";"):
        text = maybe_text(item)
        if not text:
            continue
        parts = text.split("#")
        if len(parts) >= 2:
            name = maybe_text(parts[1])
            latitude = maybe_number(parts[5]) if len(parts) > 5 else None
            longitude = maybe_number(parts[6]) if len(parts) > 6 else None
        else:
            name = text
            latitude = None
            longitude = None
        locations.append({"name": name, "latitude": latitude, "longitude": longitude})
    return locations


def gdelt_first_tone(value: Any) -> float | None:
    parts = maybe_text(value).split(",")
    if not parts:
        return None
    return maybe_number(parts[0])


def push_ranked_example(bucket: list[dict[str, Any]], example: dict[str, Any]) -> None:
    bucket.append(example)
    bucket.sort(key=lambda item: item.get("_rank", (0, 0, 0, "")), reverse=True)
    del bucket[GDELT_MATCHED_ROW_STORE_LIMIT:]


def gdelt_coverage_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    path: Path,
    sha256_value: str,
    title: str,
    text: str,
    published_at_utc: str | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return make_public_signal(
        run_id=run_id,
        round_id=round_id,
        source_skill=source_skill,
        signal_kind="table-coverage",
        external_id=f"{source_skill}:coverage:{maybe_text(path.name)}",
        title=title,
        text=text,
        url="",
        author_name="",
        channel_name=source_skill,
        language="",
        query_text="",
        published_at_utc=published_at_utc,
        engagement={},
        metadata=metadata,
        artifact_path=path,
        record_locator="$.downloads[*]",
        sha256_value=sha256_value,
        raw_obj=metadata,
    )


def normalize_public_from_gdelt_events_manifest(
    path: Path,
    payload: Any,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    mission_scope = mission_place_scope(mission)
    mission_geometry = mission_scope.get("geometry") if isinstance(mission_scope.get("geometry"), dict) else {}
    region_tokens = mission_region_tokens(mission)
    topic_tokens = mission_topic_tokens(mission)
    location_counter: Counter[str] = Counter()
    event_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()
    tones: list[float] = []
    published_candidates: list[datetime] = []
    top_examples: list[dict[str, Any]] = []
    scanned_rows = 0
    matched_rows = 0
    total_mentions = 0
    total_articles = 0
    readable_files = 0
    missing_files = 0
    scan_complete = True

    for download_index, item in manifest_download_records(payload):
        output_path_text = maybe_text(item.get("output_path"))
        if not output_path_text:
            missing_files += 1
            continue
        output_path = Path(output_path_text).expanduser().resolve()
        if not output_path.exists():
            missing_files += 1
            continue
        try:
            member_name, rows, file_complete = iter_zip_tsv_rows(output_path)
        except (OSError, ValueError, zipfile.BadZipFile):
            missing_files += 1
            continue
        readable_files += 1
        scan_complete = scan_complete and file_complete
        for row_index, row in enumerate(rows):
            scanned_rows += 1
            if len(row) <= GDELT_EVENTS_INDEX["source_url"]:
                continue
            source_url = gdelt_row_value(row, GDELT_EVENTS_INDEX["source_url"])
            action_geo_name = gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_name"])
            latitude = maybe_number(gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_lat"]))
            longitude = maybe_number(gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_lon"]))
            token_set = row_token_set(
                gdelt_row_value(row, GDELT_EVENTS_INDEX["actor1_name"]),
                gdelt_row_value(row, GDELT_EVENTS_INDEX["actor2_name"]),
                action_geo_name,
                source_url,
                minimum_length=3,
            )
            region_match = point_matches_geometry(latitude, longitude, mission_geometry) or any(
                token in token_set for token in region_tokens
            )
            topic_hits = sum(1 for token in topic_tokens if token in token_set)
            if not region_match or (topic_tokens and topic_hits == 0):
                continue

            matched_rows += 1
            event_code = gdelt_row_value(row, GDELT_EVENTS_INDEX["event_base_code"]) or gdelt_row_value(
                row, GDELT_EVENTS_INDEX["event_code"]
            )
            domain = source_domain(source_url)
            mentions = int(maybe_number(gdelt_row_value(row, GDELT_EVENTS_INDEX["num_mentions"])) or 0)
            articles = int(maybe_number(gdelt_row_value(row, GDELT_EVENTS_INDEX["num_articles"])) or 0)
            tone = maybe_number(gdelt_row_value(row, GDELT_EVENTS_INDEX["avg_tone"]))
            published_at = to_rfc3339_z(
                parse_loose_datetime(gdelt_row_value(row, GDELT_EVENTS_INDEX["date_added"]))
                or parse_loose_datetime(gdelt_row_value(row, GDELT_EVENTS_INDEX["sql_date"]))
            )
            published_dt = parse_loose_datetime(published_at)
            if published_dt is not None:
                published_candidates.append(published_dt)
            location_counter[action_geo_name or "unknown"] += 1
            event_counter[event_code or "unknown"] += 1
            if domain:
                domain_counter[domain] += 1
            total_mentions += mentions
            total_articles += articles
            if tone is not None:
                tones.append(tone)

            example_title = action_geo_name or "Mission-aligned GDELT event"
            example_text = normalize_space(
                " ".join(
                    part
                    for part in (
                        f"event_code={event_code}" if event_code else "",
                        gdelt_row_value(row, GDELT_EVENTS_INDEX["actor1_name"]),
                        gdelt_row_value(row, GDELT_EVENTS_INDEX["actor2_name"]),
                        f"mentions={mentions}",
                        f"articles={articles}",
                        f"tone={tone}" if tone is not None else "",
                    )
                    if part
                )
            )
            push_ranked_example(
                top_examples,
                {
                    "_rank": (topic_hits, mentions, articles, published_at or ""),
                    "title": f"GDELT event at {example_title}",
                    "text": example_text,
                    "url": source_url,
                    "channel_name": domain,
                    "published_at_utc": published_at,
                    "record_locator": f"$.downloads[{download_index}].{member_name}[{row_index}]",
                    "metadata": {
                        "download_output_path": str(output_path),
                        "zip_member": member_name,
                        "event_id": gdelt_row_value(row, GDELT_EVENTS_INDEX["event_id"]),
                        "event_code": gdelt_row_value(row, GDELT_EVENTS_INDEX["event_code"]),
                        "event_base_code": event_code,
                        "event_root_code": gdelt_row_value(row, GDELT_EVENTS_INDEX["event_root_code"]),
                        "action_geo_name": action_geo_name,
                        "action_geo_country": gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_country"]),
                        "num_mentions": mentions,
                        "num_sources": int(maybe_number(gdelt_row_value(row, GDELT_EVENTS_INDEX["num_sources"])) or 0),
                        "num_articles": articles,
                        "avg_tone": tone,
                    },
                    "raw_json": {
                        "event_id": gdelt_row_value(row, GDELT_EVENTS_INDEX["event_id"]),
                        "sql_date": gdelt_row_value(row, GDELT_EVENTS_INDEX["sql_date"]),
                        "actor1_name": gdelt_row_value(row, GDELT_EVENTS_INDEX["actor1_name"]),
                        "actor2_name": gdelt_row_value(row, GDELT_EVENTS_INDEX["actor2_name"]),
                        "event_code": gdelt_row_value(row, GDELT_EVENTS_INDEX["event_code"]),
                        "event_base_code": event_code,
                        "event_root_code": gdelt_row_value(row, GDELT_EVENTS_INDEX["event_root_code"]),
                        "action_geo_name": action_geo_name,
                        "action_geo_lat": latitude,
                        "action_geo_lon": longitude,
                        "source_url": source_url,
                    },
                },
            )

    coverage_metadata = {
        "matched_row_count": matched_rows,
        "scanned_row_count": scanned_rows,
        "readable_file_count": readable_files,
        "missing_file_count": missing_files,
        "scan_complete": scan_complete,
        "total_mentions": total_mentions,
        "total_articles": total_articles,
        "avg_tone": maybe_mean(tones),
        "top_locations": top_counter_items(location_counter),
        "top_event_codes": top_counter_items(event_counter),
        "top_domains": top_counter_items(domain_counter),
        "region_tokens": region_tokens,
        "topic_tokens": topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} event rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top locations: {top_counter_text(location_counter) or 'n/a'}. "
        f"Top event codes: {top_counter_text(event_counter) or 'n/a'}. "
        f"Top domains: {top_counter_text(domain_counter) or 'n/a'}."
    )
    coverage_published_at = manifest_latest_timestamp(payload)
    if coverage_published_at is None and published_candidates:
        coverage_published_at = to_rfc3339_z(max(published_candidates))
    signals = [
        gdelt_coverage_signal(
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            path=path,
            sha256_value=sha256_value,
            title="GDELT Events table coverage",
            text=coverage_text,
            published_at_utc=coverage_published_at,
            metadata=coverage_metadata,
        )
    ]
    for example_index, example in enumerate(top_examples[:GDELT_EXAMPLE_SIGNAL_LIMIT]):
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="event-record",
                external_id=f"{source_skill}:{example_index}:{maybe_text(example['record_locator'])}",
                title=maybe_text(example.get("title")),
                text=maybe_text(example.get("text")),
                url=maybe_text(example.get("url")),
                author_name="",
                channel_name=maybe_text(example.get("channel_name")),
                language="",
                query_text=maybe_text(mission.get("topic")),
                published_at_utc=example.get("published_at_utc"),
                engagement={},
                metadata=example.get("metadata") if isinstance(example.get("metadata"), dict) else {},
                artifact_path=path,
                record_locator=maybe_text(example.get("record_locator")),
                sha256_value=sha256_value,
                raw_obj=example.get("raw_json"),
            )
        )
    return signals


def normalize_public_from_gdelt_mentions_manifest(
    path: Path,
    payload: Any,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    region_tokens = mission_region_tokens(mission)
    topic_tokens = mission_topic_tokens(mission)
    source_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()
    event_counter: Counter[str] = Counter()
    confidence_values: list[float] = []
    tone_values: list[float] = []
    top_examples: list[dict[str, Any]] = []
    scanned_rows = 0
    matched_rows = 0
    readable_files = 0
    missing_files = 0
    scan_complete = True

    for download_index, item in manifest_download_records(payload):
        output_path_text = maybe_text(item.get("output_path"))
        if not output_path_text:
            missing_files += 1
            continue
        output_path = Path(output_path_text).expanduser().resolve()
        if not output_path.exists():
            missing_files += 1
            continue
        try:
            member_name, rows, file_complete = iter_zip_tsv_rows(output_path)
        except (OSError, ValueError, zipfile.BadZipFile):
            missing_files += 1
            continue
        readable_files += 1
        scan_complete = scan_complete and file_complete
        for row_index, row in enumerate(rows):
            scanned_rows += 1
            if len(row) <= GDELT_MENTIONS_INDEX["doc_tone"]:
                continue
            source_name = gdelt_row_value(row, GDELT_MENTIONS_INDEX["source_name"])
            identifier = gdelt_row_value(row, GDELT_MENTIONS_INDEX["identifier"])
            token_set = row_token_set(source_name, identifier, minimum_length=3)
            region_match = any(token in token_set for token in region_tokens)
            topic_hits = sum(1 for token in topic_tokens if token in token_set)
            if not region_match or (topic_tokens and topic_hits == 0):
                continue

            matched_rows += 1
            domain = source_domain(identifier)
            confidence = maybe_number(gdelt_row_value(row, GDELT_MENTIONS_INDEX["confidence"]))
            tone = maybe_number(gdelt_row_value(row, GDELT_MENTIONS_INDEX["doc_tone"]))
            published_at = to_rfc3339_z(parse_loose_datetime(gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_time"])))
            source_counter[source_name or "unknown"] += 1
            event_counter[gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_id"]) or "unknown"] += 1
            if domain:
                domain_counter[domain] += 1
            if confidence is not None:
                confidence_values.append(confidence)
            if tone is not None:
                tone_values.append(tone)

            push_ranked_example(
                top_examples,
                {
                    "_rank": (
                        topic_hits,
                        int(confidence or 0),
                        int(maybe_number(gdelt_row_value(row, GDELT_MENTIONS_INDEX["doc_length"])) or 0),
                        published_at or "",
                    ),
                    "title": f"GDELT mention from {source_name or domain or 'unknown source'}",
                    "text": normalize_space(
                        " ".join(
                            part
                            for part in (
                                f"mention_type={gdelt_row_value(row, GDELT_MENTIONS_INDEX['mention_type'])}",
                                f"confidence={confidence}" if confidence is not None else "",
                                f"tone={tone}" if tone is not None else "",
                                identifier,
                            )
                            if part
                        )
                    ),
                    "url": identifier,
                    "channel_name": domain or source_name,
                    "published_at_utc": published_at,
                    "record_locator": f"$.downloads[{download_index}].{member_name}[{row_index}]",
                    "metadata": {
                        "download_output_path": str(output_path),
                        "zip_member": member_name,
                        "event_id": gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_id"]),
                        "mention_type": gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_type"]),
                        "source_name": source_name,
                        "confidence": confidence,
                        "doc_length": maybe_number(gdelt_row_value(row, GDELT_MENTIONS_INDEX["doc_length"])),
                        "doc_tone": tone,
                    },
                    "raw_json": {
                        "event_id": gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_id"]),
                        "event_time": gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_time"]),
                        "mention_time": gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_time"]),
                        "mention_type": gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_type"]),
                        "source_name": source_name,
                        "identifier": identifier,
                    },
                },
            )

    coverage_metadata = {
        "matched_row_count": matched_rows,
        "scanned_row_count": scanned_rows,
        "readable_file_count": readable_files,
        "missing_file_count": missing_files,
        "scan_complete": scan_complete,
        "avg_confidence": maybe_mean(confidence_values),
        "avg_tone": maybe_mean(tone_values),
        "top_sources": top_counter_items(source_counter),
        "top_domains": top_counter_items(domain_counter),
        "top_event_ids": top_counter_items(event_counter),
        "region_tokens": region_tokens,
        "topic_tokens": topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} mention rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top sources: {top_counter_text(source_counter) or 'n/a'}. "
        f"Top domains: {top_counter_text(domain_counter) or 'n/a'}."
    )
    signals = [
        gdelt_coverage_signal(
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            path=path,
            sha256_value=sha256_value,
            title="GDELT Mentions table coverage",
            text=coverage_text,
            published_at_utc=manifest_latest_timestamp(payload),
            metadata=coverage_metadata,
        )
    ]
    for example_index, example in enumerate(top_examples[:GDELT_EXAMPLE_SIGNAL_LIMIT]):
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="mention-record",
                external_id=f"{source_skill}:{example_index}:{maybe_text(example['record_locator'])}",
                title=maybe_text(example.get("title")),
                text=maybe_text(example.get("text")),
                url=maybe_text(example.get("url")),
                author_name="",
                channel_name=maybe_text(example.get("channel_name")),
                language="",
                query_text=maybe_text(mission.get("topic")),
                published_at_utc=example.get("published_at_utc"),
                engagement={},
                metadata=example.get("metadata") if isinstance(example.get("metadata"), dict) else {},
                artifact_path=path,
                record_locator=maybe_text(example.get("record_locator")),
                sha256_value=sha256_value,
                raw_obj=example.get("raw_json"),
            )
        )
    return signals


def normalize_public_from_gdelt_gkg_manifest(
    path: Path,
    payload: Any,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    mission_scope = mission_place_scope(mission)
    mission_geometry = mission_scope.get("geometry") if isinstance(mission_scope.get("geometry"), dict) else {}
    region_tokens = mission_region_tokens(mission)
    topic_tokens = mission_topic_tokens(mission)
    theme_counter: Counter[str] = Counter()
    location_counter: Counter[str] = Counter()
    organization_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()
    tone_values: list[float] = []
    top_examples: list[dict[str, Any]] = []
    scanned_rows = 0
    matched_rows = 0
    readable_files = 0
    missing_files = 0
    scan_complete = True

    for download_index, item in manifest_download_records(payload):
        output_path_text = maybe_text(item.get("output_path"))
        if not output_path_text:
            missing_files += 1
            continue
        output_path = Path(output_path_text).expanduser().resolve()
        if not output_path.exists():
            missing_files += 1
            continue
        try:
            member_name, rows, file_complete = iter_zip_tsv_rows(output_path)
        except (OSError, ValueError, zipfile.BadZipFile):
            missing_files += 1
            continue
        readable_files += 1
        scan_complete = scan_complete and file_complete
        for row_index, row in enumerate(rows):
            scanned_rows += 1
            if len(row) <= GDELT_GKG_INDEX["all_names"]:
                continue
            document_identifier = gdelt_row_value(row, GDELT_GKG_INDEX["document_identifier"])
            source_common_name = gdelt_row_value(row, GDELT_GKG_INDEX["source_common_name"])
            themes = gdelt_theme_values(gdelt_row_value(row, GDELT_GKG_INDEX["themes"]))
            locations = gdelt_gkg_locations(gdelt_row_value(row, GDELT_GKG_INDEX["locations"]))
            organizations = gdelt_name_values(gdelt_row_value(row, GDELT_GKG_INDEX["organizations"]))
            persons = gdelt_name_values(gdelt_row_value(row, GDELT_GKG_INDEX["persons"]))
            token_set = row_token_set(
                source_common_name,
                document_identifier,
                " ".join(themes),
                " ".join(location.get("name", "") for location in locations),
                " ".join(organizations),
                " ".join(persons),
                minimum_length=3,
            )
            region_match = any(
                point_matches_geometry(location.get("latitude"), location.get("longitude"), mission_geometry)
                for location in locations
            ) or any(token in token_set for token in region_tokens)
            topic_hits = sum(1 for token in topic_tokens if token in token_set)
            if not region_match or (topic_tokens and topic_hits == 0):
                continue

            matched_rows += 1
            domain = source_domain(document_identifier)
            tone = gdelt_first_tone(gdelt_row_value(row, GDELT_GKG_INDEX["tone"]))
            published_at = to_rfc3339_z(parse_loose_datetime(gdelt_row_value(row, GDELT_GKG_INDEX["date"])))
            for value in themes[:5]:
                theme_counter[value] += 1
            for value in organizations[:5]:
                organization_counter[value] += 1
            for location in locations[:5]:
                name = maybe_text(location.get("name"))
                if name:
                    location_counter[name] += 1
            if domain:
                domain_counter[domain] += 1
            if tone is not None:
                tone_values.append(tone)

            push_ranked_example(
                top_examples,
                {
                    "_rank": (topic_hits, len(themes), len(organizations), published_at or ""),
                    "title": f"GDELT GKG document from {source_common_name or domain or 'unknown source'}",
                    "text": normalize_space(
                        " ".join(
                            part
                            for part in (
                                f"themes={', '.join(themes[:3])}" if themes else "",
                                f"locations={', '.join(maybe_text(item.get('name')) for item in locations[:3] if maybe_text(item.get('name')))}"
                                if locations
                                else "",
                                f"organizations={', '.join(organizations[:3])}" if organizations else "",
                                f"tone={tone}" if tone is not None else "",
                            )
                            if part
                        )
                    ),
                    "url": document_identifier,
                    "channel_name": domain or source_common_name,
                    "published_at_utc": published_at,
                    "record_locator": f"$.downloads[{download_index}].{member_name}[{row_index}]",
                    "metadata": {
                        "download_output_path": str(output_path),
                        "zip_member": member_name,
                        "record_id": gdelt_row_value(row, GDELT_GKG_INDEX["record_id"]),
                        "source_common_name": source_common_name,
                        "themes": themes[:6],
                        "locations": [maybe_text(item.get("name")) for item in locations[:6] if maybe_text(item.get("name"))],
                        "organizations": organizations[:6],
                        "persons": persons[:6],
                        "tone": tone,
                    },
                    "raw_json": {
                        "record_id": gdelt_row_value(row, GDELT_GKG_INDEX["record_id"]),
                        "date": gdelt_row_value(row, GDELT_GKG_INDEX["date"]),
                        "source_common_name": source_common_name,
                        "document_identifier": document_identifier,
                        "themes": themes[:8],
                        "locations": locations[:8],
                        "organizations": organizations[:8],
                        "persons": persons[:8],
                    },
                },
            )

    coverage_metadata = {
        "matched_row_count": matched_rows,
        "scanned_row_count": scanned_rows,
        "readable_file_count": readable_files,
        "missing_file_count": missing_files,
        "scan_complete": scan_complete,
        "avg_tone": maybe_mean(tone_values),
        "top_themes": top_counter_items(theme_counter),
        "top_locations": top_counter_items(location_counter),
        "top_organizations": top_counter_items(organization_counter),
        "top_domains": top_counter_items(domain_counter),
        "region_tokens": region_tokens,
        "topic_tokens": topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} GKG rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top themes: {top_counter_text(theme_counter) or 'n/a'}. "
        f"Top locations: {top_counter_text(location_counter) or 'n/a'}."
    )
    signals = [
        gdelt_coverage_signal(
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            path=path,
            sha256_value=sha256_value,
            title="GDELT GKG table coverage",
            text=coverage_text,
            published_at_utc=manifest_latest_timestamp(payload),
            metadata=coverage_metadata,
        )
    ]
    for example_index, example in enumerate(top_examples[:GDELT_EXAMPLE_SIGNAL_LIMIT]):
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="gkg-record",
                external_id=f"{source_skill}:{example_index}:{maybe_text(example['record_locator'])}",
                title=maybe_text(example.get("title")),
                text=maybe_text(example.get("text")),
                url=maybe_text(example.get("url")),
                author_name="",
                channel_name=maybe_text(example.get("channel_name")),
                language="",
                query_text=maybe_text(mission.get("topic")),
                published_at_utc=example.get("published_at_utc"),
                engagement={},
                metadata=example.get("metadata") if isinstance(example.get("metadata"), dict) else {},
                artifact_path=path,
                record_locator=maybe_text(example.get("record_locator")),
                sha256_value=sha256_value,
                raw_obj=example.get("raw_json"),
            )
        )
    return signals


def normalize_public_from_gdelt_manifest(
    path: Path,
    payload: Any,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    if source_skill == "gdelt-events-fetch":
        return normalize_public_from_gdelt_events_manifest(
            path,
            payload,
            mission=mission,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    if source_skill == "gdelt-mentions-fetch":
        return normalize_public_from_gdelt_mentions_manifest(
            path,
            payload,
            mission=mission,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    return normalize_public_from_gdelt_gkg_manifest(
        path,
        payload,
        mission=mission,
        run_id=run_id,
        round_id=round_id,
        source_skill=source_skill,
        sha256_value=sha256_value,
    )


def normalize_public_source(
    source_skill: str,
    path: Path,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    sha256_value = file_sha256(path)
    payload = parse_path_payload(path)
    if source_skill == "youtube-video-search":
        return normalize_public_from_youtube_videos(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill == "youtube-comments-fetch":
        return normalize_public_from_youtube_comments(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill == "bluesky-cascade-fetch":
        return normalize_public_from_bluesky(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill == "federal-register-doc-fetch":
        return normalize_public_from_federal_register(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    if source_skill in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}:
        return normalize_public_from_reggov(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    if source_skill == "gdelt-doc-search":
        return normalize_public_from_gdelt_doc(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill in {"gdelt-events-fetch", "gdelt-mentions-fetch", "gdelt-gkg-fetch"}:
        return normalize_public_from_gdelt_manifest(
            path,
            payload,
            mission=mission,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    raise ValueError(f"Unsupported public source skill: {source_skill}")


def normalize_public_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], str]:
    artifact_sha256 = file_sha256(path)
    cache_path = normalize_cache_path(
        run_dir,
        domain="public",
        source_skill=source_skill,
        run_id=run_id,
        round_id=round_id,
        artifact_sha256=artifact_sha256,
    )
    cached = read_cache_payload(cache_path)
    if isinstance(cached, dict):
        signals = cached.get("signals")
        if (
            cached.get("cache_version") == NORMALIZE_CACHE_VERSION
            and cached.get("artifact_sha256") == artifact_sha256
            and isinstance(signals, list)
        ):
            return [item for item in signals if isinstance(item, dict)], "hit"

    signals = normalize_public_source(source_skill, path, mission=mission, run_id=run_id, round_id=round_id)
    write_cache_payload(
        cache_path,
        {
            "cache_version": NORMALIZE_CACHE_VERSION,
            "domain": "public",
            "source_skill": source_skill,
            "run_id": run_id,
            "round_id": round_id,
            "artifact_path": str(path),
            "artifact_sha256": artifact_sha256,
            "signals": signals,
        },
    )
    return signals, "miss"


def build_compact_audit(
    *,
    total_candidate_count: int,
    retained_count: int,
    coverage_summary: str,
    concentration_flags: list[str] | None = None,
    sampling_notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "representative": not bool(concentration_flags),
        "retained_count": max(0, int(retained_count)),
        "total_candidate_count": max(0, int(total_candidate_count)),
        "coverage_summary": coverage_summary,
        "concentration_flags": [maybe_text(item) for item in (concentration_flags or []) if maybe_text(item)],
        "sampling_notes": [maybe_text(item) for item in (sampling_notes or []) if maybe_text(item)],
    }


def public_group_compact_audit(items: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts = Counter(maybe_text(item.get("source_skill")) for item in items)
    top_source = source_counts.most_common(1)[0] if source_counts else None
    concentration_flags: list[str] = []
    if top_source is not None and len(items) > 0 and top_source[1] / len(items) >= 0.8:
        concentration_flags.append(f"Public evidence is highly concentrated in {top_source[0]}.")
    return build_compact_audit(
        total_candidate_count=len(items),
        retained_count=min(len(items), 8),
        coverage_summary=(
            f"Retained {min(len(items), 8)} references from {len(items)} supporting public signals "
            f"across {len(source_counts)} source skills."
        ),
        concentration_flags=concentration_flags,
        sampling_notes=[
            f"Dominant source skills: {top_counter_text(source_counts, limit=3)}" if source_counts else "No dominant sources recorded.",
        ],
    )


def observation_group_compact_audit(group: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts = Counter(maybe_text(item.get("source_skill")) for item in group)
    concentration_flags: list[str] = []
    if len(source_counts) == 1:
        only_source = next(iter(source_counts)) if source_counts else "unknown"
        concentration_flags.append(f"Observation summary currently depends on a single source skill: {only_source}.")
    return build_compact_audit(
        total_candidate_count=len(group),
        retained_count=1,
        coverage_summary=(
            f"Aggregated {len(group)} raw environment signals into one canonical observation."
        ),
        concentration_flags=concentration_flags,
        sampling_notes=[
            f"Dominant source skills: {top_counter_text(source_counts, limit=3)}" if source_counts else "No dominant sources recorded.",
        ],
    )


def public_signals_to_claims(
    *,
    mission: dict[str, Any],
    round_id: str,
    signals: list[dict[str, Any]],
    max_claims: int,
) -> list[dict[str, Any]]:
    run_id = mission_run_id(mission)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        if maybe_text(signal.get("signal_kind")) in NON_CLAIM_PUBLIC_SIGNAL_KINDS:
            continue
        source_text = normalize_space(
            " ".join(
                part
                for part in (
                    maybe_text(signal.get("title")),
                    maybe_text(signal.get("text")),
                )
                if part
            )
        )
        if not source_text:
            continue
        claim_type = claim_type_from_text(source_text)
        if claim_type == "other":
            continue
        fingerprint = semantic_fingerprint(source_text)
        if not fingerprint:
            fingerprint = signal["signal_id"]
        groups[f"{claim_type}|{fingerprint}"].append(signal)

    ranked = sorted(
        groups.values(),
        key=lambda items: (
            -len(items),
            -(parse_loose_datetime(items[0].get("published_at_utc")) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp(),
            items[0]["signal_id"],
        ),
    )

    claims: list[dict[str, Any]] = []
    place_scope = mission_place_scope(mission)
    time_window = mission_window(mission)
    for index, items in enumerate(ranked[:max_claims], start=1):
        lead = items[0]
        combined_text = maybe_text(lead.get("text")) or maybe_text(lead.get("title"))
        summary = truncate_text(maybe_text(lead.get("title")) or combined_text, 180)
        claim_type = claim_type_from_text(summary + " " + combined_text)
        claim = {
            "schema_version": SCHEMA_VERSION,
            "claim_id": emit_row_id("claim", index),
            "run_id": run_id,
            "round_id": round_id,
            "agent_role": "sociologist",
            "claim_type": claim_type,
            "status": "candidate",
            "summary": summary or f"Candidate claim from {lead['source_skill']}",
            "statement": candidate_statement(summary, combined_text or summary),
            "priority": min(index, 5),
            "needs_physical_validation": claim_type in PHYSICAL_CLAIM_TYPES,
            "time_window": time_window,
            "place_scope": place_scope,
            "public_refs": [artifact_ref(item) for item in items[:8]],
            "source_signal_count": len(items),
            "compact_audit": public_group_compact_audit(items),
        }
        validate_payload("claim", claim)
        claims.append(claim)
    return claims


def save_public_db(db_path: Path, signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> None:
    init_sqlite_db(db_path, PUBLIC_DDL_PATH)
    with sqlite3.connect(db_path) as conn:
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO public_signals (
                signal_id, run_id, round_id, source_skill, signal_kind, external_id, title, text,
                url, author_name, channel_name, language, query_text, published_at_utc,
                captured_at_utc, engagement_json, metadata_json, artifact_path, record_locator,
                sha256, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    signal["signal_id"],
                    signal["run_id"],
                    signal["round_id"],
                    signal["source_skill"],
                    signal["signal_kind"],
                    signal["external_id"],
                    signal["title"],
                    signal["text"],
                    signal["url"],
                    signal["author_name"],
                    signal["channel_name"],
                    signal["language"],
                    signal["query_text"],
                    signal["published_at_utc"],
                    signal["captured_at_utc"],
                    json.dumps(signal.get("engagement", {}), ensure_ascii=True, sort_keys=True),
                    json.dumps(signal.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    signal["artifact_path"],
                    signal["record_locator"],
                    signal["sha256"],
                    json.dumps(signal.get("raw_json"), ensure_ascii=True, sort_keys=True),
                )
                for signal in signals
            ),
        )
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO claim_candidates (
                claim_id, run_id, round_id, claim_type, priority, summary, statement,
                source_signal_ids_json, claim_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    claim["claim_id"],
                    claim["run_id"],
                    claim["round_id"],
                    claim["claim_type"],
                    claim["priority"],
                    claim["summary"],
                    claim["statement"],
                    json.dumps(
                        [ref.get("external_id") or ref.get("record_locator") for ref in claim.get("public_refs", [])],
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                    json.dumps(claim, ensure_ascii=True, sort_keys=True),
                )
                for claim in claims
            ),
        )


def first_datetime_and_last(values: list[dict[str, Any]]) -> tuple[str, str] | None:
    datetimes: list[datetime] = []
    for item in values:
        observed = parse_loose_datetime(item.get("observed_at_utc") or item.get("window_start_utc"))
        if observed is not None:
            datetimes.append(observed)
    if not datetimes:
        return None
    datetimes.sort()
    return to_rfc3339_z(datetimes[0]) or "", to_rfc3339_z(datetimes[-1]) or ""


def aggregate_stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "max": None, "mean": None, "p95": None}
    return {
        "min": min(values),
        "max": max(values),
        "mean": statistics.fmean(values),
        "p95": percentile95(values),
    }


def make_environment_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    signal_kind: str,
    metric: str,
    value: float | None,
    unit: str,
    observed_at_utc: str | None,
    window_start_utc: str | None,
    window_end_utc: str | None,
    latitude: float | None,
    longitude: float | None,
    bbox: dict[str, Any] | None,
    quality_flags: list[str],
    metadata: dict[str, Any],
    artifact_path: Path,
    record_locator: str,
    sha256_value: str,
    raw_obj: Any,
) -> dict[str, Any]:
    canonical_metric = canonical_environment_metric(metric)
    signal_hash = stable_hash(
        source_skill,
        canonical_metric,
        observed_at_utc or window_start_utc or record_locator,
        value,
        latitude,
        longitude,
    )
    return {
        "signal_id": f"envsig-{signal_hash[:12]}",
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "metric": canonical_metric,
        "value": value,
        "unit": unit or "unknown",
        "observed_at_utc": observed_at_utc,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "latitude": latitude,
        "longitude": longitude,
        "bbox": bbox,
        "quality_flags": quality_flags,
        "metadata": metadata,
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "sha256": sha256_value,
        "raw_json": raw_obj,
    }


def open_meteo_point_scope(record: dict[str, Any], default_scope: dict[str, Any]) -> dict[str, Any]:
    lat = maybe_number(record.get("latitude"))
    lon = maybe_number(record.get("longitude"))
    if lat is None or lon is None:
        return default_scope
    return {
        "label": maybe_text(record.get("timezone")) or default_scope["label"],
        "geometry": {"type": "Point", "latitude": lat, "longitude": lon},
    }


def iter_open_meteo_signals(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    records = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        return signals
    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        latitude = maybe_number(record.get("latitude"))
        longitude = maybe_number(record.get("longitude"))
        for section_name, units_name in (("hourly", "hourly_units"), ("daily", "daily_units")):
            section = record.get(section_name)
            if not isinstance(section, dict):
                continue
            units = record.get(units_name) if isinstance(record.get(units_name), dict) else {}
            times = section.get("time") if isinstance(section.get("time"), list) else []
            for metric, series in section.items():
                if metric == "time" or not isinstance(series, list):
                    continue
                unit = maybe_text(units.get(metric)) or "unknown"
                for value_index, raw_value in enumerate(series):
                    numeric_value = maybe_number(raw_value)
                    if numeric_value is None:
                        continue
                    observed_at = parse_loose_datetime(times[value_index]) if value_index < len(times) else None
                    signals.append(
                        make_environment_signal(
                            run_id=run_id,
                            round_id=round_id,
                            source_skill=source_skill,
                            signal_kind=section_name,
                            metric=metric,
                            value=numeric_value,
                            unit=unit,
                            observed_at_utc=to_rfc3339_z(observed_at),
                            window_start_utc=None,
                            window_end_utc=None,
                            latitude=latitude,
                            longitude=longitude,
                            bbox=None,
                            quality_flags=(
                                ["modeled-background"]
                                if source_skill == "open-meteo-air-quality-fetch"
                                else ["hydrology-model"]
                                if source_skill == "open-meteo-flood-fetch"
                                else ["reanalysis-or-model"]
                            ),
                            metadata={
                                "section": section_name,
                                "timezone": maybe_text(record.get("timezone")),
                                "elevation": maybe_number(record.get("elevation")),
                                "record_index": record_index,
                            },
                            artifact_path=path,
                            record_locator=f"$.records[{record_index}].{section_name}.{metric}[{value_index}]",
                            sha256_value=sha256_value,
                            raw_obj=raw_value,
                        )
                    )
    return signals


def iter_nasa_firms_signals(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return signals
    rows = payload.get("records")
    if not isinstance(rows, list):
        return signals
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        bbox = None
        signals.append(
            make_environment_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="nasa-firms-fire-fetch",
                signal_kind="fire-detection",
                metric="fire_detection",
                value=1.0,
                unit="count",
                observed_at_utc=maybe_text(row.get("_acquired_at_utc")),
                window_start_utc=maybe_text(row.get("_chunk_start_date")),
                window_end_utc=maybe_text(row.get("_chunk_end_date")),
                latitude=maybe_number(row.get("_latitude")),
                longitude=maybe_number(row.get("_longitude")),
                bbox=bbox,
                quality_flags=["satellite-detection"],
                metadata={
                    "confidence": maybe_text(row.get("confidence")),
                    "satellite": maybe_text(row.get("satellite")),
                    "instrument": maybe_text(row.get("instrument")),
                    "frp": maybe_number(row.get("frp")),
                },
                artifact_path=path,
                record_locator=f"$.records[{index}]",
                sha256_value=sha256_value,
                raw_obj=row,
            )
        )
    return signals


def unwrap_openaq_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and "result" in payload:
        return unwrap_openaq_payload(payload["result"])
    return payload


def extract_nested_value(row: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        current: Any = row
        ok = True
        for part in path.split("."):
            if not isinstance(current, dict):
                ok = False
                break
            current = current.get(part)
        if ok and current is not None:
            return current
    return None


def openaq_row_to_signal(
    row: dict[str, Any],
    *,
    path: Path,
    run_id: str,
    round_id: str,
    index: int,
    sha256_value: str,
) -> dict[str, Any] | None:
    metric = maybe_text(
        extract_nested_value(row, "parameter.name", "parameter", "parameterName", "metric", "name")
    )
    unit = maybe_text(extract_nested_value(row, "parameter.units", "unit", "units")) or "unknown"
    value = None
    for key in OPENAQ_VALUE_KEYS:
        value = maybe_number(extract_nested_value(row, key))
        if value is not None:
            break
    if value is None or not metric:
        return None
    timestamp_text = ""
    timestamp_candidate = extract_nested_value(row, "date.utc", "date.local")
    if timestamp_candidate is None:
        for key in OPENAQ_TIME_KEYS:
            timestamp_candidate = extract_nested_value(row, key)
            if timestamp_candidate is not None:
                break
    if timestamp_candidate is not None:
        timestamp_text = maybe_text(timestamp_candidate)
    coordinates = row.get("coordinates") if isinstance(row.get("coordinates"), dict) else {}
    latitude = maybe_number(coordinates.get("latitude"))
    longitude = maybe_number(coordinates.get("longitude"))
    if latitude is None:
        for key in OPENAQ_LAT_KEYS:
            latitude = maybe_number(extract_nested_value(row, key))
            if latitude is not None:
                break
    if longitude is None:
        for key in OPENAQ_LON_KEYS:
            longitude = maybe_number(extract_nested_value(row, key))
            if longitude is not None:
                break
    metadata = {
        "location_id": extract_nested_value(row, "location.id", "locationId", "locationsId"),
        "location_name": maybe_text(extract_nested_value(row, "location.name", "location")),
        "sensor_id": extract_nested_value(row, "sensor.id", "sensorId", "sensorsId"),
        "provider": maybe_text(extract_nested_value(row, "provider.name", "provider")),
    }
    return make_environment_signal(
        run_id=run_id,
        round_id=round_id,
        source_skill="openaq-data-fetch",
        signal_kind="station-measurement",
        metric=metric,
        value=value,
        unit=unit,
        observed_at_utc=to_rfc3339_z(parse_loose_datetime(timestamp_text)),
        window_start_utc=None,
        window_end_utc=None,
        latitude=latitude,
        longitude=longitude,
        bbox=None,
        quality_flags=["station-observation"],
        metadata=metadata,
        artifact_path=path,
        record_locator=f"$[{index}]",
        sha256_value=sha256_value,
        raw_obj=row,
    )


def iter_csv_rows(path: Path) -> list[dict[str, str]]:
    open_func = gzip.open if path.suffix.lower() == ".gz" else open
    with open_func(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def iter_openaq_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    sha256_value = file_sha256(path)
    suffix = path.suffix.lower()
    rows: list[dict[str, Any]] = []
    if suffix in {".json", ".jsonl"}:
        payload = unwrap_openaq_payload(parse_path_payload(path))
        rows = collect_records(payload)
        if not rows and isinstance(payload, dict):
            output_path = maybe_text(payload.get("output_path"))
            if output_path:
                nested_path = Path(output_path).expanduser().resolve()
                if nested_path.exists():
                    return iter_openaq_signals(nested_path, run_id=run_id, round_id=round_id)
    elif suffix in {".csv", ".gz"}:
        rows = iter_csv_rows(path)
    else:
        raise ValueError(f"Unsupported OpenAQ artifact path: {path}")

    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        normalized = openaq_row_to_signal(
            row,
            path=path,
            run_id=run_id,
            round_id=round_id,
            index=index,
            sha256_value=sha256_value,
        )
        if normalized is not None:
            signals.append(normalized)
    return signals


def iter_airnow_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = parse_path_payload(path)
    rows = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []

    sha256_value = file_sha256(path)
    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        parameter_name = maybe_text(row.get("parameter_name")).upper()
        metric_base = AIRNOW_PARAMETER_METRIC_MAP.get(parameter_name)
        if not metric_base:
            continue
        latitude = maybe_number(row.get("latitude"))
        longitude = maybe_number(row.get("longitude"))
        observed_at_utc = to_rfc3339_z(parse_loose_datetime(row.get("observed_at_utc")))
        metadata = {
            "aqsid": maybe_text(row.get("aqsid")),
            "site_name": maybe_text(row.get("site_name")),
            "status": maybe_text(row.get("status")),
            "epa_region": maybe_text(row.get("epa_region")),
            "country_code": maybe_text(row.get("country_code")),
            "state_name": maybe_text(row.get("state_name")),
            "data_source": maybe_text(row.get("data_source")),
            "reporting_areas": row.get("reporting_areas") if isinstance(row.get("reporting_areas"), list) else [],
            "aqi_kind": maybe_text(row.get("aqi_kind")),
            "measured": row.get("measured"),
            "source_file_url": maybe_text(row.get("source_file_url")),
        }
        raw_concentration = maybe_number(row.get("raw_concentration"))
        if raw_concentration is not None:
            signals.append(
                make_environment_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="airnow-hourly-obs-fetch",
                    signal_kind="station-measurement",
                    metric=metric_base,
                    value=raw_concentration,
                    unit=maybe_text(row.get("unit")) or "unknown",
                    observed_at_utc=observed_at_utc,
                    window_start_utc=None,
                    window_end_utc=None,
                    latitude=latitude,
                    longitude=longitude,
                    bbox=None,
                    quality_flags=["station-observation", "preliminary", "airnow-file-product"],
                    metadata=metadata,
                    artifact_path=path,
                    record_locator=f"$.records[{index}].raw_concentration",
                    sha256_value=sha256_value,
                    raw_obj=row,
                )
            )
        aqi_value = maybe_number(row.get("aqi_value"))
        if aqi_value is not None:
            signals.append(
                make_environment_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="airnow-hourly-obs-fetch",
                    signal_kind="station-aqi",
                    metric=f"{metric_base}_aqi",
                    value=aqi_value,
                    unit="AQI",
                    observed_at_utc=observed_at_utc,
                    window_start_utc=None,
                    window_end_utc=None,
                    latitude=latitude,
                    longitude=longitude,
                    bbox=None,
                    quality_flags=["station-aqi", "preliminary", "airnow-file-product"],
                    metadata=metadata,
                    artifact_path=path,
                    record_locator=f"$.records[{index}].aqi_value",
                    sha256_value=sha256_value,
                    raw_obj=row,
                )
            )
    return signals


def iter_usgs_water_iv_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = parse_path_payload(path)
    rows = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []

    sha256_value = file_sha256(path)
    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        parameter_code = maybe_text(row.get("parameter_code"))
        metric = USGS_PARAMETER_METRIC_MAP.get(parameter_code)
        if not metric:
            continue
        value = maybe_number(row.get("value"))
        if value is None:
            continue
        latitude = maybe_number(row.get("latitude"))
        longitude = maybe_number(row.get("longitude"))
        observed_at_utc = to_rfc3339_z(parse_loose_datetime(row.get("observed_at_utc")))
        quality_flags = ["station-observation", "usgs-water-services-iv"]
        if bool(row.get("provisional")):
            quality_flags.append("provisional")
        metadata = {
            "site_number": maybe_text(row.get("site_number")),
            "site_name": maybe_text(row.get("site_name")),
            "agency_code": maybe_text(row.get("agency_code")),
            "site_type": maybe_text(row.get("site_type")),
            "state_code": maybe_text(row.get("state_code")),
            "county_code": maybe_text(row.get("county_code")),
            "huc_code": maybe_text(row.get("huc_code")),
            "parameter_code": parameter_code,
            "variable_name": maybe_text(row.get("variable_name")),
            "variable_description": maybe_text(row.get("variable_description")),
            "statistic_code": maybe_text(row.get("statistic_code")),
            "qualifiers": row.get("qualifiers") if isinstance(row.get("qualifiers"), list) else [],
            "source_query_url": maybe_text(row.get("source_query_url")),
        }
        signals.append(
            make_environment_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="usgs-water-iv-fetch",
                signal_kind="station-measurement",
                metric=metric,
                value=value,
                unit=maybe_text(row.get("unit")) or "unknown",
                observed_at_utc=observed_at_utc,
                window_start_utc=None,
                window_end_utc=None,
                latitude=latitude,
                longitude=longitude,
                bbox=None,
                quality_flags=quality_flags,
                metadata=metadata,
                artifact_path=path,
                record_locator=f"$.records[{index}].value",
                sha256_value=sha256_value,
                raw_obj=row,
            )
        )
    return signals


def normalize_environment_source(
    source_skill: str,
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signals: list[dict[str, Any]] = []
    extra_observations: list[dict[str, Any]] = []
    if source_skill in {"open-meteo-historical-fetch", "open-meteo-air-quality-fetch", "open-meteo-flood-fetch"}:
        sha256_value = file_sha256(path)
        payload = parse_path_payload(path)
        signals = iter_open_meteo_signals(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    elif source_skill == "nasa-firms-fire-fetch":
        sha256_value = file_sha256(path)
        payload = parse_path_payload(path)
        signals = iter_nasa_firms_signals(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
        if isinstance(payload, dict) and isinstance(payload.get("records"), list) and not payload.get("records"):
            run_id_value = run_id
            extra_observations.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "observation_id": "obs-placeholder",
                    "run_id": run_id_value,
                    "round_id": round_id,
                    "agent_role": "environmentalist",
                    "source_skill": "nasa-firms-fire-fetch",
                    "metric": "fire_detection_count",
                    "aggregation": "event-count",
                    "value": 0.0,
                    "unit": "count",
                    "statistics": {"min": 0.0, "max": 0.0, "mean": 0.0, "p95": 0.0},
                    "time_window": {
                        "start_utc": maybe_text((payload.get("request") or {}).get("start_date")) or utc_now_iso(),
                        "end_utc": maybe_text((payload.get("request") or {}).get("end_date")) or utc_now_iso(),
                    },
                    "place_scope": {"label": "Mission region", "geometry": {"type": "Point", "latitude": 0.0, "longitude": 0.0}},
                    "quality_flags": ["satellite-detection", "zero-detections"],
                    "provenance": {
                        "source_skill": "nasa-firms-fire-fetch",
                        "artifact_path": str(path),
                        "sha256": sha256_value,
                    },
                }
            )
    elif source_skill == "openaq-data-fetch":
        signals = iter_openaq_signals(path, run_id=run_id, round_id=round_id)
    elif source_skill == "airnow-hourly-obs-fetch":
        signals = iter_airnow_signals(path, run_id=run_id, round_id=round_id)
    elif source_skill == "usgs-water-iv-fetch":
        signals = iter_usgs_water_iv_signals(path, run_id=run_id, round_id=round_id)
    else:
        raise ValueError(f"Unsupported environment source skill: {source_skill}")
    return signals, extra_observations


def normalize_environment_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    artifact_sha256 = file_sha256(path)
    cache_path = normalize_cache_path(
        run_dir,
        domain="environment",
        source_skill=source_skill,
        run_id=run_id,
        round_id=round_id,
        artifact_sha256=artifact_sha256,
    )
    cached = read_cache_payload(cache_path)
    if isinstance(cached, dict):
        signals = cached.get("signals")
        extra_observations = cached.get("extra_observations")
        if (
            cached.get("cache_version") == NORMALIZE_CACHE_VERSION
            and cached.get("artifact_sha256") == artifact_sha256
            and isinstance(signals, list)
            and isinstance(extra_observations, list)
        ):
            return (
                [item for item in signals if isinstance(item, dict)],
                [item for item in extra_observations if isinstance(item, dict)],
                "hit",
            )

    signals, extra_observations = normalize_environment_source(source_skill, path, run_id=run_id, round_id=round_id)
    write_cache_payload(
        cache_path,
        {
            "cache_version": NORMALIZE_CACHE_VERSION,
            "domain": "environment",
            "source_skill": source_skill,
            "run_id": run_id,
            "round_id": round_id,
            "artifact_path": str(path),
            "artifact_sha256": artifact_sha256,
            "signals": signals,
            "extra_observations": extra_observations,
        },
    )
    return signals, extra_observations, "miss"


def observation_group_key(signal: dict[str, Any], mission_scope: dict[str, Any]) -> tuple[str, str, str]:
    metric = canonical_environment_metric(signal.get("metric"))
    source_skill = maybe_text(signal.get("source_skill"))
    lat = maybe_number(signal.get("latitude"))
    lon = maybe_number(signal.get("longitude"))
    if lat is None or lon is None:
        return (source_skill, metric, stable_hash(json.dumps(mission_scope, sort_keys=True))[:8])
    return (source_skill, metric, f"{lat:.3f},{lon:.3f}")


def derive_place_scope(signals: list[dict[str, Any]], mission_scope: dict[str, Any]) -> dict[str, Any]:
    if not signals:
        return mission_scope
    latitudes = [maybe_number(item.get("latitude")) for item in signals]
    longitudes = [maybe_number(item.get("longitude")) for item in signals]
    if any(value is None for value in latitudes + longitudes):
        return mission_scope
    unique_points = {(round(float(lat), 3), round(float(lon), 3)) for lat, lon in zip(latitudes, longitudes)}
    if len(unique_points) != 1:
        return mission_scope
    latitude = statistics.fmean(float(value) for value in latitudes if value is not None)
    longitude = statistics.fmean(float(value) for value in longitudes if value is not None)
    return {
        "label": mission_scope["label"],
        "geometry": {"type": "Point", "latitude": latitude, "longitude": longitude},
    }


def environment_signals_to_observations(
    *,
    mission: dict[str, Any],
    round_id: str,
    signals: list[dict[str, Any]],
    extra_observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    run_id = mission_run_id(mission)
    mission_scope = mission_place_scope(mission)
    mission_time_window = mission_window(mission)
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        grouped[observation_group_key(signal, mission_scope)].append(signal)

    observations: list[dict[str, Any]] = []
    counter = 1
    for (_, metric, _), group in sorted(grouped.items()):
        values = [float(signal["value"]) for signal in group if maybe_number(signal.get("value")) is not None]
        if not values:
            continue
        source_skill = group[0]["source_skill"]
        output_metric = metric
        aggregation = "window-summary" if len(values) > 1 else "point"
        value = statistics.fmean(values) if len(values) > 1 else values[0]
        if source_skill == "nasa-firms-fire-fetch" and metric == "fire_detection":
            output_metric = "fire_detection_count"
            aggregation = "event-count"
            value = float(len(group))
        window = first_datetime_and_last(group)
        if window is None:
            time_window = mission_time_window
        else:
            start_utc, end_utc = window
            time_window = {"start_utc": start_utc or mission_time_window["start_utc"], "end_utc": end_utc or mission_time_window["end_utc"]}
        quality_flags = sorted({flag for signal in group for flag in signal.get("quality_flags", [])})
        observation = {
            "schema_version": SCHEMA_VERSION,
            "observation_id": emit_row_id("obs", counter),
            "run_id": run_id,
            "round_id": round_id,
            "agent_role": "environmentalist",
            "source_skill": source_skill,
            "metric": output_metric,
            "aggregation": aggregation,
            "value": value,
            "unit": "count" if output_metric == "fire_detection_count" else group[0]["unit"],
            "statistics": aggregate_stats(values),
            "time_window": time_window,
            "place_scope": derive_place_scope(group, mission_scope),
            "quality_flags": quality_flags,
            "provenance": artifact_ref(group[0]),
            "compact_audit": observation_group_compact_audit(group),
        }
        validate_payload("observation", observation)
        observations.append(observation)
        counter += 1

    for item in extra_observations:
        item["observation_id"] = emit_row_id("obs", counter)
        item["run_id"] = run_id
        item["round_id"] = round_id
        item["place_scope"] = mission_scope
        item["time_window"] = mission_time_window
        item.setdefault(
            "compact_audit",
            build_compact_audit(
                total_candidate_count=1,
                retained_count=1,
                coverage_summary="The canonical observation was emitted directly from an extra deterministic observation source.",
                concentration_flags=[],
                sampling_notes=[],
            ),
        )
        validate_payload("observation", item)
        observations.append(item)
        counter += 1
    return observations


def save_environment_db(db_path: Path, signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> None:
    init_sqlite_db(db_path, ENVIRONMENT_DDL_PATH)
    with sqlite3.connect(db_path) as conn:
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO environment_signals (
                signal_id, run_id, round_id, source_skill, signal_kind, metric, value, unit,
                observed_at_utc, window_start_utc, window_end_utc, latitude, longitude,
                bbox_json, quality_flags_json, metadata_json, artifact_path, record_locator,
                sha256, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    signal["signal_id"],
                    signal["run_id"],
                    signal["round_id"],
                    signal["source_skill"],
                    signal["signal_kind"],
                    signal["metric"],
                    signal["value"],
                    signal["unit"],
                    signal["observed_at_utc"],
                    signal["window_start_utc"],
                    signal["window_end_utc"],
                    signal["latitude"],
                    signal["longitude"],
                    json.dumps(signal.get("bbox"), ensure_ascii=True, sort_keys=True) if signal.get("bbox") is not None else None,
                    json.dumps(signal.get("quality_flags", []), ensure_ascii=True, sort_keys=True),
                    json.dumps(signal.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    signal["artifact_path"],
                    signal["record_locator"],
                    signal["sha256"],
                    json.dumps(signal.get("raw_json"), ensure_ascii=True, sort_keys=True),
                )
                for signal in signals
            ),
        )
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO observation_summaries (
                observation_id, run_id, round_id, metric, source_skill, observation_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    observation["observation_id"],
                    observation["run_id"],
                    observation["round_id"],
                    observation["metric"],
                    observation["source_skill"],
                    json.dumps(observation, ensure_ascii=True, sort_keys=True),
                )
                for observation in observations
            ),
        )


def load_canonical_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    if not isinstance(payload, list):
        raise ValueError(f"Expected list in {path}")
    return [item for item in payload if isinstance(item, dict)]


def load_object_if_exists(path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return None


def append_library_events(run_dir: Path, round_id: str, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    ledger_path = evidence_library_ledger_path(run_dir, round_id)
    existing = read_jsonl(ledger_path) if ledger_path.exists() else []
    entries = [item for item in existing if isinstance(item, dict)]
    for event in events:
        object_kind = maybe_text(event.get("object_kind"))
        payload = event.get("payload")
        if not object_kind or not isinstance(payload, dict):
            continue
        entries.append(
            {
                "recorded_at_utc": utc_now_iso(),
                "object_kind": object_kind,
                "payload": payload,
            }
        )
    write_jsonl(ledger_path, entries)


def merge_unique_items(*groups: list[dict[str, Any]], key_fn: Any) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for group in groups:
        for item in group:
            if not isinstance(item, dict):
                continue
            key = maybe_text(key_fn(item))
            if not key:
                key = stable_hash(stable_json(item))
            if key not in merged:
                ordered_keys.append(key)
            merged[key] = item
    return [merged[key] for key in ordered_keys]


def previous_active_list(run_dir: Path, round_id: str, path_fn: Any) -> list[dict[str, Any]]:
    prior_round = previous_round_id(run_dir, round_id)
    if prior_round is None:
        return []
    return load_canonical_list(path_fn(run_dir, prior_round))


def merge_claim_submissions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("submission_id") or item.get("claim_id"))


def merge_observation_submissions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(
        *groups,
        key_fn=lambda item: stable_hash(stable_json(observation_signature_payload(item))),
    )


def merge_evidence_cards(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("evidence_id"))


def merge_isolated_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("isolated_id"))


def merge_remand_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("remand_id"))


def observation_signature_payload(observation: dict[str, Any]) -> dict[str, Any]:
    provenance = observation.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    return {
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": observation.get("statistics"),
        "time_window": observation.get("time_window"),
        "place_scope": observation.get("place_scope"),
        "quality_flags": sorted(
            maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)
        ),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }


def shared_observation_id(observation: dict[str, Any]) -> str:
    signature = stable_hash(stable_json(observation_signature_payload(observation)))
    return f"obs-{signature[:12]}"


def materialize_shared_observation(observation: dict[str, Any]) -> dict[str, Any]:
    item = dict(observation)
    item["observation_id"] = shared_observation_id(observation)
    return item


def merge_effective_observations(*observation_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged_by_signature: dict[str, dict[str, Any]] = {}
    ordered_signatures: list[str] = []
    for group in observation_groups:
        for observation in group:
            signature_payload = observation_signature_payload(observation)
            signature = stable_hash(stable_json(signature_payload))
            if signature not in merged_by_signature:
                ordered_signatures.append(signature)
            merged_by_signature[signature] = materialize_shared_observation(observation)
    return [merged_by_signature[signature] for signature in ordered_signatures]


def effective_shared_observations(
    run_dir: Path,
    round_id: str,
    *,
    current_round_observations: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    inherited: list[dict[str, Any]] = []
    prior_round_id = previous_round_id(run_dir, round_id)
    if prior_round_id is not None:
        inherited = effective_shared_observations(run_dir, prior_round_id)
    current = (
        current_round_observations
        if current_round_observations is not None
        else load_canonical_list(shared_observations_path(run_dir, round_id))
    )
    return merge_effective_observations(inherited, current)


def merge_effective_claims(*claim_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*claim_groups, key_fn=lambda item: item.get("claim_id"))


def effective_shared_claims(
    run_dir: Path,
    round_id: str,
    *,
    current_round_claims: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    inherited: list[dict[str, Any]] = []
    prior_round_id = previous_round_id(run_dir, round_id)
    if prior_round_id is not None:
        inherited = effective_shared_claims(run_dir, prior_round_id)
    current = (
        current_round_claims
        if current_round_claims is not None
        else load_canonical_list(shared_claims_path(run_dir, round_id))
    )
    return merge_effective_claims(inherited, current)


def active_library_list(run_dir: Path, round_id: str, path_fn: Any) -> list[dict[str, Any]]:
    current_path = path_fn(run_dir, round_id)
    if current_path.exists():
        return load_canonical_list(current_path)
    return previous_active_list(run_dir, round_id, path_fn)


def library_state(run_dir: Path, round_id: str) -> dict[str, Any]:
    return {
        "claim_submissions_current": load_canonical_list(claim_submissions_path(run_dir, round_id)),
        "observation_submissions_current": load_canonical_list(observation_submissions_path(run_dir, round_id)),
        "claims_active": active_library_list(run_dir, round_id, claims_active_path),
        "observations_active": active_library_list(run_dir, round_id, observations_active_path),
        "cards_active": active_library_list(run_dir, round_id, cards_active_path),
        "isolated_active": active_library_list(run_dir, round_id, isolated_active_path),
        "remands_open": active_library_list(run_dir, round_id, remands_open_path),
        "matching_result": load_object_if_exists(matching_result_path(run_dir, round_id)) or {},
        "evidence_adjudication": load_object_if_exists(evidence_adjudication_path(run_dir, round_id)) or {},
        "matching_authorization": load_object_if_exists(matching_authorization_path(run_dir, round_id)) or {},
        "readiness_reports": {
            "sociologist": load_object_if_exists(data_readiness_report_path(run_dir, round_id, "sociologist")) or {},
            "environmentalist": load_object_if_exists(data_readiness_report_path(run_dir, round_id, "environmentalist")) or {},
        },
    }


def metric_relevant(claim_type: str, metric: str) -> bool:
    metric = canonical_environment_metric(metric)
    if claim_type not in CLAIM_METRIC_RULES:
        return True
    support_metrics = set(CLAIM_METRIC_RULES[claim_type]["support"].keys())
    contradict_metrics = set(CLAIM_METRIC_RULES[claim_type]["contradict"].keys())
    return metric in support_metrics or metric in contradict_metrics


def assess_observation_against_claim(claim_type: str, observation: dict[str, Any]) -> tuple[int, int, str]:
    metric = canonical_environment_metric(observation.get("metric"))
    metric_value = extract_value_for_metric(observation)
    if metric_value is None:
        return 0, 0, ""
    rules = CLAIM_METRIC_RULES.get(claim_type)
    if rules is None:
        return 0, 0, ""
    support_threshold = rules["support"].get(metric)
    contradict_threshold = rules["contradict"].get(metric)
    if support_threshold is not None:
        if metric == "fire_detection_count":
            if metric_value >= support_threshold:
                return 2, 0, f"{metric}={metric_value:g}"
        elif claim_type == "drought" and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
            if metric_value <= support_threshold:
                return 2, 0, f"{metric}={metric_value:g}"
        else:
            if metric_value >= support_threshold:
                return 2, 0, f"{metric}={metric_value:g}"
    if contradict_threshold is not None:
        if metric == "fire_detection_count":
            if metric_value <= contradict_threshold:
                return 0, 1, f"{metric}={metric_value:g}"
        elif claim_type == "wildfire" and metric in {"precipitation_sum", "relative_humidity_2m"}:
            if metric_value >= contradict_threshold:
                return 0, 1, f"{metric}={metric_value:g}"
        elif claim_type == "drought" and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
            if metric_value >= contradict_threshold:
                return 0, 1, f"{metric}={metric_value:g}"
        else:
            if metric_value <= contradict_threshold:
                return 0, 1, f"{metric}={metric_value:g}"
    return 0, 0, ""


def build_evidence_summary(claim: dict[str, Any], observation_notes: list[str], verdict: str, gaps: list[str]) -> str:
    lead = claim.get("summary") or claim.get("statement") or "Claim"
    base = truncate_text(maybe_text(lead), 140)
    if observation_notes:
        return f"{base}. Matched metrics: {', '.join(observation_notes[:4])}."
    if gaps:
        return f"{base}. Evidence remains limited: {'; '.join(gaps[:2])}."
    return f"{base}. Current evidence verdict: {verdict}."


def compact_task(task: dict[str, Any]) -> dict[str, Any]:
    inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
    evidence_requirements = inputs.get("evidence_requirements") if isinstance(inputs.get("evidence_requirements"), list) else []
    return {
        "task_id": maybe_text(task.get("task_id")),
        "assigned_role": maybe_text(task.get("assigned_role")),
        "objective": truncate_text(maybe_text(task.get("objective")), 180),
        "status": maybe_text(task.get("status")),
        "evidence_requirements": [
            maybe_text(item.get("requirement_type"))
            for item in evidence_requirements
            if isinstance(item, dict) and maybe_text(item.get("requirement_type"))
        ][:3],
    }


def claim_source_skills(claim: dict[str, Any]) -> list[str]:
    refs = claim.get("public_refs")
    if not isinstance(refs, list):
        return []
    return sorted(
        {
            maybe_text(ref.get("source_skill"))
            for ref in refs
            if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
        }
    )


def compact_claim(claim: dict[str, Any]) -> dict[str, Any]:
    return {
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": truncate_text(maybe_text(claim.get("summary")), 180),
        "priority": claim.get("priority"),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "public_source_skills": claim_source_skills(claim),
    }


def compact_observation(observation: dict[str, Any]) -> dict[str, Any]:
    return {
        "observation_id": maybe_text(observation.get("observation_id")),
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "time_window": observation.get("time_window"),
        "quality_flags": [maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)][:4],
    }


def compact_evidence_card(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "evidence_id": maybe_text(card.get("evidence_id")),
        "claim_id": maybe_text(card.get("claim_id")),
        "verdict": maybe_text(card.get("verdict")),
        "confidence": maybe_text(card.get("confidence")),
        "summary": truncate_text(maybe_text(card.get("summary")), 220),
        "observation_ids": [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)][:6],
        "gaps": [truncate_text(maybe_text(item), 120) for item in card.get("gaps", []) if maybe_text(item)][:3],
    }


def compact_claim_submission(submission: dict[str, Any]) -> dict[str, Any]:
    return {
        "submission_id": maybe_text(submission.get("submission_id")),
        "claim_id": maybe_text(submission.get("claim_id")),
        "claim_type": maybe_text(submission.get("claim_type")),
        "summary": truncate_text(maybe_text(submission.get("summary")), 180),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
        "source_signal_count": submission.get("source_signal_count"),
    }


def compact_observation_submission(submission: dict[str, Any]) -> dict[str, Any]:
    return {
        "submission_id": maybe_text(submission.get("submission_id")),
        "observation_id": maybe_text(submission.get("observation_id")),
        "metric": maybe_text(submission.get("metric")),
        "source_skill": maybe_text(submission.get("source_skill")),
        "aggregation": maybe_text(submission.get("aggregation")),
        "value": submission.get("value"),
        "unit": maybe_text(submission.get("unit")),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
    }


def compact_isolated_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "isolated_id": maybe_text(item.get("isolated_id")),
        "entity_kind": maybe_text(item.get("entity_kind")),
        "entity_id": maybe_text(item.get("entity_id")),
        "summary": truncate_text(maybe_text(item.get("summary")), 200),
        "reason": truncate_text(maybe_text(item.get("reason")), 160),
    }


def compact_remand_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "remand_id": maybe_text(item.get("remand_id")),
        "entity_kind": maybe_text(item.get("entity_kind")),
        "entity_id": maybe_text(item.get("entity_id")),
        "summary": truncate_text(maybe_text(item.get("summary")), 200),
        "reasons": [truncate_text(maybe_text(reason), 120) for reason in item.get("reasons", []) if maybe_text(reason)][:3],
    }


def ordered_context_observations(observations: list[dict[str, Any]], evidence_cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {maybe_text(item.get("observation_id")): item for item in observations}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for card in evidence_cards:
        ids = card.get("observation_ids")
        if not isinstance(ids, list):
            continue
        for observation_id in ids:
            key = maybe_text(observation_id)
            if not key or key in seen or key not in by_id:
                continue
            ordered.append(by_id[key])
            seen.add(key)
    for observation in observations:
        key = maybe_text(observation.get("observation_id"))
        if not key or key in seen:
            continue
        ordered.append(observation)
        seen.add(key)
    return ordered


def build_public_signal_summary(signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now_iso(),
        "signal_count": len(signals),
        "claim_count": len(claims),
        "source_skill_counts": dict(Counter(maybe_text(item.get("source_skill")) for item in signals)),
        "signal_kind_counts": dict(Counter(maybe_text(item.get("signal_kind")) for item in signals)),
        "top_signals": [
            {
                "signal_id": maybe_text(item.get("signal_id")),
                "source_skill": maybe_text(item.get("source_skill")),
                "title": truncate_text(maybe_text(item.get("title")), 120),
                "published_at_utc": maybe_text(item.get("published_at_utc")),
            }
            for item in signals[:5]
        ],
        "claims": [compact_claim(item) for item in claims[:MAX_CONTEXT_CLAIMS]],
    }


def build_environment_signal_summary(signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now_iso(),
        "signal_count": len(signals),
        "observation_count": len(observations),
        "source_skill_counts": dict(Counter(maybe_text(item.get("source_skill")) for item in signals)),
        "metric_counts": dict(Counter(maybe_text(item.get("metric")) for item in signals)),
        "top_observations": [compact_observation(item) for item in observations[:MAX_CONTEXT_OBSERVATIONS]],
    }


def claim_submission_from_claim(claim: dict[str, Any]) -> dict[str, Any]:
    submission = {
        "schema_version": SCHEMA_VERSION,
        "submission_id": f"claimsub-{maybe_text(claim.get('claim_id'))}",
        "run_id": maybe_text(claim.get("run_id")),
        "round_id": maybe_text(claim.get("round_id")),
        "agent_role": "sociologist",
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": maybe_text(claim.get("summary")),
        "statement": maybe_text(claim.get("statement")),
        "meaning": (
            f"This public-side claim captures the mission-relevant narrative for {maybe_text(claim.get('claim_type')) or 'the current event'}."
        ),
        "priority": int(claim.get("priority") or 1),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "worth_storing": True,
        "source_signal_count": int(claim.get("source_signal_count") or max(1, len(claim.get("public_refs", [])))),
        "time_window": claim.get("time_window"),
        "place_scope": claim.get("place_scope"),
        "public_refs": claim.get("public_refs", []),
        "compact_audit": claim.get("compact_audit")
        if isinstance(claim.get("compact_audit"), dict)
        else build_compact_audit(
            total_candidate_count=max(1, len(claim.get("public_refs", []))),
            retained_count=min(max(1, len(claim.get("public_refs", []))), 8),
            coverage_summary="Derived claim was materialized into a library submission without an explicit compact audit.",
            concentration_flags=[],
            sampling_notes=[],
        ),
    }
    validate_payload("claim-submission", submission)
    return submission


def observation_submission_from_observation(observation: dict[str, Any]) -> dict[str, Any]:
    submission = {
        "schema_version": SCHEMA_VERSION,
        "submission_id": f"obssub-{maybe_text(observation.get('observation_id'))}",
        "run_id": maybe_text(observation.get("run_id")),
        "round_id": maybe_text(observation.get("round_id")),
        "agent_role": "environmentalist",
        "observation_id": maybe_text(observation.get("observation_id")),
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "meaning": (
            f"This observation records mission-window physical evidence for metric {maybe_text(observation.get('metric'))}."
        ),
        "worth_storing": True,
        "time_window": observation.get("time_window"),
        "place_scope": observation.get("place_scope"),
        "quality_flags": observation.get("quality_flags", []),
        "provenance": observation.get("provenance"),
        "compact_audit": observation.get("compact_audit")
        if isinstance(observation.get("compact_audit"), dict)
        else build_compact_audit(
            total_candidate_count=1,
            retained_count=1,
            coverage_summary="Derived observation was materialized into a library submission without an explicit compact audit.",
            concentration_flags=[],
            sampling_notes=[],
        ),
    }
    validate_payload("observation-submission", submission)
    return submission


def claims_from_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for index, submission in enumerate(submissions, start=1):
        claim = {
            "schema_version": SCHEMA_VERSION,
            "claim_id": maybe_text(submission.get("claim_id")) or emit_row_id("claim", index),
            "run_id": maybe_text(submission.get("run_id")),
            "round_id": maybe_text(submission.get("round_id")),
            "agent_role": "sociologist",
            "claim_type": maybe_text(submission.get("claim_type")),
            "status": "candidate",
            "summary": maybe_text(submission.get("summary")),
            "statement": maybe_text(submission.get("statement")),
            "priority": int(submission.get("priority") or 1),
            "needs_physical_validation": bool(submission.get("needs_physical_validation")),
            "time_window": submission.get("time_window"),
            "place_scope": submission.get("place_scope"),
            "public_refs": submission.get("public_refs", []),
            "source_signal_count": submission.get("source_signal_count"),
            "compact_audit": submission.get("compact_audit"),
        }
        validate_payload("claim", claim)
        claims.append(claim)
    return claims


def observations_from_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for index, submission in enumerate(submissions, start=1):
        observation = {
            "schema_version": SCHEMA_VERSION,
            "observation_id": maybe_text(submission.get("observation_id")) or emit_row_id("obs", index),
            "run_id": maybe_text(submission.get("run_id")),
            "round_id": maybe_text(submission.get("round_id")),
            "agent_role": "environmentalist",
            "source_skill": maybe_text(submission.get("source_skill")),
            "metric": maybe_text(submission.get("metric")),
            "aggregation": maybe_text(submission.get("aggregation")),
            "value": submission.get("value"),
            "unit": maybe_text(submission.get("unit")),
            "time_window": submission.get("time_window"),
            "place_scope": submission.get("place_scope"),
            "quality_flags": submission.get("quality_flags", []),
            "provenance": submission.get("provenance"),
            "compact_audit": submission.get("compact_audit"),
        }
        validate_payload("observation", observation)
        observations.append(observation)
    return observations


def match_claims_to_observations(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for claim in claims:
        matching = [
            observation
            for observation in observations
            if metric_relevant(maybe_text(claim.get("claim_type")), maybe_text(observation.get("metric")))
            and time_windows_overlap(claim.get("time_window", {}), observation.get("time_window", {}))
            and geometry_overlap(
                claim.get("place_scope", {}).get("geometry", {}),
                observation.get("place_scope", {}).get("geometry", {}),
            )
        ]
        support_score = 0
        contradict_score = 0
        notes: list[str] = []
        gaps: list[str] = []
        for observation in matching:
            support, contradict, note = assess_observation_against_claim(
                maybe_text(claim.get("claim_type")),
                observation,
            )
            support_score += support
            contradict_score += contradict
            if note:
                notes.append(note)

        if not matching:
            verdict = "insufficient"
            confidence = "low"
            gaps.append("No mission-aligned observations matched the claim window and geometry.")
        elif support_score > 0 and contradict_score == 0:
            verdict = "supports"
            confidence = "high" if support_score >= 4 and len(matching) >= 2 else "medium"
        elif support_score == 0 and contradict_score > 0:
            verdict = "contradicts"
            confidence = "medium"
        elif support_score > 0 and contradict_score > 0:
            verdict = "mixed"
            confidence = "medium"
        else:
            verdict = "insufficient"
            confidence = "low"
            gaps.append("Matched observations were mostly contextual and did not cross rule thresholds.")

        if maybe_text(claim.get("claim_type")) in {"smoke", "air-pollution"}:
            if not any(item.get("source_skill") == "openaq-data-fetch" for item in matching):
                gaps.append("Station-grade corroboration is missing.")
            if any("modeled-background" in item.get("quality_flags", []) for item in matching):
                gaps.append("Modeled background fields should be cross-checked with station or local observations.")

        matches.append(
            {
                "claim": claim,
                "observations": matching,
                "support_score": support_score,
                "contradict_score": contradict_score,
                "notes": notes,
                "gaps": sorted(dict.fromkeys(gaps)),
                "verdict": verdict,
                "confidence": confidence,
            }
        )
    return matches


def build_matching_result(
    *,
    authorization: dict[str, Any],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
) -> dict[str, Any]:
    matched_pairs = [
        {
            "claim_id": maybe_text(match["claim"].get("claim_id")),
            "observation_ids": [maybe_text(item.get("observation_id")) for item in match["observations"] if maybe_text(item.get("observation_id"))],
            "support_score": float(match["support_score"]),
            "contradict_score": float(match["contradict_score"]),
            "notes": [maybe_text(item) for item in match["notes"] if maybe_text(item)],
        }
        for match in matches
        if match["observations"]
    ]
    matched_claim_ids = [maybe_text(item["claim_id"]) for item in matched_pairs if maybe_text(item.get("claim_id"))]
    matched_observation_ids = unique_strings(
        [
            maybe_text(observation_id)
            for pair in matched_pairs
            for observation_id in pair.get("observation_ids", [])
            if maybe_text(observation_id)
        ]
    )
    all_claim_ids = [maybe_text(item.get("claim_id")) for item in claims if maybe_text(item.get("claim_id"))]
    all_observation_ids = [maybe_text(item.get("observation_id")) for item in observations if maybe_text(item.get("observation_id"))]
    unmatched_claim_ids = [claim_id for claim_id in all_claim_ids if claim_id not in set(matched_claim_ids)]
    unmatched_observation_ids = [obs_id for obs_id in all_observation_ids if obs_id not in set(matched_observation_ids)]
    if matched_pairs and (unmatched_claim_ids or unmatched_observation_ids):
        result_status = "partial"
    elif matched_pairs:
        result_status = "matched"
    else:
        result_status = "unmatched"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "result_id": f"matchres-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")) or maybe_text(claims[0].get("run_id")) if claims else "",
        "round_id": maybe_text(authorization.get("round_id")) or maybe_text(claims[0].get("round_id")) if claims else "",
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "result_status": result_status,
        "summary": (
            f"Matched {len(matched_pairs)} claim-observation clusters, leaving "
            f"{len(unmatched_claim_ids)} unmatched claims and {len(unmatched_observation_ids)} unmatched observations."
        ),
        "matched_pairs": matched_pairs,
        "matched_claim_ids": matched_claim_ids,
        "matched_observation_ids": matched_observation_ids,
        "unmatched_claim_ids": unmatched_claim_ids,
        "unmatched_observation_ids": unmatched_observation_ids,
    }
    validate_payload("matching-result", payload)
    return payload


def build_isolated_entries(
    *,
    run_id: str,
    round_id: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    allow_isolated_evidence: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not allow_isolated_evidence:
        return [], []
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match["observations"]
        if maybe_text(observation.get("observation_id"))
    }
    isolated: list[dict[str, Any]] = []
    for claim_index, match in enumerate(matches, start=1):
        claim = match["claim"]
        if match["observations"]:
            continue
        isolated.append(
            {
                "schema_version": SCHEMA_VERSION,
                "isolated_id": f"isolated-claim-{claim_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "claim",
                "entity_id": maybe_text(claim.get("claim_id")),
                "summary": maybe_text(claim.get("summary")),
                "reason": "Public-side evidence is currently isolated from physical corroboration.",
            }
        )
    observation_index = 1
    for observation in observations:
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in matched_observation_ids:
            continue
        isolated.append(
            {
                "schema_version": SCHEMA_VERSION,
                "isolated_id": f"isolated-observation-{observation_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "observation",
                "entity_id": observation_id,
                "summary": f"{maybe_text(observation.get('metric'))} from {maybe_text(observation.get('source_skill'))}",
                "reason": "Physical-side evidence is currently isolated from attributable public recognition.",
            }
        )
        observation_index += 1
    return isolated, []


def build_remand_entries(
    *,
    run_id: str,
    round_id: str,
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    allow_isolated_evidence: bool,
) -> list[dict[str, Any]]:
    remands: list[dict[str, Any]] = []
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match["observations"]
        if maybe_text(observation.get("observation_id"))
    }
    for index, match in enumerate(matches, start=1):
        claim = match["claim"]
        claim_id = maybe_text(claim.get("claim_id"))
        if not claim_id:
            continue
        has_observations = bool(match["observations"])
        verdict = maybe_text(match["verdict"])
        if not has_observations and allow_isolated_evidence:
            continue
        if verdict not in {"mixed", "insufficient"} and has_observations:
            continue
        remands.append(
            {
                "schema_version": SCHEMA_VERSION,
                "remand_id": f"remand-claim-{index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "claim",
                "entity_id": claim_id,
                "summary": maybe_text(claim.get("summary")),
                "reasons": [maybe_text(item) for item in match["gaps"] if maybe_text(item)] or ["Matching remained partial."],
            }
        )
    if allow_isolated_evidence:
        return remands
    observation_index = 1
    for observation in observations:
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in matched_observation_ids:
            continue
        remands.append(
            {
                "schema_version": SCHEMA_VERSION,
                "remand_id": f"remand-observation-{observation_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "observation",
                "entity_id": observation_id,
                "summary": f"{maybe_text(observation.get('metric'))} from {maybe_text(observation.get('source_skill'))}",
                "reasons": ["Observation remained unmatched and isolated evidence was not authorized."],
            }
        )
        observation_index += 1
    return remands


def build_evidence_adjudication(
    *,
    authorization: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
) -> dict[str, Any]:
    if remands and evidence_cards:
        status = "partial"
    elif remands:
        status = "remand-required"
    else:
        status = "complete"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "adjudication_id": f"adjudication-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "matching_result_id": maybe_text(matching_result.get("result_id")),
        "adjudication_status": status,
        "summary": (
            f"Produced {len(evidence_cards)} evidence cards, {len(isolated_entries)} isolated entries, "
            f"and {len(remands)} open remands."
        ),
        "matching_reasonable": bool(evidence_cards or isolated_entries or remands),
        "needs_additional_data": bool(remands),
        "card_ids": [maybe_text(item.get("evidence_id")) for item in evidence_cards if maybe_text(item.get("evidence_id"))],
        "isolated_entry_ids": [maybe_text(item.get("isolated_id")) for item in isolated_entries if maybe_text(item.get("isolated_id"))],
        "remand_ids": [maybe_text(item.get("remand_id")) for item in remands if maybe_text(item.get("remand_id"))],
        "open_questions": unique_strings(
            [
                f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
                for item in remands
                if maybe_text(item.get("remand_id"))
            ]
        ),
        "recommended_next_actions": [],
    }
    validate_payload("evidence-adjudication", payload)
    return payload


def link_claims_to_evidence(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    evidence_cards: list[dict[str, Any]] = []
    matches = match_claims_to_observations(claims=claims, observations=observations)
    for index, match in enumerate(matches, start=1):
        claim = match["claim"]
        evidence = {
            "schema_version": SCHEMA_VERSION,
            "evidence_id": emit_row_id("evidence", index),
            "run_id": claim["run_id"],
            "round_id": claim["round_id"],
            "claim_id": claim["claim_id"],
            "verdict": match["verdict"],
            "confidence": match["confidence"],
            "summary": build_evidence_summary(claim, match["notes"], match["verdict"], match["gaps"]),
            "public_refs": claim.get("public_refs", []),
            "observation_ids": [item["observation_id"] for item in match["observations"]],
            "gaps": match["gaps"],
        }
        validate_payload("evidence-card", evidence)
        evidence_cards.append(evidence)
    return evidence_cards


def build_round_snapshot(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    round_id: str,
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    role: str,
) -> dict[str, Any]:
    state = library_state(run_dir, round_id)
    run = {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "topic": maybe_text(mission.get("topic")),
        "objective": maybe_text(mission.get("objective")),
        "region": mission_place_scope(mission),
        "window": mission_window(mission),
        "role": role,
    }
    role_tasks = [task for task in tasks if role == "moderator" or task.get("assigned_role") == role]
    verdict_counter = Counter(maybe_text(item.get("verdict")) for item in evidence_cards)
    focus_claims = claims
    if role == "environmentalist":
        focus_claims = [claim for claim in claims if claim.get("needs_physical_validation")]

    dataset = {
        "generated_at_utc": utc_now_iso(),
        "task_count": len(role_tasks),
        "claim_count": len(claims),
        "observation_count": len(observations),
        "evidence_count": len(evidence_cards),
        "claim_submission_count": len(state["claim_submissions_current"]),
        "observation_submission_count": len(state["observation_submissions_current"]),
        "claims_active_count": len(state["claims_active"]),
        "observations_active_count": len(state["observations_active"]),
        "cards_active_count": len(state["cards_active"]),
        "isolated_count": len(state["isolated_active"]),
        "remand_count": len(state["remands_open"]),
    }
    focus = {
        "task_ids": [maybe_text(task.get("task_id")) for task in role_tasks],
        "claims_needing_more_evidence": [
            card["claim_id"] for card in evidence_cards if card.get("verdict") in {"mixed", "insufficient"}
        ],
    }
    if role == "sociologist":
        focus["candidate_claim_ids"] = [claim["claim_id"] for claim in focus_claims]
    if role == "environmentalist":
        focus["metrics_requested"] = sorted({observation["metric"] for observation in observations})

    compact_claims_list = [compact_claim(item) for item in focus_claims[:MAX_CONTEXT_CLAIMS]]
    compact_evidence = [compact_evidence_card(item) for item in evidence_cards[:MAX_CONTEXT_EVIDENCE]]
    compact_observations = [
        compact_observation(item)
        for item in ordered_context_observations(observations, evidence_cards)[:MAX_CONTEXT_OBSERVATIONS]
    ]

    return {
        "context_layer": "evidence-library-v1",
        "run": run,
        "dataset": dataset,
        "phase_state": {
            "readiness_statuses": {
                report_role: maybe_text(report.get("readiness_status"))
                for report_role, report in state["readiness_reports"].items()
                if isinstance(report, dict)
            },
            "matching_authorization_status": maybe_text(state["matching_authorization"].get("authorization_status")),
            "matching_result_status": maybe_text(state["matching_result"].get("result_status")),
            "adjudication_status": maybe_text(state["evidence_adjudication"].get("adjudication_status")),
        },
        "aggregates": {
            "claim_type_counts": dict(Counter(maybe_text(item.get("claim_type")) for item in claims)),
            "observation_metric_counts": dict(Counter(maybe_text(item.get("metric")) for item in observations)),
            "evidence_verdict_counts": dict(verdict_counter),
        },
        "canonical_paths": {
            "tasks": str(round_dir(run_dir, round_id) / "moderator" / "tasks.json"),
            "claims": str(shared_claims_path(run_dir, round_id)),
            "observations": str(shared_observations_path(run_dir, round_id)),
            "evidence_cards": str(shared_evidence_path(run_dir, round_id)),
            "claim_submissions": str(claim_submissions_path(run_dir, round_id)),
            "observation_submissions": str(observation_submissions_path(run_dir, round_id)),
            "sociologist_data_readiness_report": str(data_readiness_report_path(run_dir, round_id, "sociologist")),
            "environmentalist_data_readiness_report": str(data_readiness_report_path(run_dir, round_id, "environmentalist")),
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
            "evidence_library_dir": str(evidence_library_dir(run_dir, round_id)),
        },
        "tasks": [compact_task(item) for item in role_tasks[:MAX_CONTEXT_TASKS]],
        "focus": focus,
        "claims": compact_claims_list,
        "observations": compact_observations,
        "evidence_cards": compact_evidence,
        "evidence_library": {
            "claim_submissions_current": [
                compact_claim_submission(item) for item in state["claim_submissions_current"][:MAX_CONTEXT_CLAIMS]
            ],
            "observation_submissions_current": [
                compact_observation_submission(item) for item in state["observation_submissions_current"][:MAX_CONTEXT_OBSERVATIONS]
            ],
            "claims_active": [compact_claim_submission(item) for item in state["claims_active"][:MAX_CONTEXT_CLAIMS]],
            "observations_active": [
                compact_observation_submission(item) for item in state["observations_active"][:MAX_CONTEXT_OBSERVATIONS]
            ],
            "cards_active": [compact_evidence_card(item) for item in state["cards_active"][:MAX_CONTEXT_EVIDENCE]],
            "isolated_active": [compact_isolated_entry(item) for item in state["isolated_active"][:MAX_CONTEXT_EVIDENCE]],
            "remands_open": [compact_remand_entry(item) for item in state["remands_open"][:MAX_CONTEXT_EVIDENCE]],
        },
    }


def command_init_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    public_db = Path(args.public_db).expanduser().resolve() if args.public_db else default_public_db_path(run_dir_path)
    environment_db = (
        Path(args.environment_db).expanduser().resolve()
        if args.environment_db
        else default_environment_db_path(run_dir_path)
    )
    init_sqlite_db(public_db, PUBLIC_DDL_PATH)
    init_sqlite_db(environment_db, ENVIRONMENT_DDL_PATH)

    for role in ("moderator", "sociologist", "environmentalist"):
        default_context_dir(run_dir_path, args.round_id, role).mkdir(parents=True, exist_ok=True)
    (round_dir(run_dir_path, args.round_id) / "shared" / "contexts").mkdir(parents=True, exist_ok=True)
    evidence_library_dir(run_dir_path, args.round_id).mkdir(parents=True, exist_ok=True)
    for path, payload in (
        (claims_active_path(run_dir_path, args.round_id), []),
        (observations_active_path(run_dir_path, args.round_id), []),
        (cards_active_path(run_dir_path, args.round_id), []),
        (isolated_active_path(run_dir_path, args.round_id), []),
        (remands_open_path(run_dir_path, args.round_id), []),
    ):
        if not path.exists():
            write_json(path, payload, pretty=args.pretty)
    if not evidence_library_ledger_path(run_dir_path, args.round_id).exists():
        atomic_write_text_file(evidence_library_ledger_path(run_dir_path, args.round_id), "")

    manifest = load_or_build_manifest(run_dir_path, mission)
    manifest["round_id_initialized"] = args.round_id
    manifest["databases"] = {
        "public_signals": str(public_db),
        "environment_signals": str(environment_db),
    }
    manifest["normalization_cache"] = {
        "version": NORMALIZE_CACHE_VERSION,
        "directory": str(normalize_cache_dir(run_dir_path)),
    }
    manifest["initialized_at_utc"] = utc_now_iso()
    write_json(run_manifest_path(run_dir_path), manifest, pretty=args.pretty)

    return {
        "run_dir": str(run_dir_path),
        "public_db": str(public_db),
        "environment_db": str(environment_db),
        "manifest_path": str(run_manifest_path(run_dir_path)),
    }


def command_normalize_public(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    run_id = mission_run_id(mission)
    constraints = mission_constraints(mission)
    public_db = Path(args.public_db).expanduser().resolve() if args.public_db else default_public_db_path(run_dir_path)
    inputs = parse_input_specs(args.input)
    all_signals: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    for source_skill, path in inputs:
        signals, cache_status = normalize_public_source_cached(
            run_dir=run_dir_path,
            source_skill=source_skill,
            path=path,
            mission=mission,
            run_id=run_id,
            round_id=args.round_id,
        )
        all_signals.extend(signals)
        if cache_status == "hit":
            cache_hits += 1
        else:
            cache_misses += 1

    deduped_by_id: dict[str, dict[str, Any]] = {signal["signal_id"]: signal for signal in all_signals}
    signals = sorted(
        deduped_by_id.values(),
        key=lambda item: (
            item.get("published_at_utc") or "",
            item["signal_id"],
        ),
        reverse=False,
    )
    configured_claim_cap = constraints.get("claim_hard_cap_per_round") or constraints.get("max_claims_per_round") or args.max_claims
    claim_limit = max(1, min(args.max_claims, configured_claim_cap))
    claims = public_signals_to_claims(
        mission=mission,
        round_id=args.round_id,
        signals=signals,
        max_claims=claim_limit,
    )
    current_submissions = [claim_submission_from_claim(item) for item in claims]
    active_submissions = merge_claim_submissions(
        previous_active_list(run_dir_path, args.round_id, claims_active_path),
        current_submissions,
    )
    shared_claims = claims_from_submissions(active_submissions)

    save_public_db(public_db, signals, claims)
    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "sociologist")
    public_signals_file = normalized_dir / "public_signals.jsonl"
    claims_file = normalized_dir / "claim_candidates.json"
    summary_file = normalized_dir / "public_signal_summary.json"
    write_jsonl(public_signals_file, signals)
    write_json(claims_file, claims, pretty=args.pretty)
    write_json(summary_file, build_public_signal_summary(signals, claims), pretty=args.pretty)
    write_json(claim_submissions_path(run_dir_path, args.round_id), current_submissions, pretty=args.pretty)
    write_json(claims_active_path(run_dir_path, args.round_id), active_submissions, pretty=args.pretty)
    write_json(shared_claims_path(run_dir_path, args.round_id), shared_claims, pretty=args.pretty)
    append_library_events(
        run_dir_path,
        args.round_id,
        [{"object_kind": "claim-submission", "payload": item} for item in current_submissions],
    )

    return {
        "public_db": str(public_db),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "signal_count": len(signals),
        "claim_count": len(claims),
        "claim_submission_count": len(current_submissions),
        "claims_active_count": len(active_submissions),
        "signals_path": str(public_signals_file),
        "signal_summary_path": str(summary_file),
        "claims_path": str(claims_file),
        "claim_submissions_path": str(claim_submissions_path(run_dir_path, args.round_id)),
        "claims_active_path": str(claims_active_path(run_dir_path, args.round_id)),
        "shared_claims_path": str(shared_claims_path(run_dir_path, args.round_id)),
    }


def command_normalize_environment(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    run_id = mission_run_id(mission)
    environment_db = (
        Path(args.environment_db).expanduser().resolve()
        if args.environment_db
        else default_environment_db_path(run_dir_path)
    )
    inputs = parse_input_specs(args.input)
    all_signals: list[dict[str, Any]] = []
    extra_observations: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    for source_skill, path in inputs:
        source_signals, source_observations, cache_status = normalize_environment_source_cached(
            run_dir=run_dir_path,
            source_skill=source_skill,
            path=path,
            run_id=run_id,
            round_id=args.round_id,
        )
        all_signals.extend(source_signals)
        extra_observations.extend(source_observations)
        if cache_status == "hit":
            cache_hits += 1
        else:
            cache_misses += 1

    deduped_by_id: dict[str, dict[str, Any]] = {signal["signal_id"]: signal for signal in all_signals}
    signals = sorted(deduped_by_id.values(), key=lambda item: (item.get("metric") or "", item["signal_id"]))
    observations = environment_signals_to_observations(
        mission=mission,
        round_id=args.round_id,
        signals=signals,
        extra_observations=extra_observations,
    )

    save_environment_db(environment_db, signals, observations)
    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "environmentalist")
    signals_file = normalized_dir / "environment_signals.jsonl"
    observations_file = normalized_dir / "observations.json"
    summary_file = normalized_dir / "environment_signal_summary.json"
    shared_observations = effective_shared_observations(
        run_dir_path,
        args.round_id,
        current_round_observations=observations,
    )
    current_submissions = [observation_submission_from_observation(item) for item in observations]
    active_submissions = merge_observation_submissions(
        previous_active_list(run_dir_path, args.round_id, observations_active_path),
        current_submissions,
    )
    write_jsonl(signals_file, signals)
    write_json(observations_file, observations, pretty=args.pretty)
    write_json(summary_file, build_environment_signal_summary(signals, observations), pretty=args.pretty)
    write_json(observation_submissions_path(run_dir_path, args.round_id), current_submissions, pretty=args.pretty)
    write_json(observations_active_path(run_dir_path, args.round_id), active_submissions, pretty=args.pretty)
    write_json(shared_observations_path(run_dir_path, args.round_id), shared_observations, pretty=args.pretty)
    append_library_events(
        run_dir_path,
        args.round_id,
        [{"object_kind": "observation-submission", "payload": item} for item in current_submissions],
    )

    return {
        "environment_db": str(environment_db),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "signal_count": len(signals),
        "observation_count": len(observations),
        "observation_submission_count": len(current_submissions),
        "observations_active_count": len(active_submissions),
        "shared_observation_count": len(shared_observations),
        "signals_path": str(signals_file),
        "signal_summary_path": str(summary_file),
        "observations_path": str(observations_file),
        "observation_submissions_path": str(observation_submissions_path(run_dir_path, args.round_id)),
        "observations_active_path": str(observations_active_path(run_dir_path, args.round_id)),
        "shared_observations_path": str(shared_observations_path(run_dir_path, args.round_id)),
    }


def command_link_evidence(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    authorization_input = (
        Path(args.authorization_input).expanduser().resolve()
        if maybe_text(args.authorization_input)
        else matching_authorization_path(run_dir_path, args.round_id)
    )
    authorization = load_object_if_exists(authorization_input)
    if authorization is None:
        raise ValueError(f"Matching authorization is missing or invalid: {authorization_input}")
    validate_payload("matching-authorization", authorization)
    if maybe_text(authorization.get("authorization_status")) != "authorized":
        raise ValueError("Matching authorization exists but does not authorize matching.")
    claims = load_canonical_list(shared_claims_path(run_dir_path, args.round_id))
    observations = effective_shared_observations(run_dir_path, args.round_id)
    authorized_claim_ids = {
        maybe_text(item)
        for item in authorization.get("claim_ids", [])
        if maybe_text(item)
    }
    authorized_observation_ids = {
        maybe_text(item)
        for item in authorization.get("observation_ids", [])
        if maybe_text(item)
    }
    filtered_claims = [item for item in claims if not authorized_claim_ids or maybe_text(item.get("claim_id")) in authorized_claim_ids]
    filtered_observations = [
        item
        for item in observations
        if not authorized_observation_ids or maybe_text(item.get("observation_id")) in authorized_observation_ids
    ]
    matches = match_claims_to_observations(claims=filtered_claims, observations=filtered_observations)
    evidence_cards = link_claims_to_evidence(claims=filtered_claims, observations=filtered_observations)
    matching_result = build_matching_result(
        authorization=authorization,
        claims=filtered_claims,
        observations=filtered_observations,
        matches=matches,
    )
    allow_isolated_evidence = bool(authorization.get("allow_isolated_evidence"))
    isolated_entries, _unused = build_isolated_entries(
        run_id=maybe_text(authorization.get("run_id")) or mission_run_id(load_mission(run_dir_path)),
        round_id=args.round_id,
        claims=filtered_claims,
        observations=filtered_observations,
        matches=matches,
        allow_isolated_evidence=allow_isolated_evidence,
    )
    remands = build_remand_entries(
        run_id=maybe_text(authorization.get("run_id")) or mission_run_id(load_mission(run_dir_path)),
        round_id=args.round_id,
        matches=matches,
        observations=filtered_observations,
        allow_isolated_evidence=allow_isolated_evidence,
    )
    validate_payload("isolated-entry", isolated_entries)
    validate_payload("remand-entry", remands)
    adjudication = build_evidence_adjudication(
        authorization=authorization,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
    )

    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "environmentalist")
    evidence_path = normalized_dir / "evidence_cards.json"
    write_json(evidence_path, evidence_cards, pretty=args.pretty)
    write_json(shared_evidence_path(run_dir_path, args.round_id), evidence_cards, pretty=args.pretty)
    write_json(matching_result_path(run_dir_path, args.round_id), matching_result, pretty=args.pretty)
    write_json(evidence_adjudication_path(run_dir_path, args.round_id), adjudication, pretty=args.pretty)
    write_json(
        cards_active_path(run_dir_path, args.round_id),
        merge_evidence_cards(previous_active_list(run_dir_path, args.round_id, cards_active_path), evidence_cards),
        pretty=args.pretty,
    )
    write_json(
        isolated_active_path(run_dir_path, args.round_id),
        merge_isolated_entries(previous_active_list(run_dir_path, args.round_id, isolated_active_path), isolated_entries),
        pretty=args.pretty,
    )
    write_json(
        remands_open_path(run_dir_path, args.round_id),
        merge_remand_entries(previous_active_list(run_dir_path, args.round_id, remands_open_path), remands),
        pretty=args.pretty,
    )
    append_library_events(
        run_dir_path,
        args.round_id,
        [
            {"object_kind": "matching-result", "payload": matching_result},
            {"object_kind": "evidence-adjudication", "payload": adjudication},
        ],
    )

    return {
        "evidence_count": len(evidence_cards),
        "isolated_count": len(isolated_entries),
        "remand_count": len(remands),
        "evidence_path": str(evidence_path),
        "shared_evidence_path": str(shared_evidence_path(run_dir_path, args.round_id)),
        "matching_result_path": str(matching_result_path(run_dir_path, args.round_id)),
        "evidence_adjudication_path": str(evidence_adjudication_path(run_dir_path, args.round_id)),
    }


def command_build_round_context(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    tasks_path = round_dir(run_dir_path, args.round_id) / "moderator" / "tasks.json"
    tasks = load_canonical_list(tasks_path)
    claims = load_canonical_list(shared_claims_path(run_dir_path, args.round_id))
    observations = effective_shared_observations(run_dir_path, args.round_id)
    evidence_cards = load_canonical_list(shared_evidence_path(run_dir_path, args.round_id))

    outputs: dict[str, str] = {}
    for role in ("moderator", "sociologist", "environmentalist"):
        payload = build_round_snapshot(
            run_dir=run_dir_path,
            mission=mission,
            round_id=args.round_id,
            tasks=tasks,
            claims=claims,
            observations=observations,
            evidence_cards=evidence_cards,
            role=role,
        )
        context_path = default_context_dir(run_dir_path, args.round_id, role) / f"context_{role}.json"
        write_json(context_path, payload, pretty=args.pretty)
        write_json(library_context_path(run_dir_path, args.round_id, role), payload, pretty=args.pretty)
        outputs[role] = str(context_path)

    snapshot = build_round_snapshot(
        run_dir=run_dir_path,
        mission=mission,
        round_id=args.round_id,
        tasks=tasks,
        claims=claims,
        observations=observations,
        evidence_cards=evidence_cards,
        role="moderator",
    )
    shared_snapshot_path = round_dir(run_dir_path, args.round_id) / "shared" / "contexts" / "round_snapshot.json"
    write_json(shared_snapshot_path, snapshot, pretty=args.pretty)
    outputs["shared_snapshot"] = str(shared_snapshot_path)

    return {
        "claim_count": len(claims),
        "observation_count": len(observations),
        "evidence_count": len(evidence_cards),
        "cards_active_count": len(library_state(run_dir_path, args.round_id)["cards_active"]),
        "outputs": outputs,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic normalization pipeline for eco-council runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_run = sub.add_parser("init-run", help="Initialize normalization databases and derived directories.")
    init_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    init_run.add_argument("--round-id", default="round-001", help="Round identifier.")
    init_run.add_argument("--public-db", default="", help="Override public-signals SQLite path.")
    init_run.add_argument("--environment-db", default="", help="Override environment-signals SQLite path.")
    add_pretty_flag(init_run)

    normalize_public = sub.add_parser("normalize-public", help="Normalize sociologist-side raw artifacts.")
    normalize_public.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    normalize_public.add_argument("--round-id", required=True, help="Round identifier.")
    normalize_public.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input artifact in source-skill=/path form. Repeat for multiple artifacts.",
    )
    normalize_public.add_argument("--public-db", default="", help="Override public-signals SQLite path.")
    normalize_public.add_argument("--max-claims", type=int, default=8, help="Maximum canonical claims to emit.")
    add_pretty_flag(normalize_public)

    normalize_environment = sub.add_parser("normalize-environment", help="Normalize environment raw artifacts.")
    normalize_environment.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    normalize_environment.add_argument("--round-id", required=True, help="Round identifier.")
    normalize_environment.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input artifact in source-skill=/path form. Repeat for multiple artifacts.",
    )
    normalize_environment.add_argument("--environment-db", default="", help="Override environment-signals SQLite path.")
    add_pretty_flag(normalize_environment)

    link_evidence = sub.add_parser("link-evidence", help="Run authorized matching and materialize evidence cards.")
    link_evidence.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    link_evidence.add_argument("--round-id", required=True, help="Round identifier.")
    link_evidence.add_argument(
        "--authorization-input",
        default="",
        help="Optional matching-authorization JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(link_evidence)

    build_context = sub.add_parser("build-round-context", help="Build role-specific round context payloads.")
    build_context.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    build_context.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(build_context)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-run": command_init_run,
        "normalize-public": command_normalize_public,
        "normalize-environment": command_normalize_environment,
        "link-evidence": command_link_evidence,
        "build-round-context": command_build_round_context,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        result = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(result, pretty=getattr(args, "pretty", False)))
        return 1

    result = {"command": args.command, "ok": True, "payload": payload}
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
