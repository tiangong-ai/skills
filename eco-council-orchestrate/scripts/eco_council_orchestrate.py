#!/usr/bin/env python3
"""Coordinate eco-council run lifecycle around OpenClaw handoffs."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_DIR = SKILL_DIR.parent

CONTRACT_SCRIPT = REPO_DIR / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"
NORMALIZE_SCRIPT = REPO_DIR / "eco-council-normalize" / "scripts" / "eco_council_normalize.py"
REPORTING_SCRIPT = REPO_DIR / "eco-council-reporting" / "scripts" / "eco_council_reporting.py"
OPENAQ_API_SCRIPT = REPO_DIR / "openaq-data-fetch" / "scripts" / "openaq_api_client.py"

SKILL_DIRS = {
    "gdelt-doc-search": REPO_DIR / "gdelt-doc-search",
    "gdelt-events-fetch": REPO_DIR / "gdelt-events-fetch",
    "gdelt-mentions-fetch": REPO_DIR / "gdelt-mentions-fetch",
    "gdelt-gkg-fetch": REPO_DIR / "gdelt-gkg-fetch",
    "bluesky-cascade-fetch": REPO_DIR / "bluesky-cascade-fetch",
    "youtube-video-search": REPO_DIR / "youtube-video-search",
    "youtube-comments-fetch": REPO_DIR / "youtube-comments-fetch",
    "federal-register-doc-fetch": REPO_DIR / "federal-register-doc-fetch",
    "regulationsgov-comments-fetch": REPO_DIR / "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch": REPO_DIR / "regulationsgov-comment-detail-fetch",
    "airnow-hourly-obs-fetch": REPO_DIR / "airnow-hourly-obs-fetch",
    "usgs-water-iv-fetch": REPO_DIR / "usgs-water-iv-fetch",
    "open-meteo-air-quality-fetch": REPO_DIR / "open-meteo-air-quality-fetch",
    "open-meteo-historical-fetch": REPO_DIR / "open-meteo-historical-fetch",
    "open-meteo-flood-fetch": REPO_DIR / "open-meteo-flood-fetch",
    "nasa-firms-fire-fetch": REPO_DIR / "nasa-firms-fire-fetch",
    "openaq-data-fetch": REPO_DIR / "openaq-data-fetch",
}

FETCH_SCRIPT_PATHS = {
    "gdelt-doc-search": SKILL_DIRS["gdelt-doc-search"] / "scripts" / "gdelt_doc_search.py",
    "gdelt-events-fetch": SKILL_DIRS["gdelt-events-fetch"] / "scripts" / "gdelt_events_fetch.py",
    "gdelt-mentions-fetch": SKILL_DIRS["gdelt-mentions-fetch"] / "scripts" / "gdelt_mentions_fetch.py",
    "gdelt-gkg-fetch": SKILL_DIRS["gdelt-gkg-fetch"] / "scripts" / "gdelt_gkg_fetch.py",
    "bluesky-cascade-fetch": SKILL_DIRS["bluesky-cascade-fetch"] / "scripts" / "bluesky_cascade_fetch.py",
    "youtube-video-search": SKILL_DIRS["youtube-video-search"] / "scripts" / "youtube_video_search.py",
    "youtube-comments-fetch": SKILL_DIRS["youtube-comments-fetch"] / "scripts" / "youtube_comments_fetch.py",
    "federal-register-doc-fetch": SKILL_DIRS["federal-register-doc-fetch"] / "scripts" / "federal_register_doc_fetch.py",
    "regulationsgov-comments-fetch": SKILL_DIRS["regulationsgov-comments-fetch"] / "scripts" / "regulationsgov_comments_fetch.py",
    "regulationsgov-comment-detail-fetch": SKILL_DIRS["regulationsgov-comment-detail-fetch"] / "scripts" / "regulationsgov_comment_detail_fetch.py",
    "airnow-hourly-obs-fetch": SKILL_DIRS["airnow-hourly-obs-fetch"] / "scripts" / "airnow_hourly_obs_fetch.py",
    "usgs-water-iv-fetch": SKILL_DIRS["usgs-water-iv-fetch"] / "scripts" / "usgs_water_iv_fetch.py",
    "open-meteo-air-quality-fetch": SKILL_DIRS["open-meteo-air-quality-fetch"] / "scripts" / "open_meteo_air_quality_fetch.py",
    "open-meteo-historical-fetch": SKILL_DIRS["open-meteo-historical-fetch"] / "scripts" / "open_meteo_historical_fetch.py",
    "open-meteo-flood-fetch": SKILL_DIRS["open-meteo-flood-fetch"] / "scripts" / "open_meteo_flood_fetch.py",
    "nasa-firms-fire-fetch": SKILL_DIRS["nasa-firms-fire-fetch"] / "scripts" / "nasa_firms_fire_fetch.py",
}

PUBLIC_SOURCES = (
    "gdelt-doc-search",
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
    "bluesky-cascade-fetch",
    "youtube-video-search",
    "youtube-comments-fetch",
    "federal-register-doc-fetch",
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
)
GDELT_RAW_TABLE_SOURCES = {
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
}
GDELT_EXPECTED_COLUMNS = {
    "gdelt-events-fetch": "61",
    "gdelt-mentions-fetch": "16",
    "gdelt-gkg-fetch": "27",
}
GENERIC_QUERY_NOISE_TOKENS = {
    "analysis",
    "assess",
    "assessment",
    "attention",
    "attributable",
    "cause",
    "claims",
    "collect",
    "concern",
    "cross",
    "determine",
    "dialogue",
    "discourse",
    "discovery",
    "event",
    "evidence",
    "framing",
    "health",
    "high",
    "identify",
    "linked",
    "local",
    "mission",
    "patterns",
    "plausibly",
    "public",
    "regional",
    "risk",
    "risks",
    "salience",
    "same",
    "severity",
    "signals",
    "social",
    "spike",
    "three",
    "through",
    "triggered",
    "unusual",
    "value",
    "validation",
    "verification",
    "versus",
    "whether",
    "window",
}
QUERY_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")
GDELT_MAX_FILE_INPUT_KEYS = {
    "gdelt-events-fetch": "gdelt_events_max_files",
    "gdelt-mentions-fetch": "gdelt_mentions_max_files",
    "gdelt-gkg-fetch": "gdelt_gkg_max_files",
}
GDELT_PREVIEW_LINE_INPUT_KEYS = {
    "gdelt-events-fetch": "gdelt_events_preview_lines",
    "gdelt-mentions-fetch": "gdelt_mentions_preview_lines",
    "gdelt-gkg-fetch": "gdelt_gkg_preview_lines",
}
ENVIRONMENT_SOURCES = (
    "airnow-hourly-obs-fetch",
    "usgs-water-iv-fetch",
    "open-meteo-air-quality-fetch",
    "open-meteo-historical-fetch",
    "open-meteo-flood-fetch",
    "nasa-firms-fire-fetch",
    "openaq-data-fetch",
)
ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")
ENV_ASSIGNMENT_PATTERN = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")

SUPPORTED_SOURCES_BY_ROLE = {
    "sociologist": list(PUBLIC_SOURCES),
    "environmentalist": list(ENVIRONMENT_SOURCES),
}
DEFAULT_OPEN_METEO_AIR_VARS = [
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "ozone",
    "us_aqi",
]
DEFAULT_AIRNOW_PARAMETER_NAMES = [
    "PM25",
    "PM10",
    "OZONE",
    "NO2",
]
DEFAULT_AIRNOW_POINT_PADDING_DEG = 0.25
DEFAULT_USGS_PARAMETER_CODES = [
    "00060",
    "00065",
]
DEFAULT_USGS_POINT_PADDING_DEG = 0.25
DEFAULT_USGS_SITE_TYPE = "ST"
DEFAULT_USGS_SITE_STATUS = "active"
DEFAULT_OPEN_METEO_HIST_HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "soil_moisture_0_to_7cm",
]


def load_contract_module() -> Any | None:
    if not CONTRACT_SCRIPT.exists():
        return None
    module_name = "eco_council_contract_orchestrate"
    spec = importlib.util.spec_from_file_location(module_name, CONTRACT_SCRIPT)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT_MODULE = load_contract_module()
DEFAULT_OPEN_METEO_HIST_DAILY_VARS = [
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]
DEFAULT_OPEN_METEO_FLOOD_DAILY_VARS = [
    "river_discharge",
    "river_discharge_p75",
]
DEFAULT_OPENAQ_PARAMETER_NAMES = [
    "pm25",
    "pm2.5",
    "pm10",
    "o3",
    "no2",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    atomic_write_text_file(path, pretty_json(payload, pretty=pretty) + "\n")


def write_text(path: Path, content: str) -> None:
    atomic_write_text_file(path, content.rstrip() + "\n")


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


@contextmanager
def exclusive_file_lock(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def normalize_space(value: str) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(str(value))


def text_truthy(value: Any) -> bool:
    return maybe_text(value).casefold() in {"1", "true", "yes", "on"}


def truncate_text(value: str, limit: int) -> str:
    text = normalize_space(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def unique_strings(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = normalize_space(value)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def contract_call(name: str, *args: Any, **kwargs: Any) -> Any | None:
    if CONTRACT_MODULE is None or not hasattr(CONTRACT_MODULE, name):
        return None
    helper = getattr(CONTRACT_MODULE, name)
    return helper(*args, **kwargs)


def ensure_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return value


def ensure_object_list(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a JSON list.")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{label} must contain only JSON objects.")
    return value


def parse_utc_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("Expected a non-empty UTC datetime string.")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        result = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid UTC datetime: {value!r}") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def to_date_text(value: str) -> str:
    return parse_utc_datetime(value).date().isoformat()


def to_gdelt_datetime(value: str) -> str:
    return parse_utc_datetime(value).strftime("%Y%m%d%H%M%S")


def firms_source_for_window(window: dict[str, Any], requested_source: str) -> str:
    source = (requested_source or "VIIRS_NOAA20_NRT").strip()
    if not source.endswith("_NRT"):
        return source
    end_text = maybe_text(window.get("end_utc"))
    if not end_text:
        return source
    try:
        end_dt = parse_utc_datetime(end_text)
    except ValueError:
        return source
    age_days = (datetime.now(timezone.utc) - end_dt).days
    if age_days <= 30:
        return source
    archival_source = source.removesuffix("_NRT") + "_SP"
    known_archival = {
        "MODIS_SP",
        "VIIRS_NOAA20_SP",
        "VIIRS_SNPP_SP",
    }
    return archival_source if archival_source in known_archival else source


def round_dir_name(round_id: str) -> str:
    text = round_id.strip()
    if not ROUND_ID_PATTERN.match(text):
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 style.")
    return text.replace("-", "_")


def round_id_from_dirname(dirname: str) -> str | None:
    match = ROUND_DIR_PATTERN.match(dirname.strip())
    if match is None:
        return None
    return f"round-{match.group(1)}"


def round_number(round_id: str) -> int:
    if not ROUND_ID_PATTERN.match(round_id):
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 style.")
    return int(round_id.split("-")[-1])


def next_round_id(round_id: str) -> str:
    return f"round-{round_number(round_id) + 1:03d}"


def round_sort_key(round_id: str) -> tuple[int, str]:
    try:
        return (round_number(round_id), round_id)
    except ValueError:
        return (sys.maxsize, round_id)


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_dir_name(round_id)


def role_raw_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "raw"


def role_derived_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived"


def role_meta_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return role_raw_dir(run_dir, round_id, role) / "_meta"


def moderator_derived_dir(run_dir: Path, round_id: str) -> Path:
    return role_derived_dir(run_dir, round_id, "moderator")


def task_review_prompt_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "openclaw_task_review_prompt.txt"


def fetch_plan_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "fetch_plan.json"


def fetch_execution_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "fetch_execution.json"


def data_plane_execution_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "data_plane_execution.json"


def matching_execution_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "matching_adjudication_execution.json"


def matching_authorization_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_authorization.json"


def round_manifest_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "openclaw_round_manifest.json"


def fetch_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return role_derived_dir(run_dir, round_id, role) / "openclaw_fetch_prompt.txt"


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "source_selection.json"


def reporting_handoff_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "openclaw_reporting_handoff.json"


def approved_next_round_tasks_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "approved_next_round_tasks.json"


def fetch_lock_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "fetch.lock"


def default_raw_artifact_path(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    extension = ".json"
    if source_skill in {
        "youtube-video-search",
        "youtube-comments-fetch",
        "regulationsgov-comments-fetch",
        "regulationsgov-comment-detail-fetch",
    }:
        extension = ".jsonl"
    return role_raw_dir(run_dir, round_id, role) / f"{source_skill}{extension}"


def default_raw_download_dir(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return role_raw_dir(run_dir, round_id, role) / source_skill


def default_raw_quarantine_dir(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return default_raw_download_dir(run_dir, round_id, role, source_skill) / "quarantine"


def default_step_stdout_path(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return role_meta_dir(run_dir, round_id, role) / f"{source_skill}.stdout.json"


def default_step_stderr_path(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return role_meta_dir(run_dir, round_id, role) / f"{source_skill}.stderr.log"


def discover_round_ids(run_dir: Path) -> list[str]:
    output: list[str] = []
    if not run_dir.exists():
        return output
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        round_id = round_id_from_dirname(child.name)
        if round_id is not None:
            output.append(round_id)
    output.sort(key=round_sort_key)
    return output


def resolve_round_id(run_dir: Path, round_id: str) -> str:
    if round_id:
        return round_id
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}.")
    return round_ids[-1]


def load_mission(run_dir: Path) -> dict[str, Any]:
    return ensure_object(read_json(run_dir / "mission.json"), "mission.json")


def load_tasks(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    tasks_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    return ensure_object_list(read_json(tasks_path), f"{tasks_path}")


def load_source_selection(run_dir: Path, round_id: str, role: str) -> dict[str, Any] | None:
    path = source_selection_path(run_dir, round_id, role)
    if not path.exists():
        return None
    return ensure_object(read_json(path), f"{path}")


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_snapshot(path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "path": str(path),
        "exists": exists,
        "sha256": file_sha256(path) if exists else "",
    }


def fetch_plan_input_snapshot(
    *,
    run_dir: Path,
    round_id: str,
    sociologist_selection: dict[str, Any] | None,
    environmentalist_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    tasks_file = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    sociologist_path = source_selection_path(run_dir, round_id, "sociologist")
    environmentalist_path = source_selection_path(run_dir, round_id, "environmentalist")
    return {
        "tasks": file_snapshot(tasks_file),
        "source_selections": {
            "sociologist": {
                **file_snapshot(sociologist_path),
                "status": maybe_text((sociologist_selection or {}).get("status")),
            },
            "environmentalist": {
                **file_snapshot(environmentalist_path),
                "status": maybe_text((environmentalist_selection or {}).get("status")),
            },
        },
    }


def ensure_fetch_plan_inputs_match(*, run_dir: Path, round_id: str, plan: dict[str, Any]) -> None:
    snapshot = ensure_object(plan.get("input_snapshot"), "fetch_plan.input_snapshot")
    task_snapshot = ensure_object(snapshot.get("tasks"), "fetch_plan.input_snapshot.tasks")
    task_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    current_task_snapshot = file_snapshot(task_path)
    issues: list[str] = []
    if maybe_text(task_snapshot.get("sha256")) != maybe_text(current_task_snapshot.get("sha256")):
        issues.append(f"tasks.json changed ({task_path})")

    source_snapshots = ensure_object(snapshot.get("source_selections"), "fetch_plan.input_snapshot.source_selections")
    for role in ("sociologist", "environmentalist"):
        expected = ensure_object(source_snapshots.get(role), f"fetch_plan.input_snapshot.source_selections.{role}")
        path = source_selection_path(run_dir, round_id, role)
        current = file_snapshot(path)
        current_payload = load_source_selection(run_dir, round_id, role)
        current_status = maybe_text((current_payload or {}).get("status"))
        if maybe_text(expected.get("sha256")) != maybe_text(current.get("sha256")):
            issues.append(f"{role} source_selection changed ({path})")
        if maybe_text(expected.get("status")) != current_status:
            issues.append(
                f"{role} source_selection status changed (expected {maybe_text(expected.get('status')) or '<empty>'}, found {current_status or '<empty>'})"
            )
    if issues:
        raise RuntimeError("Fetch plan inputs changed since prepare-round. Rerun prepare-round. " + "; ".join(issues))


def tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def mission_window(mission: dict[str, Any]) -> dict[str, str]:
    window = ensure_object(mission.get("window"), "mission.window")
    start_utc = maybe_text(window.get("start_utc"))
    end_utc = maybe_text(window.get("end_utc"))
    if not start_utc or not end_utc:
        raise ValueError("Mission window must include start_utc and end_utc.")
    return {"start_utc": start_utc, "end_utc": end_utc}


def mission_region(mission: dict[str, Any]) -> dict[str, Any]:
    return ensure_object(mission.get("region"), "mission.region")


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    values = contract_call("allowed_sources_for_role", mission, role)
    if isinstance(values, list):
        return [maybe_text(item) for item in values if maybe_text(item)]
    return []


def effective_constraints(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("effective_constraints", mission)
    if isinstance(value, dict):
        return value
    constraints = mission.get("constraints")
    return constraints if isinstance(constraints, dict) else {}


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("policy_profile_summary", mission)
    if isinstance(value, dict):
        return value
    return {}


def task_inputs(task: dict[str, Any]) -> dict[str, Any]:
    value = task.get("inputs")
    if isinstance(value, dict):
        return value
    return {}


def task_notes(task: dict[str, Any]) -> str:
    return maybe_text(task.get("notes"))


def merged_task_string_list(tasks: list[dict[str, Any]], key: str) -> list[str]:
    output: list[str] = []
    for task in tasks:
        inputs = task_inputs(task)
        candidate = inputs.get(key)
        if isinstance(candidate, list):
            output.extend(maybe_text(item) for item in candidate if maybe_text(item))
        elif isinstance(candidate, str) and candidate.strip():
            output.append(candidate)
    return unique_strings(output)


def merged_task_scalar(tasks: list[dict[str, Any]], key: str) -> str:
    for task in tasks:
        value = task_inputs(task).get(key)
        text = maybe_text(value)
        if text:
            return text
    return ""


def task_objective_text(tasks: list[dict[str, Any]]) -> str:
    return " ".join(maybe_text(task.get("objective")) for task in tasks if maybe_text(task.get("objective")))


def role_supported_sources(role: str) -> list[str]:
    return list(SUPPORTED_SOURCES_BY_ROLE.get(role, []))


def role_source_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = contract_call("source_governance", mission)
    if not isinstance(governance, dict):
        return {}
    family_lookup = contract_call("source_family_lookup", mission, role=role)
    if isinstance(family_lookup, dict):
        role_families = list(family_lookup.values())
    else:
        role_families = [
            family
            for family in governance.get("families", [])
            if isinstance(family, dict) and maybe_text(family.get("role")) == role
        ]
    family_ids = {maybe_text(family.get("family_id")) for family in role_families if maybe_text(family.get("family_id"))}
    approvals = [
        item
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict) and maybe_text(item.get("family_id")) in family_ids
    ]
    return {
        "approval_authority": maybe_text(governance.get("approval_authority")),
        "allow_cross_round_anchors": bool(governance.get("allow_cross_round_anchors")),
        "max_selected_sources_per_role": governance.get("max_selected_sources_per_role"),
        "max_active_families_per_role": governance.get("max_active_families_per_role"),
        "max_non_entry_layers_per_role": governance.get("max_non_entry_layers_per_role"),
        "approved_layers": approvals,
        "families": role_families,
    }


def override_requests_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "override_requests.json"


def load_override_requests(run_dir: Path, round_id: str, role: str) -> list[dict[str, Any]]:
    payload = load_json_if_exists(override_requests_path(run_dir, round_id, role))
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def source_selection_selected_sources(source_selection: dict[str, Any] | None) -> list[str]:
    if not isinstance(source_selection, dict):
        return []
    if maybe_text(source_selection.get("status")) == "pending":
        return []
    family_selected = contract_call("selected_sources_from_family_plans", source_selection)
    if isinstance(family_selected, list) and family_selected:
        return unique_strings([maybe_text(item) for item in family_selected if maybe_text(item)])
    value = source_selection.get("selected_sources")
    if not isinstance(value, list):
        return []
    return unique_strings([maybe_text(item) for item in value if maybe_text(item)])


def ensure_source_selection_respects_governance(
    *,
    mission: dict[str, Any],
    role: str,
    source_selection: dict[str, Any] | None,
) -> None:
    if not isinstance(source_selection, dict):
        return
    governance = role_source_governance(mission, role)
    families = governance.get("families") if isinstance(governance.get("families"), list) else []
    if not families:
        return
    family_plans = source_selection.get("family_plans")
    if not isinstance(family_plans, list):
        raise ValueError(f"Role {role} source_selection must include family_plans.")

    family_lookup: dict[str, dict[str, Any]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        if family_id:
            family_lookup[family_id] = family
    payload_lookup: dict[str, dict[str, Any]] = {}
    for family_plan in family_plans:
        if not isinstance(family_plan, dict):
            continue
        family_id = maybe_text(family_plan.get("family_id"))
        if family_id:
            payload_lookup[family_id] = family_plan
    if set(payload_lookup) != set(family_lookup):
        missing = sorted(set(family_lookup) - set(payload_lookup))
        extra = sorted(set(payload_lookup) - set(family_lookup))
        raise ValueError(f"Role {role} family_plans must match governed families. Missing={missing}, extra={extra}")

    selected_sources = source_selection_selected_sources(source_selection)
    max_sources = governance.get("max_selected_sources_per_role")
    if isinstance(max_sources, int) and max_sources > 0 and len(selected_sources) > max_sources:
        raise ValueError(
            f"Role {role} selected {len(selected_sources)} sources but governance max_selected_sources_per_role={max_sources}."
        )

    approved_lookup = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id"))): item
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict) and maybe_text(item.get("family_id")) and maybe_text(item.get("layer_id"))
    }
    allow_cross_round_anchors = bool(governance.get("allow_cross_round_anchors"))
    selected_family_count = 0
    selected_non_entry_layers = 0

    for family_id, family_plan in payload_lookup.items():
        family_policy = family_lookup.get(family_id)
        if not isinstance(family_policy, dict):
            continue
        if family_plan.get("selected") is True:
            selected_family_count += 1
        layer_lookup = {
            maybe_text(layer.get("layer_id")): layer
            for layer in family_policy.get("layers", [])
            if isinstance(layer, dict) and maybe_text(layer.get("layer_id"))
        }
        layer_plans = family_plan.get("layer_plans")
        if not isinstance(layer_plans, list):
            raise ValueError(f"Role {role} family {family_id} must include layer_plans.")
        payload_layer_ids = {
            maybe_text(layer_plan.get("layer_id"))
            for layer_plan in layer_plans
            if isinstance(layer_plan, dict) and maybe_text(layer_plan.get("layer_id"))
        }
        if set(layer_lookup) != payload_layer_ids:
            missing = sorted(set(layer_lookup) - payload_layer_ids)
            extra = sorted(payload_layer_ids - set(layer_lookup))
            raise ValueError(f"Role {role} family {family_id} layer_plans mismatch. Missing={missing}, extra={extra}")

        for layer_plan in layer_plans:
            if not isinstance(layer_plan, dict):
                continue
            layer_id = maybe_text(layer_plan.get("layer_id"))
            layer_policy = layer_lookup.get(layer_id)
            if not isinstance(layer_policy, dict):
                continue
            if maybe_text(layer_plan.get("tier")) != maybe_text(layer_policy.get("tier")):
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} tier mismatch.")
            selected = layer_plan.get("selected") is True
            selected_skill_set = {
                maybe_text(skill)
                for skill in layer_plan.get("source_skills", [])
                if maybe_text(skill)
            }
            allowed_skill_set = {
                maybe_text(skill)
                for skill in layer_policy.get("skills", [])
                if maybe_text(skill)
            }
            if not selected_skill_set <= allowed_skill_set:
                invalid = sorted(selected_skill_set - allowed_skill_set)
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} selected invalid skills {invalid}.")
            max_selected_skills = layer_policy.get("max_selected_skills")
            if isinstance(max_selected_skills, int) and max_selected_skills > 0 and len(selected_skill_set) > max_selected_skills:
                raise ValueError(
                    f"Role {role} family {family_id} layer {layer_id} selected {len(selected_skill_set)} skills but max_selected_skills={max_selected_skills}."
                )
            if not selected:
                continue

            tier = maybe_text(layer_policy.get("tier"))
            authorization_basis = maybe_text(layer_plan.get("authorization_basis"))
            anchor_mode = maybe_text(layer_plan.get("anchor_mode"))
            anchor_refs = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
            if tier == "l2":
                selected_non_entry_layers += 1
            if layer_policy.get("requires_anchor") is True and (anchor_mode == "none" or not anchor_refs):
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} requires anchors.")
            if anchor_mode == "prior_round_l1" and not allow_cross_round_anchors:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} cannot use prior_round_l1 anchors.")
            approval_key = (family_id, layer_id)
            if tier == "l1":
                if authorization_basis != "entry-layer":
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use entry-layer authorization.")
            else:
                auto_selectable = layer_policy.get("auto_selectable") is True
                if approval_key in approved_lookup:
                    if authorization_basis != "upstream-approval":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use upstream-approval.")
                elif auto_selectable:
                    if authorization_basis != "policy-auto":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use policy-auto authorization.")
                else:
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} is not approved by governance.")

    max_families = governance.get("max_active_families_per_role")
    if isinstance(max_families, int) and max_families > 0 and selected_family_count > max_families:
        raise ValueError(
            f"Role {role} selected {selected_family_count} families but governance max_active_families_per_role={max_families}."
        )
    max_l2_layers = governance.get("max_non_entry_layers_per_role")
    if isinstance(max_l2_layers, int) and max_l2_layers >= 0 and selected_non_entry_layers > max_l2_layers:
        raise ValueError(
            f"Role {role} selected {selected_non_entry_layers} non-entry layers but governance max_non_entry_layers_per_role={max_l2_layers}."
        )


def role_selected_sources(
    *,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    role: str,
    source_selection: dict[str, Any] | None,
) -> list[str]:
    ensure_source_selection_respects_governance(mission=mission, role=role, source_selection=source_selection)
    allowed = allowed_sources_for_role(mission, role)
    supported = role_supported_sources(role)
    allowed_lookup = {source.casefold() for source in allowed}
    supported_lookup = {source.casefold() for source in supported}
    selected_lookup = {
        source.casefold()
        for source in source_selection_selected_sources(source_selection)
        if source.casefold() in supported_lookup
    }
    if not selected_lookup:
        return []
    if not allowed_lookup:
        selected = sorted(selected_lookup)
        raise ValueError(f"Role {role} selected sources {selected}, but mission.source_governance exposes no allowed sources.")
    invalid = [source for source in supported if source.casefold() in selected_lookup and source.casefold() not in allowed_lookup]
    if invalid:
        raise ValueError(f"Role {role} selected unsupported or disallowed sources: {invalid}.")
    return [source for source in supported if source.casefold() in selected_lookup and source.casefold() in allowed_lookup]


def build_plain_query(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
    query_hints = merged_task_string_list(tasks, "query_hints")
    if query_hints:
        return query_hints[0]
    region_label = primary_region_search_label(mission=mission)
    topic_tokens = compact_query_terms(mission=mission, tasks=tasks, max_terms=4)
    parts = []
    if topic_tokens:
        parts.append(" ".join(topic_tokens))
    if region_label:
        parts.append(region_label)
    return " ".join(parts) if parts else "environment public signals"


def build_gdelt_query(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
    query_hints = merged_task_string_list(tasks, "query_hints")
    if not query_hints:
        region_label = primary_region_search_label(mission=mission)
        topic_terms = compact_query_terms(mission=mission, tasks=tasks, max_terms=4)
        clauses: list[str] = []
        if region_label:
            clauses.append(gdelt_literal_term(region_label))
        if topic_terms:
            if len(topic_terms) == 1:
                clauses.append(topic_terms[0])
            else:
                clauses.append("(" + " OR ".join(topic_terms) + ")")
        if not clauses:
            return '"environment"'
        if len(clauses) == 1:
            return clauses[0]
        return " AND ".join(clauses)
    terms: list[str] = []
    for hint in query_hints[:3]:
        clean = normalize_space(hint)
        if not clean:
            continue
        if any(token in clean for token in ('"', "(", ")", " OR ", " AND ", "sourcecountry:")):
            terms.append(clean)
        elif " " in clean:
            terms.append(gdelt_literal_term(clean))
        else:
            terms.append(clean)
    if not terms:
        return '"environment"'
    if len(terms) == 1:
        return terms[0]
    return "(" + " OR ".join(terms) + ")"


def primary_region_search_label(*, mission: dict[str, Any]) -> str:
    region_label = maybe_text(mission_region(mission).get("label"))
    if not region_label:
        return ""
    primary = normalize_space(region_label.split(",")[0])
    return primary or region_label


def iter_evidence_requirement_summaries(tasks: list[dict[str, Any]]) -> list[str]:
    summaries: list[str] = []
    for task in tasks:
        inputs = task_inputs(task)
        evidence_requirements = inputs.get("evidence_requirements")
        if not isinstance(evidence_requirements, list):
            continue
        for item in evidence_requirements:
            if isinstance(item, dict) and maybe_text(item.get("summary")):
                summaries.append(maybe_text(item.get("summary")))
    return summaries


def extract_query_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in QUERY_TOKEN_PATTERN.findall(text):
        token = raw.strip()
        if not token:
            continue
        key = token.casefold()
        if key in GENERIC_QUERY_NOISE_TOKENS:
            continue
        if len(token) < 3 and not token.isupper():
            continue
        if key in seen:
            continue
        seen.add(key)
        tokens.append(token)
    return tokens


def compact_query_terms(*, mission: dict[str, Any], tasks: list[dict[str, Any]], max_terms: int) -> list[str]:
    region_tokens = {
        token.casefold()
        for token in extract_query_tokens(primary_region_search_label(mission=mission))
    }
    primary_text = maybe_text(mission.get("topic"))
    fallback_texts = [
        task_objective_text(tasks),
        maybe_text(mission.get("objective")),
        *iter_evidence_requirement_summaries(tasks),
    ]
    terms: list[str] = []
    seen: set[str] = set()

    def collect(text: str) -> bool:
        nonlocal terms
        for token in extract_query_tokens(text):
            key = token.casefold()
            if key in region_tokens or key in seen:
                continue
            seen.add(key)
            terms.append(token)
            if len(terms) >= max_terms:
                return True
        return False

    if primary_text:
        collect(primary_text)
    if terms:
        return terms
    for text in fallback_texts:
        if collect(text):
            break
    return terms


def gdelt_literal_term(text: str) -> str:
    clean = normalize_space(text)
    if not clean:
        return clean
    word_count = len(QUERY_TOKEN_PATTERN.findall(clean))
    if " " in clean and word_count <= 4 and len(clean) <= 48:
        return f'"{clean}"'
    return clean


def geometry_from_task_or_mission(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    for task in tasks:
        geometry = task_inputs(task).get("mission_geometry")
        if isinstance(geometry, dict):
            return geometry
    return ensure_object(mission_region(mission).get("geometry"), "mission.region.geometry")


def window_from_task_or_mission(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, str]:
    for task in tasks:
        window = task_inputs(task).get("mission_window")
        if isinstance(window, dict) and maybe_text(window.get("start_utc")) and maybe_text(window.get("end_utc")):
            return {"start_utc": maybe_text(window.get("start_utc")), "end_utc": maybe_text(window.get("end_utc"))}
    return mission_window(mission)


def center_point_for_geometry(geometry: dict[str, Any]) -> tuple[float, float]:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "Point":
        return float(geometry["latitude"]), float(geometry["longitude"])
    if geometry_type == "BBox":
        west = float(geometry["west"])
        south = float(geometry["south"])
        east = float(geometry["east"])
        north = float(geometry["north"])
        return ((south + north) / 2.0, (west + east) / 2.0)
    raise ValueError(f"Unsupported mission geometry type: {geometry_type!r}")


def location_strings_for_geometry(geometry: dict[str, Any]) -> list[str]:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "Point":
        return [f"{float(geometry['latitude']):.6f},{float(geometry['longitude']):.6f}"]
    if geometry_type == "BBox":
        west = float(geometry["west"])
        south = float(geometry["south"])
        east = float(geometry["east"])
        north = float(geometry["north"])
        center_lat, center_lon = center_point_for_geometry(geometry)
        candidates = [
            f"{center_lat:.6f},{center_lon:.6f}",
            f"{north:.6f},{west:.6f}",
            f"{south:.6f},{east:.6f}",
        ]
        return unique_strings(candidates)
    raise ValueError(f"Unsupported mission geometry type: {geometry_type!r}")


def bbox_text_for_geometry(geometry: dict[str, Any], *, point_padding_deg: float) -> str:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "BBox":
        return ",".join(
            [
                f"{float(geometry['west']):.6f}",
                f"{float(geometry['south']):.6f}",
                f"{float(geometry['east']):.6f}",
                f"{float(geometry['north']):.6f}",
            ]
        )
    if geometry_type != "Point":
        raise ValueError(f"Unsupported mission geometry type: {geometry_type!r}")
    latitude = float(geometry["latitude"])
    longitude = float(geometry["longitude"])
    padding = abs(point_padding_deg)
    south = max(-90.0, latitude - padding)
    north = min(90.0, latitude + padding)
    west = max(-180.0, longitude - padding)
    east = min(180.0, longitude + padding)
    return f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"


def source_role(source_skill: str) -> str:
    if source_skill in PUBLIC_SOURCES:
        return "sociologist"
    if source_skill in ENVIRONMENT_SOURCES:
        return "environmentalist"
    raise ValueError(f"Unsupported source skill: {source_skill}")


def default_env_file(skill_name: str) -> Path | None:
    skill_dir = SKILL_DIRS.get(skill_name)
    if skill_dir is None:
        return None
    primary = skill_dir / "assets" / "config.env"
    example = skill_dir / "assets" / "config.example.env"
    if primary.exists():
        return primary
    if example.exists():
        return example
    return None


def shell_join(argv: list[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in argv)


def shell_command(argv: list[str], *, env_file: Path | None = None) -> str:
    lines: list[str] = []
    if env_file is not None:
        lines.extend(
            [
                "set -a",
                f"source {shlex.quote(str(env_file))}",
                "set +a",
            ]
        )
    lines.append(shell_join(argv))
    return "\n".join(lines)


def make_step(
    *,
    step_id: str,
    role: str,
    source_skill: str,
    task_ids: list[str],
    artifact_path: Path,
    stdout_path: Path,
    stderr_path: Path,
    command: str,
    depends_on: list[str],
    notes: list[str],
    skill_refs: list[str],
    cwd: Path,
    artifact_capture: str = "",
    download_dir: Path | None = None,
    quarantine_dir: Path | None = None,
) -> dict[str, Any]:
    step = {
        "step_id": step_id,
        "role": role,
        "source_skill": source_skill,
        "task_ids": task_ids,
        "depends_on": depends_on,
        "artifact_path": str(artifact_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "cwd": str(cwd),
        "command": command,
        "notes": notes,
        "skill_refs": skill_refs,
        "normalizer_input": f"{source_skill}={artifact_path}",
    }
    if artifact_capture:
        step["artifact_capture"] = artifact_capture
    if download_dir is not None:
        step["download_dir"] = str(download_dir)
    if quarantine_dir is not None:
        step["quarantine_dir"] = str(quarantine_dir)
    return step


def new_step_id(role: str, source_skill: str, counter: int) -> str:
    return f"step-{role}-{counter:02d}-{source_skill}"


def regs_task_enabled(tasks: list[dict[str, Any]]) -> bool:
    combined = " ".join(
        [
            task_objective_text(tasks),
            " ".join(merged_task_string_list(tasks, "query_hints")),
            " ".join(merged_task_string_list(tasks, "agency_ids")),
        ]
    ).casefold()
    return any(token in combined for token in ("policy", "regulation", "epa", "docket", "comment"))


def step_task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    return [maybe_text(task.get("task_id")) for task in tasks if maybe_text(task.get("task_id"))]


def build_sociologist_steps(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    source_selection: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    role = "sociologist"
    role_tasks = tasks_for_role(tasks, role)
    if not role_tasks:
        return []
    selected = role_selected_sources(mission=mission, tasks=role_tasks, role=role, source_selection=source_selection)
    if not selected:
        return []
    task_ids = step_task_ids(role_tasks)
    window = window_from_task_or_mission(mission=mission, tasks=role_tasks)
    query_text = build_plain_query(mission=mission, tasks=role_tasks)
    gdelt_query = build_gdelt_query(mission=mission, tasks=role_tasks)
    steps: list[dict[str, Any]] = []
    counter = 0
    prior_step_ids: dict[str, str] = {}
    for source_skill in selected:
        if source_skill not in PUBLIC_SOURCES:
            continue
        counter += 1
        step_id = new_step_id(role, source_skill, counter)
        artifact_path = default_raw_artifact_path(run_dir, round_id, role, source_skill)
        stdout_path = default_step_stdout_path(run_dir, round_id, role, source_skill)
        stderr_path = default_step_stderr_path(run_dir, round_id, role, source_skill)
        env_file = default_env_file(source_skill)
        notes: list[str] = []
        depends_on: list[str] = []
        skill_refs = [f"${source_skill}"]
        artifact_capture = ""
        download_dir: Path | None = None
        quarantine_dir: Path | None = None

        if source_skill == "gdelt-doc-search":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "search",
                "--query",
                gdelt_query,
                "--mode",
                "artlist",
                "--format",
                "json",
                "--start-datetime",
                to_gdelt_datetime(window["start_utc"]),
                "--end-datetime",
                to_gdelt_datetime(window["end_utc"]),
                "--max-records",
                merged_task_scalar(role_tasks, "gdelt_max_records") or "50",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            notes.append("Use GDELT DOC as broad article discovery for public claims.")
        elif source_skill in GDELT_RAW_TABLE_SOURCES:
            doc_step_id = prior_step_ids.get("gdelt-doc-search")
            if doc_step_id:
                depends_on.append(doc_step_id)
            download_dir = default_raw_download_dir(run_dir, round_id, role, source_skill)
            quarantine_dir = default_raw_quarantine_dir(run_dir, round_id, role, source_skill)
            artifact_capture = "stdout-json"
            max_files = (
                merged_task_scalar(role_tasks, GDELT_MAX_FILE_INPUT_KEYS[source_skill])
                or merged_task_scalar(role_tasks, "gdelt_table_max_files")
                or "2"
            )
            preview_lines = (
                merged_task_scalar(role_tasks, GDELT_PREVIEW_LINE_INPUT_KEYS[source_skill])
                or merged_task_scalar(role_tasks, "gdelt_table_preview_lines")
                or "2"
            )
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--mode",
                "range",
                "--start-datetime",
                to_gdelt_datetime(window["start_utc"]),
                "--end-datetime",
                to_gdelt_datetime(window["end_utc"]),
                "--max-files",
                max_files,
                "--output-dir",
                str(download_dir),
                "--overwrite",
                "--preview-lines",
                preview_lines,
                "--validate-structure",
                "--expected-columns",
                GDELT_EXPECTED_COLUMNS[source_skill],
                "--quarantine-dir",
                str(quarantine_dir),
                "--pretty",
            ]
            notes.append(
                "Capture the stdout JSON manifest at the contract artifact path; downloaded ZIP exports remain under the raw sidecar directory."
            )
            if source_skill == "gdelt-events-fetch":
                notes.append("Use GDELT Events as event-level bulk supplement after DOC recon anchors the mission window.")
            elif source_skill == "gdelt-mentions-fetch":
                notes.append("Use GDELT Mentions as mention-volume supplement after DOC recon anchors the mission window.")
            else:
                notes.append("Use GDELT GKG as theme/location/entity supplement after DOC recon anchors the mission window.")
        elif source_skill == "bluesky-cascade-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--source-mode",
                "search",
                "--query",
                query_text,
                "--search-sort",
                "latest",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--max-pages",
                merged_task_scalar(role_tasks, "bluesky_max_pages") or "5",
                "--max-posts",
                merged_task_scalar(role_tasks, "bluesky_max_posts") or "120",
                "--max-threads",
                merged_task_scalar(role_tasks, "bluesky_max_threads") or "40",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            notes.append("Collect seed posts plus cascades for diffusion structure.")
        elif source_skill == "youtube-video-search":
            youtube_comment_count_min = maybe_text(merged_task_scalar(role_tasks, "youtube_comment_count_min"))
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "search",
                "--query",
                query_text,
                "--published-after",
                window["start_utc"],
                "--published-before",
                window["end_utc"],
                "--order",
                "date",
                "--max-pages",
                merged_task_scalar(role_tasks, "youtube_max_pages") or "4",
                "--max-results",
                merged_task_scalar(role_tasks, "youtube_max_results") or "80",
                "--save-records",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            if youtube_comment_count_min:
                argv.extend(["--comment-count-min", youtube_comment_count_min])
                notes.append(
                    f"Apply explicit YouTube comment-count floor >= {youtube_comment_count_min} from task inputs."
                )
            else:
                notes.append(
                    "Do not filter out low-comment videos by default; sparse mission-relevant videos can still form auditable public claims."
                )
            notes.append("Persist candidate video IDs so comment fetch can chain from the saved JSONL artifact.")
        elif source_skill == "youtube-comments-fetch":
            video_ids_file = merged_task_scalar(role_tasks, "youtube_video_ids_file")
            if not video_ids_file:
                dependency_step = prior_step_ids.get("youtube-video-search")
                if dependency_step:
                    depends_on.append(dependency_step)
                    video_ids_file = str(default_raw_artifact_path(run_dir, round_id, role, "youtube-video-search"))
            if not video_ids_file:
                raise ValueError("youtube-comments-fetch requires youtube-video-search output or task.inputs.youtube_video_ids_file.")
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--video-ids-file",
                video_ids_file,
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--time-field",
                "published",
                "--include-replies",
                "--order",
                "time",
                "--max-videos",
                merged_task_scalar(role_tasks, "youtube_max_videos") or "12",
                "--max-thread-pages",
                merged_task_scalar(role_tasks, "youtube_max_thread_pages") or "12",
                "--max-reply-pages",
                merged_task_scalar(role_tasks, "youtube_max_reply_pages") or "12",
                "--max-comments",
                merged_task_scalar(role_tasks, "youtube_max_comments") or "1200",
                "--save-records",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            notes.append("Use the saved YouTube video artifact as the only ID source for comment collection.")
        elif source_skill == "federal-register-doc-fetch":
            federal_register_term = merged_task_scalar(role_tasks, "federal_register_term") or query_text
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--term",
                federal_register_term,
                "--start-date",
                to_date_text(window["start_utc"]),
                "--end-date",
                to_date_text(window["end_utc"]),
                "--order",
                merged_task_scalar(role_tasks, "federal_register_order") or "newest",
                "--page-size",
                merged_task_scalar(role_tasks, "federal_register_page_size") or "25",
                "--max-pages",
                merged_task_scalar(role_tasks, "federal_register_max_pages") or "3",
                "--max-records",
                merged_task_scalar(role_tasks, "federal_register_max_records") or "150",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            for agency in merged_task_string_list(role_tasks, "federal_register_agencies"):
                argv.extend(["--agency", agency])
            for document_type in merged_task_string_list(role_tasks, "federal_register_document_types"):
                argv.extend(["--document-type", document_type])
            for topic in merged_task_string_list(role_tasks, "federal_register_topics"):
                argv.extend(["--topic", topic])
            for section in merged_task_string_list(role_tasks, "federal_register_sections"):
                argv.extend(["--section", section])
            docket_id = merged_task_scalar(role_tasks, "federal_register_docket_id")
            if docket_id:
                argv.extend(["--docket-id", docket_id])
            regulation_id_number = merged_task_scalar(role_tasks, "federal_register_regulation_id_number")
            if regulation_id_number:
                argv.extend(["--regulation-id-number", regulation_id_number])
            significant = merged_task_scalar(role_tasks, "federal_register_significant")
            if significant:
                argv.extend(["--significant", significant])
            for field_name in merged_task_string_list(role_tasks, "federal_register_fields"):
                argv.extend(["--field", field_name])
            notes.append("Use Federal Register for official U.S. rulemaking, notice, and docket-linked policy documents.")
        elif source_skill == "regulationsgov-comments-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--filter-mode",
                "last-modified",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--search-term",
                query_text,
                "--max-pages",
                merged_task_scalar(role_tasks, "reggov_max_pages") or "3",
                "--max-records",
                merged_task_scalar(role_tasks, "reggov_max_records") or "300",
                "--save-response",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            agency_id = merged_task_scalar(role_tasks, "agency_id")
            if agency_id:
                argv.extend(["--agency-id", agency_id])
                notes.append(f"Constrain Regulations.gov discovery to agency_id={agency_id}.")
            else:
                notes.append("Use Regulations.gov only when policy or public-comment coverage is mission relevant.")
        elif source_skill == "regulationsgov-comment-detail-fetch":
            comment_ids_file = merged_task_scalar(role_tasks, "comment_ids_file")
            if not comment_ids_file:
                dependency_step = prior_step_ids.get("regulationsgov-comments-fetch")
                if dependency_step:
                    depends_on.append(dependency_step)
                    comment_ids_file = str(default_raw_artifact_path(run_dir, round_id, role, "regulationsgov-comments-fetch"))
            if not comment_ids_file:
                raise ValueError("regulationsgov-comment-detail-fetch requires comment IDs or Regulations.gov list output.")
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--comment-ids-file",
                comment_ids_file,
                "--max-comments",
                merged_task_scalar(role_tasks, "reggov_max_detail_comments") or "100",
                "--include",
                "attachments",
                "--save-response",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            notes.append("Fetch detail records only after a comment ID list exists.")
        else:
            continue

        steps.append(
            make_step(
                step_id=step_id,
                role=role,
                source_skill=source_skill,
                task_ids=task_ids,
                artifact_path=artifact_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                command=shell_command(argv, env_file=env_file),
                depends_on=depends_on,
                notes=notes,
                skill_refs=skill_refs,
                cwd=SKILL_DIRS.get(source_skill, REPO_DIR),
                artifact_capture=artifact_capture,
                download_dir=download_dir,
                quarantine_dir=quarantine_dir,
            )
        )
        prior_step_ids[source_skill] = step_id
    return steps


def build_environmentalist_steps(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    source_selection: dict[str, Any] | None,
    firms_point_padding_deg: float,
) -> list[dict[str, Any]]:
    role = "environmentalist"
    role_tasks = tasks_for_role(tasks, role)
    if not role_tasks:
        return []
    selected = role_selected_sources(mission=mission, tasks=role_tasks, role=role, source_selection=source_selection)
    if not selected:
        return []
    task_ids = step_task_ids(role_tasks)
    window = window_from_task_or_mission(mission=mission, tasks=role_tasks)
    geometry = geometry_from_task_or_mission(mission=mission, tasks=role_tasks)
    location_values = location_strings_for_geometry(geometry)
    bbox_text = bbox_text_for_geometry(
        geometry,
        point_padding_deg=float(merged_task_scalar(role_tasks, "firms_point_padding_deg") or firms_point_padding_deg),
    )
    airnow_bbox_text = bbox_text_for_geometry(
        geometry,
        point_padding_deg=float(merged_task_scalar(role_tasks, "airnow_point_padding_deg") or DEFAULT_AIRNOW_POINT_PADDING_DEG),
    )
    usgs_bbox_text = bbox_text_for_geometry(
        geometry,
        point_padding_deg=float(merged_task_scalar(role_tasks, "usgs_point_padding_deg") or DEFAULT_USGS_POINT_PADDING_DEG),
    )

    steps: list[dict[str, Any]] = []
    counter = 0
    for source_skill in selected:
        if source_skill not in ENVIRONMENT_SOURCES:
            continue
        counter += 1
        step_id = new_step_id(role, source_skill, counter)
        artifact_path = default_raw_artifact_path(run_dir, round_id, role, source_skill)
        stdout_path = default_step_stdout_path(run_dir, round_id, role, source_skill)
        stderr_path = default_step_stderr_path(run_dir, round_id, role, source_skill)
        notes: list[str] = []
        skill_refs = [f"${source_skill}"]
        env_file = default_env_file(source_skill)

        if source_skill == "airnow-hourly-obs-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                f"--bbox={airnow_bbox_text}",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
            ]
            for parameter_name in merged_task_string_list(role_tasks, "airnow_parameter_names") or DEFAULT_AIRNOW_PARAMETER_NAMES:
                argv.extend(["--parameter", parameter_name])
            argv.extend(
                [
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append(
                "Collect AirNow hourly monitoring-site observations from official file products for the mission bbox and UTC window."
            )
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "usgs-water-iv-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                f"--bbox={usgs_bbox_text}",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--site-type",
                merged_task_scalar(role_tasks, "usgs_site_type") or DEFAULT_USGS_SITE_TYPE,
                "--site-status",
                merged_task_scalar(role_tasks, "usgs_site_status") or DEFAULT_USGS_SITE_STATUS,
            ]
            for parameter_code in merged_task_string_list(role_tasks, "usgs_parameter_codes") or DEFAULT_USGS_PARAMETER_CODES:
                argv.extend(["--parameter-code", parameter_code])
            argv.extend(
                [
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append(
                "Collect USGS station-based hydrology observations for the mission bbox and UTC window."
            )
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "open-meteo-air-quality-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
            ]
            for location in location_values:
                argv.extend(["--location", location])
            argv.extend(
                [
                    "--start-date",
                    to_date_text(window["start_utc"]),
                    "--end-date",
                    to_date_text(window["end_utc"]),
                ]
            )
            for metric in DEFAULT_OPEN_METEO_AIR_VARS:
                argv.extend(["--hourly-var", metric])
            argv.extend(
                [
                    "--domain",
                    merged_task_scalar(role_tasks, "open_meteo_air_domain") or "auto",
                    "--cell-selection",
                    merged_task_scalar(role_tasks, "open_meteo_air_cell_selection") or "nearest",
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append("Collect modeled background air-quality context for the mission geometry.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "open-meteo-historical-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
            ]
            for location in location_values:
                argv.extend(["--location", location])
            argv.extend(
                [
                    "--start-date",
                    to_date_text(window["start_utc"]),
                    "--end-date",
                    to_date_text(window["end_utc"]),
                ]
            )
            for metric in DEFAULT_OPEN_METEO_HIST_HOURLY_VARS:
                argv.extend(["--hourly-var", metric])
            for metric in DEFAULT_OPEN_METEO_HIST_DAILY_VARS:
                argv.extend(["--daily-var", metric])
            argv.extend(
                [
                    "--timezone",
                    "GMT",
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append("Collect meteorology and soil variables for physical verification.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "open-meteo-flood-fetch":
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
            ]
            for location in location_values:
                argv.extend(["--location", location])
            argv.extend(
                [
                    "--start-date",
                    to_date_text(window["start_utc"]),
                    "--end-date",
                    to_date_text(window["end_utc"]),
                ]
            )
            for metric in merged_task_string_list(role_tasks, "open_meteo_flood_daily_vars") or DEFAULT_OPEN_METEO_FLOOD_DAILY_VARS:
                argv.extend(["--daily-var", metric])
            if text_truthy(merged_task_scalar(role_tasks, "open_meteo_flood_ensemble")):
                argv.append("--ensemble")
            argv.extend(
                [
                    "--cell-selection",
                    merged_task_scalar(role_tasks, "open_meteo_flood_cell_selection") or "nearest",
                    "--timezone",
                    merged_task_scalar(role_tasks, "open_meteo_flood_timezone") or "GMT",
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append("Collect hydrology and flood-background discharge signals for the mission geometry.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "nasa-firms-fire-fetch":
            selected_firms_source = firms_source_for_window(
                window,
                merged_task_scalar(role_tasks, "firms_source") or "VIIRS_NOAA20_NRT",
            )
            argv = [
                "python3",
                str(FETCH_SCRIPT_PATHS[source_skill]),
                "fetch",
                "--source",
                selected_firms_source,
                f"--bbox={bbox_text}",
                "--start-date",
                to_date_text(window["start_utc"]),
                "--end-date",
                to_date_text(window["end_utc"]),
                "--check-availability",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            notes.append("Collect fire detections for the mission bbox. Point missions are expanded by a deterministic bbox padding.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "openaq-data-fetch":
            argv = [
                "python3",
                str(SCRIPT_DIR / "eco_council_orchestrate.py"),
                "collect-openaq",
                "--run-dir",
                str(run_dir),
                "--round-id",
                round_id,
                "--output",
                str(artifact_path),
                "--task-role",
                role,
                "--max-locations",
                merged_task_scalar(role_tasks, "openaq_max_locations") or "4",
                "--max-sensors-per-location",
                merged_task_scalar(role_tasks, "openaq_max_sensors_per_location") or "3",
                "--max-pages",
                merged_task_scalar(role_tasks, "openaq_max_pages") or "5",
                "--radius-meters",
                merged_task_scalar(role_tasks, "openaq_radius_meters") or "25000",
            ]
            for parameter_name in merged_task_string_list(role_tasks, "openaq_parameter_names") or DEFAULT_OPENAQ_PARAMETER_NAMES:
                argv.extend(["--parameter-name", parameter_name])
            argv.append("--pretty")
            notes.append("Collect OpenAQ station measurements through location discovery, sensor discovery, and measurement fetch aggregation.")
            command = shell_command(argv, env_file=None)
        else:
            continue

        steps.append(
            make_step(
                step_id=step_id,
                role=role,
                source_skill=source_skill,
                task_ids=task_ids,
                artifact_path=artifact_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                command=command,
                depends_on=[],
                notes=notes,
                skill_refs=skill_refs,
                cwd=REPO_DIR,
            )
        )
    return steps


def build_fetch_plan(
    *,
    run_dir: Path,
    round_id: str,
    firms_point_padding_deg: float,
) -> dict[str, Any]:
    mission = load_mission(run_dir)
    tasks = load_tasks(run_dir, round_id)
    sociologist_tasks = tasks_for_role(tasks, "sociologist")
    environmentalist_tasks = tasks_for_role(tasks, "environmentalist")
    sociologist_selection = load_source_selection(run_dir, round_id, "sociologist")
    environmentalist_selection = load_source_selection(run_dir, round_id, "environmentalist")

    steps: list[dict[str, Any]] = []
    steps.extend(
        build_sociologist_steps(
            run_dir=run_dir,
            round_id=round_id,
            mission=mission,
            tasks=tasks,
            source_selection=sociologist_selection,
        )
    )
    steps.extend(
        build_environmentalist_steps(
            run_dir=run_dir,
            round_id=round_id,
            mission=mission,
            tasks=tasks,
            source_selection=environmentalist_selection,
            firms_point_padding_deg=firms_point_padding_deg,
        )
    )
    return {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "policy_profile": policy_profile_summary(mission),
        "effective_constraints": effective_constraints(mission),
        "input_snapshot": fetch_plan_input_snapshot(
            run_dir=run_dir,
            round_id=round_id,
            sociologist_selection=sociologist_selection,
            environmentalist_selection=environmentalist_selection,
        ),
        "run": {
            "run_id": maybe_text(mission.get("run_id")),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region_label": maybe_text(mission_region(mission).get("label")),
            "window": mission_window(mission),
        },
        "roles": {
            "sociologist": {
                "task_ids": step_task_ids(sociologist_tasks),
                "objective": task_objective_text(sociologist_tasks),
                "allowed_sources": allowed_sources_for_role(mission, "sociologist"),
                "evidence_requirements": [
                    requirement
                    for task in sociologist_tasks
                    for requirement in (
                        task_inputs(task).get("evidence_requirements")
                        if isinstance(task_inputs(task).get("evidence_requirements"), list)
                        else []
                    )
                    if isinstance(requirement, dict)
                ],
                "governed_families": [
                    maybe_text(family.get("family_id"))
                    for family in role_source_governance(mission, "sociologist").get("families", [])
                    if isinstance(family, dict) and maybe_text(family.get("family_id"))
                ],
                "override_requests": load_override_requests(run_dir, round_id, "sociologist"),
                "source_selection_path": str(source_selection_path(run_dir, round_id, "sociologist")),
                "source_selection_status": maybe_text((sociologist_selection or {}).get("status")),
                "selected_sources": role_selected_sources(
                    mission=mission,
                    tasks=sociologist_tasks,
                    role="sociologist",
                    source_selection=sociologist_selection,
                ),
            },
            "environmentalist": {
                "task_ids": step_task_ids(environmentalist_tasks),
                "objective": task_objective_text(environmentalist_tasks),
                "allowed_sources": allowed_sources_for_role(mission, "environmentalist"),
                "evidence_requirements": [
                    requirement
                    for task in environmentalist_tasks
                    for requirement in (
                        task_inputs(task).get("evidence_requirements")
                        if isinstance(task_inputs(task).get("evidence_requirements"), list)
                        else []
                    )
                    if isinstance(requirement, dict)
                ],
                "governed_families": [
                    maybe_text(family.get("family_id"))
                    for family in role_source_governance(mission, "environmentalist").get("families", [])
                    if isinstance(family, dict) and maybe_text(family.get("family_id"))
                ],
                "override_requests": load_override_requests(run_dir, round_id, "environmentalist"),
                "source_selection_path": str(source_selection_path(run_dir, round_id, "environmentalist")),
                "source_selection_status": maybe_text((environmentalist_selection or {}).get("status")),
                "selected_sources": role_selected_sources(
                    mission=mission,
                    tasks=environmentalist_tasks,
                    role="environmentalist",
                    source_selection=environmentalist_selection,
                ),
            },
        },
        "steps": steps,
    }


def render_moderator_task_review_prompt(*, run_dir: Path, round_id: str) -> Path:
    mission_path = run_dir / "mission.json"
    tasks_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    validate_command = shell_join(
        [
            "python3",
            str(CONTRACT_SCRIPT),
            "validate",
            "--kind",
            "round-task",
            "--input",
            str(tasks_path),
        ]
    )
    lines = [
        "Use $eco-council-data-contract.",
        f"Open mission at: {mission_path}",
        f"Open current task list at: {tasks_path}",
        "",
        "Review and, if needed, revise the round-task list before expert fetch work begins.",
        "Requirements:",
        "1. Keep the file as a JSON list of valid round-task objects.",
        "2. Keep run_id and round_id unchanged.",
        "3. Use only moderator-owned task assignment; do not write claims, observations, evidence cards, or reports here.",
        "4. Use task.inputs.evidence_requirements to describe evidence gaps, claim focus, and priority instead of prescribing concrete source skills.",
        "5. Leave exact source-family, layer, and source-skill choice to the expert source-selection stage under mission governance.",
        "6. Keep objectives concrete enough that sociologist and environmentalist can choose and fetch raw artifacts deterministically.",
        "",
        "After editing, validate with:",
        validate_command,
        "",
        "Return only the final JSON list.",
    ]
    output_path = task_review_prompt_path(run_dir, round_id)
    write_text(output_path, "\n".join(lines))
    return output_path


def render_role_fetch_prompt(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    plan: dict[str, Any],
) -> Path | None:
    tasks = load_tasks(run_dir, round_id)
    role_tasks = tasks_for_role(tasks, role)
    if not role_tasks:
        return None
    steps = [step for step in plan.get("steps", []) if maybe_text(step.get("role")) == role]
    if not steps:
        return None

    mission = load_mission(run_dir)
    mission_window_value = mission_window(mission)
    objective_lines = [f"- {maybe_text(task.get('task_id'))}: {maybe_text(task.get('objective'))}" for task in role_tasks]
    referenced_skills = unique_strings(
        [skill_ref for step in steps for skill_ref in step.get("skill_refs", []) if maybe_text(skill_ref)]
    )
    lines = [
        f"You are the {role} for {maybe_text(mission.get('run_id'))} {round_id}.",
        "",
        "Mission:",
        f"- topic: {maybe_text(mission.get('topic'))}",
        f"- objective: {maybe_text(mission.get('objective'))}",
        f"- region: {maybe_text(mission_region(mission).get('label'))}",
        f"- window_start_utc: {mission_window_value['start_utc']}",
        f"- window_end_utc: {mission_window_value['end_utc']}",
        "",
        "Assigned tasks:",
        *objective_lines,
        "",
        "Relevant skills:",
        ", ".join(referenced_skills) if referenced_skills else "$eco-council-orchestrate",
        "",
        "Execution rules:",
        "1. Execute only the shell commands listed below for your role.",
        "2. Keep raw outputs exactly at the specified artifact paths. Those files are the contract boundary for normalization.",
        "3. Do not create claims, observations, evidence cards, or expert reports in this phase.",
        "4. If you intentionally rerun a step, overwrite only the artifact paths already listed in the plan.",
        "5. After all commands complete, return only JSON summarizing artifact paths and any blockers.",
        "",
    ]
    for step in steps:
        lines.extend(
            [
                f"Step: {maybe_text(step.get('step_id'))}",
                f"Source skill: {maybe_text(step.get('source_skill'))}",
                f"Artifact path: {maybe_text(step.get('artifact_path'))}",
            ]
        )
        if isinstance(step.get("depends_on"), list) and step.get("depends_on"):
            lines.append(f"Depends on: {', '.join(step['depends_on'])}")
        if isinstance(step.get("notes"), list):
            for note in step["notes"]:
                note_text = maybe_text(note)
                if note_text:
                    lines.append(f"Note: {note_text}")
        command_text = step.get("command")
        if not isinstance(command_text, str) or not command_text.strip():
            command_text = "# missing command"
        lines.extend(["Command:", "```bash", command_text, "```", ""])

    lines.extend(
        [
            "Return JSON only with this shape:",
            "```json",
            "{",
            f'  "role": "{role}",',
            f'  "round_id": "{round_id}",',
            '  "status": "raw-data-ready",',
            '  "artifacts": ["..."],',
            '  "notes": []',
            "}",
            "```",
        ]
    )
    output_path = fetch_prompt_path(run_dir, round_id, role)
    write_text(output_path, "\n".join(lines))
    return output_path


def write_round_manifest(
    *,
    run_dir: Path,
    round_id: str,
    stage: str,
    task_prompt: Path | None,
    fetch_plan: Path | None,
    fetch_prompts: dict[str, str],
) -> Path:
    prepare_command = shell_join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_orchestrate.py"),
            "prepare-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    data_plane_command = shell_join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_orchestrate.py"),
            "run-data-plane",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    matching_command = shell_join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_orchestrate.py"),
            "run-matching-adjudication",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    execute_fetch_command = shell_join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_orchestrate.py"),
            "execute-fetch-plan",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    manifest = {
        "manifest_kind": "eco-council-round-manifest",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "stage": stage,
        "run_dir": str(run_dir),
        "round_id": round_id,
        "task_review_prompt_path": str(task_prompt) if task_prompt is not None else "",
        "fetch_plan_path": str(fetch_plan) if fetch_plan is not None else "",
        "role_fetch_prompt_paths": fetch_prompts,
        "next_commands": {
            "prepare_round": prepare_command,
            "run_data_plane": data_plane_command,
            "run_matching_adjudication": matching_command,
            "execute_fetch_plan": execute_fetch_command,
        },
    }
    output_path = round_manifest_path(run_dir, round_id)
    write_json(output_path, manifest, pretty=True)
    return output_path


def strip_inline_comment(text: str) -> str:
    chars: list[str] = []
    in_single = False
    in_double = False
    escape = False
    for char in text:
        if escape:
            chars.append(char)
            escape = False
            continue
        if char == "\\" and not in_single:
            chars.append(char)
            escape = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            break
        chars.append(char)
    return "".join(chars).strip()


def parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise ValueError(f"Environment file does not exist: {path}")
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        clean = strip_inline_comment(line)
        if not clean:
            continue
        match = ENV_ASSIGNMENT_PATTERN.match(clean)
        if match is None:
            continue
        key, raw_value = match.groups()
        value = raw_value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
        elif value.startswith("'") and value.endswith("'") and len(value) >= 2:
            value = value[1:-1]
        env[key] = value
    return env


def run_json_cli(argv: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        detail = stderr or stdout or f"exit={completed.returncode}"
        raise RuntimeError(f"Command failed: {shell_join(argv)} :: {detail}")
    stdout_text = completed.stdout.strip()
    if not stdout_text:
        raise RuntimeError(f"Command produced no JSON output: {shell_join(argv)}")
    try:
        payload = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Command did not emit valid JSON: {shell_join(argv)}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Command JSON output must be an object: {shell_join(argv)}")
    return payload


def materialize_json_artifact_from_stdout(*, artifact_path: Path, stdout_path: Path) -> bool:
    if not stdout_path.exists():
        return False
    try:
        payload = read_json(stdout_path)
    except Exception:
        return False
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(artifact_path, payload, pretty=True)
    return artifact_path.exists()


def validate_json_artifact_if_applicable(artifact_path: Path | None) -> str:
    if artifact_path is None:
        return ""
    if artifact_path.suffix.casefold() != ".json":
        return ""
    try:
        read_json(artifact_path)
    except Exception as exc:  # noqa: BLE001
        return str(exc)
    return ""


def build_fetch_execution_payload(
    *,
    run_dir: Path,
    round_id: str,
    plan_path: Path,
    plan_sha256: str,
    step_count: int,
    statuses: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "updated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "plan_path": str(plan_path),
        "plan_sha256": plan_sha256,
        "step_count": step_count,
        "completed_count": sum(1 for status in statuses if status.get("status") == "completed"),
        "failed_count": sum(1 for status in statuses if status.get("status") == "failed"),
        "statuses": statuses,
    }


def write_fetch_execution_snapshot(
    *,
    execution_path: Path,
    run_dir: Path,
    round_id: str,
    plan_path: Path,
    plan_sha256: str,
    step_count: int,
    statuses: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = build_fetch_execution_payload(
        run_dir=run_dir,
        round_id=round_id,
        plan_path=plan_path,
        plan_sha256=plan_sha256,
        step_count=step_count,
        statuses=statuses,
    )
    write_json(execution_path, payload, pretty=True)
    payload["execution_path"] = str(execution_path)
    return payload


def build_data_plane_execution_payload(
    *,
    run_dir: Path,
    round_id: str,
    public_inputs: list[str],
    environment_inputs: list[str],
    step_count: int,
    statuses: list[dict[str, Any]],
    reporting_handoff_path: Path | None,
) -> dict[str, Any]:
    payload = {
        "updated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "step_count": step_count,
        "completed_count": sum(1 for status in statuses if status.get("status") == "completed"),
        "failed_count": sum(1 for status in statuses if status.get("status") == "failed"),
        "public_inputs": public_inputs,
        "environment_inputs": environment_inputs,
        "statuses": statuses,
        "reporting_handoff_path": str(reporting_handoff_path) if reporting_handoff_path is not None else "",
    }
    first_failed = next((status for status in statuses if status.get("status") == "failed"), None)
    if isinstance(first_failed, dict):
        payload["failed_step_id"] = maybe_text(first_failed.get("step_id"))
    return payload


def write_data_plane_execution_snapshot(
    *,
    execution_path: Path,
    run_dir: Path,
    round_id: str,
    public_inputs: list[str],
    environment_inputs: list[str],
    step_count: int,
    statuses: list[dict[str, Any]],
    reporting_handoff_path: Path | None,
) -> dict[str, Any]:
    payload = build_data_plane_execution_payload(
        run_dir=run_dir,
        round_id=round_id,
        public_inputs=public_inputs,
        environment_inputs=environment_inputs,
        step_count=step_count,
        statuses=statuses,
        reporting_handoff_path=reporting_handoff_path,
    )
    write_json(execution_path, payload, pretty=True)
    payload["execution_path"] = str(execution_path)
    return payload


def ensure_ok_envelope(payload: dict[str, Any], label: str) -> dict[str, Any]:
    if payload.get("ok") is False:
        raise RuntimeError(f"{label} returned ok=false: {payload}")
    result = payload.get("payload")
    if isinstance(result, dict):
        return result
    return payload


def prepare_round(
    *,
    run_dir: Path,
    round_id: str,
    firms_point_padding_deg: float,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    task_prompt = render_moderator_task_review_prompt(run_dir=run_path, round_id=current_round_id)
    plan = build_fetch_plan(run_dir=run_path, round_id=current_round_id, firms_point_padding_deg=firms_point_padding_deg)
    plan_output_path = fetch_plan_path(run_path, current_round_id)
    write_json(plan_output_path, plan, pretty=True)

    prompt_paths: dict[str, str] = {}
    for role in ("sociologist", "environmentalist"):
        path = render_role_fetch_prompt(run_dir=run_path, round_id=current_round_id, role=role, plan=plan)
        if path is not None:
            prompt_paths[role] = str(path)

    manifest_path = write_round_manifest(
        run_dir=run_path,
        round_id=current_round_id,
        stage="fetch-ready",
        task_prompt=task_prompt,
        fetch_plan=plan_output_path,
        fetch_prompts=prompt_paths,
    )
    return {
        "run_dir": str(run_path),
        "round_id": current_round_id,
        "fetch_plan_path": str(plan_output_path),
        "task_review_prompt_path": str(task_prompt),
        "role_fetch_prompt_paths": prompt_paths,
        "manifest_path": str(manifest_path),
        "step_count": len(plan.get("steps", [])),
    }


def execute_fetch_plan(
    *,
    run_dir: Path,
    round_id: str,
    continue_on_error: bool,
    skip_existing: bool,
    timeout_seconds: int,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    with exclusive_file_lock(fetch_lock_path(run_path, current_round_id)):
        plan_path = fetch_plan_path(run_path, current_round_id)
        plan = ensure_object(read_json(plan_path), f"{plan_path}")
        ensure_fetch_plan_inputs_match(run_dir=run_path, round_id=current_round_id, plan=plan)
        steps = ensure_object_list(plan.get("steps"), f"{plan_path}.steps")
        execution_path = fetch_execution_path(run_path, current_round_id)
        plan_sha256 = file_sha256(plan_path)

        statuses: list[dict[str, Any]] = []
        succeeded: set[str] = set()

        def snapshot() -> dict[str, Any]:
            return write_fetch_execution_snapshot(
                execution_path=execution_path,
                run_dir=run_path,
                round_id=current_round_id,
                plan_path=plan_path,
                plan_sha256=plan_sha256,
                step_count=len(steps),
                statuses=statuses,
            )

        snapshot()
        for step in steps:
            step_id = maybe_text(step.get("step_id"))
            role = maybe_text(step.get("role"))
            artifact_text = str(step.get("artifact_path") or "").strip()
            artifact_path = Path(artifact_text).expanduser().resolve() if artifact_text else None
            stdout_path = Path(maybe_text(step.get("stdout_path"))).expanduser().resolve()
            stderr_path = Path(maybe_text(step.get("stderr_path"))).expanduser().resolve()
            cwd = Path(maybe_text(step.get("cwd")) or str(REPO_DIR)).expanduser().resolve()
            depends_on = [maybe_text(item) for item in step.get("depends_on", []) if maybe_text(item)]
            artifact_capture = maybe_text(step.get("artifact_capture"))
            raw_command = step.get("command")
            if not isinstance(raw_command, str) or not raw_command.strip():
                raise ValueError(f"Fetch step {step_id} is missing a shell command.")
            command = raw_command.strip()

            if any(dep not in succeeded for dep in depends_on):
                status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "skipped",
                    "reason": f"Unmet dependencies: {depends_on}",
                }
                statuses.append(status)
                snapshot()
                if not continue_on_error:
                    break
                continue

            existing_artifact_invalid = ""
            if artifact_path is not None and artifact_path.exists():
                existing_artifact_invalid = validate_json_artifact_if_applicable(artifact_path)

            if skip_existing and artifact_path is not None and artifact_path.exists() and not existing_artifact_invalid:
                statuses.append(
                    {
                        "step_id": step_id,
                        "role": role,
                        "source_skill": maybe_text(step.get("source_skill")),
                        "status": "skipped",
                        "reason": "artifact_exists",
                        "artifact_path": str(artifact_path),
                    }
                )
                succeeded.add(step_id)
                snapshot()
                continue

            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
                try:
                    completed = subprocess.run(
                        ["/bin/bash", "-lc", command],
                        cwd=str(cwd),
                        check=False,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                        text=True,
                        timeout=timeout_seconds,
                    )
                    returncode = completed.returncode
                    timed_out = False
                except subprocess.TimeoutExpired:
                    returncode = 124
                    timed_out = True
            artifact_materialized = False
            if returncode == 0 and artifact_path is not None and artifact_capture == "stdout-json":
                artifact_materialized = materialize_json_artifact_from_stdout(
                    artifact_path=artifact_path,
                    stdout_path=stdout_path,
                )
            artifact_missing = artifact_path is not None and not artifact_path.exists()
            artifact_invalid_json = validate_json_artifact_if_applicable(artifact_path) if returncode == 0 and not artifact_missing else ""
            if returncode == 0 and not artifact_missing and not artifact_invalid_json:
                succeeded.add(step_id)
                completed_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "completed",
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                }
                if artifact_path is not None:
                    completed_status["artifact_path"] = str(artifact_path)
                if artifact_materialized:
                    completed_status["artifact_materialized"] = True
                statuses.append(completed_status)
                snapshot()
                continue

            if artifact_missing:
                failure_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "failed",
                    "reason": "artifact_missing",
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "returncode": returncode,
                    "timed_out": timed_out,
                }
                if artifact_path is not None:
                    failure_status["artifact_path"] = str(artifact_path)
                statuses.append(failure_status)
                snapshot()
                if not continue_on_error:
                    break
                continue

            if artifact_invalid_json:
                failure_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "failed",
                    "reason": "artifact_invalid_json",
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "returncode": returncode,
                    "timed_out": timed_out,
                    "artifact_validation_error": artifact_invalid_json,
                }
                if artifact_path is not None:
                    failure_status["artifact_path"] = str(artifact_path)
                statuses.append(failure_status)
                snapshot()
                if not continue_on_error:
                    break
                continue

            failure_status = {
                "step_id": step_id,
                "role": role,
                "source_skill": maybe_text(step.get("source_skill")),
                "status": "failed",
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "returncode": returncode,
                "timed_out": timed_out,
            }
            if artifact_path is not None:
                failure_status["artifact_path"] = str(artifact_path)
            statuses.append(failure_status)
            snapshot()
            if not continue_on_error:
                break

        return snapshot()


def fetch_status_role(status: dict[str, Any]) -> str:
    role = maybe_text(status.get("role")) or maybe_text(status.get("assigned_role"))
    if role:
        return role
    step_id = maybe_text(status.get("step_id"))
    match = re.match(r"^step-([a-z]+)-", step_id)
    if match is None:
        return ""
    return maybe_text(match.group(1))


def fetch_status_has_usable_artifact(status: dict[str, Any]) -> bool:
    state = maybe_text(status.get("status"))
    if state == "completed":
        return True
    return state == "skipped" and maybe_text(status.get("reason")) == "artifact_exists"


def usable_fetch_artifacts(run_dir: Path, round_id: str, *, role: str) -> tuple[dict[str, Path], bool]:
    payload = load_json_if_exists(fetch_execution_path(run_dir, round_id))
    if not isinstance(payload, dict):
        return {}, False
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        return {}, False
    artifacts: dict[str, Path] = {}
    for status in statuses:
        if not isinstance(status, dict):
            continue
        if fetch_status_role(status) != role or not fetch_status_has_usable_artifact(status):
            continue
        source_skill = maybe_text(status.get("source_skill"))
        artifact_text = maybe_text(status.get("artifact_path"))
        if not source_skill or not artifact_text:
            continue
        artifacts[source_skill] = Path(artifact_text).expanduser().resolve()
    return artifacts, True


def discover_normalize_inputs(run_dir: Path, round_id: str, *, role: str, sources: tuple[str, ...]) -> list[str]:
    input_specs: list[str] = []
    usable_artifacts, _has_execution_record = usable_fetch_artifacts(run_dir, round_id, role=role)
    for source_skill in sources:
        artifact_path = usable_artifacts.get(source_skill)
        if artifact_path is None:
            continue
        if artifact_path.exists():
            input_specs.append(f"{source_skill}={artifact_path}")
    return input_specs


def build_reporting_handoff(*, run_dir: Path, round_id: str) -> Path:
    base_round_dir = round_dir(run_dir, round_id)
    def existing_path(path: Path) -> str:
        return str(path) if path.exists() else ""

    promote_all_command = shell_join(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "promote-all",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    validate_bundle_command = shell_join(
        [
            "python3",
            str(CONTRACT_SCRIPT),
            "validate-bundle",
            "--run-dir",
            str(run_dir),
            "--pretty",
        ]
    )
    build_decision_packet_command = shell_join(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "build-decision-packet",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--prefer-draft-reports",
            "--pretty",
        ]
    )
    run_matching_adjudication_command = shell_join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_orchestrate.py"),
            "run-matching-adjudication",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    advance_round_command = shell_join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_orchestrate.py"),
            "advance-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    handoff = {
        "handoff_kind": "eco-council-reporting-handoff",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "data_readiness_prompt_paths": {
            "sociologist": existing_path(base_round_dir / "sociologist" / "derived" / "openclaw_data_readiness_prompt.txt"),
            "environmentalist": existing_path(base_round_dir / "environmentalist" / "derived" / "openclaw_data_readiness_prompt.txt"),
        },
        "matching_authorization_prompt_path": existing_path(base_round_dir / "moderator" / "derived" / "openclaw_matching_authorization_prompt.txt"),
        "expert_report_prompt_paths": {
            "sociologist": existing_path(base_round_dir / "sociologist" / "derived" / "openclaw_report_prompt.txt"),
            "environmentalist": existing_path(base_round_dir / "environmentalist" / "derived" / "openclaw_report_prompt.txt"),
        },
        "decision_prompt_path": existing_path(base_round_dir / "moderator" / "derived" / "openclaw_decision_prompt.txt"),
        "draft_paths": {
            "sociologist_data_readiness": existing_path(base_round_dir / "sociologist" / "derived" / "sociologist_data_readiness_draft.json"),
            "environmentalist_data_readiness": existing_path(base_round_dir / "environmentalist" / "derived" / "environmentalist_data_readiness_draft.json"),
            "matching_authorization": existing_path(base_round_dir / "moderator" / "derived" / "matching_authorization_draft.json"),
            "sociologist_report": existing_path(base_round_dir / "sociologist" / "derived" / "sociologist_report_draft.json"),
            "environmentalist_report": existing_path(base_round_dir / "environmentalist" / "derived" / "environmentalist_report_draft.json"),
            "moderator_decision": existing_path(base_round_dir / "moderator" / "derived" / "council_decision_draft.json"),
        },
        "promotion_commands": {
            "promote_all": promote_all_command,
            "build_decision_packet": build_decision_packet_command,
            "run_matching_adjudication": run_matching_adjudication_command,
            "validate_bundle": validate_bundle_command,
            "advance_round": advance_round_command,
        },
    }
    output_path = reporting_handoff_path(run_dir, round_id)
    write_json(output_path, handoff, pretty=True)
    return output_path


def run_data_plane_json_step(
    *,
    step_id: str,
    label: str,
    argv: list[str],
    cwd: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    started_at_utc = utc_now_iso()
    try:
        result = ensure_ok_envelope(run_json_cli(argv, cwd=cwd), label)
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "completed",
                "command": shell_join(argv),
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "result": result,
            },
            result,
        )
    except Exception as exc:  # noqa: BLE001
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "failed",
                "command": shell_join(argv),
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "error": truncate_text(str(exc), 4000),
            },
            None,
        )


def run_data_plane_callable_step(
    *,
    step_id: str,
    label: str,
    callback: Any,
) -> tuple[dict[str, Any], Any | None]:
    started_at_utc = utc_now_iso()
    try:
        result = callback()
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "completed",
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "result": result,
            },
            result,
        )
    except Exception as exc:  # noqa: BLE001
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "failed",
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "error": truncate_text(str(exc), 4000),
            },
            None,
        )


def run_data_plane(*, run_dir: Path, round_id: str) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    public_inputs = discover_normalize_inputs(run_path, current_round_id, role="sociologist", sources=PUBLIC_SOURCES)
    environment_inputs = discover_normalize_inputs(run_path, current_round_id, role="environmentalist", sources=ENVIRONMENT_SOURCES)
    execution_path = data_plane_execution_path(run_path, current_round_id)
    reporting_handoff: Path | None = None
    statuses: list[dict[str, Any]] = []
    step_count = 8

    def snapshot() -> dict[str, Any]:
        return write_data_plane_execution_snapshot(
            execution_path=execution_path,
            run_dir=run_path,
            round_id=current_round_id,
            public_inputs=public_inputs,
            environment_inputs=environment_inputs,
            step_count=step_count,
            statuses=statuses,
            reporting_handoff_path=reporting_handoff,
        )

    snapshot()

    def append_status_or_raise(status: dict[str, Any], result: Any | None) -> Any:
        statuses.append(status)
        snapshot()
        if result is None:
            raise RuntimeError(
                f"Data plane failed at {maybe_text(status.get('step_id'))}. "
                f"Inspect {execution_path}. {maybe_text(status.get('error'))}"
            )
        return result

    init_status, init_payload = run_data_plane_json_step(
        step_id="normalize-init-run",
        label="normalize init-run",
        argv=["python3", str(NORMALIZE_SCRIPT), "init-run", "--run-dir", str(run_path), "--round-id", current_round_id],
    )
    init_payload = append_status_or_raise(init_status, init_payload)

    normalize_public_cmd = ["python3", str(NORMALIZE_SCRIPT), "normalize-public", "--run-dir", str(run_path), "--round-id", current_round_id]
    for input_spec in public_inputs:
        normalize_public_cmd.extend(["--input", input_spec])
    public_status, public_payload = run_data_plane_json_step(
        step_id="normalize-public",
        label="normalize public",
        argv=normalize_public_cmd,
    )
    public_payload = append_status_or_raise(public_status, public_payload)

    normalize_environment_cmd = [
        "python3",
        str(NORMALIZE_SCRIPT),
        "normalize-environment",
        "--run-dir",
        str(run_path),
        "--round-id",
        current_round_id,
    ]
    for input_spec in environment_inputs:
        normalize_environment_cmd.extend(["--input", input_spec])
    environment_status, environment_payload = run_data_plane_json_step(
        step_id="normalize-environment",
        label="normalize environment",
        argv=normalize_environment_cmd,
    )
    environment_payload = append_status_or_raise(environment_status, environment_payload)

    context_status, context_payload = run_data_plane_json_step(
        step_id="build-round-context",
        label="build round context",
        argv=["python3", str(NORMALIZE_SCRIPT), "build-round-context", "--run-dir", str(run_path), "--round-id", current_round_id],
    )
    context_payload = append_status_or_raise(context_status, context_payload)

    reporting_status, reporting_payload = run_data_plane_json_step(
        step_id="reporting-build-data-readiness-packets",
        label="reporting build data readiness packets",
        argv=[
            "python3",
            str(REPORTING_SCRIPT),
            "build-data-readiness-packets",
            "--run-dir",
            str(run_path),
            "--round-id",
            current_round_id,
        ],
    )
    reporting_payload = append_status_or_raise(reporting_status, reporting_payload)

    prompt_status, prompt_payload = run_data_plane_json_step(
        step_id="render-openclaw-prompts",
        label="render openclaw prompts",
        argv=[
            "python3",
            str(REPORTING_SCRIPT),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_path),
            "--round-id",
            current_round_id,
        ],
    )
    prompt_payload = append_status_or_raise(prompt_status, prompt_payload)

    bundle_status, bundle_payload = run_data_plane_json_step(
        step_id="validate-bundle",
        label="validate bundle",
        argv=["python3", str(CONTRACT_SCRIPT), "validate-bundle", "--run-dir", str(run_path)],
    )
    bundle_payload = append_status_or_raise(bundle_status, bundle_payload)

    handoff_status, handoff_payload = run_data_plane_callable_step(
        step_id="build-reporting-handoff",
        label="build reporting handoff",
        callback=lambda: {"reporting_handoff_path": str(build_reporting_handoff(run_dir=run_path, round_id=current_round_id))},
    )
    handoff_payload = append_status_or_raise(handoff_status, handoff_payload)
    reporting_handoff = Path(maybe_text(handoff_payload.get("reporting_handoff_path"))).expanduser().resolve()
    snapshot()

    return {
        "run_dir": str(run_path),
        "round_id": current_round_id,
        "public_inputs": public_inputs,
        "environment_inputs": environment_inputs,
        "normalize_init": init_payload,
        "normalize_public": public_payload,
        "normalize_environment": environment_payload,
        "build_context": context_payload,
        "reporting": reporting_payload,
        "prompt_render": prompt_payload,
        "bundle_validation": bundle_payload,
        "reporting_handoff_path": str(reporting_handoff),
        "execution_path": str(execution_path),
    }


def run_matching_adjudication(*, run_dir: Path, round_id: str) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    authorization = load_json_if_exists(matching_authorization_path(run_path, current_round_id))
    if not isinstance(authorization, dict):
        raise ValueError(
            f"Matching/adjudication requires canonical moderator matching_authorization.json: "
            f"{matching_authorization_path(run_path, current_round_id)}"
        )
    if maybe_text(authorization.get("authorization_status")) != "authorized":
        raise ValueError(
            "Matching/adjudication is only allowed after moderator authorization_status=authorized. "
            f"Current status={authorization.get('authorization_status')!r}."
        )
    execution_path = matching_execution_path(run_path, current_round_id)
    reporting_handoff: Path | None = None
    statuses: list[dict[str, Any]] = []
    step_count = 6

    def snapshot() -> dict[str, Any]:
        return write_data_plane_execution_snapshot(
            execution_path=execution_path,
            run_dir=run_path,
            round_id=current_round_id,
            public_inputs=[],
            environment_inputs=[],
            step_count=step_count,
            statuses=statuses,
            reporting_handoff_path=reporting_handoff,
        )

    snapshot()

    def append_status_or_raise(status: dict[str, Any], result: Any | None) -> Any:
        statuses.append(status)
        snapshot()
        if result is None:
            raise RuntimeError(
                f"Matching/adjudication failed at {maybe_text(status.get('step_id'))}. "
                f"Inspect {execution_path}. {maybe_text(status.get('error'))}"
            )
        return result

    evidence_status, evidence_payload = run_data_plane_json_step(
        step_id="link-evidence",
        label="link evidence",
        argv=["python3", str(NORMALIZE_SCRIPT), "link-evidence", "--run-dir", str(run_path), "--round-id", current_round_id],
    )
    evidence_payload = append_status_or_raise(evidence_status, evidence_payload)

    context_status, context_payload = run_data_plane_json_step(
        step_id="build-round-context",
        label="build round context",
        argv=["python3", str(NORMALIZE_SCRIPT), "build-round-context", "--run-dir", str(run_path), "--round-id", current_round_id],
    )
    context_payload = append_status_or_raise(context_status, context_payload)

    reporting_status, reporting_payload = run_data_plane_json_step(
        step_id="reporting-build-report-packets",
        label="reporting build report packets",
        argv=[
            "python3",
            str(REPORTING_SCRIPT),
            "build-report-packets",
            "--run-dir",
            str(run_path),
            "--round-id",
            current_round_id,
        ],
    )
    reporting_payload = append_status_or_raise(reporting_status, reporting_payload)

    prompt_status, prompt_payload = run_data_plane_json_step(
        step_id="render-openclaw-prompts",
        label="render openclaw prompts",
        argv=[
            "python3",
            str(REPORTING_SCRIPT),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_path),
            "--round-id",
            current_round_id,
        ],
    )
    prompt_payload = append_status_or_raise(prompt_status, prompt_payload)

    bundle_status, bundle_payload = run_data_plane_json_step(
        step_id="validate-bundle",
        label="validate bundle",
        argv=["python3", str(CONTRACT_SCRIPT), "validate-bundle", "--run-dir", str(run_path)],
    )
    bundle_payload = append_status_or_raise(bundle_status, bundle_payload)

    handoff_status, handoff_payload = run_data_plane_callable_step(
        step_id="build-reporting-handoff",
        label="build reporting handoff",
        callback=lambda: {"reporting_handoff_path": str(build_reporting_handoff(run_dir=run_path, round_id=current_round_id))},
    )
    handoff_payload = append_status_or_raise(handoff_status, handoff_payload)
    reporting_handoff = Path(maybe_text(handoff_payload.get("reporting_handoff_path"))).expanduser().resolve()
    snapshot()

    return {
        "run_dir": str(run_path),
        "round_id": current_round_id,
        "link_evidence": evidence_payload,
        "build_context": context_payload,
        "reporting": reporting_payload,
        "prompt_render": prompt_payload,
        "bundle_validation": bundle_payload,
        "reporting_handoff_path": str(reporting_handoff),
        "execution_path": str(execution_path),
    }


def command_bootstrap_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if run_dir.exists() and any(run_dir.iterdir()) and not args.allow_existing:
        raise ValueError(f"Run directory already exists and is not empty: {run_dir}. Use --allow-existing to proceed.")
    cmd = [
        "python3",
        str(CONTRACT_SCRIPT),
        "scaffold-run-from-mission",
        "--run-dir",
        str(run_dir),
        "--mission-input",
        str(Path(args.mission_input).expanduser().resolve()),
    ]
    if args.tasks_input:
        cmd.extend(["--tasks-input", str(Path(args.tasks_input).expanduser().resolve())])
    contract_payload = ensure_ok_envelope(run_json_cli(cmd), "scaffold-run-from-mission")
    round_id = maybe_text(contract_payload.get("round_id")) or "round-001"
    task_prompt = render_moderator_task_review_prompt(run_dir=run_dir, round_id=round_id)
    manifest_path = write_round_manifest(
        run_dir=run_dir,
        round_id=round_id,
        stage="task-review",
        task_prompt=task_prompt,
        fetch_plan=None,
        fetch_prompts={},
    )
    bundle_payload = ensure_ok_envelope(
        run_json_cli(["python3", str(CONTRACT_SCRIPT), "validate-bundle", "--run-dir", str(run_dir)]),
        "validate bundle",
    )
    return {
        "run_dir": str(run_dir),
        "round_id": round_id,
        "contract": contract_payload,
        "task_review_prompt_path": str(task_prompt),
        "manifest_path": str(manifest_path),
        "bundle_validation": bundle_payload,
    }


def command_prepare_round(args: argparse.Namespace) -> dict[str, Any]:
    return prepare_round(
        run_dir=Path(args.run_dir),
        round_id=args.round_id,
        firms_point_padding_deg=args.firms_point_padding_deg,
    )


def command_execute_fetch_plan(args: argparse.Namespace) -> dict[str, Any]:
    return execute_fetch_plan(
        run_dir=Path(args.run_dir),
        round_id=args.round_id,
        continue_on_error=args.continue_on_error,
        skip_existing=args.skip_existing,
        timeout_seconds=args.timeout_seconds,
    )


def call_openaq_api(
    *,
    env: dict[str, str],
    path: str,
    query_pairs: list[str],
    max_pages: int,
    all_pages: bool,
) -> dict[str, Any]:
    argv = [
        "python3",
        str(OPENAQ_API_SCRIPT),
        "request",
        "--path",
        path,
    ]
    for pair in query_pairs:
        argv.extend(["--query", pair])
    if all_pages:
        argv.append("--all-pages")
        argv.extend(["--max-pages", str(max_pages)])
    return run_json_cli(argv, cwd=SKILL_DIRS["openaq-data-fetch"], env=env)


def normalized_parameter_name(value: Any) -> str:
    return maybe_text(value).casefold().replace("_", "").replace("-", "").replace(".", "")


def sensor_parameter_name(sensor: dict[str, Any]) -> str:
    parameter = sensor.get("parameter")
    if isinstance(parameter, dict):
        return maybe_text(parameter.get("name") or parameter.get("displayName") or parameter.get("parameter"))
    return maybe_text(parameter or sensor.get("parameterName") or sensor.get("name"))


def sensor_parameter_matches(sensor: dict[str, Any], allowed_names: list[str]) -> bool:
    if not allowed_names:
        return True
    normalized = normalized_parameter_name(sensor_parameter_name(sensor))
    if not normalized:
        return False
    return normalized in {normalized_parameter_name(name) for name in allowed_names}


def coordinates_from_location(location: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinates = location.get("coordinates")
    if isinstance(coordinates, dict):
        latitude = coordinates.get("latitude")
        longitude = coordinates.get("longitude")
        try:
            return (float(latitude), float(longitude))
        except (TypeError, ValueError):
            return (None, None)
    for lat_key in ("latitude", "lat"):
        for lon_key in ("longitude", "lon", "lng"):
            if lat_key in location and lon_key in location:
                try:
                    return (float(location[lat_key]), float(location[lon_key]))
                except (TypeError, ValueError):
                    return (None, None)
    return (None, None)


def command_collect_openaq(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    round_id = resolve_round_id(run_dir, args.round_id)
    mission = load_mission(run_dir)
    tasks = tasks_for_role(load_tasks(run_dir, round_id), args.task_role)
    if not tasks:
        raise ValueError(f"No tasks assigned to role={args.task_role!r} in {round_id}.")

    geometry = geometry_from_task_or_mission(mission=mission, tasks=tasks)
    window = window_from_task_or_mission(mission=mission, tasks=tasks)
    env_file = default_env_file("openaq-data-fetch")
    if env_file is None:
        raise ValueError("No env file found for openaq-data-fetch.")
    env = dict(os.environ)
    env.update(parse_env_file(env_file))

    output_path = Path(args.output).expanduser().resolve() if args.output else default_raw_artifact_path(
        run_dir,
        round_id,
        args.task_role,
        "openaq-data-fetch",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    location_query_pairs = ["limit=1000"]
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "BBox":
        location_query_pairs.append(f"bbox={bbox_text_for_geometry(geometry, point_padding_deg=0.0)}")
    elif geometry_type == "Point":
        latitude, longitude = center_point_for_geometry(geometry)
        location_query_pairs.append(f"coordinates={latitude:.6f},{longitude:.6f}")
        location_query_pairs.append(f"radius={args.radius_meters}")
    else:
        raise ValueError(f"Unsupported mission geometry type for OpenAQ: {geometry_type!r}")

    locations_payload = call_openaq_api(
        env=env,
        path="/v3/locations",
        query_pairs=location_query_pairs,
        max_pages=args.max_pages,
        all_pages=True,
    )
    locations = ensure_object_list(locations_payload.get("results", []), "OpenAQ locations results")
    selected_locations = locations[: args.max_locations]

    records: list[dict[str, Any]] = []
    discovery_summary: list[dict[str, Any]] = []
    allowed_parameter_names = args.parameter_name or DEFAULT_OPENAQ_PARAMETER_NAMES

    for location in selected_locations:
        location_id = location.get("id")
        if location_id in (None, ""):
            continue
        sensors_payload = call_openaq_api(
            env=env,
            path=f"/v3/locations/{location_id}/sensors",
            query_pairs=["limit=1000"],
            max_pages=args.max_pages,
            all_pages=True,
        )
        sensors = ensure_object_list(sensors_payload.get("results", []), f"OpenAQ sensors results for location {location_id}")
        selected_sensors = [sensor for sensor in sensors if sensor_parameter_matches(sensor, allowed_parameter_names)]
        selected_sensors = selected_sensors[: args.max_sensors_per_location]
        latitude, longitude = coordinates_from_location(location)

        location_summary = {
            "location_id": location_id,
            "location_name": maybe_text(location.get("name")),
            "sensor_count": len(selected_sensors),
            "selected_sensor_ids": [sensor.get("id") for sensor in selected_sensors],
        }
        discovery_summary.append(location_summary)

        for sensor in selected_sensors:
            sensor_id = sensor.get("id")
            if sensor_id in (None, ""):
                continue
            query_pairs = [
                "limit=1000",
                f"datetime_from={window['start_utc']}",
                f"datetime_to={window['end_utc']}",
            ]
            measurements_payload = call_openaq_api(
                env=env,
                path=f"/v3/sensors/{sensor_id}/measurements",
                query_pairs=query_pairs,
                max_pages=args.max_pages,
                all_pages=True,
            )
            measurement_rows = ensure_object_list(
                measurements_payload.get("results", []),
                f"OpenAQ measurements results for sensor {sensor_id}",
            )
            for row in measurement_rows:
                enriched = dict(row)
                if "location" not in enriched:
                    enriched["location"] = {
                        "id": location_id,
                        "name": maybe_text(location.get("name")),
                    }
                if "sensor" not in enriched:
                    enriched["sensor"] = {
                        "id": sensor_id,
                    }
                if "parameter" not in enriched:
                    parameter = sensor.get("parameter")
                    if parameter is not None:
                        enriched["parameter"] = parameter
                    else:
                        enriched["parameter"] = {"name": sensor_parameter_name(sensor)}
                if "coordinates" not in enriched and latitude is not None and longitude is not None:
                    enriched["coordinates"] = {"latitude": latitude, "longitude": longitude}
                records.append(enriched)

    payload = {
        "generated_at_utc": utc_now_iso(),
        "run_id": maybe_text(mission.get("run_id")),
        "round_id": round_id,
        "source_skill": "openaq-data-fetch",
        "request": {
            "geometry": geometry,
            "window": window,
            "max_locations": args.max_locations,
            "max_sensors_per_location": args.max_sensors_per_location,
            "max_pages": args.max_pages,
            "radius_meters": args.radius_meters,
            "parameter_names": allowed_parameter_names,
        },
        "discovery_summary": discovery_summary,
        "record_count": len(records),
        "records": records,
    }
    write_json(output_path, payload, pretty=args.pretty)
    return {
        "output_path": str(output_path),
        "record_count": len(records),
        "location_count": len(selected_locations),
        "discovery_summary": discovery_summary,
        "env_file": str(env_file),
    }


def command_run_data_plane(args: argparse.Namespace) -> dict[str, Any]:
    return run_data_plane(run_dir=Path(args.run_dir), round_id=args.round_id)


def command_run_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    return run_matching_adjudication(run_dir=Path(args.run_dir), round_id=args.round_id)


def command_advance_round(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    current_round_id = resolve_round_id(run_dir, args.round_id)
    decision_path = (
        Path(args.decision_input).expanduser().resolve()
        if args.decision_input
        else round_dir(run_dir, current_round_id) / "moderator" / "council_decision.json"
    )
    if not decision_path.exists():
        raise ValueError(f"Decision file does not exist: {decision_path}")
    decision = ensure_object(read_json(decision_path), f"{decision_path}")
    if not decision.get("next_round_required"):
        return {
            "run_dir": str(run_dir),
            "round_id": current_round_id,
            "decision_path": str(decision_path),
            "moderator_status": maybe_text(decision.get("moderator_status")),
            "advanced": False,
        }
    next_round_tasks = ensure_object_list(decision.get("next_round_tasks", []), "council_decision.next_round_tasks")
    if not next_round_tasks:
        raise ValueError("Decision requires another round, but next_round_tasks is empty.")
    next_round_ids = unique_strings([maybe_text(task.get("round_id")) for task in next_round_tasks if maybe_text(task.get("round_id"))])
    if len(next_round_ids) != 1:
        raise ValueError(f"Expected exactly one next round_id in next_round_tasks, got {next_round_ids}")
    next_round_id_value = next_round_ids[0]
    next_round_path = round_dir(run_dir, next_round_id_value)
    if next_round_path.exists() and any(next_round_path.iterdir()) and not args.allow_existing:
        raise ValueError(f"Next round already exists: {next_round_path}. Use --allow-existing to proceed.")

    tasks_path = approved_next_round_tasks_path(run_dir, current_round_id)
    write_json(tasks_path, next_round_tasks, pretty=True)
    scaffold_payload = ensure_ok_envelope(
        run_json_cli(
            [
                "python3",
                str(CONTRACT_SCRIPT),
                "scaffold-round",
                "--run-dir",
                str(run_dir),
                "--round-id",
                next_round_id_value,
                "--tasks-input",
                str(tasks_path),
            ]
        ),
        "scaffold-round",
    )
    task_prompt = render_moderator_task_review_prompt(run_dir=run_dir, round_id=next_round_id_value)
    manifest_path = write_round_manifest(
        run_dir=run_dir,
        round_id=next_round_id_value,
        stage="task-review",
        task_prompt=task_prompt,
        fetch_plan=None,
        fetch_prompts={},
    )
    bundle_payload = ensure_ok_envelope(
        run_json_cli(["python3", str(CONTRACT_SCRIPT), "validate-bundle", "--run-dir", str(run_dir)]),
        "validate bundle",
    )
    return {
        "run_dir": str(run_dir),
        "current_round_id": current_round_id,
        "next_round_id": next_round_id_value,
        "decision_path": str(decision_path),
        "approved_next_round_tasks_path": str(tasks_path),
        "scaffold": scaffold_payload,
        "task_review_prompt_path": str(task_prompt),
        "manifest_path": str(manifest_path),
        "bundle_validation": bundle_payload,
        "advanced": True,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Orchestrate eco-council run lifecycle and OpenClaw handoffs.")
    sub = parser.add_subparsers(dest="command", required=True)

    bootstrap = sub.add_parser("bootstrap-run", help="Scaffold a run from mission JSON and render moderator task review prompt.")
    bootstrap.add_argument("--run-dir", required=True, help="Run directory.")
    bootstrap.add_argument("--mission-input", required=True, help="Mission JSON path.")
    bootstrap.add_argument("--tasks-input", default="", help="Optional initial round-task list JSON path.")
    bootstrap.add_argument("--allow-existing", action="store_true", help="Allow writing into an existing run directory.")
    add_pretty_flag(bootstrap)

    prepare = sub.add_parser("prepare-round", help="Build one round fetch plan plus OpenClaw fetch prompts.")
    prepare.add_argument("--run-dir", required=True, help="Run directory.")
    prepare.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    prepare.add_argument(
        "--firms-point-padding-deg",
        type=float,
        default=0.5,
        help="BBox padding in degrees when NASA FIRMS is planned for a point mission geometry.",
    )
    add_pretty_flag(prepare)

    execute = sub.add_parser("execute-fetch-plan", help="Execute the shell commands in the prepared fetch plan.")
    execute.add_argument("--run-dir", required=True, help="Run directory.")
    execute.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    execute.add_argument("--continue-on-error", action="store_true", help="Continue executing remaining steps after a failure.")
    execute.add_argument("--skip-existing", action="store_true", help="Skip steps whose artifact path already exists.")
    execute.add_argument("--timeout-seconds", type=int, default=900, help="Per-step timeout in seconds.")
    add_pretty_flag(execute)

    collect_openaq = sub.add_parser("collect-openaq", help="Collect OpenAQ measurements through the multi-step discovery chain.")
    collect_openaq.add_argument("--run-dir", required=True, help="Run directory.")
    collect_openaq.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    collect_openaq.add_argument("--output", default="", help="Output JSON artifact path.")
    collect_openaq.add_argument("--task-role", default="environmentalist", help="Assigned role whose tasks provide geometry and window.")
    collect_openaq.add_argument("--max-locations", type=int, default=4, help="Maximum nearby locations to keep.")
    collect_openaq.add_argument("--max-sensors-per-location", type=int, default=3, help="Maximum sensors to keep per location.")
    collect_openaq.add_argument("--max-pages", type=int, default=5, help="Maximum pages per OpenAQ API request.")
    collect_openaq.add_argument("--radius-meters", type=int, default=25000, help="Radius used for point-based location discovery.")
    collect_openaq.add_argument(
        "--parameter-name",
        action="append",
        default=[],
        help="Preferred parameter names to keep, for example pm25 or no2. Repeat for multiple values.",
    )
    add_pretty_flag(collect_openaq)

    data_plane = sub.add_parser("run-data-plane", help="Run normalization plus data-readiness packet generation.")
    data_plane.add_argument("--run-dir", required=True, help="Run directory.")
    data_plane.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    add_pretty_flag(data_plane)

    matching = sub.add_parser("run-matching-adjudication", help="Run authorized matching/adjudication plus post-match report packet generation.")
    matching.add_argument("--run-dir", required=True, help="Run directory.")
    matching.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    add_pretty_flag(matching)

    advance = sub.add_parser("advance-round", help="Scaffold the next round from an approved council decision.")
    advance.add_argument("--run-dir", required=True, help="Run directory.")
    advance.add_argument("--round-id", default="", help="Current round identifier. Defaults to latest round.")
    advance.add_argument("--decision-input", default="", help="Optional explicit council-decision JSON path.")
    advance.add_argument("--allow-existing", action="store_true", help="Allow scaffolding into an already existing next-round directory.")
    add_pretty_flag(advance)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "bootstrap-run": command_bootstrap_run,
        "prepare-round": command_prepare_round,
        "execute-fetch-plan": command_execute_fetch_plan,
        "collect-openaq": command_collect_openaq,
        "run-data-plane": command_run_data_plane,
        "run-matching-adjudication": command_run_matching_adjudication,
        "advance-round": command_advance_round,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:
        result = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(result, pretty=getattr(args, "pretty", False)))
        return 1
    result = {"command": args.command, "ok": True, "payload": payload}
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
