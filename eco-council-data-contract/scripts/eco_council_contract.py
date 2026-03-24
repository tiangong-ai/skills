#!/usr/bin/env python3
"""Validate and scaffold shared data contracts for the eco council."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sqlite3
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
ASSETS_DIR = SKILL_DIR / "assets"
EXAMPLES_DIR = ASSETS_DIR / "examples"
DDL_PATH = ASSETS_DIR / "sqlite" / "eco_council.sql"
SCHEMA_PATH = ASSETS_DIR / "schemas" / "eco_council.schema.json"

SCHEMA_VERSION = "1.0.0"
ISO_UTC_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")

OBJECT_KINDS = (
    "mission",
    "round-task",
    "source-selection",
    "override-request",
    "claim",
    "claim-submission",
    "observation",
    "observation-submission",
    "evidence-card",
    "data-readiness-report",
    "matching-authorization",
    "matching-result",
    "evidence-adjudication",
    "isolated-entry",
    "remand-entry",
    "expert-report",
    "council-decision",
)

AGENT_ROLES = {"moderator", "sociologist", "environmentalist", "historian"}
TASK_STATUSES = {"planned", "in_progress", "completed", "blocked"}
CLAIM_TYPES = {
    "wildfire",
    "smoke",
    "flood",
    "heat",
    "drought",
    "air-pollution",
    "water-pollution",
    "policy-reaction",
    "other",
}
CLAIM_STATUSES = {"candidate", "selected", "dismissed", "validated"}
OBSERVATION_AGGREGATIONS = {"point", "window-summary", "series-summary", "event-count"}
EVIDENCE_VERDICTS = {"supports", "contradicts", "mixed", "insufficient"}
CONFIDENCE_VALUES = {"low", "medium", "high"}
REPORT_STATUSES = {"complete", "needs-more-evidence", "blocked"}
MODERATOR_STATUSES = {"continue", "complete", "blocked"}
EVIDENCE_SUFFICIENCY = {"sufficient", "partial", "insufficient"}
READINESS_STATUSES = {"ready", "needs-more-data", "blocked"}
AUTHORIZATION_STATUSES = {"authorized", "deferred", "not-authorized"}
MATCHING_RESULT_STATUSES = {"matched", "partial", "unmatched"}
ADJUDICATION_STATUSES = {"complete", "partial", "remand-required"}
LIBRARY_ENTITY_KINDS = {"claim", "observation"}
SOURCE_SELECTION_ROLES = {"sociologist", "environmentalist", "historian"}
SOURCE_SELECTION_STATUSES = {"pending", "complete", "blocked"}
SOURCE_LAYER_TIERS = {"l1", "l2"}
EVIDENCE_PRIORITIES = {"low", "medium", "high"}
ANCHOR_MODES = {"none", "same_round_l1", "prior_round_l1", "evidence_gap", "upstream_approval"}
AUTHORIZATION_BASES = {"entry-layer", "policy-auto", "upstream-approval", "not-authorized"}
APPROVAL_AUTHORITIES = {"human", "bot", "policy"}
OVERRIDE_REQUEST_ORIGINS = {"source-selection", "data-readiness-report", "expert-report", "council-decision"}
OVERRIDE_INT_TARGET_PATHS = {
    "constraints.max_rounds",
    "constraints.max_claims_per_round",
    "constraints.max_tasks_per_round",
    "constraints.claim_target_per_round",
    "constraints.claim_hard_cap_per_round",
    "source_governance.max_selected_sources_per_role",
    "source_governance.max_active_families_per_role",
    "source_governance.max_non_entry_layers_per_role",
}
OVERRIDE_BOOL_TARGET_PATHS = {"source_governance.allow_cross_round_anchors"}
OVERRIDE_APPROVAL_TARGET_PATHS = {"source_governance.approved_layers"}
OVERRIDE_TARGET_PATHS = OVERRIDE_INT_TARGET_PATHS | OVERRIDE_BOOL_TARGET_PATHS | OVERRIDE_APPROVAL_TARGET_PATHS
CONSTRAINT_KEYS = (
    "max_rounds",
    "max_claims_per_round",
    "max_tasks_per_round",
    "claim_target_per_round",
    "claim_hard_cap_per_round",
)

DEFAULT_POLICY_PROFILES: dict[str, dict[str, Any]] = {
    "focused": {
        "profile_id": "focused",
        "label": "Focused",
        "description": "Lower-cost targeted verification with tighter round, claim, and source caps.",
        "constraints": {
            "max_rounds": 2,
            "max_claims_per_round": 2,
            "max_tasks_per_round": 3,
            "claim_target_per_round": 2,
            "claim_hard_cap_per_round": 6,
        },
        "source_governance": {
            "approval_authority": "policy",
            "allow_cross_round_anchors": True,
            "max_selected_sources_per_role": 3,
            "max_active_families_per_role": 2,
            "max_non_entry_layers_per_role": 1,
            "approved_layers": [],
        },
    },
    "standard": {
        "profile_id": "standard",
        "label": "Standard",
        "description": "Balanced default envelope for most audited eco-council runs.",
        "constraints": {
            "max_rounds": 3,
            "max_claims_per_round": 3,
            "max_tasks_per_round": 4,
            "claim_target_per_round": 3,
            "claim_hard_cap_per_round": 9,
        },
        "source_governance": {
            "approval_authority": "policy",
            "allow_cross_round_anchors": True,
            "max_selected_sources_per_role": 4,
            "max_active_families_per_role": 3,
            "max_non_entry_layers_per_role": 1,
            "approved_layers": [],
        },
    },
    "expanded": {
        "profile_id": "expanded",
        "label": "Expanded",
        "description": "Broader collection envelope for multi-round or higher-coverage investigations.",
        "constraints": {
            "max_rounds": 5,
            "max_claims_per_round": 4,
            "max_tasks_per_round": 6,
            "claim_target_per_round": 4,
            "claim_hard_cap_per_round": 12,
        },
        "source_governance": {
            "approval_authority": "policy",
            "allow_cross_round_anchors": True,
            "max_selected_sources_per_role": 6,
            "max_active_families_per_role": 4,
            "max_non_entry_layers_per_role": 2,
            "approved_layers": [],
        },
    },
}

DEFAULT_SOURCE_FAMILY_CATALOG: list[dict[str, Any]] = [
    {
        "family_id": "gdelt",
        "role": "sociologist",
        "label": "GDELT public coverage",
        "layers": [
            {
                "layer_id": "recon",
                "tier": "l1",
                "skills": ["gdelt-doc-search"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Small-batch article discovery for public-claim recon.",
            },
            {
                "layer_id": "bulk",
                "tier": "l2",
                "skills": ["gdelt-events-fetch", "gdelt-mentions-fetch", "gdelt-gkg-fetch"],
                "requires_anchor": True,
                "auto_selectable": False,
                "allowed_anchor_modes": ["same_round_l1", "prior_round_l1", "evidence_gap", "upstream_approval"],
                "max_selected_skills": 3,
                "description": "Bulk GDELT tables used only after an anchored recon pass or an explicit upstream approval.",
            },
        ],
    },
    {
        "family_id": "youtube",
        "role": "sociologist",
        "label": "YouTube public discussion",
        "layers": [
            {
                "layer_id": "videos",
                "tier": "l1",
                "skills": ["youtube-video-search"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Video discovery for public-discussion recon.",
            },
            {
                "layer_id": "comments",
                "tier": "l2",
                "skills": ["youtube-comments-fetch"],
                "requires_anchor": True,
                "auto_selectable": False,
                "allowed_anchor_modes": ["same_round_l1", "prior_round_l1", "evidence_gap", "upstream_approval"],
                "max_selected_skills": 1,
                "description": "Comment collection that should stay anchored to identified videos or an explicit evidence gap.",
            },
        ],
    },
    {
        "family_id": "rulemaking",
        "role": "sociologist",
        "label": "Federal rulemaking record",
        "layers": [
            {
                "layer_id": "documents",
                "tier": "l1",
                "skills": ["federal-register-doc-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Federal Register document discovery for rulemaking scope and chronology.",
            },
            {
                "layer_id": "comments",
                "tier": "l2",
                "skills": ["regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"],
                "requires_anchor": True,
                "auto_selectable": False,
                "allowed_anchor_modes": ["same_round_l1", "prior_round_l1", "evidence_gap", "upstream_approval"],
                "max_selected_skills": 2,
                "description": "Regulations.gov comment collection used only after the rulemaking scope is anchored.",
            },
        ],
    },
    {
        "family_id": "bluesky",
        "role": "sociologist",
        "label": "Bluesky discussion",
        "layers": [
            {
                "layer_id": "posts",
                "tier": "l1",
                "skills": ["bluesky-cascade-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Fast-moving public discussion sampling from Bluesky.",
            }
        ],
    },
    {
        "family_id": "airnow",
        "role": "environmentalist",
        "label": "AirNow monitoring",
        "layers": [
            {
                "layer_id": "observations",
                "tier": "l1",
                "skills": ["airnow-hourly-obs-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Station-grade AirNow hourly observations.",
            }
        ],
    },
    {
        "family_id": "openaq",
        "role": "environmentalist",
        "label": "OpenAQ stations",
        "layers": [
            {
                "layer_id": "stations",
                "tier": "l1",
                "skills": ["openaq-data-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Station-based OpenAQ measurements.",
            }
        ],
    },
    {
        "family_id": "openmeteo-air-quality",
        "role": "environmentalist",
        "label": "Open-Meteo air quality",
        "layers": [
            {
                "layer_id": "modeled-air",
                "tier": "l1",
                "skills": ["open-meteo-air-quality-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Modeled air-quality background fields.",
            }
        ],
    },
    {
        "family_id": "openmeteo-historical",
        "role": "environmentalist",
        "label": "Open-Meteo historical weather",
        "layers": [
            {
                "layer_id": "meteorology",
                "tier": "l1",
                "skills": ["open-meteo-historical-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Meteorological background context.",
            }
        ],
    },
    {
        "family_id": "openmeteo-flood",
        "role": "environmentalist",
        "label": "Open-Meteo flood",
        "layers": [
            {
                "layer_id": "flood-model",
                "tier": "l1",
                "skills": ["open-meteo-flood-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Flood-risk or hydrological model context.",
            }
        ],
    },
    {
        "family_id": "usgs",
        "role": "environmentalist",
        "label": "USGS water observations",
        "layers": [
            {
                "layer_id": "hydrology",
                "tier": "l1",
                "skills": ["usgs-water-iv-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "USGS instantaneous values for streamflow or gage height.",
            }
        ],
    },
    {
        "family_id": "firms",
        "role": "environmentalist",
        "label": "NASA FIRMS fire detections",
        "layers": [
            {
                "layer_id": "fire-detections",
                "tier": "l1",
                "skills": ["nasa-firms-fire-fetch"],
                "requires_anchor": False,
                "auto_selectable": True,
                "allowed_anchor_modes": [],
                "max_selected_skills": 1,
                "description": "Satellite fire-detection evidence.",
            }
        ],
    },
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


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def unique_strings(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def parse_utc_datetime(value: str) -> datetime | None:
    if not isinstance(value, str) or not ISO_UTC_PATTERN.match(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def is_int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


@dataclass
class IssueCollector:
    issues: list[dict[str, Any]] = field(default_factory=list)

    def add(self, path: str, message: str, *, actual: Any | None = None) -> None:
        issue: dict[str, Any] = {"path": path, "message": message}
        if actual is not None:
            issue["actual"] = actual
        self.issues.append(issue)

    @property
    def ok(self) -> bool:
        return not self.issues

    def summary(self) -> dict[str, Any]:
        return {"ok": self.ok, "issue_count": len(self.issues), "issues": self.issues}


def require_object(value: Any, path: str, issues: IssueCollector) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        issues.add(path, "Expected an object.", actual=type(value).__name__)
        return None
    return value


def require_string(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    allow_empty: bool = False,
) -> str | None:
    value = obj.get(key)
    field_path = f"{path}.{key}"
    if not isinstance(value, str):
        issues.add(field_path, "Expected a string.", actual=value)
        return None
    if not allow_empty and not value.strip():
        issues.add(field_path, "String must not be empty.")
        return None
    return value


def require_bool(obj: dict[str, Any], key: str, path: str, issues: IssueCollector) -> bool | None:
    value = obj.get(key)
    if not isinstance(value, bool):
        issues.add(f"{path}.{key}", "Expected a boolean.", actual=value)
        return None
    return value


def require_int(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    value = obj.get(key)
    field_path = f"{path}.{key}"
    if not is_int_not_bool(value):
        issues.add(field_path, "Expected an integer.", actual=value)
        return None
    if minimum is not None and value < minimum:
        issues.add(field_path, f"Value must be >= {minimum}.", actual=value)
    if maximum is not None and value > maximum:
        issues.add(field_path, f"Value must be <= {maximum}.", actual=value)
    return value


def require_number(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    value = obj.get(key)
    field_path = f"{path}.{key}"
    if not is_number(value):
        issues.add(field_path, "Expected a number.", actual=value)
        return None
    value_float = float(value)
    if minimum is not None and value_float < minimum:
        issues.add(field_path, f"Value must be >= {minimum}.", actual=value)
    if maximum is not None and value_float > maximum:
        issues.add(field_path, f"Value must be <= {maximum}.", actual=value)
    return value_float


def require_enum(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    allowed: set[str],
) -> str | None:
    value = require_string(obj, key, path, issues)
    if value is None:
        return None
    if value not in allowed:
        issues.add(f"{path}.{key}", f"Expected one of {sorted(allowed)}.", actual=value)
        return None
    return value


def validate_string_list(
    value: Any,
    path: str,
    issues: IssueCollector,
    *,
    allow_empty: bool = True,
) -> list[str]:
    if not isinstance(value, list):
        issues.add(path, "Expected a list.", actual=type(value).__name__)
        return []
    result: list[str] = []
    if not allow_empty and not value:
        issues.add(path, "List must not be empty.")
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, str) or not item.strip():
            issues.add(item_path, "Expected a non-empty string.", actual=item)
            continue
        result.append(item)
    return result


def validate_unique_strings(values: list[str], path: str, issues: IssueCollector) -> None:
    seen: set[str] = set()
    for index, item in enumerate(values):
        key = item.casefold()
        if key in seen:
            issues.add(f"{path}[{index}]", "Duplicate string entry.", actual=item)
            continue
        seen.add(key)


def validate_schema_version(obj: dict[str, Any], path: str, issues: IssueCollector) -> str | None:
    value = require_string(obj, "schema_version", path, issues)
    if value is None:
        return None
    if not re.match(r"^\d+\.\d+\.\d+$", value):
        issues.add(f"{path}.schema_version", "Expected semantic-version-like string.", actual=value)
        return None
    return value


def validate_time_window(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    start_text = require_string(obj, "start_utc", path, issues)
    end_text = require_string(obj, "end_utc", path, issues)
    start_dt = parse_utc_datetime(start_text) if start_text is not None else None
    end_dt = parse_utc_datetime(end_text) if end_text is not None else None
    if start_text is not None and start_dt is None:
        issues.add(f"{path}.start_utc", "Expected RFC3339 UTC string with trailing Z.", actual=start_text)
    if end_text is not None and end_dt is None:
        issues.add(f"{path}.end_utc", "Expected RFC3339 UTC string with trailing Z.", actual=end_text)
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        issues.add(path, "end_utc must be >= start_utc.")


def validate_geometry(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    geometry_type = require_string(obj, "type", path, issues)
    if geometry_type is None:
        return
    if geometry_type == "Point":
        latitude = require_number(obj, "latitude", path, issues, minimum=-90, maximum=90)
        longitude = require_number(obj, "longitude", path, issues, minimum=-180, maximum=180)
        if latitude is not None and longitude is not None:
            return
    elif geometry_type == "BBox":
        west = require_number(obj, "west", path, issues, minimum=-180, maximum=180)
        south = require_number(obj, "south", path, issues, minimum=-90, maximum=90)
        east = require_number(obj, "east", path, issues, minimum=-180, maximum=180)
        north = require_number(obj, "north", path, issues, minimum=-90, maximum=90)
        if west is not None and east is not None and east <= west:
            issues.add(path, "BBox east must be greater than west.")
        if south is not None and north is not None and north <= south:
            issues.add(path, "BBox north must be greater than south.")
    else:
        issues.add(f"{path}.type", "Expected Point or BBox.", actual=geometry_type)


def validate_region_scope(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "label", path, issues)
    validate_geometry(obj.get("geometry"), f"{path}.geometry", issues)


def family_catalog() -> list[dict[str, Any]]:
    return copy.deepcopy(DEFAULT_SOURCE_FAMILY_CATALOG)


def family_catalog_lookup() -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("family_id")): item for item in family_catalog() if maybe_text(item.get("family_id"))}


def layer_lookup_for_family(family: dict[str, Any]) -> dict[str, dict[str, Any]]:
    layers = family.get("layers")
    if not isinstance(layers, list):
        return {}
    return {maybe_text(item.get("layer_id")): item for item in layers if isinstance(item, dict) and maybe_text(item.get("layer_id"))}


def policy_profile_catalog() -> dict[str, dict[str, Any]]:
    return copy.deepcopy(DEFAULT_POLICY_PROFILES)


def policy_profile_id(mission: dict[str, Any]) -> str:
    if not isinstance(mission, dict):
        raise ValueError("mission must be an object.")
    value = maybe_text(mission.get("policy_profile"))
    if not value:
        raise ValueError("mission.policy_profile is required.")
    if value not in DEFAULT_POLICY_PROFILES:
        allowed = ", ".join(sorted(DEFAULT_POLICY_PROFILES))
        raise ValueError(f"mission.policy_profile must be one of: {allowed}.")
    return value


def policy_profile_spec(value: dict[str, Any] | str) -> dict[str, Any]:
    profile_id = policy_profile_id(value) if isinstance(value, dict) else maybe_text(value)
    spec = DEFAULT_POLICY_PROFILES.get(profile_id)
    if not isinstance(spec, dict):
        allowed = ", ".join(sorted(DEFAULT_POLICY_PROFILES))
        raise ValueError(f"Unknown policy profile {profile_id!r}. Expected one of: {allowed}.")
    return copy.deepcopy(spec)


def validate_constraints_object(value: Any, path: str, issues: IssueCollector, *, require_core: bool) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    for key in ("max_rounds", "max_claims_per_round", "max_tasks_per_round"):
        if require_core or key in obj:
            require_int(obj, key, path, issues, minimum=1)
    for key in ("claim_target_per_round", "claim_hard_cap_per_round"):
        if key in obj:
            require_int(obj, key, path, issues, minimum=1)

    max_claims = int(obj.get("max_claims_per_round")) if is_int_not_bool(obj.get("max_claims_per_round")) else None
    claim_target = int(obj.get("claim_target_per_round")) if is_int_not_bool(obj.get("claim_target_per_round")) else None
    claim_hard_cap = int(obj.get("claim_hard_cap_per_round")) if is_int_not_bool(obj.get("claim_hard_cap_per_round")) else None
    if claim_target is not None and claim_hard_cap is not None and claim_target > claim_hard_cap:
        issues.add(
            f"{path}.claim_target_per_round",
            "claim_target_per_round cannot exceed claim_hard_cap_per_round.",
            actual={"claim_target_per_round": claim_target, "claim_hard_cap_per_round": claim_hard_cap},
        )
    if max_claims is not None and claim_hard_cap is not None and max_claims > claim_hard_cap:
        issues.add(
            f"{path}.claim_hard_cap_per_round",
            "claim_hard_cap_per_round must be >= max_claims_per_round.",
            actual={"max_claims_per_round": max_claims, "claim_hard_cap_per_round": claim_hard_cap},
        )


def effective_constraints(mission: dict[str, Any]) -> dict[str, int]:
    if not isinstance(mission, dict):
        raise ValueError("mission must be an object.")
    defaults = policy_profile_spec(mission).get("constraints", {})
    if not isinstance(defaults, dict):
        raise ValueError("policy profile is missing default constraints.")
    constraints = copy.deepcopy(defaults)
    explicit = mission.get("constraints")
    if explicit is not None and not isinstance(explicit, dict):
        raise ValueError("mission.constraints must be an object when provided.")
    explicit_obj = explicit if isinstance(explicit, dict) else {}
    for key in CONSTRAINT_KEYS:
        value = explicit_obj.get(key)
        if is_int_not_bool(value) and int(value) > 0:
            constraints[key] = int(value)
    return {key: int(value) for key, value in constraints.items() if is_int_not_bool(value) and int(value) > 0}


def normalize_governed_families(raw_families: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_families, list):
        return []
    normalized_families: list[dict[str, Any]] = []
    for family in raw_families:
        if not isinstance(family, dict):
            continue
        family_copy = copy.deepcopy(family)
        normalized_layers: list[dict[str, Any]] = []
        for layer in family_copy.get("layers", []):
            if not isinstance(layer, dict):
                continue
            layer_copy = copy.deepcopy(layer)
            skills = [maybe_text(skill) for skill in layer_copy.get("skills", []) if maybe_text(skill)]
            if not skills:
                continue
            layer_copy["skills"] = unique_strings(skills)
            max_selected = layer_copy.get("max_selected_skills")
            if is_int_not_bool(max_selected):
                layer_copy["max_selected_skills"] = max(1, min(int(max_selected), len(layer_copy["skills"])))
            normalized_layers.append(layer_copy)
        family_copy["layers"] = normalized_layers
        family_copy["skills"] = unique_strings(
            [maybe_text(skill) for layer in normalized_layers for skill in layer.get("skills", []) if maybe_text(skill)]
        )
        if family_copy["skills"]:
            normalized_families.append(family_copy)
    return normalized_families


def source_governance(mission: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(mission, dict):
        raise ValueError("mission must be an object.")
    profile = policy_profile_spec(mission)
    defaults = profile.get("source_governance", {})
    if not isinstance(defaults, dict):
        raise ValueError("policy profile is missing default source_governance.")
    explicit = mission.get("source_governance")
    if explicit is not None and not isinstance(explicit, dict):
        raise ValueError("mission.source_governance must be an object when provided.")
    explicit_obj = explicit if isinstance(explicit, dict) else {}

    governance = copy.deepcopy(defaults)
    governance["approval_authority"] = maybe_text(explicit_obj.get("approval_authority")) or maybe_text(governance.get("approval_authority")) or "policy"
    if "allow_cross_round_anchors" in explicit_obj:
        governance["allow_cross_round_anchors"] = bool(explicit_obj.get("allow_cross_round_anchors"))
    elif "allow_cross_round_anchors" not in governance:
        governance["allow_cross_round_anchors"] = True
    for key, fallback in (
        ("max_selected_sources_per_role", 4),
        ("max_active_families_per_role", 3),
        ("max_non_entry_layers_per_role", 1),
    ):
        if is_int_not_bool(explicit_obj.get(key)):
            governance[key] = int(explicit_obj[key])
        elif not is_int_not_bool(governance.get(key)):
            governance[key] = fallback

    if isinstance(explicit_obj.get("approved_layers"), list):
        governance["approved_layers"] = copy.deepcopy(explicit_obj["approved_layers"])
    else:
        governance["approved_layers"] = copy.deepcopy(governance.get("approved_layers", []))

    raw_families = explicit_obj.get("families") if "families" in explicit_obj else family_catalog()
    governance["families"] = normalize_governed_families(raw_families)
    return governance


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    profile = policy_profile_spec(mission)
    governance = source_governance(mission)
    return {
        "profile_id": maybe_text(profile.get("profile_id")),
        "label": maybe_text(profile.get("label")),
        "description": maybe_text(profile.get("description")),
        "defaults": {
            "constraints": copy.deepcopy(profile.get("constraints", {})),
            "source_governance": copy.deepcopy(profile.get("source_governance", {})),
        },
        "effective_constraints": effective_constraints(mission),
        "effective_source_governance": {
            "approval_authority": maybe_text(governance.get("approval_authority")),
            "allow_cross_round_anchors": bool(governance.get("allow_cross_round_anchors")),
            "max_selected_sources_per_role": governance.get("max_selected_sources_per_role"),
            "max_active_families_per_role": governance.get("max_active_families_per_role"),
            "max_non_entry_layers_per_role": governance.get("max_non_entry_layers_per_role"),
            "approved_layers": copy.deepcopy(governance.get("approved_layers", [])),
            "family_ids": [
                maybe_text(family.get("family_id"))
                for family in governance.get("families", [])
                if isinstance(family, dict) and maybe_text(family.get("family_id"))
            ],
        },
        "overrideable_paths": sorted(OVERRIDE_TARGET_PATHS),
    }


def source_governance_for_role(mission: dict[str, Any], role: str) -> list[dict[str, Any]]:
    return [
        copy.deepcopy(family)
        for family in source_governance(mission).get("families", [])
        if isinstance(family, dict) and maybe_text(family.get("role")) == role
    ]


def source_family_lookup(mission: dict[str, Any], *, role: str | None = None) -> dict[str, dict[str, Any]]:
    families = source_governance(mission).get("families", [])
    output: dict[str, dict[str, Any]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        if role and maybe_text(family.get("role")) != role:
            continue
        family_id = maybe_text(family.get("family_id"))
        if family_id:
            output[family_id] = copy.deepcopy(family)
    return output


def approved_layer_lookup(mission: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    approvals = source_governance(mission).get("approved_layers", [])
    output: dict[tuple[str, str], dict[str, Any]] = {}
    if not isinstance(approvals, list):
        return output
    for approval in approvals:
        if not isinstance(approval, dict):
            continue
        family_id = maybe_text(approval.get("family_id"))
        layer_id = maybe_text(approval.get("layer_id"))
        if family_id and layer_id:
            output[(family_id, layer_id)] = copy.deepcopy(approval)
    return output


def family_plans(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    value = payload.get("family_plans")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def selected_sources_from_family_plans(payload: dict[str, Any] | None) -> list[str]:
    output: list[str] = []
    for family in family_plans(payload):
        layers = family.get("layer_plans")
        if not isinstance(layers, list):
            continue
        for layer in layers:
            if not isinstance(layer, dict) or layer.get("selected") is not True:
                continue
            skills = layer.get("source_skills")
            if not isinstance(skills, list):
                continue
            output.extend(maybe_text(skill) for skill in skills if maybe_text(skill))
    return unique_strings(output)


def validate_artifact_ref(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "source_skill", path, issues)
    require_string(obj, "artifact_path", path, issues)
    if "record_locator" in obj and obj["record_locator"] is not None and not isinstance(obj["record_locator"], str):
        issues.add(f"{path}.record_locator", "Expected a string when provided.", actual=obj["record_locator"])
    if "external_id" in obj and obj["external_id"] is not None and not isinstance(obj["external_id"], str):
        issues.add(f"{path}.external_id", "Expected a string when provided.", actual=obj["external_id"])
    sha256 = obj.get("sha256")
    if sha256 is not None:
        if not isinstance(sha256, str) or not SHA256_PATTERN.match(sha256):
            issues.add(f"{path}.sha256", "Expected a 64-character hexadecimal SHA256 string.", actual=sha256)


def validate_recommendation(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_enum(obj, "assigned_role", path, issues, allowed=AGENT_ROLES)
    require_string(obj, "objective", path, issues)
    require_string(obj, "reason", path, issues)


def validate_evidence_requirement(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "requirement_id", path, issues)
    require_string(obj, "requirement_type", path, issues)
    require_string(obj, "summary", path, issues)
    require_enum(obj, "priority", path, issues, allowed=EVIDENCE_PRIORITIES)
    focus_claim_ids = validate_string_list(obj.get("focus_claim_ids", []), f"{path}.focus_claim_ids", issues)
    validate_unique_strings(focus_claim_ids, f"{path}.focus_claim_ids", issues)
    anchor_refs = validate_string_list(obj.get("anchor_refs", []), f"{path}.anchor_refs", issues)
    validate_unique_strings(anchor_refs, f"{path}.anchor_refs", issues)
    if "notes" in obj and obj["notes"] is not None and not isinstance(obj["notes"], str):
        issues.add(f"{path}.notes", "Expected a string when provided.", actual=obj["notes"])


def validate_source_layer_policy(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "layer_id", path, issues)
    require_enum(obj, "tier", path, issues, allowed=SOURCE_LAYER_TIERS)
    skills = validate_string_list(obj.get("skills"), f"{path}.skills", issues, allow_empty=False)
    validate_unique_strings(skills, f"{path}.skills", issues)
    require_bool(obj, "requires_anchor", path, issues)
    require_bool(obj, "auto_selectable", path, issues)
    anchor_modes = validate_string_list(obj.get("allowed_anchor_modes", []), f"{path}.allowed_anchor_modes", issues)
    validate_unique_strings(anchor_modes, f"{path}.allowed_anchor_modes", issues)
    for index, anchor_mode in enumerate(anchor_modes):
        if anchor_mode not in ANCHOR_MODES:
            issues.add(
                f"{path}.allowed_anchor_modes[{index}]",
                f"Expected one of {sorted(ANCHOR_MODES)}.",
                actual=anchor_mode,
            )
    requires_anchor = obj.get("requires_anchor")
    if requires_anchor is True and not anchor_modes:
        issues.add(f"{path}.allowed_anchor_modes", "Anchor-required layers must list at least one allowed anchor mode.")
    if "max_selected_skills" in obj:
        require_int(obj, "max_selected_skills", path, issues, minimum=1)
    if "description" in obj and obj["description"] is not None and not isinstance(obj["description"], str):
        issues.add(f"{path}.description", "Expected a string when provided.", actual=obj["description"])


def validate_source_family_policy(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "family_id", path, issues)
    require_enum(obj, "role", path, issues, allowed=SOURCE_SELECTION_ROLES)
    if "label" in obj and obj["label"] is not None and not isinstance(obj["label"], str):
        issues.add(f"{path}.label", "Expected a string when provided.", actual=obj["label"])
    layers = obj.get("layers")
    if not isinstance(layers, list) or not layers:
        issues.add(f"{path}.layers", "Expected a non-empty list.", actual=layers)
        layers = []
    layer_keys: set[str] = set()
    layer_skill_lookup: set[str] = set()
    for index, layer in enumerate(layers):
        item_path = f"{path}.layers[{index}]"
        validate_source_layer_policy(layer, item_path, issues)
        if not isinstance(layer, dict):
            continue
        layer_id = maybe_text(layer.get("layer_id"))
        if layer_id:
            key = layer_id.casefold()
            if key in layer_keys:
                issues.add(f"{item_path}.layer_id", "Duplicate layer_id entry.", actual=layer_id)
            layer_keys.add(key)
        if isinstance(layer.get("skills"), list):
            layer_skill_lookup.update(skill.casefold() for skill in layer.get("skills") if isinstance(skill, str) and skill.strip())
    if "skills" in obj:
        family_skills = validate_string_list(obj.get("skills"), f"{path}.skills", issues, allow_empty=False)
        validate_unique_strings(family_skills, f"{path}.skills", issues)
        missing = sorted(skill for skill in layer_skill_lookup if skill not in {item.casefold() for item in family_skills})
        if missing:
            issues.add(f"{path}.skills", "Family skills must include every layer skill.", actual=missing)
    if "max_active_layers" in obj:
        require_int(obj, "max_active_layers", path, issues, minimum=1)


def validate_layer_approval(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "family_id", path, issues)
    require_string(obj, "layer_id", path, issues)
    require_enum(obj, "approved_by", path, issues, allowed=APPROVAL_AUTHORITIES)
    require_string(obj, "reason", path, issues)


def validate_source_governance(value: Any, path: str, issues: IssueCollector, *, require_families: bool) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    if "approval_authority" in obj:
        require_enum(obj, "approval_authority", path, issues, allowed=APPROVAL_AUTHORITIES)
    if "allow_cross_round_anchors" in obj:
        require_bool(obj, "allow_cross_round_anchors", path, issues)
    if "max_selected_sources_per_role" in obj:
        require_int(obj, "max_selected_sources_per_role", path, issues, minimum=1)
    if "max_active_families_per_role" in obj:
        require_int(obj, "max_active_families_per_role", path, issues, minimum=1)
    if "max_non_entry_layers_per_role" in obj:
        require_int(obj, "max_non_entry_layers_per_role", path, issues, minimum=0)

    families = obj.get("families")
    family_lookup: dict[str, dict[str, Any]] = {}
    families_present = "families" in obj
    if not isinstance(families, list):
        if require_families or families_present:
            issues.add(f"{path}.families", "Expected a non-empty list.", actual=families)
        families = []
    elif require_families and not families:
        issues.add(f"{path}.families", "Expected a non-empty list.", actual=families)
    for index, family in enumerate(families):
        item_path = f"{path}.families[{index}]"
        validate_source_family_policy(family, item_path, issues)
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        if not family_id:
            continue
        key = family_id.casefold()
        if key in family_lookup:
            issues.add(f"{item_path}.family_id", "Duplicate family_id entry.", actual=family_id)
        family_lookup[key] = family

    approvals = obj.get("approved_layers", [])
    if not isinstance(approvals, list):
        issues.add(f"{path}.approved_layers", "Expected a list.", actual=approvals)
        approvals = []
    for index, approval in enumerate(approvals):
        item_path = f"{path}.approved_layers[{index}]"
        validate_layer_approval(approval, item_path, issues)
        if not isinstance(approval, dict):
            continue
        family_id = maybe_text(approval.get("family_id"))
        layer_id = maybe_text(approval.get("layer_id"))
        if not family_lookup and not require_families and not families_present:
            continue
        family = family_lookup.get(family_id.casefold()) if family_id else None
        if family is None:
            issues.add(f"{item_path}.family_id", "Approved layer family_id must appear in source_governance.families.", actual=family_id)
            continue
        layer_lookup = layer_lookup_for_family(family)
        if layer_id and layer_id not in layer_lookup:
            issues.add(f"{item_path}.layer_id", "Approved layer_id must appear in the referenced family.", actual=layer_id)


def validate_override_requested_approvals(value: Any, path: str, issues: IssueCollector) -> None:
    approvals = value if isinstance(value, list) else [value]
    for index, approval in enumerate(approvals):
        item_path = f"{path}[{index}]" if isinstance(value, list) else path
        obj = require_object(approval, item_path, issues)
        if obj is None:
            continue
        require_string(obj, "family_id", item_path, issues)
        require_string(obj, "layer_id", item_path, issues)
        if "reason" in obj and obj["reason"] is not None and not isinstance(obj["reason"], str):
            issues.add(f"{item_path}.reason", "Expected a string when provided.", actual=obj["reason"])


def validate_override_requested_value(target_path: str, value: Any, path: str, issues: IssueCollector) -> None:
    if target_path in OVERRIDE_INT_TARGET_PATHS:
        if not is_int_not_bool(value) or int(value) <= 0:
            issues.add(path, "Expected a positive integer for this override target.", actual=value)
        return
    if target_path in OVERRIDE_BOOL_TARGET_PATHS:
        if not isinstance(value, bool):
            issues.add(path, "Expected a boolean for this override target.", actual=value)
        return
    if target_path in OVERRIDE_APPROVAL_TARGET_PATHS:
        validate_override_requested_approvals(value, path, issues)
        return
    issues.add(path, "Unsupported override target_path.", actual=target_path)


def validate_override_request_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "request_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_enum(record, "request_origin_kind", path, issues, allowed=OVERRIDE_REQUEST_ORIGINS)
    target_path = require_string(record, "target_path", path, issues)
    if target_path and target_path not in OVERRIDE_TARGET_PATHS:
        issues.add(
            f"{path}.target_path",
            f"Expected one of {sorted(OVERRIDE_TARGET_PATHS)}.",
            actual=target_path,
        )
    if "current_value" in record and record["current_value"] is None:
        issues.add(f"{path}.current_value", "current_value cannot be null when provided.", actual=record["current_value"])
    if "requested_value" not in record:
        issues.add(f"{path}.requested_value", "requested_value is required.")
    elif target_path:
        validate_override_requested_value(target_path, record.get("requested_value"), f"{path}.requested_value", issues)
    require_string(record, "summary", path, issues)
    require_string(record, "reason", path, issues)
    evidence_refs = validate_string_list(record.get("evidence_refs", []), f"{path}.evidence_refs", issues)
    validate_unique_strings(evidence_refs, f"{path}.evidence_refs", issues)
    anchor_refs = validate_string_list(record.get("anchor_refs", []), f"{path}.anchor_refs", issues)
    validate_unique_strings(anchor_refs, f"{path}.anchor_refs", issues)


def validate_override_request_list(
    value: Any,
    path: str,
    issues: IssueCollector,
    *,
    run_id: str,
    round_id: str,
    agent_role: str,
    origin_kind: str,
) -> None:
    if not isinstance(value, list):
        issues.add(path, "Expected a list.", actual=value)
        return
    request_ids: set[str] = set()
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        validate_override_request_object(item, item_path, issues)
        if not isinstance(item, dict):
            continue
        request_id = maybe_text(item.get("request_id"))
        if request_id:
            key = request_id.casefold()
            if key in request_ids:
                issues.add(f"{item_path}.request_id", "Duplicate request_id entry.", actual=request_id)
            request_ids.add(key)
        if maybe_text(item.get("run_id")) and maybe_text(item.get("run_id")) != run_id:
            issues.add(f"{item_path}.run_id", "Embedded override request must match parent run_id.", actual=item.get("run_id"))
        if maybe_text(item.get("round_id")) and maybe_text(item.get("round_id")) != round_id:
            issues.add(f"{item_path}.round_id", "Embedded override request must match parent round_id.", actual=item.get("round_id"))
        if maybe_text(item.get("agent_role")) and maybe_text(item.get("agent_role")) != agent_role:
            issues.add(f"{item_path}.agent_role", "Embedded override request must match parent agent_role.", actual=item.get("agent_role"))
        if maybe_text(item.get("request_origin_kind")) and maybe_text(item.get("request_origin_kind")) != origin_kind:
            issues.add(
                f"{item_path}.request_origin_kind",
                "Embedded override request must match the parent object kind.",
                actual=item.get("request_origin_kind"),
            )


def validate_source_decision(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "source_skill", path, issues)
    require_bool(obj, "selected", path, issues)
    require_string(obj, "reason", path, issues)


def validate_layer_plan(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "layer_id", path, issues)
    require_enum(obj, "tier", path, issues, allowed=SOURCE_LAYER_TIERS)
    selected = require_bool(obj, "selected", path, issues)
    require_string(obj, "reason", path, issues)
    skills = validate_string_list(obj.get("source_skills"), f"{path}.source_skills", issues, allow_empty=False)
    validate_unique_strings(skills, f"{path}.source_skills", issues)
    anchor_mode = require_enum(obj, "anchor_mode", path, issues, allowed=ANCHOR_MODES)
    anchor_refs = validate_string_list(obj.get("anchor_refs", []), f"{path}.anchor_refs", issues)
    validate_unique_strings(anchor_refs, f"{path}.anchor_refs", issues)
    require_enum(obj, "authorization_basis", path, issues, allowed=AUTHORIZATION_BASES)
    if anchor_mode == "none" and anchor_refs:
        issues.add(f"{path}.anchor_refs", "anchor_refs must stay empty when anchor_mode is none.", actual=anchor_refs)
    if selected is True and anchor_mode is not None and anchor_mode != "none" and not anchor_refs:
        issues.add(f"{path}.anchor_refs", "Selected anchored layers must provide at least one anchor_ref.")


def validate_family_plan(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "family_id", path, issues)
    selected = require_bool(obj, "selected", path, issues)
    require_string(obj, "reason", path, issues)
    requirement_ids = validate_string_list(obj.get("evidence_requirement_ids", []), f"{path}.evidence_requirement_ids", issues)
    validate_unique_strings(requirement_ids, f"{path}.evidence_requirement_ids", issues)
    layer_plans = obj.get("layer_plans")
    if not isinstance(layer_plans, list):
        issues.add(f"{path}.layer_plans", "Expected a list.", actual=layer_plans)
        layer_plans = []
    selected_layer_count = 0
    layer_ids: set[str] = set()
    for index, layer in enumerate(layer_plans):
        item_path = f"{path}.layer_plans[{index}]"
        validate_layer_plan(layer, item_path, issues)
        if not isinstance(layer, dict):
            continue
        layer_id = maybe_text(layer.get("layer_id"))
        if layer_id:
            key = layer_id.casefold()
            if key in layer_ids:
                issues.add(f"{item_path}.layer_id", "Duplicate layer_id entry.", actual=layer_id)
            layer_ids.add(key)
        if layer.get("selected") is True:
            selected_layer_count += 1
    if selected is True and selected_layer_count == 0:
        issues.add(f"{path}.selected", "Family selected=true requires at least one selected layer.")
    if selected is False and selected_layer_count > 0:
        issues.add(f"{path}.selected", "Family selected=false cannot contain selected layer_plans.")


def validate_round_task_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "task_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "assigned_role", path, issues, allowed=AGENT_ROLES)
    require_string(record, "objective", path, issues)
    require_enum(record, "status", path, issues, allowed=TASK_STATUSES)
    if "depends_on" in record:
        validate_string_list(record["depends_on"], f"{path}.depends_on", issues)
    if "expected_output_kinds" in record:
        kinds = validate_string_list(record["expected_output_kinds"], f"{path}.expected_output_kinds", issues)
        for index, kind in enumerate(kinds):
            if kind not in OBJECT_KINDS:
                issues.add(
                    f"{path}.expected_output_kinds[{index}]",
                    f"Expected one of {list(OBJECT_KINDS)}.",
                    actual=kind,
                )
    if "inputs" in record:
        if record["inputs"] is not None and not isinstance(record["inputs"], dict):
            issues.add(f"{path}.inputs", "Expected an object when provided.", actual=record["inputs"])
        elif isinstance(record["inputs"], dict):
            inputs = record["inputs"]
            if "mission_geometry" in inputs and isinstance(inputs["mission_geometry"], dict):
                validate_geometry(inputs["mission_geometry"], f"{path}.inputs.mission_geometry", issues)
            if "mission_window" in inputs and isinstance(inputs["mission_window"], dict):
                validate_time_window(inputs["mission_window"], f"{path}.inputs.mission_window", issues)
            if "query_hints" in inputs:
                query_hints = validate_string_list(inputs["query_hints"], f"{path}.inputs.query_hints", issues)
                validate_unique_strings(query_hints, f"{path}.inputs.query_hints", issues)
            if "focus_claim_ids" in inputs:
                focus_claim_ids = validate_string_list(inputs["focus_claim_ids"], f"{path}.inputs.focus_claim_ids", issues)
                validate_unique_strings(focus_claim_ids, f"{path}.inputs.focus_claim_ids", issues)
            if "preferred_sources" in inputs:
                issues.add(
                    f"{path}.inputs.preferred_sources",
                    "Task-level preferred_sources is no longer allowed. Express evidence needs in inputs.evidence_requirements and let expert source-selection choose families, layers, and skills.",
                    actual=inputs["preferred_sources"],
                )
            if "required_sources" in inputs:
                issues.add(
                    f"{path}.inputs.required_sources",
                    "Task-level required_sources is no longer allowed. Use mission source_governance plus expert source-selection instead.",
                    actual=inputs["required_sources"],
                )
            evidence_requirements = inputs.get("evidence_requirements", [])
            if not isinstance(evidence_requirements, list):
                issues.add(f"{path}.inputs.evidence_requirements", "Expected a list.", actual=evidence_requirements)
            else:
                requirement_ids: set[str] = set()
                for index, requirement in enumerate(evidence_requirements):
                    item_path = f"{path}.inputs.evidence_requirements[{index}]"
                    validate_evidence_requirement(requirement, item_path, issues)
                    if not isinstance(requirement, dict):
                        continue
                    requirement_id = maybe_text(requirement.get("requirement_id"))
                    if not requirement_id:
                        continue
                    key = requirement_id.casefold()
                    if key in requirement_ids:
                        issues.add(f"{item_path}.requirement_id", "Duplicate requirement_id entry.", actual=requirement_id)
                    requirement_ids.add(key)
    if "notes" in record and record["notes"] is not None and not isinstance(record["notes"], str):
        issues.add(f"{path}.notes", "Expected a string when provided.", actual=record["notes"])


def validate_source_selection_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "selection_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=SOURCE_SELECTION_ROLES)
    status = require_enum(record, "status", path, issues, allowed=SOURCE_SELECTION_STATUSES)
    require_string(record, "summary", path, issues)
    task_ids = validate_string_list(record.get("task_ids"), f"{path}.task_ids", issues)
    validate_unique_strings(task_ids, f"{path}.task_ids", issues)
    allowed_sources = validate_string_list(record.get("allowed_sources"), f"{path}.allowed_sources", issues)
    validate_unique_strings(allowed_sources, f"{path}.allowed_sources", issues)
    selected_sources = validate_string_list(record.get("selected_sources"), f"{path}.selected_sources", issues)
    validate_unique_strings(selected_sources, f"{path}.selected_sources", issues)

    decisions = record.get("source_decisions")
    if not isinstance(decisions, list):
        issues.add(f"{path}.source_decisions", "Expected a list.", actual=decisions)
        decisions = []

    allowed_lookup = {item.casefold() for item in allowed_sources}
    selected_lookup = {item.casefold() for item in selected_sources}
    decision_keys: set[str] = set()
    selected_by_decision: set[str] = set()
    selected_by_family_plans: set[str] = set()

    for index, decision in enumerate(decisions):
        item_path = f"{path}.source_decisions[{index}]"
        validate_source_decision(decision, item_path, issues)
        if not isinstance(decision, dict):
            continue
        source_skill = require_string(decision, "source_skill", item_path, issues)
        selected = require_bool(decision, "selected", item_path, issues)
        if source_skill is None:
            continue
        key = source_skill.casefold()
        if key in decision_keys:
            issues.add(f"{item_path}.source_skill", "Duplicate source_skill entry.", actual=source_skill)
        else:
            decision_keys.add(key)
        if allowed_lookup and key not in allowed_lookup:
            issues.add(f"{item_path}.source_skill", "source_skill must also appear in allowed_sources.", actual=source_skill)
        if selected is True:
            selected_by_decision.add(key)
        elif key in selected_lookup:
            issues.add(
                f"{item_path}.selected",
                "selected_sources cannot include a source whose decision has selected=false.",
                actual=source_skill,
            )

    for index, source_skill in enumerate(selected_sources):
        key = source_skill.casefold()
        if allowed_lookup and key not in allowed_lookup:
            issues.add(
                f"{path}.selected_sources[{index}]",
                "Selected source must also appear in allowed_sources.",
                actual=source_skill,
            )
        if status != "pending" and key not in decision_keys:
            issues.add(
                f"{path}.selected_sources[{index}]",
                "Selected source must also appear in source_decisions once selection is complete or blocked.",
                actual=source_skill,
            )
        if key not in selected_by_decision:
            issues.add(
                f"{path}.selected_sources[{index}]",
                "Selected source must have a matching source_decisions entry with selected=true.",
                actual=source_skill,
            )

    family_plans_value = record.get("family_plans")
    if not isinstance(family_plans_value, list):
        issues.add(f"{path}.family_plans", "Expected a list.", actual=family_plans_value)
        family_plans_value = []
    family_keys: set[str] = set()
    selected_layer_signatures: set[tuple[str, str]] = set()
    for index, family_plan in enumerate(family_plans_value):
        item_path = f"{path}.family_plans[{index}]"
        validate_family_plan(family_plan, item_path, issues)
        if not isinstance(family_plan, dict):
            continue
        family_id = maybe_text(family_plan.get("family_id"))
        if family_id:
            family_key = family_id.casefold()
            if family_key in family_keys:
                issues.add(f"{item_path}.family_id", "Duplicate family_id entry.", actual=family_id)
            family_keys.add(family_key)
        layer_plans = family_plan.get("layer_plans")
        if not isinstance(layer_plans, list):
            continue
        for layer_index, layer_plan in enumerate(layer_plans):
            if not isinstance(layer_plan, dict):
                continue
            if layer_plan.get("selected") is not True:
                continue
            layer_id = maybe_text(layer_plan.get("layer_id"))
            if family_id and layer_id:
                signature = (family_id.casefold(), layer_id.casefold())
                if signature in selected_layer_signatures:
                    issues.add(
                        f"{item_path}.layer_plans[{layer_index}].layer_id",
                        "Duplicate selected layer reference.",
                        actual=layer_id,
                    )
                selected_layer_signatures.add(signature)
            skills = layer_plan.get("source_skills")
            if not isinstance(skills, list):
                continue
            for skill in skills:
                skill_text = maybe_text(skill)
                if not skill_text:
                    continue
                key = skill_text.casefold()
                selected_by_family_plans.add(key)
                if allowed_lookup and key not in allowed_lookup:
                    issues.add(
                        f"{item_path}.layer_plans[{layer_index}].source_skills",
                        "Selected family-plan source_skill must also appear in allowed_sources.",
                        actual=skill_text,
                    )

    if family_plans_value:
        for index, source_skill in enumerate(selected_sources):
            key = source_skill.casefold()
            if key not in selected_by_family_plans:
                issues.add(
                    f"{path}.selected_sources[{index}]",
                    "Selected source must also appear in one selected family_plans.layer_plans entry.",
                    actual=source_skill,
                )
        for source_key in selected_by_family_plans:
            if source_key not in selected_lookup:
                issues.add(
                    f"{path}.family_plans",
                    "Selected family_plans sources must also appear in selected_sources.",
                    actual=source_key,
                )

    if status != "pending":
        for index, source_skill in enumerate(allowed_sources):
            if source_skill.casefold() not in decision_keys:
                issues.add(
                    f"{path}.allowed_sources[{index}]",
                    "Each allowed source must have one source_decisions entry once selection is complete or blocked.",
                    actual=source_skill,
                )

    if "override_requests" not in record:
        issues.add(f"{path}.override_requests", "Expected a list.", actual=None)
    else:
        validate_override_request_list(
            record.get("override_requests"),
            f"{path}.override_requests",
            issues,
            run_id=maybe_text(record.get("run_id")),
            round_id=maybe_text(record.get("round_id")),
            agent_role=maybe_text(record.get("agent_role")),
            origin_kind="source-selection",
        )


def validate_mission_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "topic", path, issues)
    require_string(record, "objective", path, issues)
    validate_time_window(record.get("window"), f"{path}.window", issues)
    validate_region_scope(record.get("region"), f"{path}.region", issues)
    if "hypotheses" in record:
        validate_string_list(record["hypotheses"], f"{path}.hypotheses", issues)
    profile_name = require_string(record, "policy_profile", path, issues)
    if profile_name and profile_name not in DEFAULT_POLICY_PROFILES:
        issues.add(
            f"{path}.policy_profile",
            f"Expected one of {sorted(DEFAULT_POLICY_PROFILES)}.",
            actual=profile_name,
        )
    if record.get("constraints") is not None:
        validate_constraints_object(record.get("constraints"), f"{path}.constraints", issues, require_core=False)
    if record.get("source_governance") is not None:
        validate_source_governance(record.get("source_governance"), f"{path}.source_governance", issues, require_families=False)
    try:
        validate_constraints_object(effective_constraints(record), f"{path}.effective_constraints", issues, require_core=True)
    except ValueError as exc:
        issues.add(f"{path}.policy_profile", str(exc))
    try:
        validate_source_governance(source_governance(record), f"{path}.effective_source_governance", issues, require_families=True)
    except ValueError as exc:
        issues.add(f"{path}.source_governance", str(exc))


def validate_claim_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "claim_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_enum(record, "claim_type", path, issues, allowed=CLAIM_TYPES)
    require_enum(record, "status", path, issues, allowed=CLAIM_STATUSES)
    require_string(record, "summary", path, issues)
    require_string(record, "statement", path, issues)
    require_int(record, "priority", path, issues, minimum=1, maximum=5)
    require_bool(record, "needs_physical_validation", path, issues)
    validate_time_window(record.get("time_window"), f"{path}.time_window", issues)
    validate_region_scope(record.get("place_scope"), f"{path}.place_scope", issues)
    public_refs = record.get("public_refs")
    if not isinstance(public_refs, list):
        issues.add(f"{path}.public_refs", "Expected a list.", actual=public_refs)
    else:
        for index, ref in enumerate(public_refs):
            validate_artifact_ref(ref, f"{path}.public_refs[{index}]", issues)


def validate_claim_submission_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "submission_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_string(record, "claim_id", path, issues)
    require_enum(record, "claim_type", path, issues, allowed=CLAIM_TYPES)
    require_string(record, "summary", path, issues)
    require_string(record, "statement", path, issues)
    require_string(record, "meaning", path, issues)
    require_int(record, "priority", path, issues, minimum=1, maximum=5)
    require_bool(record, "needs_physical_validation", path, issues)
    require_bool(record, "worth_storing", path, issues)
    require_int(record, "source_signal_count", path, issues, minimum=1)
    validate_time_window(record.get("time_window"), f"{path}.time_window", issues)
    validate_region_scope(record.get("place_scope"), f"{path}.place_scope", issues)
    public_refs = record.get("public_refs")
    if not isinstance(public_refs, list):
        issues.add(f"{path}.public_refs", "Expected a list.", actual=public_refs)
    else:
        for index, ref in enumerate(public_refs):
            validate_artifact_ref(ref, f"{path}.public_refs[{index}]", issues)
    if "compact_audit" in record and record["compact_audit"] is not None:
        validate_compact_audit(record["compact_audit"], f"{path}.compact_audit", issues)


def validate_statistics(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    for field_name in ("min", "max", "mean", "p95"):
        if field_name not in obj:
            continue
        field_value = obj[field_name]
        if field_value is not None and not is_number(field_value):
            issues.add(f"{path}.{field_name}", "Expected a number or null.", actual=field_value)


def validate_compact_audit(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_bool(obj, "representative", path, issues)
    require_int(obj, "retained_count", path, issues, minimum=0)
    require_int(obj, "total_candidate_count", path, issues, minimum=0)
    require_string(obj, "coverage_summary", path, issues)
    concentration_flags = validate_string_list(obj.get("concentration_flags", []), f"{path}.concentration_flags", issues)
    validate_unique_strings(concentration_flags, f"{path}.concentration_flags", issues)
    if "sampling_notes" in obj:
        sampling_notes = validate_string_list(obj.get("sampling_notes", []), f"{path}.sampling_notes", issues)
        validate_unique_strings(sampling_notes, f"{path}.sampling_notes", issues)


def validate_observation_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "observation_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_string(record, "source_skill", path, issues)
    require_string(record, "metric", path, issues)
    require_enum(record, "aggregation", path, issues, allowed=OBSERVATION_AGGREGATIONS)
    value = record.get("value")
    if value is not None and not is_number(value):
        issues.add(f"{path}.value", "Expected a number or null.", actual=value)
    require_string(record, "unit", path, issues)
    if "statistics" in record and record["statistics"] is not None:
        validate_statistics(record["statistics"], f"{path}.statistics", issues)
    validate_time_window(record.get("time_window"), f"{path}.time_window", issues)
    validate_region_scope(record.get("place_scope"), f"{path}.place_scope", issues)
    validate_string_list(record.get("quality_flags"), f"{path}.quality_flags", issues)
    validate_artifact_ref(record.get("provenance"), f"{path}.provenance", issues)


def validate_observation_submission_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "submission_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_string(record, "observation_id", path, issues)
    require_string(record, "source_skill", path, issues)
    require_string(record, "metric", path, issues)
    require_enum(record, "aggregation", path, issues, allowed=OBSERVATION_AGGREGATIONS)
    value = record.get("value")
    if value is not None and not is_number(value):
        issues.add(f"{path}.value", "Expected a number or null.", actual=value)
    require_string(record, "unit", path, issues)
    require_string(record, "meaning", path, issues)
    require_bool(record, "worth_storing", path, issues)
    validate_time_window(record.get("time_window"), f"{path}.time_window", issues)
    validate_region_scope(record.get("place_scope"), f"{path}.place_scope", issues)
    validate_string_list(record.get("quality_flags"), f"{path}.quality_flags", issues)
    validate_artifact_ref(record.get("provenance"), f"{path}.provenance", issues)
    if "compact_audit" in record and record["compact_audit"] is not None:
        validate_compact_audit(record["compact_audit"], f"{path}.compact_audit", issues)


def validate_evidence_card_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "evidence_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_string(record, "claim_id", path, issues)
    require_enum(record, "verdict", path, issues, allowed=EVIDENCE_VERDICTS)
    require_enum(record, "confidence", path, issues, allowed=CONFIDENCE_VALUES)
    require_string(record, "summary", path, issues)
    public_refs = record.get("public_refs")
    if not isinstance(public_refs, list):
        issues.add(f"{path}.public_refs", "Expected a list.", actual=public_refs)
    else:
        for index, ref in enumerate(public_refs):
            validate_artifact_ref(ref, f"{path}.public_refs[{index}]", issues)
    validate_string_list(record.get("observation_ids"), f"{path}.observation_ids", issues)
    validate_string_list(record.get("gaps"), f"{path}.gaps", issues)


def validate_finding(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "finding_id", path, issues)
    require_string(obj, "title", path, issues)
    require_string(obj, "summary", path, issues)
    require_enum(obj, "confidence", path, issues, allowed=CONFIDENCE_VALUES)
    for field_name in ("claim_ids", "observation_ids", "evidence_ids"):
        if field_name in obj:
            validate_string_list(obj[field_name], f"{path}.{field_name}", issues)


def validate_data_readiness_report_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "readiness_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_enum(record, "readiness_status", path, issues, allowed=READINESS_STATUSES)
    require_bool(record, "sufficient_for_matching", path, issues)
    require_string(record, "summary", path, issues)
    findings = record.get("findings")
    if not isinstance(findings, list):
        issues.add(f"{path}.findings", "Expected a list.", actual=findings)
    else:
        for index, finding in enumerate(findings):
            validate_finding(finding, f"{path}.findings[{index}]", issues)
    validate_string_list(record.get("open_questions"), f"{path}.open_questions", issues)
    recommendations = record.get("recommended_next_actions")
    if not isinstance(recommendations, list):
        issues.add(f"{path}.recommended_next_actions", "Expected a list.", actual=recommendations)
    else:
        for index, recommendation in enumerate(recommendations):
            validate_recommendation(recommendation, f"{path}.recommended_next_actions[{index}]", issues)
    referenced_submission_ids = validate_string_list(
        record.get("referenced_submission_ids"),
        f"{path}.referenced_submission_ids",
        issues,
    )
    validate_unique_strings(referenced_submission_ids, f"{path}.referenced_submission_ids", issues)
    if "compact_audit" in record and record["compact_audit"] is not None:
        validate_compact_audit(record["compact_audit"], f"{path}.compact_audit", issues)
    if "override_requests" not in record:
        issues.add(f"{path}.override_requests", "Expected a list.", actual=None)
    else:
        validate_override_request_list(
            record.get("override_requests"),
            f"{path}.override_requests",
            issues,
            run_id=maybe_text(record.get("run_id")),
            round_id=maybe_text(record.get("round_id")),
            agent_role=maybe_text(record.get("agent_role")),
            origin_kind="data-readiness-report",
        )


def validate_matching_authorization_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "authorization_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    role = require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    if role is not None and role != "moderator":
        issues.add(f"{path}.agent_role", "matching-authorization must be moderator-owned.", actual=role)
    require_enum(record, "authorization_status", path, issues, allowed=AUTHORIZATION_STATUSES)
    require_string(record, "summary", path, issues)
    require_string(record, "rationale", path, issues)
    require_bool(record, "moderator_override", path, issues)
    require_bool(record, "allow_isolated_evidence", path, issues)
    readiness_ids = validate_string_list(record.get("referenced_readiness_ids"), f"{path}.referenced_readiness_ids", issues)
    validate_unique_strings(readiness_ids, f"{path}.referenced_readiness_ids", issues)
    claim_ids = validate_string_list(record.get("claim_ids"), f"{path}.claim_ids", issues)
    validate_unique_strings(claim_ids, f"{path}.claim_ids", issues)
    observation_ids = validate_string_list(record.get("observation_ids"), f"{path}.observation_ids", issues)
    validate_unique_strings(observation_ids, f"{path}.observation_ids", issues)
    validate_string_list(record.get("open_questions"), f"{path}.open_questions", issues)
    recommendations = record.get("recommended_next_actions")
    if not isinstance(recommendations, list):
        issues.add(f"{path}.recommended_next_actions", "Expected a list.", actual=recommendations)
    else:
        for index, recommendation in enumerate(recommendations):
            validate_recommendation(recommendation, f"{path}.recommended_next_actions[{index}]", issues)


def validate_matching_pair(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "claim_id", path, issues)
    observation_ids = validate_string_list(obj.get("observation_ids"), f"{path}.observation_ids", issues)
    validate_unique_strings(observation_ids, f"{path}.observation_ids", issues)
    require_number(obj, "support_score", path, issues, minimum=0.0)
    require_number(obj, "contradict_score", path, issues, minimum=0.0)
    validate_string_list(obj.get("notes"), f"{path}.notes", issues)


def validate_matching_result_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "result_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_string(record, "authorization_id", path, issues)
    require_enum(record, "result_status", path, issues, allowed=MATCHING_RESULT_STATUSES)
    require_string(record, "summary", path, issues)
    matched_pairs = record.get("matched_pairs")
    if not isinstance(matched_pairs, list):
        issues.add(f"{path}.matched_pairs", "Expected a list.", actual=matched_pairs)
    else:
        for index, pair in enumerate(matched_pairs):
            validate_matching_pair(pair, f"{path}.matched_pairs[{index}]", issues)
    for field_name in (
        "matched_claim_ids",
        "matched_observation_ids",
        "unmatched_claim_ids",
        "unmatched_observation_ids",
    ):
        values = validate_string_list(record.get(field_name), f"{path}.{field_name}", issues)
        validate_unique_strings(values, f"{path}.{field_name}", issues)


def validate_evidence_adjudication_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "adjudication_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_string(record, "authorization_id", path, issues)
    require_string(record, "matching_result_id", path, issues)
    require_enum(record, "adjudication_status", path, issues, allowed=ADJUDICATION_STATUSES)
    require_string(record, "summary", path, issues)
    require_bool(record, "matching_reasonable", path, issues)
    require_bool(record, "needs_additional_data", path, issues)
    for field_name in ("card_ids", "isolated_entry_ids", "remand_ids", "open_questions"):
        values = validate_string_list(record.get(field_name), f"{path}.{field_name}", issues)
        validate_unique_strings(values, f"{path}.{field_name}", issues)
    recommendations = record.get("recommended_next_actions")
    if not isinstance(recommendations, list):
        issues.add(f"{path}.recommended_next_actions", "Expected a list.", actual=recommendations)
    else:
        for index, recommendation in enumerate(recommendations):
            validate_recommendation(recommendation, f"{path}.recommended_next_actions[{index}]", issues)


def validate_isolated_entry_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "isolated_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "entity_kind", path, issues, allowed=LIBRARY_ENTITY_KINDS)
    require_string(record, "entity_id", path, issues)
    require_string(record, "summary", path, issues)
    require_string(record, "reason", path, issues)


def validate_remand_entry_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "remand_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "entity_kind", path, issues, allowed=LIBRARY_ENTITY_KINDS)
    require_string(record, "entity_id", path, issues)
    require_string(record, "summary", path, issues)
    reasons = validate_string_list(record.get("reasons"), f"{path}.reasons", issues)
    validate_unique_strings(reasons, f"{path}.reasons", issues)
    if not reasons:
        issues.add(f"{path}.reasons", "Expected at least one remand reason.")


def validate_expert_report_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "report_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_enum(record, "status", path, issues, allowed=REPORT_STATUSES)
    require_string(record, "summary", path, issues)
    findings = record.get("findings")
    if not isinstance(findings, list):
        issues.add(f"{path}.findings", "Expected a list.", actual=findings)
    else:
        for index, finding in enumerate(findings):
            validate_finding(finding, f"{path}.findings[{index}]", issues)
    validate_string_list(record.get("open_questions"), f"{path}.open_questions", issues)
    recommendations = record.get("recommended_next_actions")
    if not isinstance(recommendations, list):
        issues.add(
            f"{path}.recommended_next_actions",
            "Expected a list.",
            actual=recommendations,
        )
    else:
        for index, recommendation in enumerate(recommendations):
            validate_recommendation(
                recommendation,
                f"{path}.recommended_next_actions[{index}]",
                issues,
            )
    if "override_requests" not in record:
        issues.add(f"{path}.override_requests", "Expected a list.", actual=None)
    else:
        validate_override_request_list(
            record.get("override_requests"),
            f"{path}.override_requests",
            issues,
            run_id=maybe_text(record.get("run_id")),
            round_id=maybe_text(record.get("round_id")),
            agent_role=maybe_text(record.get("agent_role")),
            origin_kind="expert-report",
        )


def validate_council_decision_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "decision_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "moderator_status", path, issues, allowed=MODERATOR_STATUSES)
    require_number(record, "completion_score", path, issues, minimum=0.0, maximum=1.0)
    require_enum(record, "evidence_sufficiency", path, issues, allowed=EVIDENCE_SUFFICIENCY)
    require_string(record, "decision_summary", path, issues)
    require_bool(record, "next_round_required", path, issues)
    validate_string_list(record.get("missing_evidence_types"), f"{path}.missing_evidence_types", issues)
    tasks = record.get("next_round_tasks")
    if not isinstance(tasks, list):
        issues.add(f"{path}.next_round_tasks", "Expected a list.", actual=tasks)
    else:
        for index, task in enumerate(tasks):
            validate_round_task_object(task, f"{path}.next_round_tasks[{index}]", issues)
    if "final_brief" in record and record["final_brief"] is not None and not isinstance(record["final_brief"], str):
        issues.add(f"{path}.final_brief", "Expected a string when provided.", actual=record["final_brief"])
    if "override_requests" not in record:
        issues.add(f"{path}.override_requests", "Expected a list.", actual=None)
    else:
        validate_override_request_list(
            record.get("override_requests"),
            f"{path}.override_requests",
            issues,
            run_id=maybe_text(record.get("run_id")),
            round_id=maybe_text(record.get("round_id")),
            agent_role="moderator",
            origin_kind="council-decision",
        )


VALIDATORS = {
    "mission": validate_mission_object,
    "round-task": validate_round_task_object,
    "source-selection": validate_source_selection_object,
    "override-request": validate_override_request_object,
    "claim": validate_claim_object,
    "claim-submission": validate_claim_submission_object,
    "observation": validate_observation_object,
    "observation-submission": validate_observation_submission_object,
    "evidence-card": validate_evidence_card_object,
    "data-readiness-report": validate_data_readiness_report_object,
    "matching-authorization": validate_matching_authorization_object,
    "matching-result": validate_matching_result_object,
    "evidence-adjudication": validate_evidence_adjudication_object,
    "isolated-entry": validate_isolated_entry_object,
    "remand-entry": validate_remand_entry_object,
    "expert-report": validate_expert_report_object,
    "council-decision": validate_council_decision_object,
}

EXAMPLES: dict[str, Any] = {
    "mission": read_json(EXAMPLES_DIR / "mission.json"),
    "round-task": read_json(EXAMPLES_DIR / "round_task.json"),
    "source-selection": read_json(EXAMPLES_DIR / "source_selection.json"),
    "override-request": read_json(EXAMPLES_DIR / "override_request.json"),
    "claim": read_json(EXAMPLES_DIR / "claim.json"),
    "claim-submission": read_json(EXAMPLES_DIR / "claim_submission.json"),
    "observation": read_json(EXAMPLES_DIR / "observation.json"),
    "observation-submission": read_json(EXAMPLES_DIR / "observation_submission.json"),
    "evidence-card": read_json(EXAMPLES_DIR / "evidence_card.json"),
    "data-readiness-report": read_json(EXAMPLES_DIR / "data_readiness_report.json"),
    "matching-authorization": read_json(EXAMPLES_DIR / "matching_authorization.json"),
    "matching-result": read_json(EXAMPLES_DIR / "matching_result.json"),
    "evidence-adjudication": read_json(EXAMPLES_DIR / "evidence_adjudication.json"),
    "isolated-entry": read_json(EXAMPLES_DIR / "isolated_entry.json"),
    "remand-entry": read_json(EXAMPLES_DIR / "remand_entry.json"),
    "expert-report": read_json(EXAMPLES_DIR / "expert_report.json"),
    "council-decision": read_json(EXAMPLES_DIR / "council_decision.json"),
}


def validate_payload(kind: str, payload: Any) -> dict[str, Any]:
    issues = IssueCollector()
    validator = VALIDATORS[kind]
    if isinstance(payload, list):
        for index, item in enumerate(payload):
            validator(item, f"{kind}[{index}]", issues)
        top_level = "list"
        item_count = len(payload)
    else:
        validator(payload, kind, issues)
        top_level = "object"
        item_count = 1
    return {
        "kind": kind,
        "top_level": top_level,
        "item_count": item_count,
        "validation": issues.summary(),
    }


def load_ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8")


def parse_point(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 2:
        raise ValueError("--point must be in latitude,longitude format.")
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError as exc:
        raise ValueError("--point latitude and longitude must be numeric.") from exc
    if latitude < -90 or latitude > 90:
        raise ValueError("--point latitude must be between -90 and 90.")
    if longitude < -180 or longitude > 180:
        raise ValueError("--point longitude must be between -180 and 180.")
    return {"type": "Point", "latitude": latitude, "longitude": longitude}


def parse_bbox(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be in west,south,east,north format.")
    try:
        west = float(parts[0])
        south = float(parts[1])
        east = float(parts[2])
        north = float(parts[3])
    except ValueError as exc:
        raise ValueError("--bbox coordinates must be numeric.") from exc
    if west < -180 or west > 180 or east < -180 or east > 180:
        raise ValueError("--bbox west/east must be between -180 and 180.")
    if south < -90 or south > 90 or north < -90 or north > 90:
        raise ValueError("--bbox south/north must be between -90 and 90.")
    if east <= west:
        raise ValueError("--bbox east must be greater than west.")
    if north <= south:
        raise ValueError("--bbox north must be greater than south.")
    return {"type": "BBox", "west": west, "south": south, "east": east, "north": north}


def round_dir_name(round_id: str) -> str:
    value = round_id.strip()
    if not ROUND_ID_PATTERN.match(value):
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 style.")
    return value.replace("-", "_")


def round_id_from_dirname(dirname: str) -> str | None:
    match = ROUND_DIR_PATTERN.match(dirname.strip())
    if match is None:
        return None
    return f"round-{match.group(1)}"


def round_number(round_id: str) -> int:
    if not ROUND_ID_PATTERN.match(round_id):
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 style.")
    return int(round_id.split("-")[-1])


def round_sort_key(round_id: str) -> tuple[int, str]:
    try:
        return (round_number(round_id), round_id)
    except ValueError:
        return (sys.maxsize, round_id)


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    governance_families = source_governance_for_role(mission, role)
    values = [
        maybe_text(skill)
        for family in governance_families
        for skill in family.get("skills", [])
        if maybe_text(skill)
    ]
    return unique_strings(values)


def expected_output_kinds_for_role(role: str) -> list[str]:
    if role == "sociologist":
        return ["source-selection", "claim", "claim-submission", "data-readiness-report", "expert-report"]
    if role == "environmentalist":
        return ["source-selection", "observation", "observation-submission", "data-readiness-report", "expert-report"]
    if role == "historian":
        return ["expert-report"]
    return ["expert-report"]


def default_round_tasks(*, mission: dict[str, Any], round_id: str) -> list[dict[str, Any]]:
    run_id = mission["run_id"]
    geometry = mission.get("region", {}).get("geometry") if isinstance(mission.get("region"), dict) else None
    mission_window = mission.get("window") if isinstance(mission.get("window"), dict) else {}

    sociologist_task = copy.deepcopy(EXAMPLES["round-task"])
    sociologist_task["task_id"] = f"task-sociologist-{round_id}-01"
    sociologist_task["run_id"] = run_id
    sociologist_task["round_id"] = round_id
    sociologist_task["assigned_role"] = "sociologist"
    sociologist_task["objective"] = (
        "Identify mission-window public claims and judge whether the current public-evidence preparation is sufficient for later matching."
    )
    sociologist_task["expected_output_kinds"] = expected_output_kinds_for_role("sociologist")
    sociologist_task["inputs"] = {
        "mission_geometry": geometry,
        "mission_window": mission_window,
        "evidence_requirements": [
            {
                "requirement_id": f"req-sociologist-{round_id}-public-claims",
                "requirement_type": "public-claim-discovery",
                "summary": "Collect attributable public claims that explain who is saying what in the mission window.",
                "priority": "high",
                "focus_claim_ids": [],
                "anchor_refs": [],
            }
        ],
    }

    environmental_task = copy.deepcopy(EXAMPLES["round-task"])
    environmental_task["task_id"] = f"task-environmentalist-{round_id}-01"
    environmental_task["run_id"] = run_id
    environmental_task["round_id"] = round_id
    environmental_task["assigned_role"] = "environmentalist"
    environmental_task["objective"] = (
        "Collect mission-relevant physical observations and judge whether the current physical-evidence preparation is sufficient for later matching."
    )
    environmental_task["expected_output_kinds"] = expected_output_kinds_for_role("environmentalist")
    environmental_task["inputs"] = {
        "mission_geometry": geometry,
        "mission_window": mission_window,
        "evidence_requirements": [
            {
                "requirement_id": f"req-environmentalist-{round_id}-physical-corroboration",
                "requirement_type": "physical-corroboration",
                "summary": "Collect mission-window physical observations that can support or contradict candidate claims.",
                "priority": "high",
                "focus_claim_ids": [],
                "anchor_refs": [],
            }
        ],
    }

    return [sociologist_task, environmental_task]


def placeholder_source_selection(
    *,
    run_id: str,
    round_id: str,
    role: str,
    task_ids: list[str],
    allowed_sources: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "selection_id": f"source-selection-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "pending",
        "summary": f"Pending {role} source selection.",
        "task_ids": task_ids,
        "allowed_sources": allowed_sources,
        "selected_sources": [],
        "source_decisions": [],
        "family_plans": [],
        "override_requests": [],
    }


def placeholder_report(*, run_id: str, round_id: str, role: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "needs-more-evidence",
        "summary": f"Pending {role} execution.",
        "findings": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }


def normalize_round_tasks(
    *,
    tasks: list[dict[str, Any]],
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError(f"Task index {index} is not an object.")
        candidate = copy.deepcopy(task)
        candidate["run_id"] = run_id
        candidate["round_id"] = round_id
        result = validate_payload("round-task", candidate)
        if not result["validation"]["ok"]:
            issues = result["validation"]["issues"]
            raise ValueError(f"Task index {index} failed validation: {issues}")
        normalized.append(candidate)
    if not normalized:
        raise ValueError("At least one round-task is required to scaffold a round.")
    return normalized


def scaffold_round(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    tasks: list[dict[str, Any]],
    mission: dict[str, Any] | None,
    pretty: bool,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    round_path = run_path / round_dir_name(round_id)
    normalized_tasks = normalize_round_tasks(tasks=tasks, run_id=run_id, round_id=round_id)
    source_selection_roles = ("sociologist", "environmentalist")
    source_selection_files = {
        role: placeholder_source_selection(
            run_id=run_id,
            round_id=round_id,
            role=role,
            task_ids=[
                item["task_id"]
                for item in normalized_tasks
                if item.get("assigned_role") == role and isinstance(item.get("task_id"), str)
            ],
            allowed_sources=allowed_sources_for_role(mission or {}, role),
        )
        for role in source_selection_roles
    }

    files_to_write = {
        round_path / "moderator" / "tasks.json": normalized_tasks,
        round_path / "shared" / "claims.json": [],
        round_path / "shared" / "observations.json": [],
        round_path / "shared" / "evidence_cards.json": [],
        round_path / "moderator" / "override_requests.json": [],
        round_path / "sociologist" / "claim_submissions.json": [],
        round_path / "sociologist" / "override_requests.json": [],
        round_path / "environmentalist" / "observation_submissions.json": [],
        round_path / "environmentalist" / "override_requests.json": [],
        round_path / "historian" / "override_requests.json": [],
        round_path / "sociologist" / "source_selection.json": source_selection_files["sociologist"],
        round_path / "environmentalist" / "source_selection.json": source_selection_files["environmentalist"],
        round_path / "sociologist" / "sociologist_report.json": placeholder_report(
            run_id=run_id,
            round_id=round_id,
            role="sociologist",
        ),
        round_path / "environmentalist" / "environmentalist_report.json": placeholder_report(
            run_id=run_id,
            round_id=round_id,
            role="environmentalist",
        ),
    }

    directories = (
        round_path / "sociologist" / "raw",
        round_path / "sociologist" / "normalized",
        round_path / "sociologist" / "derived",
        round_path / "environmentalist" / "raw",
        round_path / "environmentalist" / "normalized",
        round_path / "environmentalist" / "derived",
        round_path / "historian" / "raw",
        round_path / "historian" / "normalized",
        round_path / "historian" / "derived",
        round_path / "moderator" / "derived",
        round_path / "shared" / "contexts",
        round_path / "shared" / "evidence-library",
    )
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

    for path, payload in files_to_write.items():
        write_json(path, payload, pretty=pretty)

    library_views = {
        round_path / "shared" / "evidence-library" / "claims_active.json": [],
        round_path / "shared" / "evidence-library" / "observations_active.json": [],
        round_path / "shared" / "evidence-library" / "cards_active.json": [],
        round_path / "shared" / "evidence-library" / "isolated_active.json": [],
        round_path / "shared" / "evidence-library" / "remands_open.json": [],
        round_path / "shared" / "evidence-library" / "context_sociologist.json": {},
        round_path / "shared" / "evidence-library" / "context_environmentalist.json": {},
        round_path / "shared" / "evidence-library" / "context_moderator.json": {},
    }
    for path, payload in library_views.items():
        write_json(path, payload, pretty=pretty)
    atomic_write_text_file(round_path / "shared" / "evidence-library" / "ledger.jsonl", "")

    return {
        "round_id": round_id,
        "round_dir": str(round_path),
        "files_written": [str(path) for path in sorted({*files_to_write, *library_views, round_path / "shared" / "evidence-library" / "ledger.jsonl"})],
        "directories_ready": [str(path) for path in sorted(directories)],
    }


def scaffold_run_from_mission(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]] | None,
    pretty: bool,
) -> dict[str, Any]:
    validation = validate_payload("mission", mission)
    if not validation["validation"]["ok"]:
        raise ValueError(f"Mission payload failed validation: {validation['validation']['issues']}")

    run_path = run_dir.expanduser().resolve()
    run_path.mkdir(parents=True, exist_ok=True)
    run_id = mission["run_id"]
    round_id = "round-001"
    task_list = tasks if tasks is not None else default_round_tasks(mission=mission, round_id=round_id)

    mission_path = run_path / "mission.json"
    write_json(mission_path, mission, pretty=pretty)
    round_result = scaffold_round(
        run_dir=run_path,
        run_id=run_id,
        round_id=round_id,
        tasks=task_list,
        mission=mission,
        pretty=pretty,
    )
    return {
        "run_dir": str(run_path),
        "run_id": run_id,
        "round_id": round_id,
        "mission_path": str(mission_path),
        "round": round_result,
        "schema_path": str(SCHEMA_PATH),
    }


def scaffold_run(
    *,
    run_dir: Path,
    run_id: str,
    topic: str,
    objective: str,
    start_utc: str,
    end_utc: str,
    region_label: str,
    geometry: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    if parse_utc_datetime(start_utc) is None:
        raise ValueError("--start-utc must be RFC3339 UTC with trailing Z.")
    if parse_utc_datetime(end_utc) is None:
        raise ValueError("--end-utc must be RFC3339 UTC with trailing Z.")
    if parse_utc_datetime(end_utc) < parse_utc_datetime(start_utc):
        raise ValueError("--end-utc must be >= --start-utc.")

    mission = copy.deepcopy(EXAMPLES["mission"])
    mission["run_id"] = run_id
    mission["topic"] = topic
    mission["objective"] = objective
    mission["window"]["start_utc"] = start_utc
    mission["window"]["end_utc"] = end_utc
    mission["region"]["label"] = region_label
    mission["region"]["geometry"] = geometry
    return scaffold_run_from_mission(
        run_dir=run_dir,
        mission=mission,
        tasks=None,
        pretty=pretty,
    )


def validate_bundle(run_dir: Path) -> dict[str, Any]:
    bundle_path = run_dir.expanduser().resolve()
    results: list[dict[str, Any]] = []
    missing_required: list[str] = []
    missing_optional: list[str] = []
    round_summaries: list[dict[str, Any]] = []

    mission_path = bundle_path / "mission.json"
    if not mission_path.exists():
        missing_required.append(str(mission_path))
    else:
        mission_payload = read_json(mission_path)
        mission_result = validate_payload("mission", mission_payload)
        mission_result["path"] = str(mission_path)
        results.append(mission_result)

    round_ids: list[str] = []
    for child in sorted(bundle_path.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        round_id = round_id_from_dirname(child.name)
        if round_id:
            round_ids.append(round_id)
    round_ids.sort(key=round_sort_key)
    if not round_ids:
        missing_required.append(str(bundle_path / "round_001"))

    for round_id in round_ids:
        round_path = bundle_path / round_dir_name(round_id)
        round_required = {
            round_path / "moderator" / "tasks.json": "round-task",
            round_path / "shared" / "claims.json": "claim",
            round_path / "shared" / "observations.json": "observation",
            round_path / "shared" / "evidence_cards.json": "evidence-card",
            round_path / "sociologist" / "claim_submissions.json": "claim-submission",
            round_path / "environmentalist" / "observation_submissions.json": "observation-submission",
            round_path / "sociologist" / "source_selection.json": "source-selection",
            round_path / "environmentalist" / "source_selection.json": "source-selection",
            round_path / "sociologist" / "sociologist_report.json": "expert-report",
            round_path / "environmentalist" / "environmentalist_report.json": "expert-report",
        }
        round_optional = {
        round_path / "historian" / "historian_report.json": "expert-report",
        round_path / "moderator" / "council_decision.json": "council-decision",
        round_path / "moderator" / "override_requests.json": "override-request",
        round_path / "sociologist" / "override_requests.json": "override-request",
        round_path / "environmentalist" / "override_requests.json": "override-request",
        round_path / "historian" / "override_requests.json": "override-request",
        round_path / "sociologist" / "data_readiness_report.json": "data-readiness-report",
        round_path / "environmentalist" / "data_readiness_report.json": "data-readiness-report",
        round_path / "moderator" / "matching_authorization.json": "matching-authorization",
            round_path / "shared" / "matching_result.json": "matching-result",
            round_path / "shared" / "evidence_adjudication.json": "evidence-adjudication",
            round_path / "shared" / "evidence-library" / "claims_active.json": "claim-submission",
            round_path / "shared" / "evidence-library" / "observations_active.json": "observation-submission",
            round_path / "shared" / "evidence-library" / "cards_active.json": "evidence-card",
            round_path / "shared" / "evidence-library" / "isolated_active.json": "isolated-entry",
            round_path / "shared" / "evidence-library" / "remands_open.json": "remand-entry",
        }

        round_results: list[dict[str, Any]] = []
        round_missing_required: list[str] = []
        round_missing_optional: list[str] = []

        for path, kind in round_required.items():
            if not path.exists():
                round_missing_required.append(str(path))
                continue
            payload = read_json(path)
            result = validate_payload(kind, payload)
            result["path"] = str(path)
            round_results.append(result)
            results.append(result)

        for path, kind in round_optional.items():
            if not path.exists():
                round_missing_optional.append(str(path))
                continue
            payload = read_json(path)
            result = validate_payload(kind, payload)
            result["path"] = str(path)
            round_results.append(result)
            results.append(result)

        missing_required.extend(round_missing_required)
        missing_optional.extend(round_missing_optional)
        round_summaries.append(
            {
                "round_id": round_id,
                "round_dir": str(round_path),
                "missing_required_files": round_missing_required,
                "missing_optional_files": round_missing_optional,
                "results": round_results,
            }
        )

    ok = not missing_required and all(item["validation"]["ok"] for item in results)
    return {
        "run_dir": str(bundle_path),
        "ok": ok,
        "round_ids": round_ids,
        "missing_required_files": missing_required,
        "missing_optional_files": missing_optional,
        "results": results,
        "rounds": round_summaries,
    }


def command_list_kinds(_: argparse.Namespace) -> dict[str, Any]:
    return {
        "kinds": list(OBJECT_KINDS),
        "schema_path": str(SCHEMA_PATH),
        "ddl_path": str(DDL_PATH),
    }


def command_write_example(args: argparse.Namespace) -> dict[str, Any]:
    payload = copy.deepcopy(EXAMPLES[args.kind])
    output_path = Path(args.output).expanduser().resolve()
    write_json(output_path, payload, pretty=args.pretty)
    return {"kind": args.kind, "output": str(output_path)}


def command_validate(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input).expanduser().resolve()
    payload = read_json(input_path)
    result = validate_payload(args.kind, payload)
    result["input"] = str(input_path)
    return result


def command_init_db(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    ddl = load_ddl()
    with sqlite3.connect(db_path) as conn:
        conn.executescript(ddl)
        conn.commit()
    return {
        "db": str(db_path),
        "ddl_path": str(DDL_PATH),
        "initialized_at": utc_now_iso(),
    }


def command_scaffold_run(args: argparse.Namespace) -> dict[str, Any]:
    if bool(args.point) == bool(args.bbox):
        raise ValueError("Provide exactly one of --point or --bbox.")
    geometry = parse_point(args.point) if args.point else parse_bbox(args.bbox)
    return scaffold_run(
        run_dir=Path(args.run_dir),
        run_id=args.run_id,
        topic=args.topic,
        objective=args.objective,
        start_utc=args.start_utc,
        end_utc=args.end_utc,
        region_label=args.region_label,
        geometry=geometry,
        pretty=args.pretty,
    )


def command_scaffold_run_from_mission(args: argparse.Namespace) -> dict[str, Any]:
    mission_path = Path(args.mission_input).expanduser().resolve()
    mission_payload = read_json(mission_path)
    tasks_payload: list[dict[str, Any]] | None = None
    if args.tasks_input:
        tasks_path = Path(args.tasks_input).expanduser().resolve()
        loaded_tasks = read_json(tasks_path)
        if not isinstance(loaded_tasks, list):
            raise ValueError("--tasks-input must contain a JSON list of round-task objects.")
        tasks_payload = [item for item in loaded_tasks if isinstance(item, dict)]
        if len(tasks_payload) != len(loaded_tasks):
            raise ValueError("--tasks-input must contain only JSON objects.")
    result = scaffold_run_from_mission(
        run_dir=Path(args.run_dir),
        mission=mission_payload,
        tasks=tasks_payload,
        pretty=args.pretty,
    )
    result["mission_input"] = str(mission_path)
    if args.tasks_input:
        result["tasks_input"] = str(Path(args.tasks_input).expanduser().resolve())
    return result


def command_scaffold_round(args: argparse.Namespace) -> dict[str, Any]:
    tasks_path = Path(args.tasks_input).expanduser().resolve()
    task_payload = read_json(tasks_path)
    if not isinstance(task_payload, list):
        raise ValueError("--tasks-input must contain a JSON list of round-task objects.")
    if not all(isinstance(item, dict) for item in task_payload):
        raise ValueError("--tasks-input must contain only JSON objects.")

    mission_path = Path(args.mission_input).expanduser().resolve() if args.mission_input else Path(args.run_dir).expanduser().resolve() / "mission.json"
    mission_payload = read_json(mission_path)
    mission_validation = validate_payload("mission", mission_payload)
    if not mission_validation["validation"]["ok"]:
        raise ValueError(f"Mission payload failed validation: {mission_validation['validation']['issues']}")

    run_id = mission_payload["run_id"]
    result = scaffold_round(
        run_dir=Path(args.run_dir),
        run_id=run_id,
        round_id=args.round_id,
        tasks=task_payload,
        mission=mission_payload,
        pretty=args.pretty,
    )
    result["mission_input"] = str(mission_path)
    result["tasks_input"] = str(tasks_path)
    return result


def command_validate_bundle(args: argparse.Namespace) -> dict[str, Any]:
    return validate_bundle(Path(args.run_dir))


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and scaffold eco-council shared contracts.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_list = subparsers.add_parser("list-kinds", help="List canonical object kinds.")
    add_pretty_flag(parser_list)

    parser_write = subparsers.add_parser("write-example", help="Write one example payload to disk.")
    parser_write.add_argument("--kind", required=True, choices=OBJECT_KINDS)
    parser_write.add_argument("--output", required=True, help="Output JSON path.")
    add_pretty_flag(parser_write)

    parser_validate = subparsers.add_parser("validate", help="Validate one JSON file.")
    parser_validate.add_argument("--kind", required=True, choices=OBJECT_KINDS)
    parser_validate.add_argument("--input", required=True, help="Input JSON path.")
    add_pretty_flag(parser_validate)

    parser_init_db = subparsers.add_parser("init-db", help="Initialize the canonical SQLite database.")
    parser_init_db.add_argument("--db", required=True, help="SQLite database path.")
    add_pretty_flag(parser_init_db)

    parser_scaffold = subparsers.add_parser("scaffold-run", help="Scaffold one eco-council run directory.")
    parser_scaffold.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold.add_argument("--run-id", required=True, help="Stable run identifier.")
    parser_scaffold.add_argument("--topic", required=True, help="Mission topic.")
    parser_scaffold.add_argument("--objective", required=True, help="Mission objective.")
    parser_scaffold.add_argument("--start-utc", required=True, help="Mission start datetime in UTC.")
    parser_scaffold.add_argument("--end-utc", required=True, help="Mission end datetime in UTC.")
    parser_scaffold.add_argument("--region-label", required=True, help="Human-readable region label.")
    parser_scaffold.add_argument("--point", help="Point geometry as latitude,longitude.")
    parser_scaffold.add_argument("--bbox", help="BBox geometry as west,south,east,north.")
    add_pretty_flag(parser_scaffold)

    parser_scaffold_mission = subparsers.add_parser(
        "scaffold-run-from-mission",
        help="Scaffold one eco-council run directory from an existing mission JSON payload.",
    )
    parser_scaffold_mission.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold_mission.add_argument("--mission-input", required=True, help="Mission JSON path.")
    parser_scaffold_mission.add_argument(
        "--tasks-input",
        default="",
        help="Optional JSON path containing initial round-task list for round-001.",
    )
    add_pretty_flag(parser_scaffold_mission)

    parser_scaffold_round = subparsers.add_parser(
        "scaffold-round",
        help="Scaffold one additional round directory from a validated round-task list.",
    )
    parser_scaffold_round.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold_round.add_argument("--round-id", required=True, help="Round identifier, for example round-002.")
    parser_scaffold_round.add_argument("--tasks-input", required=True, help="JSON path containing round-task list.")
    parser_scaffold_round.add_argument(
        "--mission-input",
        default="",
        help="Optional mission JSON path. Defaults to <run-dir>/mission.json.",
    )
    add_pretty_flag(parser_scaffold_round)

    parser_bundle = subparsers.add_parser(
        "validate-bundle",
        help="Validate a scaffolded run bundle and any canonical files already produced.",
    )
    parser_bundle.add_argument("--run-dir", required=True, help="Run directory to inspect.")
    add_pretty_flag(parser_bundle)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command_map = {
        "list-kinds": command_list_kinds,
        "write-example": command_write_example,
        "validate": command_validate,
        "init-db": command_init_db,
        "scaffold-run": command_scaffold_run,
        "scaffold-run-from-mission": command_scaffold_run_from_mission,
        "scaffold-round": command_scaffold_round,
        "validate-bundle": command_validate_bundle,
    }
    try:
        payload = command_map[args.command](args)
    except Exception as exc:
        error_payload = {
            "command": args.command,
            "ok": False,
            "error": str(exc),
        }
        print(pretty_json(error_payload, pretty=getattr(args, "pretty", False)))
        return 1

    result = {
        "command": args.command,
        "ok": True,
        "payload": payload,
    }
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
