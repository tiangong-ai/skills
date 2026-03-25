#!/usr/bin/env python3
"""Build report packets and moderator decision drafts for eco-council rounds."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import os
import re
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
CONTRACT_SCRIPT_PATH = SKILL_DIR.parent / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"

SCHEMA_VERSION = "1.0.0"
REPORT_ROLES = ("sociologist", "environmentalist")
READINESS_ROLES = ("sociologist", "environmentalist")
PROMOTABLE_REPORT_ROLES = ("sociologist", "environmentalist", "historian")
VERDICT_SCORES = {"supports": 1.0, "contradicts": 1.0, "mixed": 0.6, "insufficient": 0.25}
METEOROLOGY_METRICS = {"temperature_2m", "wind_speed_10m", "relative_humidity_2m", "precipitation_sum", "precipitation"}
PRECIPITATION_METRICS = {
    "precipitation",
    "precipitation_sum",
    "soil_moisture_0_to_7cm",
}
HYDROLOGY_METRICS = {
    "river_discharge",
    "river_discharge_mean",
    "river_discharge_max",
    "river_discharge_min",
    "river_discharge_p25",
    "river_discharge_p75",
    "gage_height",
}
QUESTION_RULES = (
    ("station-grade corroboration is missing", "Can station-grade air-quality measurements be added for the same mission window?"),
    ("modeled background fields should be cross-checked", "Can modeled air-quality fields be cross-checked with station or local observations?"),
    ("no mission-aligned observations matched", "Should the next round expand physical coverage or narrow claim scope so observations can be matched?"),
)
NEXT_ACTION_LIBRARY: dict[str, dict[str, Any]] = {
    "normalized-public-claims": {
        "assigned_role": "sociologist",
        "objective": "Collect and normalize concrete mission-window public claims from approved news and discussion sources.",
        "reason": "The round did not produce enough normalized public claims to assess public concern, event severity, or attribution narratives.",
        "requirement_type": "public-claim-discovery",
        "requirement_summary": "Collect attributable public claims from independent public-discussion channels in the mission window.",
        "priority": "high",
    },
    "evidence-cards-linking-public-claims-to-physical-observations": {
        "assigned_role": "sociologist",
        "objective": "Recover more attributable public claims that can be linked directly against the available physical observations.",
        "reason": "Public-side evidence needs more concrete and attributable claim phrasing before physical evidence cards can be linked reliably.",
        "requirement_type": "claim-attribution-recovery",
        "requirement_summary": "Recover attributable public claims that can be matched against physical observations.",
        "priority": "high",
    },
    "station-air-quality": {
        "assigned_role": "environmentalist",
        "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
        "reason": "Station-grade corroboration remains incomplete or modeled fields still need cross-checking.",
        "requirement_type": "station-air-quality",
        "requirement_summary": "Add station-grade air-quality corroboration in the same mission window and geometry.",
        "priority": "high",
    },
    "fire-detection": {
        "assigned_role": "environmentalist",
        "objective": "Fetch fire-detection evidence aligned with the mission window and geometry.",
        "reason": "Wildfire-related claims still lack direct fire-detection corroboration.",
        "requirement_type": "fire-detection",
        "requirement_summary": "Add direct fire-detection evidence aligned with the mission window and geometry.",
        "priority": "high",
    },
    "meteorology-background": {
        "assigned_role": "environmentalist",
        "objective": "Add meteorology background such as wind, humidity, and precipitation for the same mission window.",
        "reason": "Physical interpretation still needs weather context.",
        "requirement_type": "meteorology-background",
        "requirement_summary": "Add weather context such as wind, humidity, and precipitation for interpretation.",
        "priority": "medium",
    },
    "precipitation-hydrology": {
        "assigned_role": "environmentalist",
        "objective": "Add precipitation or flood-related evidence for the same mission window and geometry.",
        "reason": "Flood or water-related claims still lack direct hydrometeorological corroboration.",
        "requirement_type": "precipitation-hydrology",
        "requirement_summary": "Add direct hydrometeorological evidence for flood or water-related claims.",
        "priority": "high",
    },
    "temperature-extremes": {
        "assigned_role": "environmentalist",
        "objective": "Add temperature-extreme evidence for the same mission window and geometry.",
        "reason": "Heat-related claims still lack direct thermal corroboration.",
        "requirement_type": "temperature-extremes",
        "requirement_summary": "Add direct temperature evidence for heat-related claims.",
        "priority": "high",
    },
    "precipitation-soil-moisture": {
        "assigned_role": "environmentalist",
        "objective": "Add precipitation and soil-moisture evidence for the same mission window and geometry.",
        "reason": "Drought-related claims still lack direct precipitation or soil-moisture corroboration.",
        "requirement_type": "precipitation-soil-moisture",
        "requirement_summary": "Add precipitation and soil-moisture evidence for drought-related claims.",
        "priority": "high",
    },
    "policy-comment-coverage": {
        "assigned_role": "sociologist",
        "objective": "Collect more policy-comment or docket evidence for the same environmental issue.",
        "reason": "Policy-reaction claims still need stronger docket or public-comment coverage.",
        "requirement_type": "policy-comment-coverage",
        "requirement_summary": "Expand rulemaking or docket evidence for policy-reaction claims.",
        "priority": "medium",
    },
    "public-discussion-coverage": {
        "assigned_role": "sociologist",
        "objective": "Collect more independent public-discussion evidence for the same mission window.",
        "reason": "Current public-claim coverage is too thin or concentrated in too few channels.",
        "requirement_type": "public-discussion-coverage",
        "requirement_summary": "Broaden independent public-discussion coverage beyond the currently dominant channels.",
        "priority": "medium",
    },
}
PUBLIC_SOURCE_FAMILIES = {
    "gdelt-doc-search": "gdelt",
    "gdelt-events-fetch": "gdelt",
    "gdelt-mentions-fetch": "gdelt",
    "gdelt-gkg-fetch": "gdelt",
    "bluesky-cascade-fetch": "bluesky",
    "youtube-video-search": "youtube",
    "youtube-comments-fetch": "youtube",
    "federal-register-doc-fetch": "rulemaking",
    "regulationsgov-comments-fetch": "rulemaking",
    "regulationsgov-comment-detail-fetch": "rulemaking",
}
METRIC_FAMILY_GROUPS = {
    "air-quality": {
        "pm2_5",
        "pm2_5_aqi",
        "pm10",
        "pm10_aqi",
        "us_aqi",
        "nitrogen_dioxide",
        "nitrogen_dioxide_aqi",
        "ozone",
        "ozone_aqi",
        "sulfur_dioxide",
        "sulfur_dioxide_aqi",
        "carbon_monoxide",
        "carbon_monoxide_aqi",
    },
    "fire-detection": {
        "fire_detection",
        "fire_detection_count",
    },
    "meteorology": {
        "temperature_2m",
        "wind_speed_10m",
        "relative_humidity_2m",
        "precipitation",
        "precipitation_sum",
    },
    "hydrology": {
        "river_discharge",
        "river_discharge_mean",
        "river_discharge_max",
        "river_discharge_min",
        "river_discharge_p25",
        "river_discharge_p75",
        "gage_height",
    },
    "soil": {
        "soil_moisture_0_to_7cm",
    },
}
DEFAULT_ENVIRONMENT_FAMILY_ORDER = ("air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def normalize_space(value: str) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(str(value))


def truncate_text(value: str, limit: int) -> str:
    text = normalize_space(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    atomic_write_text_file(path, pretty_json(payload, pretty=pretty) + "\n")


def write_text(path: Path, text: str) -> None:
    atomic_write_text_file(path, text)


def atomic_write_text_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
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


def load_canonical_list(path: Path) -> list[dict[str, Any]]:
    payload = load_json_if_exists(path)
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise ValueError(f"Expected list in {path}")
    return [item for item in payload if isinstance(item, dict)]


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def counter_dict(values: list[str]) -> dict[str, int]:
    return dict(Counter(item for item in values if item))


def round_directory_name(round_id: str) -> str:
    return round_id.replace("-", "_")


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_directory_name(round_id)


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


def mission_constraints(mission: dict[str, Any]) -> dict[str, int]:
    values = contract_call("effective_constraints", mission)
    if isinstance(values, dict):
        return {key: int(value) for key, value in values.items() if isinstance(value, int) and value > 0}
    constraints = mission.get("constraints")
    if not isinstance(constraints, dict):
        return {}
    result: dict[str, int] = {}
    for key in ("max_rounds", "max_claims_per_round", "max_tasks_per_round", "claim_target_per_round", "claim_hard_cap_per_round"):
        value = constraints.get(key)
        if isinstance(value, int) and value > 0:
            result[key] = value
    return result


def mission_policy_profile(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("policy_profile_summary", mission)
    if isinstance(value, dict):
        return value
    return {}


def evidence_requirement_for_recommendation(
    *,
    recommendation: dict[str, Any],
    template: dict[str, Any] | None,
    role: str,
    requirement_id: str,
    focus_claim_ids: list[str],
    upstream_round_id: str,
    anchor_refs: list[str] | None = None,
) -> dict[str, Any]:
    requirement_type = maybe_text(template.get("requirement_type")) if isinstance(template, dict) else ""
    if not requirement_type:
        requirement_type = re.sub(r"[^a-z0-9]+", "-", maybe_text(recommendation.get("objective")).lower()).strip("-") or "follow-up"
    summary = maybe_text(template.get("requirement_summary")) if isinstance(template, dict) else ""
    if not summary:
        summary = maybe_text(recommendation.get("reason")) or maybe_text(recommendation.get("objective"))
    priority = maybe_text(template.get("priority")) if isinstance(template, dict) else ""
    if priority not in {"low", "medium", "high"}:
        priority = "medium" if role == "sociologist" else "high"
    resolved_anchor_refs = [maybe_text(item) for item in (anchor_refs or []) if maybe_text(item)]
    if not resolved_anchor_refs:
        resolved_anchor_refs = [f"{upstream_round_id}:claim:{claim_id}" for claim_id in focus_claim_ids if maybe_text(claim_id)]
    return {
        "requirement_id": requirement_id,
        "requirement_type": requirement_type,
        "summary": summary,
        "priority": priority,
        "focus_claim_ids": focus_claim_ids,
        "anchor_refs": resolved_anchor_refs,
    }


def shared_claims_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "claims.json"


def shared_observations_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "observations.json"


def shared_evidence_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_cards.json"


def claim_candidates_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "normalized" / "claim_candidates.json"


def observation_candidates_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "normalized" / "observation_candidates.json"


def claim_curation_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "claim_curation.json"


def observation_curation_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "observation_curation.json"


def claim_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "claim_submissions.json"


def observation_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "observation_submissions.json"


def data_readiness_report_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "data_readiness_report.json"


def data_readiness_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_data_readiness_draft.json"


def data_readiness_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "data_readiness_packet.json"


def data_readiness_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_data_readiness_prompt.txt"


def claim_curation_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "derived" / "claim_curation_draft.json"


def observation_curation_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "derived" / "observation_curation_draft.json"


def claim_curation_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "derived" / "claim_curation_packet.json"


def observation_curation_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "derived" / "observation_curation_packet.json"


def claim_curation_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "derived" / "openclaw_claim_curation_prompt.txt"


def observation_curation_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "derived" / "openclaw_observation_curation_prompt.txt"


def matching_authorization_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_authorization.json"


def matching_authorization_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_authorization_draft.json"


def matching_authorization_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_authorization_packet.json"


def matching_authorization_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_matching_authorization_prompt.txt"


def matching_candidate_set_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_candidate_set.json"


def matching_adjudication_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_adjudication.json"


def matching_adjudication_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_adjudication_draft.json"


def matching_adjudication_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_adjudication_packet.json"


def matching_adjudication_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_matching_adjudication_prompt.txt"


def matching_result_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "matching_result.json"


def evidence_adjudication_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_adjudication.json"


def evidence_library_dir(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library"


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


def role_context_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"context_{role}.json"


def report_target_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / f"{role}_report.json"


def report_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_report_draft.json"


def report_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "report_packet.json"


def report_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_report_prompt.txt"


def decision_target_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "council_decision.json"


def decision_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "council_decision_draft.json"


def decision_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "decision_packet.json"


def decision_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_decision_prompt.txt"


def tasks_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "tasks.json"


def fetch_execution_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch_execution.json"


def override_requests_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "override_requests.json"


def load_contract_module() -> Any | None:
    if not CONTRACT_SCRIPT_PATH.exists():
        return None
    module_name = "eco_council_contract_reporting"
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


def contract_call(name: str, *args: Any) -> Any | None:
    if CONTRACT_MODULE is None or not hasattr(CONTRACT_MODULE, name):
        return None
    helper = getattr(CONTRACT_MODULE, name)
    return helper(*args)


def effective_matching_authorization(*, mission: dict[str, Any], round_id: str, authorization: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(authorization, dict):
        return {}
    value = contract_call("apply_matching_authorization_policy", mission, round_id, authorization)
    if isinstance(value, dict):
        return value
    return dict(authorization)


def validate_payload(kind: str, payload: Any) -> None:
    if CONTRACT_MODULE is None:
        return
    result = CONTRACT_MODULE.validate_payload(kind, payload)
    validation = result.get("validation", {})
    if validation.get("ok"):
        return
    issues = []
    for issue in validation.get("issues", [])[:5]:
        issues.append(f"{issue.get('path')}: {issue.get('message')}")
    raise ValueError(f"Generated invalid {kind}: {'; '.join(issues)}")


def validate_bundle(run_dir: Path) -> dict[str, Any] | None:
    if CONTRACT_MODULE is None or not hasattr(CONTRACT_MODULE, "validate_bundle"):
        return None
    return CONTRACT_MODULE.validate_bundle(run_dir)


def parse_round_components(round_id: str) -> tuple[str, int, int] | None:
    match = re.match(r"^(.*?)(\d+)$", round_id)
    if match is None:
        return None
    prefix, digits = match.groups()
    return prefix, int(digits), len(digits)


def next_round_id_for(round_id: str) -> str:
    components = parse_round_components(round_id)
    if components is None:
        return f"{round_id}-next"
    prefix, number, width = components
    return f"{prefix}{number + 1:0{width}d}"


def current_round_number(round_id: str) -> int | None:
    components = parse_round_components(round_id)
    if components is None:
        return None
    return components[1]


def round_sort_key(round_id: str) -> tuple[str, int, str]:
    components = parse_round_components(round_id)
    if components is None:
        return (round_id, 10**9, round_id)
    prefix, number, _width = components
    return (prefix, number, round_id)


def discover_round_ids(run_dir: Path) -> list[str]:
    round_ids: list[str] = []
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        if not child.name.startswith("round_"):
            continue
        round_ids.append(child.name.replace("_", "-"))
    return sorted(unique_strings(round_ids), key=round_sort_key)


def round_ids_through(run_dir: Path, round_id: str) -> list[str]:
    current = parse_round_components(round_id)
    if current is None:
        return [item for item in discover_round_ids(run_dir) if item <= round_id]
    prefix, number, _width = current
    selected: list[str] = []
    for item in discover_round_ids(run_dir):
        components = parse_round_components(item)
        if components is None:
            continue
        item_prefix, item_number, _item_width = components
        if item_prefix == prefix and item_number <= number:
            selected.append(item)
    return selected


def observation_signature_payload(observation: dict[str, Any]) -> dict[str, Any]:
    provenance = observation.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    return {
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": observation.get("statistics"),
        "time_window": observation.get("time_window"),
        "place_scope": observation.get("place_scope"),
        "source_skills": sorted(maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)),
        "metric_bundle": sorted(maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)),
        "candidate_observation_ids": sorted(
            maybe_text(item) for item in observation.get("candidate_observation_ids", []) if maybe_text(item)
        ),
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


def observation_submission_id(observation_id: str) -> str:
    return f"obssub-{maybe_text(observation_id)}"


def effective_shared_observations(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    merged_by_signature: dict[str, dict[str, Any]] = {}
    ordered_signatures: list[str] = []
    for observed_round_id in round_ids_through(run_dir, round_id):
        for observation in load_canonical_list(shared_observations_path(run_dir, observed_round_id)):
            signature_payload = observation_signature_payload(observation)
            signature = stable_hash(stable_json(signature_payload))
            if signature not in merged_by_signature:
                ordered_signatures.append(signature)
            merged_by_signature[signature] = materialize_shared_observation(observation)
    return [merged_by_signature[signature] for signature in ordered_signatures]


def effective_shared_claims(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    merged_by_id: dict[str, dict[str, Any]] = {}
    ordered_ids: list[str] = []
    for observed_round_id in round_ids_through(run_dir, round_id):
        for claim in load_canonical_list(shared_claims_path(run_dir, observed_round_id)):
            claim_id = maybe_text(claim.get("claim_id"))
            if not claim_id:
                continue
            if claim_id not in merged_by_id:
                ordered_ids.append(claim_id)
            merged_by_id[claim_id] = dict(claim)
    return [merged_by_id[claim_id] for claim_id in ordered_ids]


def active_library_list(run_dir: Path, round_id: str, path_fn: Any) -> list[dict[str, Any]]:
    current_path = path_fn(run_dir, round_id)
    if current_path.exists():
        current = load_canonical_list(current_path)
        if current:
            return current
    prior_rounds = round_ids_through(run_dir, round_id)
    if prior_rounds and prior_rounds[-1] == round_id:
        prior_rounds = prior_rounds[:-1]
    for observed_round_id in reversed(prior_rounds):
        current = load_canonical_list(path_fn(run_dir, observed_round_id))
        if current:
            return current
    return []


def build_fallback_context(
    *,
    mission: dict[str, Any],
    round_id: str,
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    role: str,
) -> dict[str, Any]:
    role_tasks = [task for task in tasks if role == "moderator" or task.get("assigned_role") == role]
    return {
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region": mission.get("region"),
            "window": mission.get("window"),
            "role": role,
        },
        "dataset": {
            "generated_at_utc": utc_now_iso(),
            "task_count": len(role_tasks),
            "claim_count": len(claims),
            "observation_count": len(observations),
            "evidence_count": len(evidence_cards),
        },
        "aggregates": {
            "claim_type_counts": counter_dict([maybe_text(item.get("claim_type")) for item in claims]),
            "observation_metric_counts": counter_dict([maybe_text(item.get("metric")) for item in observations]),
            "evidence_verdict_counts": counter_dict([maybe_text(item.get("verdict")) for item in evidence_cards]),
        },
        "tasks": role_tasks,
        "focus": {
            "task_ids": [maybe_text(task.get("task_id")) for task in role_tasks],
            "claims_needing_more_evidence": [
                maybe_text(card.get("claim_id"))
                for card in evidence_cards
                if maybe_text(card.get("verdict")) in {"mixed", "insufficient"}
            ],
        },
        "claims": claims,
        "observations": observations,
        "evidence_cards": evidence_cards,
    }


def load_context_or_fallback(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    path = role_context_path(run_dir, round_id, role)
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return build_fallback_context(
        mission=mission,
        round_id=round_id,
        tasks=tasks,
        claims=claims,
        observations=observations,
        evidence_cards=evidence_cards,
        role=role,
    )


def report_is_placeholder(report: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict):
        return False
    return maybe_text(report.get("summary")).lower().startswith("pending ")


def report_has_substance(report: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict):
        return False
    if report_is_placeholder(report):
        return False
    if report.get("findings"):
        return True
    return bool(report.get("open_questions") or report.get("recommended_next_actions"))


def load_report_for_decision(run_dir: Path, round_id: str, role: str, *, prefer_drafts: bool) -> tuple[dict[str, Any] | None, str]:
    final_report = load_json_if_exists(report_target_path(run_dir, round_id, role))
    if not isinstance(final_report, dict):
        final_report = None
    draft_report = load_json_if_exists(report_draft_path(run_dir, round_id, role))
    if not isinstance(draft_report, dict):
        draft_report = None
    if prefer_drafts and draft_report is not None:
        return draft_report, "draft"
    if final_report is not None:
        return final_report, "final"
    if draft_report is not None:
        return draft_report, "draft"
    return None, "missing"


def claim_sort_key(claim: dict[str, Any]) -> tuple[int, str]:
    priority = claim.get("priority")
    if not isinstance(priority, int):
        priority = 99
    return (priority, maybe_text(claim.get("claim_id")))


def evidence_rank(card: dict[str, Any]) -> int:
    verdict = maybe_text(card.get("verdict"))
    if verdict in {"supports", "contradicts"}:
        return 0
    if verdict == "mixed":
        return 1
    return 2


def gap_to_question(gap: str) -> str:
    lowered = maybe_text(gap).lower()
    for needle, question in QUESTION_RULES:
        if needle in lowered:
            return question
    if lowered.endswith("?"):
        return gap
    return f"How should the next round address this gap: {maybe_text(gap)}?"


def expected_output_kinds_for_role(role: str) -> list[str]:
    if role == "sociologist":
        return ["source-selection", "claim-curation", "claim-submission", "data-readiness-report", "expert-report"]
    if role == "environmentalist":
        return ["source-selection", "observation-curation", "observation-submission", "data-readiness-report", "expert-report"]
    if role == "historian":
        return ["expert-report"]
    return ["expert-report"]


def public_source_channel(source_skill: str) -> str:
    text = maybe_text(source_skill)
    return PUBLIC_SOURCE_FAMILIES.get(text, text)


def public_source_channels(claims: list[dict[str, Any]]) -> list[str]:
    channels: list[str] = []
    for claim in claims:
        refs = claim.get("public_refs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if isinstance(ref, dict):
                channels.append(public_source_channel(maybe_text(ref.get("source_skill"))))
    return unique_strings(channels)


def compact_claim(claim: dict[str, Any]) -> dict[str, Any]:
    refs = claim.get("public_refs")
    source_skills = []
    if isinstance(refs, list):
        source_skills = unique_strings(
            [
                maybe_text(ref.get("source_skill"))
                for ref in refs
                if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
            ]
        )
    return {
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": truncate_text(maybe_text(claim.get("summary")), 180),
        "priority": claim.get("priority"),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "public_source_skills": source_skills,
        "candidate_claim_ids": [maybe_text(item) for item in claim.get("candidate_claim_ids", []) if maybe_text(item)][:6],
        "selection_reason": truncate_text(maybe_text(claim.get("selection_reason")), 160),
    }


def compact_observation(observation: dict[str, Any]) -> dict[str, Any]:
    return {
        "observation_id": maybe_text(observation.get("observation_id")),
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "metric_family": observation_metric_family(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": compact_statistics(observation.get("statistics")),
        "time_window": observation.get("time_window"),
        "source_skills": [maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)][:4],
        "metric_bundle": [maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)][:6],
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
        "candidate_claim_ids": [maybe_text(item) for item in submission.get("candidate_claim_ids", []) if maybe_text(item)][:6],
        "selection_reason": truncate_text(maybe_text(submission.get("selection_reason")), 160),
    }


def compact_observation_submission(submission: dict[str, Any]) -> dict[str, Any]:
    return {
        "submission_id": maybe_text(submission.get("submission_id")),
        "observation_id": maybe_text(submission.get("observation_id")),
        "metric": maybe_text(submission.get("metric")),
        "metric_family": observation_metric_family(submission.get("metric")),
        "source_skill": maybe_text(submission.get("source_skill")),
        "aggregation": maybe_text(submission.get("aggregation")),
        "observation_mode": maybe_text(submission.get("observation_mode")),
        "evidence_role": maybe_text(submission.get("evidence_role")),
        "value": submission.get("value"),
        "unit": maybe_text(submission.get("unit")),
        "statistics": compact_statistics(submission.get("statistics")),
        "time_window": submission.get("time_window"),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
        "source_skills": [maybe_text(item) for item in submission.get("source_skills", []) if maybe_text(item)][:4],
        "metric_bundle": [maybe_text(item) for item in submission.get("metric_bundle", []) if maybe_text(item)][:6],
        "candidate_observation_ids": [
            maybe_text(item) for item in submission.get("candidate_observation_ids", []) if maybe_text(item)
        ][:6],
        "selection_reason": truncate_text(maybe_text(submission.get("selection_reason")), 160),
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


def compact_claim_candidate_for_curation(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = compact_claim(candidate)
    payload["statement"] = truncate_text(maybe_text(candidate.get("statement")), 220)
    payload["source_signal_count"] = candidate.get("source_signal_count")
    if isinstance(candidate.get("compact_audit"), dict):
        payload["compact_audit"] = candidate.get("compact_audit")
    return payload


def compact_observation_candidate_for_curation(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = compact_observation(candidate)
    if isinstance(candidate.get("compact_audit"), dict):
        payload["compact_audit"] = candidate.get("compact_audit")
    return payload


def ranked_claim_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        candidates,
        key=lambda item: (
            -to_nonnegative_int(item.get("source_signal_count")),
            maybe_text(item.get("claim_type")),
            maybe_text(item.get("claim_id")),
        ),
    )


def candidate_claim_entry_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "claim_id": maybe_text(candidate.get("claim_id")),
        "candidate_claim_ids": [maybe_text(candidate.get("claim_id"))],
        "claim_type": maybe_text(candidate.get("claim_type")),
        "summary": maybe_text(candidate.get("summary")),
        "statement": maybe_text(candidate.get("statement")),
        "meaning": (
            f"Retain this curated claim because it represents a distinct public narrative about "
            f"{maybe_text(candidate.get('claim_type')) or 'the mission topic'}."
        ),
        "priority": max(1, min(5, int(candidate.get("priority") or 1))),
        "needs_physical_validation": bool(candidate.get("needs_physical_validation")),
        "worth_storing": True,
        "selection_reason": "Carry forward this candidate as a curated claim pending agent review.",
        "time_window": candidate.get("time_window"),
        "place_scope": candidate.get("place_scope"),
    }


def guess_observation_candidate_evidence_role(observation: dict[str, Any], claims: list[dict[str, Any]]) -> str:
    metric = maybe_text(observation.get("metric"))
    claim_types = {
        maybe_text(item.get("claim_type"))
        for item in claims
        if isinstance(item, dict) and maybe_text(item.get("claim_type"))
    }
    if "wildfire" in claim_types:
        if metric == "fire_detection_count":
            return "primary"
        if metric in {"precipitation_sum", "relative_humidity_2m"}:
            return "contradictory"
        if observation_metric_family(metric) == "meteorology":
            return "contextual"
    if claim_types & {"smoke", "air-pollution"}:
        if observation_metric_family(metric) == "air-quality":
            return "primary"
        if metric == "fire_detection_count":
            return "contextual"
    if "flood" in claim_types and (metric in PRECIPITATION_METRICS or metric in HYDROLOGY_METRICS):
        return "primary"
    if "heat" in claim_types and metric == "temperature_2m":
        return "primary"
    if "drought" in claim_types and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
        return "primary"
    return "contextual"


def candidate_observation_entry_from_candidate(candidate: dict[str, Any], claims: list[dict[str, Any]]) -> dict[str, Any]:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else None
    payload: dict[str, Any] = {
        "observation_id": maybe_text(candidate.get("observation_id")),
        "observation_mode": "atomic",
        "candidate_observation_ids": [maybe_text(candidate.get("observation_id"))],
        "metric": maybe_text(candidate.get("metric")),
        "aggregation": maybe_text(candidate.get("aggregation")),
        "value": candidate.get("value"),
        "unit": maybe_text(candidate.get("unit")),
        "meaning": (
            f"Retain this atomic observation as a curation candidate for metric "
            f"{maybe_text(candidate.get('metric')) or 'unknown'}."
        ),
        "worth_storing": True,
        "evidence_role": guess_observation_candidate_evidence_role(candidate, claims),
        "selection_reason": "Carry forward this atomic observation pending agent review and possible composition.",
        "source_skills": [maybe_text(candidate.get("source_skill"))] if maybe_text(candidate.get("source_skill")) else [],
        "metric_bundle": [maybe_text(candidate.get("metric"))] if maybe_text(candidate.get("metric")) else [],
        "time_window": candidate.get("time_window"),
        "place_scope": candidate.get("place_scope"),
        "statistics": candidate.get("statistics"),
        "quality_flags": candidate.get("quality_flags", []),
        "component_roles": [],
    }
    if provenance is not None:
        payload["provenance_refs"] = [provenance]
    return payload


def infer_missing_evidence_types(*, claims: list[dict[str, Any]], observations: list[dict[str, Any]], evidence_cards: list[dict[str, Any]]) -> list[str]:
    observation_metrics = {maybe_text(item.get("metric")) for item in observations}
    has_station_observation = any(maybe_text(item.get("source_skill")) == "openaq-data-fetch" for item in observations)
    cards_by_claim_id = {maybe_text(item.get("claim_id")): item for item in evidence_cards}
    unresolved_claims: list[dict[str, Any]] = []
    for claim in claims:
        claim_id = maybe_text(claim.get("claim_id"))
        card = cards_by_claim_id.get(claim_id)
        if card is None or maybe_text(card.get("verdict")) in {"mixed", "insufficient"}:
            unresolved_claims.append(claim)

    missing: set[str] = set()
    if not claims:
        missing.add("normalized-public-claims")
    if claims and observations and evidence_cards:
        if any(
            maybe_text(card.get("verdict")) in {"mixed", "insufficient"}
            for card in evidence_cards
            if isinstance(card, dict)
        ):
            missing.add("evidence-cards-linking-public-claims-to-physical-observations")

    for card in evidence_cards:
        gaps = card.get("gaps")
        if not isinstance(gaps, list):
            continue
        gap_text = " ".join(maybe_text(item) for item in gaps).lower()
        if "station" in gap_text or "modeled background" in gap_text:
            missing.add("station-air-quality")

    for claim in unresolved_claims:
        claim_id = maybe_text(claim.get("claim_id"))
        claim_type = maybe_text(claim.get("claim_type"))
        card = cards_by_claim_id.get(claim_id)
        gap_text = " ".join(card.get("gaps", [])) if isinstance(card, dict) and isinstance(card.get("gaps"), list) else ""
        lowered_gap_text = gap_text.lower()

        if "station" in lowered_gap_text or "modeled background" in lowered_gap_text:
            missing.add("station-air-quality")

        if claim_type in {"smoke", "air-pollution"} and not has_station_observation:
            missing.add("station-air-quality")

        if claim_type in {"smoke", "wildfire"} and "fire_detection_count" not in observation_metrics:
            if "wildfire" in maybe_text(claim.get("summary")).lower() or claim_type == "wildfire":
                missing.add("fire-detection")

        if claim_type == "wildfire" and not (observation_metrics & METEOROLOGY_METRICS):
            missing.add("meteorology-background")

        if claim_type == "flood" and not (observation_metrics & (PRECIPITATION_METRICS | HYDROLOGY_METRICS)):
            missing.add("precipitation-hydrology")

        if claim_type == "heat" and "temperature_2m" not in observation_metrics:
            missing.add("temperature-extremes")

        if claim_type == "drought" and not {"precipitation_sum", "soil_moisture_0_to_7cm"} <= observation_metrics:
            missing.add("precipitation-soil-moisture")

        if claim_type == "policy-reaction":
            refs = claim.get("public_refs")
            has_reggov = False
            if isinstance(refs, list):
                has_reggov = any(
                    isinstance(ref, dict)
                    and maybe_text(ref.get("source_skill")) in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}
                    for ref in refs
                )
            if not has_reggov:
                missing.add("policy-comment-coverage")

    if unresolved_claims and len(public_source_channels(claims)) < 2:
        if any(maybe_text(claim.get("claim_type")) != "policy-reaction" for claim in unresolved_claims):
            missing.add("public-discussion-coverage")

    return sorted(missing)


def recommendation_template(recommendation: dict[str, Any]) -> dict[str, Any] | None:
    role = maybe_text(recommendation.get("assigned_role"))
    objective = maybe_text(recommendation.get("objective")).casefold()
    if not role or not objective:
        return None
    for template in NEXT_ACTION_LIBRARY.values():
        if maybe_text(template.get("assigned_role")) != role:
            continue
        if maybe_text(template.get("objective")).casefold() == objective:
            return template
    return None


def recommendation_key(recommendation: dict[str, Any]) -> tuple[str, str]:
    return (maybe_text(recommendation.get("assigned_role")), maybe_text(recommendation.get("objective")).lower())


def base_recommendations_from_missing_types(missing_types: list[str]) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    for missing_type in missing_types:
        template = NEXT_ACTION_LIBRARY.get(missing_type)
        if template is None:
            continue
        recommendations.append(
            {
                "assigned_role": template["assigned_role"],
                "objective": template["objective"],
                "reason": template["reason"],
            }
        )
    return recommendations


def combine_recommendations(*, reports: list[dict[str, Any]], missing_types: list[str]) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    for report in reports:
        actions = report.get("recommended_next_actions")
        if not isinstance(actions, list):
            continue
        for action in actions:
            if not isinstance(action, dict):
                continue
            recommendation = {
                "assigned_role": maybe_text(action.get("assigned_role")),
                "objective": maybe_text(action.get("objective")),
                "reason": maybe_text(action.get("reason")),
            }
            if all(recommendation.values()):
                combined.append(recommendation)
    combined.extend(base_recommendations_from_missing_types(missing_types))

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for recommendation in combined:
        key = recommendation_key(recommendation)
        if not key[0] or not key[1]:
            continue
        deduped.setdefault(key, recommendation)
    return list(deduped.values())


def build_task_notes(current_round_id: str, reason: str, evidence_requirement: dict[str, Any]) -> str:
    base = f"Keep the same mission geometry and UTC window. Derived from {current_round_id}."
    requirement_type = maybe_text(evidence_requirement.get("requirement_type"))
    if requirement_type:
        base = f"{base} Evidence requirement: {requirement_type}."
    if maybe_text(reason):
        return f"{base} Reason: {maybe_text(reason)}"
    return base


def build_override_request(
    *,
    mission: dict[str, Any],
    round_id: str,
    agent_role: str,
    origin_kind: str,
    request_id: str,
    target_path: str,
    current_value: Any,
    requested_value: Any,
    summary: str,
    reason: str,
    evidence_refs: list[str],
    anchor_refs: list[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": agent_role,
        "request_origin_kind": origin_kind,
        "target_path": target_path,
        "current_value": current_value,
        "requested_value": requested_value,
        "summary": truncate_text(summary, 240),
        "reason": truncate_text(reason, 500),
        "evidence_refs": unique_strings([maybe_text(item) for item in evidence_refs if maybe_text(item)]),
        "anchor_refs": unique_strings([maybe_text(item) for item in anchor_refs if maybe_text(item)]),
    }
    validate_payload("override-request", payload)
    return payload


def build_next_round_tasks(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    current_round_id: str,
    next_round_id: str,
    recommendations: list[dict[str, Any]],
    focus_claim_ids: list[str],
    anchor_refs: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    run_id = mission_run_id(mission)
    counters: dict[str, int] = defaultdict(int)
    tasks: list[dict[str, Any]] = []
    seen_signatures: set[tuple[str, str]] = set()
    geometry = mission.get("region", {}).get("geometry") if isinstance(mission.get("region"), dict) else None
    window = mission.get("window")
    max_tasks = mission_constraints(mission).get("max_tasks_per_round", 4)
    normalized_focus_claim_ids = focus_claim_ids[:5]
    normalized_anchor_refs = unique_strings(anchor_refs)
    candidate_count = 0

    for recommendation in recommendations:
        role = maybe_text(recommendation.get("assigned_role"))
        if not role:
            continue
        objective = maybe_text(recommendation.get("objective"))
        reason = maybe_text(recommendation.get("reason"))
        template = recommendation_template(recommendation)
        requirement_type = maybe_text(template.get("requirement_type")) if isinstance(template, dict) else ""
        signature = (role, requirement_type or objective.casefold())
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        candidate_count += 1
        counters[role] += 1
        task_id = f"task-{role}-{next_round_id}-{counters[role]:02d}"
        requirement = evidence_requirement_for_recommendation(
            recommendation=recommendation,
            template=template,
            role=role,
            requirement_id=f"req-{role}-{next_round_id}-{counters[role]:02d}-01",
            focus_claim_ids=normalized_focus_claim_ids,
            upstream_round_id=current_round_id,
            anchor_refs=normalized_anchor_refs,
        )
        task = {
            "schema_version": SCHEMA_VERSION,
            "task_id": task_id,
            "run_id": run_id,
            "round_id": next_round_id,
            "assigned_role": role,
            "objective": objective,
            "status": "planned",
            "depends_on": [],
            "expected_output_kinds": expected_output_kinds_for_role(role),
            "inputs": {
                "mission_geometry": geometry,
                "mission_window": window,
                "focus_claim_ids": focus_claim_ids,
                "upstream_round_id": current_round_id,
                "evidence_requirements": [requirement],
            },
            "notes": build_task_notes(current_round_id, reason, requirement),
        }
        validate_payload("round-task", task)
        tasks.append(task)
        if len(tasks) >= max_tasks:
            break
    return tasks, {
        "max_tasks_per_round": max_tasks,
        "candidate_count": candidate_count,
        "returned_count": len(tasks),
        "truncated_by_cap": candidate_count > len(tasks),
    }


def build_decision_override_requests(
    *,
    mission: dict[str, Any],
    round_id: str,
    next_round_id: str,
    focus_claim_ids: list[str],
    anchor_refs: list[str],
    task_plan_info: dict[str, Any],
    next_round_requested_but_blocked_by_max_rounds: bool,
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    if bool(task_plan_info.get("truncated_by_cap")):
        current_cap = int(task_plan_info.get("max_tasks_per_round") or 0)
        requested_cap = max(current_cap + 1, int(task_plan_info.get("candidate_count") or current_cap))
        requests.append(
            build_override_request(
                mission=mission,
                round_id=round_id,
                agent_role="moderator",
                origin_kind="council-decision",
                request_id=f"override-moderator-{round_id}-max-tasks",
                target_path="constraints.max_tasks_per_round",
                current_value=current_cap,
                requested_value=requested_cap,
                summary="Request a higher next-round task cap.",
                reason=(
                    f"The current max_tasks_per_round={current_cap} truncates materially distinct follow-up tasks "
                    f"needed for {next_round_id}."
                ),
                evidence_refs=focus_claim_ids,
                anchor_refs=anchor_refs,
            )
        )
    if next_round_requested_but_blocked_by_max_rounds:
        current_round_cap = mission_constraints(mission).get("max_rounds")
        next_round_number = current_round_number(next_round_id)
        requested_round_cap = max(int(current_round_cap or 0) + 1, int(next_round_number or 0))
        requests.append(
            build_override_request(
                mission=mission,
                round_id=round_id,
                agent_role="moderator",
                origin_kind="council-decision",
                request_id=f"override-moderator-{round_id}-max-rounds",
                target_path="constraints.max_rounds",
                current_value=current_round_cap,
                requested_value=requested_round_cap,
                summary="Request one additional round inside the mission envelope.",
                reason=(
                    f"The current max_rounds={current_round_cap} blocks {next_round_id}, but unresolved evidence still requires another round."
                ),
                evidence_refs=focus_claim_ids,
                anchor_refs=anchor_refs,
            )
        )
    return requests


def observations_by_id_map(observations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("observation_id")): item for item in observations}


def claims_by_id_map(claims: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("claim_id")): item for item in claims}


def evidence_by_claim_map(evidence_cards: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("claim_id")): item for item in evidence_cards}


def metrics_for_evidence(card: dict[str, Any], observations_by_id: dict[str, dict[str, Any]]) -> list[str]:
    metrics: list[str] = []
    observation_ids = card.get("observation_ids")
    if not isinstance(observation_ids, list):
        return metrics
    for observation_id in observation_ids:
        observation = observations_by_id.get(maybe_text(observation_id))
        if observation is not None:
            metrics.append(maybe_text(observation.get("metric")))
    return unique_strings(metrics)


def report_status_for_role(*, role: str, claims: list[dict[str, Any]], observations: list[dict[str, Any]], evidence_cards: list[dict[str, Any]]) -> str:
    if role == "sociologist":
        if not claims:
            return "blocked"
        if not evidence_cards or any(maybe_text(card.get("verdict")) in {"mixed", "insufficient"} for card in evidence_cards):
            return "needs-more-evidence"
        return "complete"
    if not observations and not evidence_cards:
        return "blocked"
    if not evidence_cards or any(maybe_text(card.get("verdict")) in {"mixed", "insufficient"} for card in evidence_cards):
        return "needs-more-evidence"
    return "complete"


def build_summary_for_role(*, role: str, claims: list[dict[str, Any]], observations: list[dict[str, Any]], evidence_cards: list[dict[str, Any]]) -> str:
    verdict_counts = counter_dict([maybe_text(item.get("verdict")) for item in evidence_cards])
    if role == "sociologist":
        if not claims:
            return "No normalized public claims were available for this round."
        return (
            f"The round produced {len(claims)} candidate public claims. "
            f"Evidence verdicts currently include {verdict_counts.get('supports', 0)} supports, "
            f"{verdict_counts.get('contradicts', 0)} contradicts, "
            f"{verdict_counts.get('mixed', 0)} mixed, and "
            f"{verdict_counts.get('insufficient', 0)} insufficient."
        )
    if not observations and not evidence_cards:
        return "No mission-aligned physical observations were available for this round."
    metric_counts = counter_dict([maybe_text(item.get("metric")) for item in observations])
    metric_text = ", ".join(sorted(metric_counts)) if metric_counts else "no linked metrics"
    return (
        f"The round produced {len(observations)} observations and {len(evidence_cards)} evidence cards. "
        f"Current physical coverage includes {metric_text}."
    )


def build_sociologist_findings(
    *,
    claims: list[dict[str, Any]],
    evidence_by_claim: dict[str, dict[str, Any]],
    observations_by_id: dict[str, dict[str, Any]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, claim in enumerate(sorted(claims, key=claim_sort_key)[:max_findings], start=1):
        claim_id = maybe_text(claim.get("claim_id"))
        card = evidence_by_claim.get(claim_id)
        title = truncate_text(maybe_text(claim.get("summary")) or maybe_text(claim.get("statement")), 72)
        if card is None:
            summary = f"Claim {claim_id} was captured from public signals but has not yet been linked to physical evidence."
            confidence = "low"
            observation_ids: list[str] = []
            evidence_ids: list[str] = []
        else:
            metrics = metrics_for_evidence(card, observations_by_id)
            metric_text = f" Linked metrics: {', '.join(metrics[:4])}." if metrics else ""
            summary = f"Claim {claim_id} is currently {maybe_text(card.get('verdict'))}. {maybe_text(card.get('summary'))}{metric_text}".strip()
            confidence = maybe_text(card.get("confidence")) or "low"
            observation_ids = [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)]
            evidence_ids = [maybe_text(card.get("evidence_id"))] if maybe_text(card.get("evidence_id")) else []
        findings.append(
            {
                "finding_id": f"finding-{index:03d}",
                "title": title or f"Claim {claim_id}",
                "summary": truncate_text(summary, 300),
                "confidence": confidence,
                "claim_ids": [claim_id],
                "observation_ids": observation_ids[:6],
                "evidence_ids": evidence_ids,
            }
        )
    return findings


def build_environmentalist_findings(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    observations_by_id: dict[str, dict[str, Any]],
    claims_by_id: dict[str, dict[str, Any]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    ordered_cards = sorted(
        evidence_cards,
        key=lambda item: (
            evidence_rank(item),
            claim_sort_key(claims_by_id.get(maybe_text(item.get("claim_id")), {})),
            maybe_text(item.get("evidence_id")),
        ),
    )
    for index, card in enumerate(ordered_cards[:max_findings], start=1):
        claim_id = maybe_text(card.get("claim_id"))
        claim = claims_by_id.get(claim_id, {})
        metrics = metrics_for_evidence(card, observations_by_id)
        metric_text = ", ".join(metrics[:4]) if metrics else "linked observations"
        findings.append(
            {
                "finding_id": f"finding-{index:03d}",
                "title": truncate_text(maybe_text(claim.get("summary")) or f"Physical evidence for {claim_id}", 72),
                "summary": truncate_text(f"{maybe_text(card.get('summary'))} Main metrics: {metric_text}.", 300),
                "confidence": maybe_text(card.get("confidence")) or "low",
                "claim_ids": [claim_id] if claim_id else [],
                "observation_ids": [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)][:8],
                "evidence_ids": [maybe_text(card.get("evidence_id"))] if maybe_text(card.get("evidence_id")) else [],
            }
        )

    if findings:
        return findings

    for index, observation in enumerate(observations[:max_findings], start=1):
        findings.append(
            {
                "finding_id": f"finding-{index:03d}",
                "title": truncate_text(f"{maybe_text(observation.get('metric'))} observation", 72),
                "summary": truncate_text(
                    (
                        f"Observation {maybe_text(observation.get('observation_id'))} reports "
                        f"{maybe_text(observation.get('metric'))}={observation.get('value')} "
                        f"{maybe_text(observation.get('unit'))} from {maybe_text(observation.get('source_skill'))}."
                    ),
                    300,
                ),
                "confidence": "medium",
                "claim_ids": [],
                "observation_ids": [maybe_text(observation.get("observation_id"))] if maybe_text(observation.get("observation_id")) else [],
                "evidence_ids": [],
            }
        )
    return findings


def build_open_questions(evidence_cards: list[dict[str, Any]]) -> list[str]:
    questions: list[str] = []
    for card in evidence_cards:
        items = card.get("gaps")
        if not isinstance(items, list):
            continue
        for item in items:
            questions.append(gap_to_question(maybe_text(item)))
    return unique_strings(questions)[:5]


def build_report_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    role: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    max_findings: int,
) -> dict[str, Any]:
    evidence_by_claim = evidence_by_claim_map(evidence_cards)
    observations_by_id = observations_by_id_map(observations)
    claims_by_id = claims_by_id_map(claims)
    if role == "sociologist":
        findings = build_sociologist_findings(
            claims=claims,
            evidence_by_claim=evidence_by_claim,
            observations_by_id=observations_by_id,
            max_findings=max_findings,
        )
    else:
        findings = build_environmentalist_findings(
            claims=claims,
            observations=observations,
            evidence_cards=evidence_cards,
            observations_by_id=observations_by_id,
            claims_by_id=claims_by_id,
            max_findings=max_findings,
        )
    missing_types = infer_missing_evidence_types(claims=claims, observations=observations, evidence_cards=evidence_cards)
    recommendations = combine_recommendations(reports=[], missing_types=missing_types)[:4]
    open_questions = build_open_questions(evidence_cards)
    status = report_status_for_role(role=role, claims=claims, observations=observations, evidence_cards=evidence_cards)
    if status == "blocked" and not open_questions:
        if role == "sociologist":
            open_questions = ["Should the next round expand public-signal collection before report writing?"]
        else:
            open_questions = ["Should the next round expand physical-source coverage before physical validation resumes?"]
    draft = {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": role,
        "status": status,
        "summary": build_summary_for_role(role=role, claims=claims, observations=observations, evidence_cards=evidence_cards),
        "findings": findings,
        "open_questions": open_questions,
        "recommended_next_actions": recommendations,
        "override_requests": [],
    }
    validate_payload("expert-report", draft)
    return draft


def build_report_instructions(role: str) -> list[str]:
    instructions = [
        "Return one JSON object only, shaped like expert-report.",
        "Treat `context` as a compact summary layer first; only rely on `canonical_paths` when the summary is insufficient.",
        "Use only claim_ids, observation_ids, and evidence_ids already present in the packet context.",
        "Do not invent coordinates, timestamps, or raw-source facts outside the packet.",
        "If evidence remains partial or mixed, keep status as needs-more-evidence.",
        "Keep each finding traceable to specific canonical objects.",
        "If you include recommended_next_actions, each item must be an object with assigned_role, objective, and reason.",
        "If policy caps or source-governance boundaries are insufficient, keep work inside the current envelope and use override_requests instead of self-applying mission changes.",
    ]
    if role == "sociologist":
        instructions.append("Emphasize claim phrasing, public narrative concentration, and what still needs corroboration.")
    else:
        instructions.append("Emphasize metric interpretation, provenance limits, and what is or is not physically supported.")
    return instructions


def build_report_packet(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    draft_report: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == role]
    existing_report = load_json_if_exists(report_target_path(run_dir, round_id, role))
    if not isinstance(existing_report, dict):
        existing_report = None
    return {
        "packet_kind": "expert-report-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": role,
        },
        "role": role,
        "task_scope": relevant_tasks,
        "context": context,
        "instructions": build_report_instructions(role),
        "existing_override_requests": load_override_requests(run_dir, round_id, role),
        "validation": {
            "kind": "expert-report",
            "target_report_path": str(report_target_path(run_dir, round_id, role)),
            "draft_report_path": str(report_draft_path(run_dir, round_id, role)),
            "validate_command": f"python3 {CONTRACT_SCRIPT_PATH} validate --kind expert-report --input {report_draft_path(run_dir, round_id, role)}",
        },
        "existing_report": existing_report,
        "draft_report": draft_report,
    }


def to_nonnegative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def maybe_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = maybe_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def compact_statistics(value: Any) -> dict[str, float | None] | None:
    if not isinstance(value, dict):
        return None
    compacted: dict[str, float | None] = {}
    for key in ("min", "max", "mean", "p95"):
        number = maybe_number(value.get(key))
        compacted[key] = round(number, 3) if number is not None else None
    if all(number is None for number in compacted.values()):
        return None
    return compacted


def observation_metric_family(metric: Any) -> str:
    normalized = maybe_text(metric)
    for family, metrics in METRIC_FAMILY_GROUPS.items():
        if normalized in metrics:
            return family
    return "other"


def environment_family_priority_order(claims: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    claim_types = {
        maybe_text(item.get("claim_type"))
        for item in claims
        if isinstance(item, dict) and bool(item.get("needs_physical_validation"))
    }
    for claim_type in sorted(claim_types):
        if claim_type in {"smoke", "air-pollution"}:
            ordered.extend(["air-quality", "fire-detection", "meteorology"])
        elif claim_type == "wildfire":
            ordered.extend(["fire-detection", "meteorology", "air-quality"])
        elif claim_type == "flood":
            ordered.extend(["hydrology", "meteorology"])
        elif claim_type == "heat":
            ordered.extend(["meteorology"])
        elif claim_type == "drought":
            ordered.extend(["soil", "meteorology"])
    ordered.extend(DEFAULT_ENVIRONMENT_FAMILY_ORDER)
    return unique_strings(ordered)


def load_dict_if_exists(path: Path) -> dict[str, Any]:
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return {}


def load_override_requests(run_dir: Path, round_id: str, role: str | None = None) -> list[dict[str, Any]]:
    roles = (role,) if role else ("moderator", "sociologist", "environmentalist", "historian")
    output: list[dict[str, Any]] = []
    for role_name in roles:
        payload = load_json_if_exists(override_requests_path(run_dir, round_id, role_name))
        if not isinstance(payload, list):
            continue
        output.extend(item for item in payload if isinstance(item, dict))
    output.sort(key=lambda item: maybe_text(item.get("request_id")))
    return output


def matching_executed_for_state(state: dict[str, Any]) -> bool:
    return bool(
        state.get("matching_adjudication")
        or state.get("matching_result")
        or state.get("evidence_adjudication")
        or state.get("cards_active")
        or state.get("isolated_active")
        or state.get("remands_open")
    )


def compact_recommendation(action: Any) -> dict[str, Any] | None:
    if not isinstance(action, dict):
        return None
    assigned_role = maybe_text(action.get("assigned_role"))
    objective = maybe_text(action.get("objective"))
    reason = maybe_text(action.get("reason"))
    if not assigned_role or not objective or not reason:
        return None
    return {
        "assigned_role": assigned_role,
        "objective": objective,
        "reason": reason,
    }


def compact_matching_authorization_summary(authorization: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(authorization, dict):
        return {}
    return {
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "authorization_status": maybe_text(authorization.get("authorization_status")),
        "authorization_basis": maybe_text(authorization.get("authorization_basis")),
        "summary": maybe_text(authorization.get("summary")),
        "rationale": maybe_text(authorization.get("rationale")),
        "allow_isolated_evidence": bool(authorization.get("allow_isolated_evidence")),
        "claim_count": len(authorization.get("claim_ids", [])) if isinstance(authorization.get("claim_ids"), list) else 0,
        "observation_count": len(authorization.get("observation_ids", [])) if isinstance(authorization.get("observation_ids"), list) else 0,
        "open_questions": [maybe_text(item) for item in authorization.get("open_questions", []) if maybe_text(item)][:6],
    }


def compact_matching_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    matched_pairs = result.get("matched_pairs", [])
    return {
        "result_id": maybe_text(result.get("result_id")),
        "result_status": maybe_text(result.get("result_status")),
        "summary": maybe_text(result.get("summary")),
        "matched_pair_count": len(matched_pairs) if isinstance(matched_pairs, list) else 0,
        "matched_claim_count": len(result.get("matched_claim_ids", [])) if isinstance(result.get("matched_claim_ids"), list) else 0,
        "matched_observation_count": (
            len(result.get("matched_observation_ids", []))
            if isinstance(result.get("matched_observation_ids"), list)
            else 0
        ),
        "unmatched_claim_ids": [maybe_text(item) for item in result.get("unmatched_claim_ids", []) if maybe_text(item)][:8],
        "unmatched_observation_ids": [
            maybe_text(item)
            for item in result.get("unmatched_observation_ids", [])
            if maybe_text(item)
        ][:8],
    }


def compact_matching_adjudication_summary(adjudication: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(adjudication, dict):
        return {}
    recommendations = [
        compact_recommendation(item)
        for item in adjudication.get("recommended_next_actions", [])
        if isinstance(adjudication.get("recommended_next_actions"), list)
    ]
    return {
        "adjudication_id": maybe_text(adjudication.get("adjudication_id")),
        "candidate_set_id": maybe_text(adjudication.get("candidate_set_id")),
        "summary": maybe_text(adjudication.get("summary")),
        "rationale": maybe_text(adjudication.get("rationale")),
        "open_questions": [maybe_text(item) for item in adjudication.get("open_questions", []) if maybe_text(item)][:6],
        "recommended_next_actions": [item for item in recommendations if item][:4],
    }


def compact_evidence_adjudication_summary(adjudication: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(adjudication, dict):
        return {}
    recommendations = [
        compact_recommendation(item)
        for item in adjudication.get("recommended_next_actions", [])
        if isinstance(adjudication.get("recommended_next_actions"), list)
    ]
    return {
        "adjudication_id": maybe_text(adjudication.get("adjudication_id")),
        "adjudication_status": maybe_text(adjudication.get("adjudication_status")),
        "summary": maybe_text(adjudication.get("summary")),
        "matching_reasonable": bool(adjudication.get("matching_reasonable")),
        "needs_additional_data": bool(adjudication.get("needs_additional_data")),
        "open_questions": [maybe_text(item) for item in adjudication.get("open_questions", []) if maybe_text(item)][:6],
        "recommended_next_actions": [item for item in recommendations if item][:4],
    }


def augment_context_with_matching_state(*, run_dir: Path, state: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(context)
    round_id = state["round_id"]
    canonical_paths = merged.get("canonical_paths")
    if not isinstance(canonical_paths, dict):
        canonical_paths = {}
    canonical_paths.update(
        {
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_adjudication": str(matching_adjudication_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
        }
    )
    merged["canonical_paths"] = canonical_paths
    phase_state = merged.get("phase_state")
    if not isinstance(phase_state, dict):
        phase_state = {}
    phase_state.update(phase_state_from_round_state(state))
    merged["phase_state"] = phase_state
    merged["matching"] = {
        "authorization": compact_matching_authorization_summary(
            state.get("matching_authorization", {}) if isinstance(state.get("matching_authorization"), dict) else {}
        ),
        "result": compact_matching_result_summary(
            state.get("matching_result", {}) if isinstance(state.get("matching_result"), dict) else {}
        ),
        "adjudication": compact_matching_adjudication_summary(
            state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
        ),
        "evidence_adjudication": compact_evidence_adjudication_summary(
            state.get("evidence_adjudication", {}) if isinstance(state.get("evidence_adjudication"), dict) else {}
        ),
    }
    return merged


def phase_state_from_round_state(state: dict[str, Any]) -> dict[str, Any]:
    readiness_reports = state.get("readiness_reports", {}) if isinstance(state.get("readiness_reports"), dict) else {}
    readiness_statuses = {
        role: maybe_text(report.get("readiness_status"))
        for role, report in readiness_reports.items()
        if isinstance(report, dict) and report
    }
    claim_curation = state.get("claim_curation") if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = state.get("observation_curation") if isinstance(state.get("observation_curation"), dict) else {}
    authorization = state.get("matching_authorization", {}) if isinstance(state.get("matching_authorization"), dict) else {}
    moderator_adjudication = state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
    result = state.get("matching_result", {}) if isinstance(state.get("matching_result"), dict) else {}
    adjudication = state.get("evidence_adjudication", {}) if isinstance(state.get("evidence_adjudication"), dict) else {}
    return {
        "claim_curation_status": maybe_text(claim_curation.get("status")),
        "observation_curation_status": maybe_text(observation_curation.get("status")),
        "readiness_statuses": readiness_statuses,
        "readiness_received_roles": sorted(readiness_statuses),
        "matching_authorization_status": maybe_text(authorization.get("authorization_status")),
        "matching_authorization_basis": maybe_text(authorization.get("authorization_basis")),
        "matching_adjudication_id": maybe_text(moderator_adjudication.get("adjudication_id")),
        "matching_candidate_set_id": maybe_text(moderator_adjudication.get("candidate_set_id")),
        "matching_result_status": maybe_text(result.get("result_status")),
        "adjudication_status": maybe_text(adjudication.get("adjudication_status")),
        "matching_executed": matching_executed_for_state(state),
    }


def collect_round_state(run_dir: Path, round_id: str) -> dict[str, Any]:
    mission = load_mission(run_dir)
    observations = effective_shared_observations(run_dir, round_id)
    claims = effective_shared_claims(run_dir, round_id)
    claim_submissions_current = load_canonical_list(claim_submissions_path(run_dir, round_id))
    observation_submissions_current = hydrate_observation_submissions_with_observations(
        load_canonical_list(observation_submissions_path(run_dir, round_id)),
        observations,
    )
    claims_active = active_library_list(run_dir, round_id, claims_active_path)
    observations_active = hydrate_observation_submissions_with_observations(
        active_library_list(run_dir, round_id, observations_active_path),
        observations,
    )
    matching_authorization = effective_matching_authorization(
        mission=mission,
        round_id=round_id,
        authorization=load_dict_if_exists(matching_authorization_path(run_dir, round_id)),
    )
    state = {
        "mission": mission,
        "round_id": round_id,
        "tasks": load_canonical_list(tasks_path(run_dir, round_id)),
        "claims": claims,
        "observations": observations,
        "claim_candidates_current": load_canonical_list(claim_candidates_path(run_dir, round_id)),
        "observation_candidates_current": load_canonical_list(observation_candidates_path(run_dir, round_id)),
        "claim_curation": load_dict_if_exists(claim_curation_path(run_dir, round_id)),
        "observation_curation": load_dict_if_exists(observation_curation_path(run_dir, round_id)),
        "evidence_cards": load_canonical_list(shared_evidence_path(run_dir, round_id)),
        "claim_submissions_current": claim_submissions_current,
        "observation_submissions_current": observation_submissions_current,
        "claim_submissions_auditable": claims_active or claim_submissions_current,
        "observation_submissions_auditable": observations_active or observation_submissions_current,
        "claims_active": claims_active,
        "observations_active": observations_active,
        "cards_active": active_library_list(run_dir, round_id, cards_active_path),
        "isolated_active": active_library_list(run_dir, round_id, isolated_active_path),
        "remands_open": active_library_list(run_dir, round_id, remands_open_path),
        "readiness_reports": {
            "sociologist": load_dict_if_exists(data_readiness_report_path(run_dir, round_id, "sociologist")),
            "environmentalist": load_dict_if_exists(data_readiness_report_path(run_dir, round_id, "environmentalist")),
        },
        "matching_authorization": matching_authorization,
        "matching_adjudication": load_dict_if_exists(matching_adjudication_path(run_dir, round_id)),
        "matching_result": load_dict_if_exists(matching_result_path(run_dir, round_id)),
        "evidence_adjudication": load_dict_if_exists(evidence_adjudication_path(run_dir, round_id)),
    }
    state["phase_state"] = phase_state_from_round_state(state)
    return state


def state_current_submissions(state: dict[str, Any], role: str) -> list[dict[str, Any]]:
    if role == "sociologist":
        current = state.get("claim_submissions_current", [])
    else:
        current = state.get("observation_submissions_current", [])
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def state_auditable_submissions(state: dict[str, Any], role: str) -> list[dict[str, Any]]:
    if role == "sociologist":
        auditable = state.get("claim_submissions_auditable", [])
        current = state.get("claim_submissions_current", [])
    else:
        auditable = state.get("observation_submissions_auditable", [])
        current = state.get("observation_submissions_current", [])
    if isinstance(auditable, list) and auditable:
        return [item for item in auditable if isinstance(item, dict)]
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def observation_match_key(item: dict[str, Any]) -> str:
    provenance = item.get("provenance") if isinstance(item.get("provenance"), dict) else {}
    payload = {
        "source_skill": maybe_text(item.get("source_skill")),
        "metric": maybe_text(item.get("metric")),
        "aggregation": maybe_text(item.get("aggregation")),
        "observation_mode": maybe_text(item.get("observation_mode")),
        "evidence_role": maybe_text(item.get("evidence_role")),
        "value": item.get("value"),
        "unit": maybe_text(item.get("unit")),
        "time_window": item.get("time_window"),
        "place_scope": item.get("place_scope"),
        "source_skills": sorted(maybe_text(value) for value in item.get("source_skills", []) if maybe_text(value)),
        "metric_bundle": sorted(maybe_text(value) for value in item.get("metric_bundle", []) if maybe_text(value)),
        "candidate_observation_ids": sorted(
            maybe_text(value) for value in item.get("candidate_observation_ids", []) if maybe_text(value)
        ),
        "quality_flags": sorted(maybe_text(flag) for flag in item.get("quality_flags", []) if maybe_text(flag)),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }
    return stable_hash(stable_json(payload))


def hydrate_observation_submissions_with_observations(
    submissions: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observations_by_id = {
        maybe_text(item.get("observation_id")): item
        for item in observations
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    observations_by_key = {
        observation_match_key(item): item
        for item in observations
        if isinstance(item, dict)
    }
    hydrated: list[dict[str, Any]] = []
    for submission in submissions:
        if not isinstance(submission, dict):
            continue
        item = dict(submission)
        observation = observations_by_id.get(maybe_text(item.get("observation_id")))
        if observation is None:
            observation = observations_by_key.get(observation_match_key(item))
        if observation is not None:
            canonical_observation_id = maybe_text(observation.get("observation_id"))
            if canonical_observation_id and maybe_text(item.get("observation_id")) != canonical_observation_id:
                item["observation_id"] = canonical_observation_id
                item["submission_id"] = observation_submission_id(canonical_observation_id)
            if not isinstance(item.get("statistics"), dict) and isinstance(observation.get("statistics"), dict):
                item["statistics"] = observation.get("statistics")
            if not isinstance(item.get("time_window"), dict) and isinstance(observation.get("time_window"), dict):
                item["time_window"] = observation.get("time_window")
            if not maybe_text(item.get("unit")) and maybe_text(observation.get("unit")):
                item["unit"] = maybe_text(observation.get("unit"))
        hydrated.append(item)
    return hydrated


def state_submissions(state: dict[str, Any], role: str) -> list[dict[str, Any]]:
    submissions = state_auditable_submissions(state, role)
    if submissions:
        return submissions
    if role == "sociologist":
        active = state.get("claims_active", [])
    else:
        active = state.get("observations_active", [])
    if isinstance(active, list):
        return [item for item in active if isinstance(item, dict)]
    return []


def claim_submission_source_skills(submission: dict[str, Any]) -> list[str]:
    refs = submission.get("public_refs")
    if not isinstance(refs, list):
        return []
    return unique_strings(
        [
            maybe_text(ref.get("source_skill"))
            for ref in refs
            if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
        ]
    )


def claim_submission_channels_for_submission(submission: dict[str, Any]) -> list[str]:
    return unique_strings([public_source_channel(source_skill) for source_skill in claim_submission_source_skills(submission)])


def claim_submission_channels(submissions: list[dict[str, Any]]) -> list[str]:
    channels: list[str] = []
    for submission in submissions:
        channels.extend(claim_submission_channels_for_submission(submission))
    return unique_strings(channels)


def select_public_submissions(submissions: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    ordered = sorted(
        submissions,
        key=lambda item: (
            -to_nonnegative_int(item.get("source_signal_count")),
            maybe_text(item.get("claim_type")),
            maybe_text(item.get("submission_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        if len(selected) >= limit:
            return
        for candidate in ordered:
            submission_id = maybe_text(candidate.get("submission_id"))
            if not submission_id or submission_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(submission_id)
            return

    for channel in claim_submission_channels(ordered):
        take_first(lambda item, channel=channel: channel in claim_submission_channels_for_submission(item))
    for source_skill in unique_strings([skill for item in ordered for skill in claim_submission_source_skills(item)]):
        take_first(lambda item, source_skill=source_skill: source_skill in claim_submission_source_skills(item))
    for claim_type in unique_strings([maybe_text(item.get("claim_type")) for item in ordered]):
        take_first(lambda item, claim_type=claim_type: maybe_text(item.get("claim_type")) == claim_type)
    for candidate in ordered:
        submission_id = maybe_text(candidate.get("submission_id"))
        if len(selected) >= limit:
            break
        if not submission_id or submission_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(submission_id)
    return selected


def observation_submission_severity(submission: dict[str, Any]) -> float:
    statistics_obj = compact_statistics(submission.get("statistics"))
    if isinstance(statistics_obj, dict):
        for key in ("max", "p95", "mean", "min"):
            value = maybe_number(statistics_obj.get(key))
            if value is not None:
                return value
    value = maybe_number(submission.get("value"))
    return value if value is not None else 0.0


def representative_observation_order(observations: list[dict[str, Any]], claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_order = environment_family_priority_order(claims)
    ordered = sorted(
        observations,
        key=lambda item: (
            family_order.index(observation_metric_family(item.get("metric")))
            if observation_metric_family(item.get("metric")) in family_order
            else len(family_order),
            -observation_submission_severity(item),
            maybe_text(item.get("source_skill")),
            maybe_text(item.get("metric")),
            maybe_text(item.get("observation_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        for candidate in ordered:
            observation_id = maybe_text(candidate.get("observation_id"))
            if not observation_id or observation_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(observation_id)
            return

    for family in family_order:
        take_first(lambda item, family=family: observation_metric_family(item.get("metric")) == family)
    for source_skill in unique_strings([maybe_text(item.get("source_skill")) for item in ordered]):
        take_first(lambda item, source_skill=source_skill: maybe_text(item.get("source_skill")) == source_skill)
    for metric in unique_strings([maybe_text(item.get("metric")) for item in ordered]):
        take_first(lambda item, metric=metric: maybe_text(item.get("metric")) == metric)
    for candidate in ordered:
        observation_id = maybe_text(candidate.get("observation_id"))
        if not observation_id or observation_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(observation_id)
    return selected


def select_environment_submissions(submissions: list[dict[str, Any]], claims: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    family_order = environment_family_priority_order(claims)
    ordered = sorted(
        submissions,
        key=lambda item: (
            family_order.index(observation_metric_family(item.get("metric")))
            if observation_metric_family(item.get("metric")) in family_order
            else len(family_order),
            -observation_submission_severity(item),
            maybe_text(item.get("source_skill")),
            maybe_text(item.get("metric")),
            maybe_text(item.get("submission_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        if len(selected) >= limit:
            return
        for candidate in ordered:
            submission_id = maybe_text(candidate.get("submission_id"))
            if not submission_id or submission_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(submission_id)
            return

    for family in family_order:
        take_first(lambda item, family=family: observation_metric_family(item.get("metric")) == family)
    for source_skill in unique_strings([maybe_text(item.get("source_skill")) for item in ordered]):
        take_first(lambda item, source_skill=source_skill: maybe_text(item.get("source_skill")) == source_skill)
    for metric in unique_strings([maybe_text(item.get("metric")) for item in ordered]):
        take_first(lambda item, metric=metric: maybe_text(item.get("metric")) == metric)
    for candidate in ordered:
        submission_id = maybe_text(candidate.get("submission_id"))
        if len(selected) >= limit:
            break
        if not submission_id or submission_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(submission_id)
    return selected


def representative_submissions(state: dict[str, Any], role: str, submissions: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if role == "sociologist":
        return select_public_submissions(submissions, limit)
    return select_environment_submissions(submissions, state.get("claims", []), limit)


def aggregate_compact_audit(
    state: dict[str, Any],
    role: str,
    submissions: list[dict[str, Any]],
    *,
    fallback_summary: str,
    retained_limit: int,
) -> dict[str, Any]:
    selected = representative_submissions(state, role, submissions, retained_limit)
    coverage_dimensions: list[str] = []
    missing_dimensions: list[str] = []
    concentration_flags: list[str] = []
    sampling_notes: list[str] = []

    if role == "sociologist":
        full_channels = claim_submission_channels(submissions)
        full_sources = unique_strings([skill for item in submissions for skill in claim_submission_source_skills(item)])
        selected_channels = claim_submission_channels(selected)
        selected_sources = unique_strings([skill for item in selected for skill in claim_submission_source_skills(item)])
        channel_counts = Counter(channel for item in submissions for channel in claim_submission_channels_for_submission(item))
        source_counts = Counter(skill for item in submissions for skill in claim_submission_source_skills(item))
        coverage_dimensions = ["supporting-artifacts", "channel", "source-skill", "claim-type"]
        if len(full_channels) > 1 and len(selected_channels) < min(len(full_channels), max(1, retained_limit)):
            missing_dimensions.append("channel-coverage")
        if len(full_sources) > 1 and len(selected_sources) < min(len(full_sources), max(1, retained_limit)):
            missing_dimensions.append("source-skill-coverage")
        if submissions and all(to_nonnegative_int(item.get("source_signal_count")) <= 1 for item in submissions):
            missing_dimensions.append("multi-signal-corroboration")
        top_channel = channel_counts.most_common(1)[0] if channel_counts else None
        top_source = source_counts.most_common(1)[0] if source_counts else None
        if top_channel is not None and len(submissions) >= 4 and top_channel[1] / len(submissions) >= 0.8:
            concentration_flags.append(f"Auditable public submissions remain highly concentrated in the {top_channel[0]} channel.")
        if top_source is not None and len(submissions) >= 4 and top_source[1] / len(submissions) >= 0.8:
            concentration_flags.append(f"Auditable public submissions remain highly concentrated in {top_source[0]}.")
        coverage_summary = (
            f"Selected {len(selected)} auditable claim submissions from {len(submissions)} total while covering "
            f"{len(selected_channels)}/{len(full_channels) or 1} channels and {len(selected_sources)}/{len(full_sources) or 1} source skills."
        )
        sampling_notes.extend(
            [
                f"Dominant channels: {', '.join(f'{channel}:{count}' for channel, count in channel_counts.most_common(3))}" if channel_counts else "No channel distribution was available.",
                f"Dominant source skills: {', '.join(f'{source}:{count}' for source, count in source_counts.most_common(3))}" if source_counts else "No source-skill distribution was available.",
                f"Selected claim types: {', '.join(unique_strings([maybe_text(item.get('claim_type')) for item in selected]))}" if selected else "No claim submissions were selected.",
            ]
        )
    else:
        full_families = unique_strings([observation_metric_family(item.get("metric")) for item in submissions])
        full_sources = unique_strings([maybe_text(item.get("source_skill")) for item in submissions if maybe_text(item.get("source_skill"))])
        selected_families = unique_strings([observation_metric_family(item.get("metric")) for item in selected])
        selected_sources = unique_strings([maybe_text(item.get("source_skill")) for item in selected if maybe_text(item.get("source_skill"))])
        selected_with_stats = sum(1 for item in selected if compact_statistics(item.get("statistics")))
        available_with_stats = sum(1 for item in submissions if compact_statistics(item.get("statistics")))
        coverage_dimensions = ["metric-family", "source-skill", "time-window"]
        if selected_with_stats:
            coverage_dimensions.append("extrema-summary")
        if len(full_families) > 1 and len(selected_families) < min(len(full_families), max(1, retained_limit)):
            missing_dimensions.append("metric-family-coverage")
        if len(full_sources) > 1 and len(selected_sources) < min(len(full_sources), max(1, retained_limit)):
            missing_dimensions.append("source-skill-coverage")
        if available_with_stats and selected_with_stats == 0:
            missing_dimensions.append("extrema-retention")
        coverage_summary = (
            f"Selected {len(selected)} auditable observation submissions from {len(submissions)} total while covering "
            f"{len(selected_families)}/{len(full_families) or 1} metric families and {len(selected_sources)}/{len(full_sources) or 1} source skills."
        )
        sampling_notes.extend(
            [
                f"Selected metric families: {', '.join(selected_families)}" if selected_families else "No metric-family coverage was selected.",
                f"Selected source skills: {', '.join(selected_sources)}" if selected_sources else "No source-skill coverage was selected.",
                f"Selected submissions with statistics retained: {selected_with_stats} of {available_with_stats}." if available_with_stats else "No observation statistics were available.",
            ]
        )

    return {
        "representative": bool(submissions) and not concentration_flags and not missing_dimensions,
        "retained_count": len(selected),
        "total_candidate_count": len(submissions),
        "coverage_summary": coverage_summary if submissions else fallback_summary,
        "concentration_flags": unique_strings(concentration_flags),
        "coverage_dimensions": unique_strings(coverage_dimensions),
        "missing_dimensions": unique_strings(missing_dimensions),
        "sampling_notes": unique_strings(sampling_notes),
    }


def build_claim_curation_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    candidates = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    constraints = mission_constraints(mission)
    limit = max(1, int(constraints.get("claim_target_per_round") or constraints.get("max_claims_per_round") or 3))
    selected = ranked_claim_candidates(candidates)[:limit]
    status = "complete" if candidates else "blocked"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "curation_id": f"claim-curation-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "sociologist",
        "status": status,
        "summary": (
            f"Review {len(candidates)} claim candidates and curate an auditable public-claim library for this round."
            if candidates
            else "No claim candidates were available for curation."
        ),
        "curated_claims": [candidate_claim_entry_from_candidate(item) for item in selected],
        "rejected_candidate_ids": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }
    validate_payload("claim-curation", payload)
    return payload


def build_observation_curation_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    candidates = state.get("observation_candidates_current", []) if isinstance(state.get("observation_candidates_current"), list) else []
    focus_claims = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    if not focus_claims:
        focus_claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    constraints = mission_constraints(mission)
    limit = max(4, int(constraints.get("claim_target_per_round") or constraints.get("max_claims_per_round") or 3) * 2)
    ordered = representative_observation_order(candidates, focus_claims)
    status = "complete" if candidates else "blocked"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "curation_id": f"observation-curation-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "environmentalist",
        "status": status,
        "summary": (
            f"Review {len(candidates)} observation candidates and curate atomic or composite physical evidence for this round."
            if candidates
            else "No observation candidates were available for curation."
        ),
        "curated_observations": [candidate_observation_entry_from_candidate(item, focus_claims) for item in ordered[:limit]],
        "rejected_candidate_ids": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }
    validate_payload("observation-curation", payload)
    return payload


def build_claim_curation_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like claim-curation.",
        "Review the full candidate public-claim pool before deciding what enters the auditable library.",
        "You may merge multiple candidate_claim_ids into one curated claim when they express the same public narrative.",
        "Use only claim_ids and candidate_claim_ids already present in the packet.",
        "Prefer rejected_candidate_ids for discarded items; reserve worth_storing=false for rare edge cases you still want explicitly recorded.",
        "Keep summaries, statements, and meaning fields grounded in the packet candidate pool only.",
        "Do not invent raw-source facts outside the packet.",
        "If the current envelope blocks the candidate diversity you need, use override_requests instead of silently expanding scope.",
    ]


def build_observation_curation_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like observation-curation.",
        "Review the full candidate physical-observation pool before deciding what enters the auditable library.",
        "You may keep observations atomic or combine multiple candidate_observation_ids into one composite observation.",
        "Composite observations must explicitly fill candidate_observation_ids, source_skills, metric_bundle, evidence_role, and component_roles.",
        "Use evidence_role and component_roles to distinguish primary, contextual, contradictory, or mixed parts of the observation.",
        "Do not let context-only weather background stand in for direct support unless the packet evidence itself justifies it.",
        "Use only candidate observation ids and candidate claim context already present in the packet.",
        "If the current envelope blocks necessary corroboration, use override_requests instead of silently expanding scope.",
    ]


def build_claim_curation_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_curation: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == "sociologist"]
    existing_curation = load_dict_if_exists(claim_curation_path(run_dir, round_id))
    candidates = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    return {
        "packet_kind": "claim-curation-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "sociologist",
        },
        "role": "sociologist",
        "task_scope": relevant_tasks,
        "context": context,
        "candidate_pool": {
            "candidate_count": len(candidates),
            "claim_candidates": [compact_claim_candidate_for_curation(item) for item in candidates],
        },
        "instructions": build_claim_curation_instructions(),
        "existing_override_requests": load_override_requests(run_dir, round_id, "sociologist"),
        "existing_curation": existing_curation,
        "draft_curation": draft_curation,
        "validation": {
            "kind": "claim-curation",
            "target_curation_path": str(claim_curation_path(run_dir, round_id)),
            "draft_curation_path": str(claim_curation_draft_path(run_dir, round_id)),
            "validate_command": (
                f"python3 {CONTRACT_SCRIPT_PATH} validate --kind claim-curation "
                f"--input {claim_curation_draft_path(run_dir, round_id)}"
            ),
        },
    }


def build_observation_curation_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_curation: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == "environmentalist"]
    existing_curation = load_dict_if_exists(observation_curation_path(run_dir, round_id))
    observation_candidates = (
        state.get("observation_candidates_current", [])
        if isinstance(state.get("observation_candidates_current"), list)
        else []
    )
    claim_candidates = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    return {
        "packet_kind": "observation-curation-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "environmentalist",
        },
        "role": "environmentalist",
        "task_scope": relevant_tasks,
        "context": context,
        "candidate_pool": {
            "claim_candidate_count": len(claim_candidates),
            "observation_candidate_count": len(observation_candidates),
            "claim_candidates": [compact_claim_candidate_for_curation(item) for item in claim_candidates],
            "observation_candidates": [compact_observation_candidate_for_curation(item) for item in observation_candidates],
        },
        "instructions": build_observation_curation_instructions(),
        "existing_override_requests": load_override_requests(run_dir, round_id, "environmentalist"),
        "existing_curation": existing_curation,
        "draft_curation": draft_curation,
        "validation": {
            "kind": "observation-curation",
            "target_curation_path": str(observation_curation_path(run_dir, round_id)),
            "draft_curation_path": str(observation_curation_draft_path(run_dir, round_id)),
            "validate_command": (
                f"python3 {CONTRACT_SCRIPT_PATH} validate --kind observation-curation "
                f"--input {observation_curation_draft_path(run_dir, round_id)}"
            ),
        },
    }


def claim_curation_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the sociologist for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope`, `context`, and `candidate_pool` before editing.",
        "3. Start from `draft_curation` inside the packet, but revise it freely when the packet evidence warrants it.",
        "4. Return only one JSON object shaped like claim-curation.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_curation_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def observation_curation_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the environmentalist for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope`, `context`, and `candidate_pool` before editing.",
        "3. Start from `draft_curation` inside the packet, but revise it freely when the packet evidence warrants it.",
        "4. Return only one JSON object shaped like observation-curation.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_curation_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def observation_metrics_from_submissions(submissions: list[dict[str, Any]]) -> set[str]:
    return {maybe_text(item.get("metric")) for item in submissions if maybe_text(item.get("metric"))}


def environment_role_required(state: dict[str, Any]) -> bool:
    claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    if any(bool(item.get("needs_physical_validation")) for item in claims if isinstance(item, dict)):
        return True
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    return any(maybe_text(task.get("assigned_role")) == "environmentalist" for task in tasks if isinstance(task, dict))


def readiness_missing_types(state: dict[str, Any], role: str) -> list[str]:
    submissions = state_submissions(state, role)
    claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    missing: set[str] = set()
    if role == "sociologist":
        if not submissions:
            missing.add("normalized-public-claims")
            return sorted(missing)
        if len(claim_submission_channels(submissions)) < 2:
            if any(maybe_text(item.get("claim_type")) != "policy-reaction" for item in submissions):
                missing.add("public-discussion-coverage")
        has_policy_claim = any(maybe_text(item.get("claim_type")) == "policy-reaction" for item in submissions)
        has_reggov = False
        for submission in submissions:
            refs = submission.get("public_refs")
            if not isinstance(refs, list):
                continue
            if any(
                isinstance(ref, dict)
                and maybe_text(ref.get("source_skill")) in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}
                for ref in refs
            ):
                has_reggov = True
                break
        if has_policy_claim and not has_reggov:
            missing.add("policy-comment-coverage")
        return sorted(missing)

    if not environment_role_required(state):
        return []
    if not submissions:
        return []
    metrics = observation_metrics_from_submissions(submissions)
    has_station_observation = any(
        maybe_text(item.get("source_skill")) in {"openaq-data-fetch", "airnow-hourly-obs-fetch"}
        or "airnow" in maybe_text(item.get("source_skill"))
        for item in submissions
    )
    claim_types = {maybe_text(item.get("claim_type")) for item in claims if maybe_text(item.get("claim_type"))}
    if claim_types & {"smoke", "air-pollution"} and not has_station_observation:
        missing.add("station-air-quality")
    if "wildfire" in claim_types and "fire_detection_count" not in metrics:
        missing.add("fire-detection")
    if "wildfire" in claim_types and not (metrics & METEOROLOGY_METRICS):
        missing.add("meteorology-background")
    if "flood" in claim_types and not (metrics & (PRECIPITATION_METRICS | HYDROLOGY_METRICS)):
        missing.add("precipitation-hydrology")
    if "heat" in claim_types and "temperature_2m" not in metrics:
        missing.add("temperature-extremes")
    if "drought" in claim_types and not {"precipitation_sum", "soil_moisture_0_to_7cm"} <= metrics:
        missing.add("precipitation-soil-moisture")
    return sorted(missing)


def generic_readiness_recommendations(role: str, missing_types: list[str], *, has_submissions: bool) -> list[dict[str, Any]]:
    recommendations = base_recommendations_from_missing_types(missing_types)
    if not has_submissions:
        if role == "sociologist":
            recommendations.append(
                {
                    "assigned_role": "sociologist",
                    "objective": "Collect and normalize mission-window public claims from approved channels.",
                    "reason": "No auditable claim submissions are available for readiness review yet.",
                }
            )
        else:
            recommendations.append(
                {
                    "assigned_role": "environmentalist",
                    "objective": "Collect and normalize mission-window physical observations from approved sources.",
                    "reason": "No auditable observation submissions are available for readiness review yet.",
                }
            )
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for recommendation in recommendations:
        key = recommendation_key(recommendation)
        if not key[0] or not key[1]:
            continue
        deduped.setdefault(key, recommendation)
    return list(deduped.values())


def build_readiness_findings_from_submissions(
    *,
    state: dict[str, Any],
    role: str,
    submissions: list[dict[str, Any]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    selected_submissions = representative_submissions(state, role, submissions, max_findings)
    for index, submission in enumerate(selected_submissions, start=1):
        if role == "sociologist":
            claim_id = maybe_text(submission.get("claim_id"))
            findings.append(
                {
                    "finding_id": f"finding-{index:03d}",
                    "title": truncate_text(maybe_text(submission.get("summary")) or f"Claim {claim_id}", 72),
                    "summary": truncate_text(
                        (
                            f"{maybe_text(submission.get('meaning'))} "
                            f"Source-signal count: {to_nonnegative_int(submission.get('source_signal_count'))}."
                        ),
                        300,
                    ),
                    "confidence": "medium" if to_nonnegative_int(submission.get("source_signal_count")) >= 2 else "low",
                    "claim_ids": [claim_id] if claim_id else [],
                    "observation_ids": [],
                    "evidence_ids": [],
                }
            )
        else:
            observation_id = maybe_text(submission.get("observation_id"))
            statistics_obj = compact_statistics(submission.get("statistics"))
            stats_text = ""
            if isinstance(statistics_obj, dict):
                stats_parts = [
                    f"{key}={value:g}"
                    for key in ("max", "p95", "mean", "min")
                    for value in [maybe_number(statistics_obj.get(key))]
                    if value is not None
                ]
                if stats_parts:
                    stats_text = f" Summary stats: {', '.join(stats_parts[:4])}."
            findings.append(
                {
                    "finding_id": f"finding-{index:03d}",
                    "title": truncate_text(
                        f"{maybe_text(submission.get('metric'))} from {maybe_text(submission.get('source_skill'))}",
                        72,
                    ),
                    "summary": truncate_text(
                        f"{maybe_text(submission.get('meaning'))} "
                        f"Metric family: {observation_metric_family(submission.get('metric'))}.{stats_text}",
                        300,
                    ),
                    "confidence": "medium",
                    "claim_ids": [],
                    "observation_ids": [observation_id] if observation_id else [],
                    "evidence_ids": [],
                }
            )
    return findings


def build_data_readiness_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    role: str,
    state: dict[str, Any],
    max_findings: int,
) -> dict[str, Any]:
    submissions = state_submissions(state, role)
    current_submissions = state_current_submissions(state, role)
    fallback_summary = (
        "Auditable public submissions are available for readiness review."
        if role == "sociologist"
        else "Auditable physical submissions are available for readiness review."
    )
    compact_audit = aggregate_compact_audit(
        state,
        role,
        submissions,
        fallback_summary=fallback_summary,
        retained_limit=max_findings,
    )
    missing_types = readiness_missing_types(state, role)
    has_submissions = bool(submissions)
    environment_required = not (role == "environmentalist" and not environment_role_required(state))
    readiness_status = "ready"
    if not environment_required:
        compact_audit["representative"] = True
        compact_audit["coverage_summary"] = "Current claims do not require physical-side validation in this round."
    elif not has_submissions:
        readiness_status = "blocked"
    elif missing_types or not compact_audit.get("representative"):
        readiness_status = "needs-more-data"
    sufficient_for_matching = readiness_status == "ready"
    if not sufficient_for_matching:
        compact_audit["representative"] = False
    findings = build_readiness_findings_from_submissions(
        state=state,
        role=role,
        submissions=submissions,
        max_findings=max_findings,
    )
    open_questions = [
        f"Should the next round address compact-audit concentration: {flag}?"
        for flag in compact_audit.get("concentration_flags", [])
        if maybe_text(flag)
    ]
    open_questions.extend(
        f"Is the compact view missing required coverage dimension `{dimension}` for matching?"
        for dimension in compact_audit.get("missing_dimensions", [])
        if maybe_text(dimension)
    )
    if environment_required and not has_submissions:
        open_questions.append(
            "Which approved source families should be expanded first to produce auditable canonical submissions for readiness review?"
        )
    recommendations = [] if not environment_required else generic_readiness_recommendations(role, missing_types, has_submissions=has_submissions)
    if missing_types:
        open_questions.extend(
            f"Should the next round address missing evidence type `{missing_type}` before matching?"
            for missing_type in missing_types
        )
    if role == "sociologist":
        summary_lead = (
            f"Public-side readiness reviewed {len(submissions)} auditable claim submissions "
            f"({len(current_submissions)} newly materialized this round)."
        )
    else:
        summary_lead = (
            f"Physical-side readiness reviewed {len(submissions)} auditable observation submissions "
            f"({len(current_submissions)} newly materialized this round)."
        )
    summary = f"{summary_lead} Status={readiness_status}. {compact_audit.get('coverage_summary')}"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "readiness_id": f"readiness-{role}-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": role,
        "readiness_status": readiness_status,
        "sufficient_for_matching": sufficient_for_matching,
        "summary": truncate_text(summary, 400),
        "findings": findings,
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations[:4],
        "override_requests": [],
        "referenced_submission_ids": [
            maybe_text(item.get("submission_id"))
            for item in submissions
            if maybe_text(item.get("submission_id"))
        ],
        "compact_audit": compact_audit,
    }
    validate_payload("data-readiness-report", payload)
    return payload


def build_data_readiness_instructions(role: str) -> list[str]:
    instructions = [
        "Return one JSON object only, shaped like data-readiness-report.",
        "Judge whether the auditable submission library available in this round is sufficiently representative for a later matching pass.",
        "Use submission_ids, claim_ids, and observation_ids already present in the packet context only.",
        "Do not invent raw-source facts outside the packet.",
        "If the compact representation is not sufficiently representative, use readiness_status=needs-more-data.",
        "If no auditable submissions are available, use readiness_status=blocked.",
        "Before the first matching pass, zero evidence cards is expected and must not be treated as a readiness failure by itself.",
        "Keep findings and recommendations traceable to the packet context.",
        "If the current mission envelope blocks the missing preparation you need, keep the report inside bounds and use override_requests for upstream review.",
    ]
    if role == "sociologist":
        instructions.append("Focus on narrative concentration, channel diversity, attribution clarity, and whether the compact claim set is representative.")
    else:
        instructions.append("Focus on metric coverage, provenance limits, and whether the compact observation set is representative enough for matching.")
    return instructions


def build_data_readiness_packet(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    draft_report: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == role]
    existing_report = load_dict_if_exists(data_readiness_report_path(run_dir, round_id, role))
    return {
        "packet_kind": "data-readiness-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": role,
        },
        "role": role,
        "task_scope": relevant_tasks,
        "context": context,
        "instructions": build_data_readiness_instructions(role),
        "existing_override_requests": load_override_requests(run_dir, round_id, role),
        "validation": {
            "kind": "data-readiness-report",
            "target_report_path": str(data_readiness_report_path(run_dir, round_id, role)),
            "draft_report_path": str(data_readiness_draft_path(run_dir, round_id, role)),
            "validate_command": (
                f"python3 {CONTRACT_SCRIPT_PATH} validate --kind data-readiness-report "
                f"--input {data_readiness_draft_path(run_dir, round_id, role)}"
            ),
        },
        "existing_report": existing_report,
        "draft_report": draft_report,
    }


def data_readiness_prompt_text(*, role: str, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the {role} for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope` and `context` before editing.",
        "3. Start from `draft_report` inside the packet.",
        "4. Return only one JSON object shaped like data-readiness-report.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Keep `override_requests` as [] unless the current mission envelope itself is insufficient.",
        "7. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_report_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_matching_authorization_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    readiness_reports = state.get("readiness_reports", {}) if isinstance(state.get("readiness_reports"), dict) else {}
    reports = {
        role: report
        for role, report in readiness_reports.items()
        if isinstance(report, dict) and report
    }
    statuses = {role: maybe_text(report.get("readiness_status")) for role, report in reports.items()}
    final_round = bool(contract_call("is_final_allowed_round", mission, round_id))
    if any(status == "blocked" for status in statuses.values()):
        base_status = "not-authorized"
    elif set(reports) == set(READINESS_ROLES) and all(bool(report.get("sufficient_for_matching")) for report in reports.values()):
        base_status = "authorized"
    else:
        base_status = "deferred"
    if base_status == "authorized":
        summary = "Both data roles report that the current auditable submissions are sufficiently prepared for a matching pass."
        rationale = "Matching should proceed before requesting broader collection because both sides judge the current compact evidence library to be representative enough."
    elif base_status == "not-authorized":
        summary = "At least one data role reports a blocked readiness state, so matching is not yet authorized under normal readiness rules."
        rationale = "Matching is not normally reasonable when one side lacks auditable submissions or is structurally blocked."
    else:
        summary = "Current data readiness is incomplete or mixed, so matching is deferred pending more preparation under normal readiness rules."
        rationale = "Matching should usually wait until both roles either report sufficiency or the upstream operator explicitly changes the collection boundary."
    claim_ids = [
        maybe_text(item.get("claim_id"))
        for item in state.get("claims_active", [])
        if isinstance(item, dict) and maybe_text(item.get("claim_id"))
    ]
    observation_ids = [
        maybe_text(item.get("observation_id"))
        for item in state.get("observations_active", [])
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    ]
    referenced_readiness_ids = [
        maybe_text(report.get("readiness_id"))
        for report in reports.values()
        if maybe_text(report.get("readiness_id"))
    ]
    open_questions: list[str] = []
    recommendations = combine_recommendations(reports=list(reports.values()), missing_types=[])
    for report in reports.values():
        open_questions.extend(
            maybe_text(item)
            for item in report.get("open_questions", [])
            if maybe_text(item)
        )
    if final_round and base_status != "authorized":
        open_questions.append("This is the final allowed round. Which isolated or remand outcomes remain acceptable if direct matches stay limited?")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "authorization_id": f"matchauth-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "moderator",
        "authorization_status": base_status,
        "moderator_requested_status": base_status,
        "authorization_basis": (
            "readiness-ready"
            if base_status == "authorized"
            else "readiness-blocked"
            if base_status == "not-authorized"
            else "readiness-deferred"
        ),
        "summary": truncate_text(summary, 400),
        "rationale": truncate_text(rationale, 500),
        "moderator_override": False,
        "allow_isolated_evidence": base_status == "authorized" or final_round,
        "referenced_readiness_ids": referenced_readiness_ids,
        "claim_ids": unique_strings(claim_ids),
        "observation_ids": unique_strings(observation_ids),
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations[:4],
    }
    payload = effective_matching_authorization(mission=mission, round_id=round_id, authorization=payload)
    validate_payload("matching-authorization", payload)
    return payload


def build_matching_authorization_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like matching-authorization.",
        "Authorize matching based on the auditable submission libraries and readiness reports, not on the presence of pre-existing evidence cards.",
        "If this is the final allowed round, the council must still run one terminal matching/adjudication pass and may end with matched, isolated, or remand evidence.",
        "Use only claim_ids and observation_ids already present in the packet.",
        "Do not invent new evidence or prescribe exact source skills.",
        "If authorization is deferred or denied, keep recommended_next_actions limited to evidence needs rather than collection commands.",
        "Do not use moderator_override to bypass mission policy_profile or source-governance boundaries. Upstream envelope changes must stay in override_requests on source-selection, readiness, report, or decision objects instead.",
        "allow_isolated_evidence should stay true unless the packet gives a specific reason to require strict remand-only handling.",
    ]


def build_matching_authorization_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_authorization: dict[str, Any],
) -> dict[str, Any]:
    return {
        "packet_kind": "matching-authorization-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "moderator",
        },
        "context": context,
        "readiness_reports": state.get("readiness_reports", {}),
        "pending_override_requests": load_override_requests(run_dir, round_id),
        "instructions": [
            "Return one JSON object only, shaped like matching-authorization.",
            "Authorize matching based on auditable submissions and readiness reports. Zero evidence cards before the first match is expected.",
            "If this is the final allowed round, the output must still permit one terminal matching/adjudication pass.",
            "Use the packet context and referenced readiness reports only; do not invent raw data or future rounds.",
            "If readiness is incomplete or blocked, use authorization_status=deferred or not-authorized and recommend the next data-preparation actions instead.",
            "Keep claim_ids and observation_ids restricted to canonical ids already present in the packet.",
            "Pending override requests are advisory context only; they do not authorize envelope changes inside matching-authorization.",
        ],
        "validation": {
            "kind": "matching-authorization",
            "target_authorization_path": str(matching_authorization_path(run_dir, round_id)),
            "draft_authorization_path": str(matching_authorization_draft_path(run_dir, round_id)),
            "validate_command": (
                f"python3 {CONTRACT_SCRIPT_PATH} validate --kind matching-authorization "
                f"--input {matching_authorization_draft_path(run_dir, round_id)}"
            ),
            "promote_command": (
                f"python3 {SCRIPT_DIR / 'eco_council_reporting.py'} promote-matching-authorization-draft "
                f"--run-dir {run_dir} --round-id {round_id}"
            ),
        },
        "existing_authorization": load_dict_if_exists(matching_authorization_path(run_dir, round_id)),
        "draft_authorization": draft_authorization,
    }


def matching_authorization_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the moderator for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `context` and `readiness_reports` before editing.",
        "3. Start from `draft_authorization` inside the packet.",
        "4. Return only one JSON object shaped like matching-authorization.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Do not use moderator_override to bypass mission policy_profile or source-governance boundaries.",
        "7. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_authorization_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Promotion command:",
        maybe_text(validation.get("promote_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_matching_adjudication_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like matching-adjudication.",
        "Treat packet.candidate_set and draft_adjudication as rule-nominated inputs, not as a binding final result.",
        "You may merge or prune nominated observation clusters as long as all claim_ids and observation_ids stay within the authorized packet scope.",
        "Use isolated_entries for acceptable but unmatched evidence; use remand_entries for evidence that still needs targeted follow-up.",
        "Keep matching_result, evidence_cards, isolated_entries, remand_entries, and evidence_adjudication mutually consistent.",
        "Do not invent raw facts, new source artifacts, or ids outside the packet candidate set and current round context.",
        "If allow_isolated_evidence is false, leave unmatched items in remand_entries instead of isolated_entries.",
        "Use recommended_next_actions only for concrete evidence needs; do not prescribe exact source skills or self-apply policy changes.",
    ]


def build_matching_adjudication_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
    candidate_set: dict[str, Any],
    draft_adjudication: dict[str, Any],
) -> dict[str, Any]:
    return {
        "packet_kind": "matching-adjudication-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "moderator",
        },
        "context": context,
        "readiness_reports": state.get("readiness_reports", {}),
        "matching_authorization": state.get("matching_authorization", {}),
        "candidate_set": candidate_set,
        "pending_override_requests": load_override_requests(run_dir, round_id),
        "instructions": build_matching_adjudication_instructions(),
        "validation": {
            "kind": "matching-adjudication",
            "target_adjudication_path": str(matching_adjudication_path(run_dir, round_id)),
            "draft_adjudication_path": str(matching_adjudication_draft_path(run_dir, round_id)),
            "validate_command": (
                f"python3 {CONTRACT_SCRIPT_PATH} validate --kind matching-adjudication "
                f"--input {matching_adjudication_draft_path(run_dir, round_id)}"
            ),
            "promote_command": (
                f"python3 {SCRIPT_DIR / 'eco_council_reporting.py'} promote-matching-adjudication-draft "
                f"--run-dir {run_dir} --round-id {round_id}"
            ),
        },
        "existing_adjudication": load_dict_if_exists(matching_adjudication_path(run_dir, round_id)),
        "draft_adjudication": draft_adjudication,
    }


def matching_adjudication_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the moderator for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `context`, `readiness_reports`, `matching_authorization`, and `candidate_set` before editing.",
        "3. Start from `draft_adjudication` inside the packet.",
        "4. Return only one JSON object shaped like matching-adjudication.",
        "5. Keep `schema_version`, `run_id`, `round_id`, `agent_role`, and `authorization_id` consistent with the packet.",
        "6. Only use claim ids and observation ids already present in the authorized candidate set or draft.",
        "7. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_adjudication_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Promotion command:",
        maybe_text(validation.get("promote_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def representative_observations_for_state(state: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    observation_by_id = {
        maybe_text(item.get("observation_id")): item
        for item in state.get("observations", [])
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    selected = representative_submissions(
        state,
        "environmentalist",
        state_auditable_submissions(state, "environmentalist"),
        limit,
    )
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for submission in selected:
        observation_id = maybe_text(submission.get("observation_id"))
        if not observation_id or observation_id in seen:
            continue
        observation = observation_by_id.get(observation_id)
        if observation is not None:
            ordered.append(observation)
            seen.add(observation_id)
    for observation in state.get("observations", []):
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in seen:
            continue
        ordered.append(observation)
        seen.add(observation_id)
        if len(ordered) >= limit:
            break
    return ordered


def build_fallback_context_from_state(*, run_dir: Path, state: dict[str, Any], role: str) -> dict[str, Any]:
    mission = state["mission"]
    round_id = state["round_id"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    role_tasks = [task for task in tasks if role == "moderator" or maybe_text(task.get("assigned_role")) == role]
    claim_candidates_current = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    observation_candidates_current = (
        state.get("observation_candidates_current", [])
        if isinstance(state.get("observation_candidates_current"), list)
        else []
    )
    claim_curation = state.get("claim_curation", {}) if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = state.get("observation_curation", {}) if isinstance(state.get("observation_curation"), dict) else {}
    claim_submissions_auditable = state_auditable_submissions(state, "sociologist")
    observation_submissions_auditable = state_auditable_submissions(state, "environmentalist")
    representative_claim_submissions = representative_submissions(state, "sociologist", claim_submissions_auditable, 6)
    representative_observation_submissions = representative_submissions(state, "environmentalist", observation_submissions_auditable, 8)
    representative_current_claim_submissions = representative_submissions(state, "sociologist", state_current_submissions(state, "sociologist"), 6)
    representative_current_observation_submissions = representative_submissions(
        state,
        "environmentalist",
        state_current_submissions(state, "environmentalist"),
        8,
    )
    return {
        "context_layer": "reporting-fallback-v2",
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region": mission.get("region"),
            "window": mission.get("window"),
            "role": role,
        },
        "dataset": {
            "generated_at_utc": utc_now_iso(),
            "task_count": len(role_tasks),
            "claim_count": len(state.get("claims", [])),
            "observation_count": len(state.get("observations", [])),
            "evidence_count": len(state.get("cards_active", [])),
            "claim_submission_count": len(claim_submissions_auditable),
            "observation_submission_count": len(observation_submissions_auditable),
            "claim_submission_current_count": len(state_current_submissions(state, "sociologist")),
            "observation_submission_current_count": len(state_current_submissions(state, "environmentalist")),
            "claim_candidate_count": len(claim_candidates_current),
            "observation_candidate_count": len(observation_candidates_current),
            "isolated_count": len(state.get("isolated_active", [])),
            "remand_count": len(state.get("remands_open", [])),
        },
        "phase_state": state.get("phase_state", {}),
        "tasks": role_tasks,
        "claims": [compact_claim(item) for item in state.get("claims", [])[:6]],
        "observations": [compact_observation(item) for item in representative_observations_for_state(state, 8)],
        "evidence_cards": [compact_evidence_card(item) for item in state.get("cards_active", [])[:8]],
        "evidence_library": {
            "claim_submissions_auditable": [
                compact_claim_submission(item)
                for item in representative_claim_submissions
            ],
            "observation_submissions_auditable": [
                compact_observation_submission(item)
                for item in representative_observation_submissions
            ],
            "claim_submissions_current": [
                compact_claim_submission(item)
                for item in representative_current_claim_submissions
            ],
            "observation_submissions_current": [
                compact_observation_submission(item)
                for item in representative_current_observation_submissions
            ],
            "claim_candidates_current": [compact_claim_candidate_for_curation(item) for item in claim_candidates_current[:12]],
            "observation_candidates_current": [
                compact_observation_candidate_for_curation(item) for item in observation_candidates_current[:16]
            ],
            "claim_curation": {
                "status": maybe_text(claim_curation.get("status")),
                "curated_claim_count": len(claim_curation.get("curated_claims", []))
                if isinstance(claim_curation.get("curated_claims"), list)
                else 0,
            },
            "observation_curation": {
                "status": maybe_text(observation_curation.get("status")),
                "curated_observation_count": len(observation_curation.get("curated_observations", []))
                if isinstance(observation_curation.get("curated_observations"), list)
                else 0,
            },
            "claims_active": [compact_claim_submission(item) for item in representative_claim_submissions],
            "observations_active": [
                compact_observation_submission(item) for item in representative_observation_submissions
            ],
            "cards_active": [compact_evidence_card(item) for item in state.get("cards_active", [])[:8]],
            "isolated_active": [compact_isolated_entry(item) for item in state.get("isolated_active", [])[:6]],
            "remands_open": [compact_remand_entry(item) for item in state.get("remands_open", [])[:6]],
        },
        "canonical_paths": {
            "claim_candidates": str(claim_candidates_path(run_dir, round_id)),
            "observation_candidates": str(observation_candidates_path(run_dir, round_id)),
            "claim_curation": str(claim_curation_path(run_dir, round_id)),
            "observation_curation": str(observation_curation_path(run_dir, round_id)),
            "claim_submissions": str(claim_submissions_path(run_dir, round_id)),
            "observation_submissions": str(observation_submissions_path(run_dir, round_id)),
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
        },
    }


def load_context_or_fallback_from_state(*, run_dir: Path, state: dict[str, Any], role: str) -> dict[str, Any]:
    path = role_context_path(run_dir, state["round_id"], role)
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return augment_context_with_matching_state(run_dir=run_dir, state=state, context=payload)
    return augment_context_with_matching_state(
        run_dir=run_dir,
        state=state,
        context=build_fallback_context_from_state(run_dir=run_dir, state=state, role=role),
    )


def build_pre_match_report_findings(state: dict[str, Any], role: str, max_findings: int) -> list[dict[str, Any]]:
    readiness = state.get("readiness_reports", {}).get(role)
    if isinstance(readiness, dict):
        findings = readiness.get("findings")
        if isinstance(findings, list) and findings:
            return [item for item in findings if isinstance(item, dict)][:max_findings]
    return build_readiness_findings_from_submissions(
        state=state,
        role=role,
        submissions=state_submissions(state, role),
        max_findings=max_findings,
    )


def expert_report_status_from_state(state: dict[str, Any], role: str) -> str:
    authorization_status = maybe_text(state.get("matching_authorization", {}).get("authorization_status"))
    readiness_report = state.get("readiness_reports", {}).get(role)
    if isinstance(readiness_report, dict) and maybe_text(readiness_report.get("readiness_status")) == "blocked":
        return "blocked"
    if authorization_status == "authorized" and not matching_executed_for_state(state):
        return "blocked"
    if matching_executed_for_state(state):
        if state.get("remands_open") or bool(state.get("evidence_adjudication", {}).get("needs_additional_data")):
            return "needs-more-evidence"
        return "complete"
    if authorization_status in {"deferred", "not-authorized"}:
        return "needs-more-evidence"
    if isinstance(readiness_report, dict) and readiness_report:
        return "needs-more-evidence"
    return "blocked"


def build_report_summary_from_state(state: dict[str, Any], role: str, status: str) -> str:
    readiness_report = state.get("readiness_reports", {}).get(role)
    if matching_executed_for_state(state):
        cards = state.get("cards_active", [])
        isolated = state.get("isolated_active", [])
        remands = state.get("remands_open", [])
        moderator_adjudication = state.get("matching_adjudication", {})
        adjudication = state.get("evidence_adjudication", {})
        return truncate_text(
            (
                f"Matching/adjudication is available with {len(cards)} active evidence cards, "
                f"{len(isolated)} isolated entries, and {len(remands)} open remands. "
                f"Status={status}. "
                f"{maybe_text(moderator_adjudication.get('summary')) or maybe_text(adjudication.get('summary'))}"
            ),
            400,
        )
    authorization = state.get("matching_authorization", {})
    readiness_summary = maybe_text(readiness_report.get("summary")) if isinstance(readiness_report, dict) else ""
    authorization_summary = maybe_text(authorization.get("summary"))
    if readiness_summary and authorization_summary:
        return truncate_text(f"{readiness_summary} {authorization_summary}", 400)
    if readiness_summary:
        return truncate_text(readiness_summary, 400)
    if authorization_summary:
        return truncate_text(authorization_summary, 400)
    return "The round does not yet have enough canonical readiness or matching artifacts to support a stronger expert summary."


def build_expert_report_draft_from_state(
    *,
    state: dict[str, Any],
    role: str,
    max_findings: int,
) -> dict[str, Any]:
    mission = state["mission"]
    round_id = state["round_id"]
    claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    observations = state.get("observations", []) if isinstance(state.get("observations"), list) else []
    evidence_cards = state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else []
    if matching_executed_for_state(state):
        moderator_adjudication = state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
        evidence_by_claim = evidence_by_claim_map(evidence_cards)
        observations_by_id = observations_by_id_map(observations)
        claims_by_id = claims_by_id_map(claims)
        if role == "sociologist":
            findings = build_sociologist_findings(
                claims=claims,
                evidence_by_claim=evidence_by_claim,
                observations_by_id=observations_by_id,
                max_findings=max_findings,
            )
        else:
            findings = build_environmentalist_findings(
                claims=claims,
                observations=observations,
                evidence_cards=evidence_cards,
                observations_by_id=observations_by_id,
                claims_by_id=claims_by_id,
                max_findings=max_findings,
            )
        open_questions = build_open_questions(evidence_cards)
        open_questions.extend(
            f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
            for item in state.get("remands_open", [])
            if isinstance(item, dict) and maybe_text(item.get("remand_id"))
        )
        open_questions.extend(
            maybe_text(item)
            for item in moderator_adjudication.get("open_questions", [])
            if maybe_text(item)
        )
        readiness_report = state.get("readiness_reports", {}).get(role)
        additional_reports = [
            item
            for item in [moderator_adjudication, readiness_report]
            if isinstance(item, dict) and item
        ]
        recommendations = combine_recommendations(
            reports=[item for item in additional_reports if isinstance(item, dict)],
            missing_types=[],
        )[:4]
    else:
        findings = build_pre_match_report_findings(state, role, max_findings)
        readiness_report = state.get("readiness_reports", {}).get(role)
        open_questions = []
        recommendations = []
        if isinstance(readiness_report, dict):
            open_questions.extend(
                maybe_text(item)
                for item in readiness_report.get("open_questions", [])
                if maybe_text(item)
            )
            recommendations.extend(
                item
                for item in readiness_report.get("recommended_next_actions", [])
                if isinstance(item, dict)
            )
        if not recommendations:
            recommendations = generic_readiness_recommendations(
                role,
                readiness_missing_types(state, role),
                has_submissions=bool(state_submissions(state, role)),
            )[:4]
    status = expert_report_status_from_state(state, role)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": role,
        "status": status,
        "summary": build_report_summary_from_state(state, role, status),
        "findings": findings,
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations[:4],
        "override_requests": [],
    }
    validate_payload("expert-report", payload)
    return payload


def readiness_score(state: dict[str, Any]) -> float:
    readiness_reports = state.get("readiness_reports", {}) if isinstance(state.get("readiness_reports"), dict) else {}
    if not readiness_reports:
        return 0.0
    completed = sum(1 for role in READINESS_ROLES if isinstance(readiness_reports.get(role), dict) and readiness_reports.get(role))
    sufficient = sum(1 for role in READINESS_ROLES if bool((readiness_reports.get(role) or {}).get("sufficient_for_matching")))
    total = max(1, len(READINESS_ROLES))
    return round(((completed / total) * 0.5) + ((sufficient / total) * 0.5), 2)


def collect_unresolved_anchor_refs(state: dict[str, Any]) -> tuple[list[str], list[str]]:
    round_id = state["round_id"]
    focus_claim_ids: list[str] = []
    anchor_refs: list[str] = []
    for item in state.get("remands_open", []):
        if not isinstance(item, dict):
            continue
        entity_kind = maybe_text(item.get("entity_kind"))
        entity_id = maybe_text(item.get("entity_id"))
        if not entity_kind or not entity_id:
            continue
        anchor_refs.append(f"{round_id}:{entity_kind}:{entity_id}")
        if entity_kind == "claim":
            focus_claim_ids.append(entity_id)
    for item in state.get("isolated_active", []):
        if not isinstance(item, dict):
            continue
        entity_kind = maybe_text(item.get("entity_kind"))
        entity_id = maybe_text(item.get("entity_id"))
        if not entity_kind or not entity_id:
            continue
        anchor_refs.append(f"{round_id}:{entity_kind}:{entity_id}")
        if entity_kind == "claim":
            focus_claim_ids.append(entity_id)
    result = state.get("matching_result", {})
    if isinstance(result, dict):
        for claim_id in result.get("unmatched_claim_ids", []):
            if maybe_text(claim_id):
                focus_claim_ids.append(maybe_text(claim_id))
                anchor_refs.append(f"{round_id}:claim:{maybe_text(claim_id)}")
        for observation_id in result.get("unmatched_observation_ids", []):
            if maybe_text(observation_id):
                anchor_refs.append(f"{round_id}:observation:{maybe_text(observation_id)}")
    for card in state.get("cards_active", []):
        if not isinstance(card, dict):
            continue
        verdict = maybe_text(card.get("verdict"))
        claim_id = maybe_text(card.get("claim_id"))
        evidence_id = maybe_text(card.get("evidence_id"))
        if verdict in {"mixed", "insufficient"} and claim_id:
            focus_claim_ids.append(claim_id)
            anchor_refs.append(f"{round_id}:claim:{claim_id}")
        if verdict in {"mixed", "insufficient"} and evidence_id:
            anchor_refs.append(f"{round_id}:card:{evidence_id}")
    return unique_strings(focus_claim_ids), unique_strings(anchor_refs)


def missing_types_from_reason_texts(texts: list[str]) -> list[str]:
    missing: set[str] = set()
    for text in texts:
        lowered = maybe_text(text).lower()
        if not lowered:
            continue
        if "station" in lowered or "pm2" in lowered or "air-quality" in lowered:
            missing.add("station-air-quality")
        if "fire" in lowered or "wildfire" in lowered:
            missing.add("fire-detection")
        if "wind" in lowered or "humidity" in lowered or "weather" in lowered or "meteorology" in lowered:
            missing.add("meteorology-background")
        if "flood" in lowered or "river" in lowered or "hydrology" in lowered or "precipitation" in lowered:
            missing.add("precipitation-hydrology")
        if "temperature" in lowered or "heat" in lowered:
            missing.add("temperature-extremes")
        if "soil" in lowered or "drought" in lowered:
            missing.add("precipitation-soil-moisture")
        if "policy" in lowered or "comment" in lowered or "docket" in lowered:
            missing.add("policy-comment-coverage")
        if "public" in lowered or "discussion" in lowered or "claim" in lowered or "attributable" in lowered:
            missing.add("public-discussion-coverage")
    return sorted(missing)


def build_decision_missing_types(state: dict[str, Any]) -> list[str]:
    if matching_executed_for_state(state):
        reason_texts: list[str] = []
        for item in state.get("remands_open", []):
            if not isinstance(item, dict):
                continue
            reason_texts.extend(
                maybe_text(reason)
                for reason in item.get("reasons", [])
                if maybe_text(reason)
            )
        if reason_texts:
            return missing_types_from_reason_texts(reason_texts)
        return []
    return sorted(
        {
            *readiness_missing_types(state, "sociologist"),
            *readiness_missing_types(state, "environmentalist"),
        }
    )


def build_decision_summary_from_state(
    *,
    state: dict[str, Any],
    moderator_status: str,
    evidence_sufficiency: str,
    report_sources: dict[str, str],
    blocked_reason: str,
) -> str:
    if moderator_status == "blocked" and blocked_reason:
        return blocked_reason
    if matching_executed_for_state(state):
        cards = len(state.get("cards_active", []))
        isolated = len(state.get("isolated_active", []))
        remands = len(state.get("remands_open", []))
        moderator_adjudication = state.get("matching_adjudication", {})
        adjudication = state.get("evidence_adjudication", {})
        if moderator_status == "continue":
            return (
                f"Matching/adjudication produced {cards} cards, {isolated} isolated entries, and {remands} remands. "
                f"Another round is required before closure. Report sources used: "
                f"{', '.join(f'{role}:{source}' for role, source in sorted(report_sources.items()))}."
            )
        return truncate_text(
            (
                f"Matching/adjudication is {evidence_sufficiency}. "
                f"{maybe_text(moderator_adjudication.get('summary')) or maybe_text(adjudication.get('summary'))}"
            ),
            400,
        )
    authorization = state.get("matching_authorization", {})
    readiness_reports = state.get("readiness_reports", {})
    readiness_summary = " ".join(
        maybe_text(report.get("summary"))
        for role in READINESS_ROLES
        for report in [readiness_reports.get(role)]
        if isinstance(report, dict)
    )
    return truncate_text(
        (
            f"Matching was not executed. Authorization status is {maybe_text(authorization.get('authorization_status'))} "
            f"(basis={maybe_text(authorization.get('authorization_basis')) or 'unspecified'}). "
            f"Current evidence sufficiency is {evidence_sufficiency}. {readiness_summary}"
        ),
        400,
    )


def build_decision_draft_from_state(
    *,
    run_dir: Path,
    state: dict[str, Any],
    next_round_id: str,
    reports: dict[str, dict[str, Any] | None],
    report_sources: dict[str, str],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    mission = state["mission"]
    round_id = state["round_id"]
    usable_reports = [report for report in reports.values() if isinstance(report, dict)]
    missing_types = build_decision_missing_types(state)
    focus_claim_ids, anchor_refs = collect_unresolved_anchor_refs(state)
    readiness_reports = state.get("readiness_reports", {})
    recommendation_inputs = [
        item
        for item in [state.get("matching_adjudication")]
        if isinstance(item, dict) and item
    ] + usable_reports + [
        report
        for report in readiness_reports.values()
        if isinstance(report, dict) and report
    ]
    recommendations = combine_recommendations(reports=recommendation_inputs, missing_types=missing_types)
    next_round_tasks, task_plan_info = build_next_round_tasks(
        run_dir=run_dir,
        mission=mission,
        current_round_id=round_id,
        next_round_id=next_round_id,
        recommendations=recommendations,
        focus_claim_ids=focus_claim_ids,
        anchor_refs=anchor_refs,
    )
    max_rounds = mission_constraints(mission).get("max_rounds")
    current_number = current_round_number(round_id)
    next_number = current_round_number(next_round_id)
    authorization_status = maybe_text(state.get("matching_authorization", {}).get("authorization_status"))
    blocked_reason = ""
    blocked_by_max_rounds = False
    if (
        not state.get("claims")
        and not state.get("observations")
        and not state_auditable_submissions(state, "sociologist")
        and not state_auditable_submissions(state, "environmentalist")
    ):
        moderator_status = "blocked"
        next_round_required = False
        blocked_reason = "The round did not produce enough auditable submissions, claims, or observations to continue."
    elif authorization_status == "authorized" and not matching_executed_for_state(state):
        moderator_status = "blocked"
        next_round_required = False
        blocked_reason = "Matching was authorized but matching/adjudication has not been executed yet."
    elif authorization_status in {"deferred", "not-authorized"}:
        if max_rounds is not None and current_number is not None and next_number is not None and next_number > max_rounds and next_round_tasks:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = f"The configured max_rounds={max_rounds} would be exceeded by {next_round_id}."
            next_round_tasks = []
            blocked_by_max_rounds = True
        elif next_round_tasks:
            moderator_status = "continue"
            next_round_required = True
        else:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = "Matching was not authorized and no concrete next-round tasks could be derived."
    elif state.get("remands_open") or missing_types:
        if max_rounds is not None and current_number is not None and next_number is not None and next_number > max_rounds and next_round_tasks:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = f"The configured max_rounds={max_rounds} would be exceeded by {next_round_id}."
            next_round_tasks = []
            blocked_by_max_rounds = True
        elif next_round_tasks:
            moderator_status = "continue"
            next_round_required = True
        else:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = "The round still has unresolved evidence issues, but no materially different next-round task could be derived."
    else:
        moderator_status = "complete"
        next_round_required = False
        next_round_tasks = []
    if matching_executed_for_state(state):
        if state.get("remands_open"):
            evidence_sufficiency = "partial"
        elif state.get("cards_active") or state.get("isolated_active"):
            evidence_sufficiency = "sufficient"
        else:
            evidence_sufficiency = "insufficient"
        completion_score = completion_score_for_round(state.get("cards_active", []), usable_reports)
    else:
        evidence_sufficiency = "partial" if readiness_score(state) >= 0.75 else "insufficient"
        completion_score = round(min(1.0, 0.2 + (0.4 * readiness_score(state)) + (0.4 * report_completion_score(usable_reports))), 2)
    override_requests = build_decision_override_requests(
        mission=mission,
        round_id=round_id,
        next_round_id=next_round_id,
        focus_claim_ids=focus_claim_ids,
        anchor_refs=anchor_refs,
        task_plan_info=task_plan_info,
        next_round_requested_but_blocked_by_max_rounds=blocked_by_max_rounds,
    )
    decision_summary = build_decision_summary_from_state(
        state=state,
        moderator_status=moderator_status,
        evidence_sufficiency=evidence_sufficiency,
        report_sources=report_sources,
        blocked_reason=blocked_reason,
    )
    final_brief = build_final_brief(moderator_status=moderator_status, decision_summary=decision_summary, reports=reports)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "decision_id": f"decision-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "moderator_status": moderator_status,
        "completion_score": completion_score,
        "evidence_sufficiency": evidence_sufficiency,
        "decision_summary": decision_summary,
        "next_round_required": next_round_required,
        "missing_evidence_types": missing_types,
        "next_round_tasks": next_round_tasks,
        "override_requests": override_requests,
        "final_brief": final_brief,
    }
    validate_payload("council-decision", payload)
    return payload, next_round_tasks, missing_types


def build_decision_packet_from_state(
    *,
    run_dir: Path,
    state: dict[str, Any],
    next_round_id: str,
    moderator_context: dict[str, Any],
    reports: dict[str, dict[str, Any] | None],
    report_sources: dict[str, str],
    draft_decision: dict[str, Any],
    proposed_next_round_tasks: list[dict[str, Any]],
    missing_evidence_types: list[str],
) -> dict[str, Any]:
    return {
        "packet_kind": "council-decision-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(state["mission"]),
        "effective_constraints": mission_constraints(state["mission"]),
        "run": {
            "run_id": mission_run_id(state["mission"]),
            "round_id": state["round_id"],
            "next_round_id": next_round_id,
            "topic": maybe_text(state["mission"].get("topic")),
            "objective": maybe_text(state["mission"].get("objective")),
        },
        "round_context": moderator_context,
        "readiness_reports": state.get("readiness_reports", {}),
        "matching_authorization": state.get("matching_authorization", {}),
        "matching_adjudication": state.get("matching_adjudication", {}),
        "matching_result": state.get("matching_result", {}),
        "evidence_adjudication": state.get("evidence_adjudication", {}),
        "reports": reports,
        "report_sources": report_sources,
        "missing_evidence_types": missing_evidence_types,
        "proposed_next_round_tasks": proposed_next_round_tasks,
        "pending_override_requests": load_override_requests(run_dir, state["round_id"]),
        "instructions": [
            "Return one JSON object only, shaped like council-decision.",
            "Base the decision on readiness reports, matching authorization, matching/adjudication artifacts, and expert-report content, not on raw fetch artifacts.",
            "Treat `round_context` as a compact summary layer first and consult `canonical_paths` only if a summary detail is insufficient.",
            "If another round is required, add new round-task objects for next_round_id instead of editing current tasks in place.",
            "Respect mission constraints such as max_rounds and max_tasks_per_round.",
            "Use anchor_refs and evidence gaps to define follow-up scope; do not prescribe concrete source skills inside moderator tasks.",
            "If mission constraints block a necessary follow-up round or task envelope, keep the decision inside the current boundary and use override_requests for upstream review.",
            "Keep final_brief empty unless the council is complete or blocked.",
        ],
        "validation": {
            "kind": "council-decision",
            "target_decision_path": str(decision_target_path(run_dir, state["round_id"])),
            "draft_decision_path": str(decision_draft_path(run_dir, state["round_id"])),
            "validate_command": (
                f"python3 {CONTRACT_SCRIPT_PATH} validate --kind council-decision "
                f"--input {decision_draft_path(run_dir, state['round_id'])}"
            ),
        },
        "draft_decision": draft_decision,
    }


def evidence_resolution_score(evidence_cards: list[dict[str, Any]]) -> float:
    if not evidence_cards:
        return 0.0
    total = 0.0
    for card in evidence_cards:
        total += VERDICT_SCORES.get(maybe_text(card.get("verdict")), 0.0)
    return total / len(evidence_cards)


def report_completion_score(reports: list[dict[str, Any]]) -> float:
    if not reports:
        return 0.0
    complete = 0
    for report in reports:
        if report_has_substance(report):
            complete += 1
    return complete / len(reports)


def completion_score_for_round(evidence_cards: list[dict[str, Any]], reports: list[dict[str, Any]]) -> float:
    score = 0.1 + 0.7 * evidence_resolution_score(evidence_cards) + 0.2 * report_completion_score(reports)
    score = max(0.0, min(1.0, score))
    return round(score, 2)


def evidence_sufficiency_for_round(evidence_cards: list[dict[str, Any]], missing_evidence_types: list[str]) -> str:
    if not evidence_cards:
        return "insufficient"
    verdicts = [maybe_text(item.get("verdict")) for item in evidence_cards]
    confidences = [maybe_text(item.get("confidence")) for item in evidence_cards]
    if any(verdict in {"mixed", "insufficient", ""} for verdict in verdicts):
        return "insufficient"
    if missing_evidence_types:
        return "partial"
    if confidences and all(confidence == "low" for confidence in confidences):
        return "partial"
    if len(evidence_cards) > 1 and any(confidence == "low" for confidence in confidences):
        return "partial"
    if set(verdicts) <= {"supports", "contradicts"}:
        return "sufficient"
    return "partial"


def build_final_brief(*, moderator_status: str, decision_summary: str, reports: dict[str, dict[str, Any] | None]) -> str:
    if moderator_status == "continue":
        return ""
    summaries: list[str] = []
    seen: set[str] = set()
    for item in [decision_summary] + [maybe_text(report.get("summary")) for report in reports.values() if isinstance(report, dict)]:
        text = maybe_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        summaries.append(text)
    return truncate_text(" ".join(summaries), 600)


def load_required_object(path: Path, label: str) -> dict[str, Any]:
    payload = load_json_if_exists(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} is missing or not a JSON object: {path}")
    return payload


def report_prompt_text(*, role: str, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the {role} for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope` and `context` before editing.",
        "3. Start from `draft_report` inside the packet.",
        "4. Return only one JSON object shaped like expert-report.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. `recommended_next_actions` must be a list of objects with `assigned_role`, `objective`, and `reason`; do not emit strings there.",
        "7. Keep `override_requests` as [] unless the current mission envelope itself is insufficient.",
        "8. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_report_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def decision_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use $eco-council-reporting.",
        f"You are the moderator for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `round_context`, `readiness_reports`, `matching_authorization`, `matching_adjudication`, `matching_result`, `evidence_adjudication`, `reports`, and `proposed_next_round_tasks` before editing.",
        "3. Start from `draft_decision` inside the packet.",
        "4. If another round is needed, make sure each task adds at least one new evidence requirement or materially different claim focus; leave concrete source-family and layer choice to the expert source-selection stage.",
        "5. Return only one JSON object shaped like council-decision.",
        "6. Keep `schema_version`, `run_id`, and `round_id` consistent with the packet.",
        "7. Use `override_requests` only for upstream mission-envelope changes such as max_rounds or max_tasks_per_round; do not self-apply them.",
        "8. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_decision_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def can_replace_existing_report(existing_payload: dict[str, Any] | None, new_payload: dict[str, Any]) -> bool:
    if existing_payload is None:
        return True
    if existing_payload == new_payload:
        return True
    return report_is_placeholder(existing_payload)


def can_replace_existing_decision(existing_payload: dict[str, Any] | None, new_payload: dict[str, Any]) -> bool:
    if existing_payload is None:
        return True
    return existing_payload == new_payload


def can_replace_existing_matching_object(existing_payload: dict[str, Any] | None, new_payload: dict[str, Any]) -> bool:
    if existing_payload is None:
        return True
    return existing_payload == new_payload


def load_report_draft_payload(run_dir: Path, round_id: str, role: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    draft_path = Path(draft_path_text).expanduser().resolve() if draft_path_text else report_draft_path(run_dir, round_id, role)
    payload = load_required_object(draft_path, f"{role} report draft")
    if maybe_text(payload.get("agent_role")) != role:
        raise ValueError(f"Report draft role mismatch: expected {role}, got {payload.get('agent_role')!r}")
    if maybe_text(payload.get("round_id")) != round_id:
        raise ValueError(f"Report draft round mismatch: expected {round_id}, got {payload.get('round_id')!r}")
    validate_payload("expert-report", payload)
    return draft_path, payload


def load_decision_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    draft_path = Path(draft_path_text).expanduser().resolve() if draft_path_text else decision_draft_path(run_dir, round_id)
    payload = load_required_object(draft_path, "moderator decision draft")
    if maybe_text(payload.get("round_id")) != round_id:
        raise ValueError(f"Decision draft round mismatch: expected {round_id}, got {payload.get('round_id')!r}")
    validate_payload("council-decision", payload)
    return draft_path, payload


def load_matching_authorization_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    draft_path = (
        Path(draft_path_text).expanduser().resolve()
        if draft_path_text
        else matching_authorization_draft_path(run_dir, round_id)
    )
    payload = load_required_object(draft_path, "moderator matching-authorization draft")
    if maybe_text(payload.get("round_id")) != round_id:
        raise ValueError(f"Matching-authorization draft round mismatch: expected {round_id}, got {payload.get('round_id')!r}")
    if maybe_text(payload.get("agent_role")) != "moderator":
        raise ValueError(f"Matching-authorization draft role mismatch: expected moderator, got {payload.get('agent_role')!r}")
    validate_payload("matching-authorization", payload)
    return draft_path, payload


def load_matching_adjudication_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    draft_path = (
        Path(draft_path_text).expanduser().resolve()
        if draft_path_text
        else matching_adjudication_draft_path(run_dir, round_id)
    )
    payload = load_required_object(draft_path, "moderator matching-adjudication draft")
    if maybe_text(payload.get("round_id")) != round_id:
        raise ValueError(f"Matching-adjudication draft round mismatch: expected {round_id}, got {payload.get('round_id')!r}")
    if maybe_text(payload.get("agent_role")) != "moderator":
        raise ValueError(f"Matching-adjudication draft role mismatch: expected moderator, got {payload.get('agent_role')!r}")
    validate_payload("matching-adjudication", payload)
    return draft_path, payload


def promote_report_draft(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_report_draft_payload(run_dir, round_id, role, draft_path_text)
    target_path = report_target_path(run_dir, round_id, role)
    existing_payload = load_json_if_exists(target_path)
    if existing_payload is not None and not isinstance(existing_payload, dict):
        raise ValueError(f"Existing canonical report is not a JSON object: {target_path}")
    if not allow_overwrite and not can_replace_existing_report(existing_payload, payload):
        raise ValueError(f"Refusing to overwrite non-placeholder canonical report without --allow-overwrite: {target_path}")
    write_json(target_path, payload, pretty=pretty)
    return {
        "role": role,
        "draft_path": str(draft_path),
        "target_path": str(target_path),
        "overwrote_existing": existing_payload is not None and existing_payload != payload,
    }


def promote_decision_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_decision_draft_payload(run_dir, round_id, draft_path_text)
    target_path = decision_target_path(run_dir, round_id)
    existing_payload = load_json_if_exists(target_path)
    if existing_payload is not None and not isinstance(existing_payload, dict):
        raise ValueError(f"Existing canonical decision is not a JSON object: {target_path}")
    if not allow_overwrite and not can_replace_existing_decision(existing_payload, payload):
        raise ValueError(f"Refusing to overwrite canonical decision without --allow-overwrite: {target_path}")
    write_json(target_path, payload, pretty=pretty)
    return {
        "draft_path": str(draft_path),
        "target_path": str(target_path),
        "overwrote_existing": existing_payload is not None and existing_payload != payload,
    }


def promote_matching_authorization_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_matching_authorization_draft_payload(run_dir, round_id, draft_path_text)
    target_path = matching_authorization_path(run_dir, round_id)
    existing_payload = load_json_if_exists(target_path)
    if existing_payload is not None and not isinstance(existing_payload, dict):
        raise ValueError(f"Existing canonical matching-authorization is not a JSON object: {target_path}")
    if not allow_overwrite and not can_replace_existing_matching_object(existing_payload, payload):
        raise ValueError(f"Refusing to overwrite canonical matching-authorization without --allow-overwrite: {target_path}")
    write_json(target_path, payload, pretty=pretty)
    return {
        "draft_path": str(draft_path),
        "target_path": str(target_path),
        "overwrote_existing": existing_payload is not None and existing_payload != payload,
    }


def promote_matching_adjudication_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_matching_adjudication_draft_payload(run_dir, round_id, draft_path_text)
    target_path = matching_adjudication_path(run_dir, round_id)
    existing_payload = load_json_if_exists(target_path)
    if existing_payload is not None and not isinstance(existing_payload, dict):
        raise ValueError(f"Existing canonical matching-adjudication is not a JSON object: {target_path}")
    if not allow_overwrite and not can_replace_existing_matching_object(existing_payload, payload):
        raise ValueError(f"Refusing to overwrite canonical matching-adjudication without --allow-overwrite: {target_path}")
    write_json(target_path, payload, pretty=pretty)
    return {
        "draft_path": str(draft_path),
        "target_path": str(target_path),
        "overwrote_existing": existing_payload is not None and existing_payload != payload,
    }


def curation_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []

    sociologist_context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="sociologist")
    claim_draft = build_claim_curation_draft(
        mission=mission,
        round_id=round_id,
        state=state,
    )
    claim_packet = build_claim_curation_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        tasks=tasks,
        context=sociologist_context,
        state=state,
        draft_curation=claim_draft,
    )
    claim_packet_file = claim_curation_packet_path(run_dir, round_id)
    claim_draft_file = claim_curation_draft_path(run_dir, round_id)
    write_json(claim_packet_file, claim_packet, pretty=pretty)
    write_json(claim_draft_file, claim_draft, pretty=pretty)

    environmentalist_context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="environmentalist")
    observation_draft = build_observation_curation_draft(
        mission=mission,
        round_id=round_id,
        state=state,
    )
    observation_packet = build_observation_curation_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        tasks=tasks,
        context=environmentalist_context,
        state=state,
        draft_curation=observation_draft,
    )
    observation_packet_file = observation_curation_packet_path(run_dir, round_id)
    observation_draft_file = observation_curation_draft_path(run_dir, round_id)
    write_json(observation_packet_file, observation_packet, pretty=pretty)
    write_json(observation_draft_file, observation_draft, pretty=pretty)

    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "claim_candidate_count": len(state.get("claim_candidates_current", [])),
        "observation_candidate_count": len(state.get("observation_candidates_current", [])),
        "outputs": {
            "sociologist": {
                "packet_path": str(claim_packet_file),
                "draft_path": str(claim_draft_file),
            },
            "environmentalist": {
                "packet_path": str(observation_packet_file),
                "draft_path": str(observation_draft_file),
            },
        },
    }


def curation_status_complete(curation: dict[str, Any]) -> bool:
    status = maybe_text(curation.get("status"))
    return status in {"complete", "blocked"}


def curations_materialized_for_round(*, run_dir: Path, round_id: str, state: dict[str, Any]) -> bool:
    claim_curation = state.get("claim_curation", {}) if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = state.get("observation_curation", {}) if isinstance(state.get("observation_curation"), dict) else {}
    if not curation_status_complete(claim_curation) or not curation_status_complete(observation_curation):
        return False
    required_paths = (
        claim_curation_path(run_dir, round_id),
        observation_curation_path(run_dir, round_id),
        claim_submissions_path(run_dir, round_id),
        observation_submissions_path(run_dir, round_id),
    )
    if not all(path.exists() for path in required_paths):
        return False
    latest_curation_mtime = max(
        claim_curation_path(run_dir, round_id).stat().st_mtime_ns,
        observation_curation_path(run_dir, round_id).stat().st_mtime_ns,
    )
    earliest_materialized_mtime = min(
        claim_submissions_path(run_dir, round_id).stat().st_mtime_ns,
        observation_submissions_path(run_dir, round_id).stat().st_mtime_ns,
    )
    return earliest_materialized_mtime >= latest_curation_mtime


def render_openclaw_prompts(
    *,
    run_dir: Path,
    round_id: str,
) -> dict[str, Any]:
    outputs: dict[str, str] = {}
    claim_packet_path_file = claim_curation_packet_path(run_dir, round_id)
    claim_packet = load_json_if_exists(claim_packet_path_file)
    if isinstance(claim_packet, dict):
        prompt_path = claim_curation_prompt_path(run_dir, round_id)
        write_text(prompt_path, claim_curation_prompt_text(packet_path=claim_packet_path_file, packet=claim_packet))
        outputs["sociologist_claim_curation"] = str(prompt_path)
    observation_packet_path_file = observation_curation_packet_path(run_dir, round_id)
    observation_packet = load_json_if_exists(observation_packet_path_file)
    if isinstance(observation_packet, dict):
        prompt_path = observation_curation_prompt_path(run_dir, round_id)
        write_text(
            prompt_path,
            observation_curation_prompt_text(packet_path=observation_packet_path_file, packet=observation_packet),
        )
        outputs["environmentalist_observation_curation"] = str(prompt_path)
    for role in READINESS_ROLES:
        packet_path = data_readiness_packet_path(run_dir, round_id, role)
        packet = load_json_if_exists(packet_path)
        if isinstance(packet, dict):
            prompt_path = data_readiness_prompt_path(run_dir, round_id, role)
            write_text(prompt_path, data_readiness_prompt_text(role=role, packet_path=packet_path, packet=packet))
            outputs[f"{role}_data_readiness"] = str(prompt_path)
    auth_packet_path = matching_authorization_packet_path(run_dir, round_id)
    auth_packet = load_json_if_exists(auth_packet_path)
    if isinstance(auth_packet, dict):
        auth_prompt_path = matching_authorization_prompt_path(run_dir, round_id)
        write_text(auth_prompt_path, matching_authorization_prompt_text(packet_path=auth_packet_path, packet=auth_packet))
        outputs["moderator_matching_authorization"] = str(auth_prompt_path)
    adjudication_packet_path = matching_adjudication_packet_path(run_dir, round_id)
    adjudication_packet = load_json_if_exists(adjudication_packet_path)
    if isinstance(adjudication_packet, dict):
        adjudication_prompt = matching_adjudication_prompt_path(run_dir, round_id)
        write_text(
            adjudication_prompt,
            matching_adjudication_prompt_text(packet_path=adjudication_packet_path, packet=adjudication_packet),
        )
        outputs["moderator_matching_adjudication"] = str(adjudication_prompt)
    for role in REPORT_ROLES:
        packet_path = report_packet_path(run_dir, round_id, role)
        packet = load_json_if_exists(packet_path)
        if isinstance(packet, dict):
            prompt_path = report_prompt_path(run_dir, round_id, role)
            write_text(prompt_path, report_prompt_text(role=role, packet_path=packet_path, packet=packet))
            outputs[f"{role}_report"] = str(prompt_path)
    packet_path = decision_packet_path(run_dir, round_id)
    packet = load_json_if_exists(packet_path)
    if isinstance(packet, dict):
        moderator_prompt_path = decision_prompt_path(run_dir, round_id)
        write_text(moderator_prompt_path, decision_prompt_text(packet_path=packet_path, packet=packet))
        outputs["moderator_decision"] = str(moderator_prompt_path)
    if not outputs:
        raise ValueError(f"No curation, readiness, authorization, matching-adjudication, report, or decision packets exist for {round_id}.")
    return outputs


def data_readiness_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    if not curations_materialized_for_round(run_dir=run_dir, round_id=round_id, state=state):
        raise ValueError(
            "Data-readiness packets require completed claim/observation curation plus refreshed "
            "materialized submissions. Run normalize materialize-curations after both curation payloads are imported."
        )
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    max_findings = mission_constraints(mission).get("max_claims_per_round", 4)
    outputs: dict[str, dict[str, str]] = {}
    for role in READINESS_ROLES:
        context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role=role)
        draft_report = build_data_readiness_draft(
            mission=mission,
            round_id=round_id,
            role=role,
            state=state,
            max_findings=max_findings,
        )
        packet = build_data_readiness_packet(
            run_dir=run_dir,
            round_id=round_id,
            role=role,
            mission=mission,
            tasks=tasks,
            context=context,
            draft_report=draft_report,
        )
        packet_path = data_readiness_packet_path(run_dir, round_id, role)
        draft_path = data_readiness_draft_path(run_dir, round_id, role)
        write_json(packet_path, packet, pretty=pretty)
        write_json(draft_path, draft_report, pretty=pretty)
        outputs[role] = {
            "packet_path": str(packet_path),
            "draft_path": str(draft_path),
        }
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "claim_submission_count": len(state_auditable_submissions(state, "sociologist")),
        "observation_submission_count": len(state_auditable_submissions(state, "environmentalist")),
        "claim_submission_current_count": len(state_current_submissions(state, "sociologist")),
        "observation_submission_current_count": len(state_current_submissions(state, "environmentalist")),
        "outputs": outputs,
    }


def matching_authorization_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    draft_authorization = build_matching_authorization_draft(
        mission=mission,
        round_id=round_id,
        state=state,
    )
    packet = build_matching_authorization_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        context=context,
        state=state,
        draft_authorization=draft_authorization,
    )
    packet_path = matching_authorization_packet_path(run_dir, round_id)
    draft_path = matching_authorization_draft_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    write_json(draft_path, draft_authorization, pretty=pretty)
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "matching_authorization_packet_path": str(packet_path),
        "matching_authorization_draft_path": str(draft_path),
    }


def matching_adjudication_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    authorization = state.get("matching_authorization", {}) if isinstance(state.get("matching_authorization"), dict) else {}
    if maybe_text(authorization.get("authorization_status")) != "authorized":
        raise ValueError("Matching-adjudication packets require canonical matching_authorization.json with authorization_status=authorized.")
    authorization_id = maybe_text(authorization.get("authorization_id"))
    candidate_set = load_dict_if_exists(matching_candidate_set_path(run_dir, round_id))
    if not isinstance(candidate_set, dict):
        raise ValueError(
            "Matching candidate set is missing. Run normalize prepare-matching-adjudication after authorization before building the moderator adjudication packet."
        )
    if authorization_id and maybe_text(candidate_set.get("authorization_id")) != authorization_id:
        raise ValueError("Matching candidate set authorization_id does not match matching_authorization.json. Regenerate it.")
    draft_adjudication = load_dict_if_exists(matching_adjudication_draft_path(run_dir, round_id))
    if not isinstance(draft_adjudication, dict):
        raise ValueError(
            "Matching adjudication draft is missing. Run normalize prepare-matching-adjudication after authorization before building the moderator adjudication packet."
        )
    validate_payload("matching-adjudication", draft_adjudication)
    if authorization_id and maybe_text(draft_adjudication.get("authorization_id")) != authorization_id:
        raise ValueError("Matching adjudication draft authorization_id does not match matching_authorization.json. Regenerate it.")
    mission = state["mission"]
    context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    packet = build_matching_adjudication_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        context=context,
        state=state,
        candidate_set=candidate_set,
        draft_adjudication=draft_adjudication,
    )
    packet_path = matching_adjudication_packet_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "matching_candidate_set_path": str(matching_candidate_set_path(run_dir, round_id)),
        "matching_adjudication_packet_path": str(packet_path),
        "matching_adjudication_draft_path": str(matching_adjudication_draft_path(run_dir, round_id)),
    }


def report_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    if not matching_executed_for_state(state):
        raise ValueError(
            "Expert report packets require completed matching/adjudication artifacts. "
            "Use build-data-readiness-packets or build-decision-packet before matching, and build-report-packets only after run-matching-adjudication."
        )
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    max_findings = mission_constraints(mission).get("max_claims_per_round", 4)
    outputs: dict[str, dict[str, str]] = {}
    for role in REPORT_ROLES:
        context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role=role)
        draft_report = build_expert_report_draft_from_state(state=state, role=role, max_findings=max_findings)
        packet = build_report_packet(
            run_dir=run_dir,
            round_id=round_id,
            role=role,
            mission=mission,
            tasks=tasks,
            context=context,
            draft_report=draft_report,
        )
        packet_path = report_packet_path(run_dir, round_id, role)
        draft_path = report_draft_path(run_dir, round_id, role)
        write_json(packet_path, packet, pretty=pretty)
        write_json(draft_path, draft_report, pretty=pretty)
        outputs[role] = {"report_packet_path": str(packet_path), "report_draft_path": str(draft_path)}
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "claim_count": len(state.get("claims", [])),
        "observation_count": len(state.get("observations", [])),
        "evidence_count": len(state.get("cards_active", [])),
        "outputs": outputs,
    }


def decision_artifacts(
    *,
    run_dir: Path,
    round_id: str,
    next_round_id: str,
    pretty: bool,
    prefer_draft_reports: bool,
) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    reports: dict[str, dict[str, Any] | None] = {}
    report_sources: dict[str, str] = {}
    for role in REPORT_ROLES:
        report, source = load_report_for_decision(run_dir, round_id, role, prefer_drafts=prefer_draft_reports)
        reports[role] = report
        report_sources[role] = source
    moderator_context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    draft_decision, next_round_tasks, missing_types = build_decision_draft_from_state(
        run_dir=run_dir,
        state=state,
        next_round_id=next_round_id,
        reports=reports,
        report_sources=report_sources,
    )
    packet = build_decision_packet_from_state(
        run_dir=run_dir,
        state=state,
        next_round_id=next_round_id,
        moderator_context=moderator_context,
        reports=reports,
        report_sources=report_sources,
        draft_decision=draft_decision,
        proposed_next_round_tasks=next_round_tasks,
        missing_evidence_types=missing_types,
    )
    packet_path = decision_packet_path(run_dir, round_id)
    draft_path = decision_draft_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    write_json(draft_path, draft_decision, pretty=pretty)
    return {
        "run_id": mission_run_id(state["mission"]),
        "round_id": round_id,
        "next_round_id": next_round_id,
        "decision_packet_path": str(packet_path),
        "decision_draft_path": str(draft_path),
        "report_sources": report_sources,
        "missing_evidence_types": missing_types,
        "next_round_task_count": len(next_round_tasks),
    }


def command_build_report_packets(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return report_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_curation_packets(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return curation_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_data_readiness_packets(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return data_readiness_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_matching_authorization_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return matching_authorization_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_matching_adjudication_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return matching_adjudication_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_decision_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    next_round_id = args.next_round_id or next_round_id_for(args.round_id)
    return decision_artifacts(
        run_dir=run_dir,
        round_id=args.round_id,
        next_round_id=next_round_id,
        pretty=args.pretty,
        prefer_draft_reports=args.prefer_draft_reports,
    )


def command_build_all(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    next_round_id = args.next_round_id or next_round_id_for(args.round_id)
    outputs: dict[str, Any] = {
        "curation": curation_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty),
    }
    state = collect_round_state(run_dir, args.round_id)
    if curations_materialized_for_round(run_dir=run_dir, round_id=args.round_id, state=state):
        outputs["data_readiness"] = data_readiness_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            pretty=args.pretty,
        )
        state = collect_round_state(run_dir, args.round_id)
    else:
        outputs["data_readiness"] = {
            "skipped": True,
            "reason": (
                "Curation has not been materialized into claim/observation submissions yet; "
                "readiness packets were intentionally not built."
            ),
        }
    if all(isinstance(state.get("readiness_reports", {}).get(role), dict) and state.get("readiness_reports", {}).get(role) for role in READINESS_ROLES):
        outputs["matching_authorization"] = matching_authorization_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            pretty=args.pretty,
        )
        state = collect_round_state(run_dir, args.round_id)
    if maybe_text(state.get("matching_authorization", {}).get("authorization_status")) == "authorized":
        candidate_path = matching_candidate_set_path(run_dir, args.round_id)
        draft_path = matching_adjudication_draft_path(run_dir, args.round_id)
        if candidate_path.exists() and draft_path.exists():
            outputs["matching_adjudication"] = matching_adjudication_artifacts(
                run_dir=run_dir,
                round_id=args.round_id,
                pretty=args.pretty,
            )
    if matching_result_path(run_dir, args.round_id).exists() and evidence_adjudication_path(run_dir, args.round_id).exists():
        outputs["reports"] = report_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)
    authorization_status = maybe_text(state.get("matching_authorization", {}).get("authorization_status"))
    if authorization_status in {"deferred", "not-authorized"} or all(
        report_draft_path(run_dir, args.round_id, role).exists() or report_target_path(run_dir, args.round_id, role).exists()
        for role in REPORT_ROLES
    ):
        outputs["decision"] = decision_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            next_round_id=next_round_id,
            pretty=args.pretty,
            prefer_draft_reports=args.prefer_draft_reports,
        )
    return outputs


def command_render_openclaw_prompts(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    outputs = render_openclaw_prompts(run_dir=run_dir, round_id=args.round_id)
    return {
        "run_dir": str(run_dir),
        "round_id": args.round_id,
        "outputs": outputs,
    }


def command_promote_report_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_report_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        role=args.role,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_decision_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_decision_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_matching_authorization_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_matching_authorization_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_matching_adjudication_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_matching_adjudication_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_all(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    authorization_result = None
    adjudication_result = None
    if matching_authorization_draft_path(run_dir, args.round_id).exists():
        authorization_result = promote_matching_authorization_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    if matching_adjudication_draft_path(run_dir, args.round_id).exists():
        adjudication_result = promote_matching_adjudication_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    report_results = []
    for role in REPORT_ROLES:
        if report_draft_path(run_dir, args.round_id, role).exists():
            report_results.append(
                promote_report_draft(
                    run_dir=run_dir,
                    round_id=args.round_id,
                    role=role,
                    draft_path_text="",
                    pretty=args.pretty,
                    allow_overwrite=args.allow_overwrite,
                )
            )
    decision_result = None
    if decision_draft_path(run_dir, args.round_id).exists():
        decision_result = promote_decision_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    bundle_result = validate_bundle(run_dir)
    return {
        "run_dir": str(run_dir),
        "round_id": args.round_id,
        "matching_authorization_result": authorization_result,
        "matching_adjudication_result": adjudication_result,
        "report_results": report_results,
        "decision_result": decision_result,
        "bundle_validation": bundle_result,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build eco-council curation, readiness, report packets, and decision drafts.")
    sub = parser.add_subparsers(dest="command", required=True)

    curation_packets = sub.add_parser("build-curation-packets", help="Build claim/observation curation packets and draft curations.")
    curation_packets.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    curation_packets.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(curation_packets)

    readiness_packets = sub.add_parser("build-data-readiness-packets", help="Build data-readiness packets and draft readiness reports.")
    readiness_packets.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    readiness_packets.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(readiness_packets)

    authorization_packet = sub.add_parser("build-matching-authorization-packet", help="Build moderator matching-authorization packet and draft.")
    authorization_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    authorization_packet.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(authorization_packet)

    adjudication_packet = sub.add_parser("build-matching-adjudication-packet", help="Build moderator matching-adjudication packet from the nominated candidate set and draft.")
    adjudication_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    adjudication_packet.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(adjudication_packet)

    report_packets = sub.add_parser("build-report-packets", help="Build expert report packets and draft expert reports.")
    report_packets.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    report_packets.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(report_packets)

    decision_packet = sub.add_parser("build-decision-packet", help="Build moderator decision packet and decision draft.")
    decision_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    decision_packet.add_argument("--round-id", required=True, help="Round identifier.")
    decision_packet.add_argument("--next-round-id", default="", help="Optional explicit next round identifier.")
    decision_packet.add_argument("--prefer-draft-reports", action="store_true", help="Prefer derived report drafts over canonical expert reports whenever drafts are present.")
    add_pretty_flag(decision_packet)

    build_all = sub.add_parser("build-all", help="Build expert report packets and moderator decision packet together.")
    build_all.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    build_all.add_argument("--round-id", required=True, help="Round identifier.")
    build_all.add_argument("--next-round-id", default="", help="Optional explicit next round identifier.")
    build_all.add_argument("--prefer-draft-reports", action="store_true", help="Prefer derived report drafts over canonical expert reports whenever drafts are present.")
    add_pretty_flag(build_all)

    render_prompts = sub.add_parser("render-openclaw-prompts", help="Render OpenClaw text prompts from existing curation, readiness, matching, report, and decision packets.")
    render_prompts.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    render_prompts.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(render_prompts)

    promote_report = sub.add_parser("promote-report-draft", help="Promote one draft expert-report into the canonical report path.")
    promote_report.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_report.add_argument("--round-id", required=True, help="Round identifier.")
    promote_report.add_argument("--role", required=True, choices=PROMOTABLE_REPORT_ROLES, help="Expert role.")
    promote_report.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_report.add_argument("--allow-overwrite", action="store_true", help="Allow overwrite of an existing non-placeholder canonical report.")
    add_pretty_flag(promote_report)

    promote_decision = sub.add_parser("promote-decision-draft", help="Promote one draft council-decision into the canonical moderator path.")
    promote_decision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_decision.add_argument("--round-id", required=True, help="Round identifier.")
    promote_decision.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_decision.add_argument("--allow-overwrite", action="store_true", help="Allow overwrite of an existing canonical decision.")
    add_pretty_flag(promote_decision)

    promote_matching_authorization = sub.add_parser(
        "promote-matching-authorization-draft",
        help="Promote the moderator matching-authorization draft into the canonical moderator path.",
    )
    promote_matching_authorization.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_matching_authorization.add_argument("--round-id", required=True, help="Round identifier.")
    promote_matching_authorization.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_matching_authorization.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow overwrite of an existing canonical matching-authorization.",
    )
    add_pretty_flag(promote_matching_authorization)

    promote_matching_adjudication = sub.add_parser(
        "promote-matching-adjudication-draft",
        help="Promote the moderator matching-adjudication draft into the canonical moderator path.",
    )
    promote_matching_adjudication.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_matching_adjudication.add_argument("--round-id", required=True, help="Round identifier.")
    promote_matching_adjudication.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_matching_adjudication.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow overwrite of an existing canonical matching-adjudication.",
    )
    add_pretty_flag(promote_matching_adjudication)

    promote_all = sub.add_parser("promote-all", help="Promote derived expert-report drafts plus the moderator decision draft into canonical paths.")
    promote_all.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_all.add_argument("--round-id", required=True, help="Round identifier.")
    promote_all.add_argument("--allow-overwrite", action="store_true", help="Allow overwrite of existing canonical outputs.")
    add_pretty_flag(promote_all)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "build-curation-packets": command_build_curation_packets,
        "build-data-readiness-packets": command_build_data_readiness_packets,
        "build-matching-authorization-packet": command_build_matching_authorization_packet,
        "build-matching-adjudication-packet": command_build_matching_adjudication_packet,
        "build-report-packets": command_build_report_packets,
        "build-decision-packet": command_build_decision_packet,
        "build-all": command_build_all,
        "render-openclaw-prompts": command_render_openclaw_prompts,
        "promote-report-draft": command_promote_report_draft,
        "promote-decision-draft": command_promote_decision_draft,
        "promote-matching-authorization-draft": command_promote_matching_authorization_draft,
        "promote-matching-adjudication-draft": command_promote_matching_adjudication_draft,
        "promote-all": command_promote_all,
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
