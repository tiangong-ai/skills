#!/usr/bin/env python3
"""Deterministic replay and evaluation runner for eco-council fixtures."""

from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_DIR = SKILL_DIR.parent
NORMALIZE_SCRIPT = REPO_DIR / "eco-council-normalize" / "scripts" / "eco_council_normalize.py"
REPORTING_SCRIPT = REPO_DIR / "eco-council-reporting" / "scripts" / "eco_council_reporting.py"
CONTRACT_SCRIPT = REPO_DIR / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"
DEFAULT_SUITE_DIR = SKILL_DIR / "assets" / "eval-cases"
DEFAULT_OUTPUT_ROOT = Path("/tmp/eco-council-eval-runs")
SCHEMA_VERSION = "1.0.0"


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(pretty_json(payload, pretty=pretty) + "\n", encoding="utf-8")


def round_directory_name(round_id: str) -> str:
    return round_id.replace("-", "_")


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_directory_name(round_id)


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "source_selection.json"


def claim_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "claim_submissions.json"


def observation_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "observation_submissions.json"


def data_readiness_report_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "data_readiness_report.json"


def data_readiness_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_data_readiness_draft.json"


def matching_authorization_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_authorization.json"


def matching_authorization_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_authorization_draft.json"


def claims_active_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library" / "claims_active.json"


def observations_active_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library" / "observations_active.json"


def cards_active_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library" / "cards_active.json"


def isolated_active_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library" / "isolated_active.json"


def remands_open_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library" / "remands_open.json"


def promote_json_artifact(*, source_path: Path, target_path: Path, pretty: bool) -> None:
    payload = read_json(source_path)
    write_json(target_path, payload, pretty=pretty)


def unique_strings(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def load_python_module(module_path: Path, module_name: str) -> Any | None:
    if not module_path.exists():
        return None
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT_MODULE = load_python_module(CONTRACT_SCRIPT, "eco_council_contract_eval")
NORMALIZE_MODULE = load_python_module(NORMALIZE_SCRIPT, "eco_council_normalize_eval")
if CONTRACT_MODULE is not None and hasattr(CONTRACT_MODULE, "SCHEMA_VERSION"):
    SCHEMA_VERSION = CONTRACT_MODULE.SCHEMA_VERSION


def seed_library_state_from_case(case: dict[str, Any], run_dir: Path, round_id: str, *, pretty: bool) -> None:
    if NORMALIZE_MODULE is None:
        raise ValueError("Unable to load eco-council-normalize module for eval replay.")
    claims = [item for item in case.get("claims", []) if isinstance(item, dict)]
    observations = [item for item in case.get("observations", []) if isinstance(item, dict)]
    evidence_cards = [item for item in case.get("evidence_cards", []) if isinstance(item, dict)]
    isolated_entries = [item for item in case.get("isolated_entries", []) if isinstance(item, dict)]
    remand_entries = [item for item in case.get("remand_entries", []) if isinstance(item, dict)]

    claim_submissions = [NORMALIZE_MODULE.claim_submission_from_claim(item) for item in claims]
    observation_submissions = [NORMALIZE_MODULE.observation_submission_from_observation(item) for item in observations]

    write_json(claim_submissions_path(run_dir, round_id), claim_submissions, pretty=pretty)
    write_json(observation_submissions_path(run_dir, round_id), observation_submissions, pretty=pretty)
    write_json(claims_active_path(run_dir, round_id), claim_submissions, pretty=pretty)
    write_json(observations_active_path(run_dir, round_id), observation_submissions, pretty=pretty)
    write_json(cards_active_path(run_dir, round_id), evidence_cards, pretty=pretty)
    write_json(isolated_active_path(run_dir, round_id), isolated_entries, pretty=pretty)
    write_json(remands_open_path(run_dir, round_id), remand_entries, pretty=pretty)


def contract_call(name: str, *args: Any) -> Any | None:
    if CONTRACT_MODULE is None or not hasattr(CONTRACT_MODULE, name):
        return None
    helper = getattr(CONTRACT_MODULE, name)
    return helper(*args)


def extract_json_suffix(text: str) -> Any:
    clean = text.strip()
    if not clean:
        raise ValueError("Expected JSON output but command returned nothing.")
    for index, char in enumerate(clean):
        if char not in "[{":
            continue
        candidate = clean[index:]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise ValueError(f"Command output did not contain parseable JSON:\n{clean}")


def run_json_command(argv: list[str]) -> Any:
    completed = subprocess.run(
        argv,
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            + " ".join(argv)
            + "\nSTDOUT:\n"
            + completed.stdout
            + "\nSTDERR:\n"
            + completed.stderr
        )
    return extract_json_suffix(completed.stdout)


def load_case(case_path: Path) -> dict[str, Any]:
    payload = read_json(case_path)
    if not isinstance(payload, dict):
        raise ValueError(f"Case fixture must be a JSON object: {case_path}")
    return payload


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    values = contract_call("allowed_sources_for_role", mission, role)
    if isinstance(values, list):
        return unique_strings([maybe_text(item) for item in values if maybe_text(item)])
    return []


def tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def role_source_governance(mission: dict[str, Any], role: str) -> list[dict[str, Any]]:
    families = contract_call("source_governance_for_role", mission, role)
    if isinstance(families, list):
        return [item for item in families if isinstance(item, dict)]
    return []


def approved_layer_lookup(mission: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    value = contract_call("approved_layer_lookup", mission)
    if not isinstance(value, dict):
        return {}
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for key, item in value.items():
        if not isinstance(key, tuple) or len(key) != 2 or not isinstance(item, dict):
            continue
        family_id = maybe_text(key[0])
        layer_id = maybe_text(key[1])
        if family_id and layer_id:
            output[(family_id, layer_id)] = item
    return output


def task_requirement_ids(tasks: list[dict[str, Any]]) -> list[str]:
    requirement_ids: list[str] = []
    for task in tasks:
        inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
        values = inputs.get("evidence_requirements")
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, dict) and maybe_text(item.get("requirement_id")):
                requirement_ids.append(maybe_text(item.get("requirement_id")))
    return unique_strings(requirement_ids)


def status_selected_sources(statuses: list[dict[str, Any]], role: str) -> list[str]:
    selected: list[str] = []
    for status in statuses:
        if not isinstance(status, dict):
            continue
        if maybe_text(status.get("assigned_role")) != role:
            continue
        if maybe_text(status.get("status")) != "completed":
            continue
        source_skill = maybe_text(status.get("source_skill")) or maybe_text(status.get("source"))
        if source_skill:
            selected.append(source_skill)
    return unique_strings(selected)


def selected_sources_by_family(family: dict[str, Any], selected_lookup: set[str]) -> dict[str, list[str]]:
    by_layer: dict[str, list[str]] = {}
    layers = family.get("layers")
    if not isinstance(layers, list):
        return by_layer
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        layer_id = maybe_text(layer.get("layer_id"))
        if not layer_id:
            continue
        skills = layer.get("skills")
        if not isinstance(skills, list):
            continue
        chosen = [maybe_text(skill) for skill in skills if maybe_text(skill) and maybe_text(skill).casefold() in selected_lookup]
        by_layer[layer_id] = unique_strings(chosen)
    return by_layer


def build_family_plans(
    *,
    mission: dict[str, Any],
    role: str,
    round_id: str,
    selected_sources: list[str],
    evidence_requirement_ids: list[str],
) -> list[dict[str, Any]]:
    selected_lookup = {source.casefold() for source in selected_sources}
    families = role_source_governance(mission, role)
    approved_lookup = approved_layer_lookup(mission)
    plans: list[dict[str, Any]] = []

    for family in families:
        family_id = maybe_text(family.get("family_id"))
        if not family_id:
            continue
        by_layer = selected_sources_by_family(family, selected_lookup)
        selected_l1_layers = [
            maybe_text(layer.get("layer_id"))
            for layer in family.get("layers", [])
            if isinstance(layer, dict)
            and maybe_text(layer.get("tier")) == "l1"
            and by_layer.get(maybe_text(layer.get("layer_id")))
        ]
        selected_l1_layers = [item for item in selected_l1_layers if item]

        layer_plans: list[dict[str, Any]] = []
        family_selected = False
        for layer in family.get("layers", []):
            if not isinstance(layer, dict):
                continue
            layer_id = maybe_text(layer.get("layer_id"))
            tier = maybe_text(layer.get("tier"))
            layer_skills = by_layer.get(layer_id, [])
            selected = bool(layer_skills)
            if selected:
                family_selected = True
            anchor_mode = "none"
            anchor_refs: list[str] = []
            if selected and tier == "l2":
                if selected_l1_layers:
                    anchor_mode = "same_round_l1"
                    anchor_refs = [f"{round_id}:family:{family_id}:{selected_l1_layers[0]}"]
                else:
                    anchor_mode = "upstream_approval"
                    anchor_refs = [f"mission:approval:{family_id}:{layer_id}"]
            if selected:
                if tier == "l1":
                    authorization_basis = "entry-layer"
                else:
                    approval = approved_lookup.get((family_id, layer_id), {})
                    if approval:
                        authorization_basis = "upstream-approval"
                    elif layer.get("auto_selectable") is True:
                        authorization_basis = "policy-auto"
                    else:
                        authorization_basis = "not-authorized"
            else:
                authorization_basis = "entry-layer" if tier == "l1" else "not-authorized"
            reason = (
                f"Replay fixture fetched {', '.join(layer_skills)}."
                if selected
                else f"Replay fixture did not fetch any {family_id}.{layer_id} skills."
            )
            layer_plans.append(
                {
                    "layer_id": layer_id,
                    "tier": tier,
                    "selected": selected,
                    "reason": reason,
                    "source_skills": layer_skills if selected else [maybe_text(skill) for skill in layer.get("skills", []) if maybe_text(skill)],
                    "anchor_mode": anchor_mode,
                    "anchor_refs": anchor_refs,
                    "authorization_basis": authorization_basis,
                }
            )
        plans.append(
            {
                "family_id": family_id,
                "selected": family_selected,
                "reason": (
                    f"Replay fixture selected {family_id} sources from completed fetch statuses."
                    if family_selected
                    else f"Replay fixture did not use the {family_id} family."
                ),
                "evidence_requirement_ids": evidence_requirement_ids if family_selected else [],
                "layer_plans": layer_plans,
            }
        )
    return plans


def default_source_selection(
    *,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    fetch_statuses: list[dict[str, Any]],
    round_id: str,
    role: str,
) -> dict[str, Any]:
    run_id = maybe_text(mission.get("run_id"))
    role_tasks = tasks_for_role(tasks, role)
    allowed_sources = allowed_sources_for_role(mission, role)
    selected_lookup = {source.casefold() for source in status_selected_sources(fetch_statuses, role)}
    selected_sources = [source for source in allowed_sources if source.casefold() in selected_lookup]
    evidence_requirement_ids = task_requirement_ids(role_tasks)
    family_plans = build_family_plans(
        mission=mission,
        role=role,
        round_id=round_id,
        selected_sources=selected_sources,
        evidence_requirement_ids=evidence_requirement_ids,
    )
    summary = (
        f"Replay fixture selected {', '.join(selected_sources)}."
        if selected_sources
        else "Replay fixture intentionally selected no sources for this role."
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "selection_id": f"source-selection-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "complete",
        "summary": summary,
        "task_ids": [maybe_text(task.get("task_id")) for task in role_tasks if maybe_text(task.get("task_id"))],
        "allowed_sources": allowed_sources,
        "selected_sources": selected_sources,
        "override_requests": [],
        "family_plans": family_plans,
        "source_decisions": [
            {
                "source_skill": source,
                "selected": source.casefold() in selected_lookup,
                "reason": (
                    "Selected by replay fixture completed fetch statuses."
                    if source.casefold() in selected_lookup
                    else "Not selected by replay fixture completed fetch statuses."
                ),
            }
            for source in allowed_sources
        ],
    }


def materialize_case(case: dict[str, Any], run_dir: Path, *, pretty: bool) -> str:
    mission = case.get("mission")
    if not isinstance(mission, dict):
        raise ValueError("Case fixture missing mission.")
    round_id = maybe_text(case.get("round_id"))
    if not round_id:
        raise ValueError("Case fixture missing round_id.")

    write_json(run_dir / "mission.json", mission, pretty=pretty)
    base_round = round_dir(run_dir, round_id)
    tasks = case.get("tasks", [])
    write_json(base_round / "moderator" / "tasks.json", tasks, pretty=pretty)
    write_json(base_round / "shared" / "claims.json", case.get("claims", []), pretty=pretty)
    write_json(base_round / "shared" / "observations.json", case.get("observations", []), pretty=pretty)
    write_json(base_round / "shared" / "evidence_cards.json", case.get("evidence_cards", []), pretty=pretty)
    fetch_statuses = case.get("fetch_statuses") if isinstance(case.get("fetch_statuses"), list) else []
    source_selections = case.get("source_selections") if isinstance(case.get("source_selections"), dict) else {}
    task_dicts = [item for item in tasks if isinstance(item, dict)]
    for role in ("sociologist", "environmentalist"):
        payload = source_selections.get(role) if isinstance(source_selections, dict) else None
        if not isinstance(payload, dict):
            payload = default_source_selection(
                mission=mission,
                tasks=task_dicts,
                fetch_statuses=[item for item in fetch_statuses if isinstance(item, dict)],
                round_id=round_id,
                role=role,
            )
        write_json(source_selection_path(run_dir, round_id, role), payload, pretty=pretty)
    write_json(
        base_round / "moderator" / "derived" / "fetch_execution.json",
        {"statuses": fetch_statuses},
        pretty=pretty,
    )
    return round_id


def evaluate_expectations(case: dict[str, Any], run_dir: Path, round_id: str) -> list[str]:
    issues: list[str] = []
    expect = case.get("expect")
    if not isinstance(expect, dict):
        return issues
    decision = read_json(round_dir(run_dir, round_id) / "moderator" / "council_decision.json")

    decision_expect = expect.get("decision")
    if isinstance(decision_expect, dict):
        for key, expected in decision_expect.items():
            actual = decision.get(key)
            if isinstance(expected, list):
                actual_list = actual if isinstance(actual, list) else []
                if sorted(maybe_text(item) for item in actual_list) != sorted(maybe_text(item) for item in expected):
                    issues.append(f"decision.{key}: expected {expected!r}, got {actual!r}")
            else:
                if actual != expected:
                    issues.append(f"decision.{key}: expected {expected!r}, got {actual!r}")

    context_expect = expect.get("context")
    if isinstance(context_expect, dict):
        context = read_json(round_dir(run_dir, round_id) / "moderator" / "derived" / "context_moderator.json")
        if "context_layer" in context_expect and context.get("context_layer") != context_expect["context_layer"]:
            issues.append(
                f"context.context_layer: expected {context_expect['context_layer']!r}, got {context.get('context_layer')!r}"
            )
        max_observations = context_expect.get("max_observations")
        if isinstance(max_observations, int) and len(context.get("observations", [])) > max_observations:
            issues.append(
                f"context.observations length expected <= {max_observations}, got {len(context.get('observations', []))}"
            )
        max_claims = context_expect.get("max_claims")
        if isinstance(max_claims, int) and len(context.get("claims", [])) > max_claims:
            issues.append(f"context.claims length expected <= {max_claims}, got {len(context.get('claims', []))}")

    next_round_tasks_expect = expect.get("next_round_tasks")
    if isinstance(next_round_tasks_expect, list):
        actual_tasks = decision.get("next_round_tasks")
        if not isinstance(actual_tasks, list):
            actual_tasks = []
        for index, expected_task in enumerate(next_round_tasks_expect):
            if not isinstance(expected_task, dict):
                issues.append(f"expect.next_round_tasks[{index}] must be an object.")
                continue
            if not any(task_contains_expected_subset(actual_task, expected_task) for actual_task in actual_tasks):
                issues.append(f"next_round_tasks missing expected subset at index {index}: {expected_task!r}")

    forbidden_fields_expect = expect.get("next_round_tasks_forbidden_fields")
    if isinstance(forbidden_fields_expect, list):
        actual_tasks = decision.get("next_round_tasks")
        if not isinstance(actual_tasks, list):
            actual_tasks = []
        for index, item in enumerate(forbidden_fields_expect):
            if not isinstance(item, dict):
                issues.append(f"expect.next_round_tasks_forbidden_fields[{index}] must be an object.")
                continue
            match = item.get("match")
            fields = item.get("fields")
            if not isinstance(match, dict):
                issues.append(f"expect.next_round_tasks_forbidden_fields[{index}].match must be an object.")
                continue
            if not isinstance(fields, list) or not all(isinstance(field, str) and field.strip() for field in fields):
                issues.append(f"expect.next_round_tasks_forbidden_fields[{index}].fields must be a non-empty string list.")
                continue
            matched_tasks = [task for task in actual_tasks if task_contains_expected_subset(task, match)]
            if not matched_tasks:
                issues.append(f"next_round_tasks_forbidden_fields[{index}] matched no tasks: {match!r}")
                continue
            for field_path in fields:
                offenders = [task for task in matched_tasks if has_nested_field(task, field_path)]
                if offenders:
                    issues.append(
                        f"next_round_tasks_forbidden_fields[{index}] expected {field_path!r} to be absent for tasks matching {match!r}"
                    )
    return issues


def task_contains_expected_subset(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        return all(key in actual and task_contains_expected_subset(actual[key], value) for key, value in expected.items())
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return False
        if expected and all(isinstance(item, dict) for item in expected):
            remaining = list(actual)
            for expected_item in expected:
                match_index = next(
                    (idx for idx, candidate in enumerate(remaining) if task_contains_expected_subset(candidate, expected_item)),
                    None,
                )
                if match_index is None:
                    return False
                remaining.pop(match_index)
            return True
        return actual == expected
    return actual == expected


def has_nested_field(payload: Any, dotted_path: str) -> bool:
    current = payload
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def run_case(case_path: Path, *, output_root: Path, pretty: bool, overwrite: bool) -> dict[str, Any]:
    case = load_case(case_path)
    case_id = maybe_text(case.get("case_id")) or case_path.stem
    run_dir_path = output_root / case_id
    if run_dir_path.exists():
        if not overwrite:
            raise ValueError(f"Case output already exists: {run_dir_path}")
        shutil.rmtree(run_dir_path)
    run_dir_path.mkdir(parents=True, exist_ok=True)

    round_id = materialize_case(case, run_dir_path, pretty=pretty)
    run_json_command(["python3", str(NORMALIZE_SCRIPT), "init-run", "--run-dir", str(run_dir_path), "--round-id", round_id, "--pretty"])
    seed_library_state_from_case(case, run_dir_path, round_id, pretty=pretty)
    run_json_command(["python3", str(NORMALIZE_SCRIPT), "build-round-context", "--run-dir", str(run_dir_path), "--round-id", round_id, "--pretty"])
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "build-data-readiness-packets",
            "--run-dir",
            str(run_dir_path),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    for role in ("sociologist", "environmentalist"):
        promote_json_artifact(
            source_path=data_readiness_draft_path(run_dir_path, round_id, role),
            target_path=data_readiness_report_path(run_dir_path, round_id, role),
            pretty=pretty,
        )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "build-matching-authorization-packet",
            "--run-dir",
            str(run_dir_path),
            "--round-id",
            round_id,
            "--pretty",
        ]
    )
    promote_json_artifact(
        source_path=matching_authorization_draft_path(run_dir_path, round_id),
        target_path=matching_authorization_path(run_dir_path, round_id),
        pretty=pretty,
    )
    if (
        read_json(cards_active_path(run_dir_path, round_id))
        or read_json(isolated_active_path(run_dir_path, round_id))
        or read_json(remands_open_path(run_dir_path, round_id))
    ):
        run_json_command(
            [
                "python3",
                str(REPORTING_SCRIPT),
                "build-report-packets",
                "--run-dir",
                str(run_dir_path),
                "--round-id",
                round_id,
                "--pretty",
            ]
        )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "build-decision-packet",
            "--run-dir",
            str(run_dir_path),
            "--round-id",
            round_id,
            "--prefer-draft-reports",
            "--pretty",
        ]
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "promote-all",
            "--run-dir",
            str(run_dir_path),
            "--round-id",
            round_id,
            "--allow-overwrite",
            "--pretty",
        ]
    )
    bundle = run_json_command(["python3", str(CONTRACT_SCRIPT), "validate-bundle", "--run-dir", str(run_dir_path)])
    issues = evaluate_expectations(case, run_dir_path, round_id)
    return {
        "case_id": case_id,
        "description": maybe_text(case.get("description")),
        "round_id": round_id,
        "run_dir": str(run_dir_path),
        "bundle_ok": bool(bundle.get("ok")) if isinstance(bundle, dict) else False,
        "issues": issues,
        "passed": (bool(bundle.get("ok")) if isinstance(bundle, dict) else False) and not issues,
    }


def collect_case_paths(suite_dir: Path, case_id: str) -> list[Path]:
    cases = sorted(path for path in suite_dir.glob("*.json") if path.is_file())
    if case_id:
        cases = [path for path in cases if path.stem == case_id]
    if not cases:
        raise ValueError(f"No case fixtures found in {suite_dir} for case_id={case_id!r}.")
    return cases


def command_run_suite(args: argparse.Namespace) -> dict[str, Any]:
    suite_dir = Path(args.suite_dir).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    results = [
        run_case(path, output_root=output_root, pretty=args.pretty, overwrite=args.overwrite)
        for path in collect_case_paths(suite_dir, args.case_id)
    ]
    passed = sum(1 for item in results if item.get("passed"))
    return {
        "suite_dir": str(suite_dir),
        "output_root": str(output_root),
        "case_count": len(results),
        "passed_count": passed,
        "failed_count": len(results) - passed,
        "results": results,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay deterministic eco-council eval fixtures.")
    sub = parser.add_subparsers(dest="command", required=True)

    run_suite = sub.add_parser("run-suite", help="Run all eval fixtures in a suite directory.")
    run_suite.add_argument("--suite-dir", default=str(DEFAULT_SUITE_DIR), help="Eval fixture directory.")
    run_suite.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root for replayed runs.")
    run_suite.add_argument("--case-id", default="", help="Optional single case id to run.")
    run_suite.add_argument("--overwrite", action="store_true", help="Overwrite existing replay output directories.")
    run_suite.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {"run-suite": command_run_suite}
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"command": args.command, "ok": False, "error": str(exc)}, pretty=getattr(args, "pretty", False)))
        return 1
    print(pretty_json({"command": args.command, "ok": True, "payload": payload}, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
