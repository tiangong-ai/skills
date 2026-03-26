#!/usr/bin/env python3
"""Local SQLite case library for eco-council historical runs."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_DIR = SKILL_DIR.parent
DDL_PATH = SKILL_DIR / "assets" / "sqlite" / "eco_council_case_library.sql"
SUPERVISOR_PATH = SKILL_DIR / "scripts" / "eco_council_supervisor.py"
SEARCH_TOKEN_RE = re.compile(r"[a-z0-9]{2,}")
CASE_ID_SAFE_RE = re.compile(r"[^a-z0-9._-]+")
SEARCH_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "that",
    "this",
    "these",
    "those",
    "over",
    "under",
    "after",
    "before",
    "about",
    "across",
    "around",
    "within",
    "focus",
    "issue",
    "signal",
    "signals",
    "report",
    "reports",
    "council",
    "mission",
    "check",
    "checks",
    "claim",
    "claims",
    "supported",
    "support",
    "complete",
    "completed",
    "close",
    "closes",
    "closure",
    "clean",
    "cleanly",
    "verification",
    "replay",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(pretty_json(payload, pretty=pretty) + "\n", encoding="utf-8")


def load_supervisor_module() -> Any:
    module_name = "eco_council_supervisor_case_library"
    spec = importlib.util.spec_from_file_location(module_name, SUPERVISOR_PATH)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load supervisor module from {SUPERVISOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


SUP = load_supervisor_module()


def read_ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8")


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    if column_name in table_columns(conn, table_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def migrate_db(conn: sqlite3.Connection) -> None:
    # Older archive DBs predate source_governance_json; add it lazily so import-run
    # can overwrite into existing archives instead of failing on a missing column.
    ensure_column(conn, "cases", "source_governance_json", "TEXT NOT NULL DEFAULT '{}'")


def init_db(path: Path) -> None:
    with connect_db(path) as conn:
        conn.executescript(read_ddl())
        migrate_db(conn)
        conn.commit()


def state_for_run(run_dir: Path) -> dict[str, Any]:
    state_path = SUP.supervisor_state_path(run_dir)
    if state_path.exists():
        return SUP.load_state(run_dir)
    round_ids = SUP.discover_round_ids(run_dir)
    return {
        "current_round_id": round_ids[-1] if round_ids else "",
        "stage": "",
    }


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def parse_json_text(raw: Any, *, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, type(default)) else default


def normalized_text(value: Any) -> str:
    return maybe_text(value).lower()


def search_terms(*values: Any) -> list[str]:
    tokens: set[str] = set()
    fallback: set[str] = set()
    for value in values:
        text = normalized_text(value)
        if not text:
            continue
        fallback.update(part for part in text.split() if len(part) >= 2)
        for token in SEARCH_TOKEN_RE.findall(text):
            if token not in SEARCH_STOPWORDS:
                tokens.add(token)
    return sorted(tokens or fallback)


def safe_case_filename(case_id: str) -> str:
    safe = CASE_ID_SAFE_RE.sub("-", maybe_text(case_id).lower()).strip("-")
    return safe or "eco-council-case"


def sufficiency_rank(value: str) -> int:
    return {"sufficient": 3, "partial": 2, "insufficient": 1}.get(maybe_text(value), 0)


def moderator_status_rank(value: str) -> int:
    return {"complete": 3, "supported": 2, "blocked": 1}.get(maybe_text(value), 0)


def mission_constraints(mission: dict[str, Any]) -> dict[str, Any]:
    values = SUP.contract_call("effective_constraints", mission)
    if isinstance(values, dict):
        return values
    constraints = mission.get("constraints")
    return constraints if isinstance(constraints, dict) else {}


def public_source_skills(claim: dict[str, Any]) -> list[str]:
    refs = claim.get("public_refs")
    if not isinstance(refs, list):
        return []
    values = []
    for ref in refs:
        if isinstance(ref, dict):
            text = maybe_text(ref.get("source_skill"))
            if text:
                values.append(text)
    return sorted(set(values))


def round_payload_lists(run_dir: Path, round_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    claims = SUP.load_json_if_exists(SUP.shared_claims_path(run_dir, round_id))
    observations = SUP.load_json_if_exists(SUP.shared_observations_path(run_dir, round_id))
    evidence = SUP.load_json_if_exists(SUP.shared_evidence_cards_path(run_dir, round_id))
    return (
        claims if isinstance(claims, list) else [],
        observations if isinstance(observations, list) else [],
        evidence if isinstance(evidence, list) else [],
    )


def collect_run_snapshot(run_dir: Path) -> dict[str, Any]:
    mission = SUP.read_json(SUP.mission_path(run_dir))
    if not isinstance(mission, dict):
        raise ValueError(f"Invalid mission.json in {run_dir}")
    state = state_for_run(run_dir)
    round_ids = SUP.discover_round_ids(run_dir)
    round_summaries = [SUP.collect_round_summary(run_dir, state, round_id) for round_id in round_ids]
    current_round_id = maybe_text(state.get("current_round_id")) or (round_ids[-1] if round_ids else "")
    current_summary = next((item for item in round_summaries if item.get("round_id") == current_round_id), None)
    if current_summary is None and round_summaries:
        current_summary = round_summaries[-1]

    latest_decision_round = next(
        (item for item in reversed(round_summaries) if isinstance(item.get("decision"), dict)),
        None,
    )
    latest_decision = latest_decision_round.get("decision") if isinstance(latest_decision_round, dict) else None

    return {
        "mission": mission,
        "state": state,
        "round_ids": round_ids,
        "round_summaries": round_summaries,
        "current_summary": current_summary if isinstance(current_summary, dict) else {},
        "latest_decision_round": latest_decision_round if isinstance(latest_decision_round, dict) else {},
        "latest_decision": latest_decision if isinstance(latest_decision, dict) else {},
    }


def insert_case(conn: sqlite3.Connection, snapshot: dict[str, Any], run_dir: Path) -> str:
    mission = snapshot["mission"]
    state = snapshot["state"]
    current_summary = snapshot["current_summary"]
    latest_decision_round = snapshot["latest_decision_round"]
    latest_decision = snapshot["latest_decision"]
    constraints = mission_constraints(mission)
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    case_id = maybe_text(mission.get("run_id"))
    if not case_id:
        raise ValueError("mission.run_id is required")

    governance_json = json_text(SUP.contract_call("source_governance", mission) or {})
    case_values = {
        "case_id": case_id,
        "run_dir": str(run_dir),
        "topic": maybe_text(mission.get("topic")),
        "objective": maybe_text(mission.get("objective")),
        "region_label": maybe_text(region.get("label")),
        "region_geometry_json": json_text(region.get("geometry", {})),
        "window_start_utc": maybe_text(window.get("start_utc")),
        "window_end_utc": maybe_text(window.get("end_utc")),
        "max_rounds": constraints.get("max_rounds"),
        "max_claims_per_round": constraints.get("max_claims_per_round"),
        "max_tasks_per_round": constraints.get("max_tasks_per_round"),
        "source_governance_json": governance_json,
        "source_policy_json": governance_json,
        "current_round_id": maybe_text(state.get("current_round_id")),
        "current_stage": maybe_text(state.get("stage")),
        "round_count": len(snapshot["round_ids"]),
        "latest_decision_round_id": maybe_text(latest_decision_round.get("round_id")),
        "final_moderator_status": maybe_text(latest_decision.get("moderator_status")),
        "final_evidence_sufficiency": maybe_text(latest_decision.get("evidence_sufficiency")),
        "final_decision_summary": maybe_text(latest_decision.get("decision_summary")),
        "final_brief": maybe_text(latest_decision.get("final_brief")),
        "final_missing_evidence_types_json": json_text(latest_decision.get("missing_evidence_types", [])),
        "latest_claim_count": int(current_summary.get("shared", {}).get("claim_count") or 0),
        "latest_observation_count": int(current_summary.get("shared", {}).get("observation_count") or 0),
        "latest_evidence_count": int(current_summary.get("shared", {}).get("evidence_count") or 0),
        "imported_at_utc": utc_now_iso(),
        "mission_json": json_text(mission),
    }
    actual_columns = table_columns(conn, "cases")
    ordered_columns = [column for column in case_values if column in actual_columns]
    placeholders = ", ".join("?" for _ in ordered_columns)
    conn.execute(
        f"INSERT INTO cases ({', '.join(ordered_columns)}) VALUES ({placeholders})",
        tuple(case_values[column] for column in ordered_columns),
    )
    return case_id


def insert_rounds(conn: sqlite3.Connection, case_id: str, snapshot: dict[str, Any], run_dir: Path) -> None:
    for round_summary in snapshot["round_summaries"]:
        round_id = maybe_text(round_summary.get("round_id"))
        fetch = round_summary.get("fetch") if isinstance(round_summary.get("fetch"), dict) else {}
        shared = round_summary.get("shared") if isinstance(round_summary.get("shared"), dict) else {}
        normalized = round_summary.get("normalized") if isinstance(round_summary.get("normalized"), dict) else {}
        decision = round_summary.get("decision") if isinstance(round_summary.get("decision"), dict) else {}
        reports = round_summary.get("reports") if isinstance(round_summary.get("reports"), dict) else {}
        report_statuses = {
            role: maybe_text(report.get("status")) if isinstance(report, dict) else ""
            for role, report in reports.items()
        }

        conn.execute(
            """
            INSERT INTO case_rounds (
                case_id, round_id, round_number, is_current_round, status_label, task_count,
                fetch_step_count, fetch_completed_count, fetch_failed_count, claim_count, observation_count,
                evidence_count, public_signal_count, environment_signal_count, report_statuses_json,
                decision_summary, moderator_status, evidence_sufficiency, next_round_required,
                missing_evidence_types_json, decision_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                round_id,
                int(round_summary.get("round_number") or 0),
                bool_int(round_summary.get("is_current_round")),
                maybe_text(round_summary.get("status_label")),
                int(round_summary.get("task_count") or 0),
                int(fetch.get("step_count") or 0),
                int(fetch.get("completed_count") or 0),
                int(fetch.get("failed_count") or 0),
                int(shared.get("claim_count") or 0),
                int(shared.get("observation_count") or 0),
                int(shared.get("evidence_count") or 0),
                int(normalized.get("public_signal_count") or 0),
                int(normalized.get("environment_signal_count") or 0),
                json_text(report_statuses),
                maybe_text(decision.get("decision_summary")),
                maybe_text(decision.get("moderator_status")),
                maybe_text(decision.get("evidence_sufficiency")),
                bool_int(decision.get("next_round_required")),
                json_text(decision.get("missing_evidence_types", [])),
                json_text(decision) if decision else "",
            ),
        )

        for role, report in reports.items():
            if not isinstance(report, dict):
                continue
            conn.execute(
                """
                INSERT INTO case_reports (case_id, round_id, role, status, summary, report_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    case_id,
                    round_id,
                    role,
                    maybe_text(report.get("status")),
                    maybe_text(report.get("summary")),
                    json_text(report),
                ),
            )

        claims, observations, evidence_cards = round_payload_lists(run_dir, round_id)
        for claim in claims:
            conn.execute(
                """
                INSERT INTO case_claims (
                    case_id, round_id, claim_id, claim_type, priority, status,
                    needs_physical_validation, summary, statement, public_source_skills_json, claim_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case_id,
                    round_id,
                    maybe_text(claim.get("claim_id")),
                    maybe_text(claim.get("claim_type")),
                    claim.get("priority"),
                    maybe_text(claim.get("status")),
                    bool_int(claim.get("needs_physical_validation")),
                    maybe_text(claim.get("summary")),
                    maybe_text(claim.get("statement")),
                    json_text(public_source_skills(claim)),
                    json_text(claim),
                ),
            )

        for observation in observations:
            conn.execute(
                """
                INSERT INTO case_observations (
                    case_id, round_id, observation_id, source_skill, metric, aggregation,
                    value, unit, quality_flags_json, time_window_json, place_scope_json, observation_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case_id,
                    round_id,
                    maybe_text(observation.get("observation_id")),
                    maybe_text(observation.get("source_skill")),
                    maybe_text(observation.get("metric")),
                    maybe_text(observation.get("aggregation")),
                    observation.get("value"),
                    maybe_text(observation.get("unit")),
                    json_text(observation.get("quality_flags", [])),
                    json_text(observation.get("time_window", {})),
                    json_text(observation.get("place_scope", {})),
                    json_text(observation),
                ),
            )

        for evidence in evidence_cards:
            conn.execute(
                """
                INSERT INTO case_evidence (
                    case_id, round_id, evidence_id, claim_id, verdict, confidence,
                    summary, gaps_json, observation_ids_json, evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case_id,
                    round_id,
                    maybe_text(evidence.get("evidence_id")),
                    maybe_text(evidence.get("claim_id")),
                    maybe_text(evidence.get("verdict")),
                    maybe_text(evidence.get("confidence")),
                    maybe_text(evidence.get("summary")),
                    json_text(evidence.get("gaps", [])),
                    json_text(evidence.get("observation_ids", [])),
                    json_text(evidence),
                ),
            )


def delete_case(conn: sqlite3.Connection, case_id: str) -> None:
    conn.execute("DELETE FROM cases WHERE case_id = ?", (case_id,))


def import_run(db_path: Path, run_dir: Path, *, overwrite: bool) -> dict[str, Any]:
    snapshot = collect_run_snapshot(run_dir)
    case_id = maybe_text(snapshot["mission"].get("run_id"))
    if not case_id:
        raise ValueError("mission.run_id is required")

    with connect_db(db_path) as conn:
        existing = conn.execute("SELECT 1 FROM cases WHERE case_id = ?", (case_id,)).fetchone()
        if existing is not None and not overwrite:
            raise ValueError(f"Case already exists: {case_id}. Use --overwrite to replace it.")
        if existing is not None:
            delete_case(conn, case_id)
        insert_case(conn, snapshot, run_dir)
        insert_rounds(conn, case_id, snapshot, run_dir)
        conn.commit()

    return {
        "case_id": case_id,
        "run_dir": str(run_dir),
        "round_count": len(snapshot["round_ids"]),
        "current_round_id": maybe_text(snapshot["state"].get("current_round_id")),
        "current_stage": maybe_text(snapshot["state"].get("stage")),
        "final_moderator_status": maybe_text(snapshot["latest_decision"].get("moderator_status")),
        "final_evidence_sufficiency": maybe_text(snapshot["latest_decision"].get("evidence_sufficiency")),
    }


def import_runs_root(db_path: Path, runs_root: Path, *, overwrite: bool) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for child in sorted(runs_root.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "mission.json").exists():
            continue
        results.append(import_run(db_path, child, overwrite=overwrite))
    return {
        "runs_root": str(runs_root),
        "imported_count": len(results),
        "results": results,
    }


def load_case_bundle(
    conn: sqlite3.Connection,
    case_id: str,
    *,
    include_reports: bool,
    include_claims: bool,
    include_evidence: bool,
) -> dict[str, Any]:
    case_row = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    if case_row is None:
        raise ValueError(f"Case not found: {case_id}")
    rounds = conn.execute(
        """
        SELECT round_id, round_number, is_current_round, status_label, task_count,
               fetch_step_count, fetch_completed_count, fetch_failed_count,
               claim_count, observation_count, evidence_count,
               public_signal_count, environment_signal_count,
               decision_summary, moderator_status, evidence_sufficiency,
               next_round_required, missing_evidence_types_json
        FROM case_rounds
        WHERE case_id = ?
        ORDER BY round_number ASC
        """,
        (case_id,),
    ).fetchall()
    result: dict[str, Any] = {
        "case": dict(case_row),
        "rounds": [dict(row) for row in rounds],
    }
    if include_reports:
        reports = conn.execute(
            "SELECT round_id, role, status, summary FROM case_reports WHERE case_id = ? ORDER BY round_id, role",
            (case_id,),
        ).fetchall()
        result["reports"] = [dict(row) for row in reports]
    if include_claims:
        claims = conn.execute(
            """
            SELECT round_id, claim_id, claim_type, priority, status, summary, statement, public_source_skills_json
            FROM case_claims
            WHERE case_id = ?
            ORDER BY round_id, claim_id
            """,
            (case_id,),
        ).fetchall()
        result["claims"] = [dict(row) for row in claims]
    if include_evidence:
        evidence = conn.execute(
            """
            SELECT round_id, evidence_id, claim_id, verdict, confidence, summary, gaps_json, observation_ids_json
            FROM case_evidence
            WHERE case_id = ?
            ORDER BY round_id, evidence_id
            """,
            (case_id,),
        ).fetchall()
        result["evidence"] = [dict(row) for row in evidence]
    return result


def score_case_row(
    row: sqlite3.Row,
    *,
    query_terms_list: list[str],
    region_label: str,
    moderator_status: str,
    evidence_sufficiency: str,
) -> tuple[float, list[str], bool]:
    topic = normalized_text(row["topic"])
    objective = normalized_text(row["objective"])
    region = normalized_text(row["region_label"])
    final_summary = normalized_text(row["final_decision_summary"])
    final_brief = normalized_text(row["final_brief"])
    reasons: list[str] = []
    score = 0.0
    core_match = False

    if query_terms_list:
        topic_hits = sorted({term for term in query_terms_list if term in topic})
        objective_hits = sorted({term for term in query_terms_list if term in objective})
        summary_hits = sorted({term for term in query_terms_list if term in final_summary or term in final_brief})
        if topic_hits:
            score += 6.0 + (3.0 * len(topic_hits))
            reasons.append("topic:" + ",".join(topic_hits[:3]))
            core_match = True
        if objective_hits:
            score += 2.0 * len(objective_hits)
            reasons.append("objective:" + ",".join(objective_hits[:3]))
            core_match = True
        if summary_hits:
            score += 1.5 * len(summary_hits)
            reasons.append("decision:" + ",".join(summary_hits[:3]))
            core_match = True

    target_region = normalized_text(region_label)
    if target_region:
        if region == target_region:
            score += 8.0
            reasons.append("region:exact")
            core_match = True
        else:
            overlap = sorted(set(search_terms(target_region)) & set(search_terms(region)))
            if overlap:
                score += 2.0 * len(overlap)
                reasons.append("region:" + ",".join(overlap[:3]))
                core_match = True

    status_value = maybe_text(row["final_moderator_status"])
    sufficiency_value = maybe_text(row["final_evidence_sufficiency"])
    if moderator_status and status_value == moderator_status:
        score += 1.5
        reasons.append(f"status:{moderator_status}")
        core_match = True
    if evidence_sufficiency and sufficiency_value == evidence_sufficiency:
        score += 1.5
        reasons.append(f"evidence:{evidence_sufficiency}")
        core_match = True

    score += float(sufficiency_rank(sufficiency_value))
    score += 0.5 * float(moderator_status_rank(status_value))

    if (query_terms_list or target_region or moderator_status or evidence_sufficiency) and not core_match:
        return (0.0, [], False)
    return (score, reasons, True)


def default_export_path(db_path: Path, case_id: str, lang: str) -> Path:
    return db_path.parent / "exports" / f"{safe_case_filename(case_id)}.{lang}.md"


def render_case_markdown(bundle: dict[str, Any], *, lang: str) -> str:
    if lang not in {"zh", "en"}:
        raise ValueError(f"Unsupported language: {lang}")
    case = bundle.get("case", {})
    if not isinstance(case, dict):
        raise ValueError("Case bundle is missing `case`.")
    rounds = bundle.get("rounds", [])
    reports = bundle.get("reports", [])
    claims = bundle.get("claims", [])
    evidence = bundle.get("evidence", [])
    missing_types = parse_json_text(case.get("final_missing_evidence_types_json"), default=[])

    if lang == "en":
        lines = [
            f"# Eco Council Historical Case: {case.get('case_id', '')}",
            "",
            f"- Topic: {maybe_text(case.get('topic'))}",
            f"- Objective: {maybe_text(case.get('objective'))}",
            f"- Region: {maybe_text(case.get('region_label'))}",
            f"- Window: {maybe_text(case.get('window_start_utc'))} -> {maybe_text(case.get('window_end_utc'))}",
            f"- Rounds: {case.get('round_count')}",
            f"- Final moderator status: {maybe_text(case.get('final_moderator_status')) or 'unknown'}",
            f"- Final evidence sufficiency: {maybe_text(case.get('final_evidence_sufficiency')) or 'unknown'}",
            f"- Current stage at import: {maybe_text(case.get('current_stage')) or 'unknown'}",
            f"- Imported at: {maybe_text(case.get('imported_at_utc'))}",
            "",
            "## Final Decision",
            maybe_text(case.get("final_decision_summary")) or "No final decision summary was available.",
        ]
        if maybe_text(case.get("final_brief")):
            lines.extend(["", maybe_text(case.get("final_brief"))])
        if missing_types:
            lines.extend(["", f"- Missing evidence types: {', '.join(maybe_text(item) for item in missing_types if maybe_text(item))}"])
        lines.extend(["", "## Round Timeline"])
        for round_item in rounds if isinstance(rounds, list) else []:
            if not isinstance(round_item, dict):
                continue
            lines.append(
                "- "
                + f"{maybe_text(round_item.get('round_id'))}: {maybe_text(round_item.get('status_label'))}; "
                + f"tasks={round_item.get('task_count')}, "
                + f"fetch={round_item.get('fetch_completed_count')}/{round_item.get('fetch_step_count')}, "
                + f"claims={round_item.get('claim_count')}, "
                + f"evidence={round_item.get('evidence_count')}, "
                + f"moderator={maybe_text(round_item.get('moderator_status')) or 'n/a'}, "
                + f"sufficiency={maybe_text(round_item.get('evidence_sufficiency')) or 'n/a'}"
            )
        if isinstance(reports, list) and reports:
            lines.extend(["", "## Report Summaries"])
            for report in reports:
                if not isinstance(report, dict):
                    continue
                lines.append(
                    "- "
                    + f"{maybe_text(report.get('round_id'))} {maybe_text(report.get('role'))}: "
                    + f"{maybe_text(report.get('status')) or 'unknown'}; "
                    + (maybe_text(report.get("summary")) or "No summary.")
                )
        if isinstance(claims, list) and claims:
            lines.extend(["", "## Claims"])
            for claim in claims:
                if not isinstance(claim, dict):
                    continue
                skills = parse_json_text(claim.get("public_source_skills_json"), default=[])
                skill_text = ", ".join(maybe_text(item) for item in skills if maybe_text(item)) or "none"
                lines.append(
                    "- "
                    + f"{maybe_text(claim.get('round_id'))} {maybe_text(claim.get('claim_id'))} "
                    + f"[{maybe_text(claim.get('claim_type'))}] "
                    + f"{maybe_text(claim.get('status')) or 'unknown'}; "
                    + f"{maybe_text(claim.get('summary')) or maybe_text(claim.get('statement'))}; "
                    + f"public_sources={skill_text}"
                )
        if isinstance(evidence, list) and evidence:
            lines.extend(["", "## Evidence"])
            for item in evidence:
                if not isinstance(item, dict):
                    continue
                gaps = parse_json_text(item.get("gaps_json"), default=[])
                gap_text = ", ".join(maybe_text(value) for value in gaps if maybe_text(value)) or "none"
                lines.append(
                    "- "
                    + f"{maybe_text(item.get('round_id'))} {maybe_text(item.get('evidence_id'))} "
                    + f"claim={maybe_text(item.get('claim_id'))}; "
                    + f"verdict={maybe_text(item.get('verdict'))}; "
                    + f"confidence={maybe_text(item.get('confidence'))}; "
                    + f"{maybe_text(item.get('summary'))}; "
                    + f"gaps={gap_text}"
                )
        return "\n".join(lines)

    lines = [
        f"# 生态议会历史案例：{case.get('case_id', '')}",
        "",
        f"- 议题：{maybe_text(case.get('topic'))}",
        f"- 目标：{maybe_text(case.get('objective'))}",
        f"- 地区：{maybe_text(case.get('region_label'))}",
        f"- 时间窗：{maybe_text(case.get('window_start_utc'))} -> {maybe_text(case.get('window_end_utc'))}",
        f"- 轮次数：{case.get('round_count')}",
        f"- 最终议长状态：{maybe_text(case.get('final_moderator_status')) or '未知'}",
        f"- 最终证据充分性：{maybe_text(case.get('final_evidence_sufficiency')) or '未知'}",
        f"- 导入时当前阶段：{maybe_text(case.get('current_stage')) or '未知'}",
        f"- 导入时间：{maybe_text(case.get('imported_at_utc'))}",
        "",
        "## 最终结论",
        maybe_text(case.get("final_decision_summary")) or "该案例没有保存最终决策摘要。",
    ]
    if maybe_text(case.get("final_brief")):
        lines.extend(["", maybe_text(case.get("final_brief"))])
    if missing_types:
        lines.extend(["", f"- 缺失证据类型：{', '.join(maybe_text(item) for item in missing_types if maybe_text(item))}"])
    lines.extend(["", "## 轮次时间线"])
    for round_item in rounds if isinstance(rounds, list) else []:
        if not isinstance(round_item, dict):
            continue
        lines.append(
            "- "
            + f"{maybe_text(round_item.get('round_id'))}：{maybe_text(round_item.get('status_label'))}；"
            + f"tasks={round_item.get('task_count')}，"
            + f"fetch={round_item.get('fetch_completed_count')}/{round_item.get('fetch_step_count')}，"
            + f"claims={round_item.get('claim_count')}，"
            + f"evidence={round_item.get('evidence_count')}，"
            + f"议长状态={maybe_text(round_item.get('moderator_status')) or '无'}，"
            + f"证据充分性={maybe_text(round_item.get('evidence_sufficiency')) or '无'}"
        )
    if isinstance(reports, list) and reports:
        lines.extend(["", "## 报告摘要"])
        for report in reports:
            if not isinstance(report, dict):
                continue
            lines.append(
                "- "
                + f"{maybe_text(report.get('round_id'))} {maybe_text(report.get('role'))}："
                + f"{maybe_text(report.get('status')) or '未知'}；"
                + (maybe_text(report.get("summary")) or "无摘要。")
            )
    if isinstance(claims, list) and claims:
        lines.extend(["", "## 主张"])
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            skills = parse_json_text(claim.get("public_source_skills_json"), default=[])
            skill_text = ", ".join(maybe_text(item) for item in skills if maybe_text(item)) or "无"
            lines.append(
                "- "
                + f"{maybe_text(claim.get('round_id'))} {maybe_text(claim.get('claim_id'))} "
                + f"[{maybe_text(claim.get('claim_type'))}] "
                + f"{maybe_text(claim.get('status')) or '未知'}；"
                + f"{maybe_text(claim.get('summary')) or maybe_text(claim.get('statement'))}；"
                + f"舆情来源={skill_text}"
            )
    if isinstance(evidence, list) and evidence:
        lines.extend(["", "## 证据"])
        for item in evidence:
            if not isinstance(item, dict):
                continue
            gaps = parse_json_text(item.get("gaps_json"), default=[])
            gap_text = ", ".join(maybe_text(value) for value in gaps if maybe_text(value)) or "无"
            lines.append(
                "- "
                + f"{maybe_text(item.get('round_id'))} {maybe_text(item.get('evidence_id'))} "
                + f"claim={maybe_text(item.get('claim_id'))}；"
                + f"verdict={maybe_text(item.get('verdict'))}；"
                + f"confidence={maybe_text(item.get('confidence'))}；"
                + f"{maybe_text(item.get('summary'))}；"
                + f"缺口={gap_text}"
            )
    return "\n".join(lines)


def command_init_db(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    init_db(db_path)
    return {"db": str(db_path), "ddl_path": str(DDL_PATH)}


def command_import_run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    run_dir = Path(args.run_dir).expanduser().resolve()
    init_db(db_path)
    return import_run(db_path, run_dir, overwrite=args.overwrite)


def command_import_runs_root(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()
    init_db(db_path)
    return import_runs_root(db_path, runs_root, overwrite=args.overwrite)


def command_list_cases(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    with connect_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT case_id, topic, region_label, window_start_utc, window_end_utc,
                   round_count, current_round_id, current_stage,
                   final_moderator_status, final_evidence_sufficiency,
                   latest_claim_count, latest_observation_count, latest_evidence_count,
                   imported_at_utc
            FROM cases
            ORDER BY imported_at_utc DESC, case_id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
    return {
        "db": str(db_path),
        "count": len(rows),
        "cases": [dict(row) for row in rows],
    }


def command_search_cases(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    query = maybe_text(args.query)
    region_label = maybe_text(args.region_label)
    moderator_status = maybe_text(args.moderator_status)
    evidence_sufficiency = maybe_text(args.evidence_sufficiency)
    exclude_case_id = maybe_text(args.exclude_case_id)
    query_terms_list = search_terms(query)

    sql = """
        SELECT case_id, topic, objective, region_label, window_start_utc, window_end_utc,
               round_count, current_round_id, final_moderator_status, final_evidence_sufficiency,
               final_decision_summary, final_brief, final_missing_evidence_types_json, imported_at_utc
        FROM cases
        WHERE 1 = 1
    """
    sql_args: list[Any] = []
    if exclude_case_id:
        sql += " AND case_id != ?"
        sql_args.append(exclude_case_id)
    if moderator_status:
        sql += " AND final_moderator_status = ?"
        sql_args.append(moderator_status)
    if evidence_sufficiency:
        sql += " AND final_evidence_sufficiency = ?"
        sql_args.append(evidence_sufficiency)

    with connect_db(db_path) as conn:
        rows = conn.execute(sql, sql_args).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        score, reasons, matched = score_case_row(
            row,
            query_terms_list=query_terms_list,
            region_label=region_label,
            moderator_status=moderator_status,
            evidence_sufficiency=evidence_sufficiency,
        )
        if not matched:
            continue
        missing_types = parse_json_text(row["final_missing_evidence_types_json"], default=[])
        results.append(
            {
                "case_id": row["case_id"],
                "score": round(score, 2),
                "match_reasons": reasons,
                "topic": row["topic"],
                "objective": row["objective"],
                "region_label": row["region_label"],
                "window_start_utc": row["window_start_utc"],
                "window_end_utc": row["window_end_utc"],
                "round_count": row["round_count"],
                "current_round_id": row["current_round_id"],
                "final_moderator_status": row["final_moderator_status"],
                "final_evidence_sufficiency": row["final_evidence_sufficiency"],
                "final_decision_summary": row["final_decision_summary"],
                "final_brief": row["final_brief"],
                "final_missing_evidence_types": missing_types,
                "imported_at_utc": row["imported_at_utc"],
            }
        )

    results.sort(
        key=lambda item: (
            item["score"],
            sufficiency_rank(maybe_text(item.get("final_evidence_sufficiency"))),
            moderator_status_rank(maybe_text(item.get("final_moderator_status"))),
            maybe_text(item.get("imported_at_utc")),
            maybe_text(item.get("case_id")),
        ),
        reverse=True,
    )
    limited = results[: args.limit]
    return {
        "db": str(db_path),
        "query": query,
        "query_terms": query_terms_list,
        "region_label": region_label,
        "moderator_status": moderator_status,
        "evidence_sufficiency": evidence_sufficiency,
        "exclude_case_id": exclude_case_id,
        "count": len(limited),
        "cases": limited,
    }


def command_show_case(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    case_id = args.case_id
    with connect_db(db_path) as conn:
        result = load_case_bundle(
            conn,
            case_id,
            include_reports=args.include_reports,
            include_claims=args.include_claims,
            include_evidence=args.include_evidence,
        )
        result["db"] = str(db_path)
    return result


def command_export_case_markdown(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    case_id = args.case_id
    with connect_db(db_path) as conn:
        bundle = load_case_bundle(
            conn,
            case_id,
            include_reports=args.include_reports,
            include_claims=args.include_claims,
            include_evidence=args.include_evidence,
        )
    output_path = (
        Path(args.output).expanduser().resolve()
        if maybe_text(args.output)
        else default_export_path(db_path, case_id, args.lang).resolve()
    )
    markdown = render_case_markdown(bundle, lang=args.lang)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown.rstrip() + "\n", encoding="utf-8")
    return {
        "db": str(db_path),
        "case_id": case_id,
        "lang": args.lang,
        "output_path": str(output_path),
        "preview": "\n".join(markdown.splitlines()[:20]),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage a local SQLite library of eco-council historical runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_db_cmd = sub.add_parser("init-db", help="Initialize the local eco-council case-library SQLite database.")
    init_db_cmd.add_argument("--db", required=True, help="SQLite database path.")
    init_db_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_run_cmd = sub.add_parser("import-run", help="Import one run directory into the local case library.")
    import_run_cmd.add_argument("--db", required=True, help="SQLite database path.")
    import_run_cmd.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_run_cmd.add_argument("--overwrite", action="store_true", help="Replace an existing case with the same run_id.")
    import_run_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_root_cmd = sub.add_parser("import-runs-root", help="Import every run directory under one runs root.")
    import_root_cmd.add_argument("--db", required=True, help="SQLite database path.")
    import_root_cmd.add_argument("--runs-root", required=True, help="Runs root directory.")
    import_root_cmd.add_argument("--overwrite", action="store_true", help="Replace existing cases when run_id matches.")
    import_root_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    list_cases_cmd = sub.add_parser("list-cases", help="List imported historical eco-council cases.")
    list_cases_cmd.add_argument("--db", required=True, help="SQLite database path.")
    list_cases_cmd.add_argument("--limit", type=int, default=50, help="Maximum cases to return.")
    list_cases_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    search_cases_cmd = sub.add_parser("search-cases", help="Search historical cases by query, region, or status.")
    search_cases_cmd.add_argument("--db", required=True, help="SQLite database path.")
    search_cases_cmd.add_argument("--query", default="", help="Free-text query over topic, objective, and decisions.")
    search_cases_cmd.add_argument("--region-label", default="", help="Preferred region label.")
    search_cases_cmd.add_argument("--moderator-status", default="", help="Optional final moderator status filter.")
    search_cases_cmd.add_argument("--evidence-sufficiency", default="", help="Optional evidence sufficiency filter.")
    search_cases_cmd.add_argument("--exclude-case-id", default="", help="Case id to exclude from search results.")
    search_cases_cmd.add_argument("--limit", type=int, default=10, help="Maximum cases to return.")
    search_cases_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    show_case_cmd = sub.add_parser("show-case", help="Show one imported case with round summaries.")
    show_case_cmd.add_argument("--db", required=True, help="SQLite database path.")
    show_case_cmd.add_argument("--case-id", required=True, help="Case id, usually the run_id.")
    show_case_cmd.add_argument("--include-reports", action="store_true", help="Include report summaries.")
    show_case_cmd.add_argument("--include-claims", action="store_true", help="Include claim summaries.")
    show_case_cmd.add_argument("--include-evidence", action="store_true", help="Include evidence summaries.")
    show_case_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    export_case_cmd = sub.add_parser("export-case-markdown", help="Export one historical case into human-readable Markdown.")
    export_case_cmd.add_argument("--db", required=True, help="SQLite database path.")
    export_case_cmd.add_argument("--case-id", required=True, help="Case id, usually the run_id.")
    export_case_cmd.add_argument("--lang", default="zh", choices=("zh", "en"), help="Markdown language.")
    export_case_cmd.add_argument("--output", default="", help="Optional markdown output path.")
    export_case_cmd.add_argument("--include-reports", action="store_true", help="Include report summaries.")
    export_case_cmd.add_argument("--include-claims", action="store_true", help="Include claim summaries.")
    export_case_cmd.add_argument("--include-evidence", action="store_true", help="Include evidence summaries.")
    export_case_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-db": command_init_db,
        "import-run": command_import_run,
        "import-runs-root": command_import_runs_root,
        "list-cases": command_list_cases,
        "search-cases": command_search_cases,
        "show-case": command_show_case,
        "export-case-markdown": command_export_case_markdown,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"command": args.command, "ok": False, "error": str(exc)}, pretty=getattr(args, "pretty", False)))
        return 1
    print(pretty_json({"command": args.command, "ok": True, "payload": payload}, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
