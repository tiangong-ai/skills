#!/usr/bin/env python3
"""Run eco-council stages with approval gates and fixed agent handoffs."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_DIR = SKILL_DIR.parent

ORCHESTRATE_SCRIPT = REPO_DIR / "eco-council-orchestrate" / "scripts" / "eco_council_orchestrate.py"
REPORTING_SCRIPT = REPO_DIR / "eco-council-reporting" / "scripts" / "eco_council_reporting.py"
CONTRACT_SCRIPT = REPO_DIR / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"
CASE_LIBRARY_SCRIPT = SKILL_DIR / "scripts" / "eco_council_case_library.py"
SIGNAL_CORPUS_SCRIPT = SKILL_DIR / "scripts" / "eco_council_signal_corpus.py"

SCHEMA_VERSION = "1.0.0"
ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")
AGENT_ID_SAFE = re.compile(r"[^a-z0-9-]+")
ROLES = ("moderator", "sociologist", "environmentalist")
SOURCE_SELECTION_ROLES = ("sociologist", "environmentalist")
REPORT_ROLES = ("sociologist", "environmentalist")
OPENCLAW_AGENT_GUIDE_FILENAME = "OPENCLAW_AGENT_GUIDE.md"

STAGE_AWAITING_TASK_REVIEW = "awaiting-moderator-task-review"
STAGE_AWAITING_SOURCE_SELECTION = "awaiting-source-selection"
STAGE_READY_PREPARE = "ready-to-prepare-round"
STAGE_READY_FETCH = "ready-to-execute-fetch-plan"
STAGE_READY_DATA_PLANE = "ready-to-run-data-plane"
STAGE_AWAITING_REPORTS = "awaiting-expert-reports"
STAGE_AWAITING_DECISION = "awaiting-moderator-decision"
STAGE_READY_PROMOTE = "ready-to-promote"
STAGE_READY_ADVANCE = "ready-to-advance-round"
STAGE_COMPLETED = "completed"
DEFAULT_HISTORY_TOP_K = 3
MAX_HISTORY_TOP_K = 5


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def write_json(path: Path, payload: Any, *, pretty: bool = True) -> None:
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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def require_round_id(value: str) -> str:
    if not ROUND_ID_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid round id: {value!r}")
    return value


def round_dir_name(round_id: str) -> str:
    require_round_id(round_id)
    return f"round_{round_id.split('-')[1]}"


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_dir_name(round_id)


def discover_round_ids(run_dir: Path) -> list[str]:
    round_ids: list[str] = []
    if not run_dir.exists():
        return round_ids
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        match = ROUND_DIR_PATTERN.fullmatch(child.name)
        if match is None:
            continue
        round_ids.append(f"round-{match.group(1)}")
    round_ids.sort()
    return round_ids


def latest_round_id(run_dir: Path) -> str:
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}")
    return round_ids[-1]


def next_round_id(round_id: str) -> str:
    require_round_id(round_id)
    number = int(round_id.split("-")[1])
    return f"round-{number + 1:03d}"


def tasks_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "tasks.json"


def mission_path(run_dir: Path) -> Path:
    return run_dir / "mission.json"


def task_review_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_task_review_prompt.txt"


def fetch_plan_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch_plan.json"


def fetch_execution_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch_execution.json"


def fetch_lock_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch.lock"


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "source_selection.json"


def source_selection_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_source_selection_prompt.txt"


def source_selection_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "source_selection_packet.json"


def report_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_report_draft.json"


def report_target_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / f"{role}_report.json"


def report_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_report_prompt.txt"


def report_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "report_packet.json"


def decision_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "council_decision_draft.json"


def decision_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_decision_prompt.txt"


def decision_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "decision_packet.json"


def decision_target_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "council_decision.json"


def shared_claims_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "claims.json"


def shared_observations_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "observations.json"


def shared_evidence_cards_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_cards.json"


def public_signals_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "normalized" / "public_signals.jsonl"


def environment_signals_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "normalized" / "environment_signals.jsonl"


def supervisor_dir(run_dir: Path) -> Path:
    return run_dir / "supervisor"


def supervisor_state_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "state.json"


def supervisor_state_lock_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "state.lock"


def supervisor_sessions_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "sessions"


def supervisor_outbox_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "outbox"


def supervisor_responses_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "responses"


def supervisor_current_step_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "CURRENT_STEP.txt"


def reports_dir(run_dir: Path) -> Path:
    return run_dir / "reports"


def supervisor_context_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "context"


def history_context_path(run_dir: Path, round_id: str) -> Path:
    return supervisor_context_dir(run_dir) / f"{round_id}_historical_cases.txt"


def response_base_path(run_dir: Path, round_id: str, role: str, kind: str) -> Path:
    safe_kind = kind.replace("-", "_")
    return supervisor_responses_dir(run_dir) / f"{round_id}_{role}_{safe_kind}"


@contextmanager
def exclusive_file_lock(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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


def run_json_command(argv: list[str], *, cwd: Path | None = None) -> Any:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
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


def run_check_command(argv: list[str], *, cwd: Path | None = None) -> None:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
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


def load_state(run_dir: Path) -> dict[str, Any]:
    path = supervisor_state_path(run_dir)
    if not path.exists():
        raise ValueError(f"Supervisor state not found: {path}")
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Supervisor state is not a JSON object: {path}")
    return payload


def save_state(run_dir: Path, state: dict[str, Any]) -> None:
    state["updated_at_utc"] = utc_now_iso()
    refresh_supervisor_files(run_dir, state)
    write_json(supervisor_state_path(run_dir), state, pretty=True)


def normalize_agent_prefix(value: str) -> str:
    text = AGENT_ID_SAFE.sub("-", value.strip().lower()).strip("-")
    return text or "eco-council"


def ensure_openclaw_config(run_dir: Path, state: dict[str, Any], *, workspace_root_text: str = "") -> dict[str, Any]:
    openclaw_section = state.setdefault("openclaw", {})
    prefix = normalize_agent_prefix(maybe_text(openclaw_section.get("agent_prefix")) or run_dir.name)
    openclaw_section["agent_prefix"] = prefix
    workspace_root = (
        Path(workspace_root_text).expanduser().resolve()
        if workspace_root_text
        else openclaw_workspace_root(run_dir, state)
    )
    openclaw_section["workspace_root"] = str(workspace_root)
    agents = openclaw_section.setdefault("agents", {})
    for role in ROLES:
        role_info = agents.setdefault(role, {})
        role_info["id"] = maybe_text(role_info.get("id")) or f"{prefix}-{role}"
        workspace = (
            Path(maybe_text(role_info.get("workspace"))).expanduser().resolve()
            if maybe_text(role_info.get("workspace"))
            else (workspace_root / role).resolve()
        )
        role_info["workspace"] = str(workspace)
        role_info["guide_path"] = str((workspace / OPENCLAW_AGENT_GUIDE_FILENAME).resolve())
    return openclaw_section


def normalize_history_top_k(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = DEFAULT_HISTORY_TOP_K
    return max(1, min(MAX_HISTORY_TOP_K, count))


def ensure_history_context_config(state: dict[str, Any]) -> dict[str, Any]:
    history = state.get("history_context")
    if not isinstance(history, dict):
        history = {}
    history["db"] = maybe_text(history.get("db"))
    history["top_k"] = normalize_history_top_k(history.get("top_k"))
    state["history_context"] = history
    return history


def apply_history_cli_config(state: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    history = ensure_history_context_config(state)
    if bool(getattr(args, "disable_history_context", False)):
        history["db"] = ""
    elif maybe_text(getattr(args, "history_db", "")):
        history["db"] = str(Path(args.history_db).expanduser().resolve())
    top_k_value = int(getattr(args, "history_top_k", 0) or 0)
    if top_k_value > 0:
        history["top_k"] = normalize_history_top_k(top_k_value)
    state["history_context"] = history
    return history


def history_cli_updates_requested(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "disable_history_context", False)
        or maybe_text(getattr(args, "history_db", ""))
        or int(getattr(args, "history_top_k", 0) or 0) > 0
    )


def ensure_signal_corpus_config(state: dict[str, Any]) -> dict[str, Any]:
    signal_corpus = state.get("signal_corpus")
    if not isinstance(signal_corpus, dict):
        signal_corpus = {}
    signal_corpus["db"] = maybe_text(signal_corpus.get("db"))
    signal_corpus["auto_import"] = bool(signal_corpus.get("auto_import")) if signal_corpus["db"] else False
    signal_corpus["last_imported_round_id"] = maybe_text(signal_corpus.get("last_imported_round_id"))
    signal_corpus["last_imported_at_utc"] = maybe_text(signal_corpus.get("last_imported_at_utc"))
    last_import = signal_corpus.get("last_import")
    if not isinstance(last_import, dict):
        last_import = {}
    signal_corpus["last_import"] = last_import
    state["signal_corpus"] = signal_corpus
    return signal_corpus


def apply_signal_corpus_cli_config(state: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    signal_corpus = ensure_signal_corpus_config(state)
    if bool(getattr(args, "disable_signal_corpus_import", False)):
        signal_corpus["db"] = ""
        signal_corpus["auto_import"] = False
    elif maybe_text(getattr(args, "signal_corpus_db", "")):
        signal_corpus["db"] = str(Path(args.signal_corpus_db).expanduser().resolve())
        signal_corpus["auto_import"] = True
    state["signal_corpus"] = signal_corpus
    return signal_corpus


def signal_corpus_cli_updates_requested(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "disable_signal_corpus_import", False)
        or maybe_text(getattr(args, "signal_corpus_db", ""))
    )


def render_history_context_text(*, mission: dict[str, Any], search_payload: dict[str, Any]) -> str:
    cases = search_payload.get("cases") if isinstance(search_payload.get("cases"), list) else []
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    lines = [
        "Compact historical-case context from the local eco-council case library.",
        "Use it only as planning guidance. Current-round evidence remains primary.",
        "Do not repeat exhausted fetch paths unless the region, time window, or claim mix is materially different.",
        "",
        f"Current topic: {maybe_text(mission.get('topic'))}",
        f"Current objective: {maybe_text(mission.get('objective'))}",
        f"Current region: {maybe_text(region.get('label')) or 'n/a'}",
        "",
        f"Retrieved similar cases: {len(cases)}",
    ]
    for index, case in enumerate(cases, start=1):
        if not isinstance(case, dict):
            continue
        missing = case.get("final_missing_evidence_types")
        missing_text = ", ".join(maybe_text(item) for item in missing if maybe_text(item)) if isinstance(missing, list) else ""
        reasons = case.get("match_reasons")
        reason_text = ", ".join(maybe_text(item) for item in reasons if maybe_text(item)) if isinstance(reasons, list) else ""
        lines.extend(
            [
                "",
                f"{index}. case_id={maybe_text(case.get('case_id'))}; score={case.get('score')}; region={maybe_text(case.get('region_label'))}; rounds={case.get('round_count')}; moderator_status={maybe_text(case.get('final_moderator_status')) or 'unknown'}; evidence={maybe_text(case.get('final_evidence_sufficiency')) or 'unknown'}",
                f"   topic={maybe_text(case.get('topic'))}",
                f"   decision_summary={maybe_text(case.get('final_decision_summary')) or maybe_text(case.get('final_brief')) or 'n/a'}",
            ]
        )
        if missing_text:
            lines.append(f"   missing_evidence_types={missing_text}")
        if reason_text:
            lines.append(f"   match_reasons={reason_text}")
    return "\n".join(lines)


def write_history_context_file(run_dir: Path, state: dict[str, Any], round_id: str) -> Path | None:
    target = history_context_path(run_dir, round_id)
    history = ensure_history_context_config(state)
    db_text = maybe_text(history.get("db"))
    if not db_text:
        if target.exists():
            target.unlink()
        return None

    db_path = Path(db_text).expanduser().resolve()
    if not db_path.exists():
        if target.exists():
            target.unlink()
        return None

    mission_payload = load_json_if_exists(mission_path(run_dir))
    if not isinstance(mission_payload, dict):
        if target.exists():
            target.unlink()
        return None

    region = mission_payload.get("region") if isinstance(mission_payload.get("region"), dict) else {}
    query = maybe_text(mission_payload.get("topic"))
    argv = [
        "python3",
        str(CASE_LIBRARY_SCRIPT),
        "search-cases",
        "--db",
        str(db_path),
        "--exclude-case-id",
        maybe_text(mission_payload.get("run_id")),
        "--limit",
        str(normalize_history_top_k(history.get("top_k"))),
        "--pretty",
    ]
    if query:
        argv.extend(["--query", query])
    if maybe_text(region.get("label")):
        argv.extend(["--region-label", maybe_text(region.get("label"))])

    try:
        payload = run_json_command(argv, cwd=REPO_DIR)
    except Exception:
        if target.exists():
            target.unlink()
        return None
    search_payload = payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload
    cases = search_payload.get("cases") if isinstance(search_payload, dict) else None
    if not isinstance(cases, list) or not cases:
        if target.exists():
            target.unlink()
        return None

    write_text(target, render_history_context_text(mission=mission_payload, search_payload=search_payload))
    return target


def openclaw_workspace_root(run_dir: Path, state: dict[str, Any]) -> Path:
    configured = maybe_text(state.get("openclaw", {}).get("workspace_root"))
    if configured:
        return Path(configured).expanduser().resolve()
    return supervisor_dir(run_dir) / "openclaw-workspaces"


def session_prompt_path(run_dir: Path, role: str) -> Path:
    return supervisor_sessions_dir(run_dir) / f"{role}_session_prompt.txt"


def outbox_message_path(run_dir: Path, name: str) -> Path:
    return supervisor_outbox_dir(run_dir) / f"{name}.txt"


def role_display_name(role: str) -> str:
    return {
        "moderator": "Moderator",
        "sociologist": "Sociologist",
        "environmentalist": "Environmentalist",
    }[role]


def agent_workspace_path(state: dict[str, Any], role: str) -> Path:
    workspace_text = maybe_text(state.get("openclaw", {}).get("agents", {}).get(role, {}).get("workspace"))
    if not workspace_text:
        raise ValueError(f"Missing OpenClaw workspace for role={role}")
    return Path(workspace_text).expanduser().resolve()


def agent_command_guide_path(*, state: dict[str, Any], role: str) -> Path:
    workspace = agent_workspace_path(state, role)
    return workspace / OPENCLAW_AGENT_GUIDE_FILENAME


def supervisor_status_command(run_dir: Path) -> str:
    return shlex.join(
        [
            "python3",
            str(SCRIPT_DIR / "eco_council_supervisor.py"),
            "status",
            "--run-dir",
            str(run_dir),
            "--pretty",
        ]
    )


def openclaw_agent_guide_text(*, run_dir: Path, state: dict[str, Any], role: str) -> str:
    run_dir = run_dir.expanduser().resolve()
    supervisor_script = SCRIPT_DIR / "eco_council_supervisor.py"
    status_command = supervisor_status_command(run_dir)
    continue_command = shlex.join(
        [
            "python3",
            str(supervisor_script),
            "continue-run",
            "--run-dir",
            str(run_dir),
            "--yes",
            "--pretty",
        ]
    )
    run_agent_command = shlex.join(
        [
            "python3",
            str(supervisor_script),
            "run-agent-step",
            "--run-dir",
            str(run_dir),
            "--role",
            role,
            "--yes",
            "--pretty",
        ]
    )
    provision_command = shlex.join(
        [
            "python3",
            str(supervisor_script),
            "provision-openclaw-agents",
            "--run-dir",
            str(run_dir),
            "--yes",
            "--pretty",
        ]
    )
    summarize_command = shlex.join(
        [
            "python3",
            str(supervisor_script),
            "summarize-run",
            "--run-dir",
            str(run_dir),
            "--lang",
            "zh",
            "--pretty",
        ]
    )
    init_command = (
        "python3 "
        + str(supervisor_script)
        + " init-run --run-dir NEW_RUN_DIR --mission-input MISSION_JSON --yes --pretty"
    )
    return "\n".join(
        [
            "# OpenClaw Agent Guide",
            "",
            f"Run directory: {run_dir}",
            f"Role: {role}",
            "",
            "The supervisor owns stage transitions, shell stages, and JSON imports.",
            "Role agents own only the single JSON artifact requested by the current turn.",
            "",
            "Local files to trust first:",
            f"- Current step checklist: {supervisor_current_step_path(run_dir)}",
            f"- Session prompt for this role: {session_prompt_path(run_dir, role)}",
            f"- Supervisor outbox directory: {supervisor_outbox_dir(run_dir)}",
            "",
            "Command inventory:",
            f"- `{status_command}`",
            "  Purpose: inspect current round, current stage, prompt paths, and recommended next command.",
            f"- `{continue_command}`",
            "  Purpose: advance one supervisor-owned shell stage such as prepare-round, execute-fetch-plan, run-data-plane, promote-all, or advance-round. Human/supervisor only.",
            f"- `{run_agent_command}`",
            "  Purpose: supervisor wrapper that sends the current turn to an OpenClaw agent and imports the validated JSON reply. Do not call this from inside the agent already handling the turn.",
            "- `python3 ... import-task-review ...` / `import-source-selection ...` / `import-report ...` / `import-decision ...` / `import-fetch-execution ...`",
            "  Purpose: import canonical JSON after manual edits or external fetch execution. Human/supervisor only.",
            f"- `{provision_command}`",
            "  Purpose: create or repair the three fixed OpenClaw agents and workspace support files. Human/supervisor only.",
            f"- `{summarize_command}`",
            "  Purpose: render a human-readable meeting record for audit. Usually human-only.",
            f"- `{init_command}`",
            "  Purpose: bootstrap a brand-new run and provision agents. Human/supervisor only.",
            "- Validation commands printed inside the active prompt or packet",
            "  Purpose: check that the JSON artifact you just edited matches the required eco-council schema. Safe to run when the prompt explicitly asks for validation.",
            "",
            "Never do the following unless the human explicitly changes your role to supervisor operator:",
            "- Do not call `continue-run`, `run-agent-step`, or any `import-*` command from inside a normal role turn.",
            "- Do not run raw fetch shell commands during task-review, source-selection, report, or decision turns.",
            "- Do not mutate files other than the target JSON artifact named by the current turn prompt.",
            "- Do not invent fetch results, evidence cards, or reports outside the current stage contract.",
        ]
    )


def write_openclaw_workspace_files(*, run_dir: Path, state: dict[str, Any], role: str, agent_id: str) -> None:
    workspace = agent_workspace_path(state, role)
    workspace.mkdir(parents=True, exist_ok=True)
    write_text(workspace / "IDENTITY.md", identity_text(role=role, agent_id=agent_id))
    write_text(agent_command_guide_path(state=state, role=role), openclaw_agent_guide_text(run_dir=run_dir, state=state, role=role))


def session_prompt_text(*, run_dir: Path, state: dict[str, Any], role: str, agent_id: str) -> str:
    header = [
        f"You are the fixed {role_display_name(role)} agent for this eco-council workflow.",
        f"OpenClaw agent id: {agent_id}",
        "",
        "Role rules:",
    ]
    if role == "moderator":
        rules = [
            "1. Stay in role for the full run.",
            "2. Only work on the JSON file/object explicitly requested by the supervisor.",
            "3. For task review turns, return only a JSON list of round-task objects.",
            "4. For decision turns, return only one JSON object shaped like council-decision.",
            "5. Never add markdown, prose, or code fences.",
            "6. If a referenced local skill is unavailable in this OpenClaw instance, follow the referenced file as the source of truth anyway.",
            "7. If compact historical-case context is provided, use it only to prioritize work and avoid redundant fetch requests; never treat it as current-round evidence.",
        ]
    else:
        rules = [
            "1. Stay in role for the full run.",
            "2. Only work on the source-selection packet or report packet explicitly requested by the supervisor.",
            "3. For source-selection turns, return only one JSON object shaped like source-selection.",
            "4. For report turns, return only one JSON object shaped like expert-report.",
            "5. Never add markdown, prose, or code fences.",
            "6. Do not invent new raw data fetch results in the report stage.",
            "7. If a referenced local skill is unavailable in this OpenClaw instance, follow the referenced file as the source of truth anyway.",
            "8. `recommended_next_actions` must be a list of objects with `assigned_role`, `objective`, and `reason`; use [] when there are no recommendations.",
        ]
    command_notes = [
        "",
        "Supervisor command boundaries:",
        f"- Command guide: {agent_command_guide_path(state=state, role=role)}",
        f"- Safe read-only status command: {supervisor_status_command(run_dir)}",
        "- Use validation commands from the active prompt or packet when they are explicitly requested.",
        "- Do not call continue-run, run-agent-step, init-run, provision-openclaw-agents, or any import-* command unless the human explicitly asks you to act as the supervisor operator.",
        "- Raw fetch shell execution stays under supervisor control. Your role turns return JSON only.",
    ]
    return "\n".join(header + rules + command_notes)


def role_prompt_outbox_text(*, role: str, round_id: str, prompt_path: Path, history_path: Path | None = None) -> str:
    lines = [
        f"This is your current eco-council turn for {round_id}.",
        "",
        "Open and follow this file exactly:",
        str(prompt_path),
        "",
        "If this OpenClaw instance cannot open local files directly, ask the human to paste the file contents and then continue.",
        "Return only JSON.",
    ]
    if role == "moderator" and history_path is not None and history_path.exists():
        lines.extend(
            [
                "",
                "Also review this compact historical-case context before answering:",
                str(history_path),
                "Use it only as planning guidance. Current-round evidence remains primary.",
            ]
        )
    if role == "moderator":
        lines.insert(0, "Use your moderator session rules.")
    else:
        lines.insert(0, f"Use your {role} session rules.")
    return "\n".join(lines)


def build_source_selection_packet(run_dir: Path, round_id: str, role: str) -> Path:
    mission_payload = read_json(mission_path(run_dir))
    if not isinstance(mission_payload, dict):
        raise ValueError(f"Mission payload is not a JSON object: {mission_path(run_dir)}")
    task_payload = read_json(tasks_path(run_dir, round_id))
    tasks = task_payload if isinstance(task_payload, list) else []
    role_tasks = [item for item in tasks_for_role(tasks, role) if isinstance(item, dict)]
    packet = {
        "schema_version": SCHEMA_VERSION,
        "packet_kind": "eco-council-source-selection-packet",
        "run_id": maybe_text(mission_payload.get("run_id")),
        "round_id": round_id,
        "agent_role": role,
        "mission": mission_payload,
        "tasks": role_tasks,
        "allowed_sources": source_policy_for_role(mission_payload, role),
        "current_source_selection": load_json_if_exists(source_selection_path(run_dir, round_id, role)),
    }
    target = source_selection_packet_path(run_dir, round_id, role)
    write_json(target, packet, pretty=True)
    return target


def render_source_selection_prompt(run_dir: Path, round_id: str, role: str) -> Path:
    packet_path = build_source_selection_packet(run_dir, round_id, role)
    target_path = source_selection_path(run_dir, round_id, role)
    validate_command = (
        "python3 "
        + str(CONTRACT_SCRIPT)
        + " validate --kind source-selection --input "
        + str(target_path)
        + " --pretty"
    )
    lines = [
        "Use $eco-council-data-contract.",
        f"Open source-selection packet at: {packet_path}",
        f"Write the canonical source-selection object at: {target_path}",
        "",
        "Review whether your role needs any raw-data fetch sources before prepare-round.",
        "Requirements:",
        "1. Return exactly one valid source-selection JSON object.",
        "2. Keep run_id, round_id, agent_role, task_ids, and allowed_sources aligned with the packet unless the packet itself is stale.",
        "3. selected_sources must be a subset of allowed_sources.",
        "4. Include one source_decisions entry for every allowed source with selected=true or selected=false and one concrete reason.",
        "4a. In each source_decisions item, use the exact key name source_skill (not source).",
        "5. status must be exactly one of: complete, pending, blocked. For a finished selection, use complete (not completed).",
        "6. If no raw fetch is needed, keep selected_sources as [] and explain why in summary.",
        "7. Treat task.inputs.preferred_sources only as hints. They do not auto-run.",
        "8. If the moderator explicitly set task.inputs.required_sources upstream, treat those sources as mandatory forced sources.",
        "",
        "After editing, validate with:",
        validate_command,
        "",
        "Return only the final JSON object.",
    ]
    output_path = source_selection_prompt_path(run_dir, round_id, role)
    write_text(output_path, "\n".join(lines))
    return output_path


def build_current_step_text(run_dir: Path, state: dict[str, Any]) -> str:
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    lines = [
        f"Current round: {round_id}",
        f"Current stage: {stage}",
        "",
    ]
    if stage == STAGE_AWAITING_TASK_REVIEW:
        lines.extend(
            [
                "Preferred: run the moderator turn automatically:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --pretty",
                "",
                "Manual fallback:",
                "1. Open the moderator session prompt:",
                str(session_prompt_path(run_dir, "moderator")),
                "",
                "2. Send this turn prompt to the moderator agent:",
                str(outbox_message_path(run_dir, "moderator_task_review")),
                "",
                "3. Save the moderator JSON reply to any local file, then import it:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-task-review --run-dir "
                + str(run_dir)
                + " --input /path/to/moderator_tasks.json --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_SOURCE_SELECTION:
        lines.extend(
            [
                "Preferred: run the two expert source-selection turns automatically, one by one:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --role sociologist --pretty",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --role environmentalist --pretty",
                "",
                "Manual fallback:",
                "1. Open the sociologist session prompt:",
                str(session_prompt_path(run_dir, "sociologist")),
                "",
                "2. Send this source-selection prompt to the sociologist agent:",
                str(outbox_message_path(run_dir, "sociologist_source_selection")),
                "",
                "3. Import the returned JSON:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-source-selection --run-dir "
                + str(run_dir)
                + " --role sociologist --input /path/to/sociologist_source_selection.json --pretty",
                "",
                "4. Repeat the same pattern for the environmentalist:",
                str(session_prompt_path(run_dir, "environmentalist")),
                str(outbox_message_path(run_dir, "environmentalist_source_selection")),
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-source-selection --run-dir "
                + str(run_dir)
                + " --role environmentalist --input /path/to/environmentalist_source_selection.json --pretty",
            ]
        )
    elif stage == STAGE_READY_PREPARE:
        lines.extend(
            [
                "Run the next approved shell stage:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_READY_FETCH:
        lines.extend(
            [
                "Run the local raw-data fetch plan:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
                "",
                "External/manual alternative:",
                "1. Materialize the raw artifacts and canonical fetch_execution.json with an external runner.",
                "2. Import that fetch execution result:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-fetch-execution --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_READY_DATA_PLANE:
        lines.extend(
            [
                "Run normalization and draft generation:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_REPORTS:
        lines.extend(
            [
                "Preferred: run the two expert turns automatically, one by one:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --role sociologist --pretty",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --role environmentalist --pretty",
                "",
                "Manual fallback:",
                "1. Open the sociologist session prompt:",
                str(session_prompt_path(run_dir, "sociologist")),
                "",
                "2. Send this turn prompt to the sociologist agent:",
                str(outbox_message_path(run_dir, "sociologist_report")),
                "",
                "3. Import the returned JSON:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-report --run-dir "
                + str(run_dir)
                + " --role sociologist --input /path/to/sociologist_report.json --pretty",
                "",
                "4. Repeat the same pattern for the environmentalist:",
                str(session_prompt_path(run_dir, "environmentalist")),
                str(outbox_message_path(run_dir, "environmentalist_report")),
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-report --run-dir "
                + str(run_dir)
                + " --role environmentalist --input /path/to/environmentalist_report.json --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_DECISION:
        lines.extend(
            [
                "Preferred: run the moderator decision turn automatically:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --pretty",
                "",
                "Manual fallback:",
                "1. Open the moderator session prompt:",
                str(session_prompt_path(run_dir, "moderator")),
                "",
                "2. Send this decision turn prompt to the moderator agent:",
                str(outbox_message_path(run_dir, "moderator_decision")),
                "",
                "3. Import the returned JSON:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-decision --run-dir "
                + str(run_dir)
                + " --input /path/to/council_decision.json --pretty",
            ]
        )
    elif stage == STAGE_READY_PROMOTE:
        lines.extend(
            [
                "Promote the approved drafts into canonical files:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_READY_ADVANCE:
        lines.extend(
            [
                "Open the next round after approval:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    else:
        lines.append("Run completed. No further action is required.")
    return "\n".join(lines)


def refresh_supervisor_files(run_dir: Path, state: dict[str, Any]) -> None:
    run_dir = run_dir.expanduser().resolve()
    current_round_id = maybe_text(state.get("current_round_id"))
    if not current_round_id:
        return

    openclaw_section = ensure_openclaw_config(run_dir, state)
    agents = openclaw_section.setdefault("agents", {})

    for role in ROLES:
        role_agent = agents[role]
        agent_id = maybe_text(role_agent.get("id"))
        if not agent_id:
            raise ValueError(f"Missing OpenClaw agent id for role={role}")
        write_openclaw_workspace_files(run_dir=run_dir, state=state, role=role, agent_id=agent_id)
        write_text(
            session_prompt_path(run_dir, role),
            session_prompt_text(run_dir=run_dir, state=state, role=role, agent_id=agent_id),
        )

    history_path = write_history_context_file(run_dir, state, current_round_id)

    outbox_dir = supervisor_outbox_dir(run_dir)
    outbox_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "moderator_task_review",
        "sociologist_source_selection",
        "environmentalist_source_selection",
        "sociologist_report",
        "environmentalist_report",
        "moderator_decision",
    ):
        path = outbox_message_path(run_dir, name)
        if path.exists():
            path.unlink()

    stage = maybe_text(state.get("stage"))
    if stage == STAGE_AWAITING_TASK_REVIEW:
        write_text(
            outbox_message_path(run_dir, "moderator_task_review"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=task_review_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )
    if stage == STAGE_AWAITING_SOURCE_SELECTION:
        for role in SOURCE_SELECTION_ROLES:
            prompt_path = render_source_selection_prompt(run_dir, current_round_id, role)
            write_text(
                outbox_message_path(run_dir, f"{role}_source_selection"),
                role_prompt_outbox_text(
                    role=role,
                    round_id=current_round_id,
                    prompt_path=prompt_path,
                ),
            )
    if stage == STAGE_AWAITING_REPORTS:
        for role in REPORT_ROLES:
            write_text(
                outbox_message_path(run_dir, f"{role}_report"),
                role_prompt_outbox_text(
                    role=role,
                    round_id=current_round_id,
                    prompt_path=report_prompt_path(run_dir, current_round_id, role),
                ),
            )
    if stage == STAGE_AWAITING_DECISION:
        write_text(
            outbox_message_path(run_dir, "moderator_decision"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=decision_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )

    write_text(supervisor_current_step_path(run_dir), build_current_step_text(run_dir, state))


def build_state_payload(*, run_dir: Path, round_id: str, agent_prefix: str) -> dict[str, Any]:
    prefix = normalize_agent_prefix(agent_prefix or run_dir.name)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "current_round_id": round_id,
        "stage": STAGE_AWAITING_TASK_REVIEW,
        "fetch_execution": "supervisor-local-shell",
        "imports": {
            "task_review_received": False,
            "source_selection_roles_received": [],
            "report_roles_received": [],
            "decision_received": False,
        },
        "openclaw": {
            "agent_prefix": prefix,
            "workspace_root": str(supervisor_dir(run_dir) / "openclaw-workspaces"),
            "agents": {
                role: {
                    "id": f"{prefix}-{role}",
                    "workspace": str((supervisor_dir(run_dir) / "openclaw-workspaces" / role).resolve()),
                }
                for role in ROLES
            },
        },
        "history_context": {
            "db": "",
            "top_k": DEFAULT_HISTORY_TOP_K,
        },
        "signal_corpus": {
            "db": "",
            "auto_import": False,
            "last_imported_round_id": "",
            "last_imported_at_utc": "",
            "last_import": {},
        },
        "updated_at_utc": utc_now_iso(),
    }


def build_status_payload(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    run_dir = run_dir.expanduser().resolve()
    ensure_openclaw_config(run_dir, state)
    round_id = maybe_text(state.get("current_round_id"))
    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    stage = maybe_text(state.get("stage"))
    stage_outboxes = {
        STAGE_AWAITING_TASK_REVIEW: ("moderator_task_review",),
        STAGE_AWAITING_SOURCE_SELECTION: ("sociologist_source_selection", "environmentalist_source_selection"),
        STAGE_AWAITING_REPORTS: ("sociologist_report", "environmentalist_report"),
        STAGE_AWAITING_DECISION: ("moderator_decision",),
    }.get(stage, ())

    outbox_paths: dict[str, str] = {}
    for name in stage_outboxes:
        path = outbox_message_path(run_dir, name)
        if path.exists():
            outbox_paths[name] = str(path)

    session_paths = {role: str(session_prompt_path(run_dir, role)) for role in ROLES}
    history = ensure_history_context_config(state)
    signal_corpus = ensure_signal_corpus_config(state)
    history_file = history_context_path(run_dir, round_id) if round_id else None
    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "current_round_id": round_id,
        "stage": stage,
        "fetch_execution": maybe_text(state.get("fetch_execution")),
        "imports": {
            "task_review_received": bool(imports.get("task_review_received")),
            "source_selection_roles_received": sorted(
                {
                    maybe_text(role)
                    for role in imports.get("source_selection_roles_received", [])
                    if maybe_text(role)
                }
            ),
            "report_roles_received": sorted(
                {maybe_text(role) for role in imports.get("report_roles_received", []) if maybe_text(role)}
            ),
            "decision_received": bool(imports.get("decision_received")),
        },
        "task_review_prompt_path": str(task_review_prompt_path(run_dir, round_id)),
        "source_selection_paths": {
            role: str(source_selection_path(run_dir, round_id, role))
            for role in SOURCE_SELECTION_ROLES
        },
        "source_selection_prompt_paths": {
            role: str(source_selection_prompt_path(run_dir, round_id, role))
            for role in SOURCE_SELECTION_ROLES
        },
        "fetch_plan_path": str(fetch_plan_path(run_dir, round_id)),
        "session_prompt_paths": session_paths,
        "outbox_paths": outbox_paths,
        "current_step_path": str(supervisor_current_step_path(run_dir)),
        "openclaw": state.get("openclaw", {}),
        "history_context": {
            "db": maybe_text(history.get("db")),
            "top_k": normalize_history_top_k(history.get("top_k")),
            "context_path": str(history_file) if history_file is not None and history_file.exists() else "",
        },
        "signal_corpus": {
            "db": maybe_text(signal_corpus.get("db")),
            "auto_import": bool(signal_corpus.get("auto_import")),
            "last_imported_round_id": maybe_text(signal_corpus.get("last_imported_round_id")),
            "last_imported_at_utc": maybe_text(signal_corpus.get("last_imported_at_utc")),
            "last_import": signal_corpus.get("last_import", {}),
        },
    }


def maybe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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


def source_policy_for_role(mission: dict[str, Any], role: str) -> list[str]:
    policy = mission.get("source_policy")
    if not isinstance(policy, dict):
        return []
    selected = policy.get(role)
    if not isinstance(selected, list):
        return []
    return unique_strings([maybe_text(item) for item in selected if maybe_text(item)])


def tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def count_json_list(path: Path) -> int:
    payload = load_json_if_exists(path)
    return len(payload) if isinstance(payload, list) else 0


def count_jsonl_records(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def role_label_zh(role: str) -> str:
    return {
        "moderator": "议长",
        "sociologist": "社会学家",
        "environmentalist": "环境数据学家",
    }.get(role, role)


def stage_label_zh(stage: str) -> str:
    return {
        STAGE_AWAITING_TASK_REVIEW: "等待议长复审任务",
        STAGE_AWAITING_SOURCE_SELECTION: "等待专家选择数据源",
        STAGE_READY_PREPARE: "等待生成本轮抓取计划",
        STAGE_READY_FETCH: "等待执行抓取计划",
        STAGE_READY_DATA_PLANE: "等待归一化与报告草稿生成",
        STAGE_AWAITING_REPORTS: "等待专家报告",
        STAGE_AWAITING_DECISION: "等待议长作出决议",
        STAGE_READY_PROMOTE: "等待正式写入本轮产物",
        STAGE_READY_ADVANCE: "等待推进到下一轮",
        STAGE_COMPLETED: "流程已完成",
    }.get(stage, stage or "未知阶段")


def report_status_label_zh(report: dict[str, Any] | None) -> str:
    if not isinstance(report, dict):
        return "未生成"
    summary = maybe_text(report.get("summary")).lower()
    if summary.startswith("pending "):
        return "待执行"
    return {
        "needs-more-evidence": "需要更多证据",
        "supported": "已支持",
        "not-supported": "不支持",
        "blocked": "阻塞",
        "complete": "完成",
    }.get(maybe_text(report.get("status")), maybe_text(report.get("status")) or "未知")


def sufficiency_label_zh(value: str) -> str:
    return {
        "insufficient": "不足",
        "partial": "部分充分",
        "sufficient": "充分",
    }.get(value, value or "未知")


def bool_label_zh(value: Any) -> str:
    return "是" if bool(value) else "否"


def first_nonempty(items: list[str]) -> str:
    for item in items:
        text = maybe_text(item)
        if text:
            return text
    return ""


def format_list_zh(values: list[Any]) -> str:
    items = [maybe_text(value) for value in values if maybe_text(value)]
    return "、".join(items) if items else "无"


def round_number(round_id: str) -> int:
    require_round_id(round_id)
    return int(round_id.split("-")[1])


def infer_fetch_role(status: dict[str, Any]) -> str:
    step_id = maybe_text(status.get("step_id"))
    artifact_path = maybe_text(status.get("artifact_path"))
    for role in REPORT_ROLES:
        if role in step_id or f"/{role}/" in artifact_path:
            return role
    return ""


def round_status_label_zh(*, round_id: str, current_round_id: str, current_stage: str, decision: dict[str, Any] | None, fetch_execution: dict[str, Any] | None) -> str:
    if round_id == current_round_id:
        return stage_label_zh(current_stage)
    if isinstance(decision, dict):
        return "已完成并形成议长决议"
    if isinstance(fetch_execution, dict):
        return "已抓取数据，但尚未形成决议"
    return "已创建，尚未开始"


def default_summary_output_path(run_dir: Path, round_id: str = "", lang: str = "zh") -> Path:
    suffix = "" if lang == "zh" else f".{lang}"
    filename = f"eco_council_record{suffix}.md"
    if round_id:
        filename = f"eco_council_record_{round_id}{suffix}.md"
    return reports_dir(run_dir) / filename


def recommended_commands_for_stage(run_dir: Path, state: dict[str, Any]) -> list[str]:
    stage = maybe_text(state.get("stage"))
    script = f"python3 {SCRIPT_DIR / 'eco_council_supervisor.py'}"
    current_run = str(run_dir)
    if stage == STAGE_AWAITING_TASK_REVIEW:
        return [f"{script} run-agent-step --run-dir {current_run} --role moderator --yes --pretty"]
    if stage == STAGE_AWAITING_SOURCE_SELECTION:
        return [
            f"{script} run-agent-step --run-dir {current_run} --role sociologist --yes --pretty",
            f"{script} run-agent-step --run-dir {current_run} --role environmentalist --yes --pretty",
        ]
    if stage in {STAGE_READY_PREPARE, STAGE_READY_FETCH, STAGE_READY_DATA_PLANE, STAGE_READY_PROMOTE, STAGE_READY_ADVANCE}:
        return [f"{script} continue-run --run-dir {current_run} --yes --pretty"]
    if stage == STAGE_AWAITING_REPORTS:
        return [
            f"{script} run-agent-step --run-dir {current_run} --role sociologist --yes --pretty",
            f"{script} run-agent-step --run-dir {current_run} --role environmentalist --yes --pretty",
        ]
    if stage == STAGE_AWAITING_DECISION:
        return [f"{script} run-agent-step --run-dir {current_run} --role moderator --yes --pretty"]
    return []


def collect_round_summary(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any]:
    current_round_id = maybe_text(state.get("current_round_id"))
    current_stage = maybe_text(state.get("stage"))

    tasks_payload = load_json_if_exists(tasks_path(run_dir, round_id))
    tasks = tasks_payload if isinstance(tasks_payload, list) else []
    fetch_payload = load_json_if_exists(fetch_execution_path(run_dir, round_id))
    fetch = fetch_payload if isinstance(fetch_payload, dict) else {}
    fetch_statuses = fetch.get("statuses") if isinstance(fetch.get("statuses"), list) else []
    decision_payload = load_json_if_exists(decision_target_path(run_dir, round_id))
    decision = decision_payload if isinstance(decision_payload, dict) else None
    source_selections: dict[str, dict[str, Any] | None] = {}
    for role in SOURCE_SELECTION_ROLES:
        selection_payload = load_json_if_exists(source_selection_path(run_dir, round_id, role))
        source_selections[role] = selection_payload if isinstance(selection_payload, dict) else None
    reports: dict[str, dict[str, Any] | None] = {}
    for role in REPORT_ROLES:
        report_payload = load_json_if_exists(report_target_path(run_dir, round_id, role))
        reports[role] = report_payload if isinstance(report_payload, dict) else None

    return {
        "round_id": round_id,
        "round_number": round_number(round_id),
        "is_current_round": round_id == current_round_id,
        "status_label": round_status_label_zh(
            round_id=round_id,
            current_round_id=current_round_id,
            current_stage=current_stage,
            decision=decision,
            fetch_execution=fetch,
        ),
        "tasks": tasks,
        "task_count": len(tasks),
        "fetch": {
            "step_count": maybe_int(fetch.get("step_count")) if fetch else 0,
            "completed_count": maybe_int(fetch.get("completed_count")) if fetch else 0,
            "failed_count": maybe_int(fetch.get("failed_count")) if fetch else 0,
            "statuses": [item for item in fetch_statuses if isinstance(item, dict)],
        },
        "shared": {
            "claim_count": count_json_list(shared_claims_path(run_dir, round_id)),
            "observation_count": count_json_list(shared_observations_path(run_dir, round_id)),
            "evidence_count": count_json_list(shared_evidence_cards_path(run_dir, round_id)),
        },
        "normalized": {
            "public_signal_count": count_jsonl_records(public_signals_path(run_dir, round_id)),
            "environment_signal_count": count_jsonl_records(environment_signals_path(run_dir, round_id)),
        },
        "source_selections": source_selections,
        "reports": reports,
        "decision": decision,
    }


def source_selection_state(summary: dict[str, Any]) -> tuple[bool, int]:
    selections = summary.get("source_selections", {}) if isinstance(summary.get("source_selections"), dict) else {}
    all_complete = True
    selected_count = 0
    for role in SOURCE_SELECTION_ROLES:
        payload = selections.get(role)
        if not isinstance(payload, dict):
            all_complete = False
            continue
        status = maybe_text(payload.get("status"))
        if status not in {"complete", "blocked"}:
            all_complete = False
        selected = payload.get("selected_sources")
        if isinstance(selected, list):
            selected_count += len([item for item in selected if maybe_text(item)])
    return all_complete, selected_count


def build_current_issues_zh(round_summaries: list[dict[str, Any]], state: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    current_round_id = maybe_text(state.get("current_round_id"))
    current_stage = maybe_text(state.get("stage"))
    latest_decision_round = first_nonempty(
        [summary["round_id"] for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)]
    )
    latest_decision = next(
        (summary.get("decision") for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
        None,
    )
    if isinstance(latest_decision, dict):
        missing = latest_decision.get("missing_evidence_types")
        if isinstance(missing, list) and missing:
            issues.append(
                f"最新已完成决议（{latest_decision_round}）认为仍缺少这些证据类型：{format_list_zh(missing)}。"
            )

    for summary in round_summaries:
        fetch = summary.get("fetch", {})
        shared = summary.get("shared", {})
        if maybe_int(fetch.get("completed_count")) > 0 and maybe_int(shared.get("claim_count")) == 0 and maybe_int(shared.get("evidence_count")) == 0:
            issues.append(
                f"{summary['round_id']} 已完成 {maybe_int(fetch.get('completed_count'))} 个抓取步骤，但共享层仍是 claims=0、evidence_cards=0。"
            )

    current_summary = next((summary for summary in round_summaries if summary["round_id"] == current_round_id), None)
    current_stage_allows_fetch = current_stage in {
        STAGE_READY_FETCH,
        STAGE_READY_DATA_PLANE,
        STAGE_AWAITING_REPORTS,
        STAGE_AWAITING_DECISION,
        STAGE_READY_PROMOTE,
        STAGE_READY_ADVANCE,
        STAGE_COMPLETED,
    }
    if isinstance(current_summary, dict) and maybe_int(current_summary.get("fetch", {}).get("step_count")) == 0 and current_stage_allows_fetch:
        selections_complete, selected_count = source_selection_state(current_summary)
        if selections_complete and selected_count == 0:
            pass
        else:
            issues.append(f"{current_round_id} 当前停在“{current_summary['status_label']}”，还没有开始本轮抓取。")

    if not issues:
        issues.append("当前未检测到结构性阻塞，但仍需按阶段继续执行。")
    return issues


def render_run_summary_markdown(
    *,
    run_dir: Path,
    state: dict[str, Any],
    mission: dict[str, Any],
    round_summaries: list[dict[str, Any]],
    lang: str,
) -> str:
    region = mission.get("region", {}) if isinstance(mission.get("region"), dict) else {}
    window = mission.get("window", {}) if isinstance(mission.get("window"), dict) else {}
    constraints = mission.get("constraints", {}) if isinstance(mission.get("constraints"), dict) else {}
    source_policy = mission.get("source_policy", {}) if isinstance(mission.get("source_policy"), dict) else {}
    current_round_id = maybe_text(state.get("current_round_id"))
    current_stage = maybe_text(state.get("stage"))
    latest_decision_summary = next((summary for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)), None)
    latest_decision = latest_decision_summary.get("decision") if isinstance(latest_decision_summary, dict) else None
    if lang not in {"zh", "en"}:
        raise ValueError(f"Unsupported summary language: {lang}")

    def role_label(role: str) -> str:
        if lang == "en":
            return {
                "moderator": "Moderator",
                "sociologist": "Sociologist",
                "environmentalist": "Environmentalist",
            }.get(role, role)
        return role_label_zh(role)

    def stage_label(stage: str) -> str:
        if lang == "en":
            return {
                STAGE_AWAITING_TASK_REVIEW: "Waiting for moderator task review",
                STAGE_AWAITING_SOURCE_SELECTION: "Waiting for expert source selection",
                STAGE_READY_PREPARE: "Waiting to prepare the round fetch plan",
                STAGE_READY_FETCH: "Waiting to execute the fetch plan",
                STAGE_READY_DATA_PLANE: "Waiting to run normalization and report draft generation",
                STAGE_AWAITING_REPORTS: "Waiting for expert reports",
                STAGE_AWAITING_DECISION: "Waiting for moderator decision",
                STAGE_READY_PROMOTE: "Waiting to promote canonical outputs",
                STAGE_READY_ADVANCE: "Waiting to advance to the next round",
                STAGE_COMPLETED: "Workflow completed",
            }.get(stage, stage or "Unknown stage")
        return stage_label_zh(stage)

    def report_status_label(report: dict[str, Any] | None) -> str:
        if lang == "en":
            if not isinstance(report, dict):
                return "Not generated"
            summary_text = maybe_text(report.get("summary")).lower()
            if summary_text.startswith("pending "):
                return "Pending"
            return {
                "needs-more-evidence": "Needs more evidence",
                "supported": "Supported",
                "not-supported": "Not supported",
                "blocked": "Blocked",
                "complete": "Complete",
            }.get(maybe_text(report.get("status")), maybe_text(report.get("status")) or "Unknown")
        return report_status_label_zh(report)

    def sufficiency_label(value: str) -> str:
        if lang == "en":
            return {
                "insufficient": "Insufficient",
                "partial": "Partially sufficient",
                "sufficient": "Sufficient",
            }.get(value, value or "Unknown")
        return sufficiency_label_zh(value)

    def bool_label(value: Any) -> str:
        if lang == "en":
            return "Yes" if bool(value) else "No"
        return bool_label_zh(value)

    def format_list(values: list[Any]) -> str:
        items = [maybe_text(value) for value in values if maybe_text(value)]
        if not items:
            return "None" if lang == "en" else "无"
        return ", ".join(items) if lang == "en" else "、".join(items)

    def round_status_label(summary: dict[str, Any]) -> str:
        if lang == "en":
            if summary.get("is_current_round"):
                return stage_label(current_stage)
            if isinstance(summary.get("decision"), dict):
                return "Completed with moderator decision"
            if maybe_int(summary.get("fetch", {}).get("step_count")) > 0:
                return "Fetched data, decision not completed"
            return "Scaffolded, not started"
        return summary["status_label"]

    def current_issues() -> list[str]:
        if lang == "zh":
            return build_current_issues_zh(round_summaries, state)
        issues: list[str] = []
        latest_decision_round = first_nonempty(
            [summary["round_id"] for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)]
        )
        latest_decision_local = next(
            (summary.get("decision") for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
            None,
        )
        if isinstance(latest_decision_local, dict):
            missing = latest_decision_local.get("missing_evidence_types")
            if isinstance(missing, list) and missing:
                issues.append(
                    f"The latest completed decision ({latest_decision_round}) still marks these evidence types as missing: {format_list(missing)}."
                )
        for summary in round_summaries:
            fetch = summary.get("fetch", {})
            shared = summary.get("shared", {})
            if maybe_int(fetch.get("completed_count")) > 0 and maybe_int(shared.get("claim_count")) == 0 and maybe_int(shared.get("evidence_count")) == 0:
                issues.append(
                    f"{summary['round_id']} completed {maybe_int(fetch.get('completed_count'))} fetch steps, but the shared layer still has claims=0 and evidence_cards=0."
                )
        current_summary = next((summary for summary in round_summaries if summary["round_id"] == current_round_id), None)
        current_stage_allows_fetch = current_stage in {
            STAGE_READY_FETCH,
            STAGE_READY_DATA_PLANE,
            STAGE_AWAITING_REPORTS,
            STAGE_AWAITING_DECISION,
            STAGE_READY_PROMOTE,
            STAGE_READY_ADVANCE,
            STAGE_COMPLETED,
        }
        if isinstance(current_summary, dict) and maybe_int(current_summary.get("fetch", {}).get("step_count")) == 0 and current_stage_allows_fetch:
            selections_complete, selected_count = source_selection_state(current_summary)
            if not (selections_complete and selected_count == 0):
                issues.append(
                    f"{current_round_id} is currently at '{round_status_label(current_summary)}' and has not started round-level fetching yet."
                )
        if not issues:
            issues.append("No structural blocker is currently detected, but the workflow still needs to advance stage by stage.")
        return issues

    labels = {
        "title": "# Eco Council Meeting Record" if lang == "en" else "# 生态议会记录报告",
        "generated_at": "Generated at" if lang == "en" else "生成时间",
        "topic": "Topic" if lang == "en" else "主题",
        "objective": "Objective" if lang == "en" else "目标",
        "region": "Region" if lang == "en" else "区域",
        "window": "Time window" if lang == "en" else "时间窗口",
        "current_round": "Current round" if lang == "en" else "当前轮次",
        "current_stage": "Current stage" if lang == "en" else "当前阶段",
        "round_count": "Round count" if lang == "en" else "轮次数量",
        "state_file": "State file" if lang == "en" else "运行状态文件",
        "constraints": "## Constraints" if lang == "en" else "## 任务边界",
        "max_rounds": "Max rounds" if lang == "en" else "最多轮次",
        "max_tasks": "Max tasks per round" if lang == "en" else "每轮最多任务",
        "max_claims": "Max claims per round" if lang == "en" else "每轮最多 claims",
        "sociologist_sources": "Allowed sociologist sources" if lang == "en" else "社会学家允许源",
        "environmentalist_sources": "Allowed environmentalist sources" if lang == "en" else "环境数据学家允许源",
        "overall": "## Overall Assessment" if lang == "en" else "## 总体判断",
        "latest_decision_round": "Latest completed decision round" if lang == "en" else "最新完成决议轮次",
        "needs_next_round": "Requires next round" if lang == "en" else "是否要求下一轮",
        "evidence_sufficiency": "Evidence sufficiency" if lang == "en" else "证据充分性",
        "completion_score": "Completion score" if lang == "en" else "完成度评分",
        "decision_summary": "Decision summary" if lang == "en" else "决议摘要",
        "missing_evidence": "Missing evidence types" if lang == "en" else "缺失证据类型",
        "no_decision": "- No completed moderator decision is available yet." if lang == "en" else "- 当前尚无已完成的议长决议。",
        "round_records": "## Round Records" if lang == "en" else "## 各轮记录",
        "round_status": "Round status" if lang == "en" else "轮次状态",
        "is_current_round": "Is current round" if lang == "en" else "是否当前轮",
        "task_count": "Task count" if lang == "en" else "任务数量",
        "task_list": "#### Tasks" if lang == "en" else "#### 任务列表",
        "no_tasks": "- No tasks are available for this round yet." if lang == "en" else "- 本轮尚无任务清单。",
        "source_selection": "#### Source Selection" if lang == "en" else "#### 数据源选择",
        "selected_sources": "Selected sources" if lang == "en" else "已选源",
        "no_source_selection": "No source-selection generated." if lang == "en" else "未生成 source-selection。",
        "source": "Sources" if lang == "en" else "来源",
        "depends_on": "Depends on" if lang == "en" else "依赖",
        "fetch": "#### Fetch Execution" if lang == "en" else "#### 数据抓取",
        "fetch_summary": "Total steps" if lang == "en" else "总步骤",
        "completed": "completed" if lang == "en" else "完成",
        "failed": "failed" if lang == "en" else "失败",
        "unknown_role": "Unknown role" if lang == "en" else "未知角色",
        "no_fetch": "- No fetch execution record exists for this round yet." if lang == "en" else "- 本轮尚未生成抓取执行记录。",
        "normalized": "#### Normalization and Shared Layer" if lang == "en" else "#### 归一化与共享层",
        "shared_claims": "Shared claims" if lang == "en" else "共享 claims",
        "shared_observations": "Shared observations" if lang == "en" else "共享 observations",
        "shared_evidence": "Shared evidence cards" if lang == "en" else "共享 evidence cards",
        "public_signals": "Sociologist public signals" if lang == "en" else "社会学家 public signals",
        "environment_signals": "Environmentalist environment signals" if lang == "en" else "环境数据学家 environment signals",
        "reports": "#### Expert Reports" if lang == "en" else "#### 专家报告",
        "no_report": "No report generated." if lang == "en" else "未生成报告。",
        "finding": "Finding" if lang == "en" else "发现",
        "decision": "#### Moderator Decision" if lang == "en" else "#### 议长决议",
        "approved_next_round_tasks": "Approved next-round task count" if lang == "en" else "批准的下一轮任务数",
        "no_round_decision": "- No moderator decision has been finalized for this round yet." if lang == "en" else "- 本轮尚未形成议长决议。",
        "issues": "## Current Issues" if lang == "en" else "## 当前主要问题",
        "next_steps": "## Recommended Next Steps" if lang == "en" else "## 建议下一步",
        "current_action": "Current recommended action" if lang == "en" else "当前建议动作",
        "reference_file": "Reference file" if lang == "en" else "参考文件",
        "recommended_command": "Recommended command" if lang == "en" else "推荐命令",
        "no_command": "- No mandatory follow-up command is required right now." if lang == "en" else "- 当前没有必须执行的后续命令。",
    }

    lines = [
        labels["title"],
        "",
        f"- {labels['generated_at']}：{utc_now_iso()}",
        f"- Run ID：`{maybe_text(mission.get('run_id'))}`",
        f"- {labels['topic']}：{maybe_text(mission.get('topic'))}",
        f"- {labels['objective']}：{maybe_text(mission.get('objective'))}",
        f"- {labels['region']}：{maybe_text(region.get('label'))}",
        f"- {labels['window']}：{maybe_text(window.get('start_utc'))} -> {maybe_text(window.get('end_utc'))}",
        f"- {labels['current_round']}：`{current_round_id}`",
        f"- {labels['current_stage']}：{stage_label(current_stage)}（`{current_stage}`）",
        f"- {labels['round_count']}：{len(round_summaries)}",
        f"- {labels['state_file']}：`{supervisor_state_path(run_dir)}`",
        "",
        labels["constraints"],
        "",
        f"- {labels['max_rounds']}：{maybe_int(constraints.get('max_rounds'))}",
        f"- {labels['max_tasks']}：{maybe_int(constraints.get('max_tasks_per_round'))}",
        f"- {labels['max_claims']}：{maybe_int(constraints.get('max_claims_per_round'))}",
        f"- {labels['sociologist_sources']}：{format_list(source_policy.get('sociologist') if isinstance(source_policy.get('sociologist'), list) else [])}",
        f"- {labels['environmentalist_sources']}：{format_list(source_policy.get('environmentalist') if isinstance(source_policy.get('environmentalist'), list) else [])}",
        "",
        labels["overall"],
        "",
    ]
    if isinstance(latest_decision, dict):
        lines.extend(
            [
                f"- {labels['latest_decision_round']}：`{latest_decision_summary['round_id']}`",
                f"- {labels['needs_next_round']}：{bool_label(latest_decision.get('next_round_required'))}",
                f"- {labels['evidence_sufficiency']}：{sufficiency_label(maybe_text(latest_decision.get('evidence_sufficiency')))}",
                f"- {labels['completion_score']}：{latest_decision.get('completion_score')}",
                f"- {labels['decision_summary']}：{maybe_text(latest_decision.get('decision_summary'))}",
                f"- {labels['missing_evidence']}：{format_list(latest_decision.get('missing_evidence_types') if isinstance(latest_decision.get('missing_evidence_types'), list) else [])}",
            ]
        )
    else:
        lines.append(labels["no_decision"])

    lines.extend(["", labels["round_records"], ""])
    for summary in round_summaries:
        round_heading = (
            f"### Round {summary['round_number']} (`{summary['round_id']}`)"
            if lang == "en"
            else f"### 第 {summary['round_number']} 轮（`{summary['round_id']}`）"
        )
        lines.extend(
            [
                round_heading,
                "",
                f"- {labels['round_status']}：{round_status_label(summary)}",
                f"- {labels['is_current_round']}：{bool_label(summary['is_current_round'])}",
                f"- {labels['task_count']}：{summary['task_count']}",
                "",
                labels["task_list"],
                "",
            ]
        )
        tasks = summary.get("tasks", [])
        if isinstance(tasks, list) and tasks:
            for task in tasks:
                if not isinstance(task, dict):
                    continue
                line = (
                    f"- [{role_label(maybe_text(task.get('assigned_role')))}] `{maybe_text(task.get('task_id'))}`: "
                    if lang == "en"
                    else f"- [{role_label(maybe_text(task.get('assigned_role')))}] `{maybe_text(task.get('task_id'))}`："
                )
                line += (
                    f"{maybe_text(task.get('objective'))} {labels['source']}: "
                    f"{format_list(task.get('inputs', {}).get('preferred_sources') if isinstance(task.get('inputs'), dict) and isinstance(task.get('inputs', {}).get('preferred_sources'), list) else [])}; "
                    f"{labels['depends_on']}: {format_list(task.get('depends_on') if isinstance(task.get('depends_on'), list) else [])}."
                )
                lines.append(line)
        else:
            lines.append(labels["no_tasks"])

        lines.extend(["", labels["source_selection"], ""])
        source_selections = summary.get("source_selections", {}) if isinstance(summary.get("source_selections"), dict) else {}
        for role in SOURCE_SELECTION_ROLES:
            selection = source_selections.get(role)
            if not isinstance(selection, dict):
                lines.append(f"- [{role_label(role)}] {labels['no_source_selection']}")
                continue
            selected_sources = selection.get("selected_sources") if isinstance(selection.get("selected_sources"), list) else []
            line = (
                f"- [{role_label(role)}] {maybe_text(selection.get('status'))}: {maybe_text(selection.get('summary'))} "
                f"{labels['selected_sources']}: {format_list(selected_sources)}."
            )
            lines.append(line)

        lines.extend(["", labels["fetch"], ""])
        fetch = summary.get("fetch", {})
        statuses = fetch.get("statuses", []) if isinstance(fetch.get("statuses"), list) else []
        if maybe_int(fetch.get("step_count")) > 0:
            if lang == "en":
                lines.append(
                    f"- {labels['fetch_summary']}: {maybe_int(fetch.get('step_count'))}; {labels['completed']}: {maybe_int(fetch.get('completed_count'))}; {labels['failed']}: {maybe_int(fetch.get('failed_count'))}."
                )
            else:
                lines.append(
                    f"- {labels['fetch_summary']}：{maybe_int(fetch.get('step_count'))}；{labels['completed']}：{maybe_int(fetch.get('completed_count'))}；{labels['failed']}：{maybe_int(fetch.get('failed_count'))}。"
                )
            for status in statuses:
                if not isinstance(status, dict):
                    continue
                prefix = role_label(infer_fetch_role(status)) or labels["unknown_role"]
                if lang == "en":
                    lines.append(f"- [{prefix}] `{maybe_text(status.get('source_skill'))}`: {maybe_text(status.get('status'))}.")
                else:
                    lines.append(f"- [{prefix}] `{maybe_text(status.get('source_skill'))}`：{maybe_text(status.get('status'))}。")
        else:
            lines.append(labels["no_fetch"])

        shared = summary.get("shared", {})
        normalized = summary.get("normalized", {})
        lines.extend(
            [
                "",
                labels["normalized"],
                "",
                f"- {labels['shared_claims']}：{maybe_int(shared.get('claim_count'))}",
                f"- {labels['shared_observations']}：{maybe_int(shared.get('observation_count'))}",
                f"- {labels['shared_evidence']}：{maybe_int(shared.get('evidence_count'))}",
                f"- {labels['public_signals']}：{maybe_int(normalized.get('public_signal_count'))}",
                f"- {labels['environment_signals']}：{maybe_int(normalized.get('environment_signal_count'))}",
                "",
                labels["reports"],
                "",
            ]
        )
        for role in REPORT_ROLES:
            report = summary.get("reports", {}).get(role) if isinstance(summary.get("reports"), dict) else None
            if not isinstance(report, dict):
                lines.append(f"- [{role_label(role)}] {labels['no_report']}")
                continue
            if lang == "en":
                lines.append(f"- [{role_label(role)}] {report_status_label(report)}: {maybe_text(report.get('summary'))}")
            else:
                lines.append(f"- [{role_label(role)}] {report_status_label(report)}：{maybe_text(report.get('summary'))}")
            findings = report.get("findings") if isinstance(report.get("findings"), list) else []
            for finding in findings[:2]:
                if not isinstance(finding, dict):
                    continue
                title = maybe_text(finding.get("title"))
                summary_text = first_nonempty([title, maybe_text(finding.get("summary"))])
                if summary_text:
                    if lang == "en":
                        lines.append(f"- [{role_label(role)}/{labels['finding']}] {summary_text}")
                    else:
                        lines.append(f"- [{role_label(role)}/{labels['finding']}] {summary_text}")

        lines.extend(["", labels["decision"], ""])
        decision = summary.get("decision")
        if isinstance(decision, dict):
            lines.extend(
                [
                    f"- {labels['needs_next_round']}：{bool_label(decision.get('next_round_required'))}",
                    f"- {labels['evidence_sufficiency']}：{sufficiency_label(maybe_text(decision.get('evidence_sufficiency')))}",
                    f"- {labels['completion_score']}：{decision.get('completion_score')}",
                    f"- {labels['decision_summary']}：{maybe_text(decision.get('decision_summary'))}",
                    f"- {labels['missing_evidence']}：{format_list(decision.get('missing_evidence_types') if isinstance(decision.get('missing_evidence_types'), list) else [])}",
                    f"- {labels['approved_next_round_tasks']}：{len(decision.get('next_round_tasks', [])) if isinstance(decision.get('next_round_tasks'), list) else 0}",
                ]
            )
        else:
            lines.append(labels["no_round_decision"])
        lines.append("")

    lines.extend([labels["issues"], ""])
    for issue in current_issues():
        lines.append(f"- {issue}")

    lines.extend(["", labels["next_steps"], ""])
    lines.append(f"- {labels['current_action']}：{stage_label(current_stage)}。")
    lines.append(f"- {labels['reference_file']}：`{supervisor_current_step_path(run_dir)}`")
    commands = recommended_commands_for_stage(run_dir, state)
    if commands:
        for command in commands:
            lines.append(f"- {labels['recommended_command']}：`{command}`")
    else:
        lines.append(labels["no_command"])
    return "\n".join(lines).rstrip() + "\n"


def ask_for_approval(summary: str, *, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        raise ValueError("Approval is required. Rerun in a terminal or pass --yes.")
    reply = input(f"{summary}\nContinue? [y/N]: ").strip().lower()
    return reply in {"y", "yes"}


def validate_input_file(kind: str, input_path: Path) -> None:
    payload = run_json_command(
        [
            "python3",
            str(CONTRACT_SCRIPT),
            "validate",
            "--kind",
            kind,
            "--input",
            str(input_path),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    validation_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    validation = validation_payload.get("validation") if isinstance(validation_payload, dict) else None
    if not isinstance(validation, dict):
        raise RuntimeError(f"Schema validation returned an unexpected payload for {input_path}")
    if validation.get("ok"):
        return
    issues = validation.get("issues") if isinstance(validation.get("issues"), list) else []
    snippets: list[str] = []
    for issue in issues[:5]:
        if not isinstance(issue, dict):
            continue
        path = maybe_text(issue.get("path")) or "<root>"
        message = maybe_text(issue.get("message")) or "Validation failed."
        snippets.append(f"{path}: {message}")
    detail = "; ".join(snippets) if snippets else "Validation failed without issue details."
    raise ValueError(f"Invalid {kind}: {detail}")


def normalize_source_selection_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    normalized = json.loads(json.dumps(payload))
    status = maybe_text(normalized.get("status")).casefold()
    status_aliases = {
        "completed": "complete",
        "complete": "complete",
        "done": "complete",
        "finished": "complete",
        "in_progress": "pending",
        "in-progress": "pending",
        "pending": "pending",
        "blocked": "blocked",
    }
    if status in status_aliases:
        normalized["status"] = status_aliases[status]

    decisions = normalized.get("source_decisions")
    if isinstance(decisions, list):
        fixed_decisions: list[Any] = []
        for item in decisions:
            if not isinstance(item, dict):
                fixed_decisions.append(item)
                continue
            decision = dict(item)
            if "source_skill" not in decision and "source" in decision:
                decision["source_skill"] = decision.pop("source")
            fixed_decisions.append(decision)
        normalized["source_decisions"] = fixed_decisions

    return normalized


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def ensure_task_review_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, list):
        raise ValueError("Task review payload must be a JSON list.")
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("Each round task must be a JSON object.")
        item_round_id = maybe_text(item.get("round_id"))
        if item_round_id and item_round_id != round_id:
            raise ValueError(f"Task round_id mismatch: expected {round_id}, got {item_round_id}")


def ensure_source_selection_matches(payload: Any, *, round_id: str, role: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Source-selection payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    payload_status = maybe_text(payload.get("status"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Source-selection round_id mismatch: expected {round_id}, got {payload_round_id}")
    if payload_role and payload_role != role:
        raise ValueError(f"Source-selection agent_role mismatch: expected {role}, got {payload_role}")
    if payload_status == "pending":
        raise ValueError("Source-selection payload must not remain pending when imported into the supervisor.")


def ensure_report_matches(payload: Any, *, round_id: str, role: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Report payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Report round_id mismatch: expected {round_id}, got {payload_round_id}")
    if payload_role and payload_role != role:
        raise ValueError(f"Report agent_role mismatch: expected {role}, got {payload_role}")


def ensure_decision_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Decision payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Decision round_id mismatch: expected {round_id}, got {payload_round_id}")


def fetch_status_has_usable_artifact(status: dict[str, Any]) -> bool:
    state = maybe_text(status.get("status"))
    if state == "completed":
        return True
    return state == "skipped" and maybe_text(status.get("reason")) == "artifact_exists"


def fetch_plan_steps(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    payload = read_json(fetch_plan_path(run_dir, round_id))
    if not isinstance(payload, dict):
        raise ValueError("Fetch plan must be a JSON object.")
    steps = payload.get("steps")
    if not isinstance(steps, list):
        raise ValueError("Fetch plan must include a steps list.")
    if not all(isinstance(step, dict) for step in steps):
        raise ValueError("Fetch plan steps must be JSON objects.")
    return [step for step in steps if isinstance(step, dict)]


def resolved_required_path(value: Any, *, label: str) -> Path:
    text = maybe_text(value)
    if not text:
        raise ValueError(f"{label} is missing.")
    return Path(text).expanduser().resolve()


def optional_resolved_path(value: Any) -> Path | None:
    text = maybe_text(value)
    if not text:
        return None
    return Path(text).expanduser().resolve()


def ensure_fetch_execution_matches(payload: Any, *, run_dir: Path, round_id: str, source_path: Path) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Fetch execution payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    if not payload_round_id:
        raise ValueError("Fetch execution payload must include round_id.")
    if payload_round_id != round_id:
        raise ValueError(f"Fetch execution round_id mismatch: expected {round_id}, got {payload_round_id}")
    payload_run_dir = maybe_text(payload.get("run_dir"))
    expected_run_dir = run_dir.expanduser().resolve()
    if payload_run_dir:
        found_run_dir = Path(payload_run_dir).expanduser().resolve()
        if found_run_dir != expected_run_dir:
            raise ValueError(f"Fetch execution run_dir mismatch: expected {expected_run_dir}, got {found_run_dir}")
    payload_plan_path = maybe_text(payload.get("plan_path"))
    expected_plan_path = fetch_plan_path(run_dir, round_id).expanduser().resolve()
    if not payload_plan_path:
        raise ValueError("Fetch execution payload must include plan_path.")
    found_plan_path = Path(payload_plan_path).expanduser().resolve()
    if found_plan_path != expected_plan_path:
        raise ValueError(f"Fetch execution plan_path mismatch: expected {expected_plan_path}, got {found_plan_path}")
    expected_plan_sha256 = file_sha256(expected_plan_path)
    payload_plan_sha256 = maybe_text(payload.get("plan_sha256"))
    if payload_plan_sha256:
        if payload_plan_sha256 != expected_plan_sha256:
            raise ValueError("Fetch execution plan_sha256 does not match the current fetch_plan.json.")
    elif source_path.exists() and source_path.stat().st_mtime_ns < expected_plan_path.stat().st_mtime_ns:
        raise ValueError("Fetch execution appears older than the current fetch_plan.json. Regenerate it from the current round inputs.")
    expected_steps = fetch_plan_steps(run_dir, round_id)
    expected_by_step: dict[str, dict[str, Any]] = {}
    for step in expected_steps:
        step_id = maybe_text(step.get("step_id"))
        if not step_id:
            raise ValueError("Fetch plan contains a step without step_id.")
        if step_id in expected_by_step:
            raise ValueError(f"Fetch plan contains duplicate step_id: {step_id}")
        expected_by_step[step_id] = step
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        raise ValueError("Fetch execution payload must include a statuses list.")
    expected_step_count = len(expected_steps)
    payload_step_count = maybe_int(payload.get("step_count"))
    if payload_step_count != expected_step_count:
        raise ValueError(f"Fetch execution step_count mismatch: expected {expected_step_count}, got {payload_step_count}")
    if len(statuses) != expected_step_count:
        raise ValueError(f"Fetch execution statuses length mismatch: expected {expected_step_count}, got {len(statuses)}")
    failed = [item for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "failed"]
    if failed:
        raise ValueError(f"Fetch execution still contains failed steps: {failed}")
    actual_completed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "completed")
    actual_failed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "failed")
    if maybe_int(payload.get("completed_count")) != actual_completed:
        raise ValueError(
            f"Fetch execution completed_count mismatch: expected {actual_completed}, got {maybe_int(payload.get('completed_count'))}"
        )
    if maybe_int(payload.get("failed_count")) != actual_failed:
        raise ValueError(
            f"Fetch execution failed_count mismatch: expected {actual_failed}, got {maybe_int(payload.get('failed_count'))}"
        )
    seen_step_ids: set[str] = set()
    for status in statuses:
        if not isinstance(status, dict):
            raise ValueError("Fetch execution statuses must be JSON objects.")
        step_id = maybe_text(status.get("step_id"))
        if not step_id:
            raise ValueError(f"Fetch execution status is missing step_id: {status}")
        if step_id in seen_step_ids:
            raise ValueError(f"Fetch execution contains duplicate status for step_id: {step_id}")
        seen_step_ids.add(step_id)
        expected_step = expected_by_step.get(step_id)
        if expected_step is None:
            raise ValueError(f"Fetch execution contains unexpected step_id: {step_id}")
        expected_role = maybe_text(expected_step.get("role"))
        if maybe_text(status.get("role")) != expected_role:
            raise ValueError(f"Fetch execution role mismatch for {step_id}: expected {expected_role}, got {maybe_text(status.get('role'))}")
        expected_source_skill = maybe_text(expected_step.get("source_skill"))
        if maybe_text(status.get("source_skill")) != expected_source_skill:
            raise ValueError(
                f"Fetch execution source_skill mismatch for {step_id}: "
                f"expected {expected_source_skill}, got {maybe_text(status.get('source_skill'))}"
            )
        if not fetch_status_has_usable_artifact(status):
            raise ValueError(f"Fetch execution step {step_id} is not usable for downstream data-plane import: {status}")
        artifact_path = resolved_required_path(status.get("artifact_path"), label=f"fetch execution {step_id} artifact_path")
        expected_artifact_path = resolved_required_path(expected_step.get("artifact_path"), label=f"fetch plan {step_id} artifact_path")
        if artifact_path != expected_artifact_path:
            raise ValueError(
                f"Fetch execution artifact_path mismatch for {step_id}: expected {expected_artifact_path}, got {artifact_path}"
            )
        if not artifact_path.exists():
            raise ValueError(f"Fetch execution artifact_path does not exist: {artifact_path}")
        status_state = maybe_text(status.get("status"))
        expected_stdout_path = resolved_required_path(expected_step.get("stdout_path"), label=f"fetch plan {step_id} stdout_path")
        expected_stderr_path = resolved_required_path(expected_step.get("stderr_path"), label=f"fetch plan {step_id} stderr_path")
        actual_stdout_path = optional_resolved_path(status.get("stdout_path"))
        actual_stderr_path = optional_resolved_path(status.get("stderr_path"))
        if status_state == "completed":
            if actual_stdout_path is None or actual_stderr_path is None:
                raise ValueError(f"Fetch execution completed step {step_id} must include stdout_path and stderr_path.")
            if actual_stdout_path != expected_stdout_path:
                raise ValueError(
                    f"Fetch execution stdout_path mismatch for {step_id}: expected {expected_stdout_path}, got {actual_stdout_path}"
                )
            if actual_stderr_path != expected_stderr_path:
                raise ValueError(
                    f"Fetch execution stderr_path mismatch for {step_id}: expected {expected_stderr_path}, got {actual_stderr_path}"
                )
            if not actual_stdout_path.exists():
                raise ValueError(f"Fetch execution stdout_path does not exist: {actual_stdout_path}")
            if not actual_stderr_path.exists():
                raise ValueError(f"Fetch execution stderr_path does not exist: {actual_stderr_path}")
        else:
            if actual_stdout_path is not None and actual_stdout_path != expected_stdout_path:
                raise ValueError(
                    f"Fetch execution stdout_path mismatch for skipped step {step_id}: expected {expected_stdout_path}, got {actual_stdout_path}"
                )
            if actual_stderr_path is not None and actual_stderr_path != expected_stderr_path:
                raise ValueError(
                    f"Fetch execution stderr_path mismatch for skipped step {step_id}: expected {expected_stderr_path}, got {actual_stderr_path}"
                )
            if actual_stdout_path is not None and not actual_stdout_path.exists():
                raise ValueError(f"Fetch execution stdout_path does not exist: {actual_stdout_path}")
            if actual_stderr_path is not None and not actual_stderr_path.exists():
                raise ValueError(f"Fetch execution stderr_path does not exist: {actual_stderr_path}")
    missing_step_ids = sorted(set(expected_by_step) - seen_step_ids)
    if missing_step_ids:
        raise ValueError(f"Fetch execution is missing statuses for steps: {missing_step_ids}")


def import_task_review_payload(*, run_dir: Path, state: dict[str, Any], payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_task_review_matches(payload, round_id=round_id)
    target = tasks_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    state["stage"] = STAGE_AWAITING_SOURCE_SELECTION
    state["imports"] = {
        "task_review_received": True,
        "source_selection_roles_received": [],
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {
        "imported_kind": "round-task",
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def import_source_selection_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    payload: Any,
    source_path: Path,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_source_selection_matches(payload, round_id=round_id, role=role)
    target = source_selection_path(run_dir, round_id, role)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {
        maybe_text(item)
        for item in imports.get("source_selection_roles_received", [])
        if maybe_text(item)
    }
    received.add(role)
    imports["source_selection_roles_received"] = sorted(received)
    state["imports"] = imports
    state["stage"] = STAGE_READY_PREPARE if received == set(SOURCE_SELECTION_ROLES) else STAGE_AWAITING_SOURCE_SELECTION
    save_state(run_dir, state)
    return {
        "imported_kind": "source-selection",
        "role": role,
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def import_report_payload(*, run_dir: Path, state: dict[str, Any], role: str, payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_report_matches(payload, round_id=round_id, role=role)
    target = report_draft_path(run_dir, round_id, role)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {maybe_text(item) for item in imports.get("report_roles_received", []) if maybe_text(item)}
    received.add(role)
    imports["report_roles_received"] = sorted(received)
    state["imports"] = imports
    state["stage"] = STAGE_AWAITING_DECISION if received == set(REPORT_ROLES) else STAGE_AWAITING_REPORTS
    save_state(run_dir, state)
    return {
        "imported_kind": "expert-report",
        "role": role,
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def import_decision_payload(*, run_dir: Path, state: dict[str, Any], payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_decision_matches(payload, round_id=round_id)
    target = decision_draft_path(run_dir, round_id)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    imports["decision_received"] = True
    state["imports"] = imports
    state["stage"] = STAGE_READY_PROMOTE
    save_state(run_dir, state)
    return {
        "imported_kind": "council-decision",
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def import_fetch_execution_payload(*, run_dir: Path, state: dict[str, Any], payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_fetch_execution_matches(payload, run_dir=run_dir, round_id=round_id, source_path=source_path)
    target = fetch_execution_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    state["fetch_execution"] = "external-import"
    state["stage"] = STAGE_READY_DATA_PLANE
    save_state(run_dir, state)
    return {
        "imported_kind": "fetch-execution",
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def current_agent_turn(*, state: dict[str, Any], requested_role: str) -> tuple[str, str, str]:
    stage = maybe_text(state.get("stage"))
    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    requested = maybe_text(requested_role)

    if stage == STAGE_AWAITING_TASK_REVIEW:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "task-review", "round-task")

    if stage == STAGE_AWAITING_SOURCE_SELECTION:
        missing = [
            role
            for role in SOURCE_SELECTION_ROLES
            if role not in {maybe_text(item) for item in imports.get("source_selection_roles_received", [])}
        ]
        if requested:
            if requested not in SOURCE_SELECTION_ROLES:
                raise ValueError("Source-selection stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
            return (requested, "source-selection", "source-selection")
        if len(missing) == 1:
            return (missing[0], "source-selection", "source-selection")
        raise ValueError("Current stage needs --role sociologist or --role environmentalist.")

    if stage == STAGE_AWAITING_DECISION:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "decision", "council-decision")

    if stage == STAGE_AWAITING_REPORTS:
        missing = [role for role in REPORT_ROLES if role not in {maybe_text(item) for item in imports.get("report_roles_received", [])}]
        if requested:
            if requested not in REPORT_ROLES:
                raise ValueError("Report stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
            return (requested, "report", "expert-report")
        if len(missing) == 1:
            return (missing[0], "report", "expert-report")
        raise ValueError("Current stage needs --role sociologist or --role environmentalist.")

    raise ValueError(f"Current stage does not accept agent turns: {stage}")


def build_agent_message(*, run_dir: Path, state: dict[str, Any], role: str, turn_kind: str) -> str:
    round_id = maybe_text(state.get("current_round_id"))
    session_text = load_text(session_prompt_path(run_dir, role))
    history_text = ""
    if role == "moderator":
        path = history_context_path(run_dir, round_id)
        if path.exists():
            history_text = load_text(path)

    if turn_kind == "task-review":
        prompt_text = load_text(task_review_prompt_path(run_dir, round_id))
        mission_text = load_text(mission_path(run_dir))
        tasks_text = load_text(tasks_path(run_dir, round_id))
        sections = [
            session_text,
            (
                f"Current automated turn: moderator task review for {round_id}.\n"
                "All referenced file contents are embedded below. Do not ask for filesystem access. "
                "Return only the final JSON list."
            ),
            "=== TASK REVIEW PROMPT ===\n" + prompt_text,
            "=== MISSION.JSON ===\n" + mission_text,
            "=== CURRENT TASKS.JSON ===\n" + tasks_text,
        ]
        if history_text:
            sections.append("=== HISTORICAL CASE CONTEXT ===\n" + history_text)
        return "\n\n".join(sections)

    if turn_kind == "source-selection":
        prompt_text = load_text(source_selection_prompt_path(run_dir, round_id, role))
        packet_text = load_text(source_selection_packet_path(run_dir, round_id, role))
        return "\n\n".join(
            [
                session_text,
                (
                    f"Current automated turn: {role} source selection for {round_id}.\n"
                    "The required packet content is embedded below. Do not ask for filesystem access. "
                    "Return only the final JSON object."
                ),
                "=== SOURCE SELECTION PROMPT ===\n" + prompt_text,
                "=== SOURCE SELECTION PACKET.JSON ===\n" + packet_text,
            ]
        )

    if turn_kind == "report":
        prompt_text = load_text(report_prompt_path(run_dir, round_id, role))
        packet_text = load_text(report_packet_path(run_dir, round_id, role))
        return "\n\n".join(
            [
                session_text,
                (
                    f"Current automated turn: {role} report drafting for {round_id}.\n"
                    "The required packet content is embedded below. Do not ask for filesystem access. "
                    "Return only the final JSON object."
                ),
                "=== REPORT PROMPT ===\n" + prompt_text,
                "=== REPORT PACKET.JSON ===\n" + packet_text,
            ]
        )

    if turn_kind == "decision":
        prompt_text = load_text(decision_prompt_path(run_dir, round_id))
        packet_text = load_text(decision_packet_path(run_dir, round_id))
        sections = [
            session_text,
            (
                f"Current automated turn: moderator decision drafting for {round_id}.\n"
                "The required packet content is embedded below. Do not ask for filesystem access. "
                "Return only the final JSON object."
            ),
            "=== DECISION PROMPT ===\n" + prompt_text,
            "=== DECISION PACKET.JSON ===\n" + packet_text,
        ]
        if history_text:
            sections.append("=== HISTORICAL CASE CONTEXT ===\n" + history_text)
        return "\n\n".join(sections)

    raise ValueError(f"Unsupported agent turn kind: {turn_kind}")


def run_openclaw_agent_turn(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    turn_kind: str,
    schema_kind: str,
    message: str,
    timeout_seconds: int,
    thinking: str,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    agent_id = maybe_text(state.get("openclaw", {}).get("agents", {}).get(role, {}).get("id"))
    if not agent_id:
        raise ValueError(f"No configured OpenClaw agent id for role={role}")

    base_path = response_base_path(run_dir, round_id, role, turn_kind)
    stdout_path = base_path.with_suffix(".stdout.txt")
    stderr_path = base_path.with_suffix(".stderr.txt")
    json_path = base_path.with_suffix(".json")
    stdout_path.parent.mkdir(parents=True, exist_ok=True)

    argv = [
        "openclaw",
        "--no-color",
        "agent",
        "--agent",
        agent_id,
        "--local",
        "--message",
        message,
        "--timeout",
        str(timeout_seconds),
    ]
    if thinking:
        argv.extend(["--thinking", thinking])

    completed = subprocess.run(
        argv,
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        check=False,
    )
    atomic_write_text_file(stdout_path, completed.stdout)
    atomic_write_text_file(stderr_path, completed.stderr)
    if completed.returncode != 0:
        raise RuntimeError(
            f"OpenClaw agent turn failed for role={role}. "
            f"See {stdout_path} and {stderr_path}."
        )

    payload = extract_json_suffix(completed.stdout)
    if schema_kind == "source-selection":
        payload = normalize_source_selection_payload(payload)
    write_json(json_path, payload, pretty=True)
    validate_input_file(schema_kind, json_path)
    return {
        "agent_id": agent_id,
        "role": role,
        "turn_kind": turn_kind,
        "schema_kind": schema_kind,
        "response_json_path": str(json_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "payload": payload,
    }


def command_init_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    mission_input = Path(args.mission_input).expanduser().resolve()
    run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "bootstrap-run",
            "--run-dir",
            str(run_dir),
            "--mission-input",
            str(mission_input),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    round_id = latest_round_id(run_dir)
    state = build_state_payload(run_dir=run_dir, round_id=round_id, agent_prefix=args.agent_prefix)
    apply_history_cli_config(state, args)
    apply_signal_corpus_cli_config(state, args)
    ensure_openclaw_config(run_dir, state, workspace_root_text=args.workspace_root)
    provision_result: dict[str, Any]
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        save_state(run_dir, state)
        if args.no_provision_openclaw:
            provision_result = {
                "approved": False,
                "skipped": True,
                "workspace_root": maybe_text(state.get("openclaw", {}).get("workspace_root")),
                "created_agents": [],
            }
        else:
            try:
                provision_result = provision_openclaw_agents_for_run(
                    run_dir,
                    state=state,
                    workspace_root_text=args.workspace_root,
                    assume_yes=args.yes,
                    require_approval=True,
                )
            except Exception as exc:
                raise RuntimeError(
                    "init-run now provisions OpenClaw agents by default. "
                    "Install/configure OpenClaw, pass --yes in non-interactive mode, or use --no-provision-openclaw to scaffold without agents. "
                    f"Underlying error: {exc}"
                ) from exc
            save_state(run_dir, state)
    payload = build_status_payload(run_dir, state)
    payload["openclaw_provision"] = provision_result
    return payload


def command_status(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if history_cli_updates_requested(args) or signal_corpus_cli_updates_requested(args):
        with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
            state = load_state(run_dir)
            apply_history_cli_config(state, args)
            apply_signal_corpus_cli_config(state, args)
            save_state(run_dir, state)
        return build_status_payload(run_dir, state)
    state = load_state(run_dir)
    return build_status_payload(run_dir, state)


def command_summarize_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    mission = read_json(mission_path(run_dir))
    if not isinstance(mission, dict):
        raise ValueError(f"Mission payload is not a JSON object: {mission_path(run_dir)}")

    if args.round_id:
        require_round_id(args.round_id)
        target_round_dir = round_dir(run_dir, args.round_id)
        if not target_round_dir.exists():
            raise ValueError(f"Round directory does not exist: {target_round_dir}")
        round_ids = [args.round_id]
    else:
        round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}")

    round_summaries = [collect_round_summary(run_dir, state, round_id) for round_id in round_ids]
    report_text = render_run_summary_markdown(
        run_dir=run_dir,
        state=state,
        mission=mission,
        round_summaries=round_summaries,
        lang=args.lang,
    )

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = default_summary_output_path(run_dir, args.round_id, args.lang)
    write_text(output_path, report_text)

    latest_decision_round = first_nonempty(
        [summary["round_id"] for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)]
    )
    latest_decision = next(
        (summary.get("decision") for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
        None,
    )
    return {
        "ok": True,
        "run_dir": str(run_dir),
        "output_path": str(output_path),
        "lang": args.lang,
        "current_round_id": maybe_text(state.get("current_round_id")),
        "stage": maybe_text(state.get("stage")),
        "stage_label": stage_label_zh(maybe_text(state.get("stage"))) if args.lang == "zh" else maybe_text(state.get("stage")),
        "round_count": len(round_summaries),
        "round_ids": [summary["round_id"] for summary in round_summaries],
        "latest_decision_round_id": latest_decision_round,
        "latest_decision_requires_next_round": bool(latest_decision.get("next_round_required")) if isinstance(latest_decision, dict) else None,
        "preview": "\n".join(report_text.splitlines()[:20]),
    }


def continue_prepare_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "prepare-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_READY_FETCH
    save_state(run_dir, state)
    return {"action": "prepare-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_execute_fetch(run_dir: Path, state: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "execute-fetch-plan",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    execution_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    failures = [
        item
        for item in execution_payload.get("statuses", [])
        if isinstance(item, dict) and maybe_text(item.get("status")) == "failed"
    ]
    if failures:
        raise RuntimeError(f"Fetch plan reported failed steps. Inspect stderr paths: {failures}")
    state["stage"] = STAGE_READY_DATA_PLANE
    save_state(run_dir, state)
    return {"action": "execute-fetch-plan", "payload": payload, "state": build_status_payload(run_dir, state)}


def maybe_auto_import_signal_corpus(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any] | None:
    signal_corpus = ensure_signal_corpus_config(state)
    db_text = maybe_text(signal_corpus.get("db"))
    if not db_text or not bool(signal_corpus.get("auto_import")):
        return {
            "enabled": bool(db_text),
            "attempted": False,
        }
    attempted_at_utc = utc_now_iso()
    try:
        payload = run_json_command(
            [
                "python3",
                str(SIGNAL_CORPUS_SCRIPT),
                "import-run",
                "--db",
                db_text,
                "--run-dir",
                str(run_dir),
                "--overwrite",
                "--pretty",
            ],
            cwd=REPO_DIR,
        )
        result = {
            "enabled": True,
            "attempted": True,
            "ok": True,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "import_result": payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload,
        }
        signal_corpus["last_imported_round_id"] = round_id
        signal_corpus["last_imported_at_utc"] = attempted_at_utc
    except Exception as exc:  # noqa: BLE001
        result = {
            "enabled": True,
            "attempted": True,
            "ok": False,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "error": str(exc),
        }
    signal_corpus["last_import"] = result
    state["signal_corpus"] = signal_corpus
    return result


def continue_run_data_plane(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "run-data-plane",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    signal_corpus_import = maybe_auto_import_signal_corpus(run_dir, state, round_id)
    if signal_corpus_import is not None and isinstance(payload, dict):
        payload["signal_corpus_import"] = signal_corpus_import
    state["stage"] = STAGE_AWAITING_REPORTS
    state["imports"] = {
        "task_review_received": True,
        "source_selection_roles_received": list(SOURCE_SELECTION_ROLES),
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {"action": "run-data-plane", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_promote(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "promote-all",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--allow-overwrite",
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    decision_payload = read_json(decision_target_path(run_dir, round_id))
    if not isinstance(decision_payload, dict):
        raise ValueError("Canonical moderator decision is not a JSON object after promote-all.")
    if bool(decision_payload.get("next_round_required")):
        state["stage"] = STAGE_READY_ADVANCE
    else:
        state["stage"] = STAGE_COMPLETED
    save_state(run_dir, state)
    return {"action": "promote-all", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_advance_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "advance-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    new_round_id = latest_round_id(run_dir)
    state["current_round_id"] = new_round_id
    state["stage"] = STAGE_AWAITING_TASK_REVIEW
    state["imports"] = {
        "task_review_received": False,
        "source_selection_roles_received": [],
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {"action": "advance-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def command_continue_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    stage = maybe_text(state.get("stage"))
    action_map = {
        STAGE_READY_PREPARE: ("prepare-round", continue_prepare_round),
        STAGE_READY_FETCH: ("execute-fetch-plan", lambda d, s: continue_execute_fetch(d, s, args.timeout_seconds)),
        STAGE_READY_DATA_PLANE: ("run-data-plane", continue_run_data_plane),
        STAGE_READY_PROMOTE: ("promote-all", continue_promote),
        STAGE_READY_ADVANCE: ("advance-round", continue_advance_round),
    }
    action = action_map.get(stage)
    if action is None:
        raise ValueError(f"Current stage does not accept continue-run: {stage}")
    action_name, handler = action
    approved = ask_for_approval(
        f"About to run stage {action_name} for {maybe_text(state.get('current_round_id'))}.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "stage": stage,
            "state": build_status_payload(run_dir, state),
        }
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        locked_state = load_state(run_dir)
        locked_stage = maybe_text(locked_state.get("stage"))
        if locked_stage != stage:
            raise ValueError(f"Stage changed during approval window: expected {stage}, found {locked_stage}. Rerun continue-run.")
        result = handler(run_dir, locked_state)
    result["approved"] = True
    return result


def command_import_task_review(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    validate_input_file("round-task", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_TASK_REVIEW:
            raise ValueError("import-task-review is only allowed while waiting for moderator task review.")
        return import_task_review_payload(run_dir=run_dir, state=state, payload=payload, source_path=input_path)


def command_import_source_selection(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    role = args.role
    payload = normalize_source_selection_payload(read_json(input_path))
    if payload != read_json(input_path):
        write_json(input_path, payload, pretty=True)
    validate_input_file("source-selection", input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_SOURCE_SELECTION:
            raise ValueError("import-source-selection is only allowed while waiting for expert source selection.")
        return import_source_selection_payload(run_dir=run_dir, state=state, role=role, payload=payload, source_path=input_path)


def command_import_report(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    role = args.role
    validate_input_file("expert-report", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) not in {STAGE_AWAITING_REPORTS, STAGE_AWAITING_DECISION}:
            raise ValueError("import-report is only allowed while waiting for expert reports.")
        return import_report_payload(run_dir=run_dir, state=state, role=role, payload=payload, source_path=input_path)


def command_import_decision(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    validate_input_file("council-decision", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_DECISION:
            raise ValueError("import-decision is only allowed while waiting for the moderator decision.")
        return import_decision_payload(run_dir=run_dir, state=state, payload=payload, source_path=input_path)


def command_import_fetch_execution(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_READY_FETCH:
            raise ValueError("import-fetch-execution is only allowed while waiting for fetch execution.")
        round_id = maybe_text(state.get("current_round_id"))
        with exclusive_file_lock(fetch_lock_path(run_dir, round_id)):
            input_path = (
                Path(args.input).expanduser().resolve()
                if args.input
                else fetch_execution_path(run_dir, round_id).expanduser().resolve()
            )
            if not input_path.exists():
                raise ValueError(f"Fetch execution input file does not exist: {input_path}")
            payload = read_json(input_path)
            return import_fetch_execution_payload(run_dir=run_dir, state=state, payload=payload, source_path=input_path)


def command_run_agent_step(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    role, turn_kind, schema_kind = current_agent_turn(state=state, requested_role=args.role)
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    approved = ask_for_approval(
        f"About to run OpenClaw agent turn {turn_kind} for role={role} in {round_id}.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "state": build_status_payload(run_dir, state),
        }

    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        locked_state = load_state(run_dir)
        locked_round_id = maybe_text(locked_state.get("current_round_id"))
        locked_stage = maybe_text(locked_state.get("stage"))
        if locked_round_id != round_id or locked_stage != stage:
            raise ValueError(
                f"Supervisor state changed during approval window: expected round={round_id}, stage={stage}; "
                f"found round={locked_round_id}, stage={locked_stage}. Rerun run-agent-step."
            )
        locked_role, locked_turn_kind, locked_schema_kind = current_agent_turn(state=locked_state, requested_role=args.role)
        if (locked_role, locked_turn_kind, locked_schema_kind) != (role, turn_kind, schema_kind):
            raise ValueError(
                "Requested agent turn is no longer current. "
                f"Expected {(role, turn_kind, schema_kind)!r}, found {(locked_role, locked_turn_kind, locked_schema_kind)!r}."
            )

        message = build_agent_message(run_dir=run_dir, state=locked_state, role=role, turn_kind=turn_kind)
        result = run_openclaw_agent_turn(
            run_dir=run_dir,
            state=locked_state,
            role=role,
            turn_kind=turn_kind,
            schema_kind=schema_kind,
            message=message,
            timeout_seconds=args.timeout_seconds,
            thinking=args.thinking,
        )
        response_path = Path(result["response_json_path"]).resolve()
        payload = result["payload"]
        if schema_kind == "round-task":
            imported = import_task_review_payload(run_dir=run_dir, state=locked_state, payload=payload, source_path=response_path)
        elif schema_kind == "source-selection":
            imported = import_source_selection_payload(
                run_dir=run_dir,
                state=locked_state,
                role=role,
                payload=payload,
                source_path=response_path,
            )
        elif schema_kind == "expert-report":
            imported = import_report_payload(
                run_dir=run_dir,
                state=locked_state,
                role=role,
                payload=payload,
                source_path=response_path,
            )
        elif schema_kind == "council-decision":
            imported = import_decision_payload(run_dir=run_dir, state=locked_state, payload=payload, source_path=response_path)
        else:
            raise ValueError(f"Unsupported schema kind: {schema_kind}")

    return {
        "approved": True,
        "agent_turn": {
            "agent_id": result["agent_id"],
            "role": role,
            "turn_kind": turn_kind,
            "response_json_path": result["response_json_path"],
            "stdout_path": result["stdout_path"],
            "stderr_path": result["stderr_path"],
        },
        "import_result": imported,
    }


def existing_openclaw_agents() -> dict[str, dict[str, Any]]:
    payload = run_json_command(["openclaw", "agents", "list", "--json"], cwd=REPO_DIR)
    if not isinstance(payload, list):
        raise ValueError("Unexpected openclaw agents list payload.")
    output: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        agent_id = maybe_text(item.get("id"))
        if agent_id:
            output[agent_id] = item
    return output


def identity_text(*, role: str, agent_id: str) -> str:
    values = {
        "moderator": {
            "name": "Eco Council Moderator",
            "creature": "procedural council chair",
            "vibe": "skeptical, structured, concise",
            "emoji": "gavel",
        },
        "sociologist": {
            "name": "Eco Council Sociologist",
            "creature": "public-opinion analyst",
            "vibe": "evidence-led, careful, restrained",
            "emoji": "speech",
        },
        "environmentalist": {
            "name": "Eco Council Environmentalist",
            "creature": "physical-signal analyst",
            "vibe": "technical, methodical, cautious",
            "emoji": "globe",
        },
    }[role]
    return "\n".join(
        [
            "# IDENTITY.md - Who Am I?",
            "",
            f"- **Name:** {values['name']}",
            f"- **Creature:** {values['creature']}",
            f"- **Vibe:** {values['vibe']}",
            f"- **Emoji:** {values['emoji']}",
            f"- **Guide:** {OPENCLAW_AGENT_GUIDE_FILENAME}",
            "- **Avatar:**",
            "",
            f"Agent id: {agent_id}",
        ]
    )


def ensure_openclaw_agent(run_dir: Path, *, role: str, state: dict[str, Any]) -> dict[str, Any]:
    openclaw_section = ensure_openclaw_config(run_dir, state)
    agents = openclaw_section.setdefault("agents", {})
    role_info = agents.setdefault(role, {})
    agent_id = maybe_text(role_info.get("id"))
    if not agent_id:
        raise ValueError(f"Missing configured agent id for role {role}")
    write_openclaw_workspace_files(run_dir=run_dir, state=state, role=role, agent_id=agent_id)
    workspace = agent_workspace_path(state, role)

    current_agents = existing_openclaw_agents()
    if agent_id not in current_agents:
        run_json_command(
            [
                "openclaw",
                "agents",
                "add",
                agent_id,
                "--workspace",
                str(workspace),
                "--non-interactive",
                "--json",
            ],
            cwd=REPO_DIR,
        )
    run_json_command(
        [
            "openclaw",
            "agents",
            "set-identity",
            "--agent",
            agent_id,
            "--workspace",
            str(workspace),
            "--from-identity",
            "--json",
        ],
        cwd=REPO_DIR,
    )
    role_info["workspace"] = str(workspace)
    role_info["guide_path"] = str(agent_command_guide_path(state=state, role=role))
    return {
        "role": role,
        "agent_id": agent_id,
        "workspace": str(workspace),
        "guide_path": maybe_text(role_info.get("guide_path")),
    }


def provision_openclaw_agents_for_run(
    run_dir: Path,
    *,
    state: dict[str, Any],
    workspace_root_text: str,
    assume_yes: bool,
    require_approval: bool = False,
) -> dict[str, Any]:
    openclaw_section = ensure_openclaw_config(run_dir, state, workspace_root_text=workspace_root_text)
    approved = ask_for_approval(
        "About to create or reuse three OpenClaw isolated agents for moderator/sociologist/environmentalist.",
        assume_yes=assume_yes,
    )
    if not approved:
        if require_approval:
            raise ValueError(
                "OpenClaw agent provisioning was declined. Re-run init-run with --yes or pass --no-provision-openclaw to skip agent creation."
            )
        return {
            "approved": False,
            "workspace_root": maybe_text(openclaw_section.get("workspace_root")),
            "created_agents": [],
        }
    created = [ensure_openclaw_agent(run_dir, role=role, state=state) for role in ROLES]
    return {
        "approved": True,
        "workspace_root": maybe_text(openclaw_section.get("workspace_root")),
        "created_agents": created,
    }


def command_provision_openclaw_agents(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    result = provision_openclaw_agents_for_run(
        run_dir,
        state=state,
        workspace_root_text=args.workspace_root,
        assume_yes=args.yes,
    )
    if not result["approved"]:
        return {
            "approved": False,
            "state": build_status_payload(run_dir, state),
        }
    save_state(run_dir, state)
    return {
        "approved": True,
        "workspace_root": result["workspace_root"],
        "created_agents": result["created_agents"],
        "state": build_status_payload(run_dir, state),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an eco-council workflow with approval gates.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_run = sub.add_parser("init-run", help="Bootstrap a run, create supervisor state, and provision OpenClaw agents.")
    init_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    init_run.add_argument("--mission-input", required=True, help="Mission JSON file.")
    init_run.add_argument("--agent-prefix", default="", help="Optional OpenClaw agent id prefix.")
    init_run.add_argument("--workspace-root", default="", help="Optional workspace root for the three OpenClaw agents.")
    init_run.add_argument("--no-provision-openclaw", action="store_true", help="Skip automatic OpenClaw agent provisioning during init-run.")
    init_run.add_argument("--yes", action="store_true", help="Skip interactive approval when provisioning agents.")
    init_run.add_argument("--history-db", default="", help="Optional case-library SQLite path for moderator historical context.")
    init_run.add_argument("--history-top-k", type=int, default=DEFAULT_HISTORY_TOP_K, help="Number of similar historical cases to inject into moderator turns.")
    init_run.add_argument("--signal-corpus-db", default="", help="Optional signal-corpus SQLite path for automatic post-data-plane imports.")
    init_run.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    provision = sub.add_parser("provision-openclaw-agents", help="Create or reuse three isolated OpenClaw agents.")
    provision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    provision.add_argument("--workspace-root", default="", help="Optional workspace root for the three agents.")
    provision.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    provision.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    status = sub.add_parser("status", help="Show current supervisor state.")
    status.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    status.add_argument("--history-db", default="", help="Optional case-library SQLite path to attach for moderator historical context.")
    status.add_argument("--history-top-k", type=int, default=0, help="Optional override for moderator historical-case count.")
    status.add_argument("--disable-history-context", action="store_true", help="Disable moderator historical-case context for this run.")
    status.add_argument("--signal-corpus-db", default="", help="Optional signal-corpus SQLite path to attach for automatic post-data-plane imports.")
    status.add_argument("--disable-signal-corpus-import", action="store_true", help="Disable automatic post-data-plane signal-corpus imports for this run.")
    status.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    summarize = sub.add_parser("summarize-run", help="Render one human-readable run report from the run directory.")
    summarize.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    summarize.add_argument("--round-id", default="", help="Optional round id filter, for example round-001.")
    summarize.add_argument("--lang", default="zh", choices=("zh", "en"), help="Human-readable report language.")
    summarize.add_argument("--output", default="", help="Optional output markdown path.")
    summarize.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    continue_run = sub.add_parser("continue-run", help="Run the next approved local shell stage.")
    continue_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    continue_run.add_argument("--timeout-seconds", type=int, default=600, help="Timeout for execute-fetch-plan.")
    continue_run.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    continue_run.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    run_agent = sub.add_parser("run-agent-step", help="Send the current turn to OpenClaw, receive JSON, and import it.")
    run_agent.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    run_agent.add_argument("--role", default="", choices=("", "moderator", "sociologist", "environmentalist"), help="Optional role override for source-selection or expert-report stages.")
    run_agent.add_argument("--timeout-seconds", type=int, default=600, help="OpenClaw agent timeout.")
    run_agent.add_argument("--thinking", default="low", choices=("off", "minimal", "low", "medium", "high"), help="OpenClaw thinking level.")
    run_agent.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    run_agent.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_task = sub.add_parser("import-task-review", help="Import moderator task-review JSON into tasks.json.")
    import_task.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_task.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_task.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_source_selection = sub.add_parser("import-source-selection", help="Import one source-selection JSON into the canonical role path.")
    import_source_selection.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_source_selection.add_argument("--role", required=True, choices=SOURCE_SELECTION_ROLES, help="Expert role.")
    import_source_selection.add_argument("--input", required=True, help="JSON file returned by the expert agent.")
    import_source_selection.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_report = sub.add_parser("import-report", help="Import one expert-report JSON into the draft path.")
    import_report.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_report.add_argument("--role", required=True, choices=REPORT_ROLES, help="Expert role.")
    import_report.add_argument("--input", required=True, help="JSON file returned by the expert agent.")
    import_report.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_decision = sub.add_parser("import-decision", help="Import moderator decision JSON into the draft path.")
    import_decision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_decision.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_decision.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_fetch_execution = sub.add_parser(
        "import-fetch-execution",
        help="Import canonical fetch_execution.json produced by an external fetch runner.",
    )
    import_fetch_execution.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_fetch_execution.add_argument(
        "--input",
        default="",
        help="Optional fetch execution JSON path. Defaults to the canonical round fetch_execution.json path.",
    )
    import_fetch_execution.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-run": command_init_run,
        "provision-openclaw-agents": command_provision_openclaw_agents,
        "status": command_status,
        "summarize-run": command_summarize_run,
        "continue-run": command_continue_run,
        "run-agent-step": command_run_agent_step,
        "import-task-review": command_import_task_review,
        "import-source-selection": command_import_source_selection,
        "import-report": command_import_report,
        "import-decision": command_import_decision,
        "import-fetch-execution": command_import_fetch_execution,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"ok": False, "error": str(exc)}, pretty=True))
        return 1
    print(pretty_json(payload, pretty=bool(getattr(args, "pretty", False))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
