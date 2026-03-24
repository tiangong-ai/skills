PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    run_dir TEXT NOT NULL,
    topic TEXT NOT NULL,
    objective TEXT NOT NULL,
    region_label TEXT NOT NULL,
    region_geometry_json TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    window_end_utc TEXT NOT NULL,
    max_rounds INTEGER,
    max_claims_per_round INTEGER,
    max_tasks_per_round INTEGER,
    source_governance_json TEXT NOT NULL,
    current_round_id TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    round_count INTEGER NOT NULL,
    latest_decision_round_id TEXT,
    final_moderator_status TEXT,
    final_evidence_sufficiency TEXT,
    final_decision_summary TEXT,
    final_brief TEXT,
    final_missing_evidence_types_json TEXT NOT NULL,
    latest_claim_count INTEGER NOT NULL,
    latest_observation_count INTEGER NOT NULL,
    latest_evidence_count INTEGER NOT NULL,
    imported_at_utc TEXT NOT NULL,
    mission_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cases_topic ON cases(topic);
CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(final_moderator_status);
CREATE INDEX IF NOT EXISTS idx_cases_region_label ON cases(region_label);
CREATE INDEX IF NOT EXISTS idx_cases_evidence ON cases(final_evidence_sufficiency);
CREATE INDEX IF NOT EXISTS idx_cases_imported_at ON cases(imported_at_utc);

CREATE TABLE IF NOT EXISTS case_rounds (
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    round_number INTEGER NOT NULL,
    is_current_round INTEGER NOT NULL,
    status_label TEXT NOT NULL,
    task_count INTEGER NOT NULL,
    fetch_step_count INTEGER NOT NULL,
    fetch_completed_count INTEGER NOT NULL,
    fetch_failed_count INTEGER NOT NULL,
    claim_count INTEGER NOT NULL,
    observation_count INTEGER NOT NULL,
    evidence_count INTEGER NOT NULL,
    public_signal_count INTEGER NOT NULL,
    environment_signal_count INTEGER NOT NULL,
    report_statuses_json TEXT NOT NULL,
    decision_summary TEXT,
    moderator_status TEXT,
    evidence_sufficiency TEXT,
    next_round_required INTEGER,
    missing_evidence_types_json TEXT NOT NULL,
    decision_json TEXT,
    PRIMARY KEY (case_id, round_id),
    FOREIGN KEY (case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_case_rounds_case_round ON case_rounds(case_id, round_number);

CREATE TABLE IF NOT EXISTS case_reports (
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT,
    summary TEXT,
    report_json TEXT NOT NULL,
    PRIMARY KEY (case_id, round_id, role),
    FOREIGN KEY (case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS case_claims (
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    claim_type TEXT NOT NULL,
    priority INTEGER,
    status TEXT,
    needs_physical_validation INTEGER NOT NULL,
    summary TEXT NOT NULL,
    statement TEXT NOT NULL,
    public_source_skills_json TEXT NOT NULL,
    claim_json TEXT NOT NULL,
    PRIMARY KEY (case_id, round_id, claim_id),
    FOREIGN KEY (case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_case_claims_case_round ON case_claims(case_id, round_id);
CREATE INDEX IF NOT EXISTS idx_case_claims_type ON case_claims(claim_type);

CREATE TABLE IF NOT EXISTS case_observations (
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    metric TEXT NOT NULL,
    aggregation TEXT NOT NULL,
    value REAL,
    unit TEXT NOT NULL,
    quality_flags_json TEXT NOT NULL,
    time_window_json TEXT NOT NULL,
    place_scope_json TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    PRIMARY KEY (case_id, round_id, observation_id),
    FOREIGN KEY (case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_case_observations_case_round ON case_observations(case_id, round_id);
CREATE INDEX IF NOT EXISTS idx_case_observations_metric ON case_observations(metric);

CREATE TABLE IF NOT EXISTS case_evidence (
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    verdict TEXT NOT NULL,
    confidence TEXT NOT NULL,
    summary TEXT NOT NULL,
    gaps_json TEXT NOT NULL,
    observation_ids_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (case_id, round_id, evidence_id),
    FOREIGN KEY (case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_case_evidence_case_round ON case_evidence(case_id, round_id);
CREATE INDEX IF NOT EXISTS idx_case_evidence_claim ON case_evidence(claim_id);
