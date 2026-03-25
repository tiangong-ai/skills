PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS missions (
    run_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    topic TEXT NOT NULL,
    objective TEXT NOT NULL,
    policy_profile TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    window_end_utc TEXT NOT NULL,
    region_label TEXT NOT NULL,
    mission_json TEXT NOT NULL CHECK (json_valid(mission_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS rounds (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('planned', 'active', 'completed', 'blocked')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (run_id, round_id),
    FOREIGN KEY (run_id) REFERENCES missions(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS round_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    assigned_role TEXT NOT NULL CHECK (assigned_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    status TEXT NOT NULL CHECK (status IN ('planned', 'in_progress', 'completed', 'blocked')),
    objective TEXT NOT NULL,
    task_json TEXT NOT NULL CHECK (json_valid(task_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS override_requests (
    request_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    request_origin_kind TEXT NOT NULL CHECK (request_origin_kind IN ('source-selection', 'claim-curation', 'observation-curation', 'data-readiness-report', 'expert-report', 'council-decision')),
    target_path TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    claim_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('candidate', 'selected', 'dismissed', 'validated')),
    priority INTEGER NOT NULL,
    needs_physical_validation INTEGER NOT NULL CHECK (needs_physical_validation IN (0, 1)),
    summary TEXT NOT NULL,
    claim_json TEXT NOT NULL CHECK (json_valid(claim_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS claim_submissions (
    submission_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    claim_id TEXT NOT NULL,
    claim_type TEXT NOT NULL,
    worth_storing INTEGER NOT NULL CHECK (worth_storing IN (0, 1)),
    submission_json TEXT NOT NULL CHECK (json_valid(submission_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS claim_curations (
    curation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role = 'sociologist'),
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'blocked')),
    curation_json TEXT NOT NULL CHECK (json_valid(curation_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    source_skill TEXT NOT NULL,
    metric TEXT NOT NULL,
    aggregation TEXT NOT NULL CHECK (aggregation IN ('point', 'window-summary', 'series-summary', 'event-count', 'composite')),
    observation_mode TEXT CHECK (observation_mode IN ('atomic', 'composite')),
    evidence_role TEXT CHECK (evidence_role IN ('primary', 'contextual', 'contradictory', 'mixed')),
    unit TEXT NOT NULL,
    observation_json TEXT NOT NULL CHECK (json_valid(observation_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS observation_submissions (
    submission_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    observation_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    metric TEXT NOT NULL,
    aggregation TEXT NOT NULL CHECK (aggregation IN ('point', 'window-summary', 'series-summary', 'event-count', 'composite')),
    observation_mode TEXT CHECK (observation_mode IN ('atomic', 'composite')),
    evidence_role TEXT CHECK (evidence_role IN ('primary', 'contextual', 'contradictory', 'mixed')),
    worth_storing INTEGER NOT NULL CHECK (worth_storing IN (0, 1)),
    submission_json TEXT NOT NULL CHECK (json_valid(submission_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS observation_curations (
    curation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role = 'environmentalist'),
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'blocked')),
    curation_json TEXT NOT NULL CHECK (json_valid(curation_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evidence_cards (
    evidence_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('supports', 'contradicts', 'mixed', 'insufficient')),
    confidence TEXT NOT NULL CHECK (confidence IN ('low', 'medium', 'high')),
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data_readiness_reports (
    readiness_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    readiness_status TEXT NOT NULL CHECK (readiness_status IN ('ready', 'needs-more-data', 'blocked')),
    sufficient_for_matching INTEGER NOT NULL CHECK (sufficient_for_matching IN (0, 1)),
    report_json TEXT NOT NULL CHECK (json_valid(report_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS matching_authorizations (
    authorization_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    authorization_status TEXT NOT NULL CHECK (authorization_status IN ('authorized', 'deferred', 'not-authorized')),
    moderator_override INTEGER NOT NULL CHECK (moderator_override IN (0, 1)),
    allow_isolated_evidence INTEGER NOT NULL CHECK (allow_isolated_evidence IN (0, 1)),
    authorization_json TEXT NOT NULL CHECK (json_valid(authorization_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS matching_adjudications (
    adjudication_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    candidate_set_id TEXT NOT NULL,
    adjudication_json TEXT NOT NULL CHECK (json_valid(adjudication_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE,
    FOREIGN KEY (authorization_id) REFERENCES matching_authorizations(authorization_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS matching_results (
    result_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    result_status TEXT NOT NULL CHECK (result_status IN ('matched', 'partial', 'unmatched')),
    result_json TEXT NOT NULL CHECK (json_valid(result_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE,
    FOREIGN KEY (authorization_id) REFERENCES matching_authorizations(authorization_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evidence_adjudications (
    adjudication_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    matching_result_id TEXT NOT NULL,
    adjudication_status TEXT NOT NULL CHECK (adjudication_status IN ('complete', 'partial', 'remand-required')),
    needs_additional_data INTEGER NOT NULL CHECK (needs_additional_data IN (0, 1)),
    adjudication_json TEXT NOT NULL CHECK (json_valid(adjudication_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE,
    FOREIGN KEY (authorization_id) REFERENCES matching_authorizations(authorization_id) ON DELETE CASCADE,
    FOREIGN KEY (matching_result_id) REFERENCES matching_results(result_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS isolated_entries (
    isolated_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('claim', 'observation')),
    entity_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    entry_json TEXT NOT NULL CHECK (json_valid(entry_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS remand_entries (
    remand_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('claim', 'observation')),
    entity_id TEXT NOT NULL,
    remand_json TEXT NOT NULL CHECK (json_valid(remand_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS expert_reports (
    report_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    status TEXT NOT NULL CHECK (status IN ('complete', 'needs-more-evidence', 'blocked')),
    report_json TEXT NOT NULL CHECK (json_valid(report_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS council_decisions (
    decision_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    moderator_status TEXT NOT NULL CHECK (moderator_status IN ('continue', 'complete', 'blocked')),
    evidence_sufficiency TEXT NOT NULL CHECK (evidence_sufficiency IN ('sufficient', 'partial', 'insufficient')),
    next_round_required INTEGER NOT NULL CHECK (next_round_required IN (0, 1)),
    decision_json TEXT NOT NULL CHECK (json_valid(decision_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_round_tasks_run_round_role
    ON round_tasks(run_id, round_id, assigned_role);

CREATE INDEX IF NOT EXISTS idx_override_requests_run_round_role
    ON override_requests(run_id, round_id, agent_role);

CREATE INDEX IF NOT EXISTS idx_claims_run_round_type
    ON claims(run_id, round_id, claim_type);

CREATE INDEX IF NOT EXISTS idx_claim_submissions_run_round
    ON claim_submissions(run_id, round_id);

CREATE INDEX IF NOT EXISTS idx_observations_run_round_metric
    ON observations(run_id, round_id, metric);

CREATE INDEX IF NOT EXISTS idx_observation_submissions_run_round_metric
    ON observation_submissions(run_id, round_id, metric);

CREATE INDEX IF NOT EXISTS idx_evidence_cards_run_round_claim
    ON evidence_cards(run_id, round_id, claim_id);

CREATE INDEX IF NOT EXISTS idx_readiness_reports_run_round_role
    ON data_readiness_reports(run_id, round_id, agent_role);

CREATE INDEX IF NOT EXISTS idx_matching_authorizations_run_round
    ON matching_authorizations(run_id, round_id);

CREATE INDEX IF NOT EXISTS idx_matching_results_run_round
    ON matching_results(run_id, round_id);

CREATE INDEX IF NOT EXISTS idx_evidence_adjudications_run_round
    ON evidence_adjudications(run_id, round_id);

CREATE INDEX IF NOT EXISTS idx_isolated_entries_run_round
    ON isolated_entries(run_id, round_id);

CREATE INDEX IF NOT EXISTS idx_remand_entries_run_round
    ON remand_entries(run_id, round_id);

CREATE INDEX IF NOT EXISTS idx_reports_run_round_role
    ON expert_reports(run_id, round_id, agent_role);

CREATE INDEX IF NOT EXISTS idx_decisions_run_round
    ON council_decisions(run_id, round_id);
