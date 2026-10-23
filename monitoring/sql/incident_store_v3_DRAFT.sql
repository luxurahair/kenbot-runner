-- Kenbot V3 private control store — DRAFT, NOT APPLIED.
-- Requires review of roles, backups, grants and an isolated staging test.
-- Do not expose kenbot_control in Supabase Data API.
BEGIN;
CREATE SCHEMA IF NOT EXISTS kenbot_control;
REVOKE ALL ON SCHEMA kenbot_control FROM PUBLIC, anon, authenticated;
CREATE TABLE IF NOT EXISTS kenbot_control.incidents (
  incident_id uuid PRIMARY KEY,
  service_id text NOT NULL,
  environment text NOT NULL CHECK (environment IN ('production','staging')),
  tenant_scope text NOT NULL,
  fingerprint text NOT NULL,
  state text NOT NULL CHECK (state IN ('OPEN','OBSERVING','DIAGNOSING','BLOCKED','RESOLVED')),
  first_seen timestamptz NOT NULL,
  last_seen timestamptz NOT NULL,
  resolved_at timestamptz,
  current_sha text,
  evidence_refs jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS kenbot_one_open_incident
ON kenbot_control.incidents(service_id,environment,tenant_scope,fingerprint)
WHERE state <> 'RESOLVED';
CREATE TABLE IF NOT EXISTS kenbot_control.recovery_leases (
  service_id text NOT NULL,
  environment text NOT NULL,
  tenant_scope text NOT NULL,
  lease_id uuid NOT NULL,
  owner text NOT NULL,
  expires_at timestamptz NOT NULL,
  heartbeat_at timestamptz NOT NULL,
  fencing_token bigint NOT NULL CHECK (fencing_token>0),
  PRIMARY KEY(service_id,environment,tenant_scope)
);
CREATE TABLE IF NOT EXISTS kenbot_control.recovery_attempts (
  attempt_id uuid PRIMARY KEY,
  incident_id uuid NOT NULL REFERENCES kenbot_control.incidents(incident_id),
  service_id text NOT NULL,
  environment text NOT NULL,
  tenant_scope text NOT NULL,
  action text NOT NULL CHECK(action IN ('request_safe_restart','rescue_diagnosis_only')),
  executor_identity text NOT NULL,
  lease_id uuid NOT NULL,
  fencing_token bigint NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  result text NOT NULL CHECK(result IN ('STARTED','VERIFIED','FAILED','UNKNOWN'))
);
CREATE INDEX IF NOT EXISTS kenbot_attempt_scope_time
ON kenbot_control.recovery_attempts(service_id,environment,tenant_scope,started_at DESC);
CREATE TABLE IF NOT EXISTS kenbot_control.probe_observations (
  observation_id uuid PRIMARY KEY,
  incident_id uuid NOT NULL REFERENCES kenbot_control.incidents(incident_id),
  observed_at timestamptz NOT NULL,
  source text NOT NULL,
  status text NOT NULL,
  evidence_digest text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS kenbot_control.incident_events (
  event_id uuid PRIMARY KEY,
  incident_id uuid NOT NULL REFERENCES kenbot_control.incidents(incident_id),
  event_at timestamptz NOT NULL DEFAULT now(),
  event_type text NOT NULL,
  actor text NOT NULL,
  correlation_id text NOT NULL,
  payload_redacted jsonb NOT NULL DEFAULT '{}'::jsonb
);
-- Defense in depth. Dedicated DB roles and policies must be provisioned and
-- tested in staging before running any live service with this schema.
ALTER TABLE kenbot_control.incidents ENABLE ROW LEVEL SECURITY;
ALTER TABLE kenbot_control.recovery_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE kenbot_control.recovery_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE kenbot_control.probe_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE kenbot_control.incident_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ALL TABLES IN SCHEMA kenbot_control FROM PUBLIC, anon, authenticated;
COMMIT;
