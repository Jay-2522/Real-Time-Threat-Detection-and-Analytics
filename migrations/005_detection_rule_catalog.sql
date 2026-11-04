-- The threat engine reports the detection rules it found, enabled or not, so the API can list them without
-- asking a metrics system. Additive and repeatable: existing rows and tables are untouched.
CREATE TABLE IF NOT EXISTS detection_rule_catalog (
    rule_id varchar(128) PRIMARY KEY,
    name varchar(255) NOT NULL,
    severity varchar(32) NOT NULL,
    enabled boolean NOT NULL,
    tactic varchar(128),
    technique varchar(255),
    mitre_id varchar(32),
    description text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);
