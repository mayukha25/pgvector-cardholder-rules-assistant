-- sql/02_schema.sql
-- Runs once, after 01_roles.sh, on the first start of an empty database volume.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.bank_rulebook (
    rule_id      TEXT PRIMARY KEY,
    bian_domain  TEXT NOT NULL,
    card_tier    TEXT NOT NULL,
    vendor_id    TEXT NOT NULL,
    rule_title   TEXT NOT NULL,
    clause_text  TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_rulebook_tier_vendor
    ON core.bank_rulebook (card_tier, vendor_id);

-- Least privilege: loader writes rules, worker reads rules, assistant uses api.rulebook_vw only.
GRANT USAGE ON SCHEMA core TO rule_loader, embed_worker;
GRANT SELECT, INSERT, UPDATE ON core.bank_rulebook TO rule_loader;
GRANT SELECT ON core.bank_rulebook TO embed_worker;
