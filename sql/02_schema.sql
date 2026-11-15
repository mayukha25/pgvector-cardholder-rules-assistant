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

-- Least privilege: the loader writes rules, the worker and assistant only read them.
GRANT USAGE ON SCHEMA core TO rule_loader, embed_worker, mcp_assistant;
GRANT SELECT, INSERT, UPDATE ON core.bank_rulebook TO rule_loader;
GRANT SELECT ON core.bank_rulebook TO embed_worker, mcp_assistant;
