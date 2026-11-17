-- Content-addressed rule identity + per-document reconciliation.
-- Runs automatically on a fresh volume (after 02/03); on an existing database apply it with:
--   docker exec -i bank_postgres psql -U postgres -d bank_aidb -v ON_ERROR_STOP=1 < sql/04_rule_identity.sql
-- Idempotent: safe to re-run.

ALTER TABLE core.bank_rulebook
    -- Rule category (APR, FEE, ...). Previously encoded only as the rule_id prefix.
    ADD COLUMN IF NOT EXISTS category_code TEXT,
    -- File name the rule was extracted from; scopes reconciliation so re-ingesting one
    -- document never deletes another document's rules.
    ADD COLUMN IF NOT EXISTS source_doc    TEXT,
    -- Bumped every time an ingestion run extracts the rule (updated_at only moves when
    -- the content changes). Rules a complete run did not see are stale and get pruned.
    ADD COLUMN IF NOT EXISTS last_seen_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS bank_rulebook_scope_idx
    ON core.bank_rulebook (vendor_id, card_tier, source_doc, last_seen_at);

-- The loader prunes stale rules; embedding_job rows follow via ON DELETE CASCADE.
GRANT DELETE ON core.bank_rulebook TO rule_loader;
