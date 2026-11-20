-- Vendor-scoped rule lifecycle: rules are retired, never deleted.
-- Runs automatically on a fresh volume (after 05); on an existing database apply it with:
--   docker exec -i bank_postgres psql -U postgres -d bank_aidb -v ON_ERROR_STOP=1 < sql/06_rule_lifecycle.sql
-- Idempotent: safe to re-run.
--
-- An ingestion run for (vendor, tier) is that vendor's complete current rulebook, across
-- however many PDFs it is split into. Active rules of that vendor+tier the run did not
-- produce are retired: hidden from the assistant but kept, so chat_audit_log.matched_rule_ids
-- always resolves to the exact clause a customer was given. A retired rule that a later
-- snapshot produces again is reactivated by the loader's upsert.

ALTER TABLE core.bank_rulebook
    ADD COLUMN IF NOT EXISTS retired_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retired_reason TEXT;

-- The assistant only ever sees active rules. Same columns as before, so its grant still applies.
CREATE OR REPLACE VIEW api.rulebook_vw AS
    SELECT rule_id, bian_domain, card_tier, vendor_id, rule_title, clause_text, embedding
    FROM core.bank_rulebook
    WHERE retired_at IS NULL;

-- Vector index over active rules only, so retired neighbours never crowd out the top-k.
-- Retired rows keep their embedding: reactivating one needs no re-embed.
DROP INDEX IF EXISTS core.bank_rulebook_embedding_hnsw;
CREATE INDEX IF NOT EXISTS bank_rulebook_embedding_active_hnsw
    ON core.bank_rulebook USING hnsw (embedding vector_cosine_ops)
    WHERE retired_at IS NULL;

-- Reconciliation is now per vendor+tier over active rules, not per source document.
DROP INDEX IF EXISTS core.bank_rulebook_scope_idx;
CREATE INDEX IF NOT EXISTS bank_rulebook_vendor_scope_idx
    ON core.bank_rulebook (vendor_id, card_tier, last_seen_at)
    WHERE retired_at IS NULL;

-- Retiring is an UPDATE; the loader no longer needs to delete anything.
REVOKE DELETE ON core.bank_rulebook FROM rule_loader;
