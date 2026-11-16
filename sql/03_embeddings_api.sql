-- Runs once, after 02_schema.sql, on the first start of an empty database volume.
-- Adds vector search, the embedding job queue + NOTIFY trigger used by
-- workers/embedding_worker.py, and the api.* objects used by orchestrator/app.py.

------------------------------------------------------------------------------
-- 1. Embeddings on the rulebook (nomic-embed-text = 768 dimensions)
------------------------------------------------------------------------------
ALTER TABLE core.bank_rulebook ADD COLUMN IF NOT EXISTS embedding vector(768);

CREATE INDEX IF NOT EXISTS bank_rulebook_embedding_hnsw
    ON core.bank_rulebook USING hnsw (embedding vector_cosine_ops);

------------------------------------------------------------------------------
-- 2. Embedding job queue + event trigger
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.embedding_job (
    job_id      BIGSERIAL PRIMARY KEY,
    rule_id     TEXT NOT NULL REFERENCES core.bank_rulebook (rule_id) ON DELETE CASCADE,
    status      TEXT NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Queues a job and notifies the worker when a rule is new, its text changed,
-- or it still has no embedding. SECURITY DEFINER so rule_loader needs no
-- access to the job table.
CREATE OR REPLACE FUNCTION core.enqueue_embedding() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = core, pg_temp AS $$
DECLARE
    new_job_id BIGINT;
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.embedding IS NOT NULL
       AND NEW.rule_title  IS NOT DISTINCT FROM OLD.rule_title
       AND NEW.bian_domain IS NOT DISTINCT FROM OLD.bian_domain
       AND NEW.clause_text IS NOT DISTINCT FROM OLD.clause_text THEN
        RETURN NEW;
    END IF;

    INSERT INTO core.embedding_job (rule_id) VALUES (NEW.rule_id)
    RETURNING job_id INTO new_job_id;

    PERFORM pg_notify('rulebook_events',
        json_build_object('rule_id', NEW.rule_id, 'job_id', new_job_id)::text);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS bank_rulebook_enqueue_embedding ON core.bank_rulebook;
CREATE TRIGGER bank_rulebook_enqueue_embedding
    AFTER INSERT OR UPDATE OF rule_title, bian_domain, clause_text
    ON core.bank_rulebook
    FOR EACH ROW EXECUTE FUNCTION core.enqueue_embedding();

------------------------------------------------------------------------------
-- 3. API schema used by the orchestrator
------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS api;

-- Read-only window onto the rulebook; the assistant never touches core.* directly.
CREATE OR REPLACE VIEW api.rulebook_vw AS
    SELECT rule_id, bian_domain, card_tier, vendor_id, rule_title, clause_text, embedding
    FROM core.bank_rulebook;

CREATE TABLE IF NOT EXISTS api.query_embedding_cache (
    query_hash  TEXT PRIMARY KEY,
    raw_query   TEXT NOT NULL,
    model_id    TEXT NOT NULL,
    embedding   vector(768) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
);

CREATE TABLE IF NOT EXISTS api.chat_audit_log (
    log_id              BIGSERIAL PRIMARY KEY,
    session_id          TEXT NOT NULL,
    customer_tier       TEXT NOT NULL,
    user_query          TEXT NOT NULL,
    matched_rule_ids    TEXT[] NOT NULL DEFAULT '{}',
    similarity_scores   DOUBLE PRECISION[] NOT NULL DEFAULT '{}',
    generated_response  TEXT,
    execution_status    TEXT NOT NULL CHECK (execution_status IN ('SUCCESS', 'ABSTAINED', 'ERROR')),
    latency_ms          INTEGER,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

------------------------------------------------------------------------------
-- 4. Least-privilege grants
------------------------------------------------------------------------------
-- Worker: write embeddings, close out jobs, invalidate the query cache.
GRANT UPDATE (embedding, updated_at) ON core.bank_rulebook TO embed_worker;
GRANT SELECT, UPDATE ON core.embedding_job TO embed_worker;
GRANT USAGE ON SCHEMA api TO embed_worker;
GRANT TRUNCATE ON api.query_embedding_cache TO embed_worker;

-- Assistant: read rules through the view, use the cache, append to the audit log.
GRANT USAGE ON SCHEMA api TO mcp_assistant;
GRANT SELECT ON api.rulebook_vw TO mcp_assistant;
GRANT SELECT, INSERT ON api.query_embedding_cache TO mcp_assistant;
GRANT INSERT ON api.chat_audit_log TO mcp_assistant;
GRANT USAGE ON SEQUENCE api.chat_audit_log_log_id_seq TO mcp_assistant;
