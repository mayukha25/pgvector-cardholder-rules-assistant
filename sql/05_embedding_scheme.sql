-- Embedding scheme tracking + job retry bookkeeping.
-- Runs automatically on a fresh volume (after 04); on an existing database apply it with:
--   docker exec -i bank_postgres psql -U postgres -d bank_aidb -v ON_ERROR_STOP=1 < sql/05_embedding_scheme.sql
-- Idempotent: safe to re-run.

-- Which model + input format produced each vector. The worker re-embeds every rule whose
-- scheme differs from its current EMBED_SCHEME, so changing the model or the nomic task
-- prefixes ("search_document: " / "search_query: ") re-embeds the whole rulebook automatically.
-- Existing rows start as NULL and are re-embedded on the worker's next sweep.
ALTER TABLE core.bank_rulebook
    ADD COLUMN IF NOT EXISTS embedding_scheme TEXT;

ALTER TABLE core.embedding_job
    ADD COLUMN IF NOT EXISTS attempts   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- Not in the trigger's UPDATE OF list, so writing it never re-queues a job.
GRANT UPDATE (embedding_scheme) ON core.bank_rulebook TO embed_worker;

-- Question vectors cached before this change were made without the "search_query: " prefix.
TRUNCATE TABLE api.query_embedding_cache;

-- The worker no longer clears the query cache (question vectors don't depend on the rules),
-- so take back the rights 03 used to grant for it on existing databases.
REVOKE TRUNCATE ON api.query_embedding_cache FROM embed_worker;
REVOKE USAGE ON SCHEMA api FROM embed_worker;
