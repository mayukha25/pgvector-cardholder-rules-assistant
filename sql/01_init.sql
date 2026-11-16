-- ============================================================================
-- 01_init.sql
-- Purpose: Initialize pgvector, enterprise schemas (BIAN/TOGAF aligned),
--          policy document storage, HNSW indexing, and hash-chained audit logs.
-- ============================================================================

-- 1. Enable Required Extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2. Define Modular Schemas
CREATE SCHEMA IF NOT EXISTS policy;
CREATE SCHEMA IF NOT EXISTS embeddings;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS api;

-- 3. Cardholder Policy Domain Tables (BIAN Service Domain: Credit Card / Issued Device)
CREATE TABLE IF NOT EXISTS policy.cardholder_agreement (
    id                  BIGSERIAL PRIMARY KEY,
    policy_code         VARCHAR(50) UNIQUE NOT NULL,
    bian_domain         VARCHAR(100) NOT NULL DEFAULT 'Credit Card',
    togaf_layer         VARCHAR(50) NOT NULL DEFAULT 'Business/Data',
    title               TEXT NOT NULL,
    effective_date      DATE NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS policy.rule_clause (
    id                  BIGSERIAL PRIMARY KEY,
    agreement_id        BIGINT NOT NULL
                        REFERENCES policy.cardholder_agreement (id)
                        ON DELETE CASCADE,
    clause_number       VARCHAR(30) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    clause_text         TEXT NOT NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agreement_id, clause_number)
);

-- 4. Semantic Vector Store (Co-located Meaning & Relational Truth)
-- Note: Adjust VECTOR(768) or VECTOR(1536) to match your Llama / embedding model dimensions
CREATE TABLE IF NOT EXISTS embeddings.clause_embedding (
    clause_id           BIGINT PRIMARY KEY
                        REFERENCES policy.rule_clause (id)
                        ON DELETE CASCADE,
    model_name          VARCHAR(100) NOT NULL,
    embedding           VECTOR(768) NOT NULL,
    embedded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- HNSW Index for Cosine Similarity (<=>)
CREATE INDEX IF NOT EXISTS idx_clause_embedding_hnsw
    ON embeddings.clause_embedding
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- GIN Index for Metadata Filtering
CREATE INDEX IF NOT EXISTS idx_rule_clause_metadata_gin
    ON policy.rule_clause
    USING gin (metadata);

-- 5. Immutable, Hash-Chained Audit Log (Governance & Compliance Layer)
CREATE TABLE IF NOT EXISTS audit.decision_log (
    id                  BIGSERIAL PRIMARY KEY,
    session_id          UUID NOT NULL,
    user_query          TEXT NOT NULL,
    retrieved_clause_ids BIGINT[] NOT NULL DEFAULT '{}',
    decision_outcome    VARCHAR(30) NOT NULL
                        CHECK (decision_outcome IN ('APPROVE', 'DENY', 'ROUTE_TO_HUMAN')),
    confidence_score    NUMERIC(5, 4),
    prev_record_hash    TEXT NOT NULL,
    record_hash         TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decision_log_session
    ON audit.decision_log (session_id, created_at);

-- 6. Restricted Application Role (Least-Privilege MCP / Assistant Access)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mcp_assistant') THEN
        CREATE ROLE mcp_assistant NOINHERIT;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA policy, embeddings, audit, api TO mcp_assistant;
GRANT SELECT ON ALL TABLES IN SCHEMA policy, embeddings TO mcp_assistant;
GRANT INSERT, SELECT ON audit.decision_log TO mcp_assistant;
GRANT USAGE, SELECT ON SEQUENCE audit.decision_log_id_seq TO mcp_assistant;