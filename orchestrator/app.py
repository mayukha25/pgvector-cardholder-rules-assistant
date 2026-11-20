import os
import time
import uuid
import hashlib
import threading
from contextlib import asynccontextmanager, contextmanager
from fastapi import FastAPI
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
import ollama

DB_URI = os.getenv("DB_URI", "dbname=bank_aidb user=mcp_assistant password=production_mcp_secret_token host=localhost port=5432")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.70"))
# Connections are borrowed only for short DB steps, never across the LLM call, so a small
# pool serves many concurrent chats. Keep DB_POOL_MAX x replicas below Postgres max_connections.
# psycopg2 keeps only DB_POOL_MIN idle connections and closes the rest on return, so MIN
# defaults to MAX: a smaller MIN means reconnecting on every request during bursts.
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", str(DB_POOL_MAX)))
EMBED_MODEL = "nomic-embed-text"
CHAT_MODEL = "llama3.2:3b"
# nomic-embed-text task prefix for questions; rules are embedded with "search_document: "
# (workers/embedding_worker.py). The scheme is part of the cache key so vectors cached
# under an older format are never reused.
QUERY_PREFIX = "search_query: "
QUERY_SCHEME = f"{EMBED_MODEL}|prefixed-v1"

client = ollama.Client(host=OLLAMA_HOST)

# Opened at startup, not import, so the module (and its tests) load without a database.
_pool = None
# ThreadedConnectionPool raises when empty; the semaphore makes callers wait their turn instead.
_pool_slots = threading.BoundedSemaphore(DB_POOL_MAX)


@asynccontextmanager
async def lifespan(_app):
    global _pool
    _pool = ThreadedConnectionPool(DB_POOL_MIN, DB_POOL_MAX, DB_URI)
    try:
        yield
    finally:
        _pool.closeall()


app = FastAPI(title="Bank Rulebook Translator MCP Gateway", lifespan=lifespan)


@contextmanager
def db_cursor():
    """Borrow a pooled connection for one transaction: commit on success, roll back on error.
    A connection that broke (e.g. Postgres restarted) is discarded rather than returned."""
    with _pool_slots:
        conn = _pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass  # connection is dead; conn.closed is now set and it is discarded below
            raise
        finally:
            _pool.putconn(conn, close=bool(conn.closed))


class ChatRequest(BaseModel):
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_tier: str = "standard_credit"
    vendor_id: str = "CHASE"
    query: str

def get_query_vector(cur, query_text: str):
    cache_key = f"{QUERY_SCHEME}\n{query_text.strip().lower()}"
    q_hash = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
    cur.execute("SELECT embedding FROM api.query_embedding_cache WHERE query_hash = %s AND expires_at > NOW();", (q_hash,))
    cached = cur.fetchone()
    if cached:
        return cached["embedding"]

    res = client.embeddings(model=EMBED_MODEL, prompt=QUERY_PREFIX + query_text)
    vec = res["embedding"]
    cur.execute("""
        INSERT INTO api.query_embedding_cache (query_hash, raw_query, model_id, embedding)
        VALUES (%s, %s, %s, %s::vector)
        ON CONFLICT (query_hash) DO NOTHING;
    """, (q_hash, query_text, QUERY_SCHEME, vec))
    return vec

def retrieve_rules(req: "ChatRequest"):
    """Embed the question (cached) and fetch the closest rules, in one short transaction."""
    with db_cursor() as cur:
        qvec = get_query_vector(cur, req.query)
        cur.execute("""
            SELECT rule_id, bian_domain, rule_title, clause_text,
                   1 - (embedding <=> %s::vector) AS similarity_score
            FROM api.rulebook_vw
            WHERE card_tier = %s AND vendor_id = %s AND embedding IS NOT NULL
            ORDER BY embedding <=> %s::vector ASC
            LIMIT 2;
        """, (qvec, req.customer_tier, req.vendor_id, qvec))
        return cur.fetchall()


@app.post("/api/v1/chat")
def handle_chat(req: ChatRequest):
    t0 = time.time()
    status = "SUCCESS"
    matched_rules = []
    scores = []

    try:
        rows = retrieve_rules(req)  # connection is back in the pool before the LLM call
        if rows:
            matched_rules = [r["rule_id"] for r in rows]
            scores = [float(r["similarity_score"]) for r in rows]

        if not rows or scores[0] < CONFIDENCE_THRESHOLD:
            status = "ABSTAINED"
            final_text = "This inquiry falls outside the official credit card terms and conditions in our records. Please contact customer care for assistance."
        else:
            evidence_parts = []
            for r in rows:
                evidence_parts.append(
                    f"• [{r['bian_domain']}] Rule {r['rule_id']} ({r['rule_title']}): {r['clause_text']}"
                )
            evidence = "\n\n".join(evidence_parts)

            prompt = (
                f"OFFICIAL RULES:\n{evidence}\n\n"
                f"CUSTOMER INQUIRY: {req.query}\n\n"
                "Explain the answer clearly and simply using ONLY the rules above. "
                "End your answer with an 'Official Proof:' citing the BIAN Domain and Rule ID."
            )
            chat_res = client.chat(
                model=CHAT_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are the bank's Rulebook Translator. You state facts strictly as written in the official rules without guessing or altering numbers."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            final_text = chat_res["message"]["content"]

    except Exception as e:
        status = "ERROR"
        final_text = f"Internal retrieval error: {str(e)}"

    latency = int((time.time() - t0) * 1000)
    # Every request is audited; if this insert fails the request fails, as before.
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO api.chat_audit_log (
                session_id, customer_tier, user_query, matched_rule_ids,
                similarity_scores, generated_response, execution_status, latency_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (req.session_id, req.customer_tier, req.query, matched_rules, scores, final_text, status, latency))

    return {
        "status": status,
        "matched_rules": matched_rules,
        "similarity_score": scores[0] if scores else None,
        "latency_ms": latency,
        "response": final_text
    }
