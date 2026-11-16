import os
import time
import uuid
import hashlib
from fastapi import FastAPI
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
import ollama

app = FastAPI(title="Bank Rulebook Translator MCP Gateway")

DB_URI = os.getenv("DB_URI", "dbname=bank_aidb user=mcp_assistant password=production_mcp_secret_token host=localhost port=5432")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.70"))
EMBED_MODEL = "nomic-embed-text"
CHAT_MODEL = "llama3.2:3b"
INJECTION_PATTERNS = ("ignore previous", "system prompt", "bypass", "override rule", "disregard")
ESCALATION_PATTERNS = ("waive my fee", "refund my", "reverse the charge", "cancel my penalty", "dispute")

client = ollama.Client(host=OLLAMA_HOST)

class ChatRequest(BaseModel):
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_tier: str = "standard_credit"
    vendor_id: str = "CHASE"
    query: str

def get_query_vector(cur, query_text: str):
    q_hash = hashlib.sha256(query_text.strip().lower().encode("utf-8")).hexdigest()
    cur.execute(
        """
        SELECT embedding
        FROM api.query_embedding_cache
        WHERE query_hash = %s
          AND model_id = %s
          AND expires_at > NOW();
        """,
        (q_hash, EMBED_MODEL),  # <-- Must have 2 items to match the 2 %s placeholders
    )
    cached = cur.fetchone()
    if cached:
        return cached["embedding"]

    res = client.embeddings(model=EMBED_MODEL, prompt=query_text)
    vec = res["embedding"]
    cur.execute(
        """
        INSERT INTO api.query_embedding_cache (query_hash, raw_query, model_id, embedding)
        VALUES (%s, %s, %s, %s::vector)
        ON CONFLICT (query_hash) DO NOTHING;
        """,
        (q_hash, query_text, EMBED_MODEL, vec),  # <-- 4 items for 4 %s placeholders
    )
    return vec

@app.post("/api/v1/chat")
def handle_chat(req: ChatRequest):
    t0 = time.time()
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    status = "SUCCESS"
    matched_rules = []
    scores = []

    try:
        qvec = get_query_vector(cur, req.query)

        cur.execute("""
            SELECT rule_id, bian_domain, rule_title, clause_text,
                   1 - (embedding <=> %s::vector) AS similarity_score
            FROM api.rulebook_vw
            WHERE card_tier = %s AND vendor_id = %s AND embedding IS NOT NULL
            ORDER BY embedding <=> %s::vector ASC
            LIMIT 2;
        """, (qvec, req.customer_tier, req.vendor_id, qvec))

        rows = cur.fetchall()
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
        conn.rollback()  # clear the aborted transaction so the audit insert below can run
        status = "ERROR"
        final_text = f"Internal retrieval error: {str(e)}"
    finally:
        latency = int((time.time() - t0) * 1000)
        cur.execute("""
            INSERT INTO api.chat_audit_log (
                session_id, customer_tier, user_query, matched_rule_ids,
                similarity_scores, generated_response, execution_status, latency_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (req.session_id, req.customer_tier, req.query, matched_rules, scores, final_text, status, latency))
        conn.commit()
        conn.close()

    return {
        "status": status,
        "matched_rules": matched_rules,
        "similarity_score": scores[0] if scores else None,
        "latency_ms": latency,
        "response": final_text
    }

def check_guardrails(query_text: str):
    q_lower = query_text.lower()
    if any(p in q_lower for p in INJECTION_PATTERNS):
        return "ABSTAINED", "Request blocked by security guardrail: instruction override detected."
    if any(p in q_lower for p in ESCALATION_PATTERNS):
        return "ESCALATED", "High-impact account action requested (fee waiver/refund/dispute). Escalating inquiry to a human specialist with full context."
    return None, None