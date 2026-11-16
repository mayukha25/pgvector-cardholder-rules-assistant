# workers/embedding_worker.py
import os
import json
import select
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictCursor
import ollama

DB_URI = os.getenv("DB_URI", "dbname=bank_aidb user=embed_worker password=secret_worker_password host=localhost port=5432")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
MODEL_ID = "nomic-embed-text"

def process_single_job(cur, client, rule_id: str, job_id: int):
    print(f"📥 [Worker] Processing Rule: {rule_id} (Job #{job_id})")
    cur.execute(
        "SELECT rule_title, bian_domain, clause_text FROM core.bank_rulebook WHERE rule_id = %s;",
        (rule_id,)
    )
    row = cur.fetchone()
    if not row:
        cur.execute("UPDATE core.embedding_job SET status = 'FAILED', updated_at = NOW() WHERE job_id = %s;", (job_id,))
        return

    rich_text = f"{row['rule_title']} [{row['bian_domain']}]: {row['clause_text']}"
    try:
        res = client.embeddings(model=MODEL_ID, prompt=rich_text)
        vec = res["embedding"]

        cur.execute("""
            UPDATE core.bank_rulebook
            SET embedding = %s::vector, updated_at = NOW()
            WHERE rule_id = %s;

            UPDATE core.embedding_job
            SET status = 'COMPLETED', updated_at = NOW()
            WHERE job_id = %s;

            TRUNCATE TABLE api.query_embedding_cache;
        """, (vec, rule_id, job_id))
        print(f"✅ [Worker] Rule {rule_id} embedded and cache invalidated.")
    except Exception as e:
        print(f"❌ [Worker Error] Failed embedding {rule_id}: {e}")
        cur.execute("UPDATE core.embedding_job SET status = 'FAILED', updated_at = NOW() WHERE job_id = %s;", (job_id,))

def drain_pending_jobs(cur, client):
    cur.execute("""
        SELECT job_id, rule_id
        FROM core.embedding_job
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT 25
        FOR UPDATE SKIP LOCKED;
    """)
    jobs = cur.fetchall()
    for job in jobs:
        process_single_job(cur, client, job["rule_id"], job["job_id"])

def run_worker():
    print("🤖 [Worker Daemon] Connecting to PostgreSQL...")
    conn = psycopg2.connect(DB_URI)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("LISTEN rulebook_events;")
    print(f"📡 [Worker Daemon] Subscribed to 'rulebook_events'. Connected to Ollama at {OLLAMA_HOST}.")
    client = ollama.Client(host=OLLAMA_HOST)

    # Sweep any jobs queued before the worker came online
    drain_pending_jobs(cur, client)

    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            drain_pending_jobs(cur, client)
            continue

        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            event = json.loads(notify.payload)
            process_single_job(cur, client, event["rule_id"], event["job_id"])

if __name__ == "__main__":
    run_worker()