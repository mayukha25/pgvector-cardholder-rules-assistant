import os
import json
import time
import select
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictCursor
import ollama

# Required, no default: credentials never live in code. docker-compose.yml builds it from .env.
DB_URI = os.getenv("DB_URI")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
MODEL_ID = os.getenv("MODEL_ID", "nomic-embed-text")

# nomic-embed-text is trained with task prefixes: stored text gets "search_document: ",
# questions get "search_query: " (see orchestrator/app.py). Bump EMBED_SCHEME whenever the
# model or the document text format changes; the sweep then re-embeds every rule.
DOC_PREFIX = "search_document: "
EMBED_SCHEME = f"{MODEL_ID}|prefixed-v1"

# Catch-up sweep: embeds rules whose NOTIFY was missed (worker down), whose job failed,
# or whose vector came from an older scheme. Runs at startup and then on this interval.
SWEEP_INTERVAL_S = int(os.getenv("EMBED_SWEEP_INTERVAL_S", "60"))
SWEEP_BATCH = int(os.getenv("EMBED_SWEEP_BATCH", "200"))


def build_document_text(rule_title: str, bian_domain: str, clause_text: str) -> str:
    return f"{DOC_PREFIX}{rule_title} [{bian_domain}]: {clause_text}"


def embed_rule(cur, client, rule_id: str) -> bool:
    """Embed one rule and close all of its open jobs. Returns True on success."""
    cur.execute("SELECT rule_title, bian_domain, clause_text FROM core.bank_rulebook WHERE rule_id = %s;", (rule_id,))
    row = cur.fetchone()
    if not row:
        return True  # rule was pruned; its jobs went with it (ON DELETE CASCADE)

    text = build_document_text(row["rule_title"], row["bian_domain"], row["clause_text"])
    try:
        vec = client.embeddings(model=MODEL_ID, prompt=text)["embedding"]
        # One statement string = one implicit transaction, even in autocommit mode.
        cur.execute("""
            UPDATE core.bank_rulebook
            SET embedding = %s::vector, embedding_scheme = %s, updated_at = NOW()
            WHERE rule_id = %s;

            UPDATE core.embedding_job
            SET status = 'COMPLETED', attempts = attempts + 1, last_error = NULL, updated_at = NOW()
            WHERE rule_id = %s AND status <> 'COMPLETED';
        """, (vec, EMBED_SCHEME, rule_id, rule_id))
        # No query-cache invalidation here: that cache holds question vectors, which depend
        # only on the question text and QUERY_SCHEME, never on the rules.
        print(f"✅ [Worker Action] Rule {rule_id} embedded.")
        return True
    except Exception as e:
        print(f"❌ [Worker Error] Failed embedding {rule_id}: {e}")
        cur.execute("""
            UPDATE core.embedding_job
            SET status = 'FAILED', attempts = attempts + 1, last_error = %s, updated_at = NOW()
            WHERE rule_id = %s AND status <> 'COMPLETED';
        """, (str(e)[:1000], rule_id))
        return False


def sweep(cur, client) -> None:
    """Embed every active rule that has no vector or a vector from an older scheme.
    Retired rules are skipped; if one is reactivated, the next sweep catches it up."""
    cur.execute("""
        SELECT rule_id FROM core.bank_rulebook
        WHERE retired_at IS NULL
          AND (embedding IS NULL OR embedding_scheme IS DISTINCT FROM %s)
        ORDER BY updated_at
        LIMIT %s;
    """, (EMBED_SCHEME, SWEEP_BATCH))
    rule_ids = [r["rule_id"] for r in cur.fetchall()]
    if not rule_ids:
        return
    print(f"🧹 [Worker Sweep] {len(rule_ids)} rule(s) need embedding (scheme {EMBED_SCHEME}).")
    consecutive_failures = 0
    for rule_id in rule_ids:
        if embed_rule(cur, client, rule_id):
            consecutive_failures = 0
            continue
        consecutive_failures += 1
        # A run of failures usually means Ollama is down: stop and retry next sweep rather
        # than failing every remaining rule. A single bad rule doesn't block the others.
        if consecutive_failures >= 3:
            print("⏸️  [Worker Sweep] 3 failures in a row; will retry next sweep.")
            return


def run_worker():
    if not DB_URI:
        raise SystemExit("DB_URI is not set; it is supplied by docker-compose.yml from .env")
    print("🤖 [Worker Daemon] Connecting to PostgreSQL...")
    conn = psycopg2.connect(DB_URI)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("LISTEN rulebook_events;")
    print(f"📡 [Worker Daemon] Subscribed to 'rulebook_events'. Connected to Ollama at {OLLAMA_HOST}.")
    client = ollama.Client(host=OLLAMA_HOST)

    # LISTEN is active before the first sweep, so nothing inserted in between is lost.
    sweep(cur, client)
    next_sweep = time.monotonic() + SWEEP_INTERVAL_S

    while True:
        if time.monotonic() >= next_sweep:
            sweep(cur, client)
            next_sweep = time.monotonic() + SWEEP_INTERVAL_S

        if select.select([conn], [], [], 5) == ([], [], []):
            continue

        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            event = json.loads(notify.payload)
            print(f"📥 [Event Received] Processing Rule: {event['rule_id']} (Job #{event['job_id']})")
            embed_rule(cur, client, event["rule_id"])


if __name__ == "__main__":
    run_worker()
