# pgvector-cardholder-rules-assistant

Using Llama, the credit card policy helping information is engineered through BIAN, Postgres, TOGAF standards.

A fully local, air-gapped assistant that answers cardholder questions **only** from the bank's official card rulebook. Rules are stored in PostgreSQL, embedded with pgvector, retrieved by similarity, and explained by a local Llama model, with every answer citing its BIAN domain and rule ID. If no rule is a confident match, the assistant declines instead of guessing.

---

## Contents

1. [How it works](#how-it-works)
2. [Project layout](#project-layout)
3. [Prerequisites](#prerequisites)
4. [Step-by-step setup and execution](#step-by-step-setup-and-execution)
5. [Day-to-day operations](#day-to-day-operations)
6. [Configuration reference](#configuration-reference)
7. [Troubleshooting](#troubleshooting)
8. [Stopping and resetting](#stopping-and-resetting)

---

## How it works

```
 ingestion/parse_disclosure.py                       curl / client
          │  upsert rules (as rule_loader)                 │ POST /api/v1/chat
          ▼                                                ▼
 ┌─────────────────────────── PostgreSQL 18 + pgvector ─────────────────────────┐
 │ core.bank_rulebook ──trigger──► core.embedding_job                           │
 │        ▲                │                                                    │
 │        │                └── NOTIFY rulebook_events ──┐                       │
 │        │ embedding                                   │                       │
 │ api.rulebook_vw (read-only view)   api.query_embedding_cache                 │
 │ api.chat_audit_log                                   │                       │
 └────────▲─────────────────────────────────────────────┼───────────────────────┘
          │                                             ▼
   orchestrator (FastAPI :8000)                 embedding-worker
   as mcp_assistant                             as embed_worker
          │                                             │
          └──────────────► Ollama (:11434) ◄────────────┘
                 llama3.2:3b (answers)   nomic-embed-text (768-d embeddings)
```

1. **Ingest.** `parse_disclosure.py` upserts rules into `core.bank_rulebook`.
2. **Queue and notify.** A trigger creates a row in `core.embedding_job` and sends `NOTIFY rulebook_events` with the `rule_id` and `job_id`.
3. **Embed.** The worker receives the event, embeds the rule text with `nomic-embed-text`, stores the vector, marks the job `COMPLETED`, and clears the query cache.
4. **Answer.** The orchestrator embeds the question (cached for 24 hours), finds the two closest rules for the customer's card tier and vendor, and then does one of two things:
   - If the best similarity is **≥ `CONFIDENCE_THRESHOLD`** (0.70), `llama3.2:3b` writes an answer using only those rules → `SUCCESS`.
   - Otherwise it returns a fixed "contact customer care" message → `ABSTAINED`.
5. **Audit.** Every request is written to `api.chat_audit_log`.

### Database roles (least privilege)

| Role | Used by | Can do |
|---|---|---|
| `postgres` | Admin / init scripts only | Everything |
| `rule_loader` | `ingestion/parse_disclosure.py` | Insert, update and prune (delete) rules in `core.bank_rulebook` |
| `embed_worker` | `workers/embedding_worker.py` | Read rules; write only `embedding`/`updated_at`; update jobs; truncate the query cache |
| `mcp_assistant` | `orchestrator/app.py` | Read rules **only through** `api.rulebook_vw`; use the query cache; append to the audit log. 2-second statement timeout |

---

## Project layout

```
.
├── docker-compose.yml        # postgres-ai, ollama, embedding-worker*, orchestrator*  (*profile "app")
├── .env                      # generated database passwords (git-ignored, you create it in Step 3)
├── requirements.txt          # host-side Python deps (ingestion script, tests)
├── sql/                      # run automatically, in order, on the FIRST start of an empty DB volume
│   ├── 01_roles.sh           #   creates rule_loader, embed_worker, mcp_assistant
│   ├── 02_schema.sql         #   extensions, core schema, core.bank_rulebook
│   └── 03_embeddings_api.sql #   embedding column + HNSW index, job queue + trigger, api schema, grants
├── ingestion/
│   └── parse_disclosure.py   # loads sample rules (runs on the host)
├── workers/                  # embedding worker image
│   ├── Dockerfile
│   ├── embedding_worker.py
│   └── requirements.txt
└── orchestrator/             # FastAPI chat gateway image
    ├── Dockerfile
    ├── app.py
    └── requirements.txt
```

---

## Prerequisites

| Tool | Version | Check | Install (macOS) |
|---|---|---|---|
| Docker Desktop (with Compose v2) | recent | `docker --version` and `docker compose version` | `brew install --cask docker`, then `open -a Docker` |
| Python | **3.11 or 3.12** | `python3.12 --version` | `brew install python@3.12` |
| Git | any | `git --version` | `xcode-select --install` |
| OpenSSL | any | `openssl version` | preinstalled on macOS |

Also needed:
- About **5 GB free disk** (Postgres and Ollama images, plus about 2.3 GB of models).
- Free ports **5432**, **8000** and **11434**.

> **macOS notes**
> - The system `python3` is 3.9 and will **not** work, because several pinned packages have no 3.9 wheels.
> - If you use conda, run `conda deactivate` first, so `(base)` doesn't shadow the virtual environment's `python`.
> - In-place `sed` on macOS needs an empty suffix: `sed -i '' 's/a/b/' file`.

---

## Step-by-step setup and execution

Run every command from the repository root unless a step says otherwise.

### Step 1: Get the code

```zsh
git clone https://github.com/mayukha25/pgvector-cardholder-rules-assistant.git
cd pgvector-cardholder-rules-assistant
```

### Step 2: Create the Python virtual environment

The ingestion script runs on your machine (not in Docker), so it needs its own environment.

```zsh
conda deactivate 2>/dev/null    # only matters if you use conda
python3.12 -m venv venv
source venv/bin/activate        # your prompt now shows (venv)
python --version                # must print 3.11.x or 3.12.x
pip install --upgrade pip
pip install -r requirements.txt
```

✅ **Check:** `python -c "import psycopg2, ollama, fastapi; print('ok')"` prints `ok`.

> Open a new terminal later? Run `source venv/bin/activate` again before any `python` command.

### Step 3: Generate the secrets file (`.env`)

Docker Compose reads database passwords from `.env`, and so does the ingestion script. The file is git-ignored. Never commit it.

```zsh
( umask 077    # file readable only by you; applies to this block only
cat > .env <<EOF
POSTGRES_PASSWORD=$(openssl rand -hex 16)
LOADER_DB_PASSWORD=$(openssl rand -hex 16)
WORKER_DB_PASSWORD=$(openssl rand -hex 16)
APP_DB_PASSWORD=$(openssl rand -hex 16)
EOF
)
```

✅ **Check:** `cut -d= -f1 .env` lists the four keys, and `git check-ignore .env` prints `.env`.

> ⚠️ Create `.env` **before** the database starts for the first time. Postgres only runs the `sql/` init scripts on an empty volume. If it starts without these passwords, the roles are never created, and you must reset the volume (see [Stopping and resetting](#stopping-and-resetting)).

### Step 4: Start PostgreSQL and Ollama

```zsh
docker compose up -d
```

This starts only `postgres-ai` and `ollama`. The worker and orchestrator are in the `app` profile and start in Step 6. On the first run, Postgres runs `sql/01_roles.sh`, `02_schema.sql` and `03_embeddings_api.sql` in order.

✅ **Check that both containers are up and Postgres is healthy:**

```zsh
docker compose ps
# bank_postgres   ...   Up ... (healthy)
# bank_ollama     ...   Up ...
```

✅ **Check that the init scripts ran:** the roles and tables should exist.

```zsh
docker exec bank_postgres psql -U postgres -d bank_aidb -c '\du' -c '\dt core.*' -c '\dt api.*'
```

You should see the roles `rule_loader`, `embed_worker` and `mcp_assistant`, the tables `core.bank_rulebook` and `core.embedding_job`, and the tables `api.query_embedding_cache` and `api.chat_audit_log`.

If anything is missing, check `docker logs bank_postgres` for an error from a `/docker-entrypoint-initdb.d/` script.

### Step 5: Download the AI models into Ollama

This is a one-time download of about 2.3 GB. The models persist in the `ollama_storage` volume.

```zsh
docker exec bank_ollama ollama pull nomic-embed-text   # embeddings, 768 dimensions (~274 MB)
docker exec bank_ollama ollama pull llama3.2:3b        # answer generation (~2 GB)
```

✅ **Check:** `docker exec bank_ollama ollama list` shows both models.

### Step 6: Build and start the embedding worker and the orchestrator

```zsh
docker compose --profile app up -d --build
```

✅ **Check that the worker is listening for events:**

```zsh
docker logs bank_embedding_worker
# 🤖 [Worker Daemon] Connecting to PostgreSQL...
# 📡 [Worker Daemon] Subscribed to 'rulebook_events'. Connected to Ollama at http://ollama:11434.
```

✅ **Check that the API is up:**

```zsh
docker logs bank_orchestrator | tail -2         # "Uvicorn running on http://0.0.0.0:8000"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/docs    # 200
```

Interactive API docs are at <http://localhost:8000/docs>.

### Step 7: Load the rulebook

Open a **second terminal** and follow the worker's log there:

```zsh
docker logs -f bank_embedding_worker
```

In your first terminal, with the virtual environment active, load the sample rules:

```zsh
python ingestion/parse_disclosure.py
# 📄 Loaded/Updated Rule: FEE-FX-001
# 📄 Loaded/Updated Rule: FEE-LATE-005
```

Within a second or two, the worker terminal shows:

```
📥 [Event Received] Processing Rule: FEE-FX-001 (Job #1)
✅ [Worker Action] Rule FEE-FX-001 embedded and cache invalidated.
📥 [Event Received] Processing Rule: FEE-LATE-005 (Job #2)
✅ [Worker Action] Rule FEE-LATE-005 embedded and cache invalidated.
```

✅ **Check:** every rule has an embedding and every job is complete.

```zsh
docker exec bank_postgres psql -U postgres -d bank_aidb \
  -c "SELECT rule_id, embedding IS NOT NULL AS embedded FROM core.bank_rulebook;" \
  -c "SELECT job_id, rule_id, status FROM core.embedding_job ORDER BY job_id;"
```

The script is safe to re-run. Unchanged rules that already have embeddings are not re-queued.

### Step 8: Ask questions

```zsh
curl -s -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Is there a fee for purchases made in a foreign currency?"}'
```

Expected response (the wording of `response` varies between runs):

```json
{
  "status": "SUCCESS",
  "matched_rules": ["FEE-FX-001", "FEE-LATE-005"],
  "similarity_score": 0.753,
  "latency_ms": 3418,
  "response": "There is a 3.5% currency conversion fee for cross-border transactions settled in a foreign currency.\n\nOfficial Proof: BIAN Domain, Rule FEE-FX-001."
}
```

More questions to try:

| Question | Expected `status` | Why |
|---|---|---|
| `What is the late payment fee?` | `SUCCESS` (score ≈ 0.80) | Directly matches `FEE-LATE-005` |
| `Do you offer airport lounge access?` | `ABSTAINED` (score ≈ 0.44) | No rule covers it, so the assistant declines |
| `What fee will I be charged if I swipe my card for sushi in Tokyo?` | `ABSTAINED` (score ≈ 0.60) | Finds the right rule, but conversational phrasing scores below the 0.70 threshold. See [Tuning the threshold](#tuning-the-confidence-threshold) |

Optional request fields:

| Field | Default | Meaning |
|---|---|---|
| `customer_tier` | `standard_credit` | Only rules for this card tier are searched |
| `vendor_id` | `CHASE` | Only rules for this card issuer are searched |
| `session_id` | new UUID per request | Groups requests in the audit log |

> The first `SUCCESS` call after a restart can take 10–30 seconds while Ollama loads `llama3.2:3b` into memory. Later calls take about 2–4 seconds. `ABSTAINED` responses skip the LLM and return in milliseconds.

### Step 9: Review the audit trail

```zsh
docker exec bank_postgres psql -U postgres -d bank_aidb -c "
  SELECT log_id, execution_status, matched_rule_ids,
         round(similarity_scores[1]::numeric, 3) AS top_score,
         latency_ms, left(user_query, 50) AS query
  FROM api.chat_audit_log ORDER BY log_id DESC LIMIT 10;"
```

---

## Day-to-day operations

### Adding or changing rules

Ingest a document (PDF, TXT, MD or URL), or a whole folder with `--dir`:

```zsh
python ingestion/parse_disclosure.py --source ~/Downloads/agreement.pdf --vendor HDFC --tier international_credit
```

Re-running on the same document is safe:

- **One row per clause.** `rule_id` is `{vendor}-{hash of vendor, tier and normalized clause text}`. The same clause always maps to the same row, even when the model picks a different title or category. Category is stored in its own `category_code` column.
- **Stale rules are pruned per document.** Each run stamps `last_seen_at` on every rule it extracts. When a document finishes with no failed chunks, rules of that vendor, tier and `source_doc` that the run did not see are deleted. Other documents are never touched. Pass `--no-prune` to keep them.
- **Failures defer the prune.** If any chunk of a document fails, nothing is pruned for it. The run prints a `--retry-failed <run_dir>` command. Once the retry clears the failures, the deferred prune runs. An interrupted run never prunes, and a run that saved 0 rules for a document refuses to prune it.
- **Embeddings only redo what changed.** The trigger re-queues only rules that are new, whose title, domain or clause text changed, or that have no embedding yet. The worker re-embeds them and clears the query cache, so the next questions use the new rules immediately.

Every run writes `logs/<DD-Mon-YYYY_HH-MM-SS_id>/run.jsonl`. Each failed chunk gets a JSON file in that run's `failed/` folder with the chunk text and the model's raw output.

### Changing worker or orchestrator code

Rebuild and restart just that service:

```zsh
docker compose --profile app up -d --build embedding-worker   # or: orchestrator
```

### Changing the database schema

Files in `sql/` only run on an **empty** volume. For an existing database, apply a new or changed script by hand:

```zsh
docker exec -i bank_postgres psql -v ON_ERROR_STOP=1 -U postgres -d bank_aidb < sql/03_embeddings_api.sql
```

Or reset the volume to re-run everything from scratch (see [Stopping and resetting](#stopping-and-resetting)).

### Useful commands

```zsh
docker compose --profile app ps                     # status of all four services
docker logs -f bank_orchestrator                    # API logs and tracebacks
docker logs -f bank_embedding_worker                # embedding events
docker exec -it bank_postgres psql -U postgres -d bank_aidb   # SQL shell
```

---

## Configuration reference

| Setting | Where | Default | Purpose |
|---|---|---|---|
| `POSTGRES_PASSWORD` | `.env` | generated | `postgres` superuser password |
| `LOADER_DB_PASSWORD` | `.env` | generated | `rule_loader` password (ingestion script) |
| `WORKER_DB_PASSWORD` | `.env` | generated | `embed_worker` password (worker container) |
| `APP_DB_PASSWORD` | `.env` | generated | `mcp_assistant` password (orchestrator container) |
| `OLLAMA_HOST` | `.env` (set by `./ollama-mode.sh`) | `http://127.0.0.1:11434` | Ollama used by the ingestion script. `127.0.0.1` is the native Ollama.app (GPU). `[::1]` is the `bank_ollama` container (CPU) |
| `CONTAINER_OLLAMA_HOST` | `.env` (set by `./ollama-mode.sh`) | `http://ollama:11434` | Ollama used by the worker and orchestrator. `host.docker.internal` is Ollama.app (GPU). `ollama` is the container (CPU) |
| `CONFIDENCE_THRESHOLD` | `docker-compose.yml` → `orchestrator` | `0.70` | Minimum cosine similarity needed to answer |
| `EMBED_MODEL` / `MODEL_ID` | `orchestrator/app.py` / `workers/embedding_worker.py` | `nomic-embed-text` | Must match in both files, and must output 768 dimensions (or change `vector(768)` in `sql/03_embeddings_api.sql`) |
| `CHAT_MODEL` | `orchestrator/app.py` | `llama3.2:3b` | Model that writes the answers |
| Query cache TTL | `sql/03_embeddings_api.sql` | 24 hours | How long question embeddings are reused |

### Switching between GPU and CPU

Docker on macOS cannot use the Apple GPU. For GPU inference, point everything at the native Ollama.app:

```zsh
./ollama-mode.sh gpu      # ingestion, embeddings and answers on Ollama.app (Metal GPU); pulls missing models
./ollama-mode.sh cpu      # everything on the bank_ollama container (CPU)
./ollama-mode.sh status   # current settings, plus `ollama ps` for both servers (PROCESSOR column = GPU/CPU)
```

The script updates the two `.env` keys above and recreates only the worker and orchestrator containers. Embeddings come from the same `nomic-embed-text` model in both modes, so existing vectors stay valid.

### Tuning the confidence threshold

The threshold trades **coverage** (answering more questions) against **safety** (not answering from a weakly related rule). With the sample rules, relevant questions score about 0.60–0.80, and off-topic ones about 0.45. To experiment:

```zsh
# edit CONFIDENCE_THRESHOLD under the orchestrator service in docker-compose.yml, then:
docker compose --profile app up -d orchestrator
```

Tune it against a realistic set of questions once the rulebook has more than a handful of rules. Use the `similarity_scores` in `api.chat_audit_log` to see where good and bad matches fall.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `zsh: command not found: docker` | Docker Desktop isn't installed or running | `brew install --cask docker`, `open -a Docker`, then wait for the whale icon to settle |
| `pip install` fails building `psycopg2-binary` / asks for `pg_config` | Wrong Python (3.9) or an old pin with no wheel for your platform | Recreate the venv with Python 3.11/3.12 (Step 2) |
| `ResolutionImpossible … ollama 0.4.4 depends on pydantic>=2.9.0` | `pydantic` pinned below 2.9 | Pin `pydantic==2.9.2` in the relevant `requirements.txt` |
| `ModuleNotFoundError: No module named 'psycopg2'` on the host | Running with a Python outside the venv (system or conda) | `conda deactivate; source venv/bin/activate`, or run `venv/bin/python ingestion/parse_disclosure.py` |
| `ModuleNotFoundError` in `docker logs` of a container, container keeps restarting | That service's `requirements.txt` is missing the package | Add it, then `docker compose --profile app up -d --build <service>` |
| `unable to prepare context: path ".../orchestrator" not found` | Building app services whose folders don't exist | Make sure `workers/` and `orchestrator/` are present, or start only the base services with `docker compose up -d` |
| `Error response from daemon: No such container: bank_embedding_worker` | App profile not started | `docker compose --profile app up -d --build` |
| `docker logs` shows nothing for a running Python container | Output buffering | Both Dockerfiles set `ENV PYTHONUNBUFFERED=1`. Rebuild if you removed it |
| `LOADER_DB_PASSWORD is not set` in `docker logs bank_postgres` | DB first started without `.env` | Create `.env` (Step 3), then reset the volume |
| `password authentication failed for user …` | `.env` changed after the volume was created, or a hardcoded old password | Passwords are fixed at first init. Restore the old `.env` or reset the volume |
| `relation "core.bank_rulebook" / "api.…" does not exist` | Volume created before that `sql/` file existed | Apply the file by hand (see [Changing the database schema](#changing-the-database-schema)) or reset the volume |
| `HTTP 500` from `/api/v1/chat` | Unhandled DB error | `docker logs bank_orchestrator` shows the traceback |
| `"status": "ERROR"` with `model "…" not found` | Model not pulled | Step 5 |
| Rules loaded but `embedding` stays `NULL` | Worker not running, or job failed | `docker logs bank_embedding_worker`; check `SELECT * FROM core.embedding_job WHERE status <> 'COMPLETED';` |
| Postgres container won't start after an image upgrade, mentions `/var/lib/postgresql/data` | PG 18 images store data under `/var/lib/postgresql/18/docker` | The volume must mount at `/var/lib/postgresql` (already set in `docker-compose.yml`) |
| `sed: 1: ".env": invalid command code .` | GNU `sed -i` syntax on macOS | Use `sed -i '' '…' file` |

---

## Stopping and resetting

```zsh
# Stop everything, keep all data and models
docker compose --profile app down

# Start again later
docker compose --profile app up -d

# Wipe ONLY the database (re-runs sql/ init scripts on next start; keeps downloaded models)
docker compose --profile app down
docker volume rm pgvector-cardholder-rules-assistant_pgdata
docker compose --profile app up -d          # then repeat Step 7 to reload rules

# Wipe EVERYTHING, including the ~2.3 GB of models (repeat Steps 4–7 afterwards)
docker compose --profile app down -v
```
