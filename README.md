# pgvector-cardholder-rules-assistant

Using Llama, the credit card policy helping information is engineered through BIAN, Postgres, TOGAF standards.

A fully local, air-gapped assistant that answers cardholder questions **only** from the bank's official card rulebook. Rules are stored in PostgreSQL, embedded with pgvector, retrieved by similarity, and explained by a local Llama model, with every answer citing its BIAN domain and rule ID. If no rule is a confident match, the assistant declines instead of guessing.

---

## Contents

1. [How it works](#how-it-works)
2. [Ingestion pipeline](#ingestion-pipeline)
3. [Project layout](#project-layout)
4. [Prerequisites](#prerequisites)
5. [Step-by-step setup and execution](#step-by-step-setup-and-execution)
6. [Day-to-day operations](#day-to-day-operations)
7. [End-to-end verification](#end-to-end-verification)
8. [Known issues](#known-issues)
9. [Configuration reference](#configuration-reference)
10. [Secrets and credentials](#secrets-and-credentials)
11. [Troubleshooting](#troubleshooting)
12. [Stopping and resetting](#stopping-and-resetting)

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
3. **Embed.** The worker receives the event, embeds `search_document: {title} [{domain}]: {clause}` with `nomic-embed-text`, stores the vector and its `embedding_scheme`, and marks the rule's open jobs `COMPLETED`. It never clears the query cache: cached question vectors depend only on the question, not on the rules. At startup and every `EMBED_SWEEP_INTERVAL_S` (60 s) it also embeds any rule with no vector or a vector from an older scheme. That catches events missed while the worker was down, failed jobs, and re-embeds everything after a model or prefix change.
4. **Answer.** The orchestrator embeds `search_query: {question}` (cached for 24 hours), finds the two closest rules for the customer's card tier and vendor, and then does one of two things:
   - If the best similarity is **≥ `CONFIDENCE_THRESHOLD`** (0.70), `llama3.2:3b` writes an answer using only those rules → `SUCCESS`.
   - Otherwise it returns a fixed "contact customer care" message → `ABSTAINED`.
5. **Audit.** Every request is written to `api.chat_audit_log`.

### Database roles (least privilege)

| Role | Used by | Can do |
|---|---|---|
| `postgres` | Admin / init scripts only | Everything |
| `rule_loader` | `ingestion/parse_disclosure.py` | Insert, update and retire rules in `core.bank_rulebook` (no delete) |
| `embed_worker` | `workers/embedding_worker.py` | Read rules; write only `embedding`/`embedding_scheme`/`updated_at`; update jobs. No access to the `api` schema |
| `mcp_assistant` | `orchestrator/app.py` | Read active rules through `api.rulebook_vw`; use the query cache; append to and read the audit log (no update/delete). 2-second statement timeout, `hnsw.iterative_scan = strict_order` |

The services get their credentials only from the `DB_URI` environment variable, which `docker-compose.yml` builds from `.env`. There are no fallback credentials in the code. See [Secrets and credentials](#secrets-and-credentials).

---

## Ingestion pipeline

`ingestion/parse_disclosure.py` turns credit card disclosure documents into structured rules in `core.bank_rulebook`, using a local LLM through Ollama. Bad input is stopped before any model call, failures are retried and isolated, and rules are never deleted: a vendor's rules that its current documents no longer contain are retired, and only on strong evidence. A one-page version of this section is in [docs/ingestion-pipeline-summary.pdf](docs/ingestion-pipeline-summary.pdf).

```
Input ─► Limits* ─► Text ─► Relevance* ─► Chunk ─► Extract* ─► Upsert ─► Retire*
                                              * can reject input or hold back changes
```

### Inputs

- `--source`: one local `.pdf`, `.txt` or `.md` file, or an `http(s)` URL.
- `--dir`: every supported file at the top level of a folder.
- Neither flag: a built-in sample corpus, for demos and smoke tests.
- URLs: PDFs are recognised by their `%PDF-` signature, even when the server labels them as generic files. Plain text and Markdown are accepted; HTML pages (login walls, error pages) are rejected.
- Every source is tagged with `--vendor` and `--tier` (defaults `CHASE` and `standard_credit`).

### Checks before any LLM call

A rejected source is listed in the run summary and the run exits with code 2.

| Check | Rule | Failure stage |
|---|---|---|
| File size | Files and downloads over `MAX_SOURCE_MB` (25) are rejected; downloads are stopped while streaming | `limits` |
| Page count | PDFs over `MAX_PDF_PAGES` (200) are rejected | `limits` |
| Download time | `DOWNLOAD_DEADLINE_S` (120) for the whole download | `limits` |
| File exists | A missing file gives a clear "no such file" error | `read` |
| Readable text | Pages with no text (scanned images) are logged; a document with no text is skipped | warning |
| Relevance | Must mention a credit card, cardholder or card account, plus at least `MIN_RELEVANCE_TERMS` (4) distinct card-disclosure terms: APR or finance charges, annual fee, grace period, billing cycle, minimum payment, balance transfer, cash advance, foreign transaction, late fee, penalty APR, credit limit, billing error/dispute, purchases. Override with `--skip-relevance-check` | `relevance` |

### Extraction

- Text is split on paragraph and section breaks into chunks of about 2,200 characters, overlapping by about 300.
- Default model `llama3.2:3b` via Ollama, temperature 0, with output forced into a fixed JSON schema:
  - `category_code`: one of 16 (APR, FEE, FX, PAYMENT, BILLING, DISPUTE, LIMIT, REWARD, FRAUD, GRACE, LIABILITY, SECURITY, CARDMGMT, LEGAL, PRIVACY, GENERAL)
  - `bian_domain`: one of 9 BIAN Service Domains
  - `rule_title` (up to 150 characters) and `clause_text` (up to 2,000)
  - at most `MAX_RULES_PER_CHUNK` (10) rules per chunk
- Output that is cut off or doesn't match the schema is retried. Rules whose numbers (fees, %, days) don't appear in the source chunk are dropped, and so are duplicates.
- Before the run, the script checks Ollama is reachable and the model is installed, warms it up, and warns if it is running on CPU instead of GPU.

### Failure recovery

- Up to three attempts per chunk: **default**, then **resample** (different sampling settings to break repeating loops), then **split** (halve the chunk; rules from a half that works are kept).
- One failed chunk or source never stops the run. Each failure is saved to `logs/<run>/failed/` with the chunk text, the model's raw output from every attempt and the error.
- `--retry-failed <run_dir>` reprocesses only those chunks. Ctrl-C stops the run but keeps every rule already saved.

### Loading and retiring

- Rules are upserted per chunk as `rule_loader`, with stable IDs, and only new or changed rules are queued for embedding. See [Adding or changing rules](#adding-or-changing-rules) for how re-runs and retiring behave.
- A run is the vendor's **complete rulebook** for one tier, however many PDFs it spans and whatever they are named. Active rules of that vendor and tier that no document in the run produced are retired (`retired_at`, `retired_reason`), not deleted. Retiring is deferred if any chunk failed, skipped if a whole document could not be read or the run was interrupted, refused if the run saved 0 rules, and refused if it would retire more than `PRUNE_MAX_DROP_RATIO` (50%) of the vendor's active rules (`--force-retire` overrides; `--no-retire` disables).

### Logs and exit codes

- Console shows short progress lines (`--log-level`). `logs/<run>/run.jsonl` records every event at DEBUG, tagged with run, source and chunk.
- The run summary lists each failure by stage (`read`, `limits`, `relevance`, `extract`, `db`) and prints the `--retry-failed` command.

| Exit code | Meaning |
|---|---|
| `0` | Success |
| `1` | Aborted (Ollama unreachable, model missing, unhandled error) |
| `2` | Some sources or chunks failed |
| `130` | Interrupted |

### Not covered yet

These are judgment calls rather than hard rules, so they are planned for the Data Quality layer instead of blocking ingestion.

| Gap | Plan |
|---|---|
| Issuer and tier aren't checked against the document (Amex rules could be saved under `--vendor CHASE`) | Data Quality report flagging documents where the issuer isn't mentioned |
| Grounding only checks numbers; rules without numbers or reworded by the model pass | Data Quality score of each rule's word overlap with its source chunk |
| Source chunk text isn't stored with each rule | Next: needed by the Data Quality checks |
| No OCR for scanned PDFs; `--dir` skips subfolders | Not planned |

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
│   ├── 03_embeddings_api.sql #   embedding column + HNSW index, job queue + trigger, api schema, grants
│   ├── 04_rule_identity.sql  #   category_code, source_doc, last_seen_at (stable IDs + pruning)
│   ├── 05_embedding_scheme.sql # embedding_scheme column, job attempts/last_error
│   ├── 06_rule_lifecycle.sql #   retired_at/retired_reason, active-only view + vector index (vendor snapshots)
│   ├── 07_vector_search_settings.sql # hnsw.iterative_scan for the assistant (filtered HNSW keeps its top-k)
│   └── Test_client_api       #   ad-hoc audit-log queries; no .sql extension, so init skips it (keep it that way)
├── ingestion/
│   └── parse_disclosure.py   # loads sample rules (runs on the host)
├── docs/
│   └── ingestion-pipeline-summary.pdf  # one-page summary of the ingestion pipeline
├── tests/                    # pytest, no DB/Ollama/network needed: venv/bin/python -m pytest tests
│   └── TestSequels.sql       #   ad-hoc inspection queries (rules per vendor/category), not run by pytest
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
> - zsh does not treat `#` as a comment in an interactive terminal by default, so a pasted command with a trailing `# comment` passes the comment words as arguments (for example `no such service: #`). Run `setopt interactivecomments` once (add it to `~/.zshrc` to keep it), or drop the comments when pasting.
> - Shell variables such as `$S` exist only in the terminal that set them. In a new terminal they are empty, so `$S/v1` becomes `/v1`.

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

✅ **Check:** `cut -d= -f1 .env` lists the four keys, `git check-ignore .env` prints `.env`, and `git ls-files .env` prints nothing (the file is not tracked).

> ⚠️ Create `.env` **before** the database starts for the first time. Postgres only runs the `sql/` init scripts on an empty volume. If it starts without these passwords, the roles are never created, and you must reset the volume (see [Stopping and resetting](#stopping-and-resetting)).

### Step 4: Start PostgreSQL and Ollama

```zsh
docker compose up -d
```

This starts only `postgres-ai` and `ollama`. The worker and orchestrator are in the `app` profile and start in Step 6. On the first run, Postgres runs `sql/01_roles.sh` through `07_vector_search_settings.sql` in order.

✅ **Check that every init script ran:**

```zsh
docker logs bank_postgres 2>&1 | grep -E 'running /docker|ignoring|ERROR'
```

Each of `01`–`07` shows `running`, `Test_client_api` shows `ignoring`, and there are no `ERROR` lines.

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

The worker and orchestrator are not run by hand: they are these two containers. `parse_disclosure.py` does not start them, and without them nothing is embedded and `/api/v1/chat` is unreachable. Keep `--build` whenever `workers/` or `orchestrator/` changed, or Docker reuses the old images.

Start them **before** Step 7 so rules are embedded as they load. If you ingest first, nothing is lost: on startup the worker sweeps up every active rule that has no vector (or one from an older embedding scheme).

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
python ingestion/parse_disclosure.py; echo EXIT=$?
```

Output looks like this (rule IDs are hashes of the clause text, so yours will differ):

```
[built_in_schumer_corpus #1/2] ✔️ 10 rules in 6.0s
[built_in_schumer_corpus #1/2]    📄 [Payment Execution] CHASE-27861EF6C640 -> Late Payment Fee
...
🧹 CHASE/standard_credit: 15 active rule(s), retired 0 no longer in the vendor's documents
✅ Upserted 17 rules into core.bank_rulebook from 2 chunk(s) in 12s.
EXIT=0
```

`Upserted` can be higher than the number of distinct rules: overlapping chunks can extract the same clause twice, and both writes go to the same row.

Within a second or two, the worker terminal shows one pair of lines per upsert:

```
📥 [Event Received] Processing Rule: CHASE-27861EF6C640 (Job #2)
✅ [Worker Action] Rule CHASE-27861EF6C640 embedded.
```

✅ **Check:** every active rule is embedded with the current scheme and every job is complete.

```zsh
docker exec bank_postgres psql -U postgres -d bank_aidb \
  -c "SELECT vendor_id, card_tier, count(*) active, count(embedding) embedded,
             count(*) FILTER (WHERE embedding_scheme = 'nomic-embed-text|prefixed-v1') scheme_ok
      FROM core.bank_rulebook WHERE retired_at IS NULL GROUP BY 1, 2;" \
  -c "SELECT status, count(*), count(last_error) AS errors FROM core.embedding_job GROUP BY 1;"
```

`active`, `embedded` and `scheme_ok` should be equal, and every job `COMPLETED` with 0 errors. This must pass before Step 8: a rule without a vector can't be found.

The script is safe to re-run. Unchanged rules that already have embeddings are not re-queued.

### Step 8: Ask questions

```zsh
curl -s -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "What is the late payment fee?"}'
```

Expected response (rule IDs and the wording of `response` vary between runs):

```json
{
  "status": "SUCCESS",
  "matched_rules": ["CHASE-27861EF6C640", "CHASE-E6F3EC34B6F5"],
  "similarity_score": 0.837,
  "latency_ms": 1842,
  "response": "If the minimum payment is not received by 5:00 PM ... a late fee of up to $40 ...\n\nOfficial Proof: ..."
}
```

More questions to try (scores measured on the built-in corpus, 2026-11-20):

| Question | Expected `status` | Why |
|---|---|---|
| `How long do I have to dispute a billing error?` | `SUCCESS` (≈ 0.87) | Matches the billing-error dispute rule |
| Same late-fee question again | `SUCCESS`, much faster | The question vector comes from `api.query_embedding_cache` |
| `Do you offer airport lounge access?` | `ABSTAINED` (≈ 0.58) | No rule covers it, so the assistant declines |
| `Is there a fee for purchases made in a foreign currency?` | `SUCCESS` only if an FX rule was extracted | The built-in corpus has an FX clause, but extraction can drop it (see [Known issues](#known-issues)); then the best match scores ≈ 0.66 and the assistant correctly declines |
| Any question with `"vendor_id": "HDFC"` on a CHASE-only rulebook | `ABSTAINED`, `matched_rules: []` | Search is restricted to the request's vendor and tier |

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

Each run is a **vendor snapshot**: give it all of the vendor's current documents for one tier. Put them in one folder and use `--dir`, or use `--source` when the vendor has a single document:

```zsh
# HDFC's current terms, split across two PDFs
python ingestion/parse_disclosure.py --dir ~/Downloads/hdfc_2026/ --vendor HDFC --tier international_credit
```

When the vendor publishes new terms, run the folder again with the new files. The file names don't need to match the old ones.

Re-running is safe:

- **One row per clause.** `rule_id` is `{vendor}-{hash of vendor, tier and normalized clause text}`. The same clause always maps to the same row, even when the model picks a different title or category. Category is stored in its own `category_code` column.
- **Rules no longer in the vendor's documents are retired.** Each run stamps `last_seen_at` on every rule it extracts. When every document finishes with no failures, active rules of that vendor and tier that the run did not see get `retired_at` and a `retired_reason` naming the run and its documents. They disappear from `api.rulebook_vw`, so the assistant stops citing them immediately, but the row stays, so `chat_audit_log.matched_rule_ids` always resolves to the exact clause a customer was given. Other vendors and tiers are never touched. Pass `--no-retire` to keep them active.
- **Rules can come back.** If a later snapshot contains a retired clause again, the upsert reactivates it. Its embedding was kept, so it is searchable at once.
- **Failures hold back retiring.** If any chunk fails, nothing is retired. The run prints a `--retry-failed <run_dir>` command, and the retiring step runs once the retry clears every failure. If a whole document could not be read (rejected, unreadable, no text), nothing is retired and the retry can't finish it: fix or remove that file and run the vendor's folder again. An interrupted run never retires, and a run that saved 0 rules refuses to.
- **Partial snapshots are refused.** If retiring would remove more than half of the vendor's active rules (`PRUNE_MAX_DROP_RATIO`), it is skipped with a warning. That usually means only some of the vendor's documents were given (e.g. just the fee schedule). If the vendor's terms really were rewritten, re-run with `--force-retire`.
- **To see what was retired:** `SELECT rule_id, rule_title, source_doc, retired_at, retired_reason FROM core.bank_rulebook WHERE retired_at IS NOT NULL ORDER BY retired_at DESC;`
- **Embeddings only redo what changed.** The trigger re-queues only rules that are new, whose title, domain or clause text changed, or that have no embedding yet. The worker re-embeds them, so the next questions use the new rules immediately.

Inputs are checked for size, page count, download time and relevance before any model call; see [Checks before any LLM call](#checks-before-any-llm-call).

Every run writes `logs/<DD-Mon-YYYY_HH-MM-SS_id>/run.jsonl`. Each failed chunk gets a JSON file in that run's `failed/` folder with the chunk text and the model's raw output.

### Changing worker or orchestrator code

Rebuild and restart just that service:

```zsh
docker compose --profile app up -d --build embedding-worker   # or: orchestrator
```

### Changing the database schema

Files in `sql/` only run on an **empty** volume. For an existing database, re-apply **03 through 07 together, in order**. They are idempotent as a set:

```zsh
for f in 03_embeddings_api 04_rule_identity 05_embedding_scheme 06_rule_lifecycle 07_vector_search_settings; do
  docker exec -i bank_postgres psql -q -v ON_ERROR_STOP=1 -U postgres -d bank_aidb < sql/$f.sql || break
done
docker compose --profile app up -d --force-recreate orchestrator
```

> ⚠️ Never re-apply `03` or `04` on their own. `03` recreates `api.rulebook_vw` without the `retired_at` filter (the assistant would cite retired rules) and brings back the unfiltered HNSW index; `04` gives `rule_loader` `DELETE` again. `06` and later undo both, which is why the whole range must run.

Recreating the orchestrator makes its pooled connections pick up role settings such as `07`'s `hnsw.iterative_scan`. Or reset the volume to re-run everything from scratch (see [Stopping and resetting](#stopping-and-resetting)).

### Useful commands

```zsh
docker compose --profile app ps                     # status of all four services
docker logs -f bank_orchestrator                    # API logs and tracebacks
docker logs -f bank_embedding_worker                # embedding events
docker exec -it bank_postgres psql -U postgres -d bank_aidb   # SQL shell
```

---

## End-to-end verification

Run this before committing changes to `sql/`, the worker, the orchestrator or the ingestion script. It rebuilds the database from scratch and exercises every stage, from the init scripts to the audit log. The last full pass was on 2026-11-20; everything passed except the items in [Known issues](#known-issues).

It needs about 10 minutes with Ollama on the GPU (`./ollama-mode.sh gpu`). Run the phases in one terminal, with the venv active.

### Phase 0: Unit tests and backup

```zsh
python -m py_compile ingestion/parse_disclosure.py orchestrator/app.py workers/embedding_worker.py && echo COMPILE_OK
python -m pytest tests -q
```

✅ `COMPILE_OK` and all tests pass.

Phase 1 wipes the database volume. If it holds rules you want to keep, copy it first:

```zsh
docker compose --profile app down
docker volume create pgdata_backup
docker run --rm -v pgvector-cardholder-rules-assistant_pgdata:/from -v pgdata_backup:/to alpine sh -c 'cp -a /from/. /to/ && echo BACKUP_OK'
```

### Phase 1: Fresh database, schema and privileges

```zsh
docker volume rm pgvector-cardholder-rules-assistant_pgdata
docker compose --profile app up -d --build
docker logs bank_postgres 2>&1 | grep -E 'running /docker|ignoring|ERROR'
```

✅ `01`–`07` show `running`, `Test_client_api` shows `ignoring`, and there are no `ERROR` lines.

```zsh
docker exec -i bank_postgres psql -U postgres -d bank_aidb -At <<'SQL'
SELECT 'view filters retired (t):      '||(pg_get_viewdef('api.rulebook_vw') LIKE '%retired_at IS NULL%');
SELECT 'active-only HNSW index (t):    '||EXISTS(SELECT 1 FROM pg_indexes WHERE indexname='bank_rulebook_embedding_active_hnsw');
SELECT 'old unfiltered index (f):      '||EXISTS(SELECT 1 FROM pg_indexes WHERE indexname='bank_rulebook_embedding_hnsw');
SELECT 'assistant iterative scan (t):  '||(SELECT 'hnsw.iterative_scan=strict_order' = ANY(rolconfig) FROM pg_roles WHERE rolname='mcp_assistant');
SELECT 'loader DELETE (f):             '||has_table_privilege('rule_loader','core.bank_rulebook','DELETE');
SELECT 'worker UPDATE scheme (t):      '||has_column_privilege('embed_worker','core.bank_rulebook','embedding_scheme','UPDATE');
SELECT 'worker UPDATE clause (f):      '||has_column_privilege('embed_worker','core.bank_rulebook','clause_text','UPDATE');
SELECT 'worker api schema (f):         '||has_schema_privilege('embed_worker','api','USAGE');
SELECT 'assistant SELECT audit (t):    '||has_table_privilege('mcp_assistant','api.chat_audit_log','SELECT');
SELECT 'assistant UPDATE audit (f):    '||has_table_privilege('mcp_assistant','api.chat_audit_log','UPDATE');
SQL
```

✅ Every line ends with the value in brackets. Then re-apply 03–07 with the loop in [Changing the database schema](#changing-the-database-schema) and run the same check again. The results must not change.

### Phase 2: Ingestion, embedding and chat

Run [Step 7](#step-7-load-the-rulebook) and its embedding check, then:

```zsh
ask(){ curl -s -X POST http://localhost:8000/api/v1/chat -H 'Content-Type: application/json' -d "$1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['status'],round(d.get('similarity_score') or 0,3),d['latency_ms'],'ms',d['matched_rules'])"; }
ask '{"query":"What is the late payment fee?"}'
ask '{"query":"What is the late payment fee?"}'
ask '{"query":"Do you offer airport lounge access?"}'
ask '{"query":"What is the late payment fee?","vendor_id":"HDFC"}'
for i in {1..12}; do curl -s -o /dev/null -w "%{http_code} " -X POST http://localhost:8000/api/v1/chat -H 'Content-Type: application/json' -d "{\"query\":\"What is the cash advance fee $i?\"}" & done; wait; echo
docker exec bank_postgres psql -U postgres -d bank_aidb -c "SELECT execution_status, count(*) FROM api.chat_audit_log GROUP BY 1;" -c "SELECT DISTINCT model_id FROM api.query_embedding_cache;"
```

✅
- `SUCCESS ≈0.84` twice, the second call much faster (served from the cache).
- `ABSTAINED` for the lounge question and for the HDFC question (empty `[]`).
- Twelve `200`s from the concurrent burst, even though the pool has 10 connections.
- One audit row per request, and the cache `model_id` is `nomic-embed-text|prefixed-v1`.

### Phase 3: Ingestion guards and rule lifecycle

These checks use a throwaway test vendor, `ACME`, so real vendors are never touched. Create the test documents (zsh):

```zsh
T=$(mktemp -d); mkdir -p $T/v1 $T/v2 $T/empty $T/bad $T/partial
H="ACME Bank Credit Card Cardholder Agreement. This credit card account is governed by the following terms."
FX="Foreign Transaction Fee: Purchases made in a foreign currency incur a fee of 3% of each transaction."
BODY=(
  "Annual Fee: The annual fee for this credit card is \$95, charged on the first billing cycle."
  "Late Fee: If your minimum payment is not received by the due date, a late fee of \$40 is charged."
  "Cash Advance Fee: Each cash advance costs 5% of the amount or \$10, whichever is greater."
  "Balance Transfer Fee: Each balance transfer costs 4% of the amount transferred."
  "Grace Period: You have at least 25 days after the close of each billing cycle to pay purchases without interest."
  "Purchase APR: The APR for purchases is 21.99%. Penalty APR of 29.99% may apply after a late payment."
)
printf "%s\n\n" "$H" "$FX" "${BODY[@]}" > $T/v1/acme_agreement_2025.txt
printf "%s\n\n" "$H" "${BODY[@]}" > $T/v2/acme_terms_2026.txt
echo "A recipe for pancakes: mix flour and eggs." > $T/bad/notes.txt
printf "%s\n\n%s\n" "ACME Bank credit card cardholder agreement. Credit card account terms: annual fee, late fee, billing cycle, minimum payment, APR." "${BODY[2]}" > $T/partial/fees_only.txt
```

`$T` exists only in this terminal. v2 is the same agreement under a new file name, with the FX clause removed.

| # | Command | ✅ Pass when |
|---|---|---|
| 1 | `python ingestion/parse_disclosure.py --vendor ACME --dir $T/empty; echo EXIT=$?` | `EXIT=1`, `no supported files`. Never falls back to the built-in corpus |
| 2 | `python ingestion/parse_disclosure.py --vendor ACME --source $T/bad/notes.txt; echo EXIT=$?` | `EXIT=2`, failure stage `[relevance]` (not `[read]`, which means `$T` is empty), `nothing retired` |
| 3 | `python ingestion/parse_disclosure.py --vendor ACME --dir $T/v1` then `ask '{"query":"Is there a fee for purchases in a foreign currency?","vendor_id":"ACME"}'` | `EXIT=0`, about 6 rules, FX question `SUCCESS` |
| 4 | `python ingestion/parse_disclosure.py --vendor ACME --dir $T/v2` then the same `ask` | `🗄️ retired ... Foreign Transaction Fee` with a `retired_reason` naming the run and `acme_terms_2026.txt`; FX question now `ABSTAINED` |
| 5 | `python ingestion/parse_disclosure.py --vendor ACME --dir $T/v1 --no-retire` then the same `ask` | The FX rule is active again with its old embedding and **no new** `embedding_job` row; FX question `SUCCESS` |
| 6 | `python ingestion/parse_disclosure.py --vendor ACME --dir $T/partial` | `🛑 retire skipped ... over PRUNE_MAX_DROP_RATIO=50%`; ACME's active rules and every other vendor unchanged |

The model may reword a clause between runs. The reworded version gets a new rule ID, so step 4 can also retire a rule other than FX. That's expected: rule IDs are hashes of the clause text.

To check step 5:

```zsh
docker exec bank_postgres psql -U postgres -d bank_aidb -c "SELECT r.rule_id, r.retired_at IS NOT NULL AS retired, r.embedding IS NOT NULL AS embedded, (SELECT count(*) FROM core.embedding_job j WHERE j.rule_id = r.rule_id) AS jobs FROM core.bank_rulebook r WHERE vendor_id = 'ACME' AND rule_title ILIKE '%foreign%';"
```

### Phase 4: Worker catch-up

Stop the worker, insert a rule (its `NOTIFY` is missed), mark another rule as embedded with an old scheme, then start the worker again:

```zsh
docker stop bank_embedding_worker
docker exec bank_postgres psql -U postgres -d bank_aidb -c "INSERT INTO core.bank_rulebook (rule_id, bian_domain, card_tier, vendor_id, rule_title, clause_text, category_code, source_doc) VALUES ('ACME-MISSED-1', 'Payment Execution', 'standard_credit', 'ACME', 'Returned Payment Fee', 'A returned payment costs \$35.', 'FEE', 'manual');" -c "UPDATE core.bank_rulebook SET embedding_scheme = 'nomic-embed-text|legacy' WHERE vendor_id = 'CHASE' AND rule_title ILIKE '%late%';"
docker start bank_embedding_worker; sleep 8; docker logs --tail 5 bank_embedding_worker
```

✅ `🧹 [Worker Sweep] N rule(s) need embedding` followed by `✅` for `ACME-MISSED-1` and for the legacy-scheme CHASE rule(s). Retired rules are skipped.

### Phase 5: Clean up

```zsh
docker exec bank_postgres psql -U postgres -d bank_aidb -c "DELETE FROM core.bank_rulebook WHERE vendor_id = 'ACME';"
rm -rf "$T"
```

To return to the data you backed up in Phase 0:

```zsh
docker compose --profile app down
docker volume rm pgvector-cardholder-rules-assistant_pgdata
docker volume create pgvector-cardholder-rules-assistant_pgdata
docker run --rm -v pgdata_backup:/from -v pgvector-cardholder-rules-assistant_pgdata:/to alpine sh -c 'cp -a /from/. /to/ && echo RESTORE_OK'
docker compose --profile app up -d
```

Remove the backup with `docker volume rm pgdata_backup` once you no longer need it.

---

## Known issues

Found in the 2026-11-20 end-to-end run. None of them blocks normal use.

| Issue | Effect | Workaround until fixed |
|---|---|---|
| Orchestrator pool after a Postgres restart | The pool still holds dead connections. Each request uses up two (retrieval, then the audit insert), so the first ~`DB_POOL_MAX`/2 requests (5 by default) fail with HTTP 500 **and are not audited**, then the pool recovers | After restarting Postgres, also run `docker compose --profile app restart orchestrator`. Planned fix: retry once on a fresh connection in `db_cursor` |
| Re-applying `03` or `04` alone | Undoes `06`: the view shows retired rules again, or `rule_loader` regains `DELETE` | Always re-apply 03–07 together ([Changing the database schema](#changing-the-database-schema)) |
| Refused retiring exits `0` | The partial-snapshot guard logs `🛑 retire skipped` but the run still succeeds, so automation doesn't notice | Check run logs for `retire_skipped` |
| `MAX_RULES_PER_CHUNK` (10) can drop clauses | A dense chunk that hits the cap loses its remaining rules (the built-in corpus's FX fee is often lost this way) | Raise `MAX_RULES_PER_CHUNK` in `.env`, or use smaller chunks |
| Overlapping chunks re-extract a clause | The same rule is written twice; the second, sometimes worse, title wins (seen: `Ishonored Check Fee`) | Data Quality checks (see [Not covered yet](#not-covered-yet)) |
| `SUCCESS` answers that are refusals | `llama3.2:3b` sometimes replies "I cannot provide financial advice" despite a confident match | Prompt or model tuning; find them in `api.chat_audit_log` |
| `mcp_assistant` can read `core.bank_rulebook` directly | Granted in `02_schema.sql`; the app only uses the view, but the role could read retired rules | Planned: revoke it and rely on `api.rulebook_vw` |
| Worker exits when the DB connection drops | Nothing is lost: `restart: always` restarts it and the startup sweep embeds anything missed | None needed |

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
| `DB_URI` | `docker-compose.yml` → `embedding-worker`, `orchestrator` | built from `.env` | Database connection for the worker (`embed_worker`) and the orchestrator (`mcp_assistant`). **Required**, with no default in code: if it's missing, the service stops with `DB_URI is not set` |
| `CONFIDENCE_THRESHOLD` | `docker-compose.yml` → `orchestrator` | `0.70` | Minimum cosine similarity needed to answer |
| `DB_POOL_MAX` / `DB_POOL_MIN` | orchestrator environment | `10` / same as max | Postgres connection pool size. A request borrows a connection only for retrieval and for the audit insert, never during the LLM call; extra requests wait for a free one. Keep `DB_POOL_MAX` × orchestrator replicas below Postgres `max_connections`. Idle connections above `DB_POOL_MIN` are closed, so lowering it trades reconnects for fewer open connections |
| `EMBED_MODEL` / `MODEL_ID` | `orchestrator/app.py` / `workers/embedding_worker.py` | `nomic-embed-text` | Must match in both files, and must output 768 dimensions (or change `vector(768)` in `sql/03_embeddings_api.sql`) |
| `QUERY_SCHEME` / `EMBED_SCHEME` | `orchestrator/app.py` / `workers/embedding_worker.py` | `nomic-embed-text\|prefixed-v1` | Keep in step. Bump both when the model or the `search_query: `/`search_document: ` prefixes change; the worker then re-embeds every rule |
| `EMBED_SWEEP_INTERVAL_S` / `EMBED_SWEEP_BATCH` | worker environment | `60` / `200` | How often the worker looks for rules needing (re-)embedding, and how many it takes per sweep |
| `CHAT_MODEL` | `orchestrator/app.py` | `llama3.2:3b` | Model that writes the answers |
| `MAX_SOURCE_MB` / `MAX_PDF_PAGES` | `.env` | `25` / `200` | Largest file or download, and longest PDF, the ingestion script accepts |
| `DOWNLOAD_DEADLINE_S` | `.env` | `120` | Time limit for a whole URL download |
| `MIN_RELEVANCE_TERMS` | `.env` | `4` | Distinct card-disclosure terms a source needs to pass the relevance check |
| `PRUNE_MAX_DROP_RATIO` / `PRUNE_MIN_PRIOR_ROWS` | `.env` | `0.5` / `5` | Largest share of a vendor's active rules (per tier) one run may retire, and the minimum rulebook size the check applies to |
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

## Secrets and credentials

- **`.env` is the only place passwords live.** It's git-ignored, and it has never been committed. Before every push, `git ls-files .env` must print nothing.
- **No credentials in code.** The worker and orchestrator read `DB_URI` from the environment only, and `docker-compose.yml` builds it from `.env`. The ingestion script reads `LOADER_DB_PASSWORD` from the environment or `.env`. Never add a fallback connection string with a password, even a placeholder: secret scanners flag it, and a missing variable would silently use it.
- **Turn on GitHub protection.** In the repository's **Settings → Code security**, enable *Secret scanning* and *Push protection*, so a pushed secret is blocked before it lands. Locally, a `gitleaks` pre-commit hook (`brew install gitleaks`) catches it earlier.
- **Production.** Keep these values in a secrets manager (Vault, AWS Secrets Manager) or Docker secrets, not a `.env` file.

### Rotating a database password

Postgres reads the `.env` passwords only when the volume is first created, so editing `.env` alone breaks logins (`password authentication failed`). Change the password in the database first, then in `.env`, then recreate the service that uses it:

```zsh
NEW=$(openssl rand -hex 16)
docker exec bank_postgres psql -U postgres -d bank_aidb -c "ALTER ROLE mcp_assistant PASSWORD '$NEW';"
sed -i '' "s/^APP_DB_PASSWORD=.*/APP_DB_PASSWORD=$NEW/" .env
docker compose --profile app up -d orchestrator
```

| Role | `.env` key | Then |
|---|---|---|
| `mcp_assistant` | `APP_DB_PASSWORD` | `docker compose --profile app up -d orchestrator` |
| `embed_worker` | `WORKER_DB_PASSWORD` | `docker compose --profile app up -d embedding-worker` |
| `rule_loader` | `LOADER_DB_PASSWORD` | Nothing: the next ingestion run reads `.env` |
| `postgres` | `POSTGRES_PASSWORD` | Nothing: used only for admin access |

✅ **Check:** `docker logs --tail 3 bank_orchestrator` shows no `password authentication failed`, and a chat request returns `SUCCESS` or `ABSTAINED`.

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
| `password authentication failed for user …` | `.env` changed after the volume was created | Passwords are fixed at first init. Restore the old `.env`, or change it properly with `ALTER ROLE` (see [Rotating a database password](#rotating-a-database-password)) |
| `DB_URI is not set` in a container log, or when running a service by hand | The worker or orchestrator was started without `DB_URI` | Start them with `docker compose --profile app up -d`, or export `DB_URI` first |
| `no such service: #` from `docker compose` | zsh passed a trailing `# comment` as arguments | `setopt interactivecomments`, or paste the command without the comment |
| `FileNotFoundError: no such file: /v1/...` (path starts at `/`) | A shell variable such as `$S` or `$T` is empty in this terminal | Set it again in this terminal (`echo $S` to check) |
| `HTTP 500` on the first few chats after a Postgres restart | The orchestrator's pool still holds dead connections (see [Known issues](#known-issues)) | `docker compose --profile app restart orchestrator` |
| `relation "core.bank_rulebook" / "api.…" does not exist` | Volume created before that `sql/` file existed | Apply the file by hand (see [Changing the database schema](#changing-the-database-schema)) or reset the volume |
| `HTTP 500` from `/api/v1/chat` | Unhandled DB error | `docker logs bank_orchestrator` shows the traceback |
| `"status": "ERROR"` with `model "…" not found` | Model not pulled | Step 5 |
| Rules loaded but `embedding` stays `NULL` | Worker not running, or Ollama failing | `docker logs bank_embedding_worker`; check `SELECT rule_id, status, attempts, last_error FROM core.embedding_job WHERE status <> 'COMPLETED';`. The worker retries on its next sweep (60 s) |
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
