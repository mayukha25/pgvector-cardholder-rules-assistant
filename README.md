# pgvector-cardholder-rules-assistant

Using Llama, the credit card policy helping information is engineered through BIAN, Postgres, TOGAF standards.

A fully local, air-gapped assistant that answers cardholder questions **only** from the bank's official card rulebook. Rules are stored in PostgreSQL, embedded with pgvector, retrieved by similarity, and explained by a local Llama model, with every answer citing its BIAN domain and rule ID. If no rule is a confident match, the assistant declines instead of guessing.

---

## Contents

1. [How it works](#how-it-works)
2. [Ingestion pipeline](#ingestion-pipeline)
3. [Project layout](#project-layout)
4. [How the code works](#how-the-code-works)
5. [Prerequisites](#prerequisites)
6. [Step-by-step setup and execution](#step-by-step-setup-and-execution)
7. [Day-to-day operations](#day-to-day-operations)
8. [End-to-end verification](#end-to-end-verification)
9. [Known issues](#known-issues)
10. [Configuration reference](#configuration-reference)
11. [Secrets and credentials](#secrets-and-credentials)
12. [Troubleshooting](#troubleshooting)
13. [Stopping and resetting](#stopping-and-resetting)

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

## How the code works

This section explains the three programs in plain language: what each one is for, what it does step by step, and why it's built that way. Function names are given in `code` so you can find each part in the file.

### The words you'll see

| Term | What it means here |
|---|---|
| **LLM** (large language model) | The AI that reads and writes text. Here it's `llama3.2:3b`, a small model that runs on your own machine. |
| **Ollama** | The program that runs the AI models locally. The code sends it requests such as "read this and answer" or "turn this text into numbers". |
| **Embedding** (or **vector**) | A list of 768 numbers that captures the *meaning* of a piece of text. Texts with similar meanings get similar numbers, even when they use different words ("late fee" ≈ "penalty for paying after the due date"). Made by the `nomic-embed-text` model. |
| **Similarity score** | How close two embeddings are, from about 0 (unrelated) to 1 (same meaning). |
| **Chunk** | A slice of a long document, about one page of text, small enough for the AI to read carefully. |
| **Upsert** | "Insert, or update if it already exists." Saving the same rule twice changes one row instead of creating a duplicate. |
| **Hash** (fingerprint) | A short code calculated from a piece of text. The same text always gives the same code; any change gives a different one. |
| **Trigger, `NOTIFY` / `LISTEN`** | A database doorbell. When a rule is saved, a trigger rings the bell (`NOTIFY`); the worker is waiting for it (`LISTEN`). |
| **Connection pool** | A set of database connections kept open and reused, like keeping phone lines open instead of dialling for every call. |
| **Cache** | A store of answers already worked out, so the same work isn't repeated. |

### How the three programs fit together

```
 Life of a rule:     document ─► parse_disclosure.py ─► core.bank_rulebook ─(doorbell)─► embedding_worker.py ─► rule gets its 768 numbers
 Life of a question: customer ─► app.py ─► question gets its 768 numbers ─► 2 closest rules ─► AI explains them (or politely declines) ─► audit log
```

The three programs never call each other directly. They share the database: the ingestion script writes rules, the worker adds their embeddings, and the orchestrator reads both.

---

### `ingestion/parse_disclosure.py`: the rule extractor

**In one sentence:** it reads a bank's credit card terms (PDF, text or Markdown), asks the AI to list every rule it finds, double-checks the AI's work, saves each rule in the rulebook, and archives rules that no longer appear in the bank's current documents.

**Who runs it:** you, from the terminal (`python ingestion/parse_disclosure.py ...`). It runs once and exits; it isn't a background service.

Think of it as a careful clerk with a stack of terms-and-conditions booklets. The clerk splits each booklet into pages, has an assistant (the AI) write down every rule on each page, checks the assistant's numbers against the page, files each rule card in the rulebook cabinet, and moves cards that are no longer in any booklet to an archive drawer.

#### What happens, step by step

1. **Read the settings** (`env`). Settings come from your shell first, then from `.env`. The database login is always `rule_loader`, a limited account that can add and update rules but never delete them. A required setting that's missing, such as `LOADER_DB_PASSWORD`, stops the script with a clear message.

2. **Open a run log** (`setup_logging`). Every run gets an ID such as `20-Nov-2026_21-17-45_ab12cd` and a folder under `logs/`:
   - The **terminal** shows short progress lines.
   - **`run.jsonl`** records every detail, one line per event, tagged with which document and which chunk it was working on (`_ContextFilter`, `_JsonFormatter`).
   - **`failed/`** holds a file for each piece that failed, so it can be investigated or retried later.

3. **Check the AI is ready** (`preflight`). Before any real work, it confirms Ollama is reachable and the model is installed, then "warms up" the model so the first real request isn't slow. It also checks whether the model is running on the graphics chip (fast) or only on the processor (slow), and warns you if it's slow. If Ollama is down or the model is missing, it stops at once with exit code 1.

4. **Collect the documents** (`run`):
   - `--source` takes one file or web link.
   - `--dir` takes every `.pdf`, `.txt` and `.md` file in a folder.
   - With neither, it uses a built-in sample (`SYNTHETIC_DISCLOSURE_CORPUS`) for demos.
   - An empty folder is an error. It never quietly falls back to the sample, because the sample would then become that bank's entire rulebook.

5. **Read each document safely** (`read_source_content`, `_download`, `read_pdf_text`). These checks stop bad input before the AI spends any time on it:
   - Files or downloads over **25 MB** are refused, and downloads are stopped mid-way if they grow too large or take over **120 seconds**.
   - PDFs over **200 pages** are refused, since a credit card terms document is never that long.
   - A web link is treated as a PDF if its content starts with the PDF signature `%PDF-`, even when the website mislabels it. Web pages (login screens, error pages) are refused.
   - Pages with no readable text (usually scanned images) are reported. A document with no text at all is recorded as a failure.

6. **Make sure it's actually about credit cards** (`check_relevance`). This quick word check needs no AI. The text must mention a credit card, cardholder or card account, **and** at least 4 different card-specific terms (APR, annual fee, grace period, cash advance, late fee, and so on). A pancake recipe or a mortgage contract is refused here. `--skip-relevance-check` overrides it.

7. **Cut it into chunks** (`chunk_document`, `_split_oversized`). A small AI reads better in small portions, so each document is cut at paragraph and section breaks into pieces of about **2,200 characters**. Each piece repeats the last **300** characters of the previous one, so a rule that sits across a cut is never lost. A paragraph that is itself too long is split at line breaks, then at sentence ends, and only cut mid-sentence as a last resort.

8. **Ask the AI for the rules** (`extract_rules_from_chunk`). The AI gets strict instructions: list every rule, keep every number exactly as written, don't invent anything, and return at most 10 rules. Its answer must fill in a fixed form (`RuleExtractionBatch` / `ExtractedClause`), and Ollama won't let it write anything outside that form:
   - **Category**: one of 16 fixed codes, such as `FEE`, `APR` or `DISPUTE` (`CATEGORY_CODES`).
   - **BIAN domain**: one of 9 standard banking areas, such as "Payment Execution" (`BIAN_DOMAINS`).
   - **Title**: up to 150 characters.
   - **Clause text**: the rule itself, up to 2,000 characters.

   The "creativity" setting (temperature) is 0, so the same chunk gets the same answer every time.

9. **Check the AI's work.** Every answer is checked before it's used:
   - An answer that was cut off, or doesn't fit the form, is treated as a failure (`ExtractionError`) and retried.
   - **Number check** (`_is_grounded`): if a rule mentions a number (a fee, a percentage, a number of days) that doesn't appear in the original chunk, the AI made it up, so the rule is thrown away.
   - Duplicate rules in the same chunk are dropped.

10. **Give each rule a permanent ID** (`make_rule_id`, `_normalize_clause`). The ID is a fingerprint of the bank name, the card tier and the rule's text, for example `CHASE-27861EF6C640`. Before the fingerprint is taken, capital letters, quotation marks, extra spaces and trailing punctuation are ignored. The same rule therefore always gets the same ID, even if the AI picks a different title next time, and re-running never creates duplicates. A reworded rule gets a new ID.

11. **Retry when something goes wrong** (`extract_with_retries`, `_extract_split`). A failed chunk gets up to three tries, each with a different approach:
    1. **default**: the normal request.
    2. **resample**: slightly more varied settings, which breaks the AI out of repeating itself in a loop.
    3. **split**: cut the chunk in half and try each half. Rules from a half that works are kept, even if the other half fails.

    A failure that's certain to repeat, such as Ollama being unreachable, isn't retried.

12. **Save the rules** (`load_rules`). Rules are upserted into `core.bank_rulebook` after **each chunk**, so if you press Ctrl-C, everything finished so far is kept. Saving a rule:
    - Stamps `last_seen_at` with the current time, which records "this rule is still in the bank's documents".
    - Changes `updated_at` only if the rule's content really changed.
    - **Brings back** a rule that had been archived, if it appears in the documents again.

    Saving also rings the database doorbell, and the embedding worker takes over from there.

13. **Record anything that failed** (`save_failure_artifact`, `_record_failed_parts`). Each failure gets a file in `logs/<run>/failed/` with the chunk text, the AI's raw reply to every attempt, and the error. You can see exactly what went wrong and retry just those pieces.

14. **Archive rules the bank no longer has** (`snapshot_status`, `reconcile`, `retire_scope`, `_retire_verdict`, `db_now`). One run is treated as the bank's *complete* current rulebook for one card tier, however many files it spans. At the start of the run the database clock is noted (`db_now`). At the end, any active rule of that bank and tier that wasn't seen since then is **retired**: it's hidden from the assistant but kept, with the date and a reason. Rules are never deleted, so the audit log can always show exactly what a customer was told. Several safety checks decide whether archiving is allowed:
    - If any chunk failed, archiving waits until `--retry-failed` fixes it (status `pending`).
    - If a whole document couldn't be read, nothing is archived, because that document's rules would be archived wrongly (status `incomplete`).
    - If the run was interrupted, or saved 0 rules, nothing is archived.
    - If archiving would remove **more than half** of the bank's rules, it's refused, because you probably gave only some of its documents. `--force-retire` overrides this, and `--no-retire` turns archiving off.

15. **Report and exit** (`_summarize`). It prints a summary, lists each failure with the stage where it happened (`read`, `limits`, `relevance`, `extract`, `db`), and prints the exact `--retry-failed` command to use. Exit codes: `0` success, `1` aborted, `2` some parts failed, `130` interrupted.

16. **Retry mode** (`retry_failed`, `load_failed_chunks`, `load_prune_context`). `--retry-failed logs/<run>` re-processes only the pieces saved in that run's `failed/` folder, with the same bank and tier. If everything now succeeds, it finishes the archiving step the original run had to postpone.

---

### `workers/embedding_worker.py`: the indexer

**In one sentence:** it waits for new or changed rules and gives each one its 768-number embedding, so the assistant can find rules by meaning rather than exact words.

**Who runs it:** Docker, as the `bank_embedding_worker` container. It runs all the time in the background and restarts by itself if it stops.

Think of it as a librarian. Whenever a new rule card is filed, the librarian writes a summary code on the card so it can later be found by topic, not just by its exact wording.

#### Its parts

| Part | What it does |
|---|---|
| Settings (top of file) | `DB_URI` (required: the database login, `embed_worker`, supplied by Docker), `OLLAMA_HOST` (where the AI runs), `MODEL_ID` (`nomic-embed-text`), and how often to sweep (`EMBED_SWEEP_INTERVAL_S`, 60 s) and how many rules per sweep (`EMBED_SWEEP_BATCH`, 200). |
| `DOC_PREFIX`, `EMBED_SCHEME` | The embedding model is trained to tell "stored documents" apart from "questions about them". Rules are labelled `search_document: ` and questions `search_query: `, and matching the labels improves results. `EMBED_SCHEME` (`nomic-embed-text\|prefixed-v1`) is a version stamp for this recipe. It's saved with each embedding, so if the model or labels change, the worker knows which rules to redo. |
| `build_document_text` | Builds the text that gets embedded: `search_document: {title} [{BIAN domain}]: {rule text}`. Including the title and domain gives the embedding more context than the rule text alone. |
| `embed_rule` | Does the work for one rule: reads it, asks Ollama for its embedding, then saves the embedding and the version stamp. It also marks **every** open job for that rule as completed and adds one to the job's attempt count. If Ollama fails, the jobs are marked `FAILED` with the error message, so you can see why. If the rule no longer exists, there's nothing to do. |
| `sweep` | The safety net. It finds every **active** rule that has no embedding, or has one made with an older recipe, oldest first, up to 200 at a time, and embeds them. It catches doorbells that rang while the worker was off, earlier failures, and recipe changes. If 3 rules fail in a row (usually Ollama is down), it stops and tries again on the next sweep, rather than marking every remaining rule as failed. Archived rules are skipped. |
| `run_worker` | The main loop. It connects to the database and starts listening for the doorbell (`LISTEN rulebook_events`), runs one sweep straight away, then repeats forever: every 60 seconds run a sweep; otherwise wait up to 5 seconds for a doorbell, and for each one, embed that rule. It starts listening *before* the first sweep, so a rule saved in between is never missed. |

#### What it deliberately doesn't do

- **It never changes a rule's wording.** Its database account can only write the embedding, the version stamp and the update time.
- **It never touches the question cache.** Cached questions depend only on the question, not on the rules.
- **It doesn't survive a lost database connection.** If the connection drops, the program exits. Docker restarts it (`restart: always`), and the startup sweep catches up on anything missed, so no rule is left without an embedding.

---

### `orchestrator/app.py`: the chat service

**In one sentence:** it takes a customer's question, finds the two most relevant active rules for that customer's card issuer and tier, and, only if they're a close enough match, has the AI explain them in plain words with a citation. Otherwise it politely declines. Every conversation is logged.

**Who runs it:** Docker, as the `bank_orchestrator` container. It's a web service on port 8000. Clients send questions to `POST /api/v1/chat`, and `http://localhost:8000/docs` shows a page where you can try it.

Think of it as the bank's front desk. The clerk can only quote from the official rulebook. They look up the most relevant rule cards, and if a card clearly answers the question, they explain it and point to the card. If nothing fits well enough, they say so and refer the customer to customer care, rather than guessing.

#### Its parts

| Part | What it does |
|---|---|
| Settings (top of file) | `DB_URI` (required: the database login, `mcp_assistant`, which can only read active rules and write to the log), `OLLAMA_HOST`, `CONFIDENCE_THRESHOLD` (0.70: the minimum similarity needed to answer), `DB_POOL_MAX` / `DB_POOL_MIN` (how many database connections to keep open, 10 by default), `EMBED_MODEL` (`nomic-embed-text`), `CHAT_MODEL` (`llama3.2:3b`), and `QUERY_PREFIX` / `QUERY_SCHEME` (the `search_query: ` label and version stamp, which must match the worker's). |
| `lifespan` | Runs when the service starts and stops. On start it opens the pool of database connections (and refuses to start if `DB_URI` is missing); on stop it closes them. |
| `_pool_slots` | A queue ticket system. If all 10 connections are busy, a new request waits its turn instead of failing. |
| `db_cursor` | Borrows a connection for one short piece of database work. If the work succeeds it's saved (commit); if not, it's undone (rollback). Either way, the connection goes back to the pool. A broken connection is thrown away instead of being returned. |
| `ChatRequest` | The request form a client fills in: `query` (the question, required), `customer_tier` (default `standard_credit`), `vendor_id` (default `CHASE`) and `session_id` (made up automatically if not given; used to group one customer's questions in the log). |
| `get_query_vector` | Turns the question into its 768 numbers. It first checks the cache, which is keyed by a fingerprint of the version stamp plus the question with case and outer spaces ignored. If the question was asked in the last 24 hours, the stored numbers are reused, which saves an AI call. Otherwise it asks Ollama, with the `search_query: ` label, and stores the result. |
| `retrieve_rules` | Finds the **2 rules** whose embeddings are closest to the question's. It searches only `api.rulebook_vw`, which shows active rules only, and only for the request's card issuer and tier. The similarity score is `1 − distance`. The database connection is handed back **before** the AI writes its answer, so one slow answer never ties up a connection. |
| `handle_chat` (`POST /api/v1/chat`) | Handles each question, as described below. |

#### What happens when a question arrives (`handle_chat`)

1. **Find the closest rules** with `retrieve_rules`.
2. **Decide whether to answer:**
   - If no rules were found, or the best score is **below 0.70**, it returns a fixed message, "This inquiry falls outside the official credit card terms … Please contact customer care", with status **`ABSTAINED`**. The AI isn't called, so this takes a few milliseconds.
   - Otherwise it builds an "evidence" list of the matched rules (domain, rule ID, title, text) and asks the AI to explain the answer simply, using **only** those rules, and to end with an `Official Proof:` line citing the BIAN domain and rule ID. The AI is told it's the bank's "Rulebook Translator" and must not guess or change numbers. Status **`SUCCESS`**.
3. **If anything breaks** (database, Ollama), it answers with status **`ERROR`** and the reason.
4. **Write the audit log**, whatever the outcome. One row goes into `api.chat_audit_log` with the session, tier, question, matched rule IDs, scores, the exact reply, the status, and how long it took. If this write itself fails, the request fails (HTTP 500), so an unrecorded answer is never given.
5. **Reply** with JSON: `status`, `matched_rules`, `similarity_score` (the best score), `latency_ms` and `response`.

#### Why it's built this way

- **The AI never answers from memory.** It only sees the rules the search found, and it isn't called at all when the match is weak. That's what keeps answers tied to the official rulebook.
- **Archived rules are invisible.** The view filters them out, so a rule removed from the bank's current documents stops being quoted immediately, while staying in the database for the audit trail.
- **Busy periods are handled calmly.** The pool and the queue tickets let many customers ask at once without overloading the database.
- **Known gap:** after Postgres restarts, the first few requests can fail until the pool replaces its broken connections. See [Known issues](#known-issues).

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
