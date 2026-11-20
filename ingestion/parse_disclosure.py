import os
import re
import json
import hashlib
import io
import argparse
import logging
import sys
import time
import traceback
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal

import psycopg2
from psycopg2.extras import execute_batch
from pypdf import PdfReader
from pydantic import BaseModel, Field
import ollama
import requests
import httpx

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def env(key: str, default: str = None) -> str:
    """Read a setting from the environment, falling back to the project's .env file."""
    if key in os.environ:
        return os.environ[key]
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            name, sep, value = line.partition("=")
            if sep and name.strip() == key:
                return value.strip()
    if default is not None:
        return default
    raise SystemExit(f"{key} is not set (export it or add it to {ENV_FILE})")


DB_URI = (
    f"dbname={env('POSTGRES_DB', 'bank_aidb')} "
    f"user=rule_loader "
    f"host={env('DB_HOST', 'localhost')} "
    f"port={env('DB_PORT', '5432')} "
    f"password={env('LOADER_DB_PASSWORD')}"
)
# Use 127.0.0.1, not "localhost": on macOS "localhost" resolves to ::1 first, which is
# where Docker's bank_ollama container listens (CPU-only inside the VM, very slow).
# The native Ollama.app listens on 127.0.0.1 and uses the Metal GPU.
OLLAMA_HOST = env("OLLAMA_HOST", "http://127.0.0.1:11434")
EXTRACTION_MODEL = env("EXTRACTION_MODEL", "llama3.2:3b")
LLM_TIMEOUT_S = float(env("LLM_TIMEOUT_S", "300"))   # hard cap per chunk request
LLM_NUM_CTX = int(env("LLM_NUM_CTX", "8192"))        # prompt + schema + output must fit
LLM_NUM_PREDICT = int(env("LLM_NUM_PREDICT", "2048")) # stops runaway JSON generation
LOG_DIR = Path(env("LOG_DIR", str(Path(__file__).resolve().parent.parent / "logs")))
LOG_LEVEL = env("LOG_LEVEL", "INFO")

# Input guards (see read_source_content / check_relevance / retire_scope)
MAX_SOURCE_MB = float(env("MAX_SOURCE_MB", "25"))            # per file or download
MAX_PDF_PAGES = int(env("MAX_PDF_PAGES", "200"))
DOWNLOAD_DEADLINE_S = float(env("DOWNLOAD_DEADLINE_S", "120"))  # whole download, not per read
MIN_RELEVANCE_TERMS = int(env("MIN_RELEVANCE_TERMS", "4"))    # distinct card-disclosure terms required
PRUNE_MAX_DROP_RATIO = float(env("PRUNE_MAX_DROP_RATIO", "0.5"))  # refuse to prune more than this share
PRUNE_MIN_PRIOR_ROWS = int(env("PRUNE_MIN_PRIOR_ROWS", "5"))  # ratio check only applies at/above this size

# ---------------------------------------------------------------------------
# 0. Execution Logging
#    - console: concise, human-readable progress (LOG_LEVEL / --log-level)
#    - logs/<run_id>/run.jsonl: every event at DEBUG, one JSON object per line,
#      tagged with run_id/source/chunk so a single failure can be traced end to end
#    - logs/<run_id>/failed/: chunk text + raw LLM output for every failed chunk
# ---------------------------------------------------------------------------
log = logging.getLogger("ingest")
_CTX: dict = {}  # run_id / source / chunk, stamped onto every log record


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for k, v in _CTX.items():
            if not hasattr(record, k):
                setattr(record, k, v)
        where = [str(_CTX[k]) for k in ("source", "chunk") if k in _CTX]
        record.where = f"[{' #'.join(where)}] " if where else ""
        return True


class _JsonFormatter(logging.Formatter):
    _STD = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "where"}

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "func": f"{record.funcName}:{record.lineno}",
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in vars(record).items() if k not in self._STD})
        if record.exc_info:
            payload["exc_type"] = record.exc_info[0].__name__
            payload["traceback"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class _ConsoleFormatter(logging.Formatter):
    """Keeps the terminal readable: tracebacks only at DEBUG (they always go to run.jsonl)."""

    def __init__(self, show_tracebacks: bool):
        super().__init__("%(asctime)s %(levelname)-7s %(where)s%(message)s", "%H:%M:%S")
        self.show_tracebacks = show_tracebacks

    def format(self, record: logging.LogRecord) -> str:
        if self.show_tracebacks or not record.exc_info:
            return super().format(record)
        saved = record.exc_info, record.exc_text
        record.exc_info = record.exc_text = None
        try:
            return super().format(record)
        finally:
            record.exc_info, record.exc_text = saved


def _short(err: BaseException, limit: int = 200) -> str:
    """One-line error summary for the console; full detail lives in run.jsonl."""
    first = (str(err).strip().splitlines() or [""])[0]
    msg = f"{type(err).__name__}: {first}"
    return msg if len(msg) <= limit else msg[:limit] + "…"


def setup_logging(level: str) -> Path:
    # e.g. 17-Nov-2026_21-13-08_c29314 (day-month-year, 24-hour time; the suffix keeps same-second runs apart)
    run_id = f"{datetime.now():%d-%b-%Y_%H-%M-%S}_{uuid.uuid4().hex[:6]}"
    run_dir = LOG_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _CTX["run_id"] = run_id

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level.upper())
    console.setFormatter(_ConsoleFormatter(show_tracebacks=level.upper() == "DEBUG"))
    logfile = logging.FileHandler(run_dir / "run.jsonl", encoding="utf-8")
    logfile.setLevel(logging.DEBUG)
    logfile.setFormatter(_JsonFormatter())
    for h in (console, logfile):
        h.addFilter(_ContextFilter())
        log.addHandler(h)
    log.setLevel(logging.DEBUG)
    log.propagate = False
    return run_dir


def save_failure_artifact(run_dir: Path, source: str, chunk_idx, chunk_text: str, err: Exception,
                          vendor: str = None, tier: str = None, stage: str = "extract") -> Path:
    """Persist everything needed to reproduce a failed chunk offline."""
    out_dir = run_dir / "failed"
    out_dir.mkdir(exist_ok=True)
    safe_src = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(source).name)[:60]
    label = f"{chunk_idx:04d}" if isinstance(chunk_idx, int) else str(chunk_idx)
    path = out_dir / f"{safe_src}.chunk{label}.json"
    path.write_text(json.dumps({
        "run_id": _CTX.get("run_id"),
        "source": source,
        "chunk": chunk_idx,
        "vendor": vendor,
        "tier": tier,
        "stage": stage,
        "model": EXTRACTION_MODEL,
        "error": f"{type(err).__name__}: {err}",
        "attempts": getattr(err, "attempts", None),
        "traceback": traceback.format_exception(err),
        "llm_done_reason": getattr(err, "done_reason", None),
        "llm_raw_output": getattr(err, "raw_output", None),
        "chunk_text": chunk_text,
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


class ExtractionError(Exception):
    """LLM call succeeded but its output was unusable; carries the raw output for debugging."""

    def __init__(self, msg: str, raw_output: str = None, done_reason: str = None):
        super().__init__(msg)
        self.raw_output = raw_output
        self.done_reason = done_reason


def _llm_metrics(res) -> dict:
    ns = 1e9
    eval_s = (res.get("eval_duration") or 0) / ns
    return {
        "prompt_tokens": res.get("prompt_eval_count"),
        "output_tokens": res.get("eval_count"),
        "llm_total_s": round((res.get("total_duration") or 0) / ns, 2),
        "llm_load_s": round((res.get("load_duration") or 0) / ns, 2),
        "tokens_per_s": round(res["eval_count"] / eval_s, 1) if res.get("eval_count") and eval_s else None,
        "done_reason": res.get("done_reason"),
    }

# Official BIAN Service Domains for Credit Card & Retail Banking Disclosures
BIAN_DOMAINS = Literal[
    "Customer Agreement",
    "Credit Card",
    "Card Transaction Switch",
    "Issued Device Administration",
    "Card Case",
    "Payment Execution",
    "Reward Points Account",
    "Fraud Evaluation",
    "CreditCharge Card Financial Settlement",
]


# Closed set, enforced by Ollama's grammar. A free-text field let the model write
# "BIAN Service Domain: 4.1.1.1..." here and loop until num_predict ran out.
CATEGORY_CODES = Literal[
    "APR", "FEE", "FX", "PAYMENT", "BILLING", "DISPUTE", "LIMIT", "REWARD", "FRAUD", "GRACE",
    "LIABILITY", "SECURITY", "CARDMGMT", "LEGAL", "PRIVACY", "GENERAL",
]


class ExtractedClause(BaseModel):
    category_code: CATEGORY_CODES = Field(
        description="Rule category: APR, FEE, FX, PAYMENT, BILLING, DISPUTE, LIMIT, REWARD, FRAUD, GRACE, "
                    "LIABILITY, SECURITY, CARDMGMT, LEGAL, PRIVACY or GENERAL"
    )
    bian_domain: BIAN_DOMAINS = Field(
        description="The closest matching BIAN Service Domain for this credit card rule"
    )
    rule_title: str = Field(
        max_length=150,
        description="Concise, specific title for the policy rule (e.g., 'Cash Advance APR & Fee')"
    )
    clause_text: str = Field(
        max_length=2000,  # caps a runaway clause; verbatim rules from a ~2.2k-char chunk fit easily
        description="Exact, self-contained policy rule preserving all percentages, dollar amounts, time windows, and conditions verbatim."
    )


MAX_RULES_PER_CHUNK = int(env("MAX_RULES_PER_CHUNK", "10"))


class RuleExtractionBatch(BaseModel):
    # maxItems is enforced by Ollama's grammar, so the model cannot loop forever emitting rules
    rules: List[ExtractedClause] = Field(max_length=MAX_RULES_PER_CHUNK)


_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)*")


def _normalize_clause(text: str) -> str:
    """Canonical form for identity: case, quotes, whitespace and trailing punctuation don't matter."""
    t = unicodedata.normalize("NFKC", text).lower()
    t = re.sub(r"[\"'\u2018\u2019\u201c\u201d`]", "", t)
    return re.sub(r"\s+", " ", t).strip(" .;:,")


def make_rule_id(vendor_id: str, card_tier: str, clause_text: str) -> str:
    """Same clause -> same id, whatever title or category the model picks on a given run.

    Category and title are deliberately NOT part of the id: the model varies them between runs,
    which used to create duplicate rows (and identical titles on different clauses overwrote
    each other). 12 hex chars = 48 bits, so collisions are negligible at rulebook scale.
    """
    digest = hashlib.sha1(f"{vendor_id}|{card_tier}|{_normalize_clause(clause_text)}".encode("utf-8"))
    return f"{vendor_id}-{digest.hexdigest()[:12].upper()}"


def _is_grounded(clause_text: str, source_text: str) -> bool:
    """Reject clauses whose numbers (fees, %, days) do not appear in the source chunk."""
    source_digits = {n.replace(",", "") for n in _NUMBER_RE.findall(source_text)}
    return all(n.replace(",", "") in source_digits for n in _NUMBER_RE.findall(clause_text))


# ---------------------------------------------------------------------------
# 1. Multi-Channel Document Readers & Structure-Aware Chunker
# ---------------------------------------------------------------------------
def read_pdf_text(pdf: "Path | io.BytesIO") -> str:
    """Accepts a local path or an in-memory stream (PDFs downloaded from a URL)."""
    reader = PdfReader(pdf if isinstance(pdf, io.BytesIO) else str(pdf))
    if len(reader.pages) > MAX_PDF_PAGES:
        raise InputRejected(f"PDF has {len(reader.pages)} pages, over MAX_PDF_PAGES={MAX_PDF_PAGES}; "
                            "key-facts documents are short, so this is probably the wrong file")
    pages, empty = [], []
    for idx, page in enumerate(reader.pages, start=1):
        txt = page.extract_text() or ""
        if txt.strip():
            pages.append(f"--- [Page {idx}] ---\n{txt}")
        else:
            empty.append(idx)
    log.debug("pdf read", extra={"pages": len(reader.pages), "empty_pages": empty})
    if empty:
        log.warning("%d/%d PDF page(s) had no extractable text (scanned images?): %s",
                    len(empty), len(reader.pages), empty[:20])
    return "\n\n".join(pages)


SUPPORTED_SUFFIXES = {".pdf", ".txt", ".md"}


class InputRejected(ValueError):
    """The source was readable but is not something we should ingest (too big, off-topic...)."""


def _download(url: str) -> "tuple[bytes, str]":
    """GET with a byte cap and a whole-download deadline (requests' timeout is per socket read)."""
    max_bytes = int(MAX_SOURCE_MB * 1024 * 1024)
    deadline = time.monotonic() + DOWNLOAD_DEADLINE_S
    with requests.get(url, timeout=30, stream=True) as resp:
        ctype = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
        declared = int(resp.headers.get("Content-Length") or 0)
        log.debug("http fetch", extra={"status": resp.status_code, "content_type": ctype,
                                       "content_length": declared})
        resp.raise_for_status()
        if declared > max_bytes:
            raise InputRejected(f"URL declares {declared / 1e6:.1f} MB, over MAX_SOURCE_MB={MAX_SOURCE_MB}")
        buf = bytearray()
        for block in resp.iter_content(chunk_size=64 * 1024):
            buf.extend(block)
            if len(buf) > max_bytes:
                raise InputRejected(f"download exceeded MAX_SOURCE_MB={MAX_SOURCE_MB}; aborted")
            if time.monotonic() > deadline:
                raise InputRejected(f"download took longer than DOWNLOAD_DEADLINE_S={DOWNLOAD_DEADLINE_S:.0f}s; aborted")
    return bytes(buf), ctype


def read_source_content(source_path_or_url: str) -> str:
    if source_path_or_url.startswith(("http://", "https://")):
        content, ctype = _download(source_path_or_url)
        # Trust the magic bytes over the header: some servers send PDFs as octet-stream.
        if content[:5] == b"%PDF-" or ctype == "application/pdf":
            return read_pdf_text(io.BytesIO(content))
        if ctype in ("text/plain", "text/markdown"):
            return content.decode("utf-8", errors="ignore")
        raise ValueError(
            f"URL returned {ctype or 'unknown content type'}, not a PDF; "
            "key-facts documents must be PDF links (download the PDF and use --source/--dir if needed)")

    p = Path(source_path_or_url).expanduser()
    suffix = p.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError(f"unsupported file type {suffix or '(none)'}; expected one of "
                         f"{', '.join(sorted(SUPPORTED_SUFFIXES))}")
    if not p.is_file():
        raise FileNotFoundError(f"no such file: {p.resolve()}")
    size_mb = p.stat().st_size / (1024 * 1024)
    if size_mb > MAX_SOURCE_MB:
        raise InputRejected(f"file is {size_mb:.1f} MB, over MAX_SOURCE_MB={MAX_SOURCE_MB}")
    if suffix == ".pdf":
        return read_pdf_text(p)
    return p.read_text(encoding="utf-8", errors="ignore")


# Distinct signals of a credit card disclosure / key-facts statement. Generic finance words
# (interest, fee, payment) are deliberately absent: mortgages and loan agreements have them too.
_RELEVANCE_TERMS = {
    "apr": r"\bAPRs?\b|annual\s+percentage\s+rates?",
    "finance_charge": r"finance\s+charges?",  # non-US agreements (e.g. India) rarely say "APR"
    "annual_fee": r"annual\s+(?:membership\s+)?fee",
    "grace_period": r"grace\s+period",
    "billing_cycle": r"billing\s+(?:cycle|period)|statement\s+(?:balance|closing)",
    "minimum_payment": r"minimum\s+(?:payment|amount\s+due)",
    "balance_transfer": r"balance\s+transfers?",
    "cash_advance": r"cash\s+advances?",
    "foreign_transaction": r"foreign\s+(?:transaction|currency)",
    "late_fee": r"late\s+(?:payment\s+)?fee",
    "penalty_apr": r"penalty\s+(?:APR|rate)",
    "credit_limit": r"credit\s+(?:limit|line)",
    "billing_rights": r"billing\s+(?:error|rights)|dispute",
    "purchases": r"\bpurchases?\b",
}
_CARD_ANCHOR = r"credit\s+card|card\s*(?:holder|member)|card\s+account"


def check_relevance(text: str) -> dict:
    """Cheap pre-LLM gate: does this text look like a credit card disclosure at all?"""
    hits = sorted(k for k, pat in _RELEVANCE_TERMS.items() if re.search(pat, text, re.I))
    anchored = bool(re.search(_CARD_ANCHOR, text, re.I))
    return {"ok": anchored and len(hits) >= MIN_RELEVANCE_TERMS,
            "terms": hits, "anchored": anchored, "required": MIN_RELEVANCE_TERMS}


def _split_oversized(block: str, max_chars: int) -> List[str]:
    """Break a block longer than max_chars on line breaks, then sentence ends, then hard cuts."""
    if len(block) <= max_chars:
        return [block]
    for pattern in (r"\n", r"(?<=[.;:])\s+"):
        parts = [p for p in re.split(pattern, block) if p.strip()]
        if len(parts) > 1:
            out, cur = [], ""
            for part in parts:
                for piece in _split_oversized(part.strip(), max_chars):
                    if cur and len(cur) + len(piece) + 1 > max_chars:
                        out.append(cur)
                        cur = piece
                    else:
                        cur = f"{cur}\n{piece}".strip()
            if cur:
                out.append(cur)
            return out
    return [block[i:i + max_chars] for i in range(0, len(block), max_chars)]


def chunk_document(text: str, max_chars: int = 2200, overlap: int = 300) -> List[str]:
    """Splits disclosures on section/paragraph boundaries while keeping fee tables intact.

    PDF text often has single newlines only, so a whole page arrives as one "paragraph";
    those are split further so no chunk exceeds max_chars (+ overlap).
    """
    paragraphs = [
        piece
        for p in re.split(r"\n{2,}", text)
        if len(p.strip()) > 30
        for piece in _split_oversized(p.strip(), max_chars - overlap)
    ]
    chunks, current = [], ""

    for para in paragraphs:
        if len(current) + len(para) + 2 <= max_chars:
            current = f"{current}\n\n{para}".strip()
        else:
            if current:
                chunks.append(current)
            tail = current[-overlap:] if len(current) > overlap else ""
            current = f"{tail}\n\n{para}".strip()
    if current:
        chunks.append(current)
    return chunks


# ---------------------------------------------------------------------------
# 2. Schema-Constrained LLM Extractor (Llama 3.2 via Ollama)
# ---------------------------------------------------------------------------
def extract_rules_from_chunk(
    client: ollama.Client,
    chunk_text: str,
    card_tier: str,
    vendor_id: str,
    options: dict = None,
) -> List[dict]:
    system_prompt = (
        "You are a Principal Banking Data Architect specializing in BIAN and TOGAF standards. "
        "Extract every distinct credit card policy rule, fee, APR, billing cycle condition, dispute window, "
        "or cardholder obligation from the provided disclosure text. "
        "CRITICAL RULES:\n"
        "1. Preserve every number, percentage, dollar cap, and time window verbatim.\n"
        "2. Do not invent rules not present in the text.\n"
        "3. Map each rule to the most accurate BIAN Service Domain.\n"
        f"4. Return at most {MAX_RULES_PER_CHUNK} rules, each a different clause. Never repeat a rule.\n"
        "5. If the text has no concrete rule (definitions, headings, boilerplate), return {\"rules\": []}."
    )

    res = client.chat(
        model=EXTRACTION_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"DISCLOSURE TEXT:\n{chunk_text}"},
        ],
        format=RuleExtractionBatch.model_json_schema(),
        options={
            "temperature": 0.0,
            "num_ctx": LLM_NUM_CTX,
            "num_predict": LLM_NUM_PREDICT,
            **(options or {}),
        },
        keep_alive="15m",
    )
    raw_json = res["message"]["content"]
    metrics = _llm_metrics(res)
    log.debug("llm response", extra={**metrics, "raw_output": raw_json})
    if res.get("done_reason") == "length":
        raise ExtractionError(
            f"output hit num_predict={LLM_NUM_PREDICT} before the JSON closed",
            raw_output=raw_json, done_reason="length",
        )
    try:
        parsed = RuleExtractionBatch.model_validate_json(raw_json)
    except ValueError as e:  # pydantic.ValidationError subclasses ValueError
        raise ExtractionError(f"LLM output failed schema validation: {e}",
                              raw_output=raw_json, done_reason=res.get("done_reason")) from e

    extracted_records, seen_ids = [], set()
    for item in parsed.rules:
        if not _is_grounded(item.clause_text, chunk_text):
            log.warning("dropped ungrounded rule: %r", item.rule_title,
                        extra={"clause_text": item.clause_text})
            continue
        rule_id = make_rule_id(vendor_id, card_tier, item.clause_text)
        if rule_id in seen_ids:
            log.debug("dropped duplicate rule %s (%r)", rule_id, item.rule_title)
            continue
        seen_ids.add(rule_id)
        extracted_records.append(
            {
                "rule_id": rule_id,
                "category_code": item.category_code,
                "bian_domain": item.bian_domain,
                "card_tier": card_tier,
                "vendor_id": vendor_id,
                "rule_title": item.rule_title.strip(),
                "clause_text": item.clause_text.strip(),
            }
        )
    log.debug("chunk extraction stats", extra={
        **metrics, "llm_rules": len(parsed.rules), "kept_rules": len(extracted_records),
    })
    return extracted_records


# Errors worth retrying with different settings. Connection refused / model missing are not:
# those fail identically every time, so they surface immediately.
RETRYABLE = (ExtractionError, httpx.TimeoutException)
RETRY_ATTEMPTS = int(env("RETRY_ATTEMPTS", "3"))  # 1 disables retries


def extract_with_retries(client, chunk_text: str, card_tier: str, vendor_id: str):
    """Retry ladder for a chunk whose extraction failed.

    1. default     - temperature 0 (deterministic, so repeating it alone is pointless)
    2. resample    - temperature 0.3 + repeat_penalty + new seed: escapes degenerate loops
    3. split       - halve the chunk and extract each half; less input, less output per call

    Returns (records, failed_parts). failed_parts is non-empty only when 'split' recovered some
    parts but not others: [(part_text, error), ...], so the caller can save them for a later retry.
    On total failure the raised error carries every attempt's raw output (err.attempts)
    so the failure artifact shows how each strategy went wrong.
    """
    ladder = [
        ("default", None),
        ("resample", {"temperature": 0.3, "repeat_penalty": 1.15, "seed": 42}),
        ("split", {"temperature": 0.2, "repeat_penalty": 1.15, "seed": 7}),
    ][:max(1, RETRY_ATTEMPTS)]
    attempts = []
    for n, (strategy, opts) in enumerate(ladder, start=1):
        t0 = time.monotonic()
        try:
            if strategy == "split":
                records, failed_parts = _extract_split(client, chunk_text, card_tier, vendor_id, opts)
            else:
                records, failed_parts = extract_rules_from_chunk(client, chunk_text, card_tier, vendor_id, opts), []
        except RETRYABLE as e:
            attempts.append({"attempt": n, "strategy": strategy, "error": _short(e, 500),
                             "raw_output": getattr(e, "raw_output", None),
                             "seconds": round(time.monotonic() - t0, 1)})
            more = n < len(ladder)
            log.warning("attempt %d/%d (%s) failed: %s%s", n, len(ladder), strategy, _short(e),
                        f" -> retrying with '{ladder[n][0]}'" if more else "",
                        extra={"attempt": n, "strategy": strategy})
            if not more:
                e.attempts = attempts
                raise
            continue
        if n > 1:
            log.info("♻️ recovered on attempt %d (%s)", n, strategy,
                     extra={"attempt": n, "strategy": strategy, "recovered": True})
        return records, failed_parts


def _extract_split(client, chunk_text: str, card_tier: str, vendor_id: str, opts: dict):
    """Extract from halves of the chunk; keep what succeeds, fail only if every part fails."""
    parts = _split_oversized(chunk_text, max(400, len(chunk_text) // 2 + 100))
    if len(parts) < 2:
        raise ExtractionError("chunk could not be split further")
    records, failed_parts = {}, []
    for i, part in enumerate(parts, start=1):
        try:
            for r in extract_rules_from_chunk(client, part, card_tier, vendor_id, opts):
                records.setdefault(r["rule_id"], r)
        except RETRYABLE as e:
            failed_parts.append((part, e))
            log.warning("split part %d/%d failed: %s", i, len(parts), _short(e))
    if len(failed_parts) == len(parts):
        raise failed_parts[-1][1]
    if failed_parts:
        log.warning("⚠️ partial recovery: %d/%d split part(s) failed; kept %d rule(s) from the rest",
                    len(failed_parts), len(parts), len(records), extra={"partial": True})
    return list(records.values()), failed_parts


def _record_failed_parts(run_dir, failed, source, chunk_label, failed_parts, vendor, tier):
    """Save split parts that could not be extracted so --retry-failed can pick them up."""
    for i, (part, err) in enumerate(failed_parts, start=1):
        label = f"{chunk_label}.p{i}"
        artifact = save_failure_artifact(run_dir, source, label, part, err, vendor, tier)
        log.error("❌ split part %s lost: %s | details: %s", label, _short(err), artifact,
                  extra={"artifact": str(artifact)})
        failed.append({"source": source, "chunk": label, "stage": "extract",
                       "error": f"{type(err).__name__}: {err}", "artifact": str(artifact)})


def load_failed_chunks(prev_run_dir: Path):
    """Read failure artifacts from a previous run, plus that run's vendor/tier as a fallback."""
    prev_args = {}
    jsonl = prev_run_dir / "run.jsonl"
    if jsonl.exists():
        for line in jsonl.open(encoding="utf-8"):
            rec = json.loads(line)
            if rec.get("cli_args"):
                prev_args = rec["cli_args"]
                break
    items = []
    for f in sorted((prev_run_dir / "failed").glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        items.append({
            "artifact": str(f), "source": d["source"], "chunk": d["chunk"], "chunk_text": d["chunk_text"],
            "vendor": d.get("vendor") or prev_args.get("vendor"),
            "tier": d.get("tier") or prev_args.get("tier"),
        })
    return items


# ---------------------------------------------------------------------------
# 3. Exhaustive Built-In Corpus (Schumer Box + Cardholder Communications)
# ---------------------------------------------------------------------------
SYNTHETIC_DISCLOSURE_CORPUS = """
SECTION 1: INTEREST RATES AND INTEREST CHARGES (SCHUMER BOX)
Purchase Annual Percentage Rate (APR): 19.24% to 28.99% variable, based on your creditworthiness and Prime Rate.
Balance Transfer APR: 0% introductory APR for the first 15 billing cycles for transfers made within 60 days of account opening. After that, a variable APR of 19.24% to 28.99% applies.
Cash Advance APR: 29.99% variable. Interest accrues immediately from the transaction posting date with no grace period.
Penalty APR and When It Applies: 29.99% variable. This APR may be applied to your account if you make a late payment or make a payment that is returned unpaid. Once applied, the Penalty APR will remain in effect indefinitely until you make six consecutive on-time minimum payments.
Grace Period / Paying Interest: Your due date is at least 21 days after the close of each billing cycle. We will not charge you any interest on purchases if you pay your entire statement balance by the due date each month.

SECTION 2: TRANSACTION AND ACCOUNT FEES
Annual Membership Fee: $0 for Standard Credit tier; $95 for Preferred Rewards tier; $550 for Reserve Infinite tier.
Balance Transfer Fee: Either $5 or 5% of the amount of each transfer, whichever is greater.
Cash Advance Fee: Either $10 or 5% of the amount of each cash advance, whichever is greater.
Foreign Transaction / Cross-Border Currency Conversion Fee: 3.5% of each transaction in U.S. dollars for Standard Credit cards. $0 foreign transaction fee for Reserve Infinite tier.
Late Payment Fee: Up to $40 if the minimum payment is not received by 5:00 PM ET on the payment due date.
Returned Payment / Dishonored Check Fee: Up to $40 when any payment check or ACH debit is returned unpaid by your depository institution.
Over-the-Credit-Limit Fee: $0. We do not charge a fee for exceeding your credit limit, though transactions exceeding your available credit limit may be declined at the point of sale.

SECTION 3: BILLING RIGHTS, DISPUTES, AND CHARGEBACKS (REGULATION Z)
Billing Error Dispute Window: You must notify us in writing or via the secure dispute portal within 60 days after the first statement on which the suspected error or fraudulent charge appeared.
Provisional Credit & Investigation SLA: Within 30 days of receiving your dispute notice, we will acknowledge receipt. Within two complete billing cycles (and no more than 90 days), we will either issue a permanent credit or explain why the charge is valid.
Zero Fraud Liability Policy: You are not liable for unauthorized charges made with your credit card or account number if you report the loss, theft, or suspected unauthorized use promptly.

SECTION 4: CARDHOLDER REWARDS & BENEFITS COMMUNICATION BULLETIN
Reward Points Accrual & Expiration: Cardholders earn 1.5 points per $1 spent on eligible net purchases. Cash advances, balance transfers, wire transfers, and lottery tickets do not earn points. Points do not expire as long as the account remains open and in good standing, but all unredeemed points are forfeited immediately upon voluntary or involuntary account closure.
Travel Cancellation & Interruption Protection: Reimburses up to $10,000 per coveredperson and $20,000 per trip for non-refundable prepaid passenger fares when a trip is canceled due to severe weather, illness, or jury duty, provided the entire fare was charged to the eligible card.
"""


# ---------------------------------------------------------------------------
# 4. Idempotent PostgreSQL Batch Loader
# ---------------------------------------------------------------------------
def load_rules(conn, rules: List[dict], source_doc: str):
    """Upsert by content id. last_seen_at always moves (drives retiring); updated_at only on real change.
    A retired rule that the vendor's documents contain again is reactivated."""
    if not rules:
        return
    rules = [{**r, "source_doc": source_doc} for r in rules]
    cur = conn.cursor()
    upsert_sql = """
        INSERT INTO core.bank_rulebook AS rb (
            rule_id, category_code, bian_domain, card_tier, vendor_id, source_doc,
            rule_title, clause_text, last_seen_at
        )
        VALUES (
            %(rule_id)s, %(category_code)s, %(bian_domain)s, %(card_tier)s, %(vendor_id)s, %(source_doc)s,
            %(rule_title)s, %(clause_text)s, NOW()
        )
        ON CONFLICT (rule_id) DO UPDATE SET
            category_code = EXCLUDED.category_code,
            bian_domain   = EXCLUDED.bian_domain,
            source_doc    = EXCLUDED.source_doc,
            rule_title    = EXCLUDED.rule_title,
            clause_text   = EXCLUDED.clause_text,
            last_seen_at  = NOW(),
            retired_at    = NULL,
            retired_reason = NULL,
            updated_at    = CASE
                WHEN (rb.category_code, rb.bian_domain, rb.rule_title, rb.clause_text)
                     IS DISTINCT FROM
                     (EXCLUDED.category_code, EXCLUDED.bian_domain, EXCLUDED.rule_title, EXCLUDED.clause_text)
                THEN NOW() ELSE rb.updated_at END;
    """
    t0 = time.monotonic()
    try:
        execute_batch(cur, upsert_sql, rules, page_size=50)
        conn.commit()
    except Exception:
        conn.rollback()
        log.error("DB upsert failed; rolled back %d rule(s): %s",
                  len(rules), [r["rule_id"] for r in rules])
        raise
    finally:
        cur.close()
    log.debug("db upsert ok", extra={"rows": len(rules), "db_s": round(time.monotonic() - t0, 3)})
    for r in rules:
        log.info("   📄 [%s] %s -> %s", r["bian_domain"], r["rule_id"], r["rule_title"],
                 extra={"rule_id": r["rule_id"]})


# ---------------------------------------------------------------------------
# 5. Reconciliation: a run is the vendor's complete rulebook for one card tier,
#    across all of its PDFs. Active rules of that vendor+tier the run did not
#    produce are retired (hidden from the assistant, kept for the audit trail).
# ---------------------------------------------------------------------------
def scope_label(scope: dict) -> str:
    return f"{scope['vendor']}/{scope['tier']}"


def snapshot_status(failed: list) -> str:
    """'complete' (retire now), 'pending' (only chunks failed: --retry-failed can finish it) or
    'incomplete' (a whole document is missing: retiring would drop its rules, so never)."""
    if any(f.get("chunk") is None for f in failed):
        return "incomplete"
    return "pending" if failed else "complete"
def db_now(conn):
    """Database clock at run start; rows seen by this run get last_seen_at >= this."""
    with conn.cursor() as cur:
        cur.execute("SELECT now()")
        ts = cur.fetchone()[0]
    conn.commit()  # end the transaction so later NOW() calls are newer
    return ts


def _retire_verdict(seen: int, stale: int, prior: int, force: bool) -> "str | None":
    """Return why retiring should be refused, or None if it is safe."""
    if seen == 0:
        # A complete run that produced nothing is suspicious (wrong files, empty PDFs, model issue).
        return "this run saved 0 rules for the vendor"
    if force:
        return None
    if prior >= PRUNE_MIN_PRIOR_ROWS and stale / prior > PRUNE_MAX_DROP_RATIO:
        # Usually only part of the vendor's documents were given (e.g. just the fee schedule).
        return (f"it would retire {stale}/{prior} ({stale / prior:.0%}) of the vendor's active rules, over "
                f"PRUNE_MAX_DROP_RATIO={PRUNE_MAX_DROP_RATIO:.0%}. A run must include ALL of the vendor's "
                "current documents for this tier; if it does and the terms really changed that much, "
                "re-run with --force-retire")
    return None


def retire_scope(conn, scope: dict, seen_since, force: bool = False) -> int:
    """Retire active rules of one vendor+tier that were not seen since `seen_since`."""
    where = "vendor_id = %(vendor)s AND card_tier = %(tier)s AND retired_at IS NULL"
    docs = ", ".join(scope.get("documents") or []) or "built-in corpus"
    params = {**scope, "since": seen_since,
              "reason": f"not in vendor snapshot run {_CTX.get('run_id')} ({docs})"}
    with conn.cursor() as cur:
        cur.execute(f"""SELECT count(*) FILTER (WHERE last_seen_at >= %(since)s),
                               count(*) FILTER (WHERE last_seen_at IS NULL OR last_seen_at < %(since)s),
                               count(*) FILTER (WHERE created_at < %(since)s)
                        FROM core.bank_rulebook WHERE {where}""", params)
        seen, stale, prior = cur.fetchone()
        refusal = _retire_verdict(seen, stale, prior, force)
        if refusal:
            conn.rollback()
            log.warning("🛑 retire skipped for %s: %s", scope_label(scope), refusal,
                        extra={"event": "retire_skipped", "scope": scope,
                               "seen": seen, "stale": stale, "prior": prior})
            return 0
        cur.execute(f"""UPDATE core.bank_rulebook
                        SET retired_at = NOW(), retired_reason = %(reason)s
                        WHERE {where} AND (last_seen_at IS NULL OR last_seen_at < %(since)s)
                        RETURNING rule_id, rule_title, source_doc""", params)
        retired = cur.fetchall()
    conn.commit()
    for rule_id, title, source_doc in retired:
        log.info("   🗄️ retired %s -> %s (from %s)", rule_id, title, source_doc or "?",
                 extra={"rule_id": rule_id})
    log.info("🧹 %s: %d active rule(s), retired %d no longer in the vendor's documents",
             scope_label(scope), seen, len(retired),
             extra={"event": "retire", "scope": scope, "active": seen, "retired": len(retired)})
    return len(retired)


def reconcile(conn, complete: list, pending: list, seen_since, no_retire: bool,
              force_retire: bool = False) -> None:
    """Retire stale rules of complete vendor scopes now; record pending ones for --retry-failed."""
    for scope in complete:
        if no_retire:
            log.info("⏭️ --no-retire: rules missing from %s's documents left active", scope_label(scope))
        else:
            retire_scope(conn, scope, seen_since, force=force_retire)
    if pending:
        log.warning("⏸️ retiring deferred for %s until --retry-failed clears the failed chunks",
                    [scope_label(s) for s in pending])
    # Always written, so a retry run knows which scopes still await retiring and since when.
    log.info("reconcile context", extra={"event": "prune_context", "seen_since": seen_since.isoformat(),
                                         "pending": pending})


def load_prune_context(prev_run_dir: Path):
    from datetime import datetime as _dt
    for line in (prev_run_dir / "run.jsonl").open(encoding="utf-8"):
        rec = json.loads(line)
        if rec.get("event") == "prune_context":
            return _dt.fromisoformat(rec["seen_since"]), rec.get("pending") or []
    return None, []


def preflight(client: ollama.Client) -> None:
    """Fail fast if Ollama is unreachable or the model is missing, and warn on CPU-only inference."""
    try:
        names = {m["model"] for m in client.list()["models"]}
    except Exception:
        log.critical("❌ Cannot reach Ollama at %s", OLLAMA_HOST, exc_info=True)
        sys.exit(1)
    wanted = EXTRACTION_MODEL if ":" in EXTRACTION_MODEL else f"{EXTRACTION_MODEL}:latest"
    if wanted not in names:
        log.critical(
            "❌ Model '%s' not found on %s. Available: %s. Run `ollama pull %s` or set EXTRACTION_MODEL.",
            EXTRACTION_MODEL, OLLAMA_HOST, sorted(names), EXTRACTION_MODEL,
        )
        sys.exit(1)
    log.info("🔌 Ollama %s | model %s | warming up...", OLLAMA_HOST, EXTRACTION_MODEL)
    t0 = time.monotonic()
    # Warm up with the SAME num_ctx the extraction calls use. Ollama keys a loaded model on its
    # context size: warming up with the Modelfile default (e.g. 16384 in Modelfile.pro) and then
    # chatting at LLM_NUM_CTX forced a full reload, and /api/ps checked during that reload
    # reported 0 bytes of VRAM -> a false "CPU only" warning.
    client.generate(model=EXTRACTION_MODEL, prompt="", keep_alive="15m", options={"num_ctx": LLM_NUM_CTX})

    entry = None
    for _ in range(10):  # the scheduler can briefly report nothing while it settles
        entry = next((m for m in client.ps()["models"] if m["model"] == wanted), None)
        if entry and entry.get("size"):
            break
        time.sleep(0.5)
    warmup_s = round(time.monotonic() - t0, 2)
    if not entry or not entry.get("size"):
        log.warning("⚠️ could not confirm where %s is loaded (not listed by %s/api/ps)", wanted, OLLAMA_HOST)
        return
    gpu_pct = round(100 * (entry.get("size_vram") or 0) / entry["size"])
    placement = "GPU" if gpu_pct >= 99 else ("CPU only" if gpu_pct == 0 else f"{gpu_pct}% GPU / {100 - gpu_pct}% CPU")
    log.info("   model ready in %.1fs (%s)", warmup_s, placement,
             extra={"gpu": gpu_pct > 0, "gpu_pct": gpu_pct, "warmup_s": warmup_s, "num_ctx": LLM_NUM_CTX})
    if gpu_pct < 99:
        if "127.0.0.1" in OLLAMA_HOST:
            hint = ("This is the native Ollama.app. Check `ollama ps`: other loaded models or another "
                    "ingestion run can squeeze GPU memory; lowering LLM_NUM_CTX also reduces it.")
        else:
            hint = ("OLLAMA_HOST may be the Docker container, which has no Metal GPU on macOS. "
                    "Use OLLAMA_HOST=http://127.0.0.1:11434 (native Ollama.app).")
        log.warning("⚠️ Model is only %d%% on GPU; extraction will be slower. %s", gpu_pct, hint)


def main():
    parser = argparse.ArgumentParser(description="Dynamic Credit Card Disclosure & Bulletin Rule Extractor")
    parser.add_argument("--source", type=str, default=None, help="Path to PDF/TXT/MD file or URL to ingest")
    parser.add_argument("--dir", type=str, default=None, help="Directory of PDF/TXT disclosure files to batch ingest")
    parser.add_argument("--retry-failed", type=str, default=None, metavar="RUN_DIR",
                        help="Re-process only the failed chunks of a previous run (e.g. logs/17-Nov-2026_20-47-48_8d95de). "
                             "Vendor/tier default to that run's values.")
    parser.add_argument("--tier", type=str, default=None, help="Card tier (default: standard_credit)")
    parser.add_argument("--vendor", type=str, default=None, help="Issuing vendor ID (default: CHASE)")
    parser.add_argument("--no-retire", "--no-prune", dest="no_prune", action="store_true",
                        help="Keep the vendor's rules that this run no longer extracts active (default: the run "
                             "is the vendor's complete rulebook for the tier, and rules missing from all of its "
                             "documents are retired once every document is processed without failures)")
    parser.add_argument("--force-retire", "--force-prune", dest="force_prune", action="store_true",
                        help=f"Retire even if it would retire more than {PRUNE_MAX_DROP_RATIO * 100:.0f}%% of the "
                             "vendor's active rules (use when the vendor's terms were substantially rewritten)")
    parser.add_argument("--skip-relevance-check", action="store_true",
                        help="Ingest sources even if they do not look like credit card disclosures")
    parser.add_argument("--log-level", default=LOG_LEVEL.upper(),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"], type=str.upper,
                        help="Console verbosity (the run.jsonl file always records DEBUG)")
    args = parser.parse_args()

    if args.retry_failed and (args.source or args.dir):
        parser.error("--retry-failed cannot be combined with --source/--dir")
    if args.no_prune and args.force_prune:
        parser.error("--no-retire and --force-retire are contradictory")
    if not args.retry_failed:
        args.tier = args.tier or "standard_credit"
        args.vendor = args.vendor or "CHASE"

    run_dir = setup_logging(args.log_level)
    log.info("🚀 run %s started | log: %s", _CTX["run_id"], run_dir / "run.jsonl", extra={
        "cli_args": vars(args), "model": EXTRACTION_MODEL, "ollama_host": OLLAMA_HOST,
        "db": f"{env('POSTGRES_DB', 'bank_aidb')}@{env('DB_HOST', 'localhost')}:{env('DB_PORT', '5432')}",
        "num_ctx": LLM_NUM_CTX, "num_predict": LLM_NUM_PREDICT, "timeout_s": LLM_TIMEOUT_S,
        "max_rules_per_chunk": MAX_RULES_PER_CHUNK, "retry_attempts": RETRY_ATTEMPTS,
    })
    try:
        exit_code = retry_failed(args, run_dir) if args.retry_failed else run(args, run_dir)
    except SystemExit:
        raise
    except BaseException:
        log.critical("💥 run aborted by an unhandled error", exc_info=True)
        log.critical("   full traceback in %s", run_dir / "run.jsonl")
        sys.exit(1)
    sys.exit(exit_code)


def run(args, run_dir: Path) -> int:
    client = ollama.Client(host=OLLAMA_HOST, timeout=LLM_TIMEOUT_S)
    preflight(client)

    if args.source:
        candidates = [args.source]
    elif args.dir:
        candidates = [str(p) for p in sorted(Path(args.dir).glob("*"))
                      if p.suffix.lower() in SUPPORTED_SUFFIXES]
        log.info("📂 %d file(s) found in %s", len(candidates), args.dir)
        if not candidates:
            # Never fall back to the built-in corpus: it would become this vendor's whole rulebook.
            log.critical("❌ no supported files (%s) in %s", ", ".join(sorted(SUPPORTED_SUFFIXES)), args.dir)
            return 1
    else:
        log.info("ℹ️ No --source or --dir specified; extracting exhaustive rules from built-in Schumer Box & Bulletin corpus...")
        candidates = []

    sources_to_process, failed = [], []
    for src in candidates:
        _CTX["source"] = Path(src).name
        t0 = time.monotonic()
        try:
            text = read_source_content(src)
        except InputRejected as e:  # a policy decision, not a crash: no traceback
            log.error("❌ rejected %s: %s", src, e)
            failed.append({"source": src, "chunk": None, "stage": "limits", "error": str(e)})
            continue
        except Exception as e:
            log.error("❌ could not read source %s: %s", src, _short(e), exc_info=True)
            failed.append({"source": src, "chunk": None, "stage": "read", "error": f"{type(e).__name__}: {e}"})
            continue
        finally:
            _CTX.pop("source", None)
        log.info("📖 read %s (%d chars) in %.1fs", src, len(text), time.monotonic() - t0,
                 extra={"source": src, "chars": len(text)})
        if not text.strip():
            # Recorded as a failure: a vendor document we could not read must not let its rules be retired.
            log.error("❌ %s produced no text (scanned PDF?); skipping", src)
            failed.append({"source": src, "chunk": None, "stage": "empty", "error": "no extractable text"})
            continue
        rel = check_relevance(text)
        log.debug("relevance check", extra={"source": src, **rel})
        if not rel["ok"]:
            reason = (f"does not look like a credit card disclosure: {len(rel['terms'])}/{rel['required']} "
                      f"card terms found {rel['terms']}"
                      + ("" if rel["anchored"] else ", no mention of credit card/cardholder"))
            if args.skip_relevance_check:
                log.warning("⚠️ %s %s; ingesting anyway (--skip-relevance-check)", src, reason)
            else:
                log.error("❌ rejected %s: %s (override with --skip-relevance-check)", src, reason)
                failed.append({"source": src, "chunk": None, "stage": "relevance", "error": reason})
                continue
        sources_to_process.append((src, text))
    if not candidates:
        sources_to_process.append(("built_in_schumer_corpus", SYNTHETIC_DISCLOSURE_CORPUS))

    conn = psycopg2.connect(DB_URI)
    log.debug("db connected", extra={"server_version": conn.server_version})
    seen_since = db_now(conn)
    total_rules, total_chunks, interrupted, run_start = 0, 0, False, time.monotonic()
    # One reconciliation scope for the whole run: every document given is part of the vendor's
    # current rulebook for this tier, whatever the file names.
    scope = {"vendor": args.vendor, "tier": args.tier,
             "documents": [Path(s).name for s in candidates] or ["built_in_schumer_corpus"]}
    log.info("🏦 vendor snapshot %s: %s", scope_label(scope), scope["documents"],
             extra={"event": "snapshot", "scope": scope})
    try:
        for src_name, raw_text in sources_to_process:
            _CTX["source"] = Path(src_name).name
            source_doc = Path(src_name).name
            chunks = chunk_document(raw_text)
            log.info("🔍 Parsing '%s' (%d chunks) via %s...", src_name, len(chunks), EXTRACTION_MODEL,
                     extra={"chunks": len(chunks), "chunk_sizes": [len(c) for c in chunks]})
            for idx, chunk in enumerate(chunks, start=1):
                _CTX["chunk"] = f"{idx}/{len(chunks)}"
                total_chunks += 1
                log.info("⚙️ extracting (%d chars)...", len(chunk), extra={"chars": len(chunk)})
                t0 = time.monotonic()
                try:
                    batch, failed_parts = extract_with_retries(client, chunk, args.tier, args.vendor)
                except Exception as e:
                    artifact = save_failure_artifact(run_dir, src_name, idx, chunk, e, args.vendor, args.tier)
                    log.error("❌ skipped after %.1fs: %s | details: %s",
                              time.monotonic() - t0, _short(e), artifact,
                              exc_info=True, extra={"artifact": str(artifact)})
                    failed.append({"source": src_name, "chunk": idx, "stage": "extract",
                                   "error": f"{type(e).__name__}: {e}", "artifact": str(artifact)})
                    continue
                log.info("✔️ %d rules in %.1fs", len(batch), time.monotonic() - t0,
                         extra={"rules": len(batch), "chunk_s": round(time.monotonic() - t0, 2)})
                _record_failed_parts(run_dir, failed, src_name, f"{idx:04d}", failed_parts,
                                     args.vendor, args.tier)
                try:
                    load_rules(conn, batch, source_doc)  # saved per chunk
                except psycopg2.Error as e:
                    artifact = save_failure_artifact(run_dir, src_name, idx, chunk, e,
                                                     args.vendor, args.tier, stage="db")
                    log.error("❌ DB write failed: %s | details: %s", _short(e), artifact, exc_info=True,
                              extra={"pgcode": e.pgcode, "artifact": str(artifact)})
                    failed.append({"source": src_name, "chunk": idx, "stage": "db",
                                   "error": f"{type(e).__name__}: {e}", "artifact": str(artifact)})
                    if conn.closed:
                        raise
                    continue
                total_rules += len(batch)
            _CTX.pop("chunk", None)
    except KeyboardInterrupt:
        interrupted = True
        log.warning("⏹️ Interrupted; rules from completed chunks are already saved.")
    finally:
        _CTX.pop("source", None)
        _CTX.pop("chunk", None)
    status = snapshot_status(failed)
    try:
        if interrupted:
            # Unprocessed chunks were never attempted, so there is nothing --retry-failed could finish.
            log.warning("⏹️ %s: nothing retired; re-run the vendor's documents", scope_label(scope))
            reconcile(conn, [], [], seen_since, args.no_prune)
        elif status == "incomplete":
            missing = sorted({Path(f["source"]).name for f in failed if f.get("chunk") is None})
            log.warning("🛑 %s: nothing retired; documents %s were not read, so their rules would be "
                        "retired wrongly. Fix or remove them and re-run the vendor's documents",
                        scope_label(scope), missing,
                        extra={"event": "retire_skipped", "scope": scope, "missing": missing})
            reconcile(conn, [], [], seen_since, args.no_prune)
        elif status == "pending":
            reconcile(conn, [], [scope], seen_since, args.no_prune)
        else:
            reconcile(conn, [scope], [], seen_since, args.no_prune, args.force_prune)
    finally:
        conn.close()

    return _summarize(run_dir, total_rules, total_chunks, failed, interrupted, time.monotonic() - run_start)


def _summarize(run_dir: Path, total_rules: int, total_chunks: int, failed: list,
               interrupted: bool, elapsed: float, retry_of: str = None) -> int:
    log.info("✅ Upserted %d rules into core.bank_rulebook from %d chunk(s) in %.0fs.",
             total_rules, total_chunks, elapsed, extra={
                 "event": "run_summary", "rules": total_rules, "chunks": total_chunks,
                 "failed_count": len(failed), "failed": failed, "elapsed_s": round(elapsed, 1),
                 "interrupted": interrupted, "retry_of": retry_of,
             })
    if failed:
        log.warning("⚠️ %d failure(s):", len(failed))
        for f in failed:
            log.warning("   %s chunk %s [%s] %s", Path(f["source"]).name, f["chunk"], f["stage"],
                        f["error"].splitlines()[0][:200])
        log.warning("   failure details: %s", run_dir / "failed")
        log.warning("   re-process them with: python ingestion/parse_disclosure.py --retry-failed %s", run_dir)
    return 130 if interrupted else (2 if failed else 0)


def retry_failed(args, run_dir: Path) -> int:
    """Re-run extraction for the failed chunks of a previous run (idempotent upserts)."""
    prev = Path(args.retry_failed).expanduser()
    if not (prev / "run.jsonl").exists():
        log.critical("❌ %s is not a run directory (no run.jsonl)", prev)
        return 1
    if not (prev / "failed").is_dir():
        log.info("✅ run %s had no failed chunks; nothing to retry", prev.name)
        return 0
    items = load_failed_chunks(prev)
    log.info("🔁 retrying %d failed chunk(s) from run %s", len(items), prev.name,
             extra={"retry_of": prev.name, "count": len(items)})
    if not items:
        return 0

    seen_since, pending = load_prune_context(prev)
    client = ollama.Client(host=OLLAMA_HOST, timeout=LLM_TIMEOUT_S)
    preflight(client)
    conn = psycopg2.connect(DB_URI)
    total_rules, failed, interrupted, run_start = 0, [], False, time.monotonic()
    try:
        for n, it in enumerate(items, start=1):
            vendor, tier = args.vendor or it["vendor"], args.tier or it["tier"]
            if not vendor or not tier:
                log.error("❌ %s has no vendor/tier recorded; pass --vendor and --tier", it["artifact"])
                failed.append({"source": it["source"], "chunk": it["chunk"], "stage": "retry",
                               "error": "missing vendor/tier", "artifact": it["artifact"]})
                continue
            _CTX["source"] = Path(it["source"]).name
            _CTX["chunk"] = f"{it['chunk']} (retry {n}/{len(items)})"
            t0 = time.monotonic()
            try:
                batch, failed_parts = extract_with_retries(client, it["chunk_text"], tier, vendor)
                load_rules(conn, batch, Path(it["source"]).name)
            except Exception as e:
                stage = "db" if isinstance(e, psycopg2.Error) else "extract"
                artifact = save_failure_artifact(run_dir, it["source"], it["chunk"], it["chunk_text"],
                                                 e, vendor, tier, stage=stage)
                log.error("❌ still failing after %.1fs: %s | details: %s", time.monotonic() - t0,
                          _short(e), artifact, exc_info=True, extra={"artifact": str(artifact)})
                failed.append({"source": it["source"], "chunk": it["chunk"], "stage": stage,
                               "error": f"{type(e).__name__}: {e}", "artifact": str(artifact)})
                if conn.closed:
                    raise
                continue
            log.info("✔️ recovered %d rules in %.1fs", len(batch), time.monotonic() - t0,
                     extra={"rules": len(batch), "recovered_from": it["artifact"]})
            chunk_label = f"{it['chunk']:04d}" if isinstance(it["chunk"], int) else it["chunk"]
            _record_failed_parts(run_dir, failed, it["source"], chunk_label, failed_parts, vendor, tier)
            total_rules += len(batch)
    except KeyboardInterrupt:
        interrupted = True
        log.warning("⏹️ Interrupted; rules from completed chunks are already saved.")
    finally:
        _CTX.pop("source", None)
        _CTX.pop("chunk", None)
    try:
        if seen_since is None:
            log.info("ℹ️ run %s predates pruning support; no stale-rule cleanup to finish", prev.name)
        else:
            legacy = [s for s in pending if "source_doc" in s]
            if legacy:
                # Per-document scopes from before vendor snapshots: finishing them now would treat
                # one file as the vendor's whole rulebook.
                log.warning("ℹ️ run %s predates vendor snapshots; nothing retired for %s. Re-run all of "
                            "the vendor's documents to reconcile", prev.name,
                            [s["source_doc"] for s in legacy])
            pending = [s for s in pending if "source_doc" not in s]
            # Any failure left means the vendor's snapshot still has holes.
            done = [] if (interrupted or failed) else pending
            reconcile(conn, done, [s for s in pending if s not in done], seen_since, args.no_prune,
                      args.force_prune)
    finally:
        conn.close()
    return _summarize(run_dir, total_rules, len(items), failed, interrupted,
                      time.monotonic() - run_start, retry_of=prev.name)


if __name__ == "__main__":
    main()
