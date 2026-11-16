import os
import re
import json
import hashlib
import argparse
import sys
import time
from pathlib import Path
from typing import List, Literal

import psycopg2
from psycopg2.extras import execute_batch
from pypdf import PdfReader
from pydantic import BaseModel, Field
import ollama
import requests

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


class ExtractedClause(BaseModel):
    category_code: str = Field(
        description="Short uppercase code such as APR, FEE, FX, DISPUTE, LIMIT, REWARD, FRAUD, or GRACE"
    )
    bian_domain: BIAN_DOMAINS = Field(
        description="The closest matching BIAN Service Domain for this credit card rule"
    )
    rule_title: str = Field(
        description="Concise, specific title for the policy rule (e.g., 'Cash Advance APR & Fee')"
    )
    clause_text: str = Field(
        description="Exact, self-contained policy rule preserving all percentages, dollar amounts, time windows, and conditions verbatim."
    )


MAX_RULES_PER_CHUNK = int(env("MAX_RULES_PER_CHUNK", "10"))


class RuleExtractionBatch(BaseModel):
    # maxItems is enforced by Ollama's grammar, so the model cannot loop forever emitting rules
    rules: List[ExtractedClause] = Field(max_length=MAX_RULES_PER_CHUNK)


_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)*")


def _is_grounded(clause_text: str, source_text: str) -> bool:
    """Reject clauses whose numbers (fees, %, days) do not appear in the source chunk."""
    source_digits = {n.replace(",", "") for n in _NUMBER_RE.findall(source_text)}
    return all(n.replace(",", "") in source_digits for n in _NUMBER_RE.findall(clause_text))


# ---------------------------------------------------------------------------
# 1. Multi-Channel Document Readers & Structure-Aware Chunker
# ---------------------------------------------------------------------------
def read_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    pages = []
    for idx, page in enumerate(reader.pages, start=1):
        txt = page.extract_text() or ""
        if txt.strip():
            pages.append(f"--- [Page {idx}] ---\n{txt}")
    return "\n\n".join(pages)


def read_source_content(source_path_or_url: str) -> str:
    if source_path_or_url.startswith(("http://", "https://")):
        resp = requests.get(source_path_or_url, timeout=15)
        resp.raise_for_status()
        return resp.text

    p = Path(source_path_or_url)
    if p.suffix.lower() == ".pdf":
        return read_pdf_text(p)
    return p.read_text(encoding="utf-8", errors="ignore")


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
        },
        keep_alive="15m",
    )
    if res.get("done_reason") == "length":
        raise ValueError(f"output hit num_predict={LLM_NUM_PREDICT} before the JSON closed")

    raw_json = res["message"]["content"]
    parsed = RuleExtractionBatch.model_validate_json(raw_json)

    extracted_records, seen_ids = [], set()
    for item in parsed.rules:
        if not _is_grounded(item.clause_text, chunk_text):
            print(f"\n      🚫 dropped ungrounded rule: {item.rule_title!r}", end="", flush=True)
            continue
        clean_cat = re.sub(r"[^A-Z0-9]", "", item.category_code.upper())[:8] or "GEN"
        # Deterministic content hash so re-running on the same PDF updates in-place
        short_hash = hashlib.sha1(
            f"{vendor_id}:{card_tier}:{item.rule_title.lower().strip()}".encode("utf-8")
        ).hexdigest()[:6].upper()

        rule_id = f"{vendor_id}-{clean_cat}-{short_hash}"
        if rule_id in seen_ids:
            continue
        seen_ids.add(rule_id)
        extracted_records.append(
            {
                "rule_id": rule_id,
                "bian_domain": item.bian_domain,
                "card_tier": card_tier,
                "vendor_id": vendor_id,
                "rule_title": item.rule_title.strip(),
                "clause_text": item.clause_text.strip(),
            }
        )
    return extracted_records


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
def load_rules(conn, rules: List[dict]):
    if not rules:
        return
    cur = conn.cursor()
    upsert_sql = """
        INSERT INTO core.bank_rulebook (
            rule_id, bian_domain, card_tier, vendor_id, rule_title, clause_text
        )
        VALUES (
            %(rule_id)s, %(bian_domain)s, %(card_tier)s, %(vendor_id)s, %(rule_title)s, %(clause_text)s
        )
        ON CONFLICT (rule_id) DO UPDATE SET
            bian_domain = EXCLUDED.bian_domain,
            card_tier   = EXCLUDED.card_tier,
            vendor_id   = EXCLUDED.vendor_id,
            rule_title  = EXCLUDED.rule_title,
            clause_text = EXCLUDED.clause_text,
            updated_at  = NOW();
    """
    try:
        execute_batch(cur, upsert_sql, rules, page_size=50)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    for r in rules:
        print(f"      📄 [{r['bian_domain']}] {r['rule_id']} -> {r['rule_title']}")


def preflight(client: ollama.Client) -> None:
    """Fail fast if Ollama is unreachable or the model is missing, and warn on CPU-only inference."""
    try:
        names = {m["model"] for m in client.list()["models"]}
    except Exception as e:
        sys.exit(f"❌ Cannot reach Ollama at {OLLAMA_HOST}: {e}")
    wanted = EXTRACTION_MODEL if ":" in EXTRACTION_MODEL else f"{EXTRACTION_MODEL}:latest"
    if wanted not in names:
        sys.exit(
            f"❌ Model '{EXTRACTION_MODEL}' not found on {OLLAMA_HOST}. "
            f"Available: {sorted(names)}. Run `ollama pull {EXTRACTION_MODEL}` or set EXTRACTION_MODEL."
        )
    print(f"🔌 Ollama {OLLAMA_HOST} | model {EXTRACTION_MODEL} | warming up...", flush=True)
    t0 = time.monotonic()
    client.generate(model=EXTRACTION_MODEL, prompt="", keep_alive="15m")
    loaded = [m for m in client.ps()["models"] if m["model"] == wanted]
    gpu = loaded and loaded[0].get("size_vram", 0) > 0
    print(f"   model ready in {time.monotonic() - t0:.1f}s ({'GPU' if gpu else 'CPU only'})", flush=True)
    if not gpu:
        print("   ⚠️ Model is running on CPU; extraction will be slow. "
              "If this is the Docker container, point OLLAMA_HOST at the native Ollama.app.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Dynamic Credit Card Disclosure & Bulletin Rule Extractor")
    parser.add_argument("--source", type=str, default=None, help="Path to PDF/TXT/MD file or URL to ingest")
    parser.add_argument("--dir", type=str, default=None, help="Directory of PDF/TXT disclosure files to batch ingest")
    parser.add_argument("--tier", type=str, default="standard_credit", help="Card tier (e.g., standard_credit, reserve_infinite)")
    parser.add_argument("--vendor", type=str, default="CHASE", help="Issuing vendor ID (e.g., CHASE, AMEX, ANZ)")
    args = parser.parse_args()

    client = ollama.Client(host=OLLAMA_HOST, timeout=LLM_TIMEOUT_S)
    preflight(client)
    sources_to_process = []

    if args.source:
        sources_to_process.append((args.source, read_source_content(args.source)))
    elif args.dir:
        for file_path in sorted(Path(args.dir).glob("*")):
            if file_path.suffix.lower() in {".pdf", ".txt", ".md"}:
                sources_to_process.append((str(file_path), read_source_content(str(file_path))))
    else:
        print("ℹ️ No --source or --dir specified; extracting exhaustive rules from built-in Schumer Box & Bulletin corpus...")
        sources_to_process.append(("built_in_schumer_corpus", SYNTHETIC_DISCLOSURE_CORPUS))

    conn = psycopg2.connect(DB_URI)
    total_rules, failed, run_start = 0, [], time.monotonic()
    try:
        for src_name, raw_text in sources_to_process:
            chunks = chunk_document(raw_text)
            print(f"🔍 Parsing '{src_name}' ({len(chunks)} chunks) via {EXTRACTION_MODEL}...", flush=True)
            for idx, chunk in enumerate(chunks, start=1):
                print(f"   ⚙️ Chunk {idx}/{len(chunks)} ({len(chunk)} chars)...", end=" ", flush=True)
                t0 = time.monotonic()
                try:
                    batch = extract_rules_from_chunk(client, chunk, args.tier, args.vendor)
                except Exception as e:
                    print(f"skipped after {time.monotonic() - t0:.1f}s: {type(e).__name__}: {e}", flush=True)
                    failed.append((src_name, idx))
                    continue
                print(f"{len(batch)} rules in {time.monotonic() - t0:.1f}s", flush=True)
                load_rules(conn, batch)  # Upserts immediately so progress is saved per chunk
                total_rules += len(batch)
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted; rules from completed chunks are already saved.")
    finally:
        conn.close()

    print(f"✅ Upserted {total_rules} rules into core.bank_rulebook in {time.monotonic() - run_start:.0f}s.")
    if failed:
        print(f"⚠️ {len(failed)} chunk(s) failed: {failed}")


if __name__ == "__main__":
    main()
