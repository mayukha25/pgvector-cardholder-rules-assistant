import os
from pathlib import Path

import psycopg2

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def env(key):
    """Read a setting from the environment, falling back to the project's .env file."""
    if key in os.environ:
        return os.environ[key]
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            name, sep, value = line.partition("=")
            if sep and name.strip() == key:
                return value.strip()
    raise SystemExit(f"{key} is not set (export it or add it to {ENV_FILE})")


DB_URI = (
    "dbname=bank_aidb user=rule_loader host=localhost port=5432 "
    f"password={env('LOADER_DB_PASSWORD')}"
)

SAMPLE_EXTRACTED_RULES = [
    {
        "rule_id": "FEE-FX-001",
        "bian_domain": "Card Transaction Switch",
        "card_tier": "standard_credit",
        "vendor_id": "CHASE",
        "rule_title": "Cross-Border Currency Conversion",
        "clause_text": "Cross-border transactions incur a 3.5% currency conversion fee plus applicable local taxes on all international transactions settled in a foreign currency."
    },
    {
        "rule_id": "FEE-LATE-005",
        "bian_domain": "Customer Agreement",
        "card_tier": "standard_credit",
        "vendor_id": "CHASE",
        "rule_title": "Late Payment Fee & Penalty APR",
        "clause_text": "A Late Payment Fee of up to $40 applies if minimum payment is not received by 5:00 PM on due date. Penalty APR of 29.99% applies after two consecutive missed cycles."
    }
]

def load_rules(rules):
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()
    upsert_sql = """
        INSERT INTO core.bank_rulebook (rule_id, bian_domain, card_tier, vendor_id, rule_title, clause_text)
        VALUES (%(rule_id)s, %(bian_domain)s, %(card_tier)s, %(vendor_id)s, %(rule_title)s, %(clause_text)s)
        ON CONFLICT (rule_id) DO UPDATE SET
            bian_domain = EXCLUDED.bian_domain,
            card_tier   = EXCLUDED.card_tier,
            vendor_id   = EXCLUDED.vendor_id,
            rule_title  = EXCLUDED.rule_title,
            clause_text = EXCLUDED.clause_text,
            updated_at  = NOW();
    """
    for r in rules:
        cur.execute(upsert_sql, r)
        print(f"📄 Loaded/Updated Rule: {r['rule_id']}")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    load_rules(SAMPLE_EXTRACTED_RULES)