"""Input guards in ingestion/parse_disclosure.py: size/page limits, relevance gate, safe pruning.

No database, Ollama or network needed: HTTP is faked and pruning is tested via its pure verdict.
"""
import importlib.util
import os
from pathlib import Path

import pytest

os.environ.setdefault("LOADER_DB_PASSWORD", "unused-in-tests")
_SPEC = importlib.util.spec_from_file_location(
    "parse_disclosure", Path(__file__).resolve().parent.parent / "ingestion" / "parse_disclosure.py")
pd = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pd)


def make_pdf(pages: list) -> bytes:
    """Minimal valid PDF with one line of Helvetica text per page."""
    objs = ["<< /Type /Catalog /Pages 2 0 R >>", None,
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    kids = []
    for text in pages:
        stream = f"BT /F1 10 Tf 20 700 Td ({text}) Tj ET"
        objs.append(f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                    f"/Resources << /Font << /F1 3 0 R >> >> /Contents {len(objs)} 0 R >>")
        kids.append(f"{len(objs)} 0 R")
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>"
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, body in enumerate(objs, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n{body}\nendobj\n".encode()
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += "".join(f"{o:010d} 00000 n \n" for o in offsets).encode()
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


CARD_TEXT = ("Credit card account agreement. Purchase APR 24.99%. Annual fee $95. Grace period of "
             "at least 21 days. Minimum payment due. Late fee up to $40. Balance transfers 3%.")


# --- relevance gate -------------------------------------------------------
def test_builtin_corpus_is_relevant():
    assert pd.check_relevance(pd.SYNTHETIC_DISCLOSURE_CORPUS)["ok"]


def test_card_disclosure_is_relevant():
    assert pd.check_relevance(CARD_TEXT)["ok"]


@pytest.mark.parametrize("text", [
    # Mortgage: APR, late fee and minimum payment, but no card anchor and few card terms.
    "Mortgage loan estimate. Annual percentage rate 6.8%. Late fee 5% of the payment. "
    "Escrow, closing costs and principal and interest are due monthly.",
    # News article mentioning credit cards in passing.
    "Markets rallied today as consumer credit card spending rose 3% in October.",
    "Quarterly earnings report: revenue grew 12% and operating margin improved.",
])
def test_unrelated_text_is_rejected(text):
    assert not pd.check_relevance(text)["ok"]


def test_card_terms_without_anchor_are_rejected():
    text = CARD_TEXT.replace("Credit card account", "Store")
    res = pd.check_relevance(text)
    assert not res["anchored"] and not res["ok"]


# --- local file limits ----------------------------------------------------
def test_missing_file_gives_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="no such file"):
        pd.read_source_content(str(tmp_path / "nope.pdf"))


def test_oversized_file_rejected(tmp_path, monkeypatch):
    f = tmp_path / "big.txt"
    f.write_text("x" * 2048)
    monkeypatch.setattr(pd, "MAX_SOURCE_MB", 0.001)  # ~1 KB
    with pytest.raises(pd.InputRejected, match="MAX_SOURCE_MB"):
        pd.read_source_content(str(f))


def test_pdf_page_limit(tmp_path, monkeypatch):
    f = tmp_path / "two.pdf"
    f.write_bytes(make_pdf(["page one", "page two"]))
    monkeypatch.setattr(pd, "MAX_PDF_PAGES", 1)
    with pytest.raises(pd.InputRejected, match="MAX_PDF_PAGES"):
        pd.read_source_content(str(f))


def test_local_pdf_reads(tmp_path):
    f = tmp_path / "ok.pdf"
    f.write_bytes(make_pdf(["Annual fee 95 dollars"]))
    assert "Annual fee 95 dollars" in pd.read_source_content(str(f))


# --- URL limits -----------------------------------------------------------
class FakeResponse:
    def __init__(self, body: bytes, ctype: str, length: int = None, status: int = 200):
        self.body, self.status_code = body, status
        self.headers = {"Content-Type": ctype}
        if length is not None:
            self.headers["Content-Length"] = str(length)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise pd.requests.HTTPError(f"{self.status_code}")

    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def fake_get(resp):
    return lambda url, **kw: resp


def test_url_pdf_served_as_octet_stream(monkeypatch):
    monkeypatch.setattr(pd.requests, "get", fake_get(
        FakeResponse(make_pdf(["Cash advance fee 5"]), "application/octet-stream")))
    assert "Cash advance fee 5" in pd.read_source_content("https://bank.example/kf.pdf")


def test_url_html_rejected(monkeypatch):
    monkeypatch.setattr(pd.requests, "get", fake_get(FakeResponse(b"<html>login</html>", "text/html")))
    with pytest.raises(ValueError, match="not a PDF"):
        pd.read_source_content("https://bank.example/kf")


def test_url_declared_size_rejected(monkeypatch):
    monkeypatch.setattr(pd, "MAX_SOURCE_MB", 1)
    monkeypatch.setattr(pd.requests, "get", fake_get(
        FakeResponse(b"", "application/pdf", length=50 * 1024 * 1024)))
    with pytest.raises(pd.InputRejected, match="declares"):
        pd.read_source_content("https://bank.example/huge.pdf")


def test_url_streamed_size_rejected_without_content_length(monkeypatch):
    monkeypatch.setattr(pd, "MAX_SOURCE_MB", 0.1)
    monkeypatch.setattr(pd.requests, "get", fake_get(
        FakeResponse(b"%PDF-" + b"0" * 200_000, "application/pdf")))
    with pytest.raises(pd.InputRejected, match="exceeded"):
        pd.read_source_content("https://bank.example/huge.pdf")


def test_url_http_error_propagates(monkeypatch):
    monkeypatch.setattr(pd.requests, "get", fake_get(FakeResponse(b"", "text/html", status=404)))
    with pytest.raises(pd.requests.HTTPError):
        pd.read_source_content("https://bank.example/missing.pdf")


# --- safe retiring (vendor snapshot) ---------------------------------------
@pytest.mark.parametrize("seen,stale,prior,force,refused", [
    (0, 40, 40, False, True),    # nothing extracted: never prune
    (0, 40, 40, True, True),     # ...not even with --force-prune
    (5, 38, 40, False, True),    # wrong file under the same name: 95% drop
    (5, 38, 40, True, False),    # user confirms the rewrite
    (40, 3, 40, False, False),   # normal update: a few clauses changed
    (40, 20, 40, False, False),  # exactly at the 50% threshold is allowed
    (40, 21, 40, False, True),   # just over it is not
    (2, 3, 3, False, False),     # tiny documents skip the ratio check
    (12, 0, 0, False, False),    # first ingestion: nothing to prune
])
def test_retire_verdict(seen, stale, prior, force, refused):
    assert (pd._retire_verdict(seen, stale, prior, force) is not None) is refused


@pytest.mark.parametrize("failed,status", [
    ([], "complete"),
    ([{"source": "a.pdf", "chunk": 3}], "pending"),                       # --retry-failed can finish it
    ([{"source": "b.pdf", "chunk": None, "stage": "relevance"}], "incomplete"),  # a whole document missing
    ([{"source": "a.pdf", "chunk": 3}, {"source": "b.pdf", "chunk": None}], "incomplete"),
])
def test_snapshot_status(failed, status):
    assert pd.snapshot_status(failed) == status


class _RetireCursor:
    def __init__(self, counts, retired):
        self.counts, self.retired, self.sql = counts, retired, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.counts

    def fetchall(self):
        return self.retired


class _RetireConn:
    def __init__(self, cur):
        self.cur, self.commits, self.rollbacks = cur, 0, 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


SCOPE = {"vendor": "HDFC", "tier": "international_credit",
         "documents": ["agreement-2026.pdf", "fee-schedule.pdf"]}


def test_retire_scope_retires_by_vendor_across_documents_and_never_deletes():
    cur = _RetireCursor((40, 3, 40), [("HDFC-AAA", "Old FX fee", "agreement-2023.pdf")])
    conn = _RetireConn(cur)
    assert pd.retire_scope(conn, SCOPE, "2026-11-19T00:00:00+00:00") == 1
    update_sql, params = cur.sql[-1]
    assert update_sql.startswith("UPDATE core.bank_rulebook SET retired_at = NOW()")
    assert not any("DELETE" in s for s, _ in cur.sql)
    # Scoped by vendor + tier over active rules, not by file name: a rule from the 2023 PDF is retired
    # by a snapshot made of differently named 2026 PDFs.
    assert "vendor_id = %(vendor)s AND card_tier = %(tier)s AND retired_at IS NULL" in update_sql
    assert "source_doc =" not in update_sql
    assert "agreement-2026.pdf, fee-schedule.pdf" in params["reason"]
    assert conn.commits == 1


def test_retire_scope_refuses_partial_vendor_snapshot():
    # Only the fee schedule was given: most of the vendor's agreement rules look stale.
    cur = _RetireCursor((5, 38, 40), [])
    conn = _RetireConn(cur)
    assert pd.retire_scope(conn, SCOPE, "2026-11-19T00:00:00+00:00") == 0
    assert len(cur.sql) == 1 and conn.rollbacks == 1  # counted, nothing updated


def test_upsert_reactivates_a_retired_rule():
    import inspect
    src = inspect.getsource(pd.load_rules)
    assert "retired_at    = NULL" in src and "retired_reason = NULL" in src


def test_empty_dir_is_an_error_not_the_builtin_corpus(tmp_path, monkeypatch):
    monkeypatch.setattr(pd, "preflight", lambda client: None)
    monkeypatch.setattr(pd.psycopg2, "connect", lambda *a, **k: pytest.fail("must not touch the DB"))
    args = pd.argparse.Namespace(source=None, dir=str(tmp_path), vendor="HDFC", tier="international_credit",
                                 no_prune=False, force_prune=False, skip_relevance_check=False)
    assert pd.run(args, tmp_path) == 1
