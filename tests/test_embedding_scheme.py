"""Embedding task prefixes and catch-up sweep in workers/embedding_worker.py and orchestrator/app.py.

No database or Ollama needed: the DB cursor and Ollama client are faked.
"""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


worker = _load("embedding_worker", "workers/embedding_worker.py")
app = _load("orchestrator_app", "orchestrator/app.py")


class FakeClient:
    def __init__(self, fail_for=()):
        self.prompts, self.fail_for = [], set(fail_for)

    def embeddings(self, model, prompt):
        self.prompts.append(prompt)
        if any(f in prompt for f in self.fail_for):
            raise RuntimeError("ollama down")
        return {"embedding": [0.1] * 768}


class FakeCursor:
    """Answers the worker's SELECTs from an in-memory rulebook and records every statement."""

    def __init__(self, rules, pending=()):
        self.rules, self.pending, self.sql, self._result = rules, list(pending), [], None

    def execute(self, sql, params=()):
        self.sql.append((" ".join(sql.split()), params))
        if "SELECT rule_title" in sql:
            r = self.rules.get(params[0])
            self._result = [r] if r else []
        elif "SELECT rule_id FROM core.bank_rulebook" in sql:
            self._result = [{"rule_id": rid} for rid in self.pending]
        elif sql.strip().startswith("SELECT embedding FROM api.query_embedding_cache"):
            self._result = []

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result


def rule(title="Foreign Fee", domain="Credit Card", clause="3% per foreign transaction."):
    return {"rule_title": title, "bian_domain": domain, "clause_text": clause}


def test_document_and_query_prefixes_match_nomic_tasks():
    assert worker.build_document_text("T", "D", "C") == "search_document: T [D]: C"
    assert app.QUERY_PREFIX == "search_query: "


def test_worker_and_orchestrator_schemes_agree():
    assert worker.EMBED_SCHEME == app.QUERY_SCHEME
    assert worker.MODEL_ID == app.EMBED_MODEL


def test_embed_rule_stores_scheme_and_closes_all_open_jobs():
    cur, client = FakeCursor({"R1": rule()}), FakeClient()
    assert worker.embed_rule(cur, client, "R1")
    assert client.prompts == ["search_document: Foreign Fee [Credit Card]: 3% per foreign transaction."]
    sql, params = cur.sql[-1]
    assert "embedding_scheme = %s" in sql
    assert "WHERE rule_id = %s AND status <> 'COMPLETED'" in sql  # every open job, not just one job_id
    assert params[1] == worker.EMBED_SCHEME


def test_embed_rule_failure_records_attempt_and_error():
    cur = FakeCursor({"R1": rule()})
    assert not worker.embed_rule(cur, FakeClient(fail_for=["Foreign"]), "R1")
    sql, params = cur.sql[-1]
    assert "status = 'FAILED'" in sql and "attempts = attempts + 1" in sql
    assert params == ("ollama down", "R1")


def test_embed_rule_for_pruned_rule_is_a_noop():
    cur, client = FakeCursor({}), FakeClient()
    assert worker.embed_rule(cur, client, "GONE")
    assert client.prompts == []


def test_sweep_selects_missing_or_outdated_scheme():
    cur = FakeCursor({}, pending=[])
    worker.sweep(cur, FakeClient())
    sql, params = cur.sql[0]
    assert "embedding IS NULL OR embedding_scheme IS DISTINCT FROM %s" in sql
    assert "retired_at IS NULL" in sql  # retired rules are never re-embedded
    assert params[0] == worker.EMBED_SCHEME


def test_sweep_skips_a_bad_rule_but_stops_after_three_failures_in_a_row():
    rules = {"BAD": rule(clause="poison"), "OK": rule(clause="fine")}
    cur = FakeCursor(rules, pending=["BAD", "OK"])
    client = FakeClient(fail_for=["poison"])
    worker.sweep(cur, client)
    assert len(client.prompts) == 2  # one bad rule doesn't block the next

    down = FakeClient(fail_for=["search_document"])
    many = {f"R{i}": rule() for i in range(5)}
    worker.sweep(FakeCursor(many, pending=list(many)), down)
    assert len(down.prompts) == 3


def test_query_is_prefixed_and_cache_key_includes_scheme():
    cur, client = FakeCursor({}), FakeClient()
    app.client = client
    app.get_query_vector(cur, "  What is the FX fee? ")
    assert client.prompts == ["search_query:   What is the FX fee? "]
    insert_sql, params = cur.sql[-1]
    assert params[2] == app.QUERY_SCHEME
    # Old cache rows were keyed on the bare query hash; the new key must differ.
    import hashlib
    old_key = hashlib.sha256("what is the fx fee?".encode()).hexdigest()
    assert params[0] != old_key


def test_embed_rule_leaves_the_query_cache_alone():
    # Cached question vectors don't depend on the rules, so embedding a rule must not clear them.
    cur = FakeCursor({"R1": rule()})
    assert worker.embed_rule(cur, FakeClient(), "R1")
    assert not any("query_embedding_cache" in sql for sql, _ in cur.sql)


class FakeConn:
    def __init__(self, fail_rollback=False):
        self.closed, self.commits, self.rollbacks, self.fail_rollback = 0, 0, 0, fail_rollback

    def cursor(self, cursor_factory=None):

        class _Cur(FakeCursor):
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return _Cur({})

    def commit(self):
        self.commits += 1

    def rollback(self):
        if self.fail_rollback:
            self.closed = 2
            raise app.psycopg2.OperationalError("server closed the connection")
        self.rollbacks += 1


class FakePool:
    def __init__(self, conn):
        self.conn, self.returned = conn, []

    def getconn(self):
        return self.conn

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


def test_db_cursor_commits_and_returns_connection(monkeypatch):
    pool = FakePool(FakeConn())
    monkeypatch.setattr(app, "_pool", pool)
    with app.db_cursor() as cur:
        cur.execute("SELECT 1")
    assert pool.conn.commits == 1 and pool.returned == [(pool.conn, False)]


def test_db_cursor_rolls_back_on_error_and_keeps_healthy_connection(monkeypatch):
    pool = FakePool(FakeConn())
    monkeypatch.setattr(app, "_pool", pool)
    with pytest.raises(ValueError):
        with app.db_cursor():
            raise ValueError("bad query")
    assert pool.conn.rollbacks == 1 and pool.conn.commits == 0
    assert pool.returned == [(pool.conn, False)]


def test_db_cursor_discards_a_dead_connection(monkeypatch):
    pool = FakePool(FakeConn(fail_rollback=True))
    monkeypatch.setattr(app, "_pool", pool)
    with pytest.raises(app.psycopg2.OperationalError):
        with app.db_cursor() as cur:
            raise app.psycopg2.OperationalError("server closed the connection")
    assert pool.returned == [(pool.conn, True)]


def test_chat_releases_connection_before_llm_call_and_audits(monkeypatch):
    held = []

    class TrackingPool(FakePool):
        def getconn(self):
            held.append(True)
            return FakeConn()

        def putconn(self, conn, close=False):
            held.pop()

    monkeypatch.setattr(app, "_pool", TrackingPool(None))
    monkeypatch.setattr(app, "retrieve_rules", lambda req: [
        {"rule_id": "R1", "bian_domain": "Card", "rule_title": "Fee", "clause_text": "3%", "similarity_score": 0.9}
    ])

    class ChatClient:
        def chat(self, model, messages):
            assert held == [], "a DB connection was held during the LLM call"
            return {"message": {"content": "3%. Official Proof: Card R1"}}

    monkeypatch.setattr(app, "client", ChatClient())
    out = app.handle_chat(app.ChatRequest(query="fx fee?"))
    assert out["status"] == "SUCCESS" and out["matched_rules"] == ["R1"]
    assert held == []  # audit connection returned too
