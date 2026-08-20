"""Guard the repair job's selection and its write shape.

Two properties matter here and neither is obvious from reading the script:

1. It selects round-robin by domain, for the same reason Stage 3d does — on the
   day this was written finance.sina.com.cn held over a third of every bodyless
   row, and a plain ordering would spend the whole run there while the Brazilian
   sources, a handful of articles each, waited behind it.
2. It writes with an UPDATE keyed on url and touches ONLY `snippet`. An upsert
   here would rewrite the whole row and could regress a fabricated
   `published_at` or a translation overlay — exactly the write-once guarantees
   the sink goes out of its way to protect.
"""
from datetime import datetime, timedelta, timezone

from scripts.backfill_snippets import Row, _select

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def _row(url: str, domain: str, minutes_old: int = 0) -> Row:
    return Row(url=url, domain=domain, published_at=NOW - timedelta(minutes=minutes_old))


def test_one_firehose_domain_cannot_consume_the_run():
    rows = [_row(f"https://sina/{i}", "finance.sina.com.cn", i) for i in range(200)]
    rows += [_row(f"https://b247/{i}", "www.brasil247.com", 500 + i) for i in range(9)]
    rows += [_row(f"https://infra/{i}", "agenciainfra.com", 700 + i) for i in range(5)]
    picked = _select(rows, limit=30, per_domain=40)
    assert sum(1 for r in picked if r.domain == "www.brasil247.com") == 9
    assert sum(1 for r in picked if r.domain == "agenciainfra.com") == 5
    assert sum(1 for r in picked if r.domain == "finance.sina.com.cn") == 16


def test_per_domain_cap_is_respected():
    rows = [_row(f"https://a/{i}", "hog.com", i) for i in range(50)]
    picked = _select(rows, limit=50, per_domain=3)
    assert len(picked) == 3


def test_limit_is_respected():
    rows = [_row(f"https://a/{i}", f"d{i}.com", i) for i in range(50)]
    assert len(_select(rows, limit=7, per_domain=40)) == 7


def test_nothing_is_duplicated():
    rows = [_row(f"https://a/{i}", f"d{i % 5}.com", i) for i in range(40)]
    picked = _select(rows, limit=40, per_domain=40)
    assert len({r.url for r in picked}) == len(picked)


def test_the_job_writes_only_the_snippet_column(monkeypatch):
    """An upsert would rewrite the row and could regress a fabricated date or a
    translation overlay. The repair pass must be surgical."""
    import scripts.backfill_snippets as mod

    writes: list[tuple[dict, str]] = []

    class FakeTable:
        def update(self, payload):
            self._payload = payload
            return self

        def eq(self, col, val):
            writes.append((self._payload, f"{col}={val}"))
            return self

        def execute(self):
            return None

        def upsert(self, *a, **kw):  # pragma: no cover - must never be called
            raise AssertionError("the repair pass must never upsert")

    class FakeSink:
        client = type("C", (), {"table": lambda self, name: FakeTable()})()
        table = "news_articles"

    monkeypatch.setattr(mod, "get_sink", lambda: FakeSink())
    monkeypatch.setattr(mod, "_fetch_empty_rows", lambda *a, **kw: [_row("https://a/1", "d.com")])
    monkeypatch.setattr(
        mod, "enrich_item",
        lambda item, **kw: ("corpo real", item.published_at, item.url, item.source_domain, ""),
    )

    assert mod.run(hours=24, limit=10, per_domain=5, domain=None,
                   workers=2, deadline=5.0, dry_run=False) == 0
    assert len(writes) == 1
    payload, key = writes[0]
    assert payload == {"snippet": "corpo real"}
    assert key == "url=https://a/1"


def test_dry_run_writes_nothing(monkeypatch):
    import scripts.backfill_snippets as mod

    class Boom:
        def table(self, name):  # pragma: no cover - must never be reached
            raise AssertionError("--dry-run must not touch the database")

    class FakeSink:
        client = Boom()
        table = "news_articles"

    monkeypatch.setattr(mod, "get_sink", lambda: FakeSink())
    monkeypatch.setattr(mod, "_fetch_empty_rows", lambda *a, **kw: [_row("https://a/1", "d.com")])
    monkeypatch.setattr(
        mod, "enrich_item",
        lambda item, **kw: ("corpo", item.published_at, item.url, item.source_domain, ""),
    )
    assert mod.run(hours=24, limit=10, per_domain=5, domain=None,
                   workers=2, deadline=5.0, dry_run=True) == 0
