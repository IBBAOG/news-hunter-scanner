"""Zero-yield guard: a whole stage producing nothing must fail the scan.

2026-09-20 -> 2026-10-19: googlenewsdecoder 0.2.x renamed `status` to
`success`, the resolver dropped every Google News wrapper and the scan logged
`resolve=400(ok=0)` every five minutes for four weeks with a GREEN run. These
tests pin that such a scan now exits non-zero with a ZERO-YIELD ERROR line,
and that a healthy or small scan does not.

Run from repo root: python -m pytest tests/test_zero_yield_guard.py -v
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import news_hunter_service  # noqa: E402
from news_hunter import enrich, fetcher, pipeline, store, supabase_sync  # noqa: E402
from news_hunter.fetcher import RawItem  # noqa: E402
from news_hunter.zero_yield import RULES, new_stage_stats, violations  # noqa: E402

_NOW = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 1. violations()
# ---------------------------------------------------------------------------
def test_new_stage_stats_covers_every_rule():
    assert set(new_stage_stats()) == {r.name for r in RULES}


def test_healthy_scan_has_no_violation():
    # Production numbers of the first scan after the 2026-10-19 hotfix.
    stats = {
        "gnews_resolve": {"ok": 232, "total": 246},
        "registered_feeds": {"ok": 113, "total": 120},
        "gnews_queries": {"ok": 150, "total": 157},
    }
    assert violations(stats) == []


def test_the_incident_trips_gnews_resolve():
    stats = new_stage_stats()
    stats["gnews_resolve"] = {"ok": 0, "total": 400}
    msgs = violations(stats)
    assert len(msgs) == 1
    assert msgs[0].startswith("gnews_resolve ok=0/400 (0%)")


def test_small_sample_is_not_judged():
    # 19 wrappers all failing is a quiet scan, not a broken stage.
    assert violations({"gnews_resolve": {"ok": 0, "total": 19}}) == []
    assert violations({"gnews_resolve": {"ok": 0, "total": 20}}) != []


def test_ratio_boundary_is_strict():
    assert violations({"gnews_resolve": {"ok": 10, "total": 20}}) == []
    assert violations({"gnews_resolve": {"ok": 9, "total": 20}}) != []


def test_mass_dead_feeds_and_blocked_gnews_trip():
    msgs = violations({
        "registered_feeds": {"ok": 30, "total": 120},
        "gnews_queries": {"ok": 2, "total": 157},
    })
    assert [m.split(" ")[0] for m in msgs] == ["registered_feeds", "gnews_queries"]


def test_missing_or_none_stats_are_not_judged():
    assert violations(None) == []
    assert violations({}) == []


# ---------------------------------------------------------------------------
# 2. iter_collect fills the collect-stage counters
# ---------------------------------------------------------------------------
def _item(url: str) -> RawItem:
    host = url.split("//", 1)[1].split("/", 1)[0]
    return RawItem(url=url, title="Oil", summary="",
                   published_at=_NOW - timedelta(minutes=5),
                   source_domain=host, feed_domain=host)


def test_iter_collect_counts_feeds_and_gnews_queries(monkeypatch):
    rss = [("a.com", "https://a.com/feed"), ("b.com", "https://b.com/feed"),
           ("c.com", "https://c.com/feed")]
    gq = [("news.google.com", "https://news.google.com/rss/search?q=1"),
          ("news.google.com", "https://news.google.com/rss/search?q=2")]
    monkeypatch.setattr(fetcher, "all_rss_feeds", lambda: list(rss))
    monkeypatch.setattr(fetcher, "all_standard_sitemaps", lambda: [])
    monkeypatch.setattr(fetcher, "all_homepage_scrapers", lambda: [])
    monkeypatch.setattr(fetcher, "_gnews_tasks", lambda k, h: ([], list(gq), {}))

    def _fetch(url, dom):
        if url == "https://b.com/feed":
            return [], None                       # 200 + zero items
        if url == "https://c.com/feed":
            raise RuntimeError("403")             # raised: no count recorded
        if url.endswith("q=2"):
            return [], None                       # empty GNews query
        return [_item(url.replace("/feed", "/a1").replace("rss/search?q=1", "x"))], None

    monkeypatch.setattr(fetcher, "_fetch_one", _fetch)
    stats = new_stage_stats()
    list(fetcher.iter_collect(["oil"], 24, max_workers=2, stage_stats=stats))

    assert stats["registered_feeds"] == {"ok": 1, "total": 3}
    assert stats["gnews_queries"] == {"ok": 1, "total": 2}


def test_iter_collect_without_stage_stats_is_unchanged(monkeypatch):
    monkeypatch.setattr(fetcher, "all_rss_feeds", lambda: [("a.com", "https://a.com/feed")])
    monkeypatch.setattr(fetcher, "all_standard_sitemaps", lambda: [])
    monkeypatch.setattr(fetcher, "all_homepage_scrapers", lambda: [])
    monkeypatch.setattr(fetcher, "_gnews_tasks", lambda k, h: ([], [], {}))
    monkeypatch.setattr(fetcher, "_fetch_one", lambda url, dom: ([_item("https://a.com/a1")], None))
    out = list(fetcher.iter_collect(["oil"], 24, max_workers=1))
    assert [d for d, _i, _e in out] == ["a.com"]


# ---------------------------------------------------------------------------
# 3. run_search -> service exit code, end to end
# ---------------------------------------------------------------------------
def _scan(monkeypatch, n_wrappers: int, decode, *, collect=None, upsert=None):
    """Run run_search over `n_wrappers` GNews items with collector/sink stubbed.

    decode: True / False patch the resolver (decode / hand the wrapper back);
    None keeps the REAL enrich._resolve_google_news_url; a callable replaces it.
    collect / upsert override the iter_collect / upsert_articles stubs.
    """
    monkeypatch.setattr(
        pipeline, "get_config",
        lambda: {"keywords": ["oil"], "exact_keywords": set(), "window_hours": 24},
    )
    # Spread across domains so the per-domain resolve cap (20) is not the limit.
    items = [
        RawItem(url=f"https://news.google.com/rss/articles/CBM{i}",
                title="Oil prices rise", summary="",
                published_at=_NOW - timedelta(minutes=5),
                source_domain=f"site{i % 4}.com", feed_domain="news.google.com")
        for i in range(n_wrappers)
    ]
    monkeypatch.setattr(
        pipeline, "iter_collect",
        collect or (lambda *a, **k: iter([("news.google.com", items, None)])),
    )
    monkeypatch.setattr(pipeline, "upsert_articles", upsert or (lambda arts: len(arts)))
    monkeypatch.setattr(pipeline, "source_name_for", lambda d: d)
    monkeypatch.setattr(
        pipeline, "enrich_item",
        lambda it, **_k: ("Oil rose.", it.published_at, it.url, it.source_domain, it.title),
    )

    def _resolve(url):
        if not decode:                          # the incident: wrapper comes back
            return url, "news.google.com"
        real = url.replace("https://news.google.com/rss/articles/", "https://pub.com/a/")
        return real, "pub.com"

    if callable(decode):
        monkeypatch.setattr(pipeline, "_resolve_google_news_url", decode)
    elif decode is not None:
        monkeypatch.setattr(pipeline, "_resolve_google_news_url", _resolve)
    return pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=24)


def test_scan_with_broken_decoder_exits_nonzero(monkeypatch, caplog):
    res = _scan(monkeypatch, 40, decode=False)
    assert res["stage_stats"]["gnews_resolve"] == {"ok": 0, "total": 40}
    with caplog.at_level(logging.ERROR, logger="news_hunter_service"):
        code = news_hunter_service._zero_yield_exit(res)
    assert code == news_hunter_service.ZERO_YIELD_EXIT != 0
    assert any("ZERO-YIELD" in r.getMessage() and "gnews_resolve" in r.getMessage()
               for r in caplog.records if r.levelno == logging.ERROR)


def test_scan_with_working_decoder_exits_zero(monkeypatch):
    res = _scan(monkeypatch, 40, decode=True)
    assert res["stage_stats"]["gnews_resolve"] == {"ok": 40, "total": 40}
    assert news_hunter_service._zero_yield_exit(res) == 0


def test_real_resolver_with_missing_decoder_trips(monkeypatch):
    """The import-failure path for real: enrich.gnewsdecoder is None (what a
    broken googlenewsdecoder/selectolax import leaves behind) and the REAL
    resolver hands every wrapper back undecoded."""
    monkeypatch.setattr(enrich, "gnewsdecoder", None)
    res = _scan(monkeypatch, 40, decode=None)
    assert res["stage_stats"]["gnews_resolve"] == {"ok": 0, "total": 40}
    assert news_hunter_service._zero_yield_exit(res) == news_hunter_service.ZERO_YIELD_EXIT


def test_resolve_deadline_cancelling_most_resolves_trips(monkeypatch):
    """RESOLVE_EXTRA expiring before the decodes finish: the cancelled tail
    counts as not decoded."""
    monkeypatch.setattr(pipeline, "RESOLVE_EXTRA", 0.2)

    def _slow(url):
        time.sleep(1.0)
        return url.replace("https://news.google.com/rss/articles/", "https://pub.com/a/"), "pub.com"

    res = _scan(monkeypatch, 40, decode=_slow)
    s = res["stage_stats"]["gnews_resolve"]
    assert s["total"] == 40 and s["ok"] < 20
    assert news_hunter_service._zero_yield_exit(res) == news_hunter_service.ZERO_YIELD_EXIT


def test_collect_crash_exits_fatal(monkeypatch, caplog):
    """run_search catches the crash and returns normally with `fatal:`; the
    stage counters are 0/0 (never judged), so only the fatal check sees it."""
    def _boom(*_a, **_k):
        raise RuntimeError("collect exploded")

    res = _scan(monkeypatch, 40, decode=True, collect=_boom)
    assert any(e.startswith("fatal:") for e in res["errors"])
    assert res["stage_stats"]["gnews_resolve"]["total"] == 0
    with caplog.at_level(logging.ERROR, logger="news_hunter_service"):
        code = news_hunter_service._zero_yield_exit(res)
    assert code == news_hunter_service.FATAL_EXIT != 0
    assert any("FATAL scan error" in r.getMessage() for r in caplog.records)


def test_save_failure_exits_fatal(monkeypatch):
    """Supabase down: the REAL store.upsert_articles -> push_new chain swallows
    the exception and returns 0. Rows to save and none saved must fail the job."""
    class _DeadSink:
        def push(self, _articles):
            raise ConnectionError("supabase unreachable")

    monkeypatch.setattr(supabase_sync, "get_sink", lambda: _DeadSink())
    res = _scan(monkeypatch, 40, decode=True, upsert=store.upsert_articles)
    # 20, not 40: the resolved items all land on pub.com, capped at 20 enriches.
    assert any(e.startswith("fatal: save wrote 0 of ") for e in res["errors"])
    assert news_hunter_service._zero_yield_exit(res) == news_hunter_service.FATAL_EXIT


def test_healthy_scan_has_no_fatal(monkeypatch):
    res = _scan(monkeypatch, 40, decode=True)
    assert not any(str(e).startswith("fatal:") for e in res["errors"])
    assert news_hunter_service._zero_yield_exit(res) == 0
