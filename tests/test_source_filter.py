"""SCAN_ONLY_SOURCES / SCAN_SKIP_SOURCES and the allowlist zero-yield rules.

2026-11-10: Cloudflare 403s GitHub-hosted IPs on every brasilenergia.com.br
page, so the cloud scan (scan.yml) skips Brasil Energia and a BE-only scan runs
from the SectorData self-hosted runner. These tests pin that:
* the filter reaches every place a source is enumerated (feeds, sitemaps,
  homepage scrapers, every Google News `site:` block);
* a typo is a configuration error, not a scan of nothing;
* a BE-only run whose listing yields nothing exits non-zero (ZERO-YIELD), and a
  healthy one exits 0 -- while the stage-wide rules, which need >= 20 tasks,
  keep judging only full scans.

Run from repo root: python -m pytest tests/test_source_filter.py -v
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import news_hunter_service  # noqa: E402
from news_hunter import _clipinator_shim, fetcher, pipeline, source_filter  # noqa: E402
from news_hunter.fetcher import RawItem  # noqa: E402
from news_hunter.sources import HOMEPAGE_SCRAPERS, NO_RSS_DOMAINS  # noqa: E402
from news_hunter.zero_yield import new_stage_stats, violations  # noqa: E402

BE = "www.brasilenergia.com.br"
_NOW = datetime.now(timezone.utc)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(source_filter.ONLY_ENV, raising=False)
    monkeypatch.delenv(source_filter.SKIP_ENV, raising=False)


def _only(monkeypatch, value):
    monkeypatch.setenv(source_filter.ONLY_ENV, value)


def _skip(monkeypatch, value):
    monkeypatch.setenv(source_filter.SKIP_ENV, value)


# ---------------------------------------------------------------------------
# 1. parsing and matching
# ---------------------------------------------------------------------------
def test_no_env_selects_everything():
    assert source_filter.describe() == ""
    assert not source_filter.is_allowlist_run()
    assert source_filter.is_selected(BE)
    assert source_filter.is_selected("www.reuters.com")


def test_alias_covers_both_hosts(monkeypatch):
    _only(monkeypatch, "brasil_energia")
    assert source_filter.is_allowlist_run()
    assert source_filter.is_selected("brasilenergia.com.br")
    assert source_filter.is_selected("www.brasilenergia.com.br")
    assert source_filter.is_selected("https://brasilenergia.com.br/petroleoegas/x")
    assert not source_filter.is_selected("agencia.petrobras.com.br")
    assert source_filter.selected_hosts() == {"brasilenergia.com.br"}


def test_skip_drops_only_the_named_source(monkeypatch):
    _skip(monkeypatch, " Brasil_Energia , ")
    assert not source_filter.is_allowlist_run()
    assert not source_filter.is_selected(BE)
    assert source_filter.is_selected("agencia.petrobras.com.br")
    assert source_filter.filter_domains([BE, "agencia.petrobras.com.br"]) == [
        "agencia.petrobras.com.br"
    ]


def test_skip_wins_over_only(monkeypatch):
    _only(monkeypatch, "brasil_energia,agencia.petrobras.com.br")
    _skip(monkeypatch, "brasil_energia")
    assert source_filter.selected_hosts() == {"agencia.petrobras.com.br"}
    assert not source_filter.is_selected(BE)


def test_host_tokens_accept_www_or_not(monkeypatch):
    _only(monkeypatch, "brasilenergia.com.br")
    assert source_filter.is_selected(BE)


@pytest.mark.parametrize("env", [source_filter.ONLY_ENV, source_filter.SKIP_ENV])
def test_unknown_token_is_a_configuration_error(monkeypatch, env):
    monkeypatch.setenv(env, "brasil_enregia")
    with pytest.raises(source_filter.SourceFilterError):
        source_filter.describe()


def test_service_exits_2_on_a_typo(monkeypatch, caplog):
    monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "k")
    _only(monkeypatch, "brasil_enregia")
    monkeypatch.setattr(sys, "argv", ["news_hunter_service.py", "--once"])
    monkeypatch.setattr(news_hunter_service, "_run_once", lambda: pytest.fail("scan ran"))
    with caplog.at_level(logging.ERROR, logger="news_hunter_service"):
        assert news_hunter_service.main() == 2
    assert any("source filter misconfigured" in r.getMessage() for r in caplog.records)


def test_be_is_registered_where_the_filter_expects_it():
    # The alias is only useful while BE stays a homepage scraper + PT GNews net.
    assert BE in HOMEPAGE_SCRAPERS
    assert BE in NO_RSS_DOMAINS
    assert "brasilenergia.com.br" in source_filter.registered_hosts()


# ---------------------------------------------------------------------------
# 2. Google News blocks honour the filter
# ---------------------------------------------------------------------------
def _sites(urls):
    out = set()
    for u in urls:
        q = unquote(u)
        for part in q.replace("+", " ").replace("?q=", " ").split():
            if part.startswith("site:"):
                out.add(part[5:].lower())
    return out


def test_gnews_skip_removes_be_query(monkeypatch):
    pri, tasks, _ = fetcher._gnews_tasks(["petróleo"], 24)
    assert any("brasilenergia" in s for s in _sites(u for _d, u in tasks))
    _skip(monkeypatch, "brasil_energia")
    pri, tasks, _ = fetcher._gnews_tasks(["petróleo"], 24)
    sites = _sites(u for _d, u in pri + tasks)
    assert sites and not any("brasilenergia" in s for s in sites)


def test_gnews_only_keeps_just_be(monkeypatch):
    _only(monkeypatch, "brasil_energia")
    pri, tasks, lang = fetcher._gnews_tasks(["petróleo"], 24)
    assert pri == []
    sites = _sites(u for _d, u in tasks)
    assert sites and all("brasilenergia" in s for s in sites)
    assert lang == {}


# ---------------------------------------------------------------------------
# 3. iter_collect: tasks filtered + selected_sources counter
# ---------------------------------------------------------------------------
def _item(url: str, feed_domain: str) -> RawItem:
    host = url.split("//", 1)[1].split("/", 1)[0]
    return RawItem(url=url, title="Petróleo sobe", summary="",
                   published_at=_NOW - timedelta(minutes=5),
                   source_domain=host, feed_domain=feed_domain)


def _stub_registries(monkeypatch, home_items):
    rss = [("a.com", "https://a.com/feed")]
    home = [(BE, "https://brasilenergia.com.br/petroleoegas/ultimasnoticias"),
            ("agencia.petrobras.com.br", "https://agencia.petrobras.com.br/")]
    monkeypatch.setattr(fetcher, "all_rss_feeds", lambda: list(rss))
    monkeypatch.setattr(fetcher, "all_standard_sitemaps", lambda: [])
    monkeypatch.setattr(fetcher, "all_homepage_scrapers", lambda: list(home))
    monkeypatch.setattr(fetcher, "_gnews_tasks", lambda k, h: ([], [], {}))
    called: list[str] = []

    def _fetch(url, dom):
        called.append(dom)
        return [_item("https://a.com/a1", dom)], None

    def _home(url, dom):
        called.append(dom)
        return list(home_items(dom)), None

    monkeypatch.setattr(fetcher, "_fetch_one", _fetch)
    monkeypatch.setattr(fetcher, "_scrape_homepage", _home)
    return called


def test_only_run_touches_only_be(monkeypatch):
    _only(monkeypatch, "brasil_energia")
    called = _stub_registries(
        monkeypatch, lambda d: [_item("https://brasilenergia.com.br/petroleoegas/x-y-zzzzzzzzzzzz", d)]
    )
    stats = new_stage_stats()
    out = list(fetcher.iter_collect(["oil"], 24, max_workers=2, stage_stats=stats))
    assert called == [BE]
    assert [d for d, _i, _e in out] == [BE]
    assert stats["selected_sources"] == {"ok": 1, "total": 1, "dead": []}
    assert violations(stats) == []


def test_only_run_with_blocked_listing_trips(monkeypatch):
    _only(monkeypatch, "brasil_energia")
    _stub_registries(monkeypatch, lambda d: [])          # Cloudflare 403 -> []
    stats = new_stage_stats()
    list(fetcher.iter_collect(["oil"], 24, max_workers=2, stage_stats=stats))
    assert stats["selected_sources"] == {"ok": 0, "total": 1, "dead": ["brasilenergia.com.br"]}
    msgs = violations(stats)
    assert len(msgs) == 1 and msgs[0].startswith("selected_sources ok=0/1")
    assert "brasilenergia.com.br" in msgs[0]


def test_skip_run_never_touches_be_and_records_no_selected_counter(monkeypatch):
    _skip(monkeypatch, "brasil_energia")
    called = _stub_registries(monkeypatch, lambda d: [])
    stats = new_stage_stats()
    list(fetcher.iter_collect(["oil"], 24, max_workers=2, stage_stats=stats))
    assert BE not in called and "agencia.petrobras.com.br" in called
    # Not an allowlist run: the selected_* rules stay unjudged (0/0).
    assert stats["selected_sources"] == {"ok": 0, "total": 0}
    assert violations(stats) == []


def test_full_scan_is_not_judged_by_selected_rules(monkeypatch):
    _stub_registries(monkeypatch, lambda d: [])          # every homepage empty
    stats = new_stage_stats()
    list(fetcher.iter_collect(["oil"], 24, max_workers=2, stage_stats=stats))
    assert stats["selected_sources"]["total"] == 0
    assert violations(stats) == []


# ---------------------------------------------------------------------------
# 4. run_search end to end: exit code of a BE-only run
# ---------------------------------------------------------------------------
def _be_scan(monkeypatch, n_items: int, *, page_ok: bool):
    monkeypatch.setattr(
        pipeline, "get_config",
        lambda: {"keywords": ["petróleo"], "exact_keywords": set(), "window_hours": 24},
    )
    items = [
        RawItem(url=f"https://brasilenergia.com.br/petroleoegas/ep/materia-numero-{i}-abcdefghij",
                title=None, summary="", published_at=None,
                source_domain="brasilenergia.com.br", feed_domain=BE,
                title_hint=f"Petróleo {i}", published_hint=_NOW - timedelta(hours=1))
        for i in range(n_items)
    ]
    monkeypatch.setattr(
        pipeline, "iter_collect",
        lambda *a, **k: iter([(BE, items, None)] if items else []),
    )
    monkeypatch.setattr(pipeline, "upsert_articles", lambda arts: len(arts))
    monkeypatch.setattr(pipeline, "get_cached_snippets", lambda keys: {})
    monkeypatch.setattr(pipeline, "source_name_for", lambda d: "Brasil Energia")
    retried: list[bool] = []
    monkeypatch.setattr(pipeline, "_run_translation_retry",
                        lambda *a, **k: retried.append(True) or 0)

    def _enrich(it, **_k):
        if not page_ok:
            return "", None, it.url, it.source_domain, ""
        return "Petróleo body.", it.published_hint, it.url, it.source_domain, "Petróleo"

    monkeypatch.setattr(pipeline, "enrich_item", _enrich)
    res = pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=24)
    return res, retried


def test_be_only_healthy_run_exits_zero_and_skips_global_retry(monkeypatch):
    _only(monkeypatch, "brasil_energia")
    res, retried = _be_scan(monkeypatch, 6, page_ok=True)
    assert res["n_total"] == 6
    assert res["stage_stats"]["selected_pages"] == {"ok": 6, "total": 6}
    assert news_hunter_service._zero_yield_exit(res) == 0
    assert retried == []        # Stage 3e is the full scan's job


def test_be_only_run_whose_pages_all_fail_trips(monkeypatch):
    _only(monkeypatch, "brasil_energia")
    res, _ = _be_scan(monkeypatch, 6, page_ok=False)
    assert res["stage_stats"]["selected_pages"] == {"ok": 0, "total": 6}
    assert news_hunter_service._zero_yield_exit(res) == news_hunter_service.ZERO_YIELD_EXIT


def test_full_scan_still_runs_translation_retry(monkeypatch):
    res, retried = _be_scan(monkeypatch, 1, page_ok=True)
    assert retried == [True]
    assert "selected_pages" not in res["stage_stats"] or res["stage_stats"]["selected_pages"]["total"] == 0


# ---------------------------------------------------------------------------
# 5. the shim never logs in for a skipped Brasil Energia url
# ---------------------------------------------------------------------------
def test_shim_refuses_skipped_be_without_login(monkeypatch):
    _skip(monkeypatch, "brasil_energia")
    from news_hunter import brasilenergia_auth

    monkeypatch.setattr(brasilenergia_auth, "get_auth", lambda: pytest.fail("login attempted"))
    with pytest.raises(RuntimeError, match="skipped by the source filter"):
        _clipinator_shim.fetch_html("https://brasilenergia.com.br/petroleoegas/x")
