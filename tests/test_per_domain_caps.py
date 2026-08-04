"""Regression test for the per-domain cap that ate Google-News-only sources.

Discovered 2026-08-04 while fixing the `when:` position in the GNews queries.

`run_search` limits work per source domain. Until this fix both stages shared
ONE counter (`_enrich_count`):

    phase 2a  submit "resolve this news.google.com wrapper"  -> counter += 1
    phase 2b  submit "fetch_html on the resolved URL"        -> counter += 1

So a single article consumed TWO of the domain's 20 slots, and — worse — the
increments happened in that order. A domain covered ONLY by a Google News
`site:` query that produced >= 20 candidates burned every slot on resolutions,
leaving ZERO for the enrich that actually persists the article. The perverse
result: the more such a domain yielded, the fewer articles it produced.

Measured on a live scan:
  * br.investing.com — 100 fresh items per scan, 0 enrich calls, 0 persisted.
  * www.reuters.com  — 10 persisted while its query returned ~20 fresh items;
    1 persisted once the query started returning 100.

The two stages bound different resources (gnewsdecoder calls vs fetch_html), so
they now have separate counters. This test pins that a GNews-only domain with
more candidates than the cap still gets enrich submissions.

Run from repo root: python -m pytest tests/test_per_domain_caps.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import pipeline  # noqa: E402
from news_hunter.fetcher import RawItem  # noqa: E402

_NOW = datetime.now(timezone.utc)
_KWS = ["Petrobras"]
_DOM = "www.reuters.com"
_N = 40  # comfortably above the cap of 20


def _gnews_items(n: int) -> list[RawItem]:
    return [
        RawItem(
            url=f"https://news.google.com/rss/articles/CBM{i}",
            title=f"Petrobras headline number {i}",
            summary="",
            published_at=_NOW - timedelta(minutes=5),
            source_domain=_DOM,
            feed_domain="news.google.com",
        )
        for i in range(n)
    ]


def _install_stubs(monkeypatch, enrich_calls: list[str], resolve_calls: list[str]):
    monkeypatch.setattr(
        pipeline, "get_config",
        lambda: {"keywords": list(_KWS), "exact_keywords": set(), "window_hours": 24},
    )
    monkeypatch.setattr(pipeline, "upsert_articles", lambda arts: len(arts))
    monkeypatch.setattr(
        pipeline, "iter_collect",
        lambda kw, hours, **k: iter([("news.google.com", _gnews_items(_N), None)]),
    )

    def _fake_resolve(url: str):
        resolve_calls.append(url)
        i = url.rsplit("CBM", 1)[-1]
        return f"https://{_DOM}/business/energy/story-{i}", _DOM

    monkeypatch.setattr(pipeline, "_resolve_google_news_url", _fake_resolve)

    def _fake_enrich(item, **_kw):
        enrich_calls.append(item.url)
        # (snippet, published, resolved_url, resolved_domain, extracted_title)
        return ("Petrobras said on Monday", item.published_at, item.url, item.source_domain, item.title)

    monkeypatch.setattr(pipeline, "enrich_item", _fake_enrich)
    monkeypatch.setattr(pipeline, "source_name_for", lambda d: d)


def test_gnews_only_domain_still_gets_enriched_above_the_cap(monkeypatch):
    enrich_calls: list[str] = []
    resolve_calls: list[str] = []
    _install_stubs(monkeypatch, enrich_calls, resolve_calls)

    res = pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=24)

    # The resolve stage is capped (20), as intended.
    assert len(resolve_calls) == 20, resolve_calls

    # The regression: with a SHARED counter, the 20 resolutions consumed the
    # domain's whole budget and this was 0.
    assert len(enrich_calls) == 20, (
        f"GNews-only domain got {len(enrich_calls)} enrich calls; the resolve "
        "stage is eating the enrich budget again"
    )
    assert res["n_total"] == 20, res


def test_resolve_cap_is_still_enforced_per_domain(monkeypatch):
    """The cap must bound gnewsdecoder calls — it just must not double-charge."""
    enrich_calls: list[str] = []
    resolve_calls: list[str] = []
    _install_stubs(monkeypatch, enrich_calls, resolve_calls)

    pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=24)

    assert len(resolve_calls) < _N, "cap disappeared: every candidate was resolved"


def test_direct_rss_items_are_still_capped(monkeypatch):
    """Non-GNews items keep the original enrich cap of 20 per domain."""
    enrich_calls: list[str] = []
    resolve_calls: list[str] = []
    _install_stubs(monkeypatch, enrich_calls, resolve_calls)
    rss = [
        RawItem(
            url=f"https://exemplo.com.br/noticia-de-petrobras-numero-{i}",
            title=f"Petrobras headline {i}",
            summary="",
            published_at=_NOW - timedelta(minutes=5),
            source_domain="exemplo.com.br",
            feed_domain="exemplo.com.br",
        )
        for i in range(_N)
    ]
    monkeypatch.setattr(
        pipeline, "iter_collect",
        lambda kw, hours, **k: iter([("exemplo.com.br", rss, None)]),
    )

    pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=24)

    assert resolve_calls == []
    assert len(enrich_calls) == 20, len(enrich_calls)
