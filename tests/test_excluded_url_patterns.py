"""Non-article url shapes must not be persisted.

Audit of `news_articles` on 2026-09-15: the scanner was storing rows whose url
is not an article at all. Each one is a permanent bodiless row - the clipping
generator returns nothing for it, and the feed offers a headline that leads to a
page with no story:

    appointments.thetimes.com/...                8 rows  (The Times JOB BOARD,
                                                          filed under the outlet
                                                          by Google, 0 <p>)
    apnews.com/hub/<topic>                       1 row   (topic index, no date)
    cbsnews.com/video/ , usatoday.com/videos/    3 + 1   (player shells)

Confirmation round 4 (2026-09-15) found the first pass had two coverage gaps on
hosts it already knew: an apnews.com/VIDEO/ row landed at 17:05Z (the rule only
covered /hub/), and USA Today's stored rows use the SINGULAR /video/ while the
rule was written from the plural (2 rows). Both are pinned below: the lesson is
that a host-scoped rule written from the rows present at the time is a sample,
not the shape.
    npr.org/player/embed/...                     1 row   (embeddable player)
    news.sky.com/video/...                       8 rows  (8 of the 9 Sky rows)
    theglobeandmail.com/.../Newswire.ca/<id>/    3 rows  (JS-rendered wire wrapper)

`store.EXCLUDED_URL_PATTERNS` is the single host-scoped url filter. The sibling
`fetcher._NON_ARTICLE_SEGMENTS` does a different job: it prunes listing links
while SCRAPING a homepage and never sees an RSS or a Google News url.

These tests pin both sides: the shapes are dropped, and the real articles of the
same outlets are NOT.

Run from repo root: python -m pytest tests/test_excluded_url_patterns.py -v
"""
from __future__ import annotations

import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import pipeline  # noqa: E402
from news_hunter.fetcher import RawItem  # noqa: E402
from news_hunter.store import (  # noqa: E402
    EXCLUDED_URL_PATTERNS,
    excluded_url_reason,
)

_NOW = datetime.now(timezone.utc)
_KWS = ["oil"]

# (rule key, a url of that shape, as seen in production or built from it)
_EXCLUDED_URLS: tuple[tuple[str, str], ...] = (
    ("thetimes-appointments", "https://appointments.thetimes.com/job/12345/senior-oil-analyst/"),
    ("apnews-topic-hub", "https://apnews.com/hub/oil-and-gas"),
    ("cbsnews-video", "https://www.cbsnews.com/video/oil-prices-surge-after-strikes/"),
    # Round 4: the shape that slipped past the /hub/-only rule at 17:05Z.
    ("apnews-video", "https://apnews.com/video/oil-tanker-fire-opec-9c3f1a2b4d5e6f"),
    ("apnews-video", "https://www.apnews.com/video/opec-meeting-vienna-1a2b3c4d"),
    # Both spellings of the same USA Today player; the live rows are singular.
    ("usatoday-video", "https://www.usatoday.com/videos/news/2026/09/14/oil-tanker-fire/12345/"),
    ("usatoday-video", "https://www.usatoday.com/video/news/2026/09/14/oil-tanker-fire/12345/"),
    ("npr-player-embed", "https://www.npr.org/player/embed/1234567/7654321"),
    ("skynews-video", "https://news.sky.com/video/oil-tanker-attack-off-uae-13456789"),
    ("globeandmail-newswire",
     "https://www.theglobeandmail.com/investing/markets/markets-news/Newswire.ca/33322/oil-co-reports/"),
)

# Real articles on the SAME hosts: a rule that ate these would be worse than the
# rows it removes.
_KEPT_URLS: tuple[str, ...] = (
    "https://www.thetimes.com/business-money/energy/article/north-sea-oil-tax-abc123",
    "https://apnews.com/article/opec-oil-output-8f2c1d0e9b",
    # A story whose SLUG contains the word, on the widened hosts: the rules
    # match a path SEGMENT, never a substring.
    "https://apnews.com/article/oil-video-briefing-opec-8f2c1d0e9b",
    "https://www.usatoday.com/story/money/2026/09/14/oil-video-explainer/98765/",
    "https://www.cbsnews.com/news/oil-prices-gas-pump-2026/",
    "https://www.usatoday.com/story/money/2026/09/14/oil-prices-gas/12345/",
    "https://www.npr.org/2026/09/14/1234567/oil-prices-opec",
    "https://news.sky.com/story/oil-price-jumps-after-attack-13456789",
    "https://www.theglobeandmail.com/business/industry-news/energy/article-oil-sands-output/",
    "https://www.theglobeandmail.com/investing/markets/markets-news/article-oil-rally/",
)


# ---------------------------------------------------------------------------
# 1. The predicate
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("key,url", _EXCLUDED_URLS)
def test_each_non_article_shape_is_recognised(key, url):
    assert excluded_url_reason(url) == key


@pytest.mark.parametrize("url", _KEPT_URLS)
def test_a_real_article_on_the_same_host_is_kept(url):
    assert excluded_url_reason(url) is None


def test_every_rule_has_a_case_and_every_case_a_rule():
    """A rule added without a case (or vice versa) fails here, so the registry
    cannot grow untested."""
    assert {k for k, _ in _EXCLUDED_URLS} == set(EXCLUDED_URL_PATTERNS)


def test_a_rule_cannot_leak_onto_another_host():
    # The netloc regex is fullmatched: a lookalike host is not the host.
    assert excluded_url_reason("https://appointments.thetimes.com.evil.example/job/1") is None
    assert excluded_url_reason("https://notnews.sky.com/video/x") is None
    assert excluded_url_reason("https://example.com/hub/oil") is None
    assert excluded_url_reason("https://example.com/video/oil") is None
    assert excluded_url_reason("https://apnews.com.evil.example/video/oil") is None
    assert excluded_url_reason("https://usatoday.com.evil.example/video/oil") is None


def test_the_widened_video_rules_still_only_match_a_leading_path_segment():
    """`/videos?/` must not become "any path with video in it"."""
    assert excluded_url_reason("https://apnews.com/video/opec-meeting-1a2b") == "apnews-video"
    assert excluded_url_reason("https://www.usatoday.com/video/news/x/1/") == "usatoday-video"
    assert excluded_url_reason("https://www.usatoday.com/videos/news/x/1/") == "usatoday-video"
    # Neither the plural-of-the-plural nor a story that merely mentions it.
    assert excluded_url_reason("https://www.usatoday.com/videoswire/news/x/1/") is None
    assert excluded_url_reason("https://apnews.com/article/video-of-opec-1a2b") is None


def test_unrelated_urls_and_junk_are_ignored():
    assert excluded_url_reason("https://oilprice.com/x/story.html") is None
    assert excluded_url_reason("") is None
    assert excluded_url_reason("not a url") is None


# ---------------------------------------------------------------------------
# 2. End to end through run_search
# ---------------------------------------------------------------------------
def _rss_item(url: str, title: str) -> RawItem:
    host = url.split("//", 1)[1].split("/", 1)[0]
    return RawItem(
        url=url, title=title, summary="",
        published_at=_NOW - timedelta(minutes=5),
        source_domain=host, feed_domain=host,
    )


def _drive(monkeypatch, items, *, gnews=False, resolve=None):
    """Run the pipeline over `items` with collector, enricher and sink stubbed."""
    persisted: list = []
    enrich_calls: list[str] = []
    resolve_calls: list[str] = []

    monkeypatch.setattr(
        pipeline, "get_config",
        lambda: {"keywords": list(_KWS), "exact_keywords": set(), "window_hours": 24},
    )
    feed = "news.google.com" if gnews else "example.com"
    monkeypatch.setattr(
        pipeline, "iter_collect", lambda *a, **k: iter([(feed, items, None)])
    )
    monkeypatch.setattr(pipeline, "upsert_articles",
                        lambda arts: persisted.extend(arts) or len(arts))
    monkeypatch.setattr(pipeline, "source_name_for", lambda d: d)

    def _fake_enrich(it, **_kw):
        enrich_calls.append(it.url)
        return ("Brent oil rose on Monday.", it.published_at, it.url, it.source_domain, it.title)

    monkeypatch.setattr(pipeline, "enrich_item", _fake_enrich)

    if resolve is not None:
        def _fake_resolve(url: str):
            real = resolve[url]
            resolve_calls.append(url)
            return real, real.split("//", 1)[1].split("/", 1)[0]
        monkeypatch.setattr(pipeline, "_resolve_google_news_url", _fake_resolve)

    res = pipeline.run_search(
        include_google_news=gnews, fast_mode=True, hours_override=24
    )
    return res, [a.url for a in persisted], enrich_calls, resolve_calls


def test_rss_route_drops_the_job_board_and_keeps_the_article(monkeypatch):
    items = [
        _rss_item("https://appointments.thetimes.com/job/12345/senior-oil-analyst/",
                  "Senior oil analyst wanted"),
        _rss_item("https://www.thetimes.com/business-money/energy/article/north-sea-oil-tax-abc123",
                  "North Sea oil tax raid"),
    ]
    res, urls, enrich_calls, _ = _drive(monkeypatch, items)

    assert urls == ["https://thetimes.com/business-money/energy/article/north-sea-oil-tax-abc123"]
    # Dropped BEFORE the enrich: the exclusion costs no fetch.
    assert not any("appointments.thetimes.com" in u for u in enrich_calls)
    assert res["excluded"] == 1
    assert res["excluded_by_rule"] == {"thetimes-appointments": 1}


def test_gnews_route_drops_the_video_desk_and_keeps_the_story(monkeypatch):
    """The shapes all arrive through Google News, whose wrapper hides the real
    url until the resolve step - so the drop has to survive that hop."""
    wrappers = {
        "https://news.google.com/rss/articles/CBM1": "https://news.sky.com/video/oil-tanker-attack-13456789",
        "https://news.google.com/rss/articles/CBM2": "https://news.sky.com/story/oil-price-jumps-13456789",
    }
    items = [
        RawItem(url=w, title="Oil tanker attack", summary="",
                published_at=_NOW - timedelta(minutes=5),
                source_domain="news.sky.com", feed_domain="news.google.com")
        for w in wrappers
    ]
    res, urls, enrich_calls, resolve_calls = _drive(
        monkeypatch, items, gnews=True, resolve=wrappers
    )

    assert urls == ["https://news.sky.com/story/oil-price-jumps-13456789"]
    assert len(resolve_calls) == 2          # the url is only knowable after the resolve
    assert not any("/video/" in u for u in enrich_calls)
    assert res["excluded_by_rule"] == {"skynews-video": 1}


def test_a_scan_with_nothing_to_exclude_reports_zero(monkeypatch):
    items = [_rss_item("https://oilprice.com/energy/oil-prices-rise-today", "Oil prices rise")]
    res, urls, _, _ = _drive(monkeypatch, items)
    assert urls == ["https://oilprice.com/energy/oil-prices-rise-today"]
    assert res["excluded"] == 0
    assert res["excluded_by_rule"] == {}


def test_every_shape_is_dropped_end_to_end(monkeypatch):
    """Every rule, through the whole pipeline, in one scan.

    Counted with a Counter rather than one-per-key: a rule that legitimately
    covers two url spellings (usatoday /video/ and /videos/) must report BOTH
    drops under its single name.
    """
    items = [_rss_item(url, "Oil story") for _key, url in _EXCLUDED_URLS]
    items.append(_rss_item("https://oilprice.com/energy/oil-prices-rise-today", "Oil prices rise"))
    res, urls, _, _ = _drive(monkeypatch, items)

    assert urls == ["https://oilprice.com/energy/oil-prices-rise-today"]
    assert res["excluded"] == len(_EXCLUDED_URLS)
    assert res["excluded_by_rule"] == dict(Counter(key for key, _ in _EXCLUDED_URLS))


def test_the_scan_logs_the_count(monkeypatch, caplog):
    import logging

    items = [_rss_item("https://apnews.com/hub/oil-and-gas", "Oil and gas hub")]
    with caplog.at_level(logging.INFO, logger="news_hunter.pipeline"):
        _drive(monkeypatch, items)
    lines = [r.getMessage() for r in caplog.records if "excluded" in r.getMessage()]
    assert lines, "the per-scan exclusion count is not logged"
    assert "excluded 1 non-article urls" in lines[0]
    assert "apnews-topic-hub=1" in lines[0]
