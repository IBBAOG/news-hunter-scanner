"""Tests for the "feed answers 200 but stopped moving" detector.

Context: between 2026-08-07 19:55 UTC and 2026-08-14 the Poder360 /feed/ object
was pinned by Cloudflare. Every run kept fetching it successfully, kept parsing
10 dated entries out of it, and kept ingesting nothing new — the domain silently
left the product for six days. The item-count checks cannot see that; only the
date of the newest item can.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from news_hunter.fetcher import (
    RawItem,
    _newest_published,
    _render_stale,
    _stale_feeds,
)
from news_hunter.sources import (
    FEED_STALE_HOURS,
    FEED_STALE_HOURS_DEFAULT,
    RSS_FEEDS,
    feed_stale_hours,
)

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc)
PODER = ("www.poder360.com.br", "https://www.poder360.com.br/feed/")


def _item(published: datetime | None) -> RawItem:
    return RawItem(
        url="https://example.com/a",
        title="t",
        summary="",
        published_at=published,
        source_domain="example.com",
        feed_domain="example.com",
    )


def test_newest_published_ignores_undated_items():
    fresh = NOW - timedelta(hours=1)
    assert _newest_published([_item(None), _item(fresh), _item(NOW - timedelta(days=3))]) == fresh
    assert _newest_published([_item(None)]) is None
    assert _newest_published([]) is None


def test_frozen_feed_is_reported():
    # The real outage: newest item stuck at 2026-08-07 19:55 UTC.
    frozen = {PODER: datetime(2026, 8, 7, 19, 55, tzinfo=timezone.utc)}
    stale = _stale_feeds(frozen, NOW)
    assert [label for label, _, _ in stale] == ["www.poder360.com.br/feed"]
    age_h = stale[0][1]
    assert 160 < age_h < 161  # ~6.7 days


def test_moving_feed_is_not_reported():
    ok = {PODER: NOW - timedelta(hours=1)}
    assert _stale_feeds(ok, NOW) == []


def test_threshold_is_the_domain_budget_not_a_global_one():
    # A day and a half of silence: over the default budget, under ineep's.
    ts = NOW - timedelta(hours=FEED_STALE_HOURS_DEFAULT + 1)
    slow_domain = "ineep.org.br"
    assert slow_domain in FEED_STALE_HOURS  # guards the fixture below
    assert _stale_feeds({PODER: ts}, NOW)
    assert _stale_feeds({(slow_domain, "https://ineep.org.br/feed/"): ts}, NOW) == []


def test_google_news_queries_are_exempt():
    # Their recency is dictated by the `when:` operand in the query, so it says
    # nothing about the health of a feed we registered.
    gnews = ("news.google.com", "https://news.google.com/rss/search?q=site%3Ax.com")
    assert _stale_feeds({gnews: NOW - timedelta(days=30)}, NOW) == []


def test_report_is_sorted_stalest_first_and_names_the_budget():
    entries = {
        PODER: NOW - timedelta(days=6),
        ("diariodopoder.com.br", "https://diariodopoder.com.br/feed"): NOW - timedelta(days=9),
    }
    stale = _stale_feeds(entries, NOW)
    assert [label for label, _, _ in stale] == [
        "diariodopoder.com.br/feed",
        "www.poder360.com.br/feed",
    ]
    rendered = _render_stale(stale)
    assert rendered.startswith("diariodopoder.com.br/feed (newest item 9.0d ago, budget 2d)")
    assert "www.poder360.com.br/feed (newest item 6.0d ago, budget 2d)" in rendered


def test_feed_stale_hours_defaults_for_unknown_domain():
    assert feed_stale_hours("brand.new.example.com") == FEED_STALE_HOURS_DEFAULT
    assert feed_stale_hours("ineep.org.br") > FEED_STALE_HOURS_DEFAULT


def test_poder360_keeps_a_second_independently_cached_feed_path():
    # The whole fix for the six-day outage: one CDN cache object is a single
    # point of failure, so the domain carries two feed paths.
    feeds = RSS_FEEDS["www.poder360.com.br"]
    assert "https://www.poder360.com.br/feed/" in feeds
    assert "https://www.poder360.com.br/feed/atom/" in feeds
