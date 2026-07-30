"""Tests for the per-feed observability labels in news_hunter.fetcher.

These labels are what a human reads when a source dies silently, so they have to
stay short, unambiguous between two feeds of the same domain, and never blow up
on a malformed URL.
"""
from __future__ import annotations

from news_hunter.fetcher import (
    _LOG_LIST_CAP,
    _join_capped,
    _strip_domain_prefix,
    feed_label,
)
from news_hunter.sources import google_news_site_queries


def test_label_disambiguates_two_feeds_of_same_domain():
    a = feed_label("exame.com", "https://exame.com/feed/")
    b = feed_label("exame.com", "https://exame.com/noticias/sitemap.xml")
    assert a != b
    assert a == "exame.com/feed"
    assert b == "exame.com/sitemap.xml"


def test_label_handles_trailing_slash():
    # The old label did `url.rsplit("/", 1)[-1]`, which is "" on a trailing
    # slash and fell back to printing the entire URL.
    assert feed_label("monitormercantil.com.br", "https://monitormercantil.com.br/feed/") == (
        "monitormercantil.com.br/feed"
    )


def test_label_falls_back_to_domain_for_bare_root():
    assert feed_label("agencia.petrobras.com.br", "https://agencia.petrobras.com.br/") == (
        "agencia.petrobras.com.br"
    )


def test_gnews_label_names_the_site_operand():
    url = google_news_site_queries(["monitormercantil.com.br"], ["petróleo"], 24)[0]
    assert feed_label("news.google.com", url) == "gnews:monitormercantil.com.br"


def test_gnews_label_degrades_without_site_operand():
    assert feed_label("news.google.com", "https://news.google.com/rss/search?q=oil") == "gnews:query"


def test_strip_domain_prefix_avoids_duplicating_the_domain():
    # _fetch_one returns errors already prefixed with "<domain>: ".
    assert _strip_domain_prefix("monitormercantil.com.br", "monitormercantil.com.br: HTTP 403") == (
        "HTTP 403"
    )
    assert _strip_domain_prefix("other.com", "monitormercantil.com.br: HTTP 403") == (
        "monitormercantil.com.br: HTTP 403"
    )


def test_join_capped_is_sorted_and_bounded():
    assert _join_capped(["b", "a"]) == "a, b"
    labels = [f"d{i:02d}.com/feed" for i in range(_LOG_LIST_CAP + 7)]
    out = _join_capped(labels)
    assert out.endswith("(+7 more)")
    assert out.count(",") == _LOG_LIST_CAP - 1
