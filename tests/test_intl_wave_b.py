"""Wave 5B (2026-09-14) — the one NEW failure mode this wave introduces.

Every generic invariant of an international RSS registration (apex+www twins,
RSS_FEEDS <-> INTERNATIONAL_RSS_DOMAINS mapping, disjointness from the GNews
sets) is already pinned by tests/test_international_rss_source_lang.py and
applies to this wave for free.

What is NEW is a HOST COLLISION that no earlier wave had: this wave registers
the ENGLISH edition of Investing.com (www.investing.com -> apex investing.com,
international/en) while `br.investing.com` has been in NO_RSS_DOMAINS as a
BRAZILIAN (pt / national) source since long before. The two are one DNS label
apart, and the whole national/international discriminator rests on
normalize_url stripping "www." but NOT a real subdomain. A sloppy future edit
(adding "investing.com" to a *_strip_subdomain path, or listing
"br.investing.com" in INTERNATIONAL_RSS_DOMAINS "for symmetry") would silently
reclassify a Brazilian portal as international, or vice-versa, with no error.

These tests assert the VALUE per identity (which host gets which source_lang),
not set membership, so they stay red if the collision is ever introduced.

Run from repo root: python -m pytest tests/test_intl_wave_b.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import _entry_to_item  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    INTERNATIONAL_RSS_DOMAINS,
    NO_RSS_DOMAINS,
    RSS_FEEDS,
)


def _entry(link: str) -> dict:
    return {"link": link, "title": "Oil settles higher", "summary": "body"}


# ---------------------------------------------------------------------------
# The collision itself: two hosts, two verdicts.
# ---------------------------------------------------------------------------
def test_english_investing_item_is_international_en():
    it = _entry_to_item(
        _entry("https://www.investing.com/news/commodities-news/oil-settles-higher-1234"),
        "www.investing.com",
    )
    assert it.source_domain == "investing.com"
    assert it.source_lang == "en"


def test_brazilian_investing_item_stays_national():
    it = _entry_to_item(
        _entry("https://br.investing.com/news/commodities/petroleo-sobe-1234"),
        "br.investing.com",
    )
    # The BR subdomain must survive as its own source_domain (normalize_url
    # strips only a leading "www."), and must NOT be stamped 'en'.
    assert it.source_domain == "br.investing.com"
    assert it.source_lang is None


def test_br_investing_is_not_in_the_international_set():
    assert "br.investing.com" not in INTERNATIONAL_RSS_DOMAINS
    assert "br.investing.com" in NO_RSS_DOMAINS
    assert "investing.com" in INTERNATIONAL_RSS_DOMAINS
    assert "www.investing.com" in INTERNATIONAL_RSS_DOMAINS


# ---------------------------------------------------------------------------
# The nine feeds this wave registered are reachable from BOTH registries, and
# the two feeds that were measured-and-rejected are absent.
# ---------------------------------------------------------------------------
WAVE_B_RSS_KEYS = [
    "www.investing.com",
    "www.france24.com",
    "www.lemonde.fr",
    "www.irishtimes.com",
    "www.cityam.com",
    "financialpost.com",
    "calgaryherald.com",
    "www.abc.net.au",
    "www.afr.com",
]


def test_every_wave_b_feed_key_is_registered_both_sides():
    for key in WAVE_B_RSS_KEYS:
        assert key in RSS_FEEDS, f"{key} missing from RSS_FEEDS"
        assert RSS_FEEDS[key], f"{key} registered with no feed URL"
        apex = key[4:] if key.startswith("www.") else key
        assert apex in INTERNATIONAL_RSS_DOMAINS, f"{apex} would be read as national"
        assert "www." + apex in INTERNATIONAL_RSS_DOMAINS, f"www.{apex} missing"


def test_measured_and_rejected_feeds_are_not_registered():
    # Each of these was fetched on the runner 2026-09-14 and lost on the
    # numbers (see the per-entry comments in sources.py): the dateless
    # Investing.com commodities feed (span="-", unpersistable), the France 24
    # business-tech feed (4 passes, 4 false positives), the Le Monde economy
    # feed (pass=1, a subset of une.xml) and the Calgary Herald site-wide feed
    # (10 fresh items, pass=0).
    registered = {u for urls in RSS_FEEDS.values() for u in urls}
    for url in (
        "https://www.investing.com/rss/commodities.rss",
        "https://www.france24.com/en/business-tech/rss",
        "https://www.lemonde.fr/en/economy/rss_full.xml",
        "https://calgaryherald.com/feed/",
    ):
        assert url not in registered, f"{url} was measured and rejected"
