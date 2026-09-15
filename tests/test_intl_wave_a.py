"""Wave 5A floor — the 24 outlets registered on 2026-09-14 must stay registered.

Wave 5A (global wires & US mainstream) was measured on the runner and landed as
3 RSS outlets + 21 GNews-en domains, all inside the `Wave 5A` anchor blocks of
`sources.py`. Five waves merge into main in parallel, so the failure this file
exists to catch is a MERGE dropping one wave's block: nothing else would notice.
`test_international_rss_source_lang` guards only the 15 pre-expansion RSS
outlets, and `test_gnews_cohort` is deliberately size-agnostic.

Two of the entries are also easy to "tidy" into something that silently measures
a different funnel, so they are pinned by name:

  * marketwatch.com is registered PATH-SCOPED (`marketwatch.com/story`). The bare
    domain measures pass=75/7d, but ~45 of those are perennial quote / chart /
    SEC-filing pages ("BRNM36 | Brent Crude Jun 2036 Overview") — the TradingView
    pathology. The /story path measures 54, all real articles.
  * ABC News is registered as `abcnews.com`, NOT `abcnews.go.com`:
    site:abcnews.go.com returns 0 items (Google does not index the outlet under
    that host) and abcnews.com is also the host the outlet's own feed links to.

These are FLOOR assertions: later waves add entries, never remove these. No yield
is asserted here — the measured numbers live in the comments next to each entry.

Run from repo root: python -m pytest tests/test_intl_wave_a.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
    RSS_FEEDS,
    google_news_site_queries_en,
)

# The 21 GNews-en domains of Wave 5A, in registration order.
WAVE_5A_GNEWS = (
    "apnews.com",
    "wsj.com",
    "nytimes.com",
    "washingtonpost.com",
    "economist.com",
    "bbc.com",
    "thetimes.com",
    "telegraph.co.uk",
    "news.sky.com",
    "politico.com",
    "politico.eu",
    "axios.com",
    "forbes.com",
    "fortune.com",
    "businessinsider.com",
    "marketwatch.com/story",
    "barrons.com",
    "npr.org",
    "abcnews.com",
    "nbcnews.com",
    "foxbusiness.com",
)

# The 3 outlets Wave 5A kept on RSS: RSS_FEEDS key -> the feed URLs registered.
WAVE_5A_RSS = {
    "www.ft.com": (
        "https://www.ft.com/commodities?format=rss",
        "https://www.ft.com/companies/energy?format=rss",
    ),
    "www.theguardian.com": ("https://www.theguardian.com/business/oil/rss",),
    "www.cbsnews.com": ("https://www.cbsnews.com/latest/rss/world",),
}


def test_every_wave_5a_gnews_domain_is_still_registered():
    missing = [d for d in WAVE_5A_GNEWS if d not in ENGLISH_NO_RSS_DOMAINS]
    assert not missing, (
        f"dropped from ENGLISH_NO_RSS_DOMAINS: {missing} — these outlets would "
        "stop being queried with no error anywhere"
    )
    # NOT `len(set(WAVE_5A_GNEWS)) == 21`: that only measured this file's own
    # literal and stayed green while the registry emptied. Assert against the
    # live roster instead -- the wave's 21 domains must all still be in it.
    assert set(WAVE_5A_GNEWS) & set(ENGLISH_NO_RSS_DOMAINS) == set(WAVE_5A_GNEWS)
    assert len(set(WAVE_5A_GNEWS)) == len(WAVE_5A_GNEWS), "duplicate in the wave roster"


def test_wave_5a_rss_outlets_keep_their_feeds():
    for key, feeds in WAVE_5A_RSS.items():
        assert key in RSS_FEEDS, f"{key} dropped from RSS_FEEDS"
        for url in feeds:
            assert url in RSS_FEEDS[key], f"{url} dropped from RSS_FEEDS[{key}]"


def test_wave_5a_rss_outlets_are_tagged_international():
    """Both forms, or the outlet is silently classified as NATIONAL."""
    for key in WAVE_5A_RSS:
        apex = key[4:] if key.startswith("www.") else key
        assert apex in INTERNATIONAL_RSS_DOMAINS, apex
        assert f"www.{apex}" in INTERNATIONAL_RSS_DOMAINS, key


def test_wave_5a_rosters_do_not_overlap():
    """Each domain is tagged by exactly one mechanism (RSS path vs GNews route)."""
    for domain in WAVE_5A_GNEWS:
        assert domain not in INTERNATIONAL_RSS_DOMAINS, domain
    for key in WAVE_5A_RSS:
        apex = key[4:] if key.startswith("www.") else key
        assert apex not in ENGLISH_NO_RSS_DOMAINS, apex
        assert key not in ENGLISH_NO_RSS_DOMAINS, key


def test_marketwatch_stays_path_scoped():
    assert "marketwatch.com/story" in ENGLISH_NO_RSS_DOMAINS
    assert "marketwatch.com" not in ENGLISH_NO_RSS_DOMAINS, (
        "the bare domain fills its 100 GNews slots with perennial quote pages; "
        "keep the /story path scope"
    )
    url = google_news_site_queries_en(["marketwatch.com/story"], ["oil"], 24)[0]
    assert "site%3Amarketwatch.com%2Fstory" in url, url


def test_abc_news_stays_on_the_host_google_indexes():
    assert "abcnews.com" in ENGLISH_NO_RSS_DOMAINS
    assert "abcnews.go.com" not in ENGLISH_NO_RSS_DOMAINS, (
        "site:abcnews.go.com measured 0 items on 2026-09-14; the indexed host "
        "(and the one the outlet's own feed links to) is abcnews.com"
    )
