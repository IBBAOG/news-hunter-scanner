"""Wave 5D (2026-09-14) international roster — the registrations must survive.

What this pins, and why each assertion exists:

* An RSS outlet needs TWO registrations to work: the feed in `RSS_FEEDS` and its
  apex+www forms in `INTERNATIONAL_RSS_DOMAINS`. Miss the second and the outlet
  keeps being fetched while `source_lang` stays None — i.e. it silently becomes
  NATIONAL in the dashboard's selector, with no error anywhere.
* A GNews-only outlet lives in `ENGLISH_NO_RSS_DOMAINS` and must NOT also be in
  the RSS roster (each domain is tagged by exactly one mechanism).
* The five domains this wave REJECTED after measuring both surfaces must stay
  unregistered: re-adding one without a new measurement re-imports a domain that
  was proven to yield nothing (Petroleum Economist's seven identical
  "Petroleum Economist" titles) or nothing at all (Stabroek News, 0 items on
  every surface).

The numbers behind every line here are in the per-domain comments in
`sources.py` (the authoritative record); this file only pins the wiring.

Run from repo root: python -m pytest tests/test_intl_wave_d.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import _entry_to_item  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
    RSS_FEEDS,
)

# registry key -> the feed URL(s) measured and registered on 2026-09-14
WAVE_5D_RSS: dict[str, tuple[str, ...]] = {
    "www.straitstimes.com": ("https://www.straitstimes.com/news/business/rss.xml",),
    "www.channelnewsasia.com": (
        "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml",
        "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6936",
    ),
    "www.bangkokpost.com": ("https://www.bangkokpost.com/rss/data/business.xml",),
    "e.vnexpress.net": ("https://e.vnexpress.net/rss/business.rss",),
    "timesofindia.indiatimes.com": (
        "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
    ),
    "economictimes.indiatimes.com": (
        "https://economictimes.indiatimes.com/industry/energy/oil-gas/rssfeeds/13352306.cms",
    ),
    "www.business-standard.com": (
        "https://www.business-standard.com/rss/markets-106.rss",
    ),
    "www.dawn.com": ("https://www.dawn.com/feeds/business",),
    "astanatimes.com": ("https://astanatimes.com/feed/",),
    "en.trend.az": ("https://en.trend.az/feeds/index.rss",),
    "www.batimes.com.ar": ("https://www.batimes.com.ar/feed",),
    "mexiconewsdaily.com": ("https://mexiconewsdaily.com/feed/",),
}

# Outlets whose articles live on a subdomain: there is no www form to pair, the
# same shape as ET EnergyWorld in the 2026-08-18 waves.
WAVE_5D_SUBDOMAIN_ONLY = frozenset({
    "e.vnexpress.net",
    "timesofindia.indiatimes.com",
    "economictimes.indiatimes.com",
    "en.trend.az",
})

WAVE_5D_GNEWS = (
    # MercoPress was registered on RSS by the wave and MOVED here on
    # 2026-09-14: its feed's only pass over 48h was a false positive (an RAF
    # air-to-air refuelling "tanker"), while GNews measured pass=5/7d all
    # on-beat. Pinned in the GNews roster so the feed does not come back.
    "en.mercopress.com",
    "theedgemalaysia.com",
    "bnamericas.com",
    "thejakartapost.com",
    "kaieteurnewsonline.com",
    "hydrocarbonprocessing.com",
    "hydrocarbonengineering.com",
)

# Measured on the runner 2026-09-14 and refused; the numbers are in the
# commented-out REJECTED entries inside the Wave 5D ENGLISH_NO_RSS_DOMAINS block.
WAVE_5D_REJECTED = (
    "caixinglobal.com",          # GNews items=7 pass=0 (re-test of the 2026-08-18 reject)
    "oilnow.gy",                 # feed 403 every path; GNews 1 item, pass=0
    "stabroeknews.com",          # feed 404 every path; GNews 0 items (apex and www)
    "petroleum-economist.com",   # feed 404; GNews 7 items, every title "Petroleum Economist"
    "oilfieldtechnology.com",    # feed 403/404; GNews 2 items, pass=0
)


def _strip_www(d: str) -> str:
    return d[4:] if d.startswith("www.") else d


def test_every_wave_5d_feed_is_registered_with_the_measured_url():
    for key, urls in WAVE_5D_RSS.items():
        assert key in RSS_FEEDS, f"{key} lost from RSS_FEEDS"
        for url in urls:
            assert url in RSS_FEEDS[key], f"{key}: {url} lost from RSS_FEEDS"


def test_every_wave_5d_rss_outlet_is_tagged_international():
    """Both forms, or the outlet silently reverts to NATIONAL."""
    for key in WAVE_5D_RSS:
        apex = _strip_www(key)
        assert apex in INTERNATIONAL_RSS_DOMAINS, f"{apex} missing (would be national)"
        if apex in WAVE_5D_SUBDOMAIN_ONLY:
            continue
        assert f"www.{apex}" in INTERNATIONAL_RSS_DOMAINS, f"www.{apex} missing"


def test_wave_5d_gnews_outlets_are_registered_once():
    for domain in WAVE_5D_GNEWS:
        assert domain in ENGLISH_NO_RSS_DOMAINS, f"{domain} lost from ENGLISH_NO_RSS_DOMAINS"
        assert domain not in INTERNATIONAL_RSS_DOMAINS
        assert domain not in RSS_FEEDS and f"www.{domain}" not in RSS_FEEDS


def test_wave_5d_rejects_stay_unregistered():
    registered = (
        {_strip_www(k) for k in RSS_FEEDS}
        | {_strip_www(d) for d in INTERNATIONAL_RSS_DOMAINS}
        | {_strip_www(d) for d in ENGLISH_NO_RSS_DOMAINS}
    )
    for domain in WAVE_5D_REJECTED:
        assert domain not in registered, (
            f"{domain} was measured on BOTH surfaces on 2026-09-14 and refused "
            "— re-register it only with a fresh measurement in its comment"
        )


def test_the_two_economic_times_surfaces_do_not_collide():
    """ET EnergyWorld (2026-08-18) and the ET oil & gas section feed (5D).

    Different hosts, different desks. The apex registration must not have
    displaced the subdomain one, and both must be tagged international.
    """
    assert "energy.economictimes.indiatimes.com" in RSS_FEEDS
    assert "economictimes.indiatimes.com" in RSS_FEEDS
    assert RSS_FEEDS["energy.economictimes.indiatimes.com"] != RSS_FEEDS[
        "economictimes.indiatimes.com"
    ]
    for host in ("energy.economictimes.indiatimes.com", "economictimes.indiatimes.com"):
        assert host in INTERNATIONAL_RSS_DOMAINS


def test_a_www_wave_5d_item_resolves_to_the_apex_and_is_tagged_en():
    it = _entry_to_item(
        {"link": "https://www.straitstimes.com/business/economy/oil-story",
         "title": "Oil prices up over 3%", "summary": "body"},
        "www.straitstimes.com",
    )
    assert it.source_domain == "straitstimes.com"
    assert it.source_lang == "en"


def test_a_subdomain_wave_5d_item_keeps_its_host_and_is_tagged_en():
    it = _entry_to_item(
        {"link": "https://e.vnexpress.net/news/business/gasoline-prices-increase",
         "title": "Gasoline prices increase", "summary": "body"},
        "e.vnexpress.net",
    )
    assert it.source_domain == "e.vnexpress.net"
    assert it.source_lang == "en"


def test_the_vietnamese_apex_is_not_registered():
    """Only the English edition (e.vnexpress.net) is on the roster.

    vnexpress.net is the Vietnamese site: registering it would pull untranslated
    `vi` copy through a path that stamps source_lang='en'.
    """
    assert "vnexpress.net" not in INTERNATIONAL_RSS_DOMAINS
    assert "vnexpress.net" not in RSS_FEEDS and "www.vnexpress.net" not in RSS_FEEDS
