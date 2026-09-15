"""Wave 5C (2026-09-14) — Africa, Middle East & North-East Asia registrations.

These tests pin the three things a later edit can break silently:

  * an RSS outlet whose apex/www forms are missing from
    INTERNATIONAL_RSS_DOMAINS is fetched but stamped NATIONAL (source_lang
    None), i.e. it disappears from the dashboard's international selector;
  * a domain registered on BOTH surfaces would be fetched twice and land the
    same story from two routes;
  * the three PATH forms (`aa.com.tr/en`, `tradearabia.com/news`,
    `www3.nhk.or.jp/nhkworld`) are what was MEASURED. Stripping the path is
    not cosmetic: the apex of aa.com.tr also serves Turkish/Arabic, the apex
    of tradearabia.com returns perennial section landing pages instead of
    articles (10 passes, half of them evergreen URLs, vs 13 real ones on
    /news), and www3.nhk.or.jp without /nhkworld is the Japanese-language NHK.

Run from repo root: python -m pytest tests/test_intl_wave_c.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
    LANGUAGES,
    RSS_FEEDS,
    feed_timeout,
)

# host key in RSS_FEEDS -> apex used by normalize_url / SOURCE_NAMES
WAVE_C_RSS = {
    "www.businesslive.co.za": "businesslive.co.za",
    "www.pipelineoilandgasnews.com": "pipelineoilandgasnews.com",
    "www.middleeasteye.net": "middleeasteye.net",
    "www.tehrantimes.com": "tehrantimes.com",
    "www.dailysabah.com": "dailysabah.com",
    "www.koreaherald.com": "koreaherald.com",
}

WAVE_C_GNEWS = [
    "allafrica.com",
    "businessday.ng",
    "punchng.com",
    "thisdaylive.com",
    "theeastafrican.co.ke",
    "africaoilgasreport.com",
    "energycapitalpower.com",
    "english.alarabiya.net",
    "timesofisrael.com",
    "haaretz.com",
    "aa.com.tr/en",
    "english.ahram.org.eg",
    "oilandgasmiddleeast.com",
    "agbi.com",
    "tradearabia.com/news",
    "asia.nikkei.com",
    "japantimes.co.jp",
    "www3.nhk.or.jp/nhkworld",
]

WAVE_C_PATH_FORMS = ("aa.com.tr/en", "tradearabia.com/news", "www3.nhk.or.jp/nhkworld")


def test_rss_outlets_registered_with_a_feed():
    for host, feeds in ((h, RSS_FEEDS.get(h)) for h in WAVE_C_RSS):
        assert feeds, f"{host} missing from RSS_FEEDS"
        for url in feeds:
            assert url.startswith("https://"), (host, url)
            assert host in url, (host, url)


def test_rss_outlets_are_classified_international():
    """Both the apex and the www form, or the outlet reads as national."""
    for host, apex in WAVE_C_RSS.items():
        assert apex in INTERNATIONAL_RSS_DOMAINS, f"{apex} (apex) not international"
        assert host in INTERNATIONAL_RSS_DOMAINS, f"{host} (www) not international"


def test_gnews_outlets_registered():
    for domain in WAVE_C_GNEWS:
        assert domain in ENGLISH_NO_RSS_DOMAINS, f"{domain} missing from ENGLISH_NO_RSS_DOMAINS"


def test_no_domain_has_two_surfaces():
    rss_hosts = set(WAVE_C_RSS) | set(WAVE_C_RSS.values())
    for domain in WAVE_C_GNEWS:
        apex = domain.split("/", 1)[0]
        assert apex not in rss_hosts, f"{domain} registered on both surfaces"
        assert apex not in RSS_FEEDS, f"{domain} also has an RSS_FEEDS entry"
        assert f"www.{apex}" not in RSS_FEEDS, f"{domain} also has a www RSS_FEEDS entry"


def test_english_no_rss_domains_has_no_duplicates():
    assert len(ENGLISH_NO_RSS_DOMAINS) == len(set(ENGLISH_NO_RSS_DOMAINS))


def test_path_forms_are_preserved():
    for domain in WAVE_C_PATH_FORMS:
        assert domain in ENGLISH_NO_RSS_DOMAINS, f"{domain} lost its path"
        apex = domain.split("/", 1)[0]
        assert apex not in ENGLISH_NO_RSS_DOMAINS, (
            f"{apex} registered alongside {domain}: the bare apex measures a "
            "different funnel (Turkish/Arabic copy, perennial landing pages, "
            "or the Japanese-language NHK)"
        )


def test_alarabiya_english_and_arabic_stay_separate():
    """english.alarabiya.net is an English surface; alarabiya.net is the ar route."""
    assert "english.alarabiya.net" in ENGLISH_NO_RSS_DOMAINS
    assert "alarabiya.net" not in ENGLISH_NO_RSS_DOMAINS
    assert "alarabiya.net" in LANGUAGES["ar"].no_rss_domains
    assert "english.alarabiya.net" not in LANGUAGES["ar"].no_rss_domains


def test_wave_c_feeds_run_on_the_default_budget():
    """Measured 2026-09-14: the slowest Wave 5C feed was 1.74s against a 4s
    default, so none of them was given an override.

    Inverted on 2026-09-14 from `host not in FEED_TIMEOUT_OVERRIDES`: that form
    asserted the shape of a dict instead of the behaviour, and it forbade the
    only healthy reaction to a feed that starts timing out -- measuring it and
    giving it a budget. What this pins is that no Wave 5C feed is being GIVEN
    EXTRA TIME it never needed, since an inflated budget is spent out of the
    22s COLLECT_DEADLINE ~65 feeds share. A wave that measures one of these
    slower is expected to change this test, with the numbers.
    """
    default = 4.0
    for host, apex in WAVE_C_RSS.items():
        assert feed_timeout(host, default) == default, host
        assert feed_timeout(apex, default) == default, apex
