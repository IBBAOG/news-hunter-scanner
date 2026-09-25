"""Wave 5E (2026-09-14) registrations — O&G / refining / shipping trade press.

Cohort-aware, like the rest of the roster tests: these assert what THIS wave
registered, never the size or exact contents of a shared registry (five parallel
branches touch those literals, so an equality assertion would be red between
merges). What they catch is the failure this programme keeps producing silently:

  * an RSS outlet that loses its INTERNATIONAL_RSS_DOMAINS pair and quietly
    reverts to being classified as NATIONAL;
  * the eia.gov timeout override keyed so that it never bites — the RSS_FEEDS
    key is "www.eia.gov" and the override key is "eia.gov", and it is ONLY the
    www-insensitive lookup in feed_timeout() that connects them. Key it wrong
    (or drop it) and the feed silently returns 0 items again, exactly as it did
    on 2026-08-18. This asserts the resolved seconds, not the dict contents.
"""
from urllib.parse import urlparse

from news_hunter.sources import (
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
    RSS_FEEDS,
    feed_timeout,
)

# apex -> the RSS_FEEDS key that carries the feed (www form where the site uses one)
WAVE_5E_RSS = {
    "lngindustry.com": "www.lngindustry.com",
    "worldpipelines.com": "www.worldpipelines.com",
    "tanksterminals.com": "www.tanksterminals.com",
    "drillingcontractor.org": "drillingcontractor.org",
    "rbnenergy.com": "rbnenergy.com",
    "fuelsandlubes.com": "www.fuelsandlubes.com",
    "energymonitor.ai": "www.energymonitor.ai",
    "shipandbunker.com": "www.shipandbunker.com",
    "seatrade-maritime.com": "www.seatrade-maritime.com",
    "kpler.com": "www.kpler.com",
    "eia.gov": "www.eia.gov",
}

WAVE_5E_GNEWS = (
    "offshore-mag.com",
    "gasprocessingnews.com",
    "qcintel.com",
    "lloydslist.com",
    "rivieramm.com",
    "mobilityplaza.com",
    "iea.org",
    "opec.org",
    "woodmac.com",
    "rystadenergy.com",
    "vortexa.com",
    "oxfordenergy.org",
    "icis.com",
    "jpt.spe.org",
)

# Measured 0 items on every surface on 2026-09-14 (or, for petrolplaza.com, a
# retired domain): they must stay OUT, so a later wave does not resurrect them
# from the README directory.
WAVE_5E_REJECTED = ("naturalgasworld.com", "opisnet.com", "petrolplaza.com")


def test_every_rss_registration_has_its_feed():
    for apex, key in WAVE_5E_RSS.items():
        assert key in RSS_FEEDS, f"{apex}: RSS_FEEDS entry missing"
        assert RSS_FEEDS[key], f"{apex}: registered with no feed url"


def test_every_rss_outlet_is_tagged_international_in_both_forms():
    for apex in WAVE_5E_RSS:
        assert apex in INTERNATIONAL_RSS_DOMAINS, (
            f"{apex} dropped from INTERNATIONAL_RSS_DOMAINS — its feed would keep "
            "being fetched and every item would be classified as NATIONAL"
        )
        assert f"www.{apex}" in INTERNATIONAL_RSS_DOMAINS, f"www.{apex} missing"


def test_gnews_registrations_are_present_and_not_double_tagged():
    for domain in WAVE_5E_GNEWS:
        assert domain in ENGLISH_NO_RSS_DOMAINS, f"{domain}: GNews entry missing"
        assert domain not in INTERNATIONAL_RSS_DOMAINS, f"{domain} tagged twice"


def test_rejected_candidates_stay_out_of_every_registry():
    for domain in WAVE_5E_REJECTED:
        assert domain not in ENGLISH_NO_RSS_DOMAINS, domain
        assert domain not in INTERNATIONAL_RSS_DOMAINS, domain
        assert domain not in RSS_FEEDS and f"www.{domain}" not in RSS_FEEDS, domain


def test_eia_feed_gets_its_measured_budget_through_the_www_key():
    """The whole point of the override: 14s, not the 4s default, for eia.gov.

    Measured fetch=10.14s on the runner (2026-09-14). The registry key is the
    www form and the override key is the apex, so this asserts the RESOLVED
    value for both spellings — the connection is feed_timeout()'s
    www-insensitive lookup, and nothing else in the suite proves it for a key
    that is actually registered.
    """
    default = 4.0
    assert feed_timeout("www.eia.gov", default) == 14.0
    assert feed_timeout("eia.gov", default) == 14.0
    assert feed_timeout("www.eia.gov", default) > default
    # And it is scoped: a neighbouring Wave 5E feed keeps the default budget.
    assert feed_timeout("www.lngindustry.com", default) == default


def test_globalenergynetwork_feed_gets_more_than_the_default_budget():
    """The 1000-entry (~6.8 MB) archive feed must get MORE than the 4s default.

    Deliberately NOT an equality pin: 14.0 is a measurement (fetch=10.65s plus
    headroom) and re-measuring it is the healthy thing to do, so a test that
    forbids the number from moving is a test that punishes the right behaviour.
    What must never regress is the relation — and that it resolves on BOTH
    spellings, since the registry key is the apex and the wave that recorded the
    override wrote it as the apex too.
    """
    default = 4.0
    assert feed_timeout("www.globalenergynetwork.net", default) > default
    assert feed_timeout("globalenergynetwork.net", default) > default


def test_the_two_2026_08_18_timeout_casualties_are_now_rss_only():
    """intellinews and globalenergynetwork were promoted GNews -> RSS 2026-09-14.

    Each was lost to the 4s FEED_TIMEOUT on 2026-08-18 and re-measured by this
    wave (0.53s and 10.65s). Registered on RSS they carry BODIES, which the
    GNews route never did, and they give two `site:` queries back to the EN
    burst budget — but only if the GNews line is really gone: a domain on both
    surfaces is fetched twice and lands the same story from two routes.
    """
    for apex, feed_key in (
        ("intellinews.com", "www.intellinews.com"),
        ("globalenergynetwork.net", "globalenergynetwork.net"),
    ):
        assert feed_key in RSS_FEEDS, f"{apex}: RSS_FEEDS entry missing"
        assert RSS_FEEDS[feed_key], f"{apex}: registered with no feed url"
        assert apex in INTERNATIONAL_RSS_DOMAINS, f"{apex} would be NATIONAL"
        assert f"www.{apex}" in INTERNATIONAL_RSS_DOMAINS, f"www.{apex} missing"
        assert apex not in ENGLISH_NO_RSS_DOMAINS, f"{apex} still double-registered"


def test_intellinews_needs_no_timeout_override():
    """Re-measured at 0.53s: the default budget is ~8x what it needs.

    Stated as the BEHAVIOUR (the feed resolves to the default) rather than as
    `"intellinews.com" not in FEED_TIMEOUT_OVERRIDES`: what would hurt is an
    inflated budget on a fast host eating the shared 22s COLLECT_DEADLINE, not
    the presence of a key.
    """
    default = 4.0
    assert feed_timeout("www.intellinews.com", default) == default
    assert feed_timeout("intellinews.com", default) == default


def test_every_wave_5e_feed_url_is_served_by_its_own_host():
    """A feed URL whose netloc is not the registered host is a registration bug.

    Checked on the NETLOC, not with `apex in url`: the substring form passes for
    a URL that merely mentions the domain in a path or query (the three
    Palladian titles — lngindustry, worldpipelines, tanksterminals — all serve
    /rss/<name>.xml, so the apex appears twice in each URL and a substring
    assertion proves nothing about the host).
    """
    for apex, key in WAVE_5E_RSS.items():
        for url in RSS_FEEDS[key]:
            netloc = urlparse(url).netloc.lower()
            assert netloc == apex or netloc.endswith("." + apex), (
                f"{key}: {url} is served by {netloc!r}, not by {apex}"
            )
