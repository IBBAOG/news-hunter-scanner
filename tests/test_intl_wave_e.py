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
from news_hunter.sources import (
    ENGLISH_NO_RSS_DOMAINS,
    FEED_TIMEOUT_OVERRIDES,
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


def test_globalenergynetwork_override_is_registered_for_the_pending_promotion():
    """Recorded by this wave; the GNews entry stays until the CTO swaps it.

    The override is inert while the domain is GNews-only (feed_timeout is only
    consulted on the feed path), so this pins the number, not a behaviour.
    """
    assert FEED_TIMEOUT_OVERRIDES["globalenergynetwork.net"] == 14.0
    assert feed_timeout("www.globalenergynetwork.net", 4.0) == 14.0
    assert "globalenergynetwork.net" in ENGLISH_NO_RSS_DOMAINS
    # intellinews re-measured at 0.48s on 2026-09-14: an override would be a
    # no-op, so the wave deliberately did NOT add one.
    assert "intellinews.com" not in FEED_TIMEOUT_OVERRIDES
