"""Every registered international host must render an outlet NAME, not a host.

Wave 5 (2026-09-14) registered 112 English-language outlets in `sources.py` and
nothing in `SOURCE_NAMES`, so `enrich.source_name_for` fell through to its last
line — `return domain` — and the dashboard showed `businessday.ng`,
`lngindustry.com`, `www3.nhk.or.jp`. Measured on this checkout BEFORE the fix::

    source_name_for('ft.com')        -> 'ft.com'
    source_name_for('businessday.ng')-> 'businessday.ng'
    source_name_for('www.wsj.com')   -> 'www.wsj.com'
    source_name_for('www3.nhk.or.jp')-> 'www3.nhk.or.jp'

The miss was invisible because a name-shaped string came back either way. That
is also what hid the second defect: `source_name_for` stripped www with
`lstrip("www.")`, which removes leading CHARACTERS from the set {w, .} rather
than the prefix — `www.wsj.com` became `sj.com`, `worldoil.com` became
`orldoil.com`, `www3.nhk.or.jp` became `3.nhk.or.jp` — and every one of those
misses returned the raw domain, i.e. the same symptom as an unregistered
outlet. `test_www_is_stripped_as_a_prefix_not_as_a_character_set` below fails on
the pre-fix function and passes on `removeprefix`.

The roster is derived from `sources.py`, never hard-coded here, so the next wave
that registers a feed and forgets the name turns this file red.

Run from repo root: python -m pytest tests/test_wave5_source_names.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import enrich  # noqa: E402
from news_hunter._clipinator_shim import SOURCE_NAMES  # noqa: E402
from news_hunter.enrich import source_name_for  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
)

# Hosts registered WITHOUT a display name, on purpose or as a stated debt.
#   cnbc.com: pre-existing gap, not Wave 5. Only "www.cnbc.com" is keyed, in
#   BOTH SOURCE_NAMES and EXTRACTORS, and the two dicts are compared key by key
#   by tests/test_extractor_domain_resolution.py — whose `m.cnbc.com` fixture
#   asserts the resolver reaches "www.cnbc.com" precisely because the apex is
#   absent. Adding the apex key therefore means editing that test too, which is
#   outside this pass's ownership; it is one line of follow-up work, recorded
#   here rather than silently skipped.
_KNOWN_NAME_GAPS = {"cnbc.com"}


def _registered_hosts() -> set[str]:
    """Every host the scanner can attribute an English-language item to.

    RSS entries arrive normalize_url'd (www stripped) and are registered apex +
    www in `INTERNATIONAL_RSS_DOMAINS`; a Google News entry may be path-scoped
    (`marketwatch.com/story`, `aa.com.tr/en`, `www3.nhk.or.jp/nhkworld`) and the
    resolved article netloc is the host part of it, with or without www.
    """
    hosts = set(INTERNATIONAL_RSS_DOMAINS)
    for entry in ENGLISH_NO_RSS_DOMAINS:
        host = entry.split("/")[0].lower()
        hosts.add(host)
        if host.startswith("www."):
            hosts.add(host.removeprefix("www."))
    return hosts


def test_the_roster_is_not_empty():
    # A derivation that silently collapsed to a handful of hosts would make
    # every assertion below vacuous. Lower bound only: the roster grows.
    assert len(_registered_hosts()) > 100


@pytest.mark.parametrize("host", sorted(_registered_hosts() - _KNOWN_NAME_GAPS))
def test_every_registered_international_host_has_a_display_name(host):
    name = source_name_for(host)
    assert name != host, (
        f"{host} renders as its bare domain: it is registered in sources.py but "
        "has no SOURCE_NAMES key"
    )
    # A name that merely decorates the host ("www.ft.com" -> "ft.com") would
    # pass the check above while still showing a domain to the reader.
    assert host.removeprefix("www.") != name


@pytest.mark.parametrize(
    "bare",
    sorted({h.removeprefix("www.") for h in _registered_hosts()} - _KNOWN_NAME_GAPS),
)
def test_the_www_form_of_every_registered_host_has_the_same_name(bare):
    """GNews does NOT normalize the netloc, so both forms reach the dashboard."""
    assert source_name_for(f"www.{bare}") == source_name_for(bare)


def test_www_is_stripped_as_a_prefix_not_as_a_character_set(monkeypatch):
    """The falsifying test for the lstrip fix.

    An apex-only registration whose host starts with a w: `lstrip("www.")` eats
    the fourth w as well ("www.wonky.example" -> "onky.example"), misses, and
    returns the raw host. `removeprefix` yields "wonky.example" and hits.
    """
    monkeypatch.setitem(SOURCE_NAMES, "wonky.example", "Wonky Gazette")
    assert source_name_for("www.wonky.example") == "Wonky Gazette"
    # ... and the non-www host is untouched by the strip.
    assert source_name_for("wonky.example") == "Wonky Gazette"


def test_the_charset_bug_was_not_papered_over_with_mangled_keys():
    """Guard against "fixing" the miss by registering what lstrip produced."""
    for mangled in ("sj.com", "3.nhk.or.jp", "orldoil.com", "ashingtonpost.com"):
        assert mangled not in SOURCE_NAMES


def test_www3_is_not_a_www_prefix():
    # "www3.nhk.or.jp".removeprefix("www.") is a no-op, so NHK World only
    # renders if the host is keyed literally. The old lstrip turned it into
    # "3.nhk.or.jp" instead.
    assert source_name_for("www3.nhk.or.jp") == "NHK World"
    assert "www3.nhk.or.jp" in SOURCE_NAMES


def test_the_two_hosts_named_in_the_regression_report():
    assert source_name_for("www.wsj.com") == "The Wall Street Journal"
    assert source_name_for("www3.nhk.or.jp") == "NHK World"


@pytest.mark.parametrize(
    "host,expected",
    [
        # One per wave, keyed by hand so the registry-derived tests above cannot
        # go green on names nobody read.
        ("apnews.com", "Associated Press"),                     # 5A
        ("marketwatch.com", "MarketWatch"),                     # 5A, path-scoped entry
        ("theglobeandmail.com", "The Globe and Mail"),          # 5B
        ("sodir.no", "Norwegian Offshore Directorate"),         # 5B
        ("businessday.ng", "BusinessDay Nigeria"),              # 5C
        ("aa.com.tr", "Anadolu Agency"),                        # 5C, path-scoped entry
        ("e.vnexpress.net", "VnExpress International"),         # 5D, subdomain-only
        ("hydrocarbonprocessing.com", "Hydrocarbon Processing"),  # 5D
        ("lngindustry.com", "LNG Industry"),                    # 5E
        ("mobilityplaza.com", "Mobility Plaza"),                # 5E, ex-PetrolPlaza
    ],
)
def test_wave5_display_names(host, expected):
    assert source_name_for(host) == expected


@pytest.mark.parametrize(
    "host,expected",
    [
        # Second live host of an outlet whose registered host is a different
        # one. businessday.co.za and trend.az were MEASURED in `news_articles`
        # (8 and 2 rows) while the registries carry businesslive.co.za and
        # en.trend.az; the rest are the outlet's other published host.
        ("businessday.co.za", "Business Day (South Africa)"),
        ("businesslive.co.za", "Business Day (South Africa)"),
        ("trend.az", "Trend News Agency"),
        ("en.trend.az", "Trend News Agency"),
        ("abcnews.go.com", "ABC News"),
        ("markets.businessinsider.com", "Business Insider"),
        ("bbc.co.uk", "BBC News"),
        ("mercopress.com", "MercoPress"),
    ],
)
def test_resolved_host_variants_render_the_outlet(host, expected):
    assert source_name_for(host) == expected


def test_business_day_south_africa_is_not_confused_with_businessday_nigeria():
    assert source_name_for("businesslive.co.za") != source_name_for("businessday.ng")


def test_an_unregistered_host_still_falls_back_to_itself():
    # Two-sided: a lookup that answered for everything would hide the next
    # missing registration instead of surfacing it.
    assert source_name_for("nao-existe-mesmo.example") == "nao-existe-mesmo.example"
    assert source_name_for("www.nao-existe-mesmo.example") == "www.nao-existe-mesmo.example"


def test_source_name_for_is_the_function_the_pipeline_calls():
    # pipeline.py imports the symbol directly; a rename would leave these tests
    # passing against a function nobody calls.
    from news_hunter import pipeline

    assert pipeline.source_name_for is enrich.source_name_for
