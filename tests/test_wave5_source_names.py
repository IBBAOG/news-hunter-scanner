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
the pre-fix function and passes on the prefix walk.

Third defect, same shape: the name lookup only ever considered `www.`, while
`resolve_extractor_domain` had walked `_HOST_PREFIXES` ("www.", "m.", "amp.",
"mobile.") since 2026-08-18. A mobile or AMP host therefore got an extractor and
no name — `m.lngindustry.com` rendered as itself — which is how `m.yicai.com`
went a whole wave with a body and a bare domain for a byline. Both lookups now
walk the same tuple, and `test_every_host_prefix_resolves_to_the_outlet` is
parametrized over that tuple so a fifth prefix is covered the day it is added.

The roster is derived from `sources.py`, never hard-coded here, so the next wave
that registers a feed and forgets the name turns this file red.

Run from repo root: python -m pytest tests/test_wave5_source_names.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import registered_hosts  # noqa: E402
from news_hunter import enrich  # noqa: E402
from news_hunter._clipinator_shim import (  # noqa: E402
    _HOST_PREFIXES,
    SOURCE_NAMES,
    resolve_extractor_domain,
)
from news_hunter.enrich import source_name_for  # noqa: E402

# Hosts registered WITHOUT a display name, on purpose or as a stated debt.
#   cnbc.com: pre-existing gap, not Wave 5. Only "www.cnbc.com" is keyed, in
#   BOTH SOURCE_NAMES and EXTRACTORS, and the two dicts are compared key by key
#   by tests/test_extractor_domain_resolution.py — whose `m.cnbc.com` fixture
#   asserts the resolver reaches "www.cnbc.com" precisely because the apex is
#   absent. Adding the apex key therefore means editing that test too, which is
#   outside this pass's ownership; it is one line of follow-up work, recorded
#   here rather than silently skipped.
_KNOWN_NAME_GAPS = {"cnbc.com"}


def test_the_roster_is_not_empty():
    # A derivation that silently collapsed to a handful of hosts would make
    # every assertion below vacuous. Lower bound only: the roster grows.
    assert len(registered_hosts()) > 100


@pytest.mark.parametrize("host", sorted(registered_hosts() - _KNOWN_NAME_GAPS))
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
    sorted({h.removeprefix("www.") for h in registered_hosts()} - _KNOWN_NAME_GAPS),
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


@pytest.mark.parametrize("prefix", _HOST_PREFIXES)
def test_every_host_prefix_resolves_to_the_outlet(prefix):
    """One case per prefix, off the tuple itself, so a fifth one is covered."""
    host = f"{prefix}lngindustry.com"
    assert source_name_for(host) == "LNG Industry"
    # The name lookup and the extractor lookup must agree on what a host
    # variant means; they disagreed for a whole wave.
    assert resolve_extractor_domain(host) is not None


@pytest.mark.parametrize("prefix", _HOST_PREFIXES)
def test_no_prefix_invents_a_name_for_an_unregistered_host(prefix):
    """The negative half: the walk must not answer for everything."""
    host = f"{prefix}nao-existe-mesmo.example"
    assert source_name_for(host) == host


def test_a_www_only_registration_is_reachable_from_a_mobile_host():
    # Third step of the walk: strip the prefix, then re-add "www.". cnbc.com is
    # keyed www.-only (see _KNOWN_NAME_GAPS), which makes it the one live case.
    assert source_name_for("m.cnbc.com") == "CNBC"
    assert source_name_for("amp.cnbc.com") == "CNBC"


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
