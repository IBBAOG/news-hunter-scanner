"""Every registered international host must reach an extractor.

`enrich.py` gates the body fetch on `resolve_extractor_domain(host) is not
None`. A host that is registered in `sources.py` but absent from `EXTRACTORS`
therefore never has its article body parsed: the item keeps the RSS summary or,
on the Google News route, the meta description — a standfirst wearing the shape
of a body — and nothing is logged. That is the failure the 30 foreign-language
domains took until 2026-08-18, and the 112 Wave 5 outlets (2026-09-14) inherited
it verbatim. Measured on this checkout BEFORE the fix::

    resolve_extractor_domain('lngindustry.com') -> None
    resolve_extractor_domain('businessday.ng')  -> None
    resolve_extractor_domain('www.wsj.com')     -> None

The roster is derived from `sources.py`, so the next wave that registers a feed
and forgets the extractor turns this file red.

Run from repo root: python -m pytest tests/test_wave5_extractors.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter._clipinator_shim import (  # noqa: E402
    EXTRACTORS,
    SOURCE_NAMES,
    ex_auto,
    resolve_extractor_domain,
)
from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
)

# Registered hosts with no extractor, each for a stated reason.
#   themoscowtimes.com: its RSS carries full bodies, so no page fetch ever
#   reaches an extractor; registering one would advertise a path never taken
#   (see the comment in _clipinator_shim.py and
#   test_the_moscow_times_is_deliberately_extractorless).
#   cnbc.com: pre-existing gap, not Wave 5 — only "www.cnbc.com" is keyed, and
#   the `m.cnbc.com` fixture in tests/test_extractor_domain_resolution.py
#   asserts the resolver's third step by relying on that absence. Closing it
#   means editing that fixture, which is outside this pass's ownership.
_EXTRACTORLESS_ON_PURPOSE = {
    "themoscowtimes.com",
    "www.themoscowtimes.com",
    "cnbc.com",
}


def _registered_hosts() -> set[str]:
    hosts = set(INTERNATIONAL_RSS_DOMAINS)
    for entry in ENGLISH_NO_RSS_DOMAINS:
        host = entry.split("/")[0].lower()
        hosts.add(host)
        if host.startswith("www."):
            hosts.add(host.removeprefix("www."))
    return hosts


def _wave5_named_hosts() -> set[str]:
    """SOURCE_NAMES keys that belong to a host registered in sources.py.

    Keeps the assertion on the hosts the scanner can actually produce instead
    of on a hand-copied list that drifts from the registry.
    """
    registered = _registered_hosts()
    return {
        host
        for host in SOURCE_NAMES
        if host in registered or host.removeprefix("www.") in registered
    }


def test_the_roster_is_not_empty():
    assert len(_registered_hosts()) > 100
    assert len(_wave5_named_hosts()) > 100


@pytest.mark.parametrize(
    "host", sorted(_registered_hosts() - _EXTRACTORLESS_ON_PURPOSE)
)
def test_every_registered_international_host_reaches_an_extractor(host):
    key = resolve_extractor_domain(host)
    assert key is not None, (
        f"{host} is registered in sources.py but has no EXTRACTORS entry: its "
        "body fetch is skipped and the item keeps the meta description"
    )
    assert EXTRACTORS[key] is not None


@pytest.mark.parametrize(
    "host", sorted(_wave5_named_hosts() - _EXTRACTORLESS_ON_PURPOSE)
)
def test_every_named_international_host_reaches_an_extractor(host):
    assert resolve_extractor_domain(host) is not None


@pytest.mark.parametrize(
    "host,expected",
    [
        # The resolver's prefix walk is what lets the registry stay sane; these
        # pin that it actually covers the Wave 5 hosts, per prefix.
        ("www.lngindustry.com", "www.lngindustry.com"),  # keyed literally too
        ("m.lngindustry.com", "lngindustry.com"),
        ("amp.businessday.ng", "businessday.ng"),
        ("mobile.qcintel.com", "qcintel.com"),
    ],
)
def test_host_variants_of_a_wave5_outlet_resolve(host, expected):
    assert resolve_extractor_domain(host) == expected


def test_www3_needs_its_own_key_because_it_is_not_a_www_prefix():
    # "www3." is not in _HOST_PREFIXES, so NHK World only resolves because the
    # host is keyed literally. Deleting the key must break this test.
    assert resolve_extractor_domain("www3.nhk.or.jp") == "www3.nhk.or.jp"


def test_path_scoped_gnews_entries_resolve_by_their_host():
    # ENGLISH_NO_RSS_DOMAINS carries "marketwatch.com/story", "aa.com.tr/en" and
    # "tradearabia.com/news"; what reaches the enricher is the netloc.
    for host in ("marketwatch.com", "aa.com.tr", "tradearabia.com"):
        assert resolve_extractor_domain(host) is not None


def test_wave5_outlets_use_the_generic_extractor():
    """No bespoke extractor was invented for a page nobody fetched."""
    for host in (
        "apnews.com", "theglobeandmail.com", "businessday.ng",
        "e.vnexpress.net", "lngindustry.com", "www3.nhk.or.jp",
    ):
        assert EXTRACTORS[resolve_extractor_domain(host)] is ex_auto


def test_the_resolver_still_refuses_an_unregistered_host():
    # Two-sided: a resolver that answered for everything would apply ex_auto to
    # every unknown domain and make this whole file meaningless.
    assert resolve_extractor_domain("nao-existe-mesmo.example") is None
    assert resolve_extractor_domain("www.nao-existe-mesmo.example") is None


def test_named_and_extractable_stay_in_step():
    """Every Wave 5 name has an extractor key and vice versa.

    The file keys both forms of a host in both dicts even though the resolver
    would strip the www: that redundancy is what
    test_every_named_source_now_has_an_extractor_or_a_stated_reason compares,
    key by key. Asserting it here as well means a future wave that registers a
    name and skips the extractor fails on the wave's own test.
    """
    named = _wave5_named_hosts() - _EXTRACTORLESS_ON_PURPOSE
    missing = sorted(h for h in named if h not in EXTRACTORS)
    assert not missing, f"named but not extractable: {missing}"
