"""Riviera Maritime Media hides its body behind a proprietary Affino class.

Measured on the runner on 2026-09-15 against the live article
www.rivieramm.com/news-content-hub/vlccs-and-suezmaxes-sold-like-hot-cakes-90002
(HTTP 200, 108 KB, anonymous fetch):

    div.aos-DS27-WYSEdit          -> 1 match, 22 <p>   <- the body
    <article>                     -> 0 elements        <- _generic's implicit
                                                          fallback finds nothing
    div[itemprop="articleBody"]   -> 0 elements
    ex_auto                       -> 0 paragraphs, title read from meta

This is the silent shape: ex_auto does not raise, it returns nothing, and enrich
falls through to the meta description. Every Riviera row in news_articles had an
empty snippet on 2026-09-15 (8 of 8) - compounded by the apex url being
unfetchable at all (see tests/test_www_only_hosts.py).

Fixture, not network: this asserts the shape of the extractor. A real layout
change is caught by measure_source / the no_body counter, not here.

Run from repo root: python -m pytest tests/test_rivieramm_extractor.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter._clipinator_shim import (  # noqa: E402
    EXTRACTORS,
    _extract,
    ex_auto,
    ex_rivieramm,
    resolve_extractor_domain,
)

#: The dashboard's TS clipping registry carries this exact string. Parity is the
#: point: two registries drifting is how one side extracts and the other does not.
RIVIERA_SELECTOR = "div.aos-DS27-WYSEdit"

# The page shape, reduced: an Affino body div, plus the two decoys that make
# ex_auto "succeed" with nothing - a promo <div class="content"> and a list of
# related-story cards. The live page has no <article> at all.
RIVIERA_HTML = """
<html><head>
  <meta property="og:title" content="VLCC and Suezmax sales figures soar">
  <meta name="description" content="Tanker asset values almost doubled within a year">
</head><body>
  <div class="content"><p>Subscribe to Riviera Maritime Media newsletters</p></div>
  <div class="aos-DS27-WYSEdit">
    <p>Sales of VLCCs and Suezmaxes have surged as tanker asset values climb to
       levels not seen since 2008, brokers said this week.</p>
    <p>One five-year-old VLCC changed hands at almost double the price paid for a
       comparable vessel 12 months ago.</p>
    <p>Owners are taking profits while charter rates for dirty tankers remain at
       historic highs.</p>
  </div>
  <div class="related"><a href="/news-content-hub/other-90003">Other story</a></div>
</body></html>
"""


def test_the_body_comes_out_of_the_affino_container():
    title, paragraphs = _extract(RIVIERA_HTML, "www.rivieramm.com")
    assert title == "VLCC and Suezmax sales figures soar"
    assert len(paragraphs) == 3
    assert paragraphs[0].startswith("Sales of VLCCs and Suezmaxes have surged")
    # The newsletter promo in div.content is NOT the body.
    assert not any("Subscribe to Riviera" in p for p in paragraphs)


def test_the_apex_key_extracts_too():
    """Both spellings are keyed; the apex one is what a legacy row carries."""
    _title, paragraphs = _extract(RIVIERA_HTML, "rivieramm.com")
    assert len(paragraphs) == 3


def test_ex_auto_never_reaches_the_body_on_the_same_page():
    """The reason this extractor has to exist.

    On the LIVE page ex_auto returned zero paragraphs. On this fixture it
    returns the newsletter promo instead, because `div.content` is one of its
    selectors and Affino uses that class for site furniture. Either way the BODY
    never comes out, and enrich cannot tell the two outcomes apart from a real
    extraction - which is what made the miss silent for the whole wave.
    """
    from bs4 import BeautifulSoup

    _title, paragraphs = ex_auto(BeautifulSoup(RIVIERA_HTML, "lxml"))
    assert not any("VLCC" in p for p in paragraphs)
    assert paragraphs == ["Subscribe to Riviera Maritime Media newsletters"]


def test_the_selector_string_is_the_one_the_dashboard_uses():
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(RIVIERA_HTML, "lxml")
    assert len(soup.select(RIVIERA_SELECTOR)) == 1
    # Case-sensitive on purpose: Affino's widget id, not a lowercase class.
    assert soup.select("div.aos-ds27-wysedit") == []


@pytest.mark.parametrize("host", ["rivieramm.com", "www.rivieramm.com", "m.rivieramm.com"])
def test_every_riviera_host_resolves_to_the_bespoke_extractor(host):
    key = resolve_extractor_domain(host)
    assert key is not None
    assert EXTRACTORS[key] is ex_rivieramm
    assert EXTRACTORS[key] is not ex_auto
