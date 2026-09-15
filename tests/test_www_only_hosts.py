"""The five hosts that only answer on `www.` must keep the prefix in the key.

`normalize_url` strips a leading "www." from every stored url so one article
reached through two spellings is one row. For five Wave 5 hosts that rule
produces a url NOBODY CAN FETCH. Measured from the runner on 2026-09-15 with
`_clipinator_shim.fetch_html`::

    https://lngindustry.com/               -> SSLError        https://www.… 200
    https://worldpipelines.com/            -> SSLError        https://www.… 200
    https://tanksterminals.com/            -> SSLError        https://www.… 200
    https://hydrocarbonengineering.com/    -> SSLError        https://www.… 200
    https://rivieramm.com/                 -> ConnectTimeout  https://www.… 200

The four SSL failures are one www-only vhost (Palladian Publications) whose
certificate does not carry the apex name; Riviera's apex answers nothing on 443.

The consequence is not cosmetic: the stored url is what the dashboard's clipping
generator GETs, and what the scanner's own lede rescue and snippet backfill hand
to fetch_html. All 8 rivieramm rows in news_articles had an empty snippet on
2026-09-15 for exactly this reason.

Run from repo root: python -m pytest tests/test_www_only_hosts.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import enrich as enrich_mod  # noqa: E402
from news_hunter._clipinator_shim import (  # noqa: E402
    SOURCE_NAMES,
    resolve_extractor_domain,
)
from news_hunter.enrich import source_name_for  # noqa: E402
from news_hunter.fetcher import RawItem, _entry_to_item  # noqa: E402
from news_hunter.sources import INTERNATIONAL_RSS_DOMAINS  # noqa: E402
from news_hunter.store import WWW_ONLY_HOSTS, normalize_url  # noqa: E402

_ARTICLE_PATHS = {
    "lngindustry.com": "/floating-lng/14092026/kanata-clean-power-pre-feed/",
    "worldpipelines.com": "/contracts-and-tenders/14092026/baker-hughes-venture-global/",
    "tanksterminals.com": "/terminals/14092026/wood-secures-exxonmobil-png/",
    "hydrocarbonengineering.com": "/refining/12092026/valero-benicia-shutdown/",
    "rivieramm.com": "/news-content-hub/vlccs-and-suezmaxes-sold-like-hot-cakes-90002",
}


def test_the_roster_is_the_five_measured_hosts():
    assert WWW_ONLY_HOSTS == frozenset(_ARTICLE_PATHS)


@pytest.mark.parametrize("host", sorted(_ARTICLE_PATHS))
def test_an_apex_url_is_lifted_to_www(host):
    path = _ARTICLE_PATHS[host]
    assert normalize_url(f"https://{host}{path}") == f"https://www.{host}{path.rstrip('/')}"


@pytest.mark.parametrize("host", sorted(_ARTICLE_PATHS))
def test_a_www_url_keeps_its_prefix(host):
    path = _ARTICLE_PATHS[host]
    assert normalize_url(f"https://www.{host}{path}") == f"https://www.{host}{path.rstrip('/')}"


@pytest.mark.parametrize("host", sorted(_ARTICLE_PATHS))
def test_both_spellings_collapse_onto_one_key(host):
    path = _ARTICLE_PATHS[host]
    assert normalize_url(f"https://{host}{path}") == normalize_url(f"https://www.{host}{path}")


@pytest.mark.parametrize("host", sorted(_ARTICLE_PATHS))
def test_normalisation_is_idempotent(host):
    once = normalize_url(f"https://{host}{_ARTICLE_PATHS[host]}")
    assert normalize_url(once) == once


def test_every_other_host_still_loses_its_www():
    """Two-sided: a rule that kept www everywhere would split every outlet."""
    assert normalize_url("https://www.oilprice.com/x/story.html") == "https://oilprice.com/x/story.html"
    assert normalize_url("https://www.hellenicshippingnews.com/some-lng-story/") == \
        "https://hellenicshippingnews.com/some-lng-story"
    assert normalize_url("https://www.reuters.com/business/energy/x") == \
        "https://reuters.com/business/energy/x"


def test_a_real_subdomain_of_a_www_only_host_is_untouched():
    # The rule is keyed on the exact apex, not on a suffix: a genuine subdomain
    # is a different host and must not gain a "www." it never had.
    assert normalize_url("https://news.rivieramm.com/x/y") == "https://news.rivieramm.com/x/y"
    assert normalize_url("https://subscribe.lngindustry.com/a/b") == "https://subscribe.lngindustry.com/a/b"


def test_tracking_and_amp_rules_still_apply_on_a_www_only_host():
    assert normalize_url("https://www.lngindustry.com/lng/09092026/x/?utm_source=rss#top") == \
        "https://www.lngindustry.com/lng/09092026/x"


# ---------------------------------------------------------------------------
# Classification: the www form must still be international-English, named and
# extractable. It is (both spellings are registered), and this pins it.
# ---------------------------------------------------------------------------
def test_a_www_lngindustry_item_is_still_tagged_en():
    it = _entry_to_item(
        {"link": "https://www.lngindustry.com/floating-lng/14092026/kanata-pre-feed/",
         "title": "Kanata Clean Power to launch pre-FEED", "summary": "body"},
        "www.lngindustry.com",
    )
    assert it.url == "https://www.lngindustry.com/floating-lng/14092026/kanata-pre-feed"
    assert it.source_domain == "www.lngindustry.com"
    assert it.source_lang == "en"


def test_an_apex_lngindustry_item_is_normalised_to_www_and_still_tagged_en():
    it = _entry_to_item(
        {"link": "https://lngindustry.com/floating-lng/14092026/kanata-pre-feed/",
         "title": "Kanata Clean Power to launch pre-FEED", "summary": "body"},
        "www.lngindustry.com",
    )
    assert it.source_domain == "www.lngindustry.com"
    assert it.source_lang == "en"


@pytest.mark.parametrize("host", sorted(_ARTICLE_PATHS))
def test_the_www_form_is_named_and_extractable(host):
    www = f"www.{host}"
    assert source_name_for(www) != www, f"{www} would render as its bare domain"
    assert SOURCE_NAMES[www] == SOURCE_NAMES[host]
    assert resolve_extractor_domain(www) is not None


@pytest.mark.parametrize("host", ["lngindustry.com", "worldpipelines.com", "tanksterminals.com"])
def test_the_rss_registered_hosts_carry_both_spellings(host):
    # hydrocarbonengineering / rivieramm are GNews-covered, so they are not in
    # this set; the three RSS ones must be, in both forms.
    assert host in INTERNATIONAL_RSS_DOMAINS
    assert f"www.{host}" in INTERNATIONAL_RSS_DOMAINS


# ---------------------------------------------------------------------------
# The body fetch uses the STORED url verbatim — no re-derivation, no www strip.
# ---------------------------------------------------------------------------
def test_enrich_fetches_the_stored_url_verbatim(monkeypatch):
    seen: list[str] = []

    def _fake_fetch(url, timeout=6):
        seen.append(url)
        return "<html><head><title>t</title></head><body><p>x</p></body></html>"

    monkeypatch.setattr(enrich_mod, "fetch_html", _fake_fetch)

    url = "https://www.rivieramm.com/news-content-hub/vlccs-sold-like-hot-cakes-90002"
    item = RawItem(
        url=url, title="VLCCs sold like hot cakes", summary="",
        published_at=datetime.now(timezone.utc),
        source_domain="www.rivieramm.com", feed_domain="www.rivieramm.com",
    )
    enrich_mod.enrich_item(item, resolve_google_news=False, need_snippet=True)

    assert seen == [url], "the body fetch must GET the stored url, www and all"
