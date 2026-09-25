"""Suite-wide isolation for the translation module.

Two pieces of process-global state in news_hunter.translate must not leak
between tests:

  * the HTTP fallback backends (clients5, mymemory) would make real network
    calls from any test that exercises translate_to_en with a scripted
    deep-translator. They are stubbed to "declined" (None) by default; a test
    that wants them re-patches `_clients5_call` / `_mymemory_call` itself.
  * the circuit breaker counts consecutive failures across calls, so a test
    that feeds error pages would open it and change the next test's chain.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import date_credibility as _date_credibility  # noqa: E402
from news_hunter import translate as _translate  # noqa: E402


#: Hosts that carry the ARTICLE while `sources.py` registers a different host
#: for the same outlet, so `registered_hosts()` below cannot derive them.
#:
#: A feed host is not always the article host: www.pipelineoilandgasnews.com
#: links 30/30 items to energyconnects.com (measured on the parallel branch
#: intl-post-sources, item A, which registers that host in
#: INTERNATIONAL_RSS_DOMAINS), Business Day South Africa publishes on
#: businessday.co.za while its feed is businesslive.co.za, and Trend's feed is
#: en.trend.az while Google hands back trend.az. Each one was seen in
#: `news_articles` rendering as its bare domain. Listed literally on purpose:
#: this branch reads main's registries, which do not carry these hosts yet, so
#: deriving them would silently drop the cases until the branches merge.
#:
#: The second group below is the same failure from the other direction: not a
#: different outlet's article host, but a SUBDOMAIN of a registered one that
#: Google attributes items to. `resolve_extractor_domain` only strips
#: `www.`/`m.`/`amp.`/`mobile.`, so `africa.` / `ingest.` / `www-cdn.` /
#: `appointments.` are unknown hosts: no display name, no body fetch. They
#: cannot be derived from the registries either -- they are not registry
#: entries -- so each one is a literal case, added when it shows up in
#: `news_articles`.
ARTICLE_HOSTS: dict[str, str] = {
    "businessday.co.za": "Business Day (South Africa)",
    "energyconnects.com": "Energy Connects",
    "trend.az": "Trend News Agency",
    # resolved host variants seen in news_articles 2026-09-15 (row counts as of
    # that date): Google attributed items to these and they rendered bare.
    "africa.businessinsider.com": "Business Insider",     # 17 rows, Africa desk
    "www-cdn.abcnews.com": "ABC News",                    # 9 rows, CDN host
    "ingest.abcnews.com": "ABC News",                     # 3 rows, ingest host
    # 8 rows, job board. EXCLUDED at ingest since 2026-09-15 (store.
    # EXCLUDED_URL_PATTERNS) so no new row can appear; the name and the
    # extractor stay registered as defence in depth for the rows already
    # stored and for any variant the rule does not cover.
    "appointments.thetimes.com": "The Times",
}


def registered_hosts() -> set[str]:
    """Every host the scanner can attribute an English-language item to.

    A plain function, not a fixture: the international registry tests feed it to
    `pytest.mark.parametrize`, which runs at collection time, before any fixture
    exists.

    RSS entries arrive normalize_url'd (www stripped) and are registered apex +
    www in `INTERNATIONAL_RSS_DOMAINS`; a Google News entry may be path-scoped
    (`marketwatch.com/story`, `aa.com.tr/en`, `www3.nhk.or.jp/nhkworld`) and what
    reaches the enricher is the host part of it, with or without www. Derived
    from `sources.py` on every call so a wave that registers a feed and forgets
    the name or the extractor turns the tests red on its own.
    """
    from news_hunter.sources import (
        ENGLISH_NO_RSS_DOMAINS,
        INTERNATIONAL_RSS_DOMAINS,
    )

    hosts = set(INTERNATIONAL_RSS_DOMAINS)
    for entry in ENGLISH_NO_RSS_DOMAINS:
        host = entry.split("/")[0].lower()
        hosts.add(host)
        if host.startswith("www."):
            hosts.add(host.removeprefix("www."))
    return hosts


@pytest.fixture(autouse=True)
def _isolate_page_evidence():
    """The date-credibility page registry is process-global (one scan per
    process in production); a page one test records must not verify another
    test's item."""
    _date_credibility.reset_scan()
    yield
    _date_credibility.reset_scan()


@pytest.fixture(autouse=True)
def _no_network_page_fetches(monkeypatch):
    """Stage 4b checks the page of every never-seen feed item, so any test that
    runs the pipeline would otherwise reach the network. Page fetches fail
    here unless a test installs its own `news_hunter.enrich.fetch_html`."""
    from news_hunter import enrich

    def _offline(url, timeout=6):  # noqa: ARG001
        raise RuntimeError("network access disabled in tests")

    monkeypatch.setattr(enrich, "fetch_html", _offline)


@pytest.fixture(autouse=True)
def _isolate_translate_backends(monkeypatch):
    _translate.reset_breakers()
    monkeypatch.setattr(_translate, "_clients5_call", lambda payload, code: None)
    monkeypatch.setattr(_translate, "_mymemory_call", lambda payload, code: None)
    yield
    _translate.reset_breakers()
