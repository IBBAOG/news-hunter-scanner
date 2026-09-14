"""_scrape_homepage's same-site filter — `lstrip("www.")` was not a prefix strip.

`str.lstrip(chars)` removes every leading character that appears in `chars`, so
`"worldpipelines.com".lstrip("www.")` is `"orldpipelines.com"` — not the apex,
and a netloc nobody owns. The old membership tuple therefore did two wrong
things at once on any domain starting with `w`:

  * ACCEPTED `orldpipelines.com` as same-site;
  * for feed_domain `www.worldpipelines.com`, never produced `worldpipelines.com`,
    so the outlet's own article links were discarded as foreign and the scraper
    returned an empty listing — a silent zero.

Invisible on every other domain, which is why it lived in production. These
tests fail against the old expression (replicated below) and pass with
`removeprefix`.

Run from repo root: python -m pytest tests/test_fetcher_same_domain_filter.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import _same_site  # noqa: E402


def _old_filter(feed_domain: str, netloc: str) -> bool:
    """The pre-2026-09-14 expression, verbatim, so the bug is demonstrable."""
    return netloc.lower() in (
        feed_domain,
        f"www.{feed_domain}",
        feed_domain.lstrip("www."),
    )


# ---------------------------------------------------------------------------
# The "w" domain: both halves of the bug
# ---------------------------------------------------------------------------
def test_w_domain_apex_and_www_are_the_same_site():
    assert _same_site("worldpipelines.com", "worldpipelines.com")
    assert _same_site("worldpipelines.com", "www.worldpipelines.com")
    assert _same_site("www.worldpipelines.com", "worldpipelines.com")
    assert _same_site("www.worldpipelines.com", "www.worldpipelines.com")


def test_the_old_filter_dropped_the_outlets_own_articles():
    # What production would have done for a `www.` feed on a `w` domain:
    # the real article host was NOT accepted.
    assert not _old_filter("www.worldpipelines.com", "worldpipelines.com")
    # Fixed.
    assert _same_site("www.worldpipelines.com", "worldpipelines.com")


def test_the_old_filter_accepted_a_domain_nobody_owns():
    assert _old_filter("worldpipelines.com", "orldpipelines.com")
    # Fixed: a mangled host is foreign.
    assert not _same_site("worldpipelines.com", "orldpipelines.com")
    assert not _same_site("www.worldpipelines.com", "orldpipelines.com")


# ---------------------------------------------------------------------------
# No regression on the domains the scraper actually runs on today
# ---------------------------------------------------------------------------
def test_registered_homepage_scrapers_still_match_their_own_links():
    for feed_domain, apex in (
        ("www.brasilenergia.com.br", "brasilenergia.com.br"),
        ("www.atribuna.com.br", "atribuna.com.br"),
    ):
        assert _same_site(feed_domain, feed_domain)
        assert _same_site(feed_domain, apex)
        assert _same_site(apex, feed_domain)


def test_other_sites_are_still_rejected():
    for netloc in (
        "twitter.com",
        "facebook.com",
        "brasilenergia.com.br.evil.example",
        "notbrasilenergia.com.br",
        "",
    ):
        assert not _same_site("www.brasilenergia.com.br", netloc)


def test_case_is_normalised():
    assert _same_site("worldpipelines.com", "WWW.WorldPipelines.COM")
