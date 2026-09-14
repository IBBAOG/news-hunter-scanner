"""Per-host feed timeout overrides — "rich but slow" must not read as "dead".

fetcher.FEED_TIMEOUT is 4s for everyone, which is right for a scan sharing a 22s
global deadline across ~65 feeds and wrong for the handful of feeds that answer
200 with a full, dated, on-beat payload in 6-13s: those are recorded as a
ReadTimeout and the outlet looks unreachable. The 2026-08-18 international waves
lost eia.gov (9-13s), intellinews.com and globalenergynetwork.net (~6.6s) that
way and downgraded them to GNews title-only coverage or rejected them.

sources.FEED_TIMEOUT_OVERRIDES lets one host declare a measured budget. What is
pinned here:

  * a host with no entry keeps FEED_TIMEOUT — the dict ships EMPTY, so landing it
    changed no production behaviour;
  * an entry actually reaches requests.get in BOTH feed paths (_fetch_one for
    RSS/Atom, _fetch_sitemap for Google News sitemaps) — an override that looks
    registered and never reaches the socket would be the same silent failure it
    is meant to cure;
  * lookup is www-insensitive in both directions, because the RSS_FEEDS key and
    the feed URL's host disagree about `www.` all the time.

Run from repo root: python -m pytest tests/test_feed_timeout_overrides.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import fetcher, sources  # noqa: E402


# The dict as SHIPPED, captured before any test touches it.
_SHIPPED = dict(sources.FEED_TIMEOUT_OVERRIDES)


@pytest.fixture(autouse=True)
def _clean_overrides():
    """Never leak a test's entries into the real registry (or the next test)."""
    saved = dict(sources.FEED_TIMEOUT_OVERRIDES)
    sources.FEED_TIMEOUT_OVERRIDES.clear()
    yield
    sources.FEED_TIMEOUT_OVERRIDES.clear()
    sources.FEED_TIMEOUT_OVERRIDES.update(saved)


class _FakeResponse:
    status_code = 200
    content = b"<rss version='2.0'><channel></channel></rss>"
    headers: dict[str, str] = {}

    def raise_for_status(self):
        return None


def _capture_timeout(monkeypatch) -> dict:
    """Record the timeout requests.get was called with, without a socket."""
    seen: dict = {}

    def fake_get(url, **kw):
        seen["url"] = url
        seen["timeout"] = kw.get("timeout")
        return _FakeResponse()

    monkeypatch.setattr(fetcher.requests, "get", fake_get)
    return seen


# --------------------------------------------------------------------------
# The resolver
# --------------------------------------------------------------------------

def test_unlisted_host_keeps_the_default():
    assert sources.feed_timeout("example.com", fetcher.FEED_TIMEOUT) == fetcher.FEED_TIMEOUT
    assert sources.feed_timeout("example.com", 4, host="www.example.com") == 4


def test_listed_host_replaces_the_default():
    sources.FEED_TIMEOUT_OVERRIDES["eia.gov"] = 13.0
    assert sources.feed_timeout("eia.gov", 4) == 13.0


def test_lookup_is_www_insensitive_in_both_directions():
    """An override that silently does nothing because one side carries a `www.`
    is the same class of bug this dict exists to kill."""
    sources.FEED_TIMEOUT_OVERRIDES["eia.gov"] = 13.0
    assert sources.feed_timeout("www.eia.gov", 4) == 13.0

    sources.FEED_TIMEOUT_OVERRIDES.clear()
    sources.FEED_TIMEOUT_OVERRIDES["www.intellinews.com"] = 9.0
    assert sources.feed_timeout("intellinews.com", 4) == 9.0


def test_url_host_is_a_fallback_when_the_registry_key_differs():
    """The registry key and the feed URL's host need not agree (path-scoped or
    subdomain feeds); either spelling may carry the override."""
    sources.FEED_TIMEOUT_OVERRIDES["feeds.example.org"] = 8.0
    assert sources.feed_timeout("example.org", 4, host="feeds.example.org") == 8.0


def test_the_shipped_registry_changes_nothing_for_unlisted_hosts():
    """Whatever the waves add, a host absent from the dict must keep FEED_TIMEOUT.

    (Asserting the dict is EMPTY would break the moment a wave legitimately fills
    it; the invariant that matters is the default path.)"""
    sources.FEED_TIMEOUT_OVERRIDES.update(_SHIPPED)  # the fixture restores after
    host = "definitely-not-registered.invalid"
    assert host not in sources.FEED_TIMEOUT_OVERRIDES
    assert sources.feed_timeout(host, fetcher.FEED_TIMEOUT) == fetcher.FEED_TIMEOUT


# --------------------------------------------------------------------------
# It reaches the socket
# --------------------------------------------------------------------------

def test_fetch_one_uses_the_default_when_unlisted(monkeypatch):
    seen = _capture_timeout(monkeypatch)
    fetcher._fetch_one("https://example.com/feed/", "example.com")
    assert seen["timeout"] == fetcher.FEED_TIMEOUT


def test_fetch_one_honours_the_override(monkeypatch):
    seen = _capture_timeout(monkeypatch)
    sources.FEED_TIMEOUT_OVERRIDES["eia.gov"] = 13.0
    fetcher._fetch_one("https://www.eia.gov/rss/todayinenergy.xml", "eia.gov")
    assert seen["timeout"] == 13.0


def test_news_sitemap_path_honours_the_override(monkeypatch):
    """_fetch_one delegates sitemap URLs to _fetch_sitemap, which has its own
    requests.get — the override has to bite there too."""
    seen = _capture_timeout(monkeypatch)
    sources.FEED_TIMEOUT_OVERRIDES["slow.example.com"] = 11.0
    url = "https://slow.example.com/sitemap-news.xml"
    assert fetcher.is_sitemap_url(url)
    fetcher._fetch_one(url, "slow.example.com")
    assert seen["timeout"] == 11.0
