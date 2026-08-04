"""Regression tests for the paginated-sitemap discovery in _fetch_standard_sitemap.

Incident (2026-08-04): visaoagro.com.br produced ZERO articles in 33/33 runs.
Two independent bugs stacked:

  1. sources.py pointed at https://visaoagro.com.br/post-sitemap.xml, which is
     Yoast's page ONE — the OLDEST posts (1000 URLs, 2022-06-01 → 2023-04-28).
     Everything fell outside the fetcher's 96h cutoff.
  2. Pointing at the index instead did not fix it and would have made it WORSE:
     the sitemapindex branch only accepted children whose <loc> contained
     "posts-post-" (WordPress core's wp-sitemap-posts-post-N.xml). Yoast names
     its pages post-sitemapN.xml, nothing matched, and the branch did
     `return [], None` — a zero with no error, invisible even in the
     "feeds returning 0 items" log line.

The traps these tests pin:

  * Yoast naming must be recognised, WITHOUT swallowing page-sitemap.xml,
    post_tag-sitemap.xml, category-sitemap.xml, author-sitemap.xml or
    tribe_events-sitemap.xml (all present in visaoagro's real index).
  * The page must be chosen by an explicit criterion, not by document order
    (`post_urls[-1]` happened to be right and would break on page 8) and not by
    the index's <lastmod>: WordPress core emits none at all (istoedinheiro:
    481 children, zero lastmod), and Yoast's LIES — visaoagro's index stamps
    post-sitemap.xml (content from 2023) with today's timestamp.
  * "No child matched" must be a LOGGED ERROR. That silent zero is the root bug.

Run from repo root: python -m pytest tests/test_standard_sitemap_index.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import fetcher  # noqa: E402
from news_hunter.fetcher import _fetch_standard_sitemap, _post_sitemap_page  # noqa: E402

_NOW = datetime.now(timezone.utc)
_FRESH = (_NOW - timedelta(hours=2)).isoformat()
_STALE = "2023-04-28T17:20:43+00:00"


def _index(children: list[tuple[str, str | None]]) -> bytes:
    body = "".join(
        f"<sitemap><loc>{loc}</loc>"
        + (f"<lastmod>{lm}</lastmod>" if lm else "")
        + "</sitemap>"
        for loc, lm in children
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</sitemapindex>"
    ).encode()


def _urlset(entries: list[tuple[str, str]]) -> bytes:
    body = "".join(
        f"<url><loc>{loc}</loc><lastmod>{lm}</lastmod></url>" for loc, lm in entries
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</urlset>"
    ).encode()


class _Resp:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


def _fake_get(pages: dict[str, bytes], seen: list[str]):
    def _get(url, **_kw):
        seen.append(url)
        if url not in pages:
            raise AssertionError(f"unexpected fetch: {url}")
        return _Resp(pages[url])

    return _get


# --- 1. page-number parsing -------------------------------------------------


def test_recognises_both_pagination_conventions():
    # WordPress core
    assert _post_sitemap_page("https://x.com/wp-sitemap-posts-post-1.xml") == 1
    assert _post_sitemap_page("https://x.com/wp-sitemap-posts-post-481.xml") == 481
    # Yoast — the bare name is page 1
    assert _post_sitemap_page("https://x.com/post-sitemap.xml") == 1
    assert _post_sitemap_page("https://x.com/post-sitemap7.xml") == 7


def test_does_not_swallow_the_other_children_of_a_yoast_index():
    # All five are real children of visaoagro.com.br/sitemap_index.xml.
    for name in (
        "page-sitemap.xml",
        "post_tag-sitemap.xml",
        "post_tag-sitemap2.xml",
        "category-sitemap.xml",
        "tribe_events-sitemap.xml",
        "author-sitemap.xml",
        "wp-sitemap-taxonomies-category-1.xml",
    ):
        assert _post_sitemap_page(f"https://x.com/{name}") is None, name


# --- 2. selection -----------------------------------------------------------


def test_yoast_index_picks_the_newest_page_not_page_one(monkeypatch):
    """The visaoagro shape: page 1 is oldest, and the index's lastmod lies."""
    idx = "https://visaoagro.com.br/sitemap_index.xml"
    pages = {
        # Page 1 carries 2023 content but the index advertises it as modified
        # today — exactly what the live index does.
        idx: _index([
            ("https://visaoagro.com.br/post-sitemap.xml", _FRESH),
            ("https://visaoagro.com.br/post-sitemap2.xml", "2024-07-13T17:32:41+00:00"),
            ("https://visaoagro.com.br/post-sitemap7.xml", _FRESH),
            ("https://visaoagro.com.br/page-sitemap.xml", _FRESH),
            ("https://visaoagro.com.br/post_tag-sitemap.xml", _FRESH),
            ("https://visaoagro.com.br/category-sitemap.xml", _FRESH),
            ("https://visaoagro.com.br/tribe_events-sitemap.xml", _FRESH),
        ]),
        "https://visaoagro.com.br/post-sitemap7.xml": _urlset([
            ("https://visaoagro.com.br/uso-do-arroz-no-etanol-pode-conter-excedentes/", _FRESH),
            ("https://visaoagro.com.br/algo-antigo-de-fevereiro/", "2026-02-03T11:01:39+00:00"),
        ]),
    }
    seen: list[str] = []
    monkeypatch.setattr(fetcher.requests, "get", _fake_get(pages, seen))

    items, err = _fetch_standard_sitemap(idx, "visaoagro.com.br")

    assert err is None
    assert seen == [idx, "https://visaoagro.com.br/post-sitemap7.xml"]
    assert [i.url for i in items] == [
        "https://visaoagro.com.br/uso-do-arroz-no-etanol-pode-conter-excedentes"
    ]


def test_wordpress_core_index_without_any_lastmod_still_picks_the_last_page(monkeypatch):
    """istoedinheiro's shape: 481 children, not one <lastmod>.

    A "max lastmod" criterion would be undefined here; page number is not.
    """
    idx = "https://istoedinheiro.com.br/wp-sitemap.xml"
    children = [
        (f"https://istoedinheiro.com.br/wp-sitemap-posts-post-{n}.xml", None)
        for n in (1, 2, 3, 481)
    ]
    children.append(("https://istoedinheiro.com.br/wp-sitemap-taxonomies-category-1.xml", None))
    pages = {
        idx: _index(children),
        "https://istoedinheiro.com.br/wp-sitemap-posts-post-481.xml": _urlset([
            ("https://istoedinheiro.com.br/petrobras-anuncia-algo-relevante/", _FRESH),
        ]),
    }
    seen: list[str] = []
    monkeypatch.setattr(fetcher.requests, "get", _fake_get(pages, seen))

    items, err = _fetch_standard_sitemap(idx, "istoedinheiro.com.br")

    assert err is None
    assert seen[-1].endswith("wp-sitemap-posts-post-481.xml")
    assert len(items) == 1


def test_selection_does_not_depend_on_document_order(monkeypatch):
    """`post_urls[-1]` was right by luck. Page 10 listed before page 9 broke it."""
    idx = "https://x.com/sitemap_index.xml"
    pages = {
        idx: _index([
            ("https://x.com/post-sitemap10.xml", None),
            ("https://x.com/post-sitemap9.xml", None),
        ]),
        "https://x.com/post-sitemap10.xml": _urlset([
            ("https://x.com/a-noticia-mais-recente-de-todas/", _FRESH),
        ]),
    }
    seen: list[str] = []
    monkeypatch.setattr(fetcher.requests, "get", _fake_get(pages, seen))

    items, err = _fetch_standard_sitemap(idx, "x.com")

    assert err is None
    assert seen[-1] == "https://x.com/post-sitemap10.xml"
    assert len(items) == 1


# --- 3. the silent zero -----------------------------------------------------


def test_index_with_no_post_page_is_an_error_not_a_silent_zero(monkeypatch):
    """The root bug: unknown pagination naming used to `return [], None`."""
    idx = "https://x.com/sitemap_index.xml"
    pages = {
        idx: _index([
            ("https://x.com/page-sitemap.xml", _FRESH),
            ("https://x.com/category-sitemap.xml", _FRESH),
            ("https://x.com/artigo-sitemap3.xml", _FRESH),  # naming we don't know
        ]),
    }
    seen: list[str] = []
    monkeypatch.setattr(fetcher.requests, "get", _fake_get(pages, seen))

    items, err = _fetch_standard_sitemap(idx, "x.com")

    assert items == []
    assert err is not None, "an unrecognised sitemapindex must be reported, not silent"
    assert "x.com" in err
    # The message has to name what WAS there, or a human cannot act on it.
    assert "page-sitemap.xml" in err
    assert seen == [idx], "must not fetch a second page when nothing matched"


def test_stale_chosen_page_warns_instead_of_passing_silently(monkeypatch, caplog):
    """Cross-check: chosen page much older than the index's own claim."""
    idx = "https://x.com/sitemap_index.xml"
    pages = {
        idx: _index([
            ("https://x.com/post-sitemap.xml", _FRESH),
            ("https://x.com/post-sitemap2.xml", _FRESH),
        ]),
        # The newest page carries nothing but 2023 — i.e. we picked wrong, or
        # the source reversed its pagination order.
        "https://x.com/post-sitemap2.xml": _urlset([
            ("https://x.com/coisa-velha/", _STALE),
        ]),
    }
    monkeypatch.setattr(fetcher.requests, "get", _fake_get(pages, []))

    with caplog.at_level("WARNING", logger="news_hunter.fetcher"):
        items, err = _fetch_standard_sitemap(idx, "x.com")

    assert err is None      # still not fatal — the source may just be idle
    assert items == []      # 2023 is outside the 96h cutoff
    assert any("post-sitemap2.xml" in r.getMessage() for r in caplog.records), caplog.text


def test_healthy_page_does_not_warn(monkeypatch, caplog):
    idx = "https://x.com/sitemap_index.xml"
    pages = {
        idx: _index([("https://x.com/post-sitemap2.xml", _FRESH)]),
        "https://x.com/post-sitemap2.xml": _urlset([("https://x.com/nova/", _FRESH)]),
    }
    monkeypatch.setattr(fetcher.requests, "get", _fake_get(pages, []))

    with caplog.at_level("WARNING", logger="news_hunter.fetcher"):
        items, err = _fetch_standard_sitemap(idx, "x.com")

    assert err is None
    assert len(items) == 1
    assert not caplog.records, caplog.text


def test_plain_urlset_still_works_without_an_index_hop(monkeypatch):
    """A non-index standard sitemap must keep its single-request behaviour."""
    url = "https://x.com/post-sitemap7.xml"
    seen: list[str] = []
    monkeypatch.setattr(
        fetcher.requests,
        "get",
        _fake_get({url: _urlset([("https://x.com/nova/", _FRESH)])}, seen),
    )

    items, err = _fetch_standard_sitemap(url, "x.com")

    assert err is None
    assert len(items) == 1
    assert seen == [url]
