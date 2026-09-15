"""Mobility Plaza items arrive on the share form, not on the article.

Google News resolves Mobility Plaza items to `/recommend/<slug>` — the site's
"send this recommendation" FORM — while the article lives at `/news/<slug>`,
same slug. Measured from the runner on 2026-09-15 with
`_clipinator_shim.fetch_html` + `_extract`::

    https://mobilityplaza.com/recommend/argentina-shell-could-sell-its-600-gas-station-network
        200, 17 KB, 0 paragraphs over 40 chars
    https://www.mobilityplaza.com/news/argentina-shell-could-sell-its-600-gas-station-network
        200, 21 KB, 4 body paragraphs, title
        "Argentina: Shell could sell its 600 gas station network"

Six of the seven mobilityplaza rows in news_articles were /recommend/ on
2026-09-15, so the outlet was landing bodiless almost every time: title-only
matching in the scanner, and a clipping the dashboard cannot generate.

The urls below are the REAL stored rows. The test never fetches anything — it
runs them through the same normalize_url the fetcher and the pipeline use.

Run from repo root: python -m pytest tests/test_mobilityplaza_recommend_rewrite.py -v
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import _entry_to_item  # noqa: E402
from news_hunter.store import normalize_url  # noqa: E402

# news_articles rows, 2026-09-15.
_STORED_RECOMMEND_SLUGS = (
    "irelands-top-oil-to-buy-sirio-for-over-10-million",
    "chinas-two-biggest-internet-companies-bring-battle-to-the-gas-station",
    "clean-energy-delivers-143-million-gallons-of-renewable-natural-gas",
    "argentina-shell-could-sell-its-600-gas-station-network",
    "pilot-launches-digital-payments-for-diesel-at-over-800-locations",
)


@pytest.mark.parametrize("slug", _STORED_RECOMMEND_SLUGS)
def test_a_recommend_url_is_rewritten_onto_the_article(slug):
    assert normalize_url(f"https://mobilityplaza.com/recommend/{slug}") == \
        f"https://mobilityplaza.com/news/{slug}"


def test_the_www_spelling_is_rewritten_too():
    assert normalize_url("https://www.mobilityplaza.com/recommend/pilot-launches-digital-payments") == \
        "https://mobilityplaza.com/news/pilot-launches-digital-payments"


def test_an_article_url_is_left_alone_and_the_rewrite_is_idempotent():
    url = "https://mobilityplaza.com/news/argentina-shell-could-sell-its-600-gas-station-network"
    assert normalize_url(url) == url
    assert normalize_url(normalize_url(url)) == url


def test_both_surfaces_collapse_onto_one_row():
    """The point of rewriting instead of dropping: /recommend/ and /news/ are
    the same article, so they must be ONE key — not one bodiless row plus one
    good row for the same story."""
    slug = "clean-energy-delivers-143-million-gallons-of-renewable-natural-gas"
    assert normalize_url(f"https://mobilityplaza.com/recommend/{slug}") == \
        normalize_url(f"https://www.mobilityplaza.com/news/{slug}")


def test_other_paths_on_the_host_are_untouched():
    for path in ("/news/x-y-z", "/events/fuel-forum-2026", "/recommendations/abc"):
        assert normalize_url(f"https://mobilityplaza.com{path}") == f"https://mobilityplaza.com{path}"


def test_recommend_on_another_host_is_untouched():
    # The rewrite is host-scoped: /recommend/ is a legitimate path elsewhere.
    assert normalize_url("https://example.com/recommend/some-story") == \
        "https://example.com/recommend/some-story"
    assert normalize_url("https://oilprice.com/recommend/a-b-c") == \
        "https://oilprice.com/recommend/a-b-c"


def test_the_fetcher_builds_the_item_on_the_article_url():
    """End of the path that produced the six rows: a Google News entry whose
    <source href> is Mobility Plaza."""
    entry = {
        "link": "https://mobilityplaza.com/recommend/argentina-shell-could-sell-its-600-gas-station-network",
        "title": "Argentina: Shell could sell its 600 gas station network - Mobility Plaza",
        "summary": "",
    }
    it = _entry_to_item(entry, "news.google.com")
    assert it.url == \
        "https://mobilityplaza.com/news/argentina-shell-could-sell-its-600-gas-station-network"
    assert it.source_domain == "mobilityplaza.com"
