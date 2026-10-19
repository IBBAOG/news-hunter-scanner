"""OilPrice.com curated body selector (2026-10-19).

oilprice.com had no curated selector, so the domain fell to the generic
auto extractor and shipped the byline/date line, the author bio box and the
stock-ticker hover tooltip. The body `<p>` live only inside
`div#news-content` (= `div.wysiwyg.clear`); `span.article_byline`,
`div.articleAuthorBox` and `div.hover_tooltip` sit outside it.
`div.singleArticle__content div.wysiwyg` is the fallback if the id goes away.

Known residual (same on the TypeScript side): the sign-off
`By <Name> for Oilprice.com` is a bare `<p>` inside the container, so it is
still extracted; it needs a paragraph-level pattern later.

Mirrors the contract in `src/lib/clipping/sources.ts` of the dashboard repo.
"""
from __future__ import annotations

import pytest

from news_hunter._clipinator_shim import _extract

BYLINE = "By Jane Doe - Oct 16, 2026"
PARA_1 = (
    "Crude oil prices edged higher on Thursday as traders weighed fresh "
    "signals of tightening supply against a softer demand outlook in Asia."
)
PARA_2 = (
    "Analysts said inventories at the Cushing hub had fallen for a third "
    "consecutive week, adding support to the front of the futures curve."
)
SIGN_OFF = "By Jane Doe for Oilprice.com"
BIO = "What I Cover: energy markets, upstream investment and OPEC policy."
TICKER = "EXXON Mobil -0.35"


def _page(container_id: bool) -> str:
    id_attr = ' id="news-content"' if container_id else ""
    return (
        "<html><body>"
        '<div class="tableGrid__column singleArticle">'
        f'<span class="article_byline">{BYLINE}</span>'
        '<div class="singleArticle__content">'
        f'<div{id_attr} class="wysiwyg clear">'
        f"<p>{PARA_1}</p><p>{PARA_2}</p><p>{SIGN_OFF}</p>"
        "</div></div>"
        f'<div class="articleAuthorBox"><p>{BIO}</p></div>'
        f'<div class="hover_tooltip"><p>{TICKER}</p></div>'
        "</div>"
        "</body></html>"
    )


HOSTS = ["oilprice.com", "www.oilprice.com"]


@pytest.mark.parametrize("host", HOSTS)
def test_body_is_news_content_paragraphs_only(host: str) -> None:
    _, paragraphs = _extract(_page(container_id=True), host)
    # SIGN_OFF is the documented residual: a bare <p> inside the container.
    assert paragraphs == [PARA_1, PARA_2, SIGN_OFF]


@pytest.mark.parametrize("host", HOSTS)
def test_byline_bio_and_ticker_stay_out(host: str) -> None:
    _, paragraphs = _extract(_page(container_id=True), host)
    joined = "\n".join(paragraphs)
    assert "Oct 16, 2026" not in joined
    assert "What I Cover" not in joined
    assert "EXXON Mobil" not in joined


@pytest.mark.parametrize("host", HOSTS)
def test_fallback_without_news_content_id(host: str) -> None:
    _, paragraphs = _extract(_page(container_id=False), host)
    assert paragraphs == [PARA_1, PARA_2, SIGN_OFF]
