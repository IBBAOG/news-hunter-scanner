"""Energy News Bulletin research-report promos in the clipping shim (2026-10-19).

ENB paywall teasers carry a `div.research-block` promo ("A growing series of
reports...") next to the two free paragraphs; counted as body it made a teaser
look like a 3-paragraph article. Mirror of the contract shared with
`src/lib/clipping` in the dashboard repo.
"""
from __future__ import annotations

from news_hunter._clipinator_shim import NOISE_CLASS_SUBSTRINGS, _extract

P1 = (
    "Santos Basin operators lifted output by 4% in the September quarter as "
    "two new floating production units reached plateau."
)
P2 = (
    "The company said it expects a further increase once the third unit is "
    "connected to the subsea network early next year."
)
PROMO = (
    "A growing series of reports, each focused on a key discussion point for "
    "the energy sector, brought to you by our research team."
)


def test_research_block_is_a_noise_class() -> None:
    assert "research-block" in NOISE_CLASS_SUBSTRINGS
    assert NOISE_CLASS_SUBSTRINGS[-1] == "research-block"


def test_enb_research_promo_is_stripped_from_the_dom() -> None:
    html = (
        "<html><head>"
        '<meta property="og:title" content="Santos Basin output rises">'
        "</head><body><article>"
        f"<p>{P1}</p><p>{P2}</p>"
        f'<div class="research-block"><p class="report-text">{PROMO}</p></div>'
        "</article></body></html>"
    )
    _title, paragraphs = _extract(html, "energynewsbulletin.net")
    assert PROMO not in paragraphs
    assert paragraphs == [P1, P2]
