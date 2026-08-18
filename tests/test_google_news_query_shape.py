"""Tests for the shape of the Google News queries.

Incident (2026-08-04): several `site:` sources returned 100 items of which ZERO
were inside the 24h window — perennial quote pages (br.tradingview.com), tag
archives (conjur.com.br), decade-old material (agencia.petrobras.com.br). It
looked like a ranking problem. It was not: Google TRUNCATES the query at roughly
13 OR-terms, and `when:24h` was written LAST, so `when:` itself was the part
being dropped. The query degraded into a bare `site:` search.

Measured on site:noticias.r7.com with `when:24h` up front and filler terms:
"Petrobras" at position 1, or after 8 or 12 fillers -> 10 items; after 16 or 52
fillers -> 0. Moving `when:` to the front across the 21 pt-BR domains took
fresh-in-24h items from 159 to 313, with no domain regressing.

These tests pin the ORDER (the fix) and the English keyword subset. They do not
assert yields — those live in the comments in sources.py next to the numbers.

Run from repo root: python -m pytest tests/test_google_news_query_shape.py -v
"""
from __future__ import annotations

import os
import sys
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.sources import (  # noqa: E402
    ENGLISH_KEYWORD_CAP,
    ENGLISH_NO_RSS_DOMAINS,
    NO_RSS_DOMAINS,
    RSS_FEEDS,
    STANDARD_SITEMAPS,
    english_keywords,
    google_news_queries,
    google_news_site_queries,
    google_news_site_queries_en,
)

_KWS = ["Petrobras", "petróleo", "diesel", "gas", "oil", "Brent", "combustível"]


def _q(url: str) -> str:
    return parse_qs(urlparse(url).query)["q"][0]


def _params(url: str) -> dict:
    return {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}


# --- order: `when:` must precede the OR block -------------------------------


def test_site_query_puts_when_before_the_keyword_block():
    q = _q(google_news_site_queries(["br.tradingview.com"], _KWS, 24)[0])
    assert q.index("when:24h") < q.index('"Petrobras"'), q
    assert q.startswith("site:br.tradingview.com when:24h ("), q


def test_english_site_query_puts_when_before_the_keyword_block():
    q = _q(google_news_site_queries_en(["www.cnn.com"], _KWS, 24)[0])
    assert q.index("when:24h") < q.index('"Petrobras"'), q


def test_per_keyword_query_puts_when_first_too():
    q = _q(google_news_queries(["Petrobras"], 24)[0])
    assert q == 'when:24h "Petrobras"', q


def test_window_longer_than_48h_becomes_days():
    q = _q(google_news_site_queries(["x.com"], _KWS, 168)[0])
    assert "when:7d" in q and "when:168h" not in q, q


# --- language ---------------------------------------------------------------


def test_english_builder_asks_google_in_english():
    p = _params(google_news_site_queries_en(["www.cnn.com"], _KWS, 24)[0])
    assert p["hl"] == "en-US" and p["gl"] == "US" and p["ceid"] == "US:en"


def test_ptbr_builder_stays_in_portuguese():
    p = _params(google_news_site_queries(["br.investing.com"], _KWS, 24)[0])
    assert p["hl"] == "pt-BR" and p["gl"] == "BR" and p["ceid"] == "BR:pt"


# --- English keyword subset -------------------------------------------------


def test_english_subset_drops_portuguese_only_keywords():
    subset = english_keywords(_KWS)
    assert "petróleo" not in subset
    assert "combustível" not in subset
    assert {"Petrobras", "oil", "gas", "diesel", "Brent"} <= set(subset)


def test_english_subset_is_capped_under_the_truncation_limit():
    # The live set is 53 keywords; every one of them being English-ish must
    # still not produce a block Google would truncate.
    from news_hunter.sources import ENGLISH_KEYWORD_PRIORITY

    subset = english_keywords(list(ENGLISH_KEYWORD_PRIORITY) + ["extra1", "extra2"])
    assert len(subset) == ENGLISH_KEYWORD_CAP


def test_english_subset_is_a_filter_over_the_live_set_not_a_parallel_list():
    """A keyword deleted in Supabase must disappear from the query too."""
    assert "Petrobras" in english_keywords(["Petrobras", "oil"])
    assert "Petrobras" not in english_keywords(["oil", "gas"])


def test_english_subset_is_deterministic():
    a = english_keywords(_KWS)
    b = english_keywords(list(reversed(_KWS)))
    assert a == b


def test_english_subset_degrades_instead_of_emitting_an_empty_block():
    subset = english_keywords(["quitanda", "vaquejada"])
    assert subset, "an empty OR block would produce the malformed query 'site:x when:24h ()'"
    q = _q(google_news_site_queries_en(["www.cnn.com"], ["quitanda"], 24)[0])
    assert '("quitanda")' in q, q


# --- registry invariants ----------------------------------------------------


def test_english_outlets_are_not_queried_in_portuguese():
    # argusmedia was left behind when CNN and The Edge Singapore were moved on
    # 2026-08-04 and stayed on hl=pt-BR until 2026-08-18: 0 items at pt-BR vs 16
    # in 24h at en-US, measured the same minute. The workflow was green the whole
    # time — a query that answers 200 with an empty channel is the failure mode
    # this list exists to prevent.
    for dom in (
        "edition.cnn.com",
        "www.cnn.com",
        "www.theedgesingapore.com",
        "www.reuters.com",
        "www.argusmedia.com",
    ):
        assert dom in ENGLISH_NO_RSS_DOMAINS, dom
        assert dom not in NO_RSS_DOMAINS, dom


def test_dead_estadao_entries_are_gone():
    # estradao.* 301s to www.estadao.com.br/jornal-do-carro/estradao/ and GNews
    # indexes nothing under it; einvestidor's sitemap froze on 2026-05-27 and
    # its content now lives under the already-registered Estadao news sitemap.
    assert "estradao.estadao.com.br" not in NO_RSS_DOMAINS
    assert "einvestidor.estadao.com.br" not in RSS_FEEDS
    assert "www.estadao.com.br" in RSS_FEEDS


def test_visaoagro_points_at_the_index_not_at_yoast_page_one():
    urls = STANDARD_SITEMAPS["visaoagro.com.br"]
    assert urls == ["https://visaoagro.com.br/sitemap_index.xml"], urls


def test_no_domain_is_registered_in_both_language_lists():
    assert not (set(NO_RSS_DOMAINS) & set(ENGLISH_NO_RSS_DOMAINS))


def test_cnbc_points_at_the_energy_section_not_at_finance():
    # CNBC's section feeds differ only by `id`. id=10000664 is Finance: 30 fresh
    # items every scan, 0 of which matched the live keyword set (measured
    # 2026-08-18). id=19836768 is Energy: 23 of 30 matched. Same cost, same 200,
    # 23x the yield — which is why the id is pinned here and not just in a URL.
    urls = RSS_FEEDS["www.cnbc.com"]
    assert urls == [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml"
        "?partnerId=wrss01&id=19836768"
    ], urls
