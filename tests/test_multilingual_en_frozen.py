"""Frozen-output regression test for the English Google News query path.

Wave B1-a generalized the English-only ``google_news_site_queries_en`` into a
language registry (``LANGUAGES``) plus a language-generic builder
(``google_news_site_queries_lang``), keeping ``google_news_site_queries_en`` as a
one-line shim over ``LANGUAGES["en"]``. English is still the ONLY active
language — this wave is a pure, behaviour-preserving refactor.

The load-bearing test here (``test_en_urls_are_byte_identical_to_the_frozen_golden``)
pins the EXACT ``news.google.com/rss/search`` URLs the English path produced
BEFORE the refactor, captured from the pre-refactor code. It uses only
``google_news_site_queries_en`` — a symbol that exists on both sides of the
refactor — so it runs green against the pre-refactor code (proving the golden is
faithful) AND against the post-refactor code (proving the mechanism change
altered no English query). The golden strings are FROZEN: if a later wave
legitimately changes English retrieval, update them deliberately.

The remaining tests exercise the new registry mechanism and therefore import the
new symbols function-locally (they only exist post-refactor).

Run from repo root: python -m pytest tests/test_multilingual_en_frozen.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    google_news_site_queries_en,
)

# Fixed, representative inputs — deliberately NOT the live ENGLISH_NO_RSS_DOMAINS
# (which grows as sources are curated), so the golden below never drifts under a
# source edit. The keyword set is the 12 curated English priority terms plus two
# Portuguese noise terms that english_keywords() must drop from the OR block.
_DOMAINS = ["www.reuters.com", "edition.cnn.com"]
_KWS = [
    "oil", "gas", "diesel", "Brent", "WTI", "OPEC",
    "crude", "LNG", "refinery", "gasoline", "Petrobras", "sanction",
    "combustível", "Raízen",
]

# Golden output captured from the PRE-refactor google_news_site_queries_en, keyed
# by the `hours` window (24 -> when:24h, 168 -> when:7d). Frozen byte-for-byte.
_GOLDEN_EN_URLS: dict[int, list[str]] = {
    24: [
        "https://news.google.com/rss/search?q=site%3Awww.reuters.com+when%3A24h+%28%22oil%22+OR+%22gas%22+OR+%22diesel%22+OR+%22Brent%22+OR+%22WTI%22+OR+%22OPEC%22+OR+%22crude%22+OR+%22LNG%22+OR+%22refinery%22+OR+%22gasoline%22+OR+%22Petrobras%22+OR+%22sanction%22%29&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=site%3Aedition.cnn.com+when%3A24h+%28%22oil%22+OR+%22gas%22+OR+%22diesel%22+OR+%22Brent%22+OR+%22WTI%22+OR+%22OPEC%22+OR+%22crude%22+OR+%22LNG%22+OR+%22refinery%22+OR+%22gasoline%22+OR+%22Petrobras%22+OR+%22sanction%22%29&hl=en-US&gl=US&ceid=US:en",
    ],
    168: [
        "https://news.google.com/rss/search?q=site%3Awww.reuters.com+when%3A7d+%28%22oil%22+OR+%22gas%22+OR+%22diesel%22+OR+%22Brent%22+OR+%22WTI%22+OR+%22OPEC%22+OR+%22crude%22+OR+%22LNG%22+OR+%22refinery%22+OR+%22gasoline%22+OR+%22Petrobras%22+OR+%22sanction%22%29&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=site%3Aedition.cnn.com+when%3A7d+%28%22oil%22+OR+%22gas%22+OR+%22diesel%22+OR+%22Brent%22+OR+%22WTI%22+OR+%22OPEC%22+OR+%22crude%22+OR+%22LNG%22+OR+%22refinery%22+OR+%22gasoline%22+OR+%22Petrobras%22+OR+%22sanction%22%29&hl=en-US&gl=US&ceid=US:en",
    ],
}


def test_en_urls_are_byte_identical_to_the_frozen_golden():
    """The English builder emits exactly the pre-refactor URLs, byte-for-byte.

    Runs on BOTH sides of the refactor: pre-refactor it proves the golden is
    faithful; post-refactor it proves the registry mechanism changed no en URL.
    """
    for hours, golden in _GOLDEN_EN_URLS.items():
        assert google_news_site_queries_en(_DOMAINS, _KWS, hours) == golden


def test_en_shim_is_pure_passthrough_over_the_lang_builder():
    """google_news_site_queries_en adds nothing of its own: it is exactly the
    generic builder driven by LANGUAGES['en']."""
    from news_hunter.sources import LANGUAGES, google_news_site_queries_lang

    for hours in (24, 168):
        assert google_news_site_queries_en(_DOMAINS, _KWS, hours) == \
            google_news_site_queries_lang(LANGUAGES["en"], _DOMAINS, _KWS, hours)


def test_language_loop_adds_or_drops_no_english_query():
    """iter_collect/collect build the English GNews tasks by iterating LANGUAGES
    (en + ar + ru + ... as foreign waves land). This pins the ENGLISH contribution
    regardless of how many languages the registry holds: the URLs produced for
    LANGUAGES['en'] over the live English domain list must still equal the legacy
    direct call exactly — one per English domain, none added or lost by the
    registry mechanism. (Each foreign contribution is asserted separately in
    tests/test_multilingual_<code>.py + test_multilingual_registry.py.)"""
    from news_hunter.sources import LANGUAGES, google_news_site_queries_lang

    for hours in (24, 168):
        via_en = google_news_site_queries_lang(
            LANGUAGES["en"], list(LANGUAGES["en"].no_rss_domains), _KWS, hours
        )
        legacy = google_news_site_queries_en(list(ENGLISH_NO_RSS_DOMAINS), _KWS, hours)
        assert via_en == legacy
        assert len(via_en) == len(ENGLISH_NO_RSS_DOMAINS)


def test_en_config_is_unchanged_and_does_not_translate():
    """English remains a retrieval-language SWITCH, not a translation path — its
    config is byte-stable across waves. Wave B1-d deliberately opened the B1-a
    'en is the only language' scope fence by adding Arabic (translate=True); the
    English guarantees that matter — code, no-translate, hl/gl/ceid — are pinned
    here, and the byte-identical URL golden above is untouched."""
    from news_hunter.sources import LANGUAGES

    assert "en" in LANGUAGES
    en = LANGUAGES["en"]
    assert en.code == "en"
    assert en.translate is False
    assert (en.hl, en.gl, en.ceid) == ("en-US", "US", "US:en")
