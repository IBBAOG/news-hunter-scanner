"""Cross-language registry invariants — the generalized scope fence.

Wave B1-a fenced the registry to English only; B1-d opened it for the Arabic
pilot; B2a adds Russian and Chinese. Rather than re-asserting "en is the only
language" (the original fence) or "en + ar" (its B1-d form), this pins the
invariants EVERY foreign language must satisfy and enumerates the set that is
supposed to be active, so adding a language is a one-line change here and a
missing/malformed one fails loudly.

Per-language specifics live in test_multilingual_{ar,ru,zh}.py; the byte-identical
English golden lives in test_multilingual_en_frozen.py.

Run from repo root: python -m pytest tests/test_multilingual_registry.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.sources import LANGUAGES, google_news_site_queries_lang  # noqa: E402
from news_hunter.translate import TRANSLATOR_CODE  # noqa: E402


# The foreign (translate=True) languages that are supposed to be live. Grows one
# entry per wave (fa/he/es are the next). If a config is added without a test or
# dropped by accident, this set diverges and the suite fails.
EXPECTED_TRANSLATE_LANGS = {"ar", "ru", "zh"}


def _foreign():
    return {code: c for code, c in LANGUAGES.items() if c.translate}


def test_the_active_foreign_language_set_is_exactly_the_expected_one():
    assert set(_foreign()) == EXPECTED_TRANSLATE_LANGS
    # English is present and is NOT a translate language.
    assert "en" in LANGUAGES and LANGUAGES["en"].translate is False


def test_every_foreign_language_is_well_formed():
    for code, c in _foreign().items():
        assert c.code == code
        assert c.hl and c.gl and c.ceid, code
        assert c.cap >= 1, code
        assert c.no_rss_domains, code
        # foreign languages use the DEFAULT (standalone curated) resolver, so the
        # PT/live OR-block is never diluted and users never type the script.
        assert c.resolve_keywords is None, code
        assert c.keyword_priority, code
        for nat, canon in c.keyword_priority:
            assert nat and canon, code


def test_every_foreign_canonical_is_a_latin_english_concept():
    # The canonical is what lands in matched_keywords; it must be a Latin English
    # token, never native script (that is the whole point of the rewrite — §2.4).
    for code, c in _foreign().items():
        for _nat, canon in c.keyword_priority:
            assert canon.isascii(), (code, canon)


def test_every_foreign_language_has_a_translator_code():
    for code in _foreign():
        assert code in TRANSLATOR_CODE, code
        assert TRANSLATOR_CODE[code], code


def test_hl_is_unique_per_language_so_lang_recovery_is_unambiguous():
    # _lang_from_query_url maps hl= back to a code; a duplicate hl would mislabel.
    hls = [c.hl for c in LANGUAGES.values()]
    assert len(hls) == len(set(hls)), hls


def test_full_native_union_never_yields_a_non_ascii_canonical():
    # Build the global native->canonical map exactly like pipeline.run_search and
    # prove every value is ASCII: no Cyrillic/Arabic/Chinese term can survive the
    # rewrite into matched_keywords.
    n2c = {nat: canon for c in LANGUAGES.values() for nat, canon in c.keyword_priority}
    assert all(canon.isascii() for canon in n2c.values())


def test_foreign_query_carries_its_own_locale():
    for code, c in _foreign().items():
        url = google_news_site_queries_lang(c, list(c.no_rss_domains)[:1], [], hours=24)[0]
        assert f"hl={c.hl}" in url, code
        assert f"gl={c.gl}" in url, code
        assert f"ceid={c.ceid}" in url, code
