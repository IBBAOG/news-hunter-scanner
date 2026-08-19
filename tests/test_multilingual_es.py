"""Wave B2b — Spanish: config, matching union + canonical rewrite, the es GNews
query shape, and the Latin-script collision guards.

Spanish is added as CONFIG ONLY on the mechanism the Arabic pilot proved, but it
is the FIRST Latin-script foreign language, so its native terms ARE tested by the
matching union against PT/EN text. The load-bearing tests here therefore pin
collision SAFETY (only ES-specific, non-colliding terms) rather than the
"non-Latin, so zero Latin FP by construction" argument the other languages use.
The English golden stays byte-identical (test_multilingual_en_frozen.py);
cross-language invariants live in test_multilingual_registry.py.

Run from repo root: python -m pytest tests/test_multilingual_es.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from urllib.parse import unquote_plus, urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import _lang_from_query_url  # noqa: E402
from news_hunter.filter import matches_keywords  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    ES_KEYWORDS,
    SPANISH_NO_RSS_DOMAINS,
    LANGUAGES,
    google_news_site_queries_lang,
)
from news_hunter.translate import TRANSLATOR_CODE  # noqa: E402


def _match_keywords(live: list[str]) -> list[str]:
    native = [nat for c in LANGUAGES.values() if c.translate for nat, _ in c.keyword_priority]
    return list(live) + native


def _native_to_canon() -> dict[str, str]:
    return {nat: canon for c in LANGUAGES.values() for nat, canon in c.keyword_priority}


def _rewrite(final_match: list[str]) -> list[str]:
    n2c = _native_to_canon()
    out: list[str] = []
    for k in final_match:
        c = n2c.get(k, k)
        if c not in out:
            out.append(c)
    return out


# The ES concept (non-proper-noun) terms — the ones whose FALSE match on PT/EN
# text would be a spurious canonical. Proper nouns matching a native mention is
# correct, not spurious, so they are excluded from the FP guard.
_ES_PROPER_NOUNS = {"Pemex", "Ecopetrol", "PDVSA", "YPF", "Vaca Muerta"}
_ES_CONCEPT_TERMS = [nat for nat, _ in ES_KEYWORDS if nat not in _ES_PROPER_NOUNS]


# ============================================================================
# 1. Spanish config
# ============================================================================
def test_es_config_shape():
    assert "es" in LANGUAGES
    es = LANGUAGES["es"]
    assert es.code == "es"
    assert es.translate is True
    assert es.hl == "es-419"
    assert (es.gl, es.ceid) == ("US", "US:es-419")
    assert es.no_rss_domains == SPANISH_NO_RSS_DOMAINS
    assert es.keyword_priority is ES_KEYWORDS
    assert es.resolve_keywords is None     # foreign uses the DEFAULT (standalone) resolver
    assert es.cap == 12


def test_es_translator_code_is_identity():
    assert TRANSLATOR_CODE["es"] == "es"


def test_es_vocab_covers_the_beat_and_all_canonicals_are_ascii():
    n2c = dict(ES_KEYWORDS)
    for nat, canon in ES_KEYWORDS:
        assert nat and canon
        assert canon.isascii(), (nat, canon)   # canonical is a Latin English token
    assert n2c["crudo"] == "crude"
    assert n2c["petrolero"] == "oil"
    assert n2c["refinería"] == "refinery"
    assert n2c["gasóleo"] == "diesel"
    assert n2c["oleoducto"] == "pipeline"
    assert n2c["sanciones"] == "sanction"
    assert n2c["Pemex"] == "Pemex"
    assert n2c["Ecopetrol"] == "Ecopetrol"
    assert n2c["Vaca Muerta"] == "Vaca Muerta"
    assert n2c["buque petrolero"] == "tanker"


def test_es_does_not_re_add_terms_already_in_the_keyword_set():
    # A Spanish article that only names a SHARED term (gas/diesel/Brent/OPEC/OPEP/
    # Ormuz/petróleo/gasolina) already matches via the existing keyword set — so
    # re-adding those here would only dilute the OR-block. Pins the "ES-specific
    # only" rule.
    natives = {nat.lower() for nat, _ in ES_KEYWORDS}
    for shared in ("gas", "diesel", "brent", "wti", "opec", "opep", "ormuz",
                   "petróleo", "petroleo", "gasolina", "crude"):
        assert shared not in natives, shared


# ============================================================================
# 2. Latin-script collision safety (the whole point for Spanish)
# ============================================================================
def test_es_concept_terms_do_not_hit_a_representative_pt_headline():
    # No ES concept term is a substring of common PT/EN words (measured 0 hits on
    # 40,309 native titles). This pins it against a realistic PT headline.
    pt = "Petrobras reduz preço da gasolina e do diesel nas refinarias; ações sobem"
    assert matches_keywords(pt, _ES_CONCEPT_TERMS) == []


def test_es_adds_no_new_canonical_to_a_pt_headline_without_es_terms():
    # The union (which now includes the ES terms) must tag this PT headline
    # exactly as the live keyword set alone does — the ES layer adds nothing.
    live = ["Petrobras", "diesel", "gasolina", "óleo"]
    pt = "Petrobras reduz preço da gasolina e do diesel nas refinarias"
    assert set(matches_keywords(pt, live)) == set(matches_keywords(pt, _match_keywords(live)))


def test_es_proper_noun_enriches_a_native_mention_correctly():
    # A PT/EN article that genuinely names Ecopetrol SHOULD carry "Ecopetrol" —
    # that is correct enrichment via the rewrite, not a false positive.
    pt = "Ecopetrol assume controle da Brava por US$ 1,2 bilhão no Brasil"
    canon = _rewrite(matches_keywords(pt, _match_keywords(["Brava", "Petrobras"])))
    assert "Ecopetrol" in canon
    assert all(k.isascii() for k in canon)


# ============================================================================
# 3. Spanish GNews query shape (hl=es-419, when: BEFORE OR, native terms, cap)
# ============================================================================
def test_es_query_shape():
    es = LANGUAGES["es"]
    urls = google_news_site_queries_lang(es, ["ambito.com"], ["oil", "gás"], hours=168)
    assert len(urls) == 1
    url = urls[0]
    assert "hl=es-419" in url and "gl=US" in url and "ceid=US:es-419" in url

    q = unquote_plus(parse_qs(urlparse(url).query)["q"][0])
    assert q.startswith("site:ambito.com when:7d (")
    assert q.index("when:") < q.index(" OR ")
    assert '"crudo"' in q
    assert '"oil"' not in q                 # canonical is for matching, not retrieval
    assert '"gás"' not in q                 # live keyword arg ignored by the foreign resolver


def test_es_query_respects_the_cap_of_12():
    es = LANGUAGES["es"]
    q = unquote_plus(parse_qs(urlparse(
        google_news_site_queries_lang(es, ["ambito.com"], [], hours=24)[0]
    ).query)["q"][0])
    assert q.count(" OR ") == es.cap - 1 == 11
    # matching-only terms beyond the cap are NOT retrieved
    for beyond in ('"diésel"', '"YPF"', '"Vaca Muerta"', '"buque petrolero"', '"gasoducto"'):
        assert beyond not in q
    # but the first-slice terms ARE
    assert '"crudo"' in q and '"Pemex"' in q and '"PDVSA"' in q


# ============================================================================
# 4. source_lang tagging (§1.4)
# ============================================================================
def test_lang_from_query_url_recovers_es():
    es = LANGUAGES["es"]
    es_url = google_news_site_queries_lang(es, ["ambito.com"], [], hours=24)[0]
    assert _lang_from_query_url(es_url) == "es"


# ============================================================================
# 5. Matching union + canonical rewrite (§2.3 / §2.4)
# ============================================================================
def test_union_lets_spanish_pass_and_rewrites_to_canonical():
    live = ["Petrobras", "oil"]
    es_text = "El crudo sube por la incertidumbre en el estrecho de Ormuz; Pemex recorta gasto"
    hits = matches_keywords(es_text, _match_keywords(live))
    canon = _rewrite(hits)
    assert "crude" in canon
    assert "Pemex" in canon
    # Ormuz is already an EN keyword; if present here it comes from the live set.
    assert all(k.isascii() for k in canon)


def test_es_buque_petrolero_wins_leftmost_longest_over_petrolero():
    # "buque petrolero" (oil tanker) -> tanker wins over "petrolero" -> oil inside
    # it; standalone "petrolero" stays oil.
    assert _rewrite(matches_keywords("Un buque petrolero fue atacado", _match_keywords([]))) == ["tanker"]
    assert _rewrite(matches_keywords("El sector petrolero crece", _match_keywords([]))) == ["oil"]


def test_es_canonical_rewrite_dedupes():
    # oleoducto and gasoducto both -> pipeline; carry it once.
    assert _rewrite(["oleoducto", "gasoducto"]) == ["pipeline"]


# ============================================================================
# 6. Article overlay round-trips through the sync serializer (parity w/ ru)
# ============================================================================
def _article(source_lang, title, snippet=""):
    from news_hunter.store import Article
    return Article(
        url=f"https://ambito.com/{title}", domain="ambito.com",
        source_name="Ámbito", title=title, snippet=snippet,
        published_at=datetime.now(timezone.utc), found_at=datetime.now(timezone.utc),
        matched_keywords=["crude"], source_lang=source_lang,
    )


def test_es_overlay_serializes():
    from news_hunter.supabase_sync import _article_to_row

    a = _article("es", "El crudo sube por la tensión en Ormuz")
    a.title_original = a.title
    a.title_en = "Crude rises on Hormuz tension"
    a.snippet_en = None
    row = _article_to_row(a)
    assert row["source_lang"] == "es"
    assert row["title_original"] == a.title
    assert row["title_en"] == "Crude rises on Hormuz tension"
    assert row["title"] == a.title
