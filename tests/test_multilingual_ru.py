"""Wave B2a — Russian: config, native-stem matching union + canonical rewrite,
the ru GNews query shape, and the газ/Gaza collision guard.

Russian is added as CONFIG ONLY on the mechanism the Arabic pilot proved
(retrieve in-language -> match native -> translate-after-filter); these tests pin
the ru-specific data. The English golden stays byte-identical
(test_multilingual_en_frozen.py); cross-language invariants live in
test_multilingual_registry.py.

Run from repo root: python -m pytest tests/test_multilingual_ru.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from urllib.parse import unquote_plus, urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import RawItem, _lang_from_query_url  # noqa: E402
from news_hunter.filter import matches_keywords  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    RU_KEYWORDS,
    RUSSIAN_NO_RSS_DOMAINS,
    LANGUAGES,
    google_news_site_queries_lang,
)
from news_hunter.translate import TRANSLATOR_CODE  # noqa: E402


# --- helpers to rebuild what pipeline.run_search builds (§2.3/§2.4) ----------
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


# A Cyrillic char range, used to prove no native term leaks into matched_keywords.
def _has_cyrillic(s: str) -> bool:
    return any("Ѐ" <= ch <= "ӿ" for ch in s)


# ============================================================================
# 1. Russian config
# ============================================================================
def test_ru_config_shape():
    assert "ru" in LANGUAGES
    ru = LANGUAGES["ru"]
    assert ru.code == "ru"
    assert ru.translate is True
    assert ru.hl == "ru"
    assert (ru.gl, ru.ceid) == ("RU", "RU:ru")
    assert ru.no_rss_domains == RUSSIAN_NO_RSS_DOMAINS
    assert ru.keyword_priority is RU_KEYWORDS
    assert ru.resolve_keywords is None     # foreign uses the DEFAULT (standalone) resolver
    assert ru.cap == 12


def test_ru_translator_code_is_identity():
    assert TRANSLATOR_CODE["ru"] == "ru"


def test_ru_vocab_is_native_to_canonical_pairs_covering_the_beat():
    n2c = dict(RU_KEYWORDS)
    for nat, canon in RU_KEYWORDS:
        assert nat and canon
        assert nat != canon
    # O&G + fuels + Middle-East conflict all represented
    assert n2c["нефт"] == "oil"
    assert n2c["газ"] == "gas"
    assert n2c["ОПЕК"] == "OPEC"
    assert n2c["Газпром"] == "Gazprom"
    assert n2c["Роснефт"] == "Rosneft"
    assert n2c["Лукойл"] == "Lukoil"
    assert n2c["дизел"] == "diesel"
    assert n2c["бензин"] == "gasoline"
    assert n2c["НПЗ"] == "refinery"
    assert n2c["танкер"] == "tanker"
    assert n2c["санкц"] == "sanction"
    assert n2c["Ормуз"] == "Hormuz"
    assert n2c["Иран"] == "Iran"
    assert n2c["СПГ"] == "LNG"
    assert n2c["Брент"] == "Brent"
    assert n2c["Юралс"] == "Urals"


def test_ru_vocab_is_entirely_non_latin():
    # The union tests every native term against PT/EN/AR text too; a Latin term
    # (e.g. a bare "Urals"/"Brent") would add false positives to that text. Keeping
    # the whole vocab Cyrillic is what makes "adds zero false positives to Latin
    # text" true by construction (pipeline.run_search §2.3).
    for nat, _canon in RU_KEYWORDS:
        assert _has_cyrillic(nat), nat
        assert not any("a" <= ch.lower() <= "z" for ch in nat), nat


def test_ru_sources_are_energy_only():
    # The Gaza collision (`газ` ⊂ Газе/Газы/Газу) is closed by retrieving only
    # from energy publishers — a general-news domain would reopen it. This pins
    # the curation so a future edit can't quietly add rbc/tass/ria here.
    assert RUSSIAN_NO_RSS_DOMAINS == ("neftegaz.ru", "oilcapital.ru", "eprussia.ru")
    for banned in ("rbc.ru", "tass.ru", "ria.ru", "kommersant.ru", "vedomosti.ru", "interfax.ru"):
        assert banned not in RUSSIAN_NO_RSS_DOMAINS


# ============================================================================
# 2. Russian GNews query shape (hl=ru, when: BEFORE OR, native stems, cap)
# ============================================================================
def test_ru_query_shape():
    ru = LANGUAGES["ru"]
    urls = google_news_site_queries_lang(ru, ["neftegaz.ru"], ["oil", "gás"], hours=168)
    assert len(urls) == 1
    url = urls[0]
    assert "hl=ru" in url and "gl=RU" in url and "ceid=RU:ru" in url

    q = unquote_plus(parse_qs(urlparse(url).query)["q"][0])
    assert q.startswith("site:neftegaz.ru when:7d (")
    assert q.index("when:") < q.index(" OR ")
    # retrieval uses the NATIVE stems, not the live English set and not canonicals
    assert '"нефт"' in q
    assert '"oil"' not in q                 # canonical is for matching, not retrieval
    assert '"gás"' not in q                 # live keyword arg ignored by the foreign resolver


def test_ru_query_respects_the_cap_of_12():
    ru = LANGUAGES["ru"]
    q = unquote_plus(parse_qs(urlparse(
        google_news_site_queries_lang(ru, ["neftegaz.ru"], [], hours=24)[0]
    ).query)["q"][0])
    assert q.count(" OR ") == ru.cap - 1 == 11
    # a matching-only stem beyond the cap (добыч = production, #18) is NOT retrieved
    assert '"добыч"' not in q
    # but the first-slice stems ARE
    assert '"нефт"' in q and '"Ормуз"' in q


# ============================================================================
# 3. source_lang tagging (§1.4)
# ============================================================================
def test_lang_from_query_url_recovers_ru():
    ru = LANGUAGES["ru"]
    ru_url = google_news_site_queries_lang(ru, ["neftegaz.ru"], [], hours=24)[0]
    assert _lang_from_query_url(ru_url) == "ru"


# ============================================================================
# 4. Matching union + canonical rewrite (§2.3 / §2.4)
# ============================================================================
def test_russian_stem_matches_through_declensions():
    # нефть (nominative) / нефти (genitive) both CONTAIN the stem нефт; substring
    # passes where \b-exact would fail on the declension ending (design §2.1).
    for hay in (
        "Цены на нефть растут на фоне напряжённости в Ормузском проливе",   # нефть
        "Экспорт нефти из России достиг рекордного уровня",                  # нефти
    ):
        hits = matches_keywords(hay, _match_keywords(["oil"]))
        assert "нефт" in hits


def test_union_lets_russian_pass_and_rewrites_to_canonical():
    live = ["Petrobras", "oil", "gás"]
    hay = "Газпром сократил поставки газа в ЕС; растёт спрос на СПГ"  # Gazprom cut gas supplies; LNG demand up
    hits = matches_keywords(hay, _match_keywords(live))
    assert hits
    canon = _rewrite(hits)
    assert "Gazprom" in canon
    assert "gas" in canon
    assert "LNG" in canon
    # no raw Russian term survives into matched_keywords
    assert all(not _has_cyrillic(k) for k in canon)


def test_ru_canonical_rewrite_dedupes():
    # both нефтепровод -> pipeline and газопровод -> pipeline; carry "pipeline" once
    assert _rewrite(["нефтепровод", "газопровод"]) == ["pipeline"]


def test_ru_nested_terms_resolve_leftmost_longest():
    # Роснефт ("Rosneft") contains нефт ("oil"); longest-first + non-overlapping
    # matching tags the most specific concept (Rosneft) within the word, not oil —
    # oil co-fires only if нефт also appears separately.
    assert _rewrite(matches_keywords("Роснефть нарастила экспорт", _match_keywords([]))) == ["Rosneft"]
    both = _rewrite(matches_keywords("Роснефть и цены на нефть", _match_keywords([])))
    assert "Rosneft" in both and "oil" in both


def test_union_adds_no_false_positive_to_latin_text():
    # Russian stems are non-Latin, so a PT/EN headline can never hit them.
    live = ["Petrobras", "oil"]
    text = "Petrobras raises diesel and oil prices"
    assert set(matches_keywords(text, live)) == set(matches_keywords(text, _match_keywords(live)))


def test_ru_gas_stem_matches_lowercase_gas_declensions():
    # The stem must catch real gas headlines in their common cases.
    for hay in ("экспорт газа вырос", "цены на газ", "газовый рынок ЕС"):
        assert "газ" in matches_keywords(hay, _match_keywords([]))


# ============================================================================
# 5. Article overlay round-trips through the sync serializer (parity w/ ar)
# ============================================================================
def _article(source_lang, title, snippet=""):
    from news_hunter.store import Article
    return Article(
        url=f"https://neftegaz.ru/{title}", domain="neftegaz.ru",
        source_name="Neftegaz.RU (Нефтегаз)", title=title, snippet=snippet,
        published_at=datetime.now(timezone.utc), found_at=datetime.now(timezone.utc),
        matched_keywords=["oil"], source_lang=source_lang,
    )


def test_ru_overlay_serializes():
    from news_hunter.supabase_sync import _article_to_row

    a = _article("ru", "Цены на нефть растут")
    a.title_original = a.title
    a.title_en = "Oil prices rise"
    a.snippet_en = None
    row = _article_to_row(a)
    assert row["source_lang"] == "ru"
    assert row["title_original"] == a.title
    assert row["title_en"] == "Oil prices rise"
    assert row["title"] == a.title
