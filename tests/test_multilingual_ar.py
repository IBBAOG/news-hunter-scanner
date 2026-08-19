"""Wave B1-d — Arabic pilot: source-lang tagging, translation Stage 3c, the
matching union + canonical rewrite, and the ar GNews query shape.

The English path is pinned separately (test_multilingual_en_frozen.py) and must
stay byte-identical; these tests exercise the FOREIGN mechanism the pilot proves.

Run from repo root: python -m pytest tests/test_multilingual_ar.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from urllib.parse import unquote_plus, urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import translate as translate_mod  # noqa: E402
from news_hunter.fetcher import RawItem, _lang_from_query_url  # noqa: E402
from news_hunter.filter import matches_keywords  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    AR_KEYWORDS,
    ARABIC_NO_RSS_DOMAINS,
    LANGUAGES,
    google_news_site_queries_lang,
)
from news_hunter.store import Article  # noqa: E402
from news_hunter.translate import TRANSLATOR_CODE, translate_to_en  # noqa: E402


# --- helpers to rebuild what pipeline.run_search builds (§2.3/§2.4) ----------
def _match_keywords(live: list[str]) -> list[str]:
    native = [nat for c in LANGUAGES.values() if c.translate for nat, _ in c.keyword_priority]
    return list(live) + native


def _native_to_canon() -> dict[str, str]:
    return {nat: canon for c in LANGUAGES.values() for nat, canon in c.keyword_priority}


def _rewrite(final_match: list[str]) -> list[str]:
    """Mirror of Stage 4's canonical rewrite + order-preserving dedupe."""
    n2c = _native_to_canon()
    out: list[str] = []
    for k in final_match:
        c = n2c.get(k, k)
        if c not in out:
            out.append(c)
    return out


# ============================================================================
# 1. Arabic config
# ============================================================================
def test_ar_config_shape():
    assert "ar" in LANGUAGES
    ar = LANGUAGES["ar"]
    assert ar.code == "ar"
    assert ar.translate is True            # foreign -> translate for display
    assert ar.hl == "ar"
    assert (ar.gl, ar.ceid) == ("EG", "EG:ar")
    assert ar.no_rss_domains == ARABIC_NO_RSS_DOMAINS
    assert ar.keyword_priority is AR_KEYWORDS
    assert ar.resolve_keywords is None     # foreign uses the DEFAULT (standalone) resolver


def test_ar_vocab_is_native_to_canonical_pairs_covering_the_beat():
    n2c = dict(AR_KEYWORDS)
    # every entry is a (native Arabic, English concept) pair
    for nat, canon in AR_KEYWORDS:
        assert nat and canon
        assert nat != canon                # native term differs from its concept
    # the beat: O&G + fuels + Middle-East conflict all represented
    assert n2c["نفط"] == "oil"
    assert n2c["غاز"] == "gas"
    assert n2c["أوبك"] == "OPEC"
    assert n2c["مصفاة"] == "refinery"
    assert n2c["ديزل"] == "diesel"
    assert n2c["بنزين"] == "gasoline"
    assert n2c["هرمز"] == "Hormuz"
    assert n2c["الحوثي"] == "Houthi"
    assert n2c["عقوبات"] == "sanction"
    assert n2c["ناقلة"] == "tanker"
    assert n2c["أرامكو"] == "Aramco"


# ============================================================================
# 2. Arabic GNews query shape (hl=ar, when: BEFORE OR, native terms, cap)
# ============================================================================
def test_ar_query_shape():
    ar = LANGUAGES["ar"]
    urls = google_news_site_queries_lang(ar, ["attaqa.net"], ["oil", "gás"], hours=168)
    assert len(urls) == 1
    url = urls[0]

    # locale params
    assert "hl=ar" in url and "gl=EG" in url and "ceid=EG:ar" in url

    q = unquote_plus(parse_qs(urlparse(url).query)["q"][0])
    # site: scope + when: BEFORE the OR block (do NOT reorder — else Google drops
    # the time filter under truncation)
    assert q.startswith("site:attaqa.net when:7d (")
    assert q.index("when:") < q.index(" OR ")

    # retrieval uses the NATIVE terms, not the live English set and not canonicals
    assert '"نفط"' in q
    assert '"oil"' not in q                 # canonical is for matching, not retrieval
    # the live keyword arg is IGNORED by the standalone foreign resolver
    assert '"gás"' not in q


def test_ar_query_respects_the_cap_of_12():
    ar = LANGUAGES["ar"]
    q = unquote_plus(parse_qs(urlparse(
        google_news_site_queries_lang(ar, ["attaqa.net"], [], hours=24)[0]
    ).query)["q"][0])
    # exactly the first 12 native terms (cap) -> 11 " OR " separators
    assert q.count(" OR ") == ar.cap - 1 == 11
    # a matching-only term beyond the cap (خام = crude, #13) is NOT retrieved
    assert '"خام"' not in q
    # but the first-slice terms ARE
    assert '"نفط"' in q and '"الحوثي"' in q


# ============================================================================
# 3. source_lang tagging (§1.4)
# ============================================================================
def test_rawitem_has_source_lang_defaulting_none():
    it = RawItem(url="u", title="t", summary="", published_at=None,
                 source_domain="d", feed_domain="d")
    assert it.source_lang is None
    it.source_lang = "ar"
    assert it.source_lang == "ar"


def test_lang_from_query_url_recovers_registry_langs_only():
    ar = LANGUAGES["ar"]
    ar_url = google_news_site_queries_lang(ar, ["attaqa.net"], [], hours=24)[0]
    en = LANGUAGES["en"]
    en_url = google_news_site_queries_lang(en, ["www.reuters.com"], ["oil"], hours=24)[0]

    assert _lang_from_query_url(ar_url) == "ar"
    assert _lang_from_query_url(en_url) == "en"
    # Portuguese (hl=pt-BR) is not in the registry -> None (native/untranslated)
    pt_url = "https://news.google.com/rss/search?q=x&hl=pt-BR&gl=BR&ceid=BR:pt"
    assert _lang_from_query_url(pt_url) is None
    # RSS feed (no hl=) -> None
    assert _lang_from_query_url("https://oilprice.com/rss/main") is None


# ============================================================================
# 4. Matching union + canonical rewrite (§2.3 / §2.4)
# ============================================================================
def test_arabic_substring_matches_through_the_al_clitic():
    # النفط ("al-naft" = "the oil") CONTAINS the bare stem نفط; \b-exact would
    # fail (no boundary before the clitic), substring passes — design §2.1.
    hits = matches_keywords("أسعار النفط ترتفع مع تصاعد التوترات في مضيق هرمز",
                            _match_keywords(["oil", "gas"]))
    assert "نفط" in hits


def test_union_lets_arabic_pass_and_rewrites_to_canonical():
    live = ["Petrobras", "oil", "gás"]
    hay = "أرامكو السعودية تعلن عن اكتشاف حقل غاز جديد"   # Saudi Aramco announces a new gas field
    hits = matches_keywords(hay, _match_keywords(live))
    assert hits                                         # Arabic item passes the sieve
    canon = _rewrite(hits)
    # user filtering "gas"/"Aramco" sees this story
    assert "gas" in canon
    assert "Aramco" in canon
    # no raw Arabic term survives into matched_keywords
    assert all(not any("؀" <= ch <= "ۿ" for ch in k) for k in canon)


def test_canonical_rewrite_dedupes():
    # both غاز -> gas and حقل غاز -> gas; the row must carry "gas" once
    assert _rewrite(["غاز", "حقل غاز"]) == ["gas"]


def test_union_adds_no_false_positive_to_latin_text():
    # Arabic native terms are non-Latin, so a PT/EN headline can never hit them.
    live = ["Petrobras", "oil"]
    hits_plain = matches_keywords("Petrobras raises diesel and oil prices", live)
    hits_union = matches_keywords("Petrobras raises diesel and oil prices",
                                  _match_keywords(live))
    assert set(hits_plain) == set(hits_union)           # union changed nothing here


def test_sentinels_survive_the_rewrite():
    assert _rewrite(["#topic"]) == ["#topic"]
    assert _rewrite(["#pending"]) == ["#pending"]


# ============================================================================
# 5. Translation module (fail-soft, code mapping, native guard)
# ============================================================================
def test_translator_code_mapping_has_the_gotchas():
    # Hebrew's tag IS 'iw' end to end (B2b): GoogleTranslator(source='he') raises,
    # so the key, source_lang and value are all 'iw', never 'he'.
    assert TRANSLATOR_CODE["iw"] == "iw"
    assert "he" not in TRANSLATOR_CODE
    assert TRANSLATOR_CODE["zh"] == "zh-CN"
    assert TRANSLATOR_CODE["ar"] == "ar"


def test_translate_never_touches_native_or_empty():
    # native / untagged tags are never translated (guarded in the module too)
    assert translate_to_en("hello", "en") is None
    assert translate_to_en("olá", "pt") is None
    assert translate_to_en("x", None) is None
    assert translate_to_en("", "ar") is None
    assert translate_to_en("   ", "ar") is None


class _RaisingTranslator:
    def __init__(self, *a, **k):
        pass

    def translate(self, text):
        raise RuntimeError("429 Too Many Requests")


class _FixedTranslator:
    last_source = None

    def __init__(self, source=None, target=None):
        type(self).last_source = source

    def translate(self, text):
        return "TRANSLATED: " + text


def test_translate_fail_soft_returns_none(monkeypatch):
    monkeypatch.setattr(translate_mod, "_translator_cls", lambda: _RaisingTranslator)
    # every backend attempt raises -> None (never raises out)
    assert translate_to_en("أسعار النفط ترتفع", "ar") is None


def test_translate_success_uses_mapped_code(monkeypatch):
    monkeypatch.setattr(translate_mod, "_translator_cls", lambda: _FixedTranslator)
    out = translate_to_en("مرحبا", "ar")
    assert out == "TRANSLATED: مرحبا"
    assert _FixedTranslator.last_source == "ar"   # used the mapped code, not 'auto'


# ============================================================================
# 6. Stage 3c orchestration (translate-only-kept, fail-soft, title_original)
# ============================================================================
def _article(source_lang, title="عنوان عربي", snippet=""):
    return Article(
        url=f"https://x/{title}/{source_lang}", domain="attaqa.net",
        source_name="attaqa", title=title, snippet=snippet,
        published_at=datetime.now(timezone.utc), found_at=datetime.now(timezone.utc),
        matched_keywords=["oil"], source_lang=source_lang,
    )


def test_stage3c_translates_only_foreign_and_stamps_original(monkeypatch):
    import news_hunter.pipeline as pipe

    monkeypatch.setattr(translate_mod, "translate_to_en",
                        lambda text, src: ("EN:" + text) if src == "ar" else None)

    ar = _article("ar", title="أسعار النفط", snippet="نص عربي")
    en = _article("en", title="Oil prices rise", snippet="body")
    pt = _article("pt", title="Preço do petróleo", snippet="corpo")
    none = _article(None, title="Preço", snippet="")

    n = pipe._run_translation([ar, en, pt, none], errors=[])

    assert n == 1                                   # only the Arabic row translated
    assert ar.title_en == "EN:أسعار النفط"
    assert ar.snippet_en == "EN:نص عربي"
    assert ar.title_original == "أسعار النفط"       # native decoupled from title
    assert ar.title == "أسعار النفط"                # title stays native
    # en/pt/None rows are untouched — no overlay written
    for other in (en, pt, none):
        assert other.title_en is None
        assert other.snippet_en is None
        assert other.title_original is None


def test_stage3c_fail_soft_keeps_row_with_null_title_en(monkeypatch):
    import news_hunter.pipeline as pipe

    # translator raises for everything -> translate_to_en returns None
    monkeypatch.setattr(translate_mod, "_translator_cls", lambda: _RaisingTranslator)

    ar = _article("ar", title="أسعار النفط ترتفع", snippet="نص")
    errors: list[str] = []
    n = pipe._run_translation([ar], errors)

    assert n == 0
    assert ar.title_en is None                      # NULL on failure
    assert ar.snippet_en is None
    assert ar.title_original == "أسعار النفط ترتفع"  # still stamped (native preserved)
    assert ar.title == "أسعار النفط ترتفع"          # row NEVER dropped, title intact


def test_stage3c_serializes_overlay_columns():
    from news_hunter.supabase_sync import _article_to_row

    ar = _article("ar")
    ar.title_original = ar.title
    ar.title_en = "Oil title"
    ar.snippet_en = None
    row = _article_to_row(ar)
    assert row["source_lang"] == "ar"
    assert row["title_original"] == ar.title
    assert row["title_en"] == "Oil title"
    assert row["snippet_en"] is None                # None -> SQL NULL (back-compat)
    # native columns keep their as-scraped meaning
    assert row["title"] == ar.title
