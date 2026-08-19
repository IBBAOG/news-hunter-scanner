"""Wave B2b — Hebrew: config, native-substring matching union + canonical
rewrite, the iw GNews query shape, and the collision decisions that shaped the
vocabulary.

Hebrew is added as CONFIG ONLY on the mechanism the Arabic pilot proved
(retrieve in-language -> match native -> translate-after-filter); these tests pin
the iw-specific data. Its tag is 'iw' (Google's legacy Hebrew code) end to end,
never 'he' (GoogleTranslator(source='he') raises). The English golden stays
byte-identical (test_multilingual_en_frozen.py); cross-language invariants live in
test_multilingual_registry.py.

Run from repo root: python -m pytest tests/test_multilingual_iw.py -v
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
    IW_KEYWORDS,
    HEBREW_NO_RSS_DOMAINS,
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


def _has_hebrew(s: str) -> bool:
    return any("֐" <= ch <= "׿" for ch in s)


# ============================================================================
# 1. Hebrew config
# ============================================================================
def test_iw_config_shape():
    assert "iw" in LANGUAGES
    iw = LANGUAGES["iw"]
    assert iw.code == "iw"
    assert iw.translate is True
    assert iw.hl == "iw"
    assert (iw.gl, iw.ceid) == ("IL", "IL:iw")
    assert iw.no_rss_domains == HEBREW_NO_RSS_DOMAINS
    assert iw.keyword_priority is IW_KEYWORDS
    assert iw.resolve_keywords is None     # foreign uses the DEFAULT (standalone) resolver
    assert iw.cap == 12


def test_iw_translator_code_is_identity_and_he_is_not_the_tag():
    # The tag is 'iw' end to end; 'he' must never be used because
    # GoogleTranslator(source='he') raises LanguageNotSupportedException.
    assert TRANSLATOR_CODE["iw"] == "iw"
    assert "he" not in {c.code for c in LANGUAGES.values()}


def test_iw_vocab_is_native_to_canonical_pairs_covering_the_beat():
    n2c = dict(IW_KEYWORDS)
    for nat, canon in IW_KEYWORDS:
        assert nat and canon
        assert nat != canon
    # O&G + fuels + Middle-East conflict all represented
    assert n2c["נפט"] == "oil"
    assert n2c["גז"] == "gas"
    assert n2c["בנזין"] == "gasoline"
    assert n2c["סולר"] == "diesel"
    assert n2c["זיקוק"] == "refinery"
    assert n2c["מכלית"] == "tanker"
    assert n2c["סנקציות"] == "sanction"
    assert n2c["הורמוז"] == "Hormuz"
    assert n2c["איראן"] == "Iran"
    assert n2c["תימן"] == "Yemen"
    assert n2c["ברנט"] == "Brent"
    assert n2c["נפט גולמי"] == "crude"
    assert n2c["הים האדום"] == "Red Sea"


def test_iw_vocab_is_entirely_non_latin():
    # The union tests every native term against PT/EN/AR text too; a Latin term
    # would add false positives to that text. Keeping the whole vocab Hebrew is
    # what makes "adds zero false positives to Latin text" true by construction.
    # (Spaces are allowed: הים האדום / נפט גולמי are multi-word concepts.)
    for nat, _canon in IW_KEYWORDS:
        assert _has_hebrew(nat), nat
        assert not any("a" <= ch.lower() <= "z" for ch in nat), nat


def test_iw_dropped_collision_terms_are_absent():
    # These bare terms were measured to collide catastrophically as substrings and
    # are deliberately NOT carried; this pins the decision so a later edit can't
    # quietly re-add them.  חות ⊂ דוחות(reports)/לקוחות(customers)/כוחות(forces);
    # צינור ⊂ generic "conduit"/burst water pipes; אופק = "horizon"; דלק ⊂
    # דלקת("inflammation").
    natives = {nat for nat, _ in IW_KEYWORDS}
    for banned in ("חות", "צינור", "אופק", "אופ", "דלק", "עיצומים"):
        assert banned not in natives, banned


def test_iw_sources_are_business_energy_and_not_general_news():
    # globes/themarker/calcalist are business/energy desks; ynet/mako are general
    # news (more war/politics than O&G) and were rejected. Pins the curation.
    assert HEBREW_NO_RSS_DOMAINS == ("globes.co.il", "themarker.com", "calcalist.co.il")
    for banned in ("ynet.co.il", "mako.co.il", "walla.co.il", "en.globes.co.il"):
        assert banned not in HEBREW_NO_RSS_DOMAINS


# ============================================================================
# 2. Hebrew GNews query shape (hl=iw, when: BEFORE OR, native terms, cap)
# ============================================================================
def test_iw_query_shape():
    iw = LANGUAGES["iw"]
    urls = google_news_site_queries_lang(iw, ["globes.co.il"], ["oil", "gás"], hours=168)
    assert len(urls) == 1
    url = urls[0]
    assert "hl=iw" in url and "gl=IL" in url and "ceid=IL:iw" in url

    q = unquote_plus(parse_qs(urlparse(url).query)["q"][0])
    assert q.startswith("site:globes.co.il when:7d (")
    assert q.index("when:") < q.index(" OR ")
    # retrieval uses the NATIVE terms, not the live English set and not canonicals
    assert '"נפט"' in q
    assert '"oil"' not in q                 # canonical is for matching, not retrieval
    assert '"gás"' not in q                 # live keyword arg ignored by the foreign resolver


def test_iw_query_respects_the_cap_of_12():
    iw = LANGUAGES["iw"]
    q = unquote_plus(parse_qs(urlparse(
        google_news_site_queries_lang(iw, ["globes.co.il"], [], hours=24)[0]
    ).query)["q"][0])
    assert q.count(" OR ") == iw.cap - 1 == 11
    # the matching-only term (נפט גולמי = crude, #13) is NOT retrieved
    assert '"נפט גולמי"' not in q
    # but the first-slice terms ARE
    assert '"נפט"' in q and '"הורמוז"' in q and '"תימן"' in q


# ============================================================================
# 3. source_lang tagging (§1.4)
# ============================================================================
def test_lang_from_query_url_recovers_iw():
    iw = LANGUAGES["iw"]
    iw_url = google_news_site_queries_lang(iw, ["globes.co.il"], [], hours=24)[0]
    assert _lang_from_query_url(iw_url) == "iw"


# ============================================================================
# 4. Matching union + canonical rewrite (§2.3 / §2.4)
# ============================================================================
def test_hebrew_term_matches_through_attached_proclitics():
    # הנפט (the-oil) / בנפט (in-oil) both CONTAIN נפט; substring passes where
    # \b-exact would fail on the attached article (design §2.1).
    for hay in (
        "מחירי הנפט עלו בעקבות המתיחות במפרץ",   # הנפט (the-oil)
        "השקעה בנפט ובגז מזנקת",                  # בנפט (in-oil)
    ):
        hits = matches_keywords(hay, _match_keywords(["oil"]))
        assert "נפט" in hits


def test_union_lets_hebrew_pass_and_rewrites_to_canonical():
    live = ["Petrobras", "oil", "gás"]
    # "NewMed will look for gas; the Tamar and Leviathan gas platforms" — the
    # essential Israeli gas beat.
    hay = "ניו מד תחפש גז בישראל; אסדות הגז תמר ולוויתן"
    hits = matches_keywords(hay, _match_keywords(live))
    assert hits
    canon = _rewrite(hits)
    assert "gas" in canon
    # no raw Hebrew term survives into matched_keywords
    assert all(not _has_hebrew(k) for k in canon)


def test_iw_nested_terms_resolve_leftmost_longest():
    # נפט גולמי ("crude oil") contains נפט ("oil"); longest-first + non-overlapping
    # matching tags the specific concept (crude), and oil co-fires only when נפט
    # also appears separately.
    assert _rewrite(matches_keywords("מלאי נפט גולמי ירד", _match_keywords([]))) == ["crude"]
    both = _rewrite(matches_keywords("נפט גולמי מול הנפט המזוקק", _match_keywords([])))
    assert "crude" in both and "oil" in both


def test_iw_canonical_rewrite_dedupes():
    # a headline naming gas twice (bare + prefixed) carries "gas" once.
    assert _rewrite(matches_keywords("גז טבעי; מאגרי הגז", _match_keywords([]))) == ["gas"]


def test_union_adds_no_false_positive_to_latin_text():
    # Hebrew terms are non-Latin, so a PT/EN headline can never hit them.
    live = ["Petrobras", "oil"]
    text = "Petrobras raises diesel and oil prices"
    assert set(matches_keywords(text, live)) == set(matches_keywords(text, _match_keywords(live)))


def test_iw_gas_collision_with_sector_is_known_and_bounded():
    # DOCUMENTED residue (not a bug): גז ⊂ מגזר ("sector"), which substring
    # matching cannot avoid without \b (Hebrew proclitics forbid \b). Kept because
    # dropping גז would blind the scanner to the Israeli gas sector. This pins the
    # known behaviour so it is a conscious trade-off, not a surprise.
    assert "גז" in matches_keywords("המגזר הבנקאי", _match_keywords([]))
    # ...and the essential real-gas headlines still match, which is the point.
    assert "גז" in matches_keywords("מחירי הגז הטבעי עלו", _match_keywords([]))


# ============================================================================
# 5. Article overlay round-trips through the sync serializer (parity w/ ru)
# ============================================================================
def _article(source_lang, title, snippet=""):
    from news_hunter.store import Article
    return Article(
        url=f"https://globes.co.il/{title}", domain="globes.co.il",
        source_name="Globes (גלובס)", title=title, snippet=snippet,
        published_at=datetime.now(timezone.utc), found_at=datetime.now(timezone.utc),
        matched_keywords=["oil"], source_lang=source_lang,
    )


def test_iw_overlay_serializes():
    from news_hunter.supabase_sync import _article_to_row

    a = _article("iw", "מחירי הנפט עלו")
    a.title_original = a.title
    a.title_en = "Oil prices rose"
    a.snippet_en = None
    row = _article_to_row(a)
    assert row["source_lang"] == "iw"
    assert row["title_original"] == a.title
    assert row["title_en"] == "Oil prices rose"
    assert row["title"] == a.title
