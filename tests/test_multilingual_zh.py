"""Wave B2a — Chinese (Simplified): config, native substring matching union +
canonical rewrite, the zh GNews query shape, and the zh-CN translator gotcha.

Chinese is added as CONFIG ONLY on the mechanism the Arabic pilot proved; these
tests pin the zh-specific data. The English golden stays byte-identical
(test_multilingual_en_frozen.py); cross-language invariants live in
test_multilingual_registry.py.

Run from repo root: python -m pytest tests/test_multilingual_zh.py -v
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
    ZH_KEYWORDS,
    CHINESE_NO_RSS_DOMAINS,
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


def _has_cjk(s: str) -> bool:
    return any("㐀" <= ch <= "鿿" for ch in s)


# ============================================================================
# 1. Chinese config
# ============================================================================
def test_zh_config_shape():
    assert "zh" in LANGUAGES
    zh = LANGUAGES["zh"]
    assert zh.code == "zh"
    assert zh.translate is True
    # hl is zh-CN but ceid is CN:zh-Hans — Google's own convention, measured live.
    assert zh.hl == "zh-CN"
    assert (zh.gl, zh.ceid) == ("CN", "CN:zh-Hans")
    assert zh.no_rss_domains == CHINESE_NO_RSS_DOMAINS
    assert zh.keyword_priority is ZH_KEYWORDS
    assert zh.resolve_keywords is None
    assert zh.cap == 12


def test_zh_translator_code_is_zh_cn_not_bare_zh():
    # GoogleTranslator(source='zh') raises; the mapped code must be 'zh-CN'.
    assert TRANSLATOR_CODE["zh"] == "zh-CN"


def test_zh_vocab_covers_the_beat():
    n2c = dict(ZH_KEYWORDS)
    for nat, canon in ZH_KEYWORDS:
        assert nat and canon
        assert nat != canon
    assert n2c["石油"] == "oil"
    assert n2c["天然气"] == "gas"
    assert n2c["原油"] == "crude"
    assert n2c["柴油"] == "diesel"
    assert n2c["汽油"] == "gasoline"
    assert n2c["炼油"] == "refinery"
    assert n2c["欧佩克"] == "OPEC"
    assert n2c["制裁"] == "sanction"
    assert n2c["油轮"] == "tanker"
    assert n2c["霍尔木兹"] == "Hormuz"
    assert n2c["伊朗"] == "Iran"
    assert n2c["中石油"] == "PetroChina"
    assert n2c["中石化"] == "Sinopec"
    assert n2c["液化天然气"] == "LNG"
    assert n2c["布伦特"] == "Brent"
    assert n2c["沙特阿美"] == "Aramco"
    assert n2c["胡塞"] == "Houthi"
    assert n2c["红海"] == "Red Sea"


def test_zh_uses_full_aramco_not_the_colliding_clip():
    # 沙特阿美 is the unambiguous form; the bare 阿美 (a common name / 阿美族) is
    # deliberately NOT a term — pin that so a future edit can't reintroduce it.
    natives = [nat for nat, _ in ZH_KEYWORDS]
    assert "沙特阿美" in natives
    assert "阿美" not in natives


def test_zh_sources_are_the_measured_trio():
    assert CHINESE_NO_RSS_DOMAINS == ("yicai.com", "finance.sina.com.cn", "jiemian.com")
    # rejected sources must not creep back in
    for banned in ("cnenergynews.cn", "caixin.com", "eastmoney.com"):
        assert banned not in CHINESE_NO_RSS_DOMAINS


# ============================================================================
# 2. Chinese GNews query shape (hl=zh-CN, when: BEFORE OR, native terms, cap)
# ============================================================================
def test_zh_query_shape():
    zh = LANGUAGES["zh"]
    urls = google_news_site_queries_lang(zh, ["yicai.com"], ["oil", "gás"], hours=168)
    assert len(urls) == 1
    url = urls[0]
    assert "hl=zh-CN" in url and "gl=CN" in url and "ceid=CN:zh-Hans" in url

    q = unquote_plus(parse_qs(urlparse(url).query)["q"][0])
    assert q.startswith("site:yicai.com when:7d (")
    assert q.index("when:") < q.index(" OR ")
    assert '"石油"' in q
    assert '"oil"' not in q                 # canonical is for matching, not retrieval
    assert '"gás"' not in q                 # live keyword arg ignored by the foreign resolver


def test_zh_query_respects_the_cap_of_12():
    zh = LANGUAGES["zh"]
    q = unquote_plus(parse_qs(urlparse(
        google_news_site_queries_lang(zh, ["yicai.com"], [], hours=24)[0]
    ).query)["q"][0])
    assert q.count(" OR ") == zh.cap - 1 == 11
    # a matching-only term beyond the cap (中石化 = Sinopec, #13) is NOT retrieved
    assert '"中石化"' not in q
    # but the first-slice terms ARE
    assert '"石油"' in q and '"中石油"' in q


# ============================================================================
# 3. source_lang tagging (§1.4)
# ============================================================================
def test_lang_from_query_url_recovers_zh():
    zh = LANGUAGES["zh"]
    zh_url = google_news_site_queries_lang(zh, ["yicai.com"], [], hours=24)[0]
    assert _lang_from_query_url(zh_url) == "zh"


# ============================================================================
# 4. Matching union + canonical rewrite (§2.3 / §2.4)
# ============================================================================
def test_chinese_substring_matches_inside_a_longer_word():
    # 石油 ("oil") is a substring of 石油公司 ("oil company"); no boundary needed.
    hits = matches_keywords("中国石油公司宣布新的天然气发现", _match_keywords(["oil"]))
    assert "石油" in hits
    assert "天然气" in hits


def test_union_lets_chinese_pass_and_rewrites_to_canonical():
    live = ["Petrobras", "oil", "gás"]
    hay = "霍尔木兹海峡紧张局势推高油价，伊朗威胁封锁原油出口"  # Hormuz tension lifts oil price; Iran threatens to block crude exports
    hits = matches_keywords(hay, _match_keywords(live))
    assert hits
    canon = _rewrite(hits)
    assert "Hormuz" in canon
    assert "Iran" in canon
    assert "crude" in canon
    # no raw Chinese term survives into matched_keywords
    assert all(not _has_cjk(k) for k in canon)


def test_zh_nested_terms_resolve_leftmost_longest():
    # 液化天然气 ("LNG") contains 天然气 ("gas"); the matcher is longest-first +
    # non-overlapping, so a pure-LNG title tags ONLY the most specific concept
    # (LNG), exactly like the PT gasolina>gas rule — NOT gas.
    canon = _rewrite(matches_keywords("中国液化天然气进口创新高", _match_keywords([])))
    assert canon == ["LNG"]
    # The shorter term co-fires only when it also appears in a SEPARATE position.
    canon2 = _rewrite(matches_keywords("天然气和液化天然气需求上升", _match_keywords([])))
    assert "gas" in canon2 and "LNG" in canon2
    # Same rule for 中石油 ("PetroChina") over 石油 ("oil").
    assert _rewrite(matches_keywords("中石油宣布新发现", _match_keywords([]))) == ["PetroChina"]


def test_union_adds_no_false_positive_to_latin_text():
    live = ["Petrobras", "oil"]
    text = "Petrobras raises diesel and oil prices"
    assert set(matches_keywords(text, live)) == set(matches_keywords(text, _match_keywords(live)))


# ============================================================================
# 5. Article overlay round-trips through the sync serializer (parity w/ ar)
# ============================================================================
def _article(source_lang, title, snippet=""):
    from news_hunter.store import Article
    return Article(
        url=f"https://yicai.com/{title}", domain="www.yicai.com",
        source_name="Yicai (第一财经)", title=title, snippet=snippet,
        published_at=datetime.now(timezone.utc), found_at=datetime.now(timezone.utc),
        matched_keywords=["oil"], source_lang=source_lang,
    )


def test_zh_overlay_serializes():
    from news_hunter.supabase_sync import _article_to_row

    a = _article("zh", "国际油价上涨")
    a.title_original = a.title
    a.title_en = "International oil prices rise"
    a.snippet_en = None
    row = _article_to_row(a)
    assert row["source_lang"] == "zh"
    assert row["title_original"] == a.title
    assert row["title_en"] == "International oil prices rise"
    assert row["title"] == a.title
