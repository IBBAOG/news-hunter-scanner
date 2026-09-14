"""International English RSS sources — source_lang stamping (national/international).

`source_lang` is the discriminator the dashboard's News Hunter national/
international selector relies on:

    international <=> source_lang in {en, ar, ru, zh, iw, es}
    national      <=> source_lang in {pt, None}

Foreign + English GNews-en items are tagged on the per-language `site:` route
(lang_by_url in iter_collect/collect). The international ENGLISH outlets that
live in RSS_FEEDS are fetched through the plain RSS path, which does NOT go
through that stamping — so fetcher._entry_to_item stamps them 'en' from
INTERNATIONAL_RSS_DOMAINS. These tests pin that guard, prove the Brazilian
default (None) is untouched, prove the GNews / ENGLISH_NO_RSS tagging is
unchanged, and prove 'en' is excluded from translation (zero extra work).

Run from repo root: python -m pytest tests/test_international_rss_source_lang.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import translate as translate_mod  # noqa: E402
from news_hunter.fetcher import _entry_to_item  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    ENGLISH_NO_RSS_DOMAINS,
    INTERNATIONAL_RSS_DOMAINS,
    LANGUAGES,
    RSS_FEEDS,
)
from news_hunter.store import Article  # noqa: E402


def _strip_www(d: str) -> str:
    return d[4:] if d.startswith("www.") else d


def _entry(link: str, title: str = "Oil prices climb", summary: str = "body") -> dict:
    """Minimal feedparser-style entry (dict supports the .get() path)."""
    return {"link": link, "title": title, "summary": summary}


# ============================================================================
# 1. The fetch-path guard: an international RSS item is stamped 'en'
# ============================================================================
def test_apex_international_rss_item_is_tagged_en():
    it = _entry_to_item(
        _entry("https://oilprice.com/Latest-Energy-News/World-News/x.html"),
        "oilprice.com",
    )
    assert it.source_domain == "oilprice.com"
    assert it.source_lang == "en"


def test_www_feed_resolves_to_apex_and_is_tagged_en():
    # The feed lives on www; normalize_url strips it, so source_domain is the
    # apex form — which is also in the set. Proves the www outlets are covered.
    it = _entry_to_item(
        _entry("https://www.hellenicshippingnews.com/some-lng-story/"),
        "www.hellenicshippingnews.com",
    )
    assert it.source_domain == "hellenicshippingnews.com"
    assert it.source_lang == "en"


def test_subdomain_source_is_tagged_en():
    # ET EnergyWorld has no www form — it is a subdomain; normalize_url leaves it.
    it = _entry_to_item(
        _entry("https://energy.economictimes.indiatimes.com/news/oil-and-gas/x"),
        "energy.economictimes.indiatimes.com",
    )
    assert it.source_domain == "energy.economictimes.indiatimes.com"
    assert it.source_lang == "en"


# ============================================================================
# 2. Everything else stays None (the Brazilian / native default)
# ============================================================================
def test_brazilian_rss_item_stays_none():
    it = _entry_to_item(
        _entry("https://g1.globo.com/economia/noticia/petrobras.ghtml"),
        "g1.globo.com",
    )
    assert it.source_domain == "g1.globo.com"
    assert it.source_lang is None


def test_english_named_brazilian_domain_stays_none():
    # theagribiz.com has an English name but is a Brazilian (pt) outlet — it must
    # NOT be tagged 'en'. Guards against inclusion-by-name.
    it = _entry_to_item(_entry("https://www.theagribiz.com/agro/x/"), "www.theagribiz.com")
    assert it.source_domain == "theagribiz.com"
    assert it.source_lang is None


def test_generic_unregistered_rss_item_stays_none():
    it = _entry_to_item(_entry("https://example.com/whatever/story"), "example.com")
    assert it.source_lang is None


# ============================================================================
# 3. The GNews-en / foreign path is UNCHANGED by the RSS guard
# ============================================================================
def test_gnews_english_no_rss_item_is_not_stamped_by_the_rss_guard():
    # A Google News item resolves to its publisher via <source href>; that domain
    # is an ENGLISH_NO_RSS one (reuters), which is NOT in INTERNATIONAL_RSS_DOMAINS,
    # so _entry_to_item leaves source_lang None — it is stamped 'en' LATER by the
    # lang_by_url loop in iter_collect/collect. The RSS guard must not touch it.
    entry = {
        "link": "https://news.google.com/rss/articles/CBM123",
        "title": "Oil rises on Hormuz risk - Reuters",
        "summary": "",
        "source": {"href": "https://www.reuters.com"},
    }
    it = _entry_to_item(entry, "news.google.com")
    assert it.source_domain == "www.reuters.com"
    assert it.source_lang is None


def test_set_is_disjoint_from_gnews_english_and_foreign_domains():
    # The RSS roster and the GNews-tagged rosters never overlap: each domain is
    # tagged by exactly one mechanism.
    assert INTERNATIONAL_RSS_DOMAINS.isdisjoint(ENGLISH_NO_RSS_DOMAINS)
    for cfg in LANGUAGES.values():
        assert INTERNATIONAL_RSS_DOMAINS.isdisjoint(cfg.no_rss_domains), cfg.code


# ============================================================================
# 4. Roster integrity — every entry is a real RSS_FEEDS registration
# ============================================================================
def test_every_entry_maps_to_an_rss_feeds_registration():
    rss_norm = {_strip_www(k) for k in RSS_FEEDS}
    for d in INTERNATIONAL_RSS_DOMAINS:
        assert _strip_www(d) in rss_norm, f"{d} is not a registered RSS feed"


# The 15 international-English RSS outlets registered before the 2026-09
# expansion (the four 2026-08-18 waves plus CNBC and The Moscow Times, which
# predate the programme). This is a FLOOR, not a snapshot: waves ADD outlets, and
# five parallel branches all editing one exact-equality set literal would collide
# on every merge and leave the suite red in between. What must never happen is
# LOSING one — an outlet dropped from here silently reverts to being classified
# as national, with no error anywhere.
_PRE_EXPANSION_APEXES = frozenset({
    "oilprice.com", "oedigital.com", "gcaptain.com", "splash247.com",
    "hellenicshippingnews.com", "lngprime.com", "cnbc.com",
    "oilandgas360.com", "naturalgasintel.com", "offshore-energy.biz",
    "themoscowtimes.com", "thenationalnews.com",
    "energy.economictimes.indiatimes.com", "thehindubusinessline.com",
    "livemint.com",
})


def test_no_pre_expansion_rss_outlet_was_lost():
    apexes = {_strip_www(d) for d in INTERNATIONAL_RSS_DOMAINS}
    missing = _PRE_EXPANSION_APEXES - apexes
    assert not missing, (
        f"dropped from INTERNATIONAL_RSS_DOMAINS: {sorted(missing)} — these feeds "
        "would keep being fetched and would silently be classified as NATIONAL"
    )
    assert len(_PRE_EXPANSION_APEXES) == 15
    # The two that predated the 2026-08-18 waves.
    assert "cnbc.com" in apexes and "themoscowtimes.com" in apexes


def test_every_www_entry_has_its_apex_twin():
    """Both forms of a host are listed, at any roster size.

    _entry_to_item resolves source_domain per item and, although normalize_url
    strips a leading "www.", other item-construction paths may not — so a host
    listed in only one form is tagged only sometimes. (ET EnergyWorld is
    subdomain-only and has no www form, hence the one-directional check.)
    """
    for d in INTERNATIONAL_RSS_DOMAINS:
        if d.startswith("www."):
            assert d[4:] in INTERNATIONAL_RSS_DOMAINS, f"{d} listed without its apex"


def test_no_brazilian_domain_leaked_into_the_set():
    for pt_domain in (
        "g1.globo.com", "valor.globo.com", "www.cnnbrasil.com.br",
        "www.theagribiz.com", "theagribiz.com", "clickpetroleoegas.com.br",
        "eixos.com.br", "www1.folha.uol.com.br",
    ):
        assert pt_domain not in INTERNATIONAL_RSS_DOMAINS


# ============================================================================
# 5. Translation exclusion — tagging 'en' adds ZERO work
# ============================================================================
def _article(source_lang, title="Oil prices climb", snippet="body"):
    return Article(
        url=f"https://x/{title}/{source_lang}", domain="oilprice.com",
        source_name="OilPrice", title=title, snippet=snippet,
        published_at=datetime.now(timezone.utc), found_at=datetime.now(timezone.utc),
        matched_keywords=["oil"], source_lang=source_lang,
    )


def test_translate_stage_never_translates_an_en_item(monkeypatch):
    import news_hunter.pipeline as pipe

    # Only Arabic would translate under this stub; en/pt/None must be skipped.
    monkeypatch.setattr(
        translate_mod, "translate_to_en",
        lambda text, src: ("EN:" + text) if src == "ar" else None,
    )

    en = _article("en", title="Brent slips as OPEC meets")
    pt = _article("pt", title="Petrobras reajusta diesel")
    none = _article(None, title="Preco")
    ar = _article("ar", title="أسعار النفط ترتفع")

    n = pipe._run_translation([en, pt, none, ar], errors=[])

    assert n == 1                       # only the Arabic row was translated
    assert ar.title_en == "EN:أسعار النفط ترتفع"
    # The 'en' international-RSS row is left entirely alone: no translation, and
    # not even a title_original stamp (that is a foreign-only side effect).
    for other in (en, pt, none):
        assert other.title_en is None
        assert other.snippet_en is None
        assert other.title_original is None
