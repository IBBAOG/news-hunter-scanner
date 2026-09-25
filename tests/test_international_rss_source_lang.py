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
# Outlets whose ARTICLE host is not the host their feed is served from. The set
# is keyed on the host of the ITEM LINK, so these hosts MUST be registered even
# though they are not RSS_FEEDS keys — that is the whole point of the
# 2026-09-14 fix. Measured on 2026-09-14 by fetching each feed and reading the
# link host of every entry (counts in the sources.py comments).
_ARTICLE_HOST_ALIASES: dict[str, str] = {
    "businessday.co.za": "www.businesslive.co.za",
    "energyconnects.com": "www.pipelineoilandgasnews.com",
    "trend.az": "en.trend.az",
}

# Outlets whose RSS_FEEDS entry is PAUSED (commented out) while they stay in
# INTERNATIONAL_RSS_DOMAINS on purpose, so their articles keep landing 'en' and
# their Wave 5 item pin below stays live. apex -> the paused RSS_FEEDS key.
# kpler.com, 2026-09-25: its Webflow feed re-stamps <pubDate> on every re-publish.
_PAUSED_FEEDS: dict[str, str] = {
    "kpler.com": "www.kpler.com",
}


def test_every_entry_maps_to_an_rss_feeds_registration():
    rss_norm = {_strip_www(k) for k in RSS_FEEDS}
    for d in INTERNATIONAL_RSS_DOMAINS:
        apex = _strip_www(d)
        if apex in _ARTICLE_HOST_ALIASES:
            # An article host: its FEED must still be a registration.
            feed = _ARTICLE_HOST_ALIASES[apex]
            assert feed in RSS_FEEDS, f"{d} claims to be the article host of {feed}, which is not registered"
            continue
        if apex in _PAUSED_FEEDS:
            assert _PAUSED_FEEDS[apex] not in RSS_FEEDS, f"{d}: feed is back, drop it from _PAUSED_FEEDS"
            continue
        assert apex in rss_norm, f"{d} is not a registered RSS feed"


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


# ============================================================================
# 4b. The guard that was missing: a REAL item URL from every Wave 5 feed
#
# INTERNATIONAL_RSS_DOMAINS is keyed on the host of the ITEM LINK, which is not
# always the host the feed is served from. Registering only the feed host is
# silent: the outlet keeps being fetched and every article lands source_lang=
# NULL, i.e. NATIONAL. It happened to three of the 42 Wave 5 feeds and was only
# caught by eyeballing production rows (businessday.co.za 8 rows,
# energyconnects.com 6, trend.az 2 — all English, all filed as Brazilian).
#
# Each URL below is a REAL item link, taken from the feed itself on 2026-09-14
# (bangkokpost from Supabase — its feed answers HTTP 451 outside Thailand).
# They are frozen strings on purpose: a stale URL is fine, the test never
# fetches anything, it only runs the link through the same _entry_to_item the
# fetcher uses.
# ============================================================================
_WAVE5_ITEM_URLS: tuple[tuple[str, str], ...] = (
    # --- Wave 5A ---
    ("www.ft.com", "https://www.ft.com/content/4845a503-3ddd-4bb7-a469-c51f275bc873?syn-25a6b1a6=1"),
    ("www.theguardian.com", "https://www.theguardian.com/business/2026/sep/14/oil-prices-rise-drone-attacks-saudi-arabia-east-west-pipeline"),
    ("www.cbsnews.com", "https://www.cbsnews.com/live-updates/iran-war-us-trump-oil-strait-of-hormuz/"),
    # --- Wave 5B ---
    ("www.investing.com", "https://www.investing.com/news/commodities-news/diplomacy-stumbles-with-postponement-of-meeting-on-strait-of-hormuz-proposal-4898762"),
    ("www.france24.com", "https://www.france24.com/en/nigerian-oil-refinery-dangote-aims-to-raise-1-6-billion-dollars"),
    ("www.lemonde.fr", "https://www.lemonde.fr/en/international/article/2026/09/14/vladimir-putin-a-president-sealed-off-in-a-russia-under-lockdown_6757522_4.html"),
    ("www.irishtimes.com", "https://www.irishtimes.com/business/2026/09/14/i-thought-i-was-going-to-die-aer-lingus-pilot-on-flight-he-claims-left-him-with-brain-injury/"),
    ("www.cityam.com", "https://www.cityam.com/ministers-poised-to-nationalise-second-steel-firm/"),
    ("financialpost.com", "https://financialpost.com/globe-newswire/primaris-reit-announces-200-million-equity-offering"),
    ("calgaryherald.com", "https://calgaryherald.com/business/local-business/is-less-construction-affecting-affordability-gains-calgary"),
    ("www.abc.net.au", "https://www.abc.net.au/news/2026-09-15/gambling-authority-looks-to-offload-clubgrants-pokies-tax-scheme/107065894"),
    ("www.afr.com", "https://www.afr.com/rear-window/richard-white-s-abandoned-aldi-supermarket-gets-sold-20260913-p60wv3?ref=rss&utm_medium=rss&utm_source=rss_markets"),
    # --- Wave 5C (the first two are the misclassification cases) ---
    ("www.businesslive.co.za", "https://www.businessday.co.za/world/international-companies/2026-09-14-ai-safety-warnings-spark-tech-stock-slide/"),
    ("www.pipelineoilandgasnews.com", "https://www.energyconnects.com/news/oil/2026/september/trump-announces-russia-ukraine-energy-truce-kyiv-says-not-yet/"),
    ("www.middleeasteye.net", "https://www.middleeasteye.net/news/houthi-attacks-expose-gaps-pakistans-defence-pact-saudi-arabia"),
    ("www.tehrantimes.com", "https://www.tehrantimes.com/news/530029/Iran-says-it-is-no-longer-afraid-of-confronting-US-or-Israel"),
    ("www.dailysabah.com", "https://www.dailysabah.com/business/economy/norway-mulls-jail-terms-for-trade-with-illegal-israeli-settlements"),
    ("www.koreaherald.com", "https://www.koreaherald.com/article/10873094"),
    # --- Wave 5D (en.trend.az is the third misclassification case) ---
    ("www.straitstimes.com", "https://www.straitstimes.com/business/companies-markets/global-ai-stocks-fall-as-industry-chiefs-call-for-slowing-development"),
    ("www.channelnewsasia.com", "https://www.channelnewsasia.com/sport/boufal-embarks-new-adventure-libyas-al-ittihad-6384371"),
    ("www.bangkokpost.com", "https://www.bangkokpost.com/business/general/3319469/nigerian-oil-refinery-launches-possible-largest-ipo-in-africa"),
    ("e.vnexpress.net", "https://e.vnexpress.net/news/business/companies/tesla-of-world-s-richest-person-elon-musk-sets-up-vietnam-company-5120126.html"),
    ("timesofindia.indiatimes.com", "https://timesofindia.indiatimes.com/business/india-business/four-homebuyers-paid-rs-2-08-crore-each-for-flats/articleshow/134243422.cms"),
    ("economictimes.indiatimes.com", "https://economictimes.indiatimes.com/industry/healthcare/biotech/pharmaceuticals/centre-proposes-easier-norms-for-faster-drug-rollout/articleshow/134247112.cms"),
    ("www.business-standard.com", "https://www.business-standard.com/markets/news/regulatory-changes-hit-derivatives-volumes-daily-contracts-fall-23-in-aug-126091401052_1.html"),
    ("www.dawn.com", "https://www.dawn.com/news/2029913/govt-increases-petrol-by-rs442-per-litre-hsd-by-rs610-per-litre-amid-rising-global-oil-prices"),
    ("astanatimes.com", "https://astanatimes.com/2026/09/kashagan-expansion-could-reshape-kazakhstans-oil-export-needs-experts-say/"),
    ("en.trend.az", "https://www.trend.az/casia/uzbekistan/4223720.html"),
    # en.mercopress.com moved to GNews on 2026-09-14 (feed 100% off-beat), so it
    # is no longer an RSS feed and has no pinned item URL here.
    ("www.batimes.com.ar", "https://www.batimes.com.ar/news/latin-america/lula-and-bolsonaro-in-dead-heat-ahead-of-brazil-vote-polls-show.phtml"),
    ("mexiconewsdaily.com", "https://mexiconewsdaily.com/news/pemex-contains-another-gulf-oil-spill/"),
    # --- Wave 5E ---
    ("www.lngindustry.com", "https://www.lngindustry.com/floating-lng/14092026/kanata-clean-power-and-hanwha-ocean-to-launch-pre-feed-for-proposed-floating-lng-project/"),
    ("www.worldpipelines.com", "https://www.worldpipelines.com/contracts-and-tenders/14092026/baker-hughes-and-venture-global-advance-next-phase-of-us-gas-infrastructure-growth/"),
    ("www.tanksterminals.com", "https://www.tanksterminals.com/terminals/14092026/wood-secures-long-term-construction-services-contract-with-exxonmobil-png/"),
    ("drillingcontractor.org", "https://drillingcontractor.org/touchstone-mobilizes-rig-for-wd-4-drilling-campaign-in-trinidad-79950"),
    ("rbnenergy.com", "https://rbnenergy.com/daily-posts/blog/us-refiners-already-running-hard-relief-diesel-remains-elusive"),
    ("www.fuelsandlubes.com", "https://www.fuelsandlubes.com/arteco-coolant-wins-nvidia-qualification-for-data-centres/"),
    ("www.energymonitor.ai", "https://www.energymonitor.ai/analyst-comment/europe-policies-offshore-wind-as-renewable-power/"),
    ("www.shipandbunker.com", "https://shipandbunker.com/news/apac/791714-owners-no-longer-fear-bunker-supply-shortage-despite-ongoing-hormuz-disruption"),
    ("www.seatrade-maritime.com", "https://www.seatrade-maritime.com/containers/gemini-brings-four-services-back-to-red-sea-suez-route"),
    ("www.kpler.com", "https://www.kpler.com/blog/crude-tanker-rates-hit-new-highs-as-hormuz-risk-escalates"),
    ("www.eia.gov", "https://www.eia.gov/todayinenergy/detail.php?id=68125"),
    # Promoted from GNews to RSS on 2026-09-14:
    # intellinews's feed answers 403 to everything except the GitHub runner, so
    # this URL is a real article taken from production rows (the GNews route
    # captured it) rather than from the feed; globalenergynetwork's is the first
    # item of its feed, fetched 2026-09-14 (apex host, not www).
    ("www.intellinews.com", "https://www.intellinews.com/uganda-appoints-vitol-to-market-pearl-sweet-crude-ahead-of-2027-exports-466128"),
    ("globalenergynetwork.net", "https://globalenergynetwork.net/news-item/wood-secures-long-term-construction-services-contract-with-exxonmobil-png/"),
)


def _wave5_rss_feed_keys() -> set[str]:
    """RSS_FEEDS keys registered inside a `Wave 5x ... BEGIN/END` block.

    Read from the source text, so a feed added to a wave block without a pinned
    item URL below fails the coverage test instead of shipping unguarded.
    """
    import re
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "news_hunter" / "sources.py"
    keys: set[str] = set()
    inside_dict = inside_wave = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("RSS_FEEDS"):
            inside_dict = True
            continue
        if inside_dict and line.startswith("}"):
            break
        if not inside_dict:
            continue
        if re.match(r"\s*# --- Wave 5[A-E].*BEGIN", line):
            inside_wave = True
            continue
        if re.match(r"\s*# --- Wave 5[A-E].*END", line):
            inside_wave = False
            continue
        m = re.match(r'\s*"([^"]+)":\s*\[', line)
        if m and inside_wave:
            keys.add(m.group(1))
    return keys


def test_every_wave5_feed_has_a_pinned_item_url():
    # A paused feed keeps its pin (still run by the 'en' test below) but is no
    # longer a registration, so it is left out of the comparison.
    pinned = {feed for feed, _ in _WAVE5_ITEM_URLS} - set(_PAUSED_FEEDS.values())
    registered = _wave5_rss_feed_keys()
    assert registered, "wave-5 anchor parsing found no RSS_FEEDS keys"
    assert pinned == registered, (
        "pinned item URLs out of sync with the Wave 5 RSS registrations — "
        f"missing: {sorted(registered - pinned)}; stale: {sorted(pinned - registered)}"
    )


def test_a_real_item_url_of_every_wave5_feed_is_tagged_en():
    """The businessday.co.za guard: run one REAL link per feed through the
    fetcher's own item builder and demand 'en'. A feed whose articles live on
    another host fails here instead of in the dashboard."""
    misclassified = []
    for feed_domain, url in _WAVE5_ITEM_URLS:
        it = _entry_to_item(_entry(url), feed_domain)
        if it.source_lang != "en":
            misclassified.append((feed_domain, it.source_domain, url))
    assert not misclassified, (
        "these feeds' articles would be classified as NATIONAL — register the "
        "ARTICLE host (apex + www) in INTERNATIONAL_RSS_DOMAINS: "
        + "; ".join(f"{feed} -> {host}" for feed, host, _ in misclassified)
    )


def test_the_three_known_article_host_aliases_are_registered():
    """Pins the 2026-09-14 production bug by host, not only by URL."""
    for article_host in _ARTICLE_HOST_ALIASES:
        assert article_host in INTERNATIONAL_RSS_DOMAINS
        assert f"www.{article_host}" in INTERNATIONAL_RSS_DOMAINS


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
