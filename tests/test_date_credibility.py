"""Unit tests for news_hunter/date_credibility.py (the Kpler re-stamp incident, 2026-09-25).

The fixtures are Kpler-shaped and, where it matters, literal: feed titles, <h1>
texts and JSON-LD blocks were captured from www.kpler.com/blog/rss.xml and the
article pages on 2026-09-25, the day its Webflow feed re-dated 81 of 100 old
posts to "today" while each page kept its own datePublished.

Run from repo root: python -m pytest tests/test_date_credibility.py -v
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import date_credibility as dc  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


def _kpler_page(h1: str, date_published: str, *, seo: str | None = None, extra_ld: str = "") -> str:
    """The shape of a Kpler blog page: one @graph with a BlogPosting + breadcrumbs."""
    ld = {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "BlogPosting",
                "headline": h1,
                "description": "",
                "datePublished": date_published,
                "dateModified": "Sep 25, 2026",
                "author": {"@type": "Person", "name": "Analyst"},
                "publisher": {"@type": "Organization", "name": "Kpler", "url": "https://www.kpler.com"},
                "articleBody": "",
            },
            {"@type": "BreadcrumbList", "itemListElement": [{"@type": "ListItem", "position": 1, "name": "Blog"}]},
        ],
    }
    return (
        f"<html><head><title>{seo or h1} {h1} | Kpler - {date_published}</title>"
        f'<meta content="{seo or h1}" property="og:title"/>'
        f'<meta property="og:type" content="website"/>'
        f'<script type="application/ld+json">\n\n{json.dumps(ld, indent=4)}\n</script>{extra_ld}'
        f'</head><body><h1 class="single-blog-heading">{h1}</h1><p>Body text.</p></body></html>'
    )


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# ---------------------------------------------------------------------------
# parse_date_value: only full calendar dates, never garbage
# ---------------------------------------------------------------------------

def test_parses_the_kpler_jsonld_date_as_a_whole_day():
    p = dc.parse_date_value("Apr 01, 2026", now=NOW)
    assert p is not None
    assert p.value == datetime(2026, 4, 1, tzinfo=UTC)
    assert p.date_only is True
    assert p.latest == datetime(2026, 4, 2, tzinfo=UTC)


@pytest.mark.parametrize("raw", [
    "", "   ", None, 12345, "2", "2026", "Sep 25", "garbage text 2026",
    "0001-01-01T00:00:00", "1970-01-01T00:00:00Z", "2031-01-01",
    "05/09/2026",   # 5 Sep in Brazil, 9 May to dateutil: refused, never guessed
    "x" * 80 + " 2026",
])
def test_rejects_empty_partial_ambiguous_and_implausible_values(raw):
    assert dc.parse_date_value(raw, now=NOW) is None


def test_unambiguous_day_first_numeric_date_is_read():
    p = dc.parse_date_value("25/09/2026", now=NOW)
    assert p is not None and p.value == datetime(2026, 9, 25, tzinfo=UTC)


def test_timezone_aware_timestamp_is_converted_to_utc():
    p = dc.parse_date_value("2026-04-01T14:02:58+02:00", now=NOW)
    assert p is not None
    assert p.value == datetime(2026, 4, 1, 12, 2, 58, tzinfo=UTC)
    assert p.date_only is False


def test_naive_timestamp_is_read_as_utc_and_keeps_its_time():
    p = dc.parse_date_value("2026-09-24 21:30:00", now=NOW)
    assert p is not None and p.value == datetime(2026, 9, 24, 21, 30, tzinfo=UTC)
    assert p.date_only is False


# ---------------------------------------------------------------------------
# R2: the earliest credible date wins, with a 24 h tolerance
# ---------------------------------------------------------------------------

def test_older_page_date_replaces_the_feed_date():
    feed = datetime(2026, 9, 25, 10, 58, 42, tzinfo=UTC)   # the re-publish stamp
    page = dc.parse_date_value("Apr 01, 2026", now=NOW)
    assert dc.is_older(page, feed)
    assert dc.credible_published(feed, page) == datetime(2026, 4, 1, tzinfo=UTC)


def test_page_date_within_tolerance_keeps_the_feed_date():
    feed = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    page = dc.parse_date_value("2026-09-24T14:00:00Z", now=NOW)   # 20 h earlier
    assert not dc.is_older(page, feed)
    assert dc.credible_published(feed, page) == feed


def test_a_later_page_date_never_pushes_an_item_forward():
    feed = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    page = dc.parse_date_value("2026-09-25T09:00:00Z", now=NOW)
    assert dc.credible_published(feed, page) == feed


def test_date_only_value_is_compared_by_the_end_of_its_day():
    # Kpler prints the day in US time: a post from late on the 24th (local)
    # carries "Sep 24, 2026" while its UTC feed date is already the 25th.
    feed = datetime(2026, 9, 25, 3, 30, tzinfo=UTC)
    page = dc.parse_date_value("Sep 24, 2026", now=NOW)
    assert not dc.is_older(page, feed)
    # ...but a day-only date two days back is older without any doubt.
    assert dc.is_older(dc.parse_date_value("Sep 22, 2026", now=NOW), feed)


def test_no_page_date_keeps_the_feed_date_and_no_feed_date_is_left_to_the_caller():
    feed = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    assert dc.credible_published(feed, None) == feed
    assert dc.credible_published(None, dc.parse_date_value("Apr 01, 2026", now=NOW)) is None


# ---------------------------------------------------------------------------
# page_published_date: the page's own date, never dateModified
# ---------------------------------------------------------------------------

def test_reads_kpler_graph_blogposting_and_never_date_modified():
    html = _kpler_page("How to build a risk tree to assess shadow fleet exposure in your network", "Apr 01, 2026")
    p = dc.page_published_date(_soup(html))
    assert p is not None
    assert p.value == datetime(2026, 4, 1, tzinfo=UTC)
    assert p.source == "jsonld:BlogPosting"


def test_empty_date_published_is_no_date_even_with_a_date_modified():
    # /blog/russian-barrels-to-fill-chinese-crude-stocks, 2026-09-25:
    # "datePublished": "" and "dateModified": "Sep 25, 2026".
    html = _kpler_page("Russian barrels to fill Chinese crude stocks?", "")
    assert dc.page_published_date(_soup(html)) is None


def test_meta_article_published_time_comes_first():
    html = (
        '<html><head><meta property="article:published_time" content="2026-09-25T08:00:00Z"/>'
        '<script type="application/ld+json">{"@type":"NewsArticle","datePublished":"2026-01-01"}</script>'
        "</head><body></body></html>"
    )
    p = dc.page_published_date(_soup(html))
    assert p is not None and p.value == datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    assert p.source == "meta:article:published_time"


def test_itemprop_date_published_is_read():
    html = '<html><head><meta itemprop="datePublished" content="2026-09-25T09:15:00-03:00"/></head></html>'
    p = dc.page_published_date(_soup(html))
    assert p is not None and p.value == datetime(2026, 9, 25, 12, 15, tzinfo=UTC)


def test_site_and_related_article_dates_are_never_the_page_date():
    # A WebSite's datePublished is the site launch; an ItemList of related
    # stories carries OTHER articles' dates. Neither may date this page.
    html = (
        '<html><head><script type="application/ld+json">'
        '[{"@type":"WebSite","datePublished":"2011-05-01"},'
        ' {"@type":"ItemList","itemListElement":[{"@type":"NewsArticle","datePublished":"2020-02-02"}]}]'
        "</script></head><body></body></html>"
    )
    assert dc.page_published_date(_soup(html)) is None


def test_webpage_entity_is_a_fallback_when_no_article_entity_exists():
    html = (
        '<html><head><script type="application/ld+json">'
        '{"@graph":[{"@type":"WebPage","datePublished":"2026-09-20T10:00:00Z"}]}'
        "</script></head></html>"
    )
    p = dc.page_published_date(_soup(html))
    assert p is not None and p.value == datetime(2026, 9, 20, 10, 0, tzinfo=UTC)


def test_broken_json_ld_still_yields_its_date_published():
    html = (
        '<html><head><script type="application/ld+json">'
        '{"@type":"NewsArticle","datePublished":"2026-03-01T10:00:00Z",}'
        "</script></head></html>"
    )
    p = dc.page_published_date(_soup(html))
    assert p is not None and p.value == datetime(2026, 3, 1, 10, 0, tzinfo=UTC)


def test_record_page_is_fail_soft(monkeypatch):
    def _boom(_soup):
        raise ValueError("unexpected markup")

    monkeypatch.setattr(dc, "page_published_date", _boom)
    ev = dc.record_page("https://example.com/a", _soup("<html></html>"))
    assert ev.page_date is None and ev.headlines == ()
    assert dc.page_seen("https://example.com/a") is ev


def test_page_headlines_reads_the_h1():
    html = _kpler_page("New pipelines bypassing the Strait of Hormuz could come online in 1-2 years", "Jul 10, 2026")
    assert dc.page_headlines(_soup(html)) == (
        "New pipelines bypassing the Strait of Hormuz could come online in 1-2 years",
    )


# ---------------------------------------------------------------------------
# T: clean titles, on real Kpler samples (feed title, page <h1>) of 2026-09-25
# ---------------------------------------------------------------------------

KPLER_TITLES = [
    # (feed title, page h1, expected display title)
    (
        "Gulf States Race to Build Hormuz Bypass Infrastructure New pipelines bypassing the Strait of "
        "Hormuz could come online in 1-2 years | Kpler - Jul 09, 2026",
        "New pipelines bypassing the Strait of Hormuz could come online in 1-2 years",
        "New pipelines bypassing the Strait of Hormuz could come online in 1-2 years",
    ),
    (
        "How to build a risk tree to assess shadow fleet exposure How to build a risk tree to assess "
        "shadow fleet exposure in your network | Kpler - Mar 31, 2026",
        "How to build a risk tree to assess shadow fleet exposure in your network",
        "How to build a risk tree to assess shadow fleet exposure in your network",
    ),
    (
        "Middle East Oil Outages Climb as Hormuz Transits Slow [UPDATE] Middle Eastern supply recovery "
        "postponed to early 2027 | Kpler - Jul 23, 2026",
        "[UPDATE] Middle Eastern supply recovery postponed to early 2027",
        "[UPDATE] Middle Eastern supply recovery postponed to early 2027",
    ),
    (
        "EIA Digest: US gasoline stocks hit year-to-date low  EIA Digest: US gasoline stocks hit "
        "year-to-date low as refiners tilt output towards middle distillates | Kpler -",
        "EIA Digest: US gasoline stocks hit year-to-date low as refiners tilt output towards middle distillates",
        "EIA Digest: US gasoline stocks hit year-to-date low as refiners tilt output towards middle distillates",
    ),
    (
        "Can Saudi Arabia keep its Red Sea oil exports flowing EXPLAINER: Can Saudi Arabia keep its Red "
        "Sea oil exports flowing without Bab el-Mandeb? | Kpler -",
        "EXPLAINER: Can Saudi Arabia keep its Red Sea oil exports flowing without Bab el-Mandeb?",
        "EXPLAINER: Can Saudi Arabia keep its Red Sea oil exports flowing without Bab el-Mandeb?",
    ),
    (
        "Crude tanker rates hit new highs as Hormuz risk escalates Crude tanker rates hit new highs as "
        "Hormuz risk escalates | Kpler -",
        "Crude tanker rates hit new highs as Hormuz risk escalates",
        "Crude tanker rates hit new highs as Hormuz risk escalates",
    ),
]


@pytest.mark.parametrize("feed_title,h1,expected", KPLER_TITLES)
def test_kpler_titles_become_the_page_headline(feed_title, h1, expected):
    assert dc.clean_title(feed_title, "Kpler", [h1]) == expected


def test_without_the_page_the_suffix_still_goes_and_a_doubled_title_collapses():
    assert dc.clean_title(
        "The Fed joins the hiking camp The Fed joins the hiking camp | Kpler -", "Kpler"
    ) == "The Fed joins the hiking camp"
    assert dc.clean_title(
        "Refining margins to remain supported through H2  Refining margins to remain supported through H2 "
        "| Kpler - Jun 25, 2026",
        "Kpler",
    ) == "Refining margins to remain supported through H2"
    # SEO title != headline and no page: only the suffix goes.
    assert dc.clean_title(KPLER_TITLES[0][0], "Kpler") == (
        "Gulf States Race to Build Hormuz Bypass Infrastructure New pipelines bypassing the Strait of "
        "Hormuz could come online in 1-2 years"
    )


def test_split_source_suffix_hands_back_the_printed_date():
    head, date = dc.split_source_suffix(KPLER_TITLES[0][0], "Kpler")
    assert head.endswith("could come online in 1-2 years")
    assert date is not None and date.value == datetime(2026, 7, 9, tzinfo=UTC) and date.date_only


def test_a_suffix_that_is_not_the_items_own_source_is_left_alone():
    assert dc.split_source_suffix("Oil prices rise | Kpler Insights", "Kpler") == (
        "Oil prices rise | Kpler Insights", None
    )
    assert dc.split_source_suffix("Oil prices rise | Kpler - Houston", "Kpler") == (
        "Oil prices rise | Kpler - Houston", None
    )


# Stored titles containing " | " from OTHER sources (news_articles, 2026-09-25;
# 525 such rows across 46 domains). The full set is re-checked by
# `python -m scripts.diagnose_date_credibility --titles`; these cover every
# shape in it: section labels, show names, bylines, tickers, CJK/Arabic series.
UNCHANGED = [
    ("The Wall Street Journal", "Opinion | Why California Diesel Is $8.35 a Gallon"),
    ("The Wall Street Journal", "Exclusive | Exxon Is Nearing Preliminary Deal to Invest in Venezuela’s Oil Fields"),
    ("CNN", "Saudi oil pipeline shut down after attack triggers fires | CNN Politics"),
    ("CNN", "Are you cutting more spending amid ongoing high gas prices? Share your story | CNN Business"),
    ("NHK World", "Saudi pipeline repair may take weeks | NHK WORLD-JAPAN News"),
    ("Sky News", "Oil and gas prices fuel rise in inflation | Paul Kelso analysis"),
    ("The Guardian", "Trump’s love of fossil fuels is about more than money | Moira Donegan"),
    ("Bloomberg", "Watch Oil Nears $100 After Attacks Hit Saudi Energy Facilities | The Pulse 9/8/2026"),
    ("Bloomberg", "Watch US Retail Gasoline Hits Four-Year High; Yen Gains | Bloomberg Brief 09/03/2026"),
    ("Investing.com", "Análises das Ações PETR4 | Petrobras PN - Investing.com Brasil"),
    ("Investing.com", "OMC - Ações OMC Tankers DRC | Cotação Hoje - Investing.com Brasil"),
    ("Reuters", "OIL.AX - | Stock Price & Latest News"),
    ("Moneycontrol", "Live: Choppy day for markets, oil above $92; Rupee recovers to 94.8/$ | Closing Bell"),
    ("Moneycontrol", "Vedanta Oil and Gas | Splits > Refineries > Dividends declared by Vedanta Oil and Gas - BSE: 544782, NSE: VOGL"),
    ("Business Day (South Africa)", "WATCH | Inflation expectations ease despite oil price risks"),
    ("Correio Braziliense", "ANP endurece regras para o biometano | Eco Braziliense"),
    ("eixos", "EXCLUSIVO | Fronteira da Margem Equatorial já está aberta, desafio agora é a logística, diz Sylvia Anjos"),
    ("Times Brasil", "TIMES | CNBC Parlatório Talks: Brasil virou estratégico para a Shell e responde por quase 30% da produção global, diz presidente"),
    ("R7", "Podcast JR 15 Min #1465 | Estreito de Ormuz: a crise que pode mexer no bolso dos brasileiros"),
    ("Valor Econômico", "Calendário do Brasil na Copa do Mundo 2026 | Todas as possíveis datas e horários dos jogos da Seleção"),
    ("Barron's", "Warren Buffett, Stock Market, Oil Prices | September 18 Barron’s Daily"),
    ("S&P Global Commodity Insights", "The Pipeline: M&A and IPO Insights | S.4 Ep. 5 - How hybrid capital can unlock value in a slow middle-market M&A market"),
    ("Lloyd's List", "The week in charts: Houthis seek to reassure shipping after Mokha seizure | Suez Canal recovery gathers pace"),
    ("MEES", "Chevron Moves Closer To Finalizing Iraq Oil Field Deal |..."),
    ("Wood Mackenzie", "The South Atlantic’s next exploration chapter |"),
    ("Ahram Online", "INTERVIEW| Yemen, Horn of Africa, Sahel tensions pose growing challenges for Egypt - Politics - Egypt"),
    ("The Times", "Mechanical Design Engineer| Gas Turbine Components | Derby/ Bristol"),
    ("Sina Finance (新浪财经)", "港股异动 | 石油股尾盘涨幅进一步扩大 中东局势迅速升温 上海原油较布伦特已转为正溢价"),
    ("Sina Finance (新浪财经)", "天蝎座油轮三艘成品油轮锁定长期租约 | 航运界"),
    ("Yicai (第一财经)", "美国对伊朗经济制裁再度加码 中东局势将何去何从？| 夜话"),
    ("Al Arabiya", "مرصد هرمز | 15 مليون برميل تعبر بدعم أميركي وتسهيلات لناقلات العراق"),
    ("Asharq Business", "نفط فنزويلا قد يتضاعف بأكثر من مرتين خلال سنوات قليلة 🔴 تابعوا الشرق بلومبرغ للمزيد | الشرق للأخبار"),
    ("CBC News", "N.W.T residents feeling strain of rising home heating oil costs as winter looms | The Trailbreaker | On Demand"),
]


@pytest.mark.parametrize("source,title", UNCHANGED)
def test_other_sources_titles_with_a_pipe_do_not_change(source, title):
    assert dc.clean_title(title, source) == title
    # Worst case for the h1 rule: the page <h1> is any " | " segment of the
    # title. A label before the separator must survive.
    segments = [s.strip() for s in title.split("|") if s.strip()]
    assert dc.clean_title(title, source, segments) == title


@pytest.mark.parametrize("source,title,expected", [
    ("Jornal do Comércio",
     "'Ormuz não deve servir a chantagem', diz Macron em último discurso na ONU | Jornal do Comércio",
     "'Ormuz não deve servir a chantagem', diz Macron em último discurso na ONU"),
    ("CBC News",
     "N.W.T residents feeling strain of rising home heating oil costs | CBC News",
     "N.W.T residents feeling strain of rising home heating oil costs"),
])
def test_the_items_own_source_name_suffix_is_removed(source, title, expected):
    # The only stored titles outside kpler.com the rule touches: 176 Jornal do
    # Comercio rows and 1 CBC row, all "<headline> | <own source name>".
    assert dc.clean_title(title, source) == expected


def test_h1_is_not_used_when_the_title_does_not_end_with_it():
    title = "Oil prices rise as Hormuz transits slow"
    assert dc.clean_title(title, "Reuters", ["Something else entirely here"]) == title
    # A two-word h1 is too short to be trusted as a headline.
    assert dc.clean_title("Weekly outlook Oil prices", "X", ["Oil prices"]) == "Weekly outlook Oil prices"


# ---------------------------------------------------------------------------
# R3 helpers
# ---------------------------------------------------------------------------

def test_restamp_uses_min_of_published_and_first_seen():
    first_seen = datetime(2026, 9, 14, 20, 36, tzinfo=UTC)
    # published_at already pushed forward by an earlier re-stamp: created_at
    # still exposes the next one.
    stored = dc.StoredDates(published_at=datetime(2026, 9, 25, 10, 43, 54, tzinfo=UTC), created_at=first_seen)
    assert dc.restamps("u", datetime(2026, 9, 25, 10, 43, 54, tzinfo=UTC), stored)
    # A feed date within the tolerance of the first sighting is not evidence.
    fresh = dc.StoredDates(published_at=first_seen, created_at=first_seen + timedelta(minutes=3))
    assert not dc.restamps("u", first_seen + timedelta(hours=20), fresh)
    assert not dc.restamps("u", datetime(2026, 9, 25, tzinfo=UTC), None)


def _epochs(*secs: int) -> list[datetime]:
    return [datetime.fromtimestamp(s, UTC) for s in secs]


def test_batch_rule_on_the_measured_sequences():
    # news_articles_kpler_restamp_20260925_bak: the six stored posts re-dated
    # 10:41:49 .. 10:45:08 on 2026-09-25.
    kpler = _epochs(1790332909, 1790332994, 1790333034, 1790333041, 1790333087, 1790333108)
    assert dc.largest_batch(kpler) == 6 and dc.is_batch(kpler)
    # investing.com's four "live levels" pages, re-dated together (09-18).
    investing = _epochs(1789759113, 1789759120, 1789759122, 1789759126)
    assert dc.is_batch(investing)
    # estadao, clamp backup: six unrelated updates in 2.2 h -- never three in
    # ten minutes (closest pair 6 minutes apart), three within 27 minutes.
    estadao = _epochs(1788213264, 1788214539, 1788214905, 1788216237, 1788218794, 1788221054)
    assert dc.largest_batch(estadao) == 2 and not dc.is_batch(estadao)
    assert dc.largest_batch(estadao, timedelta(minutes=30)) == 3
    # one or two re-dates are never a batch, whatever their spacing
    assert not dc.is_batch(kpler[:2]) and not dc.is_batch([]) and not dc.is_batch(kpler[:1])


def test_challenge_pages_are_recognised():
    cloudflare = (
        "<html><head><title>Just a moment...</title>"
        '<script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1"></script></head>'
        "<body><h1>www.example.com</h1></body></html>"
    )
    datadome = (
        '<html><head><title>example.com</title></head><body>'
        '<iframe src="https://geo.captcha-delivery.com/captcha/?initialCid=x"></iframe></body></html>'
    )
    assert dc.looks_like_challenge(_soup(cloudflare))
    assert dc.looks_like_challenge(_soup(datadome))
    for title in ("Attention Required! | Cloudflare", "Access denied",
                  "Access denied | www.example.com used Cloudflare to restrict access",
                  "Request unsuccessful. Incapsula incident ID: 123-456", "Pardon Our Interruption"):
        assert dc.looks_like_challenge(_soup(f"<html><head><title>{title}</title></head></html>")), title
    ev = dc.record_page("https://example.com/blocked", _soup(cloudflare))
    assert ev.challenge is True and ev.page_date is None


def test_articles_are_never_taken_for_challenges():
    kpler = _kpler_page("Russian barrels to fill Chinese crude stocks?", "")
    assert not dc.looks_like_challenge(_soup(kpler))
    headline = ("<html><head><title>Access denied: the week Iran closed the Strait of Hormuz to every "
                "tanker</title></head><body><h1>Access denied</h1></body></html>")
    assert not dc.looks_like_challenge(_soup(headline))
    # a dated page is never judged at all, whatever it embeds
    dated = ('<html><head><title>Just a moment...</title><meta property="article:published_time" '
             'content="2026-09-25T08:00:00Z"/></head></html>')
    assert dc.record_page("https://example.com/dated", _soup(dated)).challenge is False


# ---------------------------------------------------------------------------
# A source's own name is not a keyword on its own domain
# ---------------------------------------------------------------------------

def test_own_name_registry_is_explicit_and_only_kpler():
    from news_hunter import keyword_senses as ks

    assert ks.SOURCE_OWN_NAME_KEYWORDS == {"kpler.com": frozenset({"kpler"})}
    assert ks.own_name_keywords("kpler.com") == {"kpler"}
    assert ks.own_name_keywords("www.kpler.com") == {"kpler"}
    assert ks.drop_own_name(["Kpler", "LNG", "Hormuz"], "kpler.com") == ["LNG", "Hormuz"]
    assert ks.drop_own_name(["kpler"], "www.kpler.com") == []
    # NOT generic: every Petrobras item on the Petrobras agency is ours.
    assert ks.drop_own_name(["Petrobras"], "agencia.petrobras.com.br") == ["Petrobras"]
    assert ks.drop_own_name(["Kpler"], "reuters.com") == ["Kpler"]


def test_keep_candidate_ignores_kpler_on_kpler_com():
    from news_hunter.fetcher import RawItem
    from news_hunter.pipeline import LEDE_RESCUE_MARKER, _keep_candidate

    kws = ["Kpler", "LNG", "Hormuz", "crude"]
    now = datetime.now(UTC)
    marketing = RawItem(
        url="https://kpler.com/blog/how-to-choose-ship-tracking-software-for-your-business",
        title="How to choose ship tracking software for your business How to choose the right ship "
              "tracking software for your business | Kpler - Mar 22, 2026",
        summary="This guide explains how ship tracking works and how to choose the right solution.",
        published_at=now - timedelta(hours=1), source_domain="kpler.com", feed_domain="www.kpler.com",
    )
    drops: dict[str, int] = {}
    assert _keep_candidate(marketing, kws, 24, set(), allow_lede_rescue=True,
                           own_name_drops=drops) == [LEDE_RESCUE_MARKER]
    assert drops == {"kpler.com": 1}
    lng = RawItem(
        url="https://kpler.com/blog/hormuz-risk-and-winter-restocking-keep-lng-bid",
        title="Hormuz risk and winter restocking keep LNG bid Hormuz risk and winter restocking keep "
              "LNG bid | Kpler - Sep 09, 2026",
        summary="", published_at=now - timedelta(hours=1), source_domain="kpler.com",
        feed_domain="www.kpler.com",
    )
    assert _keep_candidate(lng, kws, 24, set(), own_name_drops=drops) == ["Hormuz", "LNG"]
    assert drops == {"kpler.com": 1}


def test_rotate_budget_gives_the_head_its_half_and_rotates_the_rest():
    items = [f"i{n}" for n in range(20)]
    seen: set[str] = set()
    for bucket in range(5):
        sel, over = dc.rotate_budget(items, 8, bucket=bucket)
        assert len(sel) == 8 and len(over) == 12
        assert sel[:4] == items[:4]           # newest four every time
        assert set(sel) | set(over) == set(items)
        seen.update(sel)
    assert seen == set(items)                  # nobody starves across scans
    assert dc.rotate_budget(items[:3], 8, bucket=7) == (items[:3], [])


def test_stored_timestamps_parse_postgrest_iso_strings():
    assert dc.parse_stored_timestamp("2026-09-14T20:36:12.446198+00:00") == datetime(
        2026, 9, 14, 20, 36, 12, 446198, tzinfo=UTC
    )
    assert dc.parse_stored_timestamp("2026-09-25T10:43:54+00:00") == datetime(2026, 9, 25, 10, 43, 54, tzinfo=UTC)
    assert dc.parse_stored_timestamp(None) is None
    assert dc.parse_stored_timestamp("not a date") is None


def test_sink_lookup_returns_both_dates_and_none_on_failure():
    from news_hunter import supabase_sync

    class _Q:
        def __init__(self, rows, fail):
            self.rows, self.fail = rows, fail

        def select(self, cols):
            assert cols == "url, published_at, created_at"
            return self

        def in_(self, _col, urls):
            self.urls = set(urls)
            return self

        def execute(self):
            if self.fail:
                raise RuntimeError("PostgREST down")
            return type("R", (), {"data": [r for r in self.rows if r["url"] in self.urls]})

    def _sink(rows, fail=False):
        s = supabase_sync._SupabaseSink.__new__(supabase_sync._SupabaseSink)
        s.client = type("C", (), {"table": lambda _self, _n: _Q(rows, fail)})()
        s.table = "news_articles"
        return s

    row = {"url": "https://kpler.com/blog/x", "published_at": "2026-09-25T10:43:54+00:00",
           "created_at": "2026-09-14T20:36:12.446198+00:00"}
    got = _sink([row])._existing_dates(["https://kpler.com/blog/x", "https://kpler.com/blog/y"])
    assert set(got) == {"https://kpler.com/blog/x"}
    assert got["https://kpler.com/blog/x"].reference == datetime(2026, 9, 14, 20, 36, 12, 446198, tzinfo=UTC)
    assert _sink([row], fail=True)._existing_dates(["https://kpler.com/blog/x"]) is None

    # One transient failure (the runner saw `ConnectionTerminated`) is retried.
    calls = {"n": 0}

    class _Flaky(_Q):
        def execute(self):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("ConnectionTerminated")
            return super().execute()

    flaky = supabase_sync._SupabaseSink.__new__(supabase_sync._SupabaseSink)
    flaky.client = type("C", (), {"table": lambda _self, _n: _Flaky([row], False)})()
    flaky.table = "news_articles"
    assert set(flaky._existing_dates(["https://kpler.com/blog/x"])) == {"https://kpler.com/blog/x"}
    assert calls["n"] == 2


# ---------------------------------------------------------------------------
# R2 inside enrich_item: whenever the page is fetched, its date is read
# ---------------------------------------------------------------------------

def test_enrich_item_prefers_the_older_page_date(monkeypatch):
    from news_hunter import enrich
    from news_hunter.fetcher import RawItem

    url = "https://kpler.com/blog/how-to-build-a-risk-tree-to-assess-shadow-fleet-exposure-in-your-network"
    html = _kpler_page("How to build a risk tree to assess shadow fleet exposure in your network", "Apr 01, 2026")
    monkeypatch.setattr(enrich, "fetch_html", lambda u, timeout=6: html)
    feed_date = datetime.now(UTC) - timedelta(hours=1)
    item = RawItem(url=url, title="How to build a risk tree", summary="", published_at=feed_date,
                   source_domain="kpler.com", feed_domain="www.kpler.com")
    _snippet, published, *_ = enrich.enrich_item(item, need_snippet=True)
    assert published == datetime(2026, 4, 1, tzinfo=UTC)
    # ...and the page was recorded for the rest of the scan.
    ev = dc.page_seen(url)
    assert ev is not None and ev.headlines == ("How to build a risk tree to assess shadow fleet exposure in your network",)


def test_enrich_item_keeps_the_feed_date_when_the_page_has_none(monkeypatch):
    from news_hunter import enrich
    from news_hunter.fetcher import RawItem

    html = _kpler_page("Russian barrels to fill Chinese crude stocks?", "")
    monkeypatch.setattr(enrich, "fetch_html", lambda u, timeout=6: html)
    feed_date = datetime.now(UTC) - timedelta(hours=1)
    item = RawItem(url="https://kpler.com/blog/russian-barrels-to-fill-chinese-crude-stocks", title="t",
                   summary="", published_at=feed_date, source_domain="kpler.com", feed_domain="www.kpler.com")
    _snippet, published, *_ = enrich.enrich_item(item, need_snippet=True)
    assert published == feed_date


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
