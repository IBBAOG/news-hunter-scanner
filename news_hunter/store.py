"""Stateless shim sobre Supabase. Substitui o SQLite local do Clipinator.

O scanner cloud e efemero - nenhum estado persistido localmente. Toda a
dedup e cache vem do Supabase:

  - news_articles.url (PRIMARY KEY)      -> dedupe automatico via UPSERT
  - news_hunter_default_keywords          -> keywords-padrao globais (com match_type)
  - news_hunter_keywords (UNION de users) -> keywords per-user (com match_type)

Mantemos as mesmas exports que pipeline.py importa para nao precisar
refatorar a pipeline: Article, normalize_url, get_config, start_run,
finish_run, get_cached_snippets, upsert_articles.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .config import DEFAULT_KEYWORDS, DEFAULT_WINDOW_HOURS
from . import supabase_sync

log = logging.getLogger(__name__)


TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src",
    "__twitter_impression",
    # Added 2026-09-11 from the duplicate audit of news_articles: every one of
    # these produced 2+ rows for one article, and none identifies the article.
    "traffic_source",           # aljazeera ?traffic_source=rss
    "srnd",                     # bloomberg ?srnd=phx-industries
    "ysclid",                   # Yandex click id (neftegaz)
    ".tsrc",                    # offshore-technology ?.tsrc=rss
    "__hstc", "__hssc", "__hsfp",  # HubSpot (tradewinds)
    "zephr_sso_ott",            # one-time SSO token (tradewinds/upstream)
}

# AMP switches carried in the query string: dropped only for these exact values,
# so e.g. an `outputType=json` elsewhere is left alone.
_AMP_QUERY_VALUES = {
    "amp": {"", "1", "true"},        # globalenergynetwork ?amp=1
    "outputtype": {"amp"},           # bloomberglinea ?outputType=amp
    "ampmode": {"1", "true"},        # investing ?ampMode=1
}

# Host-specific tracking keys: generic enough to be noise ON THAT HOST, too
# generic to strip everywhere (`source`, `module`, `chan` can be real ids on
# another site). Matched on the host or any subdomain of it.
_HOST_TRACKING_PARAMS: dict[str, frozenset[str]] = {
    "rigzone.com": frozenset({"rss"}),
    "intellinews.com": frozenset({"source"}),
    "reuters.com": frozenset({"chan"}),
    "cnn.com": frozenset({"cid", "iid", "recs_exp", "tenant_id"}),
    "scmp.com": frozenset({"module", "pgtype", "tpcc", "uuid"}),
}

# Hosts whose ARTICLE pages carry nothing but tracking in the query string. Sina
# Finance appends a different `?cre=tianyi&mod=pchp&loc=NN&rfunc=NN...` for every
# homepage slot that links the story, plus `?finpagefr=p_108` from the section
# page — one headline was stored under 8 urls (2026-09-11), each translated
# separately. Only static article paths (.shtml/.html) are stripped; a dynamic
# page such as stock.finance.sina.com.cn/.../paper.php?reportid=... keeps its id.
_DROP_QUERY_ON_ARTICLE_PATH: tuple[str, ...] = ("sina.com.cn",)
_ARTICLE_PATH = re.compile(r"\.s?html?$", re.IGNORECASE)

# Quintype's AMP scheme (gulfnews.com/amp/story/<encoded path>) has no plain
# counterpart at the same path, so the /amp/ prefix is only collapsed elsewhere.
_AMP_PREFIX_KEEP = ("/amp/story/",)

# Hosts that serve ONLY on the `www.` name: stripping the prefix (what this
# module does for every other host) produces a url that CANNOT BE FETCHED by
# anything. Measured from the runner on 2026-09-15 — `https://<apex>/` fails for
# all five while `https://www.<apex>/` answers 200 with the page:
#
#   lngindustry.com, worldpipelines.com, tanksterminals.com,
#   hydrocarbonengineering.com  -> SSLError (Palladian Publications serves the
#       four titles from one www vhost; the certificate does not carry the apex
#       name, so TLS fails before HTTP)
#   rivieramm.com               -> ConnectTimeout (the apex answers nothing on
#       443 at all)
#
# The stored url is NOT only a key: the dashboard's clipping generator GETs it,
# and so do the scanner's own body fetches (enrich -> lede rescue -> snippet
# backfill, all of which pass Article.url / RawItem.url straight to fetch_html).
# An apex url is therefore a row that can never gain a body: all 8 rivieramm rows
# in news_articles had snippet = '' when this landed (measured 2026-09-15), and
# ex_auto on the www page yields 22 paragraphs.
#
# PRIMARY-KEY NOTE: news_articles is url-keyed, so this constant CHANGES the
# primary key for these five hosts. The apex rows already stored (35 on
# 2026-09-15 — lngindustry 13, hydrocarbonengineering 8, rivieramm 8,
# tanksterminals 3, worldpipelines 3) keep their apex url, and a re-scan of the
# same article lands a SECOND row under the www url. Re-keying the existing rows
# is a separate Supabase task (an UPDATE on news_articles.url); nothing in the
# scanner runs it, on purpose — this module never writes SQL.
WWW_ONLY_HOSTS: frozenset[str] = frozenset({
    "lngindustry.com",
    "worldpipelines.com",
    "tanksterminals.com",
    "hydrocarbonengineering.com",
    "rivieramm.com",
})


def _canonical_netloc(netloc: str) -> str:
    """Strip a leading `www.`, except on the hosts that only exist WITH it.

    Both directions matter: the www form must survive normalisation (the feeds
    hand over www links) AND an apex form arriving from somewhere else — a
    Google News resolve, a hand-typed url, a row written before this existed —
    must be lifted to www, or the two spellings would be two rows of which one
    is unfetchable. Only the exact apex and its www form are affected; a real
    subdomain (news.rivieramm.com) is left alone, as everywhere else in here.
    """
    if netloc.startswith("www."):
        apex = netloc[4:]
        return netloc if apex in WWW_ONLY_HOSTS else apex
    if netloc in WWW_ONLY_HOSTS:
        return f"www.{netloc}"
    return netloc


def _host_matches(netloc: str, host: str) -> bool:
    return netloc == host or netloc.endswith("." + host)


def _deamp_path(path: str) -> str:
    """Collapse an AMP mirror path onto the canonical article path.

    Verified against news_articles on 2026-09-11 — for every host below the
    de-AMP'd path was itself stored as a row, i.e. both are the same article:
      * prefix  /amp/<path>  -> /<path>   alarabiya, asharqbusiness, aljazeera,
        neftegaz, portafolio, theedgesingapore, eleconomista
      * suffix  <path>/amp   -> <path>    tass (/economy/2186047/amp),
        arabnews (/node/2656503/amp)
    """
    lower = path.lower()
    if lower.startswith("/amp/") and not lower.startswith(_AMP_PREFIX_KEEP):
        path = path[4:]
    elif lower.endswith("/amp") and len(path) > len("/amp"):
        path = path[: -len("/amp")]
    elif lower.endswith("/amp/") and len(path) > len("/amp/"):
        path = path[: -len("/amp/")]
    return path


# Host path rewrites: the collector is handed a url that addresses a NON-ARTICLE
# surface of an article the same site serves under the SAME SLUG somewhere else.
# Rewriting here (rather than dropping the item) keeps the article and makes the
# key the fetchable address.
#
# mobilityplaza.com — Google News hands back `/recommend/<slug>`, which is the
# site's "send this recommendation" share FORM, while the article itself is
# `/news/<slug>`. Measured from the runner on 2026-09-15:
#
#   https://mobilityplaza.com/recommend/argentina-shell-could-sell-its-600-gas-station-network
#       -> 200, 17 KB, ZERO paragraphs over 40 chars
#   https://www.mobilityplaza.com/news/argentina-shell-could-sell-its-600-gas-station-network
#       -> 200, 21 KB, 4 body paragraphs; _extract() reads the title
#          "Argentina: Shell could sell its 600 gas station network"
#
# Six of the seven mobilityplaza rows stored in news_articles were /recommend/
# (2026-09-15) — i.e. the outlet was landing bodiless almost every time.
_PATH_REWRITES: tuple[tuple[str, str, str], ...] = (
    ("mobilityplaza.com", "/recommend/", "/news/"),
)


def _rewrite_path(netloc: str, path: str) -> str:
    for host, old, new in _PATH_REWRITES:
        if path.startswith(old) and _host_matches(netloc, host):
            return new + path[len(old):]
    return path


def _keep_param(netloc: str, key: str, value: str) -> bool:
    k = key.lower()
    if k in TRACKING_PARAMS or k.startswith("utm_"):
        return False
    amp_values = _AMP_QUERY_VALUES.get(k)
    if amp_values is not None and value.lower() in amp_values:
        return False
    for host, keys in _HOST_TRACKING_PARAMS.items():
        if k in keys and _host_matches(netloc, host):
            return False
    return True


def _route_fragment(fragment: str) -> str:
    """Keep a fragment only when it IS the page address (hash-routed SPA).

    A normal fragment (`#top`, `#comments`) points inside one page and is
    dropped. But S&P Global's Platts portal routes every article through the
    fragment — `core.spglobal.com/#platts/insightsArticle?articleID=<uuid>` —
    so there the fragment is the only thing telling 371 stored articles apart
    (measured 2026-09-11; it is the only domain in news_articles with a
    fragment at all). Dropping it would fold all of them onto one url, and the
    dedupe job would DELETE 370 distinct articles. Kept when the fragment
    carries its own query string, or is a `#/` or `#!` route.
    """
    if not fragment:
        return ""
    if "?" in fragment or fragment.startswith(("/", "!")):
        return fragment
    return ""


# =============================================================================
# Non-article url shapes (added 2026-09-15)
# =============================================================================
# Surfaces that are NOT a news article but which the GNews route keeps resolving
# to and the pipeline kept persisting: a job board, topic hubs, video players and
# a syndicated-wire wrapper. Each one is a permanent row with no body — the
# clipping generator returns nothing for it, and the feed shows a headline that
# leads to a page with no article.
#
# This is the only host-scoped url filter in the scanner; keep new cases HERE
# rather than adding a second mechanism. (fetcher._NON_ARTICLE_SEGMENTS is a
# different job: it prunes listing/category links while SCRAPING a homepage, and
# never sees an RSS or a Google News url.)
#
# Format: key -> (netloc regex, path-prefix regex or None). The netloc regex is
# fullmatched (so a rule cannot leak onto another host), the path regex is
# matched from the start of the path; both are case-insensitive. A None path
# excludes the whole host. Row counts below are news_articles on 2026-09-15 —
# the audit that produced this list.
EXCLUDED_URL_PATTERNS: dict[str, tuple[str, str | None]] = {
    # The Times' JOB BOARD, not the newspaper. Google files its listings under
    # the outlet, so they arrive named "The Times" and carry ZERO <p>. 8 rows.
    "thetimes-appointments": (r"appointments\.thetimes\.com", None),
    # AP topic hubs (/hub/oil-and-gas): a perpetually-updated index page, no
    # article, no date of its own. 1 row.
    "apnews-topic-hub": (r"(?:www\.)?apnews\.com", r"/hub/"),
    # Video players: the "body" is a transcript-less player shell. 3 + 1 rows.
    "cbsnews-video": (r"(?:www\.)?cbsnews\.com", r"/video/"),
    # AP's video desk. Its own shape, not a hub: the /hub/ rule was written
    # against the one row stored at the time and an apnews.com/video/ row landed
    # at 17:05Z on 2026-09-15, hours after the rule shipped. Counted separately
    # from the hub so the two never hide behind one number.
    "apnews-video": (r"(?:www\.)?apnews\.com", r"/video/"),
    # USA Today serves the video desk under both spellings; the stored row uses
    # the plural, and `videos?` costs nothing against the day the singular
    # arrives - this is one surface with two urls, not two phenomena.
    "usatoday-video": (r"(?:www\.)?usatoday\.com", r"/videos?/"),
    # /embed/video/<id>: the EMBEDDABLE player for the same clip, and the only
    # live usatoday.com row that reads as a singular "/video/". The path rules
    # are anchored at the start of the path, so neither video rule could ever
    # have seen it. 1 row. `/embed/` and not `/embed/video/`: nothing a
    # publisher offers for embedding is the article.
    "usatoday-embed-player": (r"(?:www\.)?usatoday\.com", r"/embed/"),
    # NPR's embeddable audio player, not the story page. 1 row.
    "npr-player-embed": (r"(?:www\.)?npr\.org", r"/player/"),
    # Sky's video desk — 8 of the 9 stored news.sky.com rows.
    "skynews-video": (r"news\.sky\.com", r"/video/"),
    # The Globe and Mail republishes Newswire.ca press releases inside a
    # JS-rendered wrapper (/investing/markets/markets-news/Newswire.ca/<id>);
    # the served HTML holds no prose. 3 rows.
    "globeandmail-newswire": (r"(?:www\.)?theglobeandmail\.com", r"/.*/Newswire\.ca/"),
}

_EXCLUDED_URL_RULES: tuple[tuple[str, re.Pattern[str], re.Pattern[str] | None], ...] = tuple(
    (
        key,
        re.compile(host_re, re.IGNORECASE),
        re.compile(path_re, re.IGNORECASE) if path_re else None,
    )
    for key, (host_re, path_re) in EXCLUDED_URL_PATTERNS.items()
)


def excluded_url_reason(url: str) -> str | None:
    """Return the EXCLUDED_URL_PATTERNS key this url matches, or None.

    Returns the KEY rather than a bool so the caller can log which rule fired:
    a silent drop is how a rule that starts eating real articles stays invisible.
    """
    try:
        p = urlparse(url)
    except ValueError:
        return None
    netloc = p.netloc.lower()
    if not netloc:
        return None
    path = p.path or "/"
    for key, host_re, path_re in _EXCLUDED_URL_RULES:
        if not host_re.fullmatch(netloc):
            continue
        if path_re is None or path_re.match(path):
            return key
    return None


def normalize_url(url: str) -> str:
    """Canonical form of an article url, used as the news_articles primary key.

    Removes in-page fragments, tracking params, AMP mirrors and 'www.' so the
    same article reached through different links is ONE row — and therefore one
    translation, not one per link variant. A hash-route fragment that
    identifies the page is kept (see _route_fragment), the five hosts that only
    answer on www keep (or regain) the prefix (see WWW_ONLY_HOSTS), and a
    non-article surface of an article served under the same slug is rewritten
    onto the article (see _PATH_REWRITES).
    """
    try:
        p = urlparse(url)
    except ValueError:
        return url
    netloc = _canonical_netloc(p.netloc.lower())
    path = _rewrite_path(netloc, _deamp_path(p.path))
    if any(_host_matches(netloc, h) for h in _DROP_QUERY_ON_ARTICLE_PATH) and _ARTICLE_PATH.search(path):
        query: list[tuple[str, str]] = []
    else:
        query = [
            (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if _keep_param(netloc, k, v)
        ]
    return urlunparse((
        p.scheme, netloc, path.rstrip("/") or path, p.params, urlencode(query),
        _route_fragment(p.fragment),
    ))


@dataclass
class Article:
    url: str
    domain: str
    source_name: str
    title: str
    snippet: str
    published_at: datetime | None
    found_at: datetime
    matched_keywords: list[str] = field(default_factory=list)
    # True quando published_at foi FABRICADO (now() como carimbo de primeira
    # descoberta, ou clamp de timestamp futuro) em vez de lido da fonte.
    # supabase_sync trata esses como write-once: nunca sobrescrevem a data de
    # uma linha que ja existe. Re-aplicar um now() a cada scan e exatamente o
    # que fazia um artigo de 6 dias atras flutuar no topo do feed como "13m ago".
    published_is_approx: bool = False
    # --- Multilingual overlay (§4) ------------------------------------------
    # `title`/`snippet` always keep their as-scraped, NATIVE meaning. The four
    # fields below are display overlays for FOREIGN items (source_lang not in
    # en/pt/None); the frontend renders `title_en ?? title`. All None for PT/EN
    # rows, so the English/PT write path is unchanged except source_lang.
    source_lang: str | None = None      # 'ar'/'ru'/'zh'/'iw'/'es'/'en'/'pt'; None = legacy/native
    title_original: str | None = None   # native title as scraped (foreign only)
    title_en: str | None = None         # English translation (None on failure — never drops the row)
    snippet_en: str | None = None       # English snippet translation (foreign only)

    @property
    def published_iso(self) -> str | None:
        return self.published_at.isoformat() if self.published_at else None


def _fetch_default_keywords(sink) -> dict:
    """Fetch global default keywords with match_type from Supabase.

    Calls RPC get_default_news_keywords_with_flags() which returns rows of
    (keyword text, match_type text).  Falls back to direct table SELECT if the
    RPC call fails (e.g. scanner deployed before the RPC was created).

    Returns a dict mapping keyword -> match_type ('substring' | 'exact').
    Empty dict on any failure (caller merges with per-user keywords).
    """
    if sink.client is None:
        return {}
    try:
        res = sink.client.rpc("get_default_news_keywords_with_flags").execute()
        rows = res.data or []
        result: dict = {}
        for r in rows:
            kw = r.get("keyword")
            mt = r.get("match_type") or "substring"
            if kw:
                result[kw] = mt
        log.info("default keywords loaded via RPC: %d entries", len(result))
        return result
    except Exception as rpc_err:  # noqa: BLE001
        log.info(
            "get_default_news_keywords_with_flags RPC failed (%s) -- falling back to direct table SELECT",
            rpc_err,
        )
    try:
        res = sink.client.table("news_hunter_default_keywords").select(
            "keyword, match_type"
        ).execute()
        rows = res.data or []
        result = {}
        for r in rows:
            kw = r.get("keyword")
            mt = r.get("match_type") or "substring"
            if kw:
                result[kw] = mt
        log.info("default keywords loaded via direct table: %d entries", len(result))
        return result
    except Exception as tbl_err:  # noqa: BLE001
        log.warning("default keywords table SELECT also failed: %s", tbl_err)
        return {}


def get_config() -> dict:
    """Returns {'keywords': [...], 'exact_keywords': {...}, 'window_hours': 24}.

    Keywords are the UNION of:
      1. Global defaults from news_hunter_default_keywords (via RPC
         get_default_news_keywords_with_flags, with match_type per row).
      2. Per-user keywords from news_hunter_keywords (UNION of all users,
         with match_type per row).

    match_type aggregation rule: if the same keyword appears in multiple
    sources/users with different match_types, the result is promoted to
    'exact' (conservative: fewer false positives).

    The exact_keywords set contains every keyword whose effective
    match_type is 'exact'.  The keywords list contains all keywords
    regardless of match_type (consumed by the filter as the full set).

    Fallback to DEFAULT_KEYWORDS (all substring, hardcoded in config.py) when:
      - Supabase is not configured (local dev)
      - Both default-keyword fetches fail
      - Both tables are empty

    Accent handling (critical fix -- 2026-05-26):
    Keywords are stored in the DB with their canonical form (accents
    preserved).  The scanner must NOT strip diacritics before matching
    because that turns the Iran keyword (with tilde-n) into 'ira', causing it
    to hit 'diretoria', 'irma-with-tilde', etc. -- generating thousands of
    false positives.  filter.py now applies re.IGNORECASE directly on the
    original text without any NFD normalisation.  The DB ships BOTH
    'petroleo' and 'petroleo-accented' as separate entries for sources that
    omit accents.
    """
    sink = supabase_sync.get_sink()
    if sink.client is None:
        return {
            "keywords": list(DEFAULT_KEYWORDS),
            "exact_keywords": set(),
            "window_hours": DEFAULT_WINDOW_HOURS,
        }

    # Aggregated map: keyword -> effective match_type ('substring' | 'exact').
    # 'exact' wins over 'substring' when the same keyword appears in both.
    aggregated: dict = {}

    # --- Source 1: global default keywords ---
    for kw, mt in _fetch_default_keywords(sink).items():
        if mt == "exact":
            aggregated[kw] = "exact"
        else:
            aggregated.setdefault(kw, "substring")

    # --- Source 2: per-user keywords (UNION of all authenticated users) ---
    try:
        try:
            res = sink.client.table("news_hunter_keywords").select(
                "keyword, match_type"
            ).execute()
        except Exception as col_err:  # noqa: BLE001
            # Likely column "match_type" does not exist if scanner deployed before
            # the migration that added the column. Fall back to keyword-only SELECT.
            log.info(
                "select(keyword,match_type) failed (%s) -- falling back to select(keyword)",
                col_err,
            )
            res = sink.client.table("news_hunter_keywords").select("keyword").execute()
        rows = res.data or []
        for r in rows:
            kw = r.get("keyword")
            if not kw:
                continue
            mt = r.get("match_type") or "substring"
            if mt == "exact":
                aggregated[kw] = "exact"
            else:
                aggregated.setdefault(kw, "substring")
    except Exception as e:  # noqa: BLE001
        log.warning("per-user keywords fetch failed: %s", e)

    if not aggregated:
        log.info("All keyword sources empty -- using DEFAULT_KEYWORDS (all substring)")
        return {
            "keywords": list(DEFAULT_KEYWORDS),
            "exact_keywords": set(),
            "window_hours": DEFAULT_WINDOW_HOURS,
        }

    kws = sorted(aggregated)
    exact: set = {kw for kw, mt in aggregated.items() if mt == "exact"}

    log.info(
        "keywords loaded: total=%d exact=%d substring=%d",
        len(kws),
        len(exact),
        len(kws) - len(exact),
    )
    return {
        "keywords": kws,
        "exact_keywords": exact,
        "window_hours": DEFAULT_WINDOW_HOURS,
    }


def get_cached_snippets(urls) -> dict:
    """Stateless: always returns empty dict.

    The cloud container does not persist state -- each scan re-enriches all
    candidates.  Cost: fetch_html on up to ~50 URLs/scan (ENRICH_CAP).
    With a 30 s interval and 24 workers this stays well within budget.
    """
    return {}


def existing_dates(urls):
    """Passthrough to supabase_sync.existing_dates: stored published_at +
    created_at per url, and the urls whose lookup failed (StoredLookup)."""
    return supabase_sync.existing_dates(list(urls))


def urls_with_snippet(urls) -> set:
    """Passthrough para supabase_sync.urls_with_snippet (ver docstring de la).

    Devolve set() quando o Supabase nao esta configurado (modo local) e None
    quando a consulta falha — quem chama distingue os dois: set() vazio quer
    dizer "nada gravado, pode buscar tudo"; None quer dizer "nao sei", e a fase
    de backfill entao se limita aos itens mais novos em vez de chutar.
    """
    return supabase_sync.urls_with_snippet(list(urls))


def start_run() -> int:
    """No-op: no runs table in Supabase.  Returns 0 as a placeholder."""
    return 0


def finish_run(run_id: int, n_found: int, errors) -> None:
    """No-op: no runs table.  Stats are emitted to stdout by the service."""
    return


def upsert_articles(articles) -> int:
    """Batch-push to Supabase.  Returns number of rows sent.

    Unlike the original SQLite implementation that returned n_new (rows that
    did not previously exist), we return n_pushed because in stateless mode
    we do not know what already exists before the UPSERT.  Operationally
    equivalent for monitoring purposes.
    """
    if not articles:
        return 0
    try:
        return supabase_sync.push_new(articles)
    except Exception as e:  # noqa: BLE001
        log.warning("upsert_articles failed: %s", e)
        return 0
