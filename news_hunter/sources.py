"""Registro de feeds RSS por dominio + queries Google News.

Cobertura: feeds RSS publicos quando existem. Sites sem RSS ou com RSS
instavel sao cobertos via Google News search (site:dominio + keyword).
"""
from __future__ import annotations

from urllib.parse import quote_plus

# -----------------------------------------------------------------------------
# RSS feeds por dominio.
# Chave = dominio canonico (sem www); pode ter multiplos feeds (economia, gerais).
# Quando um site tem RSS com janela curta (so ultimos 20 items), tudo bem:
# o filtro de data descarta o resto.
# -----------------------------------------------------------------------------

RSS_FEEDS: dict[str, list[str]] = {
    # Imprensa geral
    # Grupo Globo: usamos Google News Sitemap (urlset com news:news) que e
    # atualizado em tempo real e sempre funciona (RSS tradicional quebrou).
    "valor.globo.com": [
        "https://valor.globo.com/sitemap/valor/news.xml",
    ],
    "g1.globo.com": [
        "https://g1.globo.com/rss/g1/economia/",
        "https://g1.globo.com/rss/g1/",
    ],
    "oglobo.globo.com": [
        "https://oglobo.globo.com/sitemap/oglobo/news.xml",
    ],
    "pipelinevalor.globo.com": [
        "https://pipelinevalor.globo.com/sitemap/pipelinevalor/news.xml",
    ],
    "globorural.globo.com": [
        "https://globorural.globo.com/sitemap/globorural/news.xml",
    ],
    "cbn.globo.com": [
        "https://cbn.globo.com/sitemap/cbn/news.xml",
    ],
    "www.estadao.com.br": [
        "https://www.estadao.com.br/arc/outboundfeeds/news-sitemap/?outputType=xml",
    ],
    "einvestidor.estadao.com.br": [
        "https://einvestidor.estadao.com.br/post/sitemap-news-1.xml",
    ],
    "www1.folha.uol.com.br": [
        "https://feeds.folha.uol.com.br/mercado/rss091.xml",
        "https://feeds.folha.uol.com.br/emcimadahora/rss091.xml",
    ],
    "noticias.uol.com.br": [
        "https://rss.uol.com.br/feed/noticias.xml",
    ],
    # UOL Economia: own domain, economy-focused feed — most relevant for
    # oil & gas / fuel / Petrobras coverage. Feed serves ~15 items per poll
    # with proper published_at timestamps (ISO-8859-1 XML, feedparser handles it).
    "economia.uol.com.br": [
        "https://rss.uol.com.br/feed/economia.xml",
    ],
    "www.terra.com.br": [
        "https://www.terra.com.br/noticias/rss.xml",
    ],
    "www.metropoles.com": [
        "https://www.metropoles.com/feed",
    ],
    # R7 removeu feed RSS publico — coberto via Google News site:
    # "noticias.r7.com": [],
    "www.correiobraziliense.com.br": [
        "https://www.correiobraziliense.com.br/sitemap-news.xml",
    ],
    # Correio do Povo (Porto Alegre/RS). Atex Polopoly CMS: no RSS at all
    # (/rss, /feed, /rss.xml all 404) but it publishes a proper Google News
    # sitemap at /sitemap_news.xml (urlset + news:news with title and
    # publication_date), ~245 items covering the last ~3 days. The generic
    # /sitemap.xml is a sitemapindex of MONTHLY polopoly_fs files whose URLs
    # carry a rotating asset id — not stable enough to hardcode; the news
    # sitemap path is stable. Caveat: <loc> carries a RAW accented path
    # segment ("/notícias/..."), which requests percent-encodes on fetch.
    "www.correiodopovo.com.br": [
        "https://www.correiodopovo.com.br/sitemap_news.xml",
    ],
    "veja.abril.com.br": [
        "https://veja.abril.com.br/feed",
    ],
    "www.cnnbrasil.com.br": [
        "https://www.cnnbrasil.com.br/feed/",
        # /economia/feed/ retorna 404 desde 2025 — feed geral ja cobre economia
    ],
    "operamundi.uol.com.br": [
        "https://operamundi.uol.com.br/feed",
    ],
    # Conjur (Consultor Juridico): the RSS feed was healthy and productive for
    # months (~25 articles/week, full snippets) until 2026-07-27 ~21:50 UTC,
    # when Cloudflare started serving a Managed Challenge ("Just a moment...",
    # HTTP 403, cf-mitigated: challenge) to this feed. The block is ZONE-WIDE —
    # /rss.xml, /feed, /rss, /atom.xml, /sitemap.xml, /sitemap_news.xml and the
    # bare homepage all answer the same 403, on the apex and on www alike — so
    # article bodies are unreachable too and there is no alternate feed URL to
    # move to. Unlike Monitor Mercantil, this one is NOT a datacenter-IP
    # reputation gate: a residential connection is challenged exactly the same,
    # and curl_cffi browser impersonation fails from both, so nothing short of a
    # JS-executing browser clears it (neither HOMEPAGE_SCRAPERS nor a
    # residential runner would help). Covered via Google News site: below.
    # Re-test any time with:
    #   gh workflow run diagnose_feed.yml -f url=https://www.conjur.com.br/rss.xml
    # and uncomment this if it ever answers 200 again.
    # "www.conjur.com.br": [
    #     "https://www.conjur.com.br/rss.xml",
    # ],

    # Economia / Mercado
    "www.infomoney.com.br": [
        "https://www.infomoney.com.br/feed/",
    ],
    "www.bloomberglinea.com.br": [
        "https://www.bloomberglinea.com.br/arc/outboundfeeds/rss/?outputType=xml",
    ],
    "braziljournal.com": [
        "https://braziljournal.com/feed/",
    ],
    "investnews.com.br": [
        "https://investnews.com.br/feed/",
    ],
    "neofeed.com.br": [
        "https://neofeed.com.br/feed/",
    ],
    # exame.com is a Next.js site whose WordPress-style /feed/ only emits ~25
    # latest items from a curated subset of sections — Revista Exame and other
    # editorial verticals are NOT included. The Google News sitemap at
    # /noticias/sitemap.xml carries ~370 items including /revista-exame/* and
    # is the canonical discovery surface (see Compass biometano gap, 2026-05-27).
    "exame.com": [
        "https://exame.com/feed/",
        "https://exame.com/noticias/sitemap.xml",
    ],
    "www.moneytimes.com.br": [
        "https://www.moneytimes.com.br/feed/",
    ],
    # IstoE Dinheiro: feed retorna 200 mas corpo vazio — coberto via Google News
    # "istoedinheiro.com.br": [],
    "www.cnbc.com": [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    ],
    # Reuters encerrou todos os feeds RSS oficialmente em 2020 — coberta via Google News
    # "www.reuters.com": [],

    # Energia / Oil & Gas
    # Brasil Energia: subscriber paywall (ASP.NET Core). No public RSS; covered
    # by the authenticated homepage scraper (HOMEPAGE_SCRAPERS below), which logs
    # in via news_hunter.brasilenergia_auth and fetches full article bodies.
    # "www.brasilenergia.com.br": [],
    "eixos.com.br": [
        "https://eixos.com.br/feed/",
    ],
    # Agencia Petrobras usa Liferay — sem feed RSS/sitemap publico — coberta via Google News
    # "agencia.petrobras.com.br": [],
    "clickpetroleoegas.com.br": [
        "https://clickpetroleoegas.com.br/feed/",
    ],
    "megawhat.uol.com.br": [
        "https://megawhat.uol.com.br/feed/",
    ],
    "ineep.org.br": [
        "https://ineep.org.br/feed/",
    ],

    # Setor publico / infra
    # Camara dos Deputados (Agencia Camara de Noticias): public gov site, no
    # auth. The "ultimas noticias" page (/noticias/ultimas) has no <link> feed
    # tag, but /noticias/rss lists subscribable feeds — the all-areas one is
    # /noticias/rss/ultimas-noticias (classic RSS 2.0, ~20 items, full
    # title/link/pubDate/content:encoded). The global keyword set narrows it to
    # oil & gas / fuel-distribution items (Petrobras, diesel, ANP, etc.).
    # Served gzip (feedparser handles it); we do NOT advertise br (Pegadinha #12).
    "www.camara.leg.br": [
        "https://www.camara.leg.br/noticias/rss/ultimas-noticias",
    ],
    "www.poder360.com.br": [
        "https://www.poder360.com.br/feed/",
    ],
    "diariodopoder.com.br": [
        "https://diariodopoder.com.br/feed",
    ],

    # Outros
    "timesbrasil.com.br": [
        "https://timesbrasil.com.br/feed/",
    ],
    # Monitor Mercantil: the WordPress /feed/ is fine, but Cloudflare serves a
    # Managed Challenge ("Just a moment...", HTTP 403, cf-mitigated: challenge)
    # to datacenter IPs — which is every IP the scanner ever runs from. From a
    # residential connection the SAME request answers 200 with 12 entries, which
    # is why this looked healthy for three months while producing nothing: the
    # last article landed 2026-04-29. The challenge covers the WHOLE domain, feed
    # and article pages alike, and curl_cffi browser impersonation does NOT get
    # through (Cloudflare is gating on IP reputation, not TLS fingerprint), so
    # the HOMEPAGE_SCRAPERS route is unavailable too. Covered via Google News
    # site: below. Re-test any time with:
    #   gh workflow run diagnose_feed.yml -f url=https://monitormercantil.com.br/feed/
    # and uncomment this if it ever answers 200 from the runner (e.g. if the
    # scanner moves to a self-hosted/residential runner).
    # "monitormercantil.com.br": [
    #     "https://monitormercantil.com.br/feed/",
    # ],
    "veronoticias.com": [
        "https://veronoticias.com/feed/",
    ],
    "claudiodantas.com.br": [
        "https://claudiodantas.com.br/feed/",
    ],
    "obastidor.com.br": [
        "https://obastidor.com.br/feed/",
    ],
    "tconline.com.br": [
        "https://tconline.com.br/feed/",
    ],
    # Visnoinvest recusa conexoes de servidor (reset) — coberto via Google News
    # "visnoinvest.com.br": [],
    # Visao Agro: feed retorna XML malformado e post-sitemap nao e news sitemap — Google News
    # "visaoagro.com.br": [],
    "www.theagribiz.com": [
        "https://www.theagribiz.com/feed/",
    ],
}


# Dominios cadastrados no clipinator mas sem RSS conhecido / confiavel.
# Cobertura feita pelo Google News com site: operator (hl=pt-BR).
NO_RSS_DOMAINS: list[str] = [
    "br.investing.com",
    "br.tradingview.com",
    "www.argusmedia.com",
    "www.theedgesingapore.com",
    "www12.senado.leg.br",
    "edition.cnn.com",
    "www.cnn.com",
    "estradao.estadao.com.br",
    "aovivo.folha.uol.com.br",
    # RSS seletivo (curado, nao lista todos os artigos) — GNews site: complementa
    "www.bloomberglinea.com.br",
    # RSS descontinuado ou bloqueado — cobertura via Google News site:
    "www.brasil247.com",
    "brasil247.com",
    "observatorio.firjan.com.br",
    "agenciainfra.com",
    "noticias.r7.com",           # removeu feed RSS publico
    "agencia.petrobras.com.br",  # CMS Liferay sem feed publico
    "www.brasilenergia.com.br",  # paywall — primary path is the authenticated homepage scraper; GNews kept as a redundant net
    "visnoinvest.com.br",        # recusa conexoes de servidor
    # Monitor Mercantil: Cloudflare Managed Challenge (403) on every datacenter
    # IP, whole domain, curl_cffi impersonation included — see the commented-out
    # RSS entry above. Google News indexes the domain well (a site: query with
    # the keyword set returns ~100 items over the last week), and GNews items
    # carry title + published_at, which is all fast_mode needs to persist an
    # article. Article bodies stay unreachable, so these land with an empty
    # snippet — same shape as r7 / agencia.petrobras.
    "monitormercantil.com.br",
    # Conjur: zone-wide Cloudflare Managed Challenge (403) since 2026-07-27,
    # from residential IPs too and through curl_cffi impersonation — see the
    # commented-out RSS entry above. APEX form on purpose: Google News indexes
    # the canonical apex (article URLs are https://conjur.com.br/...); a site:
    # query returns ~9 items over the last week against 2 for the www form.
    # This is a partial recovery, not a replacement, and the reason is worth
    # recording. Googlebot still crawls the site fine (a bare site: query shows
    # ~55 items in the last 7 days, including articles published today), but two
    # things cut the yield:
    #   1. Bodies are unreachable, so keyword matching is TITLE-ONLY. Of the 233
    #      Conjur articles captured in the 60 days before the block, only 26.6%
    #      carry a keyword in the title — the other ~73% matched on the article
    #      lede that enrich_item used to fetch. That caps this route at roughly
    #      a quarter of the ~25/week the feed delivered.
    #   2. AND-ing site: with the 50+ keyword OR-block makes Google rank
    #      tag/archive pages ("Arquivo de Braskem", "Arquivo de
    #      Cide-Combustiveis") above real articles, because those pages are
    #      literally made of the keyword. They will be persisted like any other
    #      item; the pollution is bounded because news_articles is keyed by url,
    #      so each archive page lands at most once.
    # Kept anyway because the alternative is zero: the global per-keyword Google
    # News queries surface NO Conjur items at all (a legal outlet is outranked
    # by the mainstream press on "Petrobras" / "gas" / "ANP"), and the query does
    # find the high-value sector pieces when they exist — the Refit/STJ and
    # gas-pipeline-tariff articles both showed up in it. Bodies stay unreachable,
    # so these land with an empty snippet — same shape as monitormercantil / r7 /
    # agencia.petrobras.
    "conjur.com.br",
    # A Tribuna (Santos): no RSS (WAF 403 on /rss) and sitemap generation is
    # off since 2026-07-01. Primary path is the "Últimas Notícias" scraper in
    # HOMEPAGE_SCRAPERS; GNews site: query is a redundant net in case the
    # listing markup changes.
    "www.atribuna.com.br",
]

# Dominios que publicam em ingles — consultados com hl=en-US para aparecer no GNews.
ENGLISH_NO_RSS_DOMAINS: list[str] = [
    "www.reuters.com",           # RSS encerrado oficialmente em 2020
]

# Sitemaps WordPress padrao (sem namespace news:news).
# Itens chegam sem titulo/summary — enrich_item busca a pagina e preenche.
# Filtrados por <lastmod> antes de qualquer enriquecimento.
STANDARD_SITEMAPS: dict[str, list[str]] = {
    "istoedinheiro.com.br": [
        # Sitemap index: o fetcher detecta <sitemapindex> e usa a ultima pagina automaticamente
        "https://istoedinheiro.com.br/wp-sitemap.xml",
    ],
    "visaoagro.com.br": [
        "https://visaoagro.com.br/post-sitemap.xml",
    ],
}

# Sites que bloqueiam RSS mas permitem acesso via browser (curl_cffi).
# O scraper pega links de artigos da homepage; enrich_item busca cada um.
# Chave = dominio, valor = URL da pagina de noticias.
HOMEPAGE_SCRAPERS: dict[str, str] = {
    # Oil & gas section — 30+ recent articles, all sector. Fetched through the
    # authenticated session (news_hunter.brasilenergia_auth): the listing and
    # each article page are requested with the be-auth cookie so we get full
    # bodies behind the subscriber paywall.
    "www.brasilenergia.com.br": "https://brasilenergia.com.br/petroleoegas/ultimasnoticias",
    "agencia.petrobras.com.br": "https://agencia.petrobras.com.br/",
    # A Tribuna (Santos/SP — the Port of Santos daily). No feed of any kind:
    # /rss and /rss/ answer 403 "Acesso Bloqueado" (WAF), every other RSS path
    # 404s, and the CMS config embedded in the page carries "feeds":[]. Its
    # sitemaps exist but are FROZEN — /sitemap/sitemap_1.xml and
    # /sitemap/sitemap_news.xml both stop at 2026-07-01 while the site keeps
    # publishing, and the same page config shows "featureToggleSitemap":false,
    # i.e. sitemap regeneration was switched off server-side. So the only live
    # discovery surface is the server-rendered "Últimas Notícias" listing
    # (~21 article links per fetch, plenty for a 5-minute poll).
    "www.atribuna.com.br": "https://www.atribuna.com.br/noticias/ultimas-noticias",
}

# Subconjunto de HOMEPAGE_SCRAPERS cujas URLs apontam para paginas de
# "ultimas noticias" (so artigos recentes, sem fixados antigos).
# Quando enrich_item nao consegue extrair published_at (ex.: paywall bloqueia
# o fetch do artigo), usamos now() como data aproximada em vez de descartar.
RECENT_ONLY_SCRAPERS: frozenset[str] = frozenset({
    "www.brasilenergia.com.br",
    "www.atribuna.com.br",
})


# URLs de sitemap Google News (urlset + news:news) - nao sao RSS mas entram
# no mesmo pipeline via parser dedicado em fetcher._fetch_one.
SITEMAP_URL_MARKERS: tuple[str, ...] = (
    "/sitemap/",
    "sitemap-news",
    "news-sitemap",
    "sitemap_news",
    "news.xml",
    # Next.js-style sites (Exame, etc.) host the Google News sitemap at
    # /<section>/sitemap.xml — e.g. https://exame.com/noticias/sitemap.xml.
    # Matches /noticias/, /news/, /noticia/ + sitemap.xml.
    "/noticias/sitemap.xml",
    "/noticia/sitemap.xml",
    "/news/sitemap.xml",
)


def is_sitemap_url(url: str) -> bool:
    u = url.lower()
    return any(m in u for m in SITEMAP_URL_MARKERS)


def all_rss_feeds() -> list[tuple[str, str]]:
    """Lista (dominio, url_feed) para todo feed registrado (RSS + news sitemaps)."""
    out: list[tuple[str, str]] = []
    for domain, feeds in RSS_FEEDS.items():
        for feed_url in feeds:
            out.append((domain, feed_url))
    return out


def all_standard_sitemaps() -> list[tuple[str, str]]:
    """Lista (dominio, url) dos sitemaps WordPress padrao."""
    out: list[tuple[str, str]] = []
    for domain, urls in STANDARD_SITEMAPS.items():
        for url in urls:
            out.append((domain, url))
    return out


def all_homepage_scrapers() -> list[tuple[str, str]]:
    """Lista (dominio, url) das homepages a raspar por links de artigos."""
    return list(HOMEPAGE_SCRAPERS.items())


def google_news_queries(keywords: list[str], hours: int) -> list[str]:
    """URLs de RSS do Google News, uma por keyword, com janela temporal.

    Usamos 'when:Xh' para restringir ao periodo. O Google agrega notícias de
    centenas de sites em PT-BR e US, o que serve tanto de fallback para sites
    sem RSS quanto de reforco de cobertura para sites com RSS truncado.
    """
    if hours <= 48:
        when = f"{hours}h"
    else:
        days = max(1, hours // 24)
        when = f"{days}d"
    out: list[str] = []
    for kw in keywords:
        q = quote_plus(f'"{kw}" when:{when}')
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt"
        )
    return out


def google_news_site_queries(domains: list[str], keywords: list[str], hours: int) -> list[str]:
    """Uma query Google News por dominio sem RSS proprio, OR das keywords."""
    if hours <= 48:
        when = f"{hours}h"
    else:
        days = max(1, hours // 24)
        when = f"{days}d"
    kw_or = " OR ".join(f'"{k}"' for k in keywords)
    out: list[str] = []
    for domain in domains:
        q = quote_plus(f"site:{domain} ({kw_or}) when:{when}")
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt"
        )
    return out


def google_news_site_queries_en(domains: list[str], keywords: list[str], hours: int) -> list[str]:
    """Igual a google_news_site_queries mas com hl=en-US para sites em ingles."""
    if hours <= 48:
        when = f"{hours}h"
    else:
        days = max(1, hours // 24)
        when = f"{days}d"
    kw_or = " OR ".join(f'"{k}"' for k in keywords)
    out: list[str] = []
    for domain in domains:
        q = quote_plus(f"site:{domain} ({kw_or}) when:{when}")
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        )
    return out
