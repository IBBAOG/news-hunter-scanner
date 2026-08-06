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
    # einvestidor.estadao.com.br REMOVIDO em 2026-08-04. O sitemap
    # /post/sitemap-news-1.xml congelou: 400 entradas, todas entre 2026-05-18 e
    # 2026-05-27 (o sitemap-news-2.xml e ainda mais velho, entao parou o
    # gerador, nao a paginacao). O e-Investidor migrou de CMS e hoje publica em
    # www.estadao.com.br/einvestidor/..., que o news sitemap do Estadao — ja
    # cadastrado logo acima — cobre: 13 das 100 URLs da ultima coleta estao sob
    # esse path, 366 artigos nos ultimos 60 dias. Manter a entrada so gastava um
    # fetch de 254 KB de conteudo de maio a cada 5 min. Nao ha perda de
    # cobertura; e higiene.
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
    # Gazeta do Povo (Curitiba/PR, national reach). Next.js site served from
    # S3 + CloudFront — no Cloudflare, no WAF: the feeds, the article pages and
    # the sitemaps all answer 200 from the GHA runner exactly as they do from a
    # residential connection (diagnose_feed, 2026-08-06: 0.05s, x-cache HIT,
    # 24/80/24 entries, _fetch_one err=None; an article page is 307 KB with the
    # full body). The metered paywall (`cXenseParse:gdp-paywall`) is applied
    # CLIENT-SIDE by Cxense/Piano — the server ships every paragraph to an
    # anonymous fetch — so no subscriber cookie is needed and none is used.
    #
    # WHICH FEEDS. The site publishes 75 feeds at /rss/, including a general
    # `ultimas-noticias.xml`. We deliberately register the three closest to the
    # sector beat instead of the general one. Measured on 2026-08-06 with the
    # LIVE keyword set, counting a hit as REAL only when a matched keyword
    # survives a word-boundary check (the substring keyword "gas" fires on
    # "Gaspar" / "gastos" / "desgaste", which is a global config issue, not a
    # Gazeta one):
    #
    #     feed                items  span  filter-pass  REAL
    #     ultimas-noticias      269   48h      26          5
    #     economia               24   76h       3          2
    #     republica              80   47h      11          2
    #     agronegocio            24   50d       2          1
    #     brasil / mundo / opiniao / parana / vida-e-cidadania          0
    #
    # economia + republica + agronegocio catch 4 of the 5 real hits the general
    # feed catches (the 5th was a `vozes` column), for a THIRD of the junk and a
    # third of the bytes per 5-minute poll. The narrower pool also matters for
    # the lede rescue: it is capped at LEDE_RESCUE_CAP_DOMAIN=8 fetches per
    # domain per scan, so drawing those 8 from ~39 near-misses in economia +
    # republica is far likelier to rescue a sector story than drawing them from
    # ~120 near-misses dominated by politics, world and opinion.
    #   - economia    : the beat itself — fuel prices, GLP, Petrobras, Raízen.
    #   - republica   : where fuel POLICY lands ("PL da Gasolina" ×2 in 48h).
    #                   Also carries /eleicoes/ URLs, i.e. it is the politics
    #                   feed; it is the noisiest of the three.
    #   - agronegocio : ethanol / biofuel trade. Barely moves (24 items span 50
    #                   days), so it is nearly free; 1 real hit in that window.
    # Re-measure any time and drop republica if the political noise outweighs
    # the 2-per-48h policy hits — that is the marginal one.
    "www.gazetadopovo.com.br": [
        "https://www.gazetadopovo.com.br/feed/rss/economia.xml",
        "https://www.gazetadopovo.com.br/feed/rss/republica.xml",
        "https://www.gazetadopovo.com.br/feed/rss/agronegocio.xml",
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
    # Visao Agro: /feed retorna XML malformado e nao ha news sitemap (urlset com
    # news:news). Coberto via sitemap WordPress padrao — ver STANDARD_SITEMAPS.
    # "visaoagro.com.br": [],
    "www.theagribiz.com": [
        "https://www.theagribiz.com/feed/",
    ],
}


# Dominios cadastrados no clipinator mas sem RSS conhecido / confiavel.
# Cobertura feita pelo Google News com site: operator (hl=pt-BR).
NO_RSS_DOMAINS: list[str] = [
    "br.investing.com",
    # TradingView: mudo de 2026-07-23 ate 2026-08-04. Causa: o GNews enchia os
    # 100 slots com as paginas PERENES de cotacao/previsao ("Ação PETR3:
    # Cotação hoje", "Previsão BRKM3 — Preço Alvo para 2027") em vez de
    # materias — 37 dos 100 itens eram pre-2026 e ZERO estavam dentro da janela
    # de 24h. Nao era ranking: era a query perdendo o `when:` por truncagem
    # (ver o bloco de comentario acima de google_news_queries). Com `when:` na
    # frente a mesma query devolve 7 itens, 5 deles frescos em 24h — medido
    # duas vezes, 2026-08-04. Fica cadastrado.
    "br.tradingview.com",
    "www.argusmedia.com",
    "www12.senado.leg.br",
    # estradao.estadao.com.br REMOVIDO em 2026-08-04: o host responde 301 para
    # https://www.estadao.com.br/jornal-do-carro/estradao/, ou seja, ja nao e um
    # dominio proprio. Uma query GNews `site:` NUA (sem keyword, sem janela)
    # devolve 0 itens — o Google nao indexa mais nada sob esse host. As URLs
    # /jornal-do-carro/ aparecem no news sitemap do Estadao, que ja esta
    # cadastrado em RSS_FEEDS. Zero perda de cobertura.
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

# Dominios que publicam em ingles — consultados com hl=en-US para aparecer no
# GNews, e com o subconjunto de keywords em ingles (ver english_keywords).
ENGLISH_NO_RSS_DOMAINS: list[str] = [
    "www.reuters.com",           # RSS encerrado oficialmente em 2020
    # CNN e The Edge Singapore estavam em NO_RSS_DOMAINS, ou seja, consultados
    # com hl=pt-BR&gl=BR — em portugues, para veiculos que publicam em ingles.
    # Resultado: ZERO itens desde sempre nos tres. Medicao de 2026-08-04, janela
    # de 24h, itens frescos em 24h:
    #
    #     dominio                   pt-BR (antes)   en-US + subset (agora)
    #     www.theedgesingapore.com        0                  19
    #     edition.cnn.com                 0                  16
    #     www.cnn.com                     0                  29
    #
    # A CNN nao vinha nem em ingles enquanto o bloco OR tinha 53 keywords: a
    # truncagem da query (ver comentario acima de google_news_queries) matava o
    # `when:` e depois o subconjunto util de keywords. Com `when:` na frente e o
    # bloco em ingles, ela produz. Expectativa honesta de volume: a CNN e uma
    # fonte de baixa densidade para o setor — 16 itens em 30 dias em
    # www.cnn.com e 3 em edition.cnn.com casando o bloco COMPLETO —, entao
    # espere alguns itens por dia, nao dezenas.
    "edition.cnn.com",
    "www.cnn.com",
    "www.theedgesingapore.com",
]

# Sitemaps WordPress padrao (sem namespace news:news).
# Itens chegam sem titulo/summary — enrich_item busca a pagina e preenche.
# Filtrados por <lastmod> antes de qualquer enriquecimento.
STANDARD_SITEMAPS: dict[str, list[str]] = {
    "istoedinheiro.com.br": [
        # Sitemap index: o fetcher detecta <sitemapindex> e usa a ultima pagina automaticamente
        "https://istoedinheiro.com.br/wp-sitemap.xml",
    ],
    # Visao Agro roda Yoast, que pagina o sitemap de posts em post-sitemap.xml,
    # post-sitemap2.xml ... post-sitemap7.xml. Estavamos apontando direto para
    # post-sitemap.xml, que e a PAGINA 1 — a dos posts mais ANTIGOS (1000 URLs,
    # de 2022-06-01 a 2023-04-28). Com o corte de 96h do fetcher, isso e zero
    # item garantido, e foi zero em 33/33 runs. Os posts vivos estao na pagina
    # 7. Apontamos para o INDICE e deixamos _fetch_standard_sitemap escolher a
    # pagina (maior numero de pagina — ver a justificativa la, incluindo por que
    # o <lastmod> do indice nao serve: ele anuncia post-sitemap.xml como
    # modificado hoje).
    #
    # Rendimento perdido enquanto durou, contando SO o que casa keyword no slug
    # (piso — o match real e no corpo, via enrich_item): 12 posts em 96h, 34 em
    # 7d, 152 em 30d.
    "visaoagro.com.br": [
        "https://visaoagro.com.br/sitemap_index.xml",
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
# "ultimas noticias" — sem artigos FIXADOS antigos no topo. Quando o item tem
# titulo mas o enrich nao consegue extrair published_at (ex.: o paywall esconde
# a data), usamos now() como aproximacao em vez de descartar.
#
# ATENCAO — esse now() e um CHUTE DE PRIMEIRA DESCOBERTA, nunca um fato:
# `Article.published_is_approx` marca a linha e `supabase_sync` garante que ela
# jamais sobrescreva a data de uma linha que ja existe. Sem essa trava o
# carimbo se auto-renova a cada scan: como a data e sempre "agora", o item
# nunca sai da janela de 24h, logo continua sendo re-descoberto e re-carimbado,
# e um artigo de dias atras flutua eternamente no topo do feed (incidente
# 2026-08-04, Brasil Energia).
#
# E "recente" NAO quer dizer "ultimas 24h": a listagem do Brasil Energia traz
# 30 links cobrindo ~7 DIAS. Por isso `_scrape_homepage` le a data que a propria
# listagem imprime ao lado do link (RawItem.published_hint) — e ela, nao o
# now(), que classifica esses itens.
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


# -----------------------------------------------------------------------------
# Google News: o operador `when:` vem ANTES do bloco OR. Nao e estetica.
#
# O Google TRUNCA a query. Medido em 2026-08-04 contra site:noticias.r7.com,
# com `when:24h` na frente e o resto preenchido com termos-lixo:
#
#     "Petrobras" na posicao 1  ......................  10 itens
#     "Petrobras" depois de  8 termos-lixo ...........  10 itens
#     "Petrobras" depois de 12 termos-lixo ...........  10 itens
#     "Petrobras" depois de 16 termos-lixo ...........   0 itens
#     "Petrobras" depois de 52 termos-lixo ...........   0 itens
#
# Ou seja: tudo depois de ~13 termos do bloco OR e DESCARTADO. Com `when:` no
# FIM da query (formato anterior), o que era descartado era o proprio `when:` —
# a query virava um `site:` sem filtro de tempo e o Google enchia os 100 slots
# com paginas perenes e material de arquivo. Efeito medido nos 21 dominios de
# NO_RSS_DOMAINS, janela de 24h, mesmo bloco de 53 keywords:
#
#     when: no fim (antes) ......  159 itens frescos em 24h, ~1400 itens brutos
#     when: na frente (agora) ...  313 itens frescos em 24h,  ~330 itens brutos
#
# Nenhum dominio regrediu. Os maiores ganhos: br.investing.com 73->100,
# brasil247 12->47, noticias.r7.com 53->73, agenciainfra 1->15, argusmedia
# 1->7, conjur 0->7, br.tradingview.com 0->5, monitormercantil 0->2.
# De quebra, ~1070 itens brutos de arquivo deixam de ser baixados e filtrados a
# cada scan.
#
# NAO reordene isto de volta. E a mesma patologia que ja tinha sido descrita
# (sem causa-raiz) nos comentarios de conjur.com.br ("arquivo de Braskem"
# ranqueando acima de artigos) e de br.tradingview.com (paginas de cotacao).
#
# Consequencia ainda ABERTA (nao tratada aqui): como so ~13 keywords do
# bloco sobrevivem e `store.get_config` devolve o conjunto ORDENADO
# ALFABETICAMENTE, as keywords que efetivamente chegam ao Google hoje sao
# "ANP, ANS, barril, barris, blau, Brasil Energia, Braskem, Brava, Brent,
# cogna, combustiveis, combustíveis, combustivel" — "Petrobras", "petróleo",
# "diesel", "gasolina" e "óleo" caem fora por acidente alfabetico. Corrigir
# isso muda O QUE o scanner procura em 21 dominios (decisao editorial), entao
# fica registrado aqui em vez de ser mudado de lado. Quantificacao: quebrar o
# bloco em 5 chunks de 12 rendeu, em 24h, 19 vs 0 (edition.cnn.com), 36 vs 0
# (www.cnn.com) e 29 vs 2 (theedgesingapore) — ao custo de 5x mais queries.
# -----------------------------------------------------------------------------


def _when_clause(hours: int) -> str:
    if hours <= 48:
        return f"{hours}h"
    return f"{max(1, hours // 24)}d"


def _kw_or(keywords: list[str]) -> str:
    return " OR ".join(f'"{k}"' for k in keywords)


# Keywords que podem aparecer num texto em INGLES, em ordem de prioridade.
#
# Como o Google so honra ~13 termos do bloco OR (ver acima), mandar as 53
# keywords para um site em ingles gasta os slots uteis com termos que nunca vao
# casar ("combustiveis", "petroleo", "refinaria", "Raízen"...). Medicao de
# 2026-08-04, janela de 24h, hl=en-US, itens frescos em 24h:
#
#     dominio                    bloco completo (53)   subconjunto abaixo (12)
#     edition.cnn.com                     0                     16
#     www.cnn.com                         0                     29
#     www.theedgesingapore.com            2                     19
#     www.reuters.com                   100 (ja no teto)       100
#
# Uma query por dominio nos dois casos — o ganho e de graca. O mesmo bloco
# quebrado em 5 chunks de 12 (5x mais queries) rendeu 19 / 36 / 29: o
# subconjunto captura a maior parte do ganho a 1/5 do custo, e o custo importa
# (o news.google.com ja derrubou TODAS as queries de uma vez em 2 de 33 runs).
#
# E um FILTRO sobre o conjunto vivo que vem do Supabase, nao uma lista
# paralela: keyword apagada no banco some daqui junto. Keyword nova so entra
# quando alguem a adiciona aqui — de proposito, para o bloco nao voltar a
# estourar o limite de truncagem em silencio.
ENGLISH_KEYWORD_PRIORITY: tuple[str, ...] = (
    "Petrobras", "oil", "gas", "diesel", "Brent", "WTI", "OPEC",
    "Braskem", "Cosan", "refinery", "Hormuz", "Vibra",
    # Reservas: so entram se alguma das acima nao estiver no conjunto vivo.
    "PRIO", "Ultrapar", "Ipiranga", "OceanPact", "refit", "ANP",
)
# Teto de termos por query em ingles. 12 termos = 25 "palavras" contando
# `site:`, `when:` e os OR — abaixo do ponto de truncagem medido (~28).
ENGLISH_KEYWORD_CAP = 12


def english_keywords(keywords: list[str]) -> list[str]:
    """Subconjunto em ingles do conjunto vivo de keywords, capado e deterministico."""
    live = {k.casefold(): k for k in keywords}
    subset = [live[p.casefold()] for p in ENGLISH_KEYWORD_PRIORITY if p.casefold() in live]
    if not subset:
        # Conjunto vivo sem nenhuma keyword da allowlist (config exotica):
        # degrada para o comportamento antigo em vez de emitir "( )".
        subset = list(keywords)
    return subset[:ENGLISH_KEYWORD_CAP]


def google_news_queries(keywords: list[str], hours: int) -> list[str]:
    """URLs de RSS do Google News, uma por keyword, com janela temporal.

    Usamos 'when:Xh' para restringir ao periodo. O Google agrega notícias de
    centenas de sites em PT-BR e US, o que serve tanto de fallback para sites
    sem RSS quanto de reforco de cobertura para sites com RSS truncado.
    """
    when = _when_clause(hours)
    out: list[str] = []
    for kw in keywords:
        q = quote_plus(f'when:{when} "{kw}"')
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt"
        )
    return out


def google_news_site_queries(domains: list[str], keywords: list[str], hours: int) -> list[str]:
    """Uma query Google News por dominio sem RSS proprio, OR das keywords."""
    when = _when_clause(hours)
    kw_or = _kw_or(keywords)
    out: list[str] = []
    for domain in domains:
        q = quote_plus(f"site:{domain} when:{when} ({kw_or})")
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt"
        )
    return out


def google_news_site_queries_en(domains: list[str], keywords: list[str], hours: int) -> list[str]:
    """Igual a google_news_site_queries mas em ingles (hl=en-US).

    Alem do idioma, restringe o bloco OR ao subconjunto de keywords que faz
    sentido em ingles — ver ENGLISH_KEYWORD_PRIORITY.
    """
    when = _when_clause(hours)
    kw_or = _kw_or(english_keywords(keywords))
    out: list[str] = []
    for domain in domains:
        q = quote_plus(f"site:{domain} when:{when} ({kw_or})")
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        )
    return out
