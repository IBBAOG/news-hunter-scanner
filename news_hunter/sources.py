"""Registro de feeds RSS por dominio + queries Google News.

Cobertura: feeds RSS publicos quando existem. Sites sem RSS ou com RSS
instavel sao cobertos via Google News search (site:dominio + keyword).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable
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
    # JOTA — legal/regulatory outlet (tributario, STF, Congresso, energia).
    # Registered 2026-08-20: it had NEVER been cadastrada, so `news_articles`
    # held zero jota.info rows and the dashboard could not surface a single JOTA
    # story. Only the GENERAL feed exists — /energia/feed, /tributos/feed and
    # every other per-editoria form answer 404, so the per-editoria preference
    # documented for Gazeta do Povo does not apply here.
    # Measured on the runner 2026-08-20 (measure_source, LIVE 121-keyword set,
    # 72h window): items=25 span=19h fresh=25 pass=1 near=24 rescued=0 no_body=0
    # fetch=1.7s. One on-beat pass in 19h ("Imposto de Exportacao pune quem
    # produz e ameaca competitividade do petroleo") is ~1.3/day, i.e. ~9/7d —
    # above the >=3/7d bar. The feed rolls fast (25 items / 19h), so nothing is
    # lost between 5-minute scans.
    # `rescued=0` in that run is a MEASUREMENT artifact, not the ceiling: JOTA
    # serves no article <p> in the DOM, so the near-miss lede rescue was reading
    # the meta description. ex_jota (_clipinator_shim) now lifts the body out of
    # the __NEXT_DATA__ island in the same commit, which is what makes body
    # matching possible at all here.
    "www.jota.info": [
        "https://www.jota.info/feed",
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
    # Jornal do Comercio (Porto Alegre/RS) — the RS economy-and-business daily,
    # jornaldocomercio.com. NOT the Recife paper of the same name
    # (jc.ne10.uol.com.br), which is a different outlet and is not registered.
    #
    # Infrastructure: a STATIC site on S3 (`x-host:
    # jornaldocomercio.com.s3-website-us-west-2.amazonaws.com`) behind Varnish
    # and Cloudflare. No WAF challenge anywhere: the feeds, /sitemap.xml and the
    # article pages all answer 200 from the GHA runner (diagnose_feed,
    # 2026-08-11: 0.08-0.27s, cf-cache-status HIT, no cf-mitigated header; an
    # article page is 128 KB gzip).
    #
    # THE PRIMARY SURFACE IS THE SITEMAP, NOT THESE FEEDS — see the
    # jornaldocomercio entry in STANDARD_SITEMAPS below for the full table. The
    # short version: /sitemap.xml is a rolling ~5-day window of every section
    # (361 URLs) while each RSS feed holds 5-10 items that rotate out within
    # hours, so the sitemap catches twice as much of the beat.
    #
    # SO WHY REGISTER TWO FEEDS THAT PASS ZERO ITEMS ON THEIR OWN? Because
    # sitemap items arrive TITLE-LESS. The pipeline can only pre-filter those on
    # the URL slug, and — the part that matters — a title-less item can never
    # become a lede-rescue candidate (_keep_candidate only offers the rescue to
    # items that already have a title and a date). An article whose keyword
    # lives only in the body is therefore invisible through the sitemap, and
    # those are exactly the RS-local stories no other registered source carries.
    # Measured case, 2026-08-11: "Plataforma P-33 já está em Rio Grande, mas
    # tempo adia entrada no canal" — no keyword in the title or the slug, while
    # the body opens "A plataforma P-33, da Petrobras". It was rescued through
    # the economia feed and missed entirely through the sitemap.
    #   - economia    : the beat itself, and the feed that carried the P-33 story.
    #   - jc-logistica: freight/road transport — diesel, fuel costs. Rescued
    #                   "Nova lei do frete mínimo" on the keyword "combustível".
    # Both are ~5-7 items per poll, so their near-misses are almost all on-beat
    # and the LEDE_RESCUE_CAP_DOMAIN=8 budget is spent well.
    #
    # /_conteudo/home/rss.xml is deliberately NOT registered even though it is
    # the richest feed (30 items, 14h). Measured against the live 47-keyword
    # set: its 5 passes are a strict SUBSET of the sitemap's 10, and of its 25
    # near-misses ZERO rescued — they are sports, culture and city items. It
    # would burn the whole 8-fetch rescue budget on off-beat items and crowd out
    # the two feeds above, which rescued 2.
    #
    # KNOWN INTERACTION, looks like a bug if you watch a single scan: the
    # pipeline dedupes by normalized URL and FIRST COLLECTED WINS, so when the
    # sitemap's title-less copy of an article beats the RSS copy, that scan
    # loses the rescue path for it. It is not fatal — the race re-runs every
    # ~5 min and news_articles is keyed by url, so one win persists the article
    # for good — but do not conclude the feeds are useless from one scan's log.
    #
    # Paywall: metered, and many economia items are outright
    # `isAccessibleForFree: False`. Irrelevant to us — the server still ships
    # the 150-200 char LEDE anonymously, and the keyword is usually in it (the
    # Ormuz story carries "petróleo" only there). Body extraction needs the
    # dedicated ex_jornaldocomercio: the body is in `section.paywall-carregando`
    # and has NO <p> at all. See _clipinator_shim.
    "www.jornaldocomercio.com": [
        "https://www.jornaldocomercio.com/_conteudo/economia/rss.xml",
        "https://www.jornaldocomercio.com/_conteudo/cadernos/jc-logistica/rss.xml",
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
    # sector beat instead of the general one. Measured 2026-08-06 by running the
    # real fetcher + filter over each feed:
    #
    #     feed                items  span  filter-pass  of which SECTOR
    #     ultimas-noticias      270   48h      23             5
    #     economia               24   71h       3             2
    #     republica              80   47h       4             2
    #     agronegocio            24   50d       4             2
    #     mundo                  85   48h       6             0
    #     vida-e-cidadania       24  148h       4             0
    #     brasil / opiniao / parana                            0
    #
    # The trio returns 11 passes for 6 sector stories; the general feed returns
    # 23 passes for 5, i.e. the SAME stories plus ~15 off-beat items, at three
    # times the bytes on a 5-minute poll. The narrow pool also protects the lede
    # rescue, capped at LEDE_RESCUE_CAP_DOMAIN=8 fetches per domain per scan:
    # drawing those 8 from ~40 near-misses that are already economy/politics/agro
    # is likelier to rescue a body-only mention than drawing them from ~120
    # dominated by world, religion and culture.
    #   - economia    : the beat itself — fuel prices, GLP, Petrobras, Raízen.
    #   - republica   : where fuel POLICY lands ("PL da Gasolina" ×2 in 48h).
    #                   Also carries /eleicoes/ URLs — it is the politics feed.
    #   - agronegocio : ethanol / biofuel trade. Barely moves (24 items spanning
    #                   50 days), so it is nearly free.
    # The one sector story the general feed caught and the trio missed was a
    # `vozes` column (opinion). Not worth 270 items a poll; revisit if columns
    # start carrying sector news.
    #
    # MEASUREMENT TRAP, worth more than the table above: `store.get_config()`
    # falls back to the 25 hardcoded DEFAULT_KEYWORDS (all substring) whenever
    # the Supabase service key is not reachable — which is the normal state of a
    # dev machine. The first pass of this analysis was run that way and produced
    # a plausible, wrong table: it showed "gas" hitting "Gaspar" / "gastos" /
    # "desgaste" everywhere, because in production `gas` and `gás` are EXACT
    # (54 keywords, 15 exact). Pin the live keyword set explicitly before
    # measuring a candidate feed, or you will tune against a keyword set the
    # scanner does not use. (Under the real set the dominant off-beat match is
    # the substring keyword "ANS", which fires inside "trANSformar" / "mANSão";
    # that is a global keyword-config matter, not a Gazeta one.)
    "www.gazetadopovo.com.br": [
        "https://www.gazetadopovo.com.br/feed/rss/economia.xml",
        "https://www.gazetadopovo.com.br/feed/rss/republica.xml",
        "https://www.gazetadopovo.com.br/feed/rss/agronegocio.xml",
    ],
    # Radio Itatiaia (Belo Horizonte/MG). Canonical domain is
    # www.itatiaia.com.br — the apex 301s to it, and every article link in every
    # surface is www-form (normalize_url then strips it, so articles land as
    # itatiaia.com.br; that is the key the extractor is registered under).
    #
    # SPLIT INFRASTRUCTURE, and this is the whole trick to registering it: the
    # site the reader sees is a Next.js front on www with NO feed at all
    # (/feed/, /rss, /rss.xml and /wp-sitemap.xml are 404s served as a Next
    # error page — 218 KB of HTML behind an HTTP 404, so a naive prober that
    # only reads status is fine but one that only reads bytes is not). The
    # WordPress backend that publishes it is exposed at admin.itatiaia.com.br,
    # and THAT host serves the real RSS. So the feed URL below is on `admin`
    # while the key, the articles and the extractor are all `itatiaia`.
    #
    # WHY ?cat= INSTEAD OF A PRETTY SECTION FEED: the WP permalinks for feeds
    # are broken on that host — /category/economia/feed/, /economia/feed/ and
    # /?feed=rss2&cat=<id> all answer HTTP 500. The plain /feed/ with a `cat`
    # query is the only per-editoria form that works. 68828 = "Economia" (the
    # `cat` query includes child categories, so this also carries Negocios,
    # Investimentos and Imoveis).
    #
    # WHICH SURFACE. Itatiaia publishes ~171 posts/day, overwhelmingly football,
    # police, celebrity and recipes, so the choice is entirely about precision.
    # Four surfaces exist; measured 2026-08-18 on the GHA runner with the real
    # fetcher + filter against the LIVE 47-keyword set (15 exact), window 24h:
    #
    #     surface                       items  span  fresh  pass  near  resc
    #     sitemap-news.xml (all sects)    100   12h    100     4    96     0
    #     /feed/?cat=68828  economia       60  142h     16     4    12     0
    #     /feed/?cat=68816  politica       60   25h     55     2    53     0
    #     /feed/?cat=68796  mg              —     —      —     —     —     —   (read timeout)
    #     /feed/?cat=68883  agro            60  223h      6     0     6     0
    #     /feed/          general           60   10h     60     1    59     0
    #
    # The general surfaces are NOT registered. Their extra recall is duplicate
    # national wire copy and their extra volume is junk — measured by replaying
    # the live keyword set over the title of every one of the 1,200 posts
    # published in the 7 days to 2026-08-18 (via the WP REST API, so the count
    # is the section's real output rather than a feed window):
    #
    #     section    posts  title-matches  genuine  junk
    #     economia      26        8            8      0
    #     politica      47        3            3      0
    #     mg            97        1            0      1
    #     agro          22        0            0      0
    #     other      1,008        5            1      4
    #
    # i.e. registering a general surface buys +4 genuine sector items a week and
    # +5 junk ones ("Quanto combustivel seu carro consome parado com motor
    # ligado", "Molho verde caseiro fica cremoso com tecnica simples de
    # adicionar oleo aos poucos", "Cruzeiro: Lucho VIBRA com assistencia") —
    # `oleo`, `combustivel` and `gasolina` are SUBSTRING keywords and this
    # outlet writes about cars, cooking and traffic all day. And the +4 is not
    # even unique: the Margem Equatorial / Alcolumbre cycle those items belong
    # to was already captured 300+ times from 20 other registered domains in the
    # same week (estadao 79, brasil247 20, g1 15, ...). economia gets 8 of the
    # 12 genuine items at 100% precision; the 4 it misses are wire duplicates.
    #
    # It also protects the lede rescue, which is capped at
    # LEDE_RESCUE_CAP_DOMAIN=8 fetches per DOMAIN per scan — a cap the sitemap
    # would not raise but would fill with football and police near-misses (96 of
    # them) instead of this feed's 12 economy ones. Rescue measured 0 on every
    # surface even after registering the extractor, so that budget currently
    # buys nothing here; the point is that it costs nothing either.
    #
    # KNOWN FLAKINESS, deliberately accepted: the feed sits behind a Varnish
    # whose TTL is short and irregular. A cache HIT is 0.11-0.35s, a MISS is
    # 4.08-4.88s — straddling the scanner's FEED_TIMEOUT of 4s (diagnose_feed
    # saw a HIT and a MISS in two back-to-back requests, 2026-08-18). Expect
    # this feed to be named in the run summary occasionally. It loses NOTHING
    # when that happens: the feed holds 60 items spanning ~134h, we poll every
    # ~5 min, and an item only has to be collected once. Do not "fix" this by
    # switching to sitemap-news.xml, which is static and always 0.17s — that
    # trades an occasional cosmetic error for the precision loss tabled above.
    #
    # Cadence 10.6 posts/day, largest healthy gap 20.1h over the 5-day feed
    # window — comfortably inside FEED_STALE_HOURS_DEFAULT (48h), so no entry
    # there.
    #
    # VERIFIED END TO END, 2026-08-18: the first scan carrying this entry
    # persisted exactly the 4 expected articles with source_name "Radio
    # Itatiaia", real published_at and correct keywords. Two of them landed with
    # an EMPTY snippet, which looks like a broken extractor and is not: the scan
    # runs run_search(fast_mode=True), so enrich_item returns before fetch_html
    # for any item that already has a title and a date, and the snippet is
    # simply the feed's <description> — kept when it clears
    # SNIPPET_MIN_RSS_CHARS=150 (177 and 210 chars here) and dropped when it
    # does not (148 and 92). Reproduced exactly, both paths, per article. So the
    # ex_auto registration in _clipinator_shim buys nothing on this path; it
    # buys the lede rescue a 357-360 char body instead of a ~140 char
    # standfirst. Article pages themselves are healthy from the runner (200 in
    # 0.28-0.72s, 391 KB, no cf-* headers).
    #
    # Nothing from this domain existed in news_articles before this commit
    # (0 rows of 38,781), so the global Google News queries never surfaced it —
    # this is new coverage.
    "www.itatiaia.com.br": [
        "https://admin.itatiaia.com.br/feed/?cat=68828",
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
    # CNBC publishes one feed per section under the same view.xml endpoint,
    # discriminated only by `id`. We were on id=10000664, which is the FINANCE
    # section — markets/personal-finance copy, not energy. It answers 200 with
    # 30 fresh items every scan, so nothing ever looked broken; it simply never
    # matched. Measured 2026-08-18 against the live keyword set (47 keywords,
    # 15 exact), all 30 entries the feed returned, title+summary only:
    #
    #     id=10000664 (Finance) ...  30 items, span 161h, pass  0, near 30
    #     id=19836768 (Energy) ....  30 items, span 244h, pass 23, near  7
    #
    # Same cost per scan, same publisher, 23x the yield. The Energy feed is
    # slower-moving (244h to cover 30 items vs 161h), which is expected of a
    # section feed and is not a problem: the fetcher filters by published_at.
    "www.cnbc.com": [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19836768",
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
    # The Moscow Times (English edition). Registered for the geopolitical beat
    # no Brazilian outlet covers at this granularity: Russian refinery throughput
    # under drone strikes, the gasoline/diesel export ban, Urals, OPEC+, shadow
    # fleet sanctions. Verified from the GHA runner via diagnose_feed on
    # 2026-08-06: HTTP 200 in 0.5-1.1s, 274 KB gzip, Cloudflare origin with
    # cf-cache-status DYNAMIC and NO cf-mitigated header (no challenge),
    # feedparser 50 entries bozo=False, _fetch_one 50 items err=None. No paywall:
    # an article page returns its full 23-paragraph body anonymously.
    #
    # WHY /rss/all AND NOT THE NARROWER FEEDS — the opposite call from Gazeta do
    # Povo above, on purpose. Three feeds exist (/rss/news, /rss/opinion,
    # /rss/all; there is no /rss/business, it 404s) and all cap at 50 items.
    # Measured 2026-08-06 against the live 47-keyword set:
    #
    #     feed          items  span    pass   ~/day   note
    #     /rss/news        50   195h      5     0.6   all on-beat, but see below
    #     /rss/opinion     50  2041h      1     0.0   dead — 1 hit in 85 days
    #     /rss/all         50   168h     17     2.4   ~8 on-beat, ~9 off-beat
    #
    # Two things make the general feed the right one HERE, where it was the wrong
    # one for Gazeta:
    #   1. /rss/all is the only feed carrying the BUSINESS section. A coverage
    #      diff found 4 items in /rss/all present in neither other feed, one of
    #      them "Russian Oil Refining Falls to 24-Year Low After Ukrainian Drone
    #      Strikes — Bloomberg" — the single most on-beat item in the window.
    #      /rss/news structurally misses that whole class of story.
    #   2. /rss/all embeds FULL ARTICLE BODIES (~9.5 KB summaries), so keyword
    #      matching is full-text with no fetch at all — it never touches the
    #      lede-rescue budget, and no extractor is needed. That matters a lot for
    #      an English outlet, where only 9 of our 47 keywords can ever match
    #      (oil / gas / Brent / WTI / OPEC exact + diesel / refinery / Hormuz /
    #      refit substring): full-text is what caught the shadow-fleet sanctions
    #      story, whose title carries no keyword.
    # The price is ~1 off-beat row/day (a war story that says "oil" once) and
    # 274 KB per 5-minute poll. If egress ever matters, /rss/news is the cheap
    # fallback at 31 KB — at the cost of the business section. /rss/opinion is
    # deliberately NOT registered.
    #
    # Item links use the APEX (https://themoscowtimes.com/...) while the feed
    # lives on www — so SOURCE_NAMES must be keyed on the apex, or every article
    # renders as the bare domain instead of "The Moscow Times". Verified: all 50
    # items come back with source_domain='themoscowtimes.com'.
    #
    # No Russian edition: moscowtimes.ru does not resolve (apex and www both
    # fail to connect), and independently of that, zero of our keywords are in
    # Cyrillic — a Russian-language feed could only match Latin tokens like
    # Brent/OPEC/WTI. Not registered rather than presumed.
    "www.themoscowtimes.com": [
        "https://www.themoscowtimes.com/rss/all",
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
    # Poder360 publishes ~100 articles/day, but Cloudflare fronts each feed
    # PATH as its own cache object and hands it out well past the origin's
    # `cache-control: max-age=300`. Measured 2026-08-14: /feed/ answered with
    # `age: 1023` and a newest item 28 min behind /feed/atom/, which answered
    # with `age: 206`. The two objects drift independently.
    #
    # Between 2026-08-07 19:55 UTC and 2026-08-14 the /feed/ object was pinned
    # outright. The scanner kept re-seeing the SAME 2026-08-07 items for a full
    # 24h — their `found_at` in news_articles advanced to exactly published+24h,
    # the moment the window filter finally dropped them — and then ingested
    # NOTHING from this domain for six days, while the same URL answered fresh
    # from a residential IP and from a GHA runner on demand. HTTP 200, valid
    # RSS, 10 dated entries: invisible to every "returned 0 items" check.
    #
    # A query-string cache buster does NOT help: the zone ignores the query in
    # its cache key (`/feed/?nhcb=<epoch>` still answered HIT with the same
    # age), and neither does `Cache-Control: no-cache` from the client. The fix
    # is a SECOND, independently cached path. Atom carries the same posts with
    # the same canonical links, and the pipeline dedupes by normalized URL, so
    # the overlap costs nothing. The staleness check in fetcher.py now names
    # either one out loud if it freezes again.
    "www.poder360.com.br": [
        "https://www.poder360.com.br/feed/",
        "https://www.poder360.com.br/feed/atom/",
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

    # =========================================================================
    # International oil & gas / shipping trade press (English) — Wave 1a.
    #
    # Registered 2026-08-18. Every yield below was MEASURED on the GHA runner
    # (measure_source.yml — so from a datacenter IP, with the Supabase secrets)
    # against the LIVE keyword set (91 keywords, 41 exact) with --lede, 7-day
    # window. English outlets match only on the Latin subset of the set (oil /
    # gas / diesel / Brent / WTI / OPEC / LNG / crude / offshore / tanker /
    # pipeline / Hormuz / ExxonMobil / Chevron / Petrobras / ... — accents never
    # fire), which is plenty: these feeds carry full descriptions, so most
    # matches land on title+summary with no body fetch.
    #
    # Item links are apex on some feeds and www on others, but normalize_url
    # strips www, so the RESOLVED source_domain — and thus the SOURCE_NAMES /
    # EXTRACTORS key that renders the outlet name — is ALWAYS the apex form.
    # Both keys are registered in _clipinator_shim regardless. None of these
    # publish slowly enough to need a FEED_STALE_HOURS entry (stalest healthy
    # span here is lngprime at 29h, inside the 48h default).
    #
    # Sources whose feed the scanner's feed path cannot USE — behind a WAF it
    # cannot clear (OGJ, Energy Voice, Offshore Technology, Rigzone) or DATELESS
    # with no recoverable article-page date (World Oil) — are in
    # ENGLISH_NO_RSS_DOMAINS instead, where Google News supplies the date. See
    # there.
    # =========================================================================
    # OilPrice.com — the highest-density surface measured in this wave. General
    # energy-markets wire. 2026-08-18: items=15 span=10h fresh=15 pass=11 near=4
    # rescued=0 — eleven of fifteen items in a TEN-HOUR window match on
    # title/summary alone (Hormuz, diesel, Brent, tankers, LNG, refinery,
    # gasoline). Article links are apex (oilprice.com/Latest-Energy-News/...).
    "oilprice.com": [
        "https://oilprice.com/rss/main",
    ],
    # gCaptain — maritime/shipping daily; heavy tanker + Strait-of-Hormuz +
    # oil-logistics coverage no Brazilian outlet carries. 2026-08-18: items=12
    # span=21h fresh=12 pass=9 near=3 rescued=0. Apex article links.
    "gcaptain.com": [
        "https://gcaptain.com/feed/",
    ],
    # Hellenic Shipping News — shipping + commodities wire/aggregator.
    # 2026-08-18: items=20 span=20h fresh=20 pass=7 near=13 rescued=0 (LNG
    # shipping, EU gas storage, Brent, ADNOC, Hormuz). Feed and article links are
    # both www, so source_domain resolves to the apex hellenicshippingnews.com.
    "www.hellenicshippingnews.com": [
        "https://www.hellenicshippingnews.com/feed/",
    ],
    # LNG Prime — pure LNG trade press; every item is on-beat by construction.
    # 2026-08-18: items=10 span=29h fresh=10 pass=10 near=0 rescued=0. Apex links.
    "lngprime.com": [
        "https://lngprime.com/feed/",
    ],
    # Offshore Engineer (oedigital.com) — offshore E&P / subsea. 2026-08-18:
    # items=15 span=24h fresh=15 pass=7 near=8 rescued=0 (Equinor Namibia,
    # ExxonMobil Rovuma LNG, Chevron Angola, Rosebank). /rss is the real feed;
    # /feed and /component/obrss/* return HTML. Feed and article links are www,
    # so source_domain resolves to the apex oedigital.com.
    "www.oedigital.com": [
        "https://www.oedigital.com/rss",
    ],
    # Splash247 — maritime/shipping daily. 2026-08-18: items=10 span=6h fresh=10
    # pass=3 near=7 rescued=0 — only a 6h window is visible in the feed, so
    # pass=3 in 6h is a high rate, not a marginal weekly one. Apex article links.
    "splash247.com": [
        "https://splash247.com/feed/",
    ],
    # (World Oil has a real feed too but it is DATELESS and its apex article page
    # carries no machine-readable date either, so it cannot be dated through the
    # scanner's feed path — it is covered via Google News instead. See the
    # worldoil entry in ENGLISH_NO_RSS_DOMAINS for the full autopsy.)
    # =========================================================================
    # International oil & gas / regional business press (English) -- Wave 2.
    #
    # The National (thenationalnews.com) -- UAE (Abu Dhabi) business daily. The
    # ONLY Wave 2 candidate with a runner-reachable, dated RSS feed; every
    # other Russia/CIS + Middle East source this wave is WAF-blocked,
    # paywalled, dateless, TLS-broken or geo-unindexed and lives in
    # ENGLISH_NO_RSS_DOMAINS.
    #
    # Arc Publishing CMS. The generic /rss redirects to a 404; the real
    # surface is the Arc outbound feed, PATH-SCOPED to the business category
    # on purpose (the site-wide feed is general news). Measured on the runner
    # (measure_source.yml, live 91-keyword set, 7d, --lede) 2026-08-18:
    # items=57 fresh=16 pass=6 near=10 rescued=2 -- six title/summary passes
    # plus two lede rescues, all on-beat (oil pipeline, Aramco, Libya oil &
    # gas, Hormuz, offshore, Iran crude). Item links are www, so normalize_url
    # strips to the apex thenationalnews.com -- where SOURCE_NAMES is keyed.
    "www.thenationalnews.com": [
        "https://www.thenationalnews.com/arc/outboundfeeds/rss/category/business/?outputType=xml",
    ],
    # =========================================================================
    # US + Europe oil & gas trade press (English) -- Wave 3 (2026-08-18). These
    # have a runner-reachable, dated RSS feed and are registered here; the rest
    # of the wave (E&E News, Montel, Global Energy Network) is WAF-blocked,
    # paywalled or feed-too-slow and lives in ENGLISH_NO_RSS_DOMAINS. Yields
    # measured on the runner (measure_source.yml, live 91-keyword set, 7d,
    # --lede) 2026-08-18.
    # =========================================================================
    # Oil & Gas 360 (oilandgas360.com) -- US E&P / energy-markets wire, the
    # highest-density RSS surface of this wave. 2026-08-18: items=15 span=99h
    # fresh=15 pass=14 near=1 rescued=1 -- fourteen of fifteen items match on
    # title/summary alone (Hormuz, Brent, shale, diesel, Permian gas pipeline,
    # Chevron Angola, Alaska LNG, Petro-Victory Brazil). Article links are apex.
    "oilandgas360.com": [
        "https://oilandgas360.com/feed/",
    ],
    # Offshore Energy (offshore-energy.biz) -- Dutch (Navingo) offshore E&P /
    # energy wire; heavy FPSO / offshore-drilling / North Sea coverage. WordPress
    # /feed/, only ~10 items deep but every one on-beat: 2026-08-18 items=10
    # span=24h fresh=10 pass=10 near=0 (Brava/Ecopetrol Brazil, FPSO deck
    # machinery, SLB/Shell, Chevron Angola discovery, North Sea oil & gas
    # project). Feed and article links are www, so normalize_url strips to the
    # apex offshore-energy.biz -- where SOURCE_NAMES is keyed.
    "www.offshore-energy.biz": [
        "https://www.offshore-energy.biz/feed/",
    ],
    # Natural Gas Intelligence (naturalgasintel.com) -- THE US natural-gas price
    # wire; every item is on-beat by construction. WordPress /feed/, ~10 items
    # but a fast-moving window: 2026-08-18 items=10 span=6h fresh=10 pass=10
    # near=0 (Henry Hub futures, LNG buildout, Rovuma LNG, Vaca Muerta shale,
    # Venture Global CP2). pass=10 in a 6-hour window is a high rate, not a
    # marginal weekly one. Feed and article links are www, so normalize_url
    # strips to the apex naturalgasintel.com -- where SOURCE_NAMES is keyed.
    "www.naturalgasintel.com": [
        "https://www.naturalgasintel.com/feed/",
    ],
    # =========================================================================
    # India oil & gas / business press (English) -- Wave 4 (2026-08-18). India
    # is the world's third-largest oil importer, so its trade press carries the
    # crude-sourcing / refining / Hormuz-exposure beat at a granularity no
    # Brazilian or Gulf source matches (Russian-crude discounts, ONGC / Reliance,
    # LPG allocation, India's own import parity). The three below have a
    # runner-reachable, dated RSS feed and are registered here; Moneycontrol
    # (feed WAF-blocked) is GNews-covered in ENGLISH_NO_RSS_DOMAINS. Yields
    # measured on the runner (measure_source.yml, live 91-keyword set, 7d window,
    # --lede) 2026-08-18.
    # =========================================================================
    # ET EnergyWorld (energy.economictimes.indiatimes.com) -- the Economic Times
    # energy vertical, a PURE oil & gas / power section and the anchor of this
    # wave. items=20 span=56h fresh=20 pass=14 near=6 rescued=1 -- fourteen of
    # twenty items in a 56h window match on title/summary alone, every one
    # on-beat (ONGC gas, India tanker exports, Saudi Hormuz loadings, US refiners,
    # Brent/crude, Russia-India gasoline, BPCL diesel, offshore West Asia). Feed
    # and article links are on the energy subdomain (no www), so the resolved
    # source_domain is energy.economictimes.indiatimes.com -- keyed there in
    # SOURCE_NAMES. Deliberately NOT the bare economictimes.indiatimes.com apex,
    # which would mislabel generic ET business copy as ET EnergyWorld.
    "energy.economictimes.indiatimes.com": [
        "https://energy.economictimes.indiatimes.com/rss/oil-and-gas",
    ],
    # Mint (livemint.com) -- India business daily (HT Media), /rss/industry
    # section. A GENERAL business-industry feed, thinner on O&G than ET
    # EnergyWorld: items=35 span=128h fresh=35 pass=5 near=30 rescued=0 -- five
    # title/summary passes over 7d, four on-beat (L&T offshore energy order, a
    # covert-Mideast-oil-flows markets piece, and the govt LPG-refinery
    # allocation x2) and one an "edible oil" false positive on the substring
    # keyword `oil`. Clears the >=3/7d-on-beat bar for a general-business domain,
    # though most of its on-beat items are also carried by ET EnergyWorld /
    # BusinessLine / Moneycontrol -- it is registered for the marginal
    # markets-desk pieces (e.g. covert Mideast flows) those miss. Feed and
    # article links are www, so normalize_url strips to the apex livemint.com --
    # where SOURCE_NAMES is keyed. Watch the edible/coconut-oil `oil` false
    # positive; it is bounded (title-only, url-keyed, lands at most once).
    "www.livemint.com": [
        "https://www.livemint.com/rss/industry",
    ],
    # The Hindu BusinessLine (thehindubusinessline.com) -- India's financial
    # daily (The Hindu group). Two section feeds registered, the same
    # narrow-over-general call as Gazeta do Povo: the site-wide feed is general
    # news, these two are the sector.
    #   - markets/commodities: the anchor. items=60 span=222h fresh=48 pass=23
    #     near=25 rescued=1 -- twenty-three passes in the 7d window, ~21 on-beat
    #     (crude / Brent / oil futures, OPEC & IEA demand, Hormuz-blocked refiner
    #     buying, HPCL / MRPL crude tenders, India Russian-crude record). The two
    #     off-beat are "coconut oil" prices (Onam demand) on the substring `oil`.
    #   - economy: items=60 span=56h fresh=60 pass=6 near=54 rescued=0 -- six
    #     passes, ~5 on-beat and DISTINCT from commodities (LNG-truck logistics,
    #     India tanker exports, ethanol-blend fuel policy, Chabahar vs US
    #     sanctions), one "coconut oil" false positive. Registered because those
    #     downstream / logistics / policy stories are India-domestic and neither
    #     the commodities feed nor the global wires carry them.
    # Feed and article links are www, so normalize_url strips to the apex
    # thehindubusinessline.com -- where SOURCE_NAMES is keyed. Watch the recurring
    # coconut/edible-oil `oil` false positive (bounded: title-only, url-keyed).
    "www.thehindubusinessline.com": [
        "https://www.thehindubusinessline.com/markets/commodities/feeder/default.rss",
        "https://www.thehindubusinessline.com/economy/feeder/default.rss",
    ],
    # --- India RSS REJECTED (measured 2026-08-18 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # Business Standard (business-standard.com), /rss/industry-217.rss: a
    # general business-industry feed. items=35 span=31h fresh=35 pass=1 near=34
    # rescued=0 -- the single pass ("5-yr age extension for EV, CNG, hydrogen
    # commercial vehicles") is itself off-beat (vehicle policy). Below the
    # >=3/7d on-beat bar. Only the industry feed was measured; a future wave
    # could try its markets/commodities feed before concluding the domain is
    # dead.
    # "www.business-standard.com": [
    #     "https://www.business-standard.com/rss/industry-217.rss",
    # ],

    # =======================================================================
    # WAVE 5 ANCHORS (2026-09-14) — five parallel branches, ONE block each.
    # A wave appends its entries ONLY between its own BEGIN/END markers and
    # leaves every fence line alone: >=4 untouched lines between blocks is what
    # keeps the five diffs in non-overlapping hunks, so the branches merge
    # without conflicts. Empty is the correct state until a wave lands.
    # =======================================================================
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- BEGIN
    # Measured 2026-09-14 on the runner (measure_source.yml) against the LIVE
    # keyword set -- which is now 187 terms (78 exact / 109 substring), not the
    # 91 the README paragraph quotes. Window 168h, --lede on.
    #
    # SURFACE RULE APPLIED IN THIS WAVE: keep RSS when the outlet's own feed is
    # SECTION-scoped and its measured yield is within reach of the keyword-scoped
    # GNews surface; go GNews when the feed is WAF-blocked, frozen, a stub, or a
    # general firehose whose yield is a fraction of it (the Gulf News /
    # Al Jazeera call of 2026-08-18). Only 3 of this wave's 24 candidates kept
    # RSS; the other 21 are in ENGLISH_NO_RSS_DOMAINS, each with its numbers.
    #
    # NOTE FOR THE WHOLE WAVE: display names live in SOURCE_NAMES
    # (_clipinator_shim.py), which carries NO Wave 5 anchor, so every outlet this
    # wave registers renders as its bare domain (enrich.source_name_for falls
    # back to the domain) until a post-merge pass adds the names in one change.
    #
    # Financial Times (ft.com) -- paywalled, but both section feeds answer from
    # the runner, are dated and carry title+summary, which is all a title/summary
    # match needs. Two feeds registered, the same narrow-over-general call as The
    # Hindu BusinessLine:
    #   - commodities: the anchor. items=25 span=109h fresh=25 pass=21 near=4
    #     rescued=0 no_body=0 fetch=0.52s -- twenty-one passes in the 7d window,
    #     ~all on-beat (Saudi East-West pipeline shut, oil at $109, record US
    #     diesel, IEA "lost period" in oil demand, Houthis in the Red Sea, Shell
    #     upstream, the KPMG / State Oil audit probe).
    #   - companies/energy: items=25 span=109h fresh=25 pass=19 near=6 rescued=0
    #     no_body=0 fetch=0.34s. Overlaps commodities heavily but is NOT a subset
    #     -- the Russia/Ukraine energy-truce claim, the Musk gas-turbine piece and
    #     the coal-vs-gas analysis land only here, while "Oil is scary again",
    #     the $109 bond sell-off and the Houthi island seizure land only in
    #     commodities. Both registered, for the union.
    #   - rss/home (the site-wide firehose) measured items=13 span=14h fresh=13
    #     pass=5 -- a 13-item stub of the front page. NOT registered.
    # GNews site:ft.com measured pass=36/7d: higher recall (it also sees FT copy
    # outside these two sections) but title-only. RSS wins here because the feeds
    # are section-scoped, fast (0.34-0.52s) and carry summaries. Feed and article
    # links are www, so normalize_url strips to the apex ft.com.
    "www.ft.com": [
        "https://www.ft.com/commodities?format=rss",
        "https://www.ft.com/companies/energy?format=rss",
    ],
    # The Guardian (theguardian.com) -- the highest-precision feed of this wave.
    # /business/oil/rss is the outlet's own oil tag: items=20 span=290h fresh=15
    # pass=11 near=4 rescued=0 no_body=0 fetch=0.49s -- eleven of the fifteen
    # fresh items pass, ~10 on-beat (Saudi pipeline satellite imagery, Houthis
    # taking an island on the oil route, oil above $100/bbl, the Dangote refinery
    # IPO, the ECB warning that the Iran war fuels inflation, the Venezuela oil
    # deal). The recurring false positive is a UK air-traffic-control live blog
    # on the substring `gas`; bounded (title-only, url-keyed, lands once).
    # Also measured, NOT registered (recorded so no later wave re-tests them):
    # business/commodities items=20 fresh=13 pass=7 (six of the seven are the
    # same stories as business/oil); environment/energy items=20 fresh=14 pass=7,
    # of which ~3 are offshore-WIND / climate-study copy on the substrings
    # `offshore` and `ethane`; business (site-wide) items=20 fresh=16 pass=6, all
    # duplicates. GNews site:theguardian.com measured pass=23/7d -- more recall
    # across the world / environment desks, but title-only and noisier; the oil
    # tag IS the beat, so RSS. Links are www -> apex theguardian.com.
    "www.theguardian.com": [
        "https://www.theguardian.com/business/oil/rss",
    ],
    # CBS News (cbsnews.com) -- registered on RSS deliberately, AGAINST the much
    # bigger GNews number, because the two surfaces carry different things:
    #   - RSS /latest/rss/world (registered): items=30 span=197h fresh=29 pass=7
    #     near=22 rescued=0 no_body=5 fetch=0.10s -- five of the seven are the
    #     global energy beat (Hormuz talks stalling, an Iranian vessel struck in
    #     Hormuz, Iran proxies compounding the oil shock, Trump on Russian
    #     refineries, Houthis widening control of the shipping lane); the other
    #     two are `missile` defence copy. /latest/rss/moneywatch measured pass=3
    #     (on-beat but a subset) and /latest/rss/main pass=4, so `world` is it.
    #   - GNews site:cbsnews.com measured pass=60/7d -- the largest number in the
    #     wave and mostly NOT usable: ~30 of the 60 are CBS local-affiliate pump
    #     copy (Michigan / Philadelphia / Sacramento / Bay Area gas prices) and
    #     ~6 are gas-station crime / food false positives ("gas station heroin",
    #     cinnamon rolls at a Sacramento station, a station shooting). Path
    #     scoping kills the noise but also the volume: site:cbsnews.com/news
    #     items=35 span=150h fresh=35 pass=8 near=27, clean but title-only and no
    #     better than the feed. Hence RSS. Links are www -> apex cbsnews.com.
    "www.cbsnews.com": [
        "https://www.cbsnews.com/latest/rss/world",
    ],
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- BEGIN
    # =========================================================================
    # US regional, Europe, Canada, Oceania mainstream -- Wave 5B (2026-09-14).
    # Nine of this wave's twenty registered outlets have a runner-reachable,
    # dated RSS feed that beats their Google News surface and are registered
    # here; the other eleven are GNews-only and live in ENGLISH_NO_RSS_DOMAINS,
    # and four candidates were rejected outright (their numbers are in the
    # ENGLISH_NO_RSS_DOMAINS Wave 5B REJECTED block). Yields measured on the
    # runner (measure_source.yml, live 91-keyword set, 7d window, --lede)
    # 2026-09-14.
    #
    # SURFACE PICK for this wave, since almost every candidate has BOTH a feed
    # and a GNews surface: take the higher measured 7-DAY yield, normalising a
    # feed whose span is shorter than the window (pass x 168/span -- legitimate
    # because the scanner polls every ~5 min and therefore sees essentially
    # every item of a 10-30 slot rolling feed), and TIE-BREAK TO RSS whenever
    # GNews is less than 2x richer, because an RSS item lands with a summary
    # while a GNews item lands title-only. Both numbers are recorded per entry
    # so the call stays auditable and reversible.
    # =========================================================================
    # Investing.com, English edition (investing.com) -- global markets /
    # commodities wire. DO NOT CONFUSE with br.investing.com, which is a
    # DIFFERENT HOST and stays in NO_RSS_DOMAINS as a Brazilian (pt-BR) source:
    # normalize_url only strips a leading "www.", so the BR subdomain never
    # resolves to this key and keeps its national classification.
    # /rss/news_11.rss (commodities & futures news): items=10 span=2h fresh=10
    # pass=6 near=4 rescued=0 no_body=4 fetch=0.10s -- SIX passes inside a TWO
    # HOUR span, all pure sector (oil settles higher after Saudi strikes,
    # Natural Gas live levels, Crude Oil WTI, London Gas Oil, Brent, NW European
    # gasoline margins), i.e. ~500/7d normalised against GNews pass=94/7d
    # (items=100 fresh=100 near=6). Part of the feed is auto-generated "Live
    # levels" technical-level copy: repetitive in shape but on-beat, and
    # url-keyed, so each lands at most once.
    # NOT registered: /rss/commodities.rss -- items=10 pass=6 but span="-", i.e.
    # DATELESS (every item printed [-1.0h]); the scanner cannot persist an item
    # without published_at, the same autopsy as World Oil.
    "www.investing.com": [
        "https://www.investing.com/rss/news_11.rss",
    ],
    # France 24, English edition (france24.com) -- French public international
    # broadcaster. /en/rss is the English front page: items=24 span=10h fresh=24
    # pass=6 near=18 rescued=0 fetch=0.31s -- six passes in a TEN HOUR span
    # (~100/7d normalised) against GNews pass=46/7d, so the feed wins by >2x.
    # Pass set is the supply-chokepoint beat (coal re-routing around a shut
    # Strait of Hormuz, Yemen displacement, an Iranian missile/drone "axis"
    # analysis, Russian strikes on Ukraine). One false positive in six, on the
    # substring `gas`: "tear gas" at a Turkish protest -- bounded, title-only,
    # url-keyed.
    # NOT registered: /en/business-tech/rss -- items=29 span=1547h fresh=6
    # pass=4 and ALL FOUR passes are false positives (champagne on `gas`,
    # Canadian retaliatory tariffs on `embargo`, Bombardier on `gasoline`, a VW
    # defence plant on `missile`). A section feed usually beats the front page
    # (the Gazeta do Povo lesson); here it is measurably the worse pick.
    # Feed and article links are www, so normalize_url strips to the apex
    # france24.com.
    "www.france24.com": [
        "https://www.france24.com/en/rss",
    ],
    # Le Monde in English (lemonde.fr) -- the English edition of the French
    # daily, published under /en/. The language check this programme requires
    # (GNews hl=en-US site:lemonde.fr) came back in ENGLISH, and so does the
    # feed, so the domain qualifies as an international English source.
    # /en/rss/une.xml (English front page): items=18 span=13h fresh=18 pass=3
    # near=15 rescued=0 fetch=0.08s -- three passes in a THIRTEEN HOUR span
    # (~39/7d normalised) against GNews pass=11/7d, all three on-beat (Houthi
    # control of the Bab al-Mandab strait, a Strait of Hormuz reportage, the
    # Russian drone strike on a train near the Polish border).
    # NOT registered: /en/economy/rss_full.xml -- items=20 span=118h fresh=20
    # pass=1, and its single pass (the Bab al-Mandab piece) is already carried
    # by une.xml; the economy section alone is thinner than the front page.
    # Feed and article links are www, so normalize_url strips to the apex
    # lemonde.fr.
    "www.lemonde.fr": [
        "https://www.lemonde.fr/en/rss/une.xml",
    ],
    # The Irish Times (irishtimes.com) -- Irish daily of record; the Arc
    # outboundfeeds BUSINESS section. items=100 span=168h fresh=99 pass=10
    # near=89 rescued=0 fetch=2.42s against GNews pass=16/7d (items=100
    # fresh=100): GNews is 1.6x richer, i.e. inside this wave's 2x tie-break, so
    # RSS wins for the summary it carries. Pass set is Irish energy policy plus
    # the global price tape (Ireland "seriously considering" fracked US gas for
    # a Clare LNG reserve, Hormuz, oil past $100 / $105, Brent near $101, diesel
    # above EUR 2 and home heating oil). Two false positives in ten: a UK GDP
    # item where `oil` appears in passing, and a salmon-tag story matching the
    # substring `upstream`.
    # FETCH-TIME WATCH: 2.42s is the slowest feed of this wave against the
    # scanner's 4s FEED_TIMEOUT. It is inside budget, so it deliberately gets NO
    # FEED_TIMEOUT_OVERRIDES entry (that dict is for feeds measured ABOVE 4s) --
    # but if this outlet ever reads as dead, re-measure with --feed-timeout 12
    # before blaming a WAF.
    # Feed and article links are www, so normalize_url strips to the apex
    # irishtimes.com.
    "www.irishtimes.com": [
        "https://www.irishtimes.com/arc/outboundfeeds/feed-business/?outputType=xml",
    ],
    # City A.M. (cityam.com) -- London financial daily. WordPress /feed/:
    # items=30 span=13h fresh=30 pass=3 near=27 rescued=0 fetch=0.40s -- three
    # passes in a THIRTEEN HOUR span (~39/7d normalised) against GNews
    # pass=4/7d, the widest RSS-over-GNews margin of this wave. Pass set: the
    # regulator triple probe into Prax Group oil audits (the Lindsey refinery
    # owner -- a UK downstream story no other registered source carries), FTSE
    # 100 with oil at a four-month high, and UK sanctions on Israeli
    # settlements. One false positive in three: a London-buses column whose
    # summary matches the substring `refin` (refinancing) -- bounded,
    # title/summary-only, url-keyed.
    # Feed and article links are www, so normalize_url strips to the apex
    # cityam.com.
    "www.cityam.com": [
        "https://www.cityam.com/feed/",
    ],
    # Financial Post (financialpost.com) -- Postmedia Canadian business daily
    # and the strongest Calgary-oil-patch candidate of this wave. TWO feeds
    # registered, the same narrow + general pair as The Hindu BusinessLine:
    #   - /feed/ (site-wide): items=10 span=2h fresh=10 pass=1 near=9 rescued=0
    #     fetch=0.44s -- one pass inside a TWO HOUR span (~84/7d normalised)
    #     against GNews pass=16/7d, so the feed wins by >2x. A 10-slot feed on a
    #     2h churn is only usable BECAUSE the scanner polls every ~5 min.
    #   - /category/commodities/energy/feed/: items=10 span=569h fresh=4 pass=2
    #     near=2 rescued=0 fetch=0.51s -- a low-frequency section (4 fresh in
    #     7d) but 2/2 on-beat and Alberta-specific (the Tamarack / Headwater
    #     $10-billion oil-play consolidation; Alberta energy ambitions vs
    #     caribou habitat), exactly the class the 10-slot site-wide feed rolls
    #     past between polls.
    # GNews pass set for reference (16/7d): Enbridge buying Tallgrass, OPEC+
    # quotas, Trans Mountain flows to Asia, IEA demand warnings.
    # Feed and article links are apex (no www), so the key is the apex.
    "financialpost.com": [
        "https://financialpost.com/feed/",
        "https://financialpost.com/category/commodities/energy/feed/",
    ],
    # Calgary Herald (calgaryherald.com) -- Postmedia Calgary daily, i.e. the
    # oil patch hometown paper. ENERGY SECTION ONLY:
    # /category/business/energy/feed/ items=10 span=238h fresh=5 pass=2 near=3
    # rescued=0 fetch=0.70s -- both passes are Chris Varcoe oil-patch columns
    # ("Wind at their backs": public support for exporting Canadian oil as crude
    # jumps above $100; Tamarack acquiring Headwater in a $3.2B deal), 100%
    # on-beat.
    # NOT registered: the site-wide /feed/ -- items=10 span=23h fresh=10 pass=0,
    # a general city-news firehose that yielded NOTHING in a 23h window.
    # GNews alternative: pass=2/7d (items=37 fresh=34), and one of those two is
    # a drone-manufacturing false positive -- so the section feed matches it on
    # volume and beats it on precision.
    # Low volume is intrinsic (a city daily); registered for the Varcoe column
    # and the Alberta deal flow the national wires do not carry.
    # Feed and article links are apex (no www), so the key is the apex.
    "calgaryherald.com": [
        "https://calgaryherald.com/category/business/energy/feed/",
    ],
    # ABC News Australia (abc.net.au) -- the Australian public broadcaster,
    # BUSINESS section feed (/news/feed/51120/rss.xml): items=25 span=12h
    # fresh=25 pass=2 near=23 rescued=0 fetch=0.32s -- two passes in a TWELVE
    # HOUR span (~28/7d normalised) against GNews pass=34/7d, inside the 2x
    # tie-break; and the feed is measurably CLEANER, because the GNews pass set
    # carries a recurring emergency-services class ("Smell of Gas (MIRRABOOKA,
    # CITY OF STIRLING, ... CAD-ID: 815747)", 2 of the first 12) that the
    # business feed never produces. Both feed passes on-beat (Houthis seizing
    # more Red Sea islands, NATO on Russian border drones); the domestic
    # gas-reservation policy fight that dominates the GNews set also lands here
    # between polls.
    # Feed and article links are www, so normalize_url strips to the apex
    # abc.net.au.
    "www.abc.net.au": [
        "https://www.abc.net.au/news/feed/51120/rss.xml",
    ],
    # Australian Financial Review (afr.com) -- Australia business daily, hard
    # paywall. The paywall does not matter on this surface: both feeds carry a
    # dated title + summary, which is where the match lands (same shape as the
    # paywalled GNews sources, but with a summary). TWO feeds registered:
    #   - /rss/markets.xml: items=20 span=109h fresh=20 pass=7 near=13
    #     rescued=0 fetch=0.15s -- ~10.8/7d normalised, 7/7 on-beat (the ASX
    #     against the oil spike, oil toward US$110/bbl, Woodside, traders
    #     pricing no end to high oil as the Houthis reach the Red Sea).
    #   - /rss/feed.xml (front page): items=20 span=9h fresh=20 pass=3 near=17
    #     rescued=0 fetch=0.05s -- three passes in a NINE HOUR span (~56/7d
    #     normalised), two of them Hormuz policy pieces (Australia should
    #     mobilise Asian partners to reopen the strait; Trump on reimbursement)
    #     that markets.xml does not carry.
    # Together far above GNews pass=17/7d (items=100 fresh=78).
    # NOT feeds: /rss/companies/energy, /rss/business.xml, /rss.xml and /feed
    # are all HTTP 404 -- the AFR feed index is /rss/<name>.xml only.
    # Feed and article links are www, so normalize_url strips to the apex
    # afr.com.
    "www.afr.com": [
        "https://www.afr.com/rss/markets.xml",
        "https://www.afr.com/rss/feed.xml",
    ],
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- BEGIN
    # =======================================================================
    # Wave 5C (2026-09-14) -- Africa, Middle East & North-East Asia mainstream.
    # Six of the wave's 24 candidates have a feed the scanner's fetcher reaches
    # from the datacenter runner AND that carries dates; they are registered
    # here. The other eighteen went to ENGLISH_NO_RSS_DOMAINS -- eight because
    # their own feed answers 200 at home and HTTP 403 on the runner (WAF
    # variant A: africaoilgasreport, energycapitalpower, agbi, timesofisrael,
    # english.ahram, english.alarabiya, oilandgasmiddleeast, japantimes
    # /news/business), two because the feed is dateless or stale (asia.nikkei
    # /rss/feed/nar: 50 items, span=-, which the scanner cannot persist;
    # japantimes /feed/topstories/: 5 items, fresh=0) and the rest because
    # GNews measured several times richer than the feed. Yields measured on the
    # runner (measure_source.yml, live 91-keyword set, 7d window) 2026-09-14.
    # No feed of this wave needed more than 4s, so the wave's
    # FEED_TIMEOUT_OVERRIDES block stays empty (slowest: BusinessLive 1.74s).
    # =======================================================================
    # Business Day / BusinessLive (businesslive.co.za) -- South Africa's
    # financial daily (Arena Holdings). The feed is not a preference here, it
    # is the WHOLE source: Google News does not index the domain at all
    # (site:businesslive.co.za AND site:businesslive.co.za/bd both returned
    # items=0 on 2026-09-14). Arc XP outbound feed: items=100 span=297h
    # fresh=99 pass=10 near=89 fetch=1.74s -- ~6 of the ten on-beat (oil at
    # $108 after the Saudi pipeline attack, the Dangote refinery IPO, "SA's
    # refinery collapse cost R76bn in extra fuel imports", Trump on Russian
    # refinery strikes, EPA power-plant gas rules). Noise class to watch, all
    # bounded (title-only, url-keyed): a "LETTERS TO THE EDITOR" digest and
    # company copy matching the substring keywords (`sanction`, `refin`). Feed
    # and article links are www, so normalize_url strips to the apex
    # businesslive.co.za.
    "www.businesslive.co.za": [
        "https://www.businesslive.co.za/arc/outboundfeeds/rss/?outputType=xml",
    ],
    # Pipeline Oil & Gas Magazine (pipelineoilandgasnews.com) -- Gulf /
    # Middle East upstream-to-downstream trade monthly. Same shape as
    # BusinessLive above and a much richer feed: site:pipelineoilandgasnews.com
    # returns items=0 on GNews, so the feed is the only surface --
    # items=30 span=121h fresh=30 pass=14 near=16 fetch=1.48s, the pass set
    # ~all on-beat (the Russia-Ukraine energy truce and the diesel / refinery
    # strikes behind it, the Dangote refinery IPO, the Asian LNG market, Houthi
    # attacks on Saudi Arabia, US EPA carbon standards for gas power plants).
    # Feed and article links are www -> apex pipelineoilandgasnews.com.
    "www.pipelineoilandgasnews.com": [
        "https://www.pipelineoilandgasnews.com/feed/",
    ],
    # Middle East Eye (middleeasteye.net) -- London-based Middle East daily and
    # the fastest-moving feed of the wave: items=20 span=7h fresh=20 pass=14
    # near=6 fetch=0.21s, i.e. fourteen title/summary passes inside a
    # SEVEN-HOUR window (an IRGC report of a supertanker mined in the Strait of
    # Hormuz, the Saudi East-West pipeline attack and its five-week repair,
    # Houthi control of Bab al-Mandab, Iran-related sanctions on VTB, Saudi
    # strikes on Yemen). Its GNews surface was measured too (pass=59/7d) and is
    # deliberately NOT used: the feed rate is several times higher and carries
    # bodies. Watch the bounded off-beat class the geopolitics keywords bring
    # in (`sanction` / `ceasefire` on Gaza-diplomacy copy, e.g. a French-TV
    # censorship item). Feed and article links are www -> apex
    # middleeasteye.net.
    "www.middleeasteye.net": [
        "https://www.middleeasteye.net/rss",
    ],
    # Tehran Times (tehrantimes.com) -- Iran's English daily. It does NOT
    # geo-block the datacenter runner (the open question when the candidate was
    # proposed): items=30 span=26h fresh=30 pass=7 near=23 fetch=0.71s, ~6.5
    # passes a day of the Iran-side read on the beat -- the Hormuz and Bab
    # al-Mandab chokepoints, the Salman field's $1.2b revenue, Ansarallah's
    # Yemen advance, Iran sanctions. Richer than its GNews surface
    # (pass=11/7d), which is why the feed is the registered one. False-positive
    # caveat measured on the GNews side and equally possible here: the exact
    # keyword `Brent` matches the PERSON "Brent Scowcroft"; bounded
    # (title-only, url-keyed). Feed and article links are www -> apex
    # tehrantimes.com.
    "www.tehrantimes.com": [
        "https://www.tehrantimes.com/rss",
    ],
    # Daily Sabah (dailysabah.com) -- Turkish English daily. Two SECTION feeds,
    # the same narrow-over-general call as The Hindu BusinessLine. The numeric
    # /rssFeed/13 URL found first serves the same payload as /rss/business, and
    # the named path is registered instead; /rssFeed/9 and /rssFeed/32 serve a
    # third, generic payload (36 items span=10h pass=6) that adds nothing.
    #   - business: items=34 span=84h fresh=34 pass=7 near=27 fetch=0.45s --
    #     the Saudi pipeline drone strike, oil above $100 with US diesel at a
    #     record, Africa's biggest refinery opening to public ownership, US gas
    #     prices inside a CPI story; two off-beat (a 10-year Treasury yield
    #     piece and a Turkish missile test).
    #   - business/energy: items=50 span=791h fresh=10 pass=5 near=5
    #     fetch=0.37s -- slower-moving but denser: half of everything it
    #     published inside the 7d window passes.
    # GNews measured pass=20/7d for the domain; the two feeds together are the
    # richer surface once bodies are counted. Feed and article links are www ->
    # apex dailysabah.com.
    "www.dailysabah.com": [
        "https://www.dailysabah.com/rss/business",
        "https://www.dailysabah.com/rss/business/energy",
    ],
    # The Korea Herald (koreaherald.com) -- South Korea's English daily. The
    # site-wide /rss/newsAll is registered rather than the /rss/kh_Business
    # section -- the opposite of the BusinessLine call, and what the numbers
    # say: newsAll items=50 span=8h fresh=50 pass=4 near=46 fetch=1.58s
    # (~12 passes/day: Posco International's 532.9bn-won US shale-gas raise, HD
    # Hyundai's W435b LNG cogeneration plant, Seoul shares on rising oil
    # prices) against kh_Business items=50 span=100h fresh=50 pass=8 near=42
    # (~1.9/day). newsAll is the superset and rolls over every ~8h, well inside
    # the scan cadence. GNews alternative: pass=10/7d. Korea-desk noise to
    # watch: North-Korea `missile` / `sanction` items pass on the geopolitics
    # keywords. Feed and article links are www -> apex koreaherald.com.
    "www.koreaherald.com": [
        "https://www.koreaherald.com/rss/newsAll",
    ],
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- BEGIN
    # =====================================================================
    # Wave 5D RSS (2026-09-14) -- South & South-East Asia + Latin America
    # mainstream press with a runner-reachable, DATED feed. Every number below
    # was measured on the runner (measure_source.yml, live 91-keyword set, 7d
    # window, --lede) on 2026-09-14, under the LOWERED acceptance bar of the
    # 2026-09 programme: register on pass>=1 over 7d (or >=3 over 48h), reject
    # only a candidate whose every surface returns 0 items or whose passes are
    # >=80% off-beat. The wave's candidates WITHOUT a usable feed (The Jakarta
    # Post, The Edge Malaysia, Kaieteur News, BNamericas, Hydrocarbon
    # Processing, Hydrocarbon Engineering) are GNews-covered in
    # ENGLISH_NO_RSS_DOMAINS, where the five rejects are recorded too. No feed
    # of this wave needed more than the 4s default (slowest fetch=1.99s), so
    # the FEED_TIMEOUT_OVERRIDES Wave 5D block stays empty on purpose.
    # =====================================================================
    # The Straits Times (straitstimes.com) -- Singapore's daily of record and
    # the Asia-markets read of this wave. Section feed /news/business/rss.xml:
    # items=16 span=41h fresh=16 pass=2 near=14 rescued=1 no_body=0 fetch=0.35s
    # -- "Oil prices up over 3% following new strikes on Saudi Arabia, Strait of
    # Hormuz" and "Shares skid in Asia as oil rises, rate hikes loom", plus one
    # body-rescued near-miss: bodies ARE reachable here, unlike the GNews half
    # of this wave. The Singapore section feed was measured too and deliberately
    # NOT registered (/news/singapore/rss.xml: items=45 span=43h fresh=45 pass=2
    # near=43 -- one distinct domestic story, "diesel past $4 per litre at major
    # retailer", one "drone" false positive, against 43 general-news
    # near-misses; 45 items per 43h for ~1 story a week is the wrong trade).
    # Feed and article links are www, so normalize_url strips to the apex
    # straitstimes.com.
    "www.straitstimes.com": [
        "https://www.straitstimes.com/news/business/rss.xml",
    ],
    # CNA / Channel NewsAsia (channelnewsasia.com) -- Mediacorp's English
    # broadcaster; carries the Reuters energy wire on an Asian clock. TWO feeds
    # registered, the same narrow+general split The Hindu BusinessLine uses:
    #   - /api/v1/rss-outbound-feed?_format=xml (the general outbound feed):
    #     items=20 span=5h fresh=20 pass=3 near=17 rescued=0 no_body=0
    #     fetch=0.22s -- "Trump says Ukraine, Russia agree not to hit energy
    #     targets", "Iran nuclear chief blocked from Vienna meeting after US
    #     pressure", "Oil settles higher after Saudi strikes stoke supply
    #     worries". Three passes inside a FIVE-HOUR span: a firehose, not a
    #     weekly trickle.
    #   - the same endpoint scoped to the business category (category=6936):
    #     items=20 span=7h fresh=20 pass=2 near=18 rescued=0 no_body=0
    #     fetch=0.66s -- "Oil settles higher after Saudi strikes", "Dollar rises
    #     as Middle East conflict lifts oil, Fed hike looms". Registered
    #     ALONGSIDE the general feed because that one is world/politics-led and
    #     rotates a business item out of its 20-item window within hours.
    # Overlap between the two is url-keyed, so a story carried by both lands
    # once. Feed and article links are www -> apex channelnewsasia.com.
    "www.channelnewsasia.com": [
        "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml",
        "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6936",
    ],
    # Bangkok Post (bangkokpost.com) -- Thailand's English daily. Section feed
    # /rss/data/business.xml: items=10 span=19h fresh=10 pass=2 near=8 rescued=0
    # no_body=0 fetch=1.99s -- "Gold falls to over one-month low as oil rally,
    # inflation data boost rate hike bets" (a markets wrap naming oil as the
    # driver) and "Nigerian oil refinery launches possible largest IPO in
    # Africa". Only 10 items deep, but that is a 19h window, i.e. a
    # continuously refreshed feed. fetch=1.99s is the slowest of this wave and
    # still inside the 4s default, so NO FEED_TIMEOUT_OVERRIDES entry. Feed and
    # article links are www -> apex bangkokpost.com.
    "www.bangkokpost.com": [
        "https://www.bangkokpost.com/rss/data/business.xml",
    ],
    # VnExpress International (e.vnexpress.net) -- English edition of Vietnam's
    # largest news site; carries the regulated pump-price cycle and the Nghi
    # Son / Dung Quat refinery beat. /rss/business.rss: items=60 span=178h
    # fresh=55 pass=2 near=53 rescued=0 no_body=0 fetch=1.75s -- one on-beat
    # ("Gasoline prices increase", the fortnightly administered move) and one
    # FALSE POSITIVE on the substring `offshore` ("Indonesia's billionaire
    # Hartono family shifts at least $1.4B offshore" -- offshore BANKING, the
    # recurring class to watch on a general business feed; bounded: title-only,
    # url-keyed). Registered on the lowered bar for the Vietnamese fuel-price
    # stories nothing else on the roster carries. The English edition lives on
    # the `e.` subdomain (no www form); the apex vnexpress.net is the
    # VIETNAMESE edition and is deliberately NOT registered -- it would arrive
    # as untranslated `vi` copy.
    "e.vnexpress.net": [
        "https://e.vnexpress.net/rss/business.rss",
    ],
    # The Times of India (timesofindia.indiatimes.com) -- India's largest
    # English daily, the demand-side mass-market read. Business feed
    # /rssfeeds/1898055.cms: items=20 span=25h fresh=20 pass=4 near=16
    # rescued=1 no_body=0 fetch=0.08s -- "Oil prices jump over 2% as fresh
    # Saudi, Strait of Hormuz strikes fuel supply fears", "Oil infra hit by
    # Ukraine, Russia gets 70% of its fuel from India", "Fed, crude, dollar and
    # Iran war: 4 factors that could move gold next week", and a BRICS
    # critical-minerals item that matched on `exploration` (adjacent, not O&G).
    # A mass-market daily: its value is the India-demand angle and speed, not
    # depth -- ET (below) and ET EnergyWorld carry the same beat harder, and
    # duplicates are url-keyed. Subdomain, no www form.
    "timesofindia.indiatimes.com": [
        "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
    ],
    # The Economic Times (economictimes.indiatimes.com) -- the apex ET site,
    # registered through its OIL & GAS section feed. A SECOND ET surface on the
    # roster, and it does not collide with the already-registered
    # energy.economictimes.indiatimes.com (ET EnergyWorld): different host,
    # different desk, url-keyed dedupe across the two.
    # /industry/energy/oil-gas/rssfeeds/13352306.cms: items=50 span=26h fresh=50
    # pass=8 near=42 rescued=0 no_body=0 fetch=0.55s -- eight passes in a 26h
    # window, seven on-beat ("Natural gas imports rise despite price surge",
    # India-China energy cooperation talks, "The world wishes it had a 'diesel
    # printer'", "Oil markets survived the Iran war sprint", "India, Asian peers
    # pile up $7.4 billion spot-gas bill, turning wary of LNG", "Crude pool
    # drying up as Middle East conflict drags on, refiners wary"), one off-beat
    # infra-order item that matched on `pipeline` (KEC International). The
    # 2026-08-18 note next to ET EnergyWorld warns that the BARE apex feed would
    # mislabel generic ET business copy -- which is why the SECTION feed is
    # registered here and the site-wide ET feed is not. SOURCE_NAMES is outside
    # this wave's blocks, so the display name falls back to the domain.
    "economictimes.indiatimes.com": [
        "https://economictimes.indiatimes.com/industry/energy/oil-gas/rssfeeds/13352306.cms",
    ],
    # Business Standard (business-standard.com) -- RE-TEST of the 2026-08-18
    # "India RSS REJECTED" note above, which measured only /rss/industry-217.rss
    # (pass=1 over 31h), refused it under the old >=3/7d on-beat bar and asked a
    # future wave to try the markets/commodities feed before calling the domain
    # dead. It clears: /rss/markets-106.rss items=35 span=81h fresh=35 pass=6
    # near=29 rescued=0 no_body=0 fetch=0.27s -- "Asian shares slide as oil
    # spike fuels inflation, rate hike concerns", "Oil prices jump over 3% after
    # new strikes on Saudi Arabia, Strait of Hormuz", "Fed decision, inflation,
    # oil may drive markets in holiday-shortened week", "Torrent Gas
    # consolidated net profit rises 133.21%". TWO false positives, both bounded
    # (title-only, url-keyed): the EXACT keyword `SLB` matches India's
    # Securities Lending & Borrowing segment ("Stock market holiday: BSE, NSE
    # closed today") and not Schlumberger, and an FPI-flows piece matched
    # `crude`/`oil` from its summary. Only the markets feed is registered; the
    # industry feed above stays rejected. www -> apex business-standard.com.
    "www.business-standard.com": [
        "https://www.business-standard.com/rss/markets-106.rss",
    ],
    # Dawn (dawn.com) -- Pakistan's English daily of record and the RICHEST RSS
    # surface of this wave. /feeds/business: items=30 span=88h fresh=30 pass=15
    # near=15 rescued=0 no_body=0 fetch=0.32s -- fifteen of thirty items match
    # on title/summary alone and nearly all are on-beat: the fortnightly
    # petrol/HSD price notifications ("Govt increases petrol by Rs4.42 per
    # litre, HSD by Rs6.10"), the petroleum-levy talks, "Brent crude at $107 per
    # barrel following new strikes on Saudi Arabia, Strait of Hormuz", LPG and
    # weekly-inflation fuel baskets, a Qatari LNG cargo at Port Qasim,
    # Bangladesh LNG terminals, and an upstream licensing row (Jura Energy).
    # Pakistan is a pure import-parity market, so this is the demand-side read
    # on the same crude the Brazilian desk watches. One bounded false-positive
    # class: `oil` inside edible-oil corporate/graft items ("Unity Foods CEO").
    # www -> apex dawn.com.
    "www.dawn.com": [
        "https://www.dawn.com/feeds/business",
    ],
    # The Astana Times (astanatimes.com) -- Kazakhstan's English daily, the
    # Caspian upstream read (Tengiz / Kashagan / CPC). /feed/: items=10 span=11h
    # fresh=10 pass=2 near=8 rescued=0 no_body=0 fetch=0.20s -- "Kashagan
    # Expansion Could Reshape Kazakhstan's Oil Export Needs, Experts Say" and
    # "Oil Transport via Kazakhstan-China Pipeline Rises to 13.1 Million Tons",
    # both squarely on-beat, inside an ELEVEN-HOUR window: a 10-item WordPress
    # feed that turns over fast, which the 5-minute scan harvests continuously.
    # Feed and article links are apex (no www).
    "astanatimes.com": [
        "https://astanatimes.com/feed/",
    ],
    # Trend News Agency (en.trend.az) -- Baku-based Caspian wire, English
    # edition: Azerbaijan / Kazakhstan / Turkmenistan volumes, BTC and the
    # Southern Gas Corridor. /feeds/index.rss: items=25 span=6h fresh=25 pass=2
    # near=23 rescued=1 no_body=0 fetch=1.08s -- "Kazakhstan's cargo turnover
    # edges up 0.8% despite rail, pipeline declines" and "Baker Hughes, Venture
    # Global ink agreements on US LNG infrastructure", plus one body-rescued
    # near-miss, inside a SIX-HOUR span (a true wire, ~100 items/day). The
    # general index feed is registered rather than a section feed because the
    # agency exposes no oil & gas section RSS; the near-misses are regional
    # economy / politics copy. The English edition is the `en.` subdomain -- the
    # apex trend.az 301-redirects to the Russian/Azeri site and is NOT
    # registered.
    "en.trend.az": [
        "https://en.trend.az/feeds/index.rss",
    ],
    # MercoPress (en.mercopress.com) -- South Atlantic News Agency
    # (Montevideo), the Mercosur / Falklands desk. /rss/: items=10 span=48h
    # fresh=10 pass=1 near=9 rescued=0 no_body=0 fetch=0.33s -- and that single
    # pass is a FALSE POSITIVE ("Falklands RAF's Voyager tanker back in MPA", an
    # air-to-air refuelling aircraft matching `tanker`). The domain was
    # therefore cross-checked on the OTHER surface before being registered:
    # GNews en-US site:en.mercopress.com over 7d returns items=68 fresh=61
    # pass=5 near=56, and those are on-beat -- "Argentina files lawsuit against
    # oil firms operating off Malvinas", "Brent climbs back above US$100 to its
    # highest level since late July", "Argentina claims Chile is committed to no
    # logistic support for Falklands' oil industry". RSS is the registered
    # surface (bodies reachable, and a 10-item/48h feed is harvested
    # continuously by the 5-minute scan, which a one-shot 48h measurement
    # under-counts); the GNews numbers are recorded here only as the on-beat
    # proof. Watch the `tanker` naval/RAF false positive -- this is a
    # military-heavy beat. Subdomain, no www form.
    "en.mercopress.com": [
        "https://en.mercopress.com/rss/",
    ],
    # Buenos Aires Times (batimes.com.ar) -- Argentina's English-language paper
    # (Perfil group), the Vaca Muerta / YPF read. /feed: items=100 span=405h
    # fresh=42 pass=6 near=36 rescued=0 no_body=0 fetch=0.36s -- six passes
    # inside the 7d window: "Argentina's YPF says close to signing more LNG
    # sales contracts", "Milei steps up pressure on oil companies over
    # Malvinas", "Argentina files lawsuit against oil firms operating off
    # Malvinas", "Oil and crop prices save Milei from surging demand for
    # dollars", plus a naval-base piece that matched `exploration` and an
    # opinion column that matched `sanction`. The feed is 100 items / ~17 days
    # deep, so the 7d window is covered with margin. GNews was measured for
    # comparison (items=46 fresh=42 pass=4) and is strictly worse than the feed,
    # hence RSS. www -> apex batimes.com.ar.
    "www.batimes.com.ar": [
        "https://www.batimes.com.ar/feed",
    ],
    # Mexico News Daily (mexiconewsdaily.com) -- English daily on Mexico, the
    # Pemex / Dos Bocas read for a desk that does not read Spanish. /feed/:
    # items=10 span=28h fresh=10 pass=1 near=9 rescued=0 no_body=0 fetch=0.21s
    # -- one pass in a 28h window and it is exactly the beat ("Pemex contains
    # another oil spill in the Gulf of Mexico"). Thin by construction (a
    # 10-item general-interest feed), registered under the lowered 2026-09 bar
    # because nothing else on the roster covers Pemex in English; the Spanish
    # GNews route stays as it is (this wave adds no non-English language). Feed
    # and article links are apex (no www).
    "mexiconewsdaily.com": [
        "https://mexiconewsdaily.com/feed/",
    ],
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- BEGIN
    # =======================================================================
    # O&G / refining / shipping trade press & institutions (English) -- Wave 5E
    # (2026-09-14). Every number below was measured ON THE RUNNER
    # (measure_source.yml, live 91-keyword set, hours=168, --lede) on
    # 2026-09-14, under the LOWERED acceptance bar of the 2026-09 international
    # programme (register any surface with pass >= 1 over 7d; reject only a
    # candidate whose every surface returns 0 items or whose passes are >=80%
    # off-beat). The eleven below have a runner-reachable, DATED feed; the other
    # fourteen outlets of the wave are WAF-blocked, dateless, credential-walled
    # or feed-frozen and live in ENGLISH_NO_RSS_DOMAINS.
    #
    # FOLLOW-UP this wave could not do: FEED_STALE_HOURS has no Wave 5 anchor, so
    # it was left untouched -- eia.gov (newest item 102h old at measurement, one
    # Today-in-Energy note per weekday) will nag against the 48h default every
    # run until it gets an entry, and worldpipelines / tanksterminals (~1.3-1.7
    # days between items) will nag occasionally.
    # =======================================================================
    # LNG Industry (lngindustry.com) -- Palladian Publications' pure-LNG trade
    # daily; on-beat by construction. items=20 span=146h fresh=20 pass=20 near=0
    # rescued=0 no_body=0 fetch=0.55s -- twenty of twenty items inside the 7d
    # window match on title/summary alone (floating-LNG pre-FEED, PETRONAS /
    # JOGMEC, Rovuma LNG, LNG bunkering, LNG carriers, Williams / Momentum
    # midstream). Feed and article links are www, so normalize_url strips to the
    # apex lngindustry.com -- where SOURCE_NAMES would be keyed.
    "www.lngindustry.com": [
        "https://www.lngindustry.com/rss/lngindustry.xml",
    ],
    # World Pipelines (worldpipelines.com) -- Palladian's midstream / pipeline
    # title, same CMS and same /rss/<title>.xml feed shape as LNG Industry.
    # items=20 span=599h fresh=4 pass=4 near=0 rescued=0 fetch=0.53s -- a slow
    # title (20 items spread over 25 days), so only four items fall inside the 7d
    # window, and all four pass: Baker Hughes / Venture Global gas infrastructure,
    # Black & Veatch FLNG liquefaction, and two pipeline-integrity features. Two
    # of the four are vendor / technical features rather than news, which is what
    # a pipeline-engineering title publishes; kept for the midstream-equipment
    # beat no other registered source carries. Feed and article links are www ->
    # apex worldpipelines.com.
    "www.worldpipelines.com": [
        "https://www.worldpipelines.com/rss/worldpipelines.xml",
    ],
    # Tanks and Terminals (tanksterminals.com) -- Palladian's storage / terminals
    # title. items=20 span=792h fresh=6 pass=5 near=1 rescued=0 no_body=1
    # fetch=0.56s -- five of the six items inside the 7d window pass (Wood /
    # ExxonMobil PNG construction, Covestro-Fertiglobe ammonia supply chain,
    # Argent LNG Albania, AFR tank-farm FEED, Petromidia refinery storage). The
    # slowest of the three Palladian titles (20 items over 33 days). It shares
    # stories with LNG Industry above (the Argent LNG and Wood / ExxonMobil items
    # ran in both); news_articles is url-keyed, so each copy lands at most once.
    # Feed and article links are www -> apex tanksterminals.com.
    "www.tanksterminals.com": [
        "https://www.tanksterminals.com/rss/tanksterminals.xml",
    ],
    # Drilling Contractor (drillingcontractor.org) -- the IADC's drilling
    # journal, plain WordPress /feed. items=10 span=95h fresh=10 pass=10 near=0
    # rescued=0 fetch=0.45s -- ten of ten on-beat (Touchstone Trinidad campaign,
    # Southern Energy Williamsburg test well, ConocoPhillips' 126-well Willow
    # plan, Var Energi / Deepsea Bergen, ExxonMobil's 20th Angola Block 15
    # discovery, Utica gas wells). Apex links (no www), so the resolved
    # source_domain is drillingcontractor.org.
    "drillingcontractor.org": [
        "https://drillingcontractor.org/feed",
    ],
    # RBN Energy (rbnenergy.com) -- US midstream / refining daily analysis blog,
    # the densest analytical feed of the wave. items=30 span=408h fresh=13
    # pass=11 near=2 rescued=0 fetch=0.12s -- eleven of the thirteen items inside
    # the 7d window pass, all on-beat (diesel cracks above $100/bbl, US refiners
    # already running hard, Summit's Double E Permian gas expansion, record Gulf
    # Coast propane inventories, Venezuela crude deals, US LNG feedgas, Canadian
    # rig counts). Apex links.
    "rbnenergy.com": [
        "https://rbnenergy.com/rss.xml",
    ],
    # Fuels & Lubes / F+L Daily (fuelsandlubes.com) -- downstream fuels, base-oil
    # and lubricants trade daily. items=20 span=182h fresh=18 pass=17 near=1
    # rescued=0 fetch=0.78s -- seventeen passes, the core of them the fuels /
    # base-oil beat no other registered source carries (Group III base-oil prices
    # +235% since the Gulf conflict began, EIA's raised Brent forecast,
    # re-refined base oil, PETRONAS / Tata used-oil recycling, Malaysia's engine-
    # oil certification order). Bounded false-positive class (title-only,
    # url-keyed, each lands at most once): the trade's data-centre-cooling and
    # coolant items, which match on `lubricant` / `propylene` without being O&G
    # news -- roughly a quarter of the pass set. Feed and article links are www
    # -> apex fuelsandlubes.com.
    "www.fuelsandlubes.com": [
        "https://www.fuelsandlubes.com/feed/",
    ],
    # Energy Monitor (energymonitor.ai) -- GlobalData's energy-transition title
    # (sister to Offshore Technology, which is GNews-covered above). items=10
    # span=55h fresh=10 pass=4 near=6 rescued=0 no_body=6 fetch=0.16s -- four
    # passes: two O&G (oil and gas electrification's power problem; Big Oil
    # profits vs clean aviation) and two the transition beat matching on a shared
    # keyword (offshore WIND economics on `offshore`, a solar-drone piece on
    # `drone`). 50% off-beat is under the >=80% rejection threshold, and the
    # on-beat half is the transition-vs-oil read that Recharge (rejected
    # 2026-08-18 as ~86% offshore wind) could not provide -- registered WITH the
    # caveat that an `offshore` hit here may well be wind. no_body=6: the
    # near-misses' bodies are unreachable from the runner, so pass is the
    # ceiling, not a floor. Feed and article links are www -> apex
    # energymonitor.ai.
    "www.energymonitor.ai": [
        "https://www.energymonitor.ai/feed/",
    ],
    # Ship & Bunker (shipandbunker.com) -- marine-fuels / bunker price wire.
    # items=10 span=72h fresh=10 pass=10 near=0 rescued=0 fetch=0.33s -- ten of
    # ten on-beat (bunker supply under the Hormuz disruption, Hormuz transits in
    # single digits, the first operational ammonia bunker vessel, truck-to-ship
    # LNG bunkering, IMO ammonia seminar). The bunker-price beat no other
    # registered source carries. Feed and article links are www -> apex
    # shipandbunker.com.
    "www.shipandbunker.com": [
        "https://www.shipandbunker.com/rss",
    ],
    # Seatrade Maritime News (seatrade-maritime.com) -- Informa's shipping daily.
    # items=50 span=176h fresh=42 pass=19 near=23 rescued=0 fetch=0.43s --
    # nineteen passes, essentially all the tanker / chokepoint beat (Iran's
    # 77-ship Hormuz blacklist, a tanker on fire after a Hormuz attack, sky-high
    # large-tanker rates, Suez returns and bunching fears, Houthis at Bab
    # al-Mandeb, the US cutting off bunkering for ships in Iranian trade).
    # near=23 is container / dry-bulk copy that never carries a keyword in the
    # title -- the correct miss, not a gap. Feed and article links are www ->
    # apex seatrade-maritime.com.
    "www.seatrade-maritime.com": [
        "https://www.seatrade-maritime.com/rss.xml",
    ],
    # Kpler (kpler.com) -- commodity-flow analytics house. Registered as RSS, not
    # GNews, deliberately: its /blog/rss.xml is a real dated feed of the Insights
    # desk. items=100 span=624h fresh=13 pass=13 near=0 rescued=0 fetch=0.12s --
    # thirteen passes inside the 7d window (crude tanker rates vs escalating
    # Hormuz risk, Iran's reach over Saudi's east-west pipeline, Rosneft's Vostok
    # exports via Sever bay, US refiners answering the global product call,
    # China's road-fuel demand -1.3 Mbd, Dangote July maintenance, LPG freight
    # through the Panama Canal). Measured the same day, site:kpler.com on GNews
    # gave pass=18/7d -- higher, but a third of it is PERENNIAL nav pages
    # ("Market Insights", "Tech", "Press", "Defense Intelligence") matching on
    # the `Kpler` keyword itself, and GNews lands title-only; the feed carries
    # articles with bodies, so it is the better surface. One bounded false
    # positive inside the feed set: a US labour-market note matching on `Kpler`
    # alone. Feed and article links are www -> apex kpler.com.
    "www.kpler.com": [
        "https://www.kpler.com/blog/rss.xml",
    ],
    # US EIA -- Today in Energy (eia.gov). The 2026-08-18 waves LOST this feed to
    # the 4s FEED_TIMEOUT (it measured 9-13s then and was recorded as an error);
    # re-measured 2026-09-14 with feed_timeout=15 it is alive, dated and
    # unchanged: items=15 span=912h fresh=2 pass=2 near=0 rescued=0 fetch=10.14s
    # -- the US government's own energy-data desk (record US crude oil production
    # in 2026; New England natural-gas prices at record discounts to Henry Hub).
    # Two passes over 7d clears the 2026-09 bar; volume is LOW by construction --
    # one Today-in-Energy note per weekday and most are electricity / renewables.
    # Registered WITH its FEED_TIMEOUT_OVERRIDES entry (10.14s + headroom);
    # without that entry this registration silently returns 0 items again, which
    # is exactly how the outlet was lost the first time. Feed and article links
    # are www -> apex eia.gov.
    "www.eia.gov": [
        "https://www.eia.gov/rss/todayinenergy.xml",
    ],
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
}


# -----------------------------------------------------------------------------
# International English RSS sources — the national/international discriminator.
#
# INVARIANT (relied on by the dashboard's News Hunter national/international
# selector): international <=> source_lang in {en, ar, ru, zh, iw, es};
# national  <=> source_lang in {pt, None}.
#
# The per-language Google News `site:` route already stamps source_lang for
# every FOREIGN query and for the English GNews-en domains (ENGLISH_NO_RSS_DOMAINS,
# via LANGUAGES["en"] + the lang_by_url map in fetcher.iter_collect/collect). But
# the international ENGLISH outlets that live in RSS_FEEDS above are fetched
# through the plain RSS feed path, which does NOT go through that stamping — so
# without this set they arrive with source_lang=None, indistinguishable from a
# Brazilian (pt) RSS feed. This frozenset is the authoritative roster of those
# RSS-registered international English domains; fetcher._entry_to_item stamps
# source_lang='en' for any item whose resolved source_domain is in it. Tagging
# them 'en' costs ZERO extra translation — the translate stage
# (pipeline._run_translation) only translates source_lang not in ('en','pt',None).
#
# MEMBERSHIP = the "Surface = RSS" rows of the README "International coverage"
# section (15 outlets: the 13 added across the four 2026-08-18 waves + CNBC and
# The Moscow Times, which predated the program). Both apex AND www variants are
# listed: _entry_to_item resolves source_domain per item and, although
# normalize_url strips a leading "www.", other item-construction paths may not.
# Deliberately EXCLUDES the Brazilian (pt) feeds and the ENGLISH_NO_RSS_DOMAINS /
# foreign-language GNews domains, which are already tagged on the GNews route.
#
# KEEP IN SYNC: when an international English RSS source is added to or removed
# from RSS_FEEDS, add/remove its apex+www forms here in the same change — or it
# silently reverts to being classified as national.
# -----------------------------------------------------------------------------
INTERNATIONAL_RSS_DOMAINS: frozenset[str] = frozenset({
    # --- Global trade press & shipping ---
    "oilprice.com", "www.oilprice.com",                         # OilPrice
    "oedigital.com", "www.oedigital.com",                       # Offshore Engineer (OE Digital)
    "gcaptain.com", "www.gcaptain.com",                         # gCaptain
    "splash247.com", "www.splash247.com",                       # Splash247
    "hellenicshippingnews.com", "www.hellenicshippingnews.com", # Hellenic Shipping News
    "lngprime.com", "www.lngprime.com",                         # LNG Prime
    "cnbc.com", "www.cnbc.com",                                 # CNBC (Energy) — predates the program
    # --- United States ---
    "oilandgas360.com", "www.oilandgas360.com",                 # Oil & Gas 360
    "naturalgasintel.com", "www.naturalgasintel.com",           # Natural Gas Intelligence
    # --- Europe ---
    "offshore-energy.biz", "www.offshore-energy.biz",           # Offshore Energy
    # --- Russia–CIS (English edition) ---
    "themoscowtimes.com", "www.themoscowtimes.com",             # The Moscow Times — predates the program
    # --- Middle East ---
    "thenationalnews.com", "www.thenationalnews.com",           # The National
    # --- India ---
    "energy.economictimes.indiatimes.com",                      # ET EnergyWorld (subdomain, no www form)
    "thehindubusinessline.com", "www.thehindubusinessline.com", # The Hindu BusinessLine
    "livemint.com", "www.livemint.com",                         # Mint / Livemint

    # =======================================================================
    # WAVE 5 ANCHORS (2026-09-14) — five parallel branches, ONE block each.
    # A wave appends its entries ONLY between its own BEGIN/END markers and
    # leaves every fence line alone: >=4 untouched lines between blocks is what
    # keeps the five diffs in non-overlapping hunks, so the branches merge
    # without conflicts. Empty is the correct state until a wave lands.
    # =======================================================================
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- BEGIN
    # The three outlets this wave kept on RSS (full numbers and the surface
    # rationale live next to their RSS_FEEDS entries above). The other 21 Wave 5A
    # candidates are GNews-en and are tagged on that route, so they must NOT be
    # listed here -- test_international_rss_source_lang asserts the two rosters
    # are disjoint.
    "ft.com", "www.ft.com",                                     # Financial Times
    "theguardian.com", "www.theguardian.com",                   # The Guardian
    "cbsnews.com", "www.cbsnews.com",                           # CBS News
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- BEGIN
    # --- US regional, Europe, Canada, Oceania mainstream (Wave 5B, 2026-09-14):
    #     the NINE outlets this wave registered in RSS_FEEDS. Apex AND www for
    #     each -- without both forms the outlet is classified as national.
    "investing.com", "www.investing.com",                       # Investing.com (English edition; br.investing.com stays pt/national)
    "france24.com", "www.france24.com",                         # France 24 (English edition)
    "lemonde.fr", "www.lemonde.fr",                             # Le Monde in English (/en/)
    "irishtimes.com", "www.irishtimes.com",                     # The Irish Times
    "cityam.com", "www.cityam.com",                             # City A.M.
    "financialpost.com", "www.financialpost.com",               # Financial Post
    "calgaryherald.com", "www.calgaryherald.com",               # Calgary Herald
    "abc.net.au", "www.abc.net.au",                             # ABC News Australia
    "afr.com", "www.afr.com",                                   # Australian Financial Review
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- BEGIN
    # --- Africa (Wave 5C, 2026-09-14) ---
    "businesslive.co.za", "www.businesslive.co.za",             # Business Day / BusinessLive (South Africa)
    # --- Middle East (Wave 5C, 2026-09-14) ---
    "middleeasteye.net", "www.middleeasteye.net",               # Middle East Eye
    "tehrantimes.com", "www.tehrantimes.com",                   # Tehran Times
    "dailysabah.com", "www.dailysabah.com",                     # Daily Sabah
    "pipelineoilandgasnews.com", "www.pipelineoilandgasnews.com",  # Pipeline Oil & Gas Magazine
    # --- North-East Asia (Wave 5C, 2026-09-14) ---
    "koreaherald.com", "www.koreaherald.com",                   # The Korea Herald
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- BEGIN
    # --- Wave 5D (2026-09-14): the 13 RSS outlets registered in RSS_FEEDS
    #     above. Apex AND www forms, per the invariant at the top of this set;
    #     the five subdomain-only outlets (e.vnexpress.net,
    #     timesofindia.indiatimes.com, economictimes.indiatimes.com,
    #     en.trend.az, en.mercopress.com) have no www form, exactly like ET
    #     EnergyWorld. Without these entries the outlet arrives with
    #     source_lang=None and the dashboard classifies it as NATIONAL. ---
    # South & South-East Asia
    "straitstimes.com", "www.straitstimes.com",                 # The Straits Times
    "channelnewsasia.com", "www.channelnewsasia.com",           # CNA / Channel NewsAsia
    "bangkokpost.com", "www.bangkokpost.com",                   # Bangkok Post
    "e.vnexpress.net",                                          # VnExpress International (subdomain)
    "timesofindia.indiatimes.com",                              # The Times of India (subdomain)
    "economictimes.indiatimes.com",                             # The Economic Times (subdomain)
    "business-standard.com", "www.business-standard.com",       # Business Standard
    "dawn.com", "www.dawn.com",                                 # Dawn (Pakistan)
    # Caspian / Central Asia
    "astanatimes.com", "www.astanatimes.com",                   # The Astana Times
    "en.trend.az",                                              # Trend News Agency (subdomain)
    # Latin America
    "en.mercopress.com",                                        # MercoPress (subdomain)
    "batimes.com.ar", "www.batimes.com.ar",                     # Buenos Aires Times
    "mexiconewsdaily.com", "www.mexiconewsdaily.com",           # Mexico News Daily
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- BEGIN
    # --- Wave 5E (2026-09-14): the eleven RSS registrations of the O&G /
    # refining / shipping trade-press wave. Apex AND www for each, per the
    # invariant above; without these the outlets are classified as NATIONAL.
    "lngindustry.com", "www.lngindustry.com",                   # LNG Industry
    "worldpipelines.com", "www.worldpipelines.com",             # World Pipelines
    "tanksterminals.com", "www.tanksterminals.com",             # Tanks and Terminals
    "drillingcontractor.org", "www.drillingcontractor.org",     # Drilling Contractor (IADC)
    "rbnenergy.com", "www.rbnenergy.com",                       # RBN Energy
    "fuelsandlubes.com", "www.fuelsandlubes.com",               # Fuels & Lubes (F+L Daily)
    "energymonitor.ai", "www.energymonitor.ai",                 # Energy Monitor (GlobalData)
    "shipandbunker.com", "www.shipandbunker.com",               # Ship & Bunker
    "seatrade-maritime.com", "www.seatrade-maritime.com",       # Seatrade Maritime News
    "kpler.com", "www.kpler.com",                               # Kpler (Insights blog feed)
    "eia.gov", "www.eia.gov",                                   # US EIA (Today in Energy)
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
})


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
    # www.argusmedia.com MOVED to ENGLISH_NO_RSS_DOMAINS on 2026-08-18 — it
    # publishes only in English and was being queried at hl=pt-BR. See the
    # measurement table there.
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
    # Argus Media: exactly the same bug as CNN above, left behind when CNN and
    # The Edge Singapore were moved on 2026-08-04. Argus publishes only in
    # English and was still being queried at hl=pt-BR&gl=BR. It did not look
    # dead — the workflow stayed green and the query answered 200 — it just
    # answered with nothing, and the row count in news_articles decayed
    # 31 -> 15 -> 3 per month and stopped on 2026-08-06.
    #
    # Measured 2026-08-18 against the live keyword set, same day, same machine:
    #
    #     window   hl=pt-BR (before)          hl=en-US + subset (after)
    #     24h      0 raw items                16 raw, 15 fresh
    #     7d       (not probed, 0 at 24h)     75 raw, 63 unique titles, 8 pass
    #
    # "pass" = matches a keyword on the title alone, i.e. what lands without
    # paying a body fetch. Argus is a paywalled trade wire, so treat the
    # lede-rescue path as unavailable and the title-only figure as the yield.
    #
    # Volume expectation: ~1 item/day, not dozens. Most of what Argus files is
    # petchems/fertilizer/metals, which our keyword set correctly ignores.
    "www.argusmedia.com",
    # -----------------------------------------------------------------------
    # International oil & gas trade press — Wave 1a (2026-08-18). These four
    # HAVE a real RSS feed, but it is unreachable from the scanner's feed path:
    # the feed fetch in fetcher._fetch_one is plain `requests` at FEED_TIMEOUT
    # with NO curl_cffi fallback, so a WAF challenge on the feed URL yields 0
    # items. Diagnosed on the runner (diagnose_feed.yml) 2026-08-18; covered via
    # Google News site: (hl=en-US) with the 12-term English subset instead.
    # Bodies are behind the same WAF, so items land title-only (empty snippet) —
    # same shape as Argus / Reuters above. Each yield below is the GNews
    # title-pass count over 7d, measured on the runner against the live set.
    # -----------------------------------------------------------------------
    # OGJ (Oil & Gas Journal). GNews pass=24/7d. Feed: residential gets a
    # Cloudflare Managed Challenge (403, cf-mitigated: challenge); the runner
    # gets HTTP 200 but an HTML interstitial (Dynatrace/Cloudflare, brotli), not
    # RSS, so feedparser / _fetch_one return 0 items. Zone-wide, no usable feed.
    "www.ogj.com",
    # Energy Voice (Aberdeen — North Sea / UK upstream). GNews pass=4/7d, but
    # near=96: Google returns ~100 items/wk mentioning the keywords in the BODY,
    # of which only ~4 carry one in the TITLE, and the bodies are unreachable, so
    # the effective yield is those 4 title-passes. Registered for the North Sea
    # beat no other source covers. Feed: zone-wide Cloudflare Managed Challenge
    # (403, cf-mitigated: challenge) from the runner on plain requests AND
    # curl_cffi impersonation alike (runner diag 2026-08-18) — nothing short of a
    # JS browser clears it, like Conjur.
    "www.energyvoice.com",
    # Offshore Technology (GlobalData). GNews pass=21/7d. Feed: DataDome (403,
    # x-datadome: protected, Server: Varnish) on plain requests. NOTE for a
    # future enhancement: curl_cffi impersonation DID clear it on the runner
    # (200, application/rss+xml, feedparser 10 entries bozo=False, 2026-08-18) —
    # but fetcher._fetch_one fetches FEEDS with plain requests only (the
    # curl_cffi fallback lives solely in fetch_html, for article bodies), so
    # end-to-end _fetch_one still returns 0 items / HTTP 403. If the feed path
    # ever grows a curl_cffi fallback this could move to RSS_FEEDS.
    "www.offshore-technology.com",
    # Rigzone — jobs board + news wire. PATH-SCOPED on /news on purpose: the bare
    # site:www.rigzone.com query is ~59% job listings ("Gas Process Engineer
    # Jobs...", the perennial "Career Center for Oil & Gas Professionals"), while
    # site:www.rigzone.com/news is essentially all articles. Measured 2026-08-18
    # on the runner: bare pass=32/7d (~half jobs) vs /news pass=31/7d with ONE
    # job title in the pass set. Feed: AWS WAF JS challenge (202,
    # x-amzn-waf-action: challenge) on the feed AND the homepage, zone-wide, from
    # residential and datacenter alike — curl cannot run the JS. A path in the
    # site: slot is honoured by Google and only affects the query; the resolved
    # article domain is still www.rigzone.com (where SOURCE_NAMES is keyed).
    "www.rigzone.com/news",
    # World Oil — upstream E&P daily. It HAS a real, fetchable RSS feed
    # (/rss?feed=news, apex, HTTP 200 from the runner, 10 full-CDATA items,
    # measured pass=10) — but that feed is DATELESS (no <pubDate>, and no dated
    # variant: /rss?feed=issue and topic feeds are dateless too). Normally
    # enrich_item recovers a missing date from the article page, but here it
    # cannot: normalize_url strips www, so it fetches the APEX article page
    # (worldoil.com/news/...), which returns 200 but carries NO machine-readable
    # date at all — no article:published_time, JSON-LD datePublished, meta date
    # or <time datetime> (only the WWW page has article:published_time, and the
    # scanner never fetches www). Verified 2026-08-18: two live scans persisted
    # ZERO worldoil rows because every item was dropped in stage 4 for a null
    # date. Google News carries the same articles WITH a real published_at, so
    # this is the reliable route: GNews pass=22/7d (items=37 fresh=37 near=15),
    # resolved to www.worldoil.com. NOT a WAF case — a dateless-source case.
    "www.worldoil.com",
    # =======================================================================
    # Premium / paywalled international wires — Wave 1b (2026-08-18). Same
    # shape as the Wave 1a GNews block above: English-only publishers that are
    # paywalled or WAF-blocked, so the only surface is Google News site:
    # (hl=en-US) + the 12-term english_keywords() subset. Bodies stay behind
    # the paywall, so items land title-only (empty snippet) — same shape as
    # Argus / Reuters / OGJ. Each yield is the GNews title-pass count over 7d,
    # measured on the runner (measure_source.yml) against the live 91-keyword
    # set on 2026-08-18.
    # =======================================================================
    # Bloomberg. GNews pass=55/7d, and the pass set is ~100% on-beat (oil, gas,
    # OPEC, Hormuz, tankers, refineries, Petrobras/Braskem, LNG, shale) — a
    # top-tier signal wire that needs NO path scope. Counter-checked the
    # opposite way: site:bloomberg.com/energy returned 0 items, because
    # Bloomberg files articles under /news/articles/... not /energy/..., so a
    # path scope would have silently zeroed the source. Bare apex is the query;
    # articles resolve to www.bloomberg.com.
    "bloomberg.com",
    # S&P Global Commodity Insights (Platts). GNews pass=44/7d on the BARE apex
    # (site:spglobal.com) — which is the ONLY queryable surface: site:spglobal.
    # com/commodityinsights returned 3 items, and site:spglobal.com/commodity-
    # insights and site:platts.com both returned 0. Google News does not honour
    # S&P's path filter, so the Rigzone /news path-scope trick is unavailable
    # here. The bare pass set splits ~half/half: the high-value Platts /
    # Commodity Insights beat (Platts price assessments, Hormuz shipping
    # traffic, crude routing, LNG netbacks, shadow-fleet premiums — content no
    # other registered source carries) and S&P Global RATINGS press releases
    # ("... Ratings Affirmed", "... Senior Notes"), a few of them archived
    # actions resurfacing with a fresh crawl date. Kept anyway, Conjur-style:
    # the ratings pollution is BOUNDED (news_articles is keyed by url, so each
    # lands at most once) and title-only, while the Platts pieces are exactly
    # the premium market intelligence this wave targets. If GNews ever honours
    # -site:spglobal.com/ratings, scope it out. Articles resolve to
    # www.spglobal.com.
    "spglobal.com",
    # Upstream Online (upstreamonline.com) — pure-play upstream E&P wire. GNews
    # pass=60/7d, and the pass set is essentially 100% on-beat: offshore, FPSO,
    # LNG, exploration wells, Petrobras / Foz do Amazonas, bid rounds, subsea
    # contracts. near=0 — every headline carries an upstream term — so there is
    # nothing for the lede rescue to recover. Articles resolve to
    # www.upstreamonline.com.
    "upstreamonline.com",
    # TradeWinds (tradewindsnews.com) — tanker / shipping-markets wire (NHST,
    # sister title to Upstream). GNews pass=44/7d; the pass set is all shipping:
    # tanker spot rates, LNG carriers, Strait of Hormuz transits, oil spills,
    # secondhand tanker deals — the shadow-fleet / freight beat no other
    # registered source covers. Articles resolve to www.tradewindsnews.com.
    "tradewindsnews.com",
    # Energy Intelligence (energyintel.com) — premium oil & gas market
    # intelligence (Petroleum Intelligence Weekly, Oil Daily, etc.). GNews
    # pass=65/7d, the highest of the wave, and the pass set is pure sector:
    # crude / gas prices, OPEC+ output, LNG markets, refining margins,
    # sanctions, Hormuz, Petrobras / Foz do Amazonas. Articles resolve to
    # www.energyintel.com.
    "energyintel.com",
    # Hart Energy (hartenergy.com) — US shale / Permian / midstream E&P daily.
    # Tried RSS FIRST (the preferred surface): /feed, /rss and /rss.xml on
    # www.hartenergy.com ALL return HTTP 403 (WAF) from the runner, so no
    # fetchable feed exists on the scanner's path — hence GNews. GNews
    # pass=23/7d, pass set all on-beat: Permian / shale, gas power & pipelines,
    # LNG, rig counts, upstream M&A. Articles resolve to www.hartenergy.com.
    "hartenergy.com",
    # =======================================================================
    # Russia / CIS + Middle East oil & gas press (English) -- Wave 2
    # (2026-08-18). Same shape as the Wave 1 GNews blocks above: English-only
    # publishers reached via Google News site: (hl=en-US) + the 12-term
    # english_keywords() subset, because their own feed is WAF-blocked,
    # paywalled, TLS-broken from the runner, or only dated by Google. Bodies
    # stay unreachable, so items land title-only (empty snippet) -- same
    # shape as Argus / Reuters / OGJ. Each yield is the GNews title-pass
    # count over 7d, measured on the runner (measure_source.yml) against the
    # live 91-keyword set on 2026-08-18. Targets the crude / sanctions /
    # shadow-fleet / Urals beats no Brazilian or Wave 1 source carries.
    # -----------------------------------------------------------------------
    # bne IntelliNews (intellinews.com) -- CIS / emerging-markets energy &
    # sanctions wire. GNews pass=11/7d (items=100 near=89), pass set on-beat:
    # Russian LNG to the EU, Chevron Peru offshore exit, Iraq Kirkuk-Ceyhan
    # pipeline, Russian refinery drone strikes, EU gas storage, Armenia
    # sanctions evasion. HAS a real Atom feed (www.intellinews.com/feed/) but
    # it TIMES OUT against the scanner's 4s FEED_TIMEOUT from the runner (Read
    # timed out, 2026-08-18) -- not WAF, just slow -- so GNews is the reliable
    # surface. Articles resolve to www.intellinews.com.
    "intellinews.com",
    # TASS (tass.com) -- Russian state wire, English edition. GNews pass=8/7d
    # (items=100 near=92): Brent crude, Kazakhstan oil exports, Russian LNG,
    # OPEC+ output, Arctic LNG, Hormuz, Western sanctions. Its RSS FIREHOSE
    # (tass.com/rss/v2.xml) IS runner-reachable (200, 100 items) but it is the
    # whole-wire feed -- pass=8/100 in a 10h window, war / politics / sport
    # noise and false substring hits (a NATO-planes item matched oil /
    # Compass) -- the classic polluted-general-domain case, so GNews
    # (keyword-scoped) is preferred over the firehose. Articles resolve to
    # tass.com.
    "tass.com",
    # Interfax (interfax.com) -- Russian / CIS business & energy wire, English
    # edition. GNews pass=11/7d (items=27 near=16), high precision: Kazakh oil
    # via CPC, Novak fuel-supply controls, Lukoil sanctions license, Sinopec /
    # Uzbekneftegaz, Orsk refinery drone shutdown, Nostrum Oil & Gas,
    # Azerbaijan gasoline / diesel output. No RSS (/rss.asp and /rss/ 404).
    # Articles resolve to interfax.com.
    "interfax.com",
    # MEES -- Middle East Economic Survey (mees.com). THE Middle East oil &
    # gas reference; hard paywall (no /feed, 404). GNews pass=10/7d (items=15
    # near=4), ~100% on-beat: Saudi Yanbu crude exports, XRG Venezuela
    # upstream, Egypt bid rounds, Iraq export pipelines, ADNOC Gas, Leviathan
    # gas record, Libya upstream, Iran gasoline shortage. Kept well above the
    # paywall-source ~2/7d floor -- this is the premium ME intelligence this
    # wave targets. Articles resolve to www.mees.com.
    "mees.com",
    # Arab News (arabnews.com) -- Saudi / Gulf English daily. Its business RSS
    # (/cat/4/rss.xml, Business & Economy) works from a browser but 403s from
    # the runner (WAF), so GNews. pass=79/7d (items=100 near=21) -- but ~30 of
    # the 79 are bare section / tag titles (Oil, OPEC) that Google ranks up
    # when the keyword OR-block AND-s the site: operator; those are BOUNDED
    # pollution (news_articles keyed by url, each lands once), Conjur / S&P
    # style. The other ~49 are substantial on-beat stories: Oman tanker oil
    # spill, Hormuz vessel strikes, Pakistan fuel prices, IEA demand cuts,
    # Russian refinery drone. Articles resolve to www.arabnews.com.
    "arabnews.com",
    # Shana (shana.ir) -- Iran oil-ministry news agency, English edition at
    # en.shana.ir. GNews pass=7/7d (items=10 near=3): Iran gas output /
    # savings, oil-ministry appointments, ICOFC gas targets -- the Iranian
    # upstream / ministry beat no other source carries. The English site is
    # en.shana.ir (www.shana.ir/en is 404); site:shana.ir covers the
    # subdomain, and articles resolve to en.shana.ir -- keyed in SOURCE_NAMES
    # alongside apex + www.
    "shana.ir",
    # Gulf News (gulfnews.com) -- UAE English daily. Its /feed IS
    # runner-reachable and dated, but it is a GENERAL news feed (2 passes / 18
    # items in a 15h window, mostly off-beat), so GNews site: (keyword-scoped)
    # is cleaner than downloading the whole feed every poll. pass=24/7d
    # (items=98 near=74): Hormuz shipping, US sanctions, ADNOC, Gulf war /
    # oil, tanker incidents. Articles resolve to gulfnews.com.
    "gulfnews.com",
    # Al Jazeera (aljazeera.com) -- Qatar-based global news. Its all.xml feed
    # IS runner-reachable but very low precision (1 pass / 25 items in 6h,
    # rescued=0 across 24 near-miss body fetches -- a general world-news
    # firehose with no energy / economy RSS scope; /economy/rss and
    # /tag/energy/rss are 404), so GNews site: (keyword-scoped) is the right
    # surface. pass=17/7d (items=100 near=83): Hormuz, Iran oil, Gulf oil
    # spill, LNG, sanctions. Articles resolve to www.aljazeera.com.
    "aljazeera.com",
    # Zawya (zawya.com) -- MENA business / markets aggregator (LSEG /
    # Refinitiv). The single richest source in this wave: GNews pass=38/7d
    # (items=100 near=60), pass set ~100% on-beat -- UAE trade, oil-price
    # moves, Russian ice-class LNG carriers, Hormuz oil routing, ADNOC,
    # IEA / OPEC demand forecasts, Aramco crude, ExxonMobil Mozambique LNG,
    # Egypt bid rounds. No path scope needed. Articles resolve to
    # www.zawya.com.
    "zawya.com",
    # Iraq Oil Report (iraqoilreport.com) -- the definitive independent Iraq
    # oil & gas intelligence outlet; paywall. Its /feed/ is dated from a
    # browser but returns 0 usable items from the runner (WAF interstitial),
    # so GNews. pass=4/7d (items=4 near=0) -- low VOLUME (Google indexes few
    # of a paywalled weekly) but 4 DISTINCT on-beat stories (Iraq crude
    # exports, Khor Mor gas, Turkey pipeline deal), not duplicates, on a beat
    # nothing else covers. Kept at the paywall-source floor. Articles resolve
    # to www.iraqoilreport.com.
    "iraqoilreport.com",
    # =======================================================================
    # US + Europe oil & gas / energy press (English) -- Wave 3 (2026-08-18).
    # Same shape as the Wave 1/2 GNews blocks above: English-only publishers
    # reached via Google News site: (hl=en-US) + the 12-term english_keywords()
    # subset, because their feed is WAF-blocked (E&E News, Montel) or too slow
    # for the scanner's 4s FEED_TIMEOUT (Global Energy Network). Bodies stay
    # unreachable, so items land title-only (empty snippet). Each yield is the
    # GNews title-pass count over 7d, measured on the runner (measure_source.yml)
    # against the live 91-keyword set on 2026-08-18.
    # -----------------------------------------------------------------------
    # E&E News (eenews.net, POLITICO's energy & environment desk) -- US energy
    # POLICY, a beat no other registered source carries. GNews pass=15/7d
    # (items=41 near=26), pass set almost entirely on-beat and UNIQUE: Supreme
    # Court climate cases tied to oil companies, Interior Gulf oil & gas lease
    # sales, federal oil-pipeline tunnel approvals, California gasoline-blend /
    # pipeline policy, offshore fracking, EU Rosatom sanctions. No RSS reachable
    # from the runner (403). Articles resolve to www.eenews.net.
    "eenews.net",
    # Montel News (montelnews.com) -- European power & gas market wire; the EU
    # gas / power beat no other source carries. GNews pass=8/7d (items=62
    # fresh=57 near=49), pass set on-beat European market intelligence: EU gas
    # storage refill above EUR 60/MWh, European LNG import recovery, German /
    # Italian winter gas outlooks, gas-fired generation in heatwaves, Nordic H2
    # pipeline. Feed WAF-blocked from the runner. Articles resolve to
    # www.montelnews.com.
    "montelnews.com",
    # Global Energy Network (globalenergynetwork.net) -- international upstream
    # oil & gas wire (the rebranded/expanded home of OGV Energy; the ogv.energy
    # domain redirects here). The single richest source of this wave: GNews
    # pass=18/7d (items=26 near=8), pass set ~100% on-beat -- Petrobras
    # Equatorial Margin / Amapa discoveries, Chevron Angola CABGOC, ConocoPhillips
    # appointments, Venezuela gas licenses (bp/XRG), ExxonMobil Mozambique,
    # Technip Malaysia LNG, Oando / Predator / Bass Oil E&P, US Gulf subsea
    # tie-back. NOTE its "offshore" hits are offshore OIL (e.g. deepwater Amapa,
    # US Gulf tie-back), not wind -- so unlike Recharge (rejected below) the pass
    # set is genuinely O&G. Its RSS (ogv.energy/feed/ -> globalenergynetwork.net/
    # feed/) IS runner-reachable and dated but is 6.8 MB / 1000 entries and takes
    # ~6.6s, so it TIMES OUT against the scanner's 4s FEED_TIMEOUT (same class as
    # intellinews), and site:ogv.energy returns 0 GNews items (content is indexed
    # under the new domain), so this apex is the reliable surface. Articles
    # resolve to www.globalenergynetwork.net.
    "globalenergynetwork.net",
    # --- Wave 2 REJECTED (measured 2026-08-18 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # Neftegaz.RU (neftegaz.ru): no viable surface from the runner. Its RSS
    # and homepage fail TLS (SSLCertVerificationError -- the cert chain does
    # not verify from a US datacenter; DNS resolves, so not a geo-block) AND
    # site:neftegaz.ru returns 0 GNews items (not indexed under en-US).
    # Both routes 0.
    # "neftegaz.ru",
    # OPEC (opec.org): no RSS (SPA; /rss and /feed serve HTML). site:opec.org
    # GNews pass=4/7d but all four are the SAME OPEC Award for Research press
    # release (every opec.org page trivially matches OPEC) -- ~1 distinct
    # story, press-release-only, very low frequency. OPEC+ output / MOMR news
    # is already carried by TASS / Interfax / Zawya / MEES. Below bar.
    # "opec.org",
    # --- Wave 3 REJECTED (measured 2026-08-18 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # Houston Chronicle (houstonchronicle.com): general-news domain. Bare
    # site: GNews pass=6/7d but near=94, and the pass set is generic AP wire
    # (Asia shares / US stocks "as oil prices swing", plus Hormuz / Oman-tanker
    # items already carried by gCaptain / TradeWinds / Zawya) -- not Houston
    # energy reporting. Path-scoping is unavailable: site:houstonchronicle.com/
    # business/energy returns 0 GNews items (Google honours a /news path for
    # Rigzone but ignores this deeper one). Duplicate-dominated + un-narrowable.
    # "houstonchronicle.com",
    # Euractiv (euractiv.com): general EU-policy domain. RSS 403 from the
    # runner; bare site: GNews pass=2/7d (items=96 fresh=56 near=54), both
    # passes generic geopolitics (Hormuz, EU Russia sanctions) already carried
    # elsewhere, buried in 54 non-energy near-misses. Path-scope unavailable
    # (Google ignores the path, same as Houston Chronicle above). Below bar.
    # "euractiv.com",
    # ICIS (icis.com): chemicals/energy price wire, hard paywall -- Google
    # indexes few articles. site: GNews pass=2/7d (items=9 near=7), both
    # generic Hormuz duplicates. Below the paywall-source floor.
    # "icis.com",
    # Recharge (rechargenews.com): energy-TRANSITION / renewables outlet.
    # site: GNews pass=7/7d (items=15 near=8) but six of seven passes are
    # offshore WIND (Atlantic Shores, Vestas, Finland/Germany/Denmark wind) --
    # the "offshore" keyword matching offshore wind, a false positive for the
    # O&G beat; only ~1 gas-adjacent story/wk (an RWE gas pivot). Off-beat, it
    # would inject wind-farm noise. Rejected.
    # "rechargenews.com",
    # Kallanish (kallanish.com): steel / metals commodities wire. site: GNews
    # pass=2/7d (items=45 fresh=40 near=38); the passes are "crude STEEL" (a
    # false positive on "crude") and a steel-industry-on-gas item. Off-beat +
    # below bar.
    # "kallanish.com",
    # JPT / SPE (jpt.spe.org): SPE technical journal. /feed is HTTP 404; site:
    # GNews pass=2/7d (items=9 near=7), both duplicates of stories other
    # sources carry (Rovuma LNG, Petrobras Equatorial Margin). Google indexes
    # few of a technical monthly. Below bar.
    # "jpt.spe.org",
    # =======================================================================
    # India + China oil & gas / business press (English) -- Wave 4 (2026-08-18).
    # Same shape as the Wave 1/2/3 GNews blocks above: English-only publishers
    # reached via Google News site: (hl=en-US) + the 12-term english_keywords()
    # subset, because their own feed is WAF-blocked (Moneycontrol) or absent
    # (the Chinese state / regional wires have no usable RSS). Bodies stay
    # unreachable, so items land title-only (empty snippet). Each yield is the
    # GNews title-pass count over 7d, measured on the runner (measure_source.yml)
    # against the live 91-keyword set on 2026-08-18.
    # -----------------------------------------------------------------------
    # Moneycontrol (moneycontrol.com) -- India's dominant markets / business
    # portal (Network18). The richest India surface of this wave: GNews
    # pass=57/7d (items=100 fresh=95 near=38), pass set heavily on-beat --
    # crude / Brent / oil-price moves, Hormuz risk, Petronet LNG, ONGC Venezuela
    # after US sanctions, diesel, refiner LPG allocation, India crude basket. RSS
    # is WAF-blocked (403 from the runner), hence GNews. Two bounded noise
    # classes, both title-only + url-keyed so each lands at most once: ~2
    # coconut-oil / skincare items on the substring `oil`, and a handful of
    # index-wrap headlines ("GIFT Nifty ... rising oil prices") where oil is
    # named as a market driver rather than the subject. Articles resolve to
    # www.moneycontrol.com.
    "moneycontrol.com",
    # -----------------------------------------------------------------------
    # China state / regional oil & gas press (English). No usable RSS on any of
    # these -- all GNews en-US. They carry the China-domestic O&G buildout
    # (PipeChina, the 2030 oil/gas supply targets) plus a Chinese-wire read on
    # the global crude / OPEC / Hormuz beat. Heavy syndication: the same wire
    # story recurs across Xinhua / China Daily / Global Times and, url-keyed,
    # each copy lands at most once -- so the secondary wires are largely a
    # duplicate of Xinhua and are kept only because each clears the >=3/7d
    # on-beat bar in its own right (numbers per entry). Recurring false-positive
    # class to watch: coal-mine "gas outburst" / gas-explosion ACCIDENT stories
    # and "non-oil exports" on the exact `gas` / substring `oil` keywords.
    # -----------------------------------------------------------------------
    # Xinhua (english.news.cn) -- the Chinese state wire, English edition, and
    # the anchor of the China set. GNews pass=43/7d (items=100 fresh=100
    # near=57). After discounting ~7 gas-accident / "non-oil exports" false
    # positives and the wire's heavy same-title syndication (OPEC demand x3,
    # "Crude futures settle" x4, Aramco-refinery-drone x3), ~26 DISTINCT on-beat
    # remain: Hormuz tanker attacks, OPEC / IEA demand cuts, Houthi strikes on
    # the Aramco Jazan refinery, China's 440 Mt 2030 oil / gas target, Novatek,
    # Libya oil revenues, Australia jet-fuel / diesel reserves -- a genuinely
    # additive Chinese-wire read on the global beat. English articles resolve to
    # english.news.cn (a subdomain, no www) -- keyed there in SOURCE_NAMES.
    "english.news.cn",
    # China Daily (chinadaily.com.cn) -- China's flagship English daily. GNews
    # pass=9/7d (items=100 fresh=100 near=91). Deduping same-title copies (OPEC
    # demand x2, coal-mine gas-outburst x2) and dropping the gas-accident false
    # positive leaves ~5 distinct on-beat -- PipeChina's East-China crude
    # pipeline, China's 220,000 km oil / gas pipeline plan to 2030, OPEC oil
    # demand, Tehran-vs-Trump on Hormuz, a Greenland / Texas oil-company row.
    # That clears the >=3/7d on-beat bar, but nearly all of it is ALSO carried
    # by Xinhua at ~5x the volume; kept per the numeric bar, with the redundancy
    # recorded here for a future pruning pass. near=91 is China-general copy that
    # mentions a keyword in passing and never passes on the title. Articles
    # resolve to www.chinadaily.com.cn.
    "chinadaily.com.cn",
    # Global Times (globaltimes.cn) -- CPC-affiliated English tabloid. GNews
    # pass=7/7d (items=26 near=19), ~5 distinct on-beat after dropping a
    # coal-mine gas-outburst accident and an "offshore" marine-bunkering false
    # positive: China's 440 Mt 2030 oil / gas supply plan, the East-China
    # 18 Mt/yr crude pipeline, OPEC oil demand, and two Hormuz tanker items.
    # Clears the >=3/7d on-beat bar; like China Daily it is largely a Xinhua
    # duplicate on the same China-buildout + Hormuz stories and is kept per the
    # numeric bar. Articles resolve to www.globaltimes.cn.
    "globaltimes.cn",
    # South China Morning Post (scmp.com) -- Hong Kong English daily, the most
    # DISTINCTIVE of the China set (an Asia-geopolitics desk, not a state wire).
    # GNews pass=13/7d (items=90 near=69), ~8 on-beat: "US diesel crack tops
    # $100/bbl", Myanmar seeking Russian oil, Philippines-China oil deal, US
    # shifting its Iran-war goal to cheaper oil, EU Russia sanctions, China
    # peak-oil-by-2030, and Hormuz items. Highest false-positive rate of the
    # wave (~4/13): "crude" as an adjective (a cartoon-ox headline), "palm oil"
    # data centres, a "toxic gas" accident, and a "running out of gas" idiom --
    # all bounded (title-only, url-keyed). Kept because the on-beat set clears
    # the bar AND is genuinely additive (an Asia crude-trade / sanctions angle no
    # other source carries). span in the raw measure is ~10y because Google
    # surfaced a few archived pages; fresh=82 within the 7d window. Articles
    # resolve to www.scmp.com.
    "scmp.com",
    # --- Wave 4 REJECTED (measured 2026-08-18 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # Yicai Global (yicaiglobal.com): China's answer to the FT, English edition.
    # site: GNews items=2 fresh=2 pass=0 near=2 -- Google indexes almost nothing
    # of it under en-US (a hard paywall + a thin English index). Zero passes,
    # nowhere near the bar.
    # "yicaiglobal.com",
    # Caixin Global (caixinglobal.com): China business / markets, English
    # edition. site: GNews items=8 fresh=8 pass=2 near=6 -- both passes are
    # genuinely on-beat (China's oil & gas buildout to 2030, a coal-mine gas
    # outburst) but only 2 in 7d: a hard paywall means Google indexes few
    # articles. Below the >=3/7d bar despite being the markets-focused outlet
    # expected to be the strongest of the two.
    # "caixinglobal.com",

    # =======================================================================
    # WAVE 5 ANCHORS (2026-09-14) — five parallel branches, ONE block each.
    # A wave appends its entries ONLY between its own BEGIN/END markers and
    # leaves every fence line alone: >=4 untouched lines between blocks is what
    # keeps the five diffs in non-overlapping hunks, so the branches merge
    # without conflicts. Empty is the correct state until a wave lands.
    # =======================================================================
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- BEGIN
    # Global wires & US mainstream, measured 2026-09-14 on the runner
    # (measure_source.yml) against the LIVE keyword set -- 187 terms today
    # (78 exact / 109 substring), not the 91 the README paragraph quotes.
    # Window 168h. Every number below is the GNews en-US title-pass count over
    # those 7 days unless said otherwise, and each entry also records what the
    # outlet's OWN feed did, so no later wave re-tests a surface this one killed.
    #
    # 24 candidates measured, 24 registered, 0 rejected: 3 kept RSS (Financial
    # Times, The Guardian, CBS News -- see RSS_FEEDS) and the 21 below are
    # GNews-en. Bodies stay unreachable on this route, so items land title-only
    # (empty snippet).
    #
    # Adding 21 domains takes ENGLISH_NO_RSS_DOMAINS past EN_GNEWS_QUERIES_PER_SCAN
    # (34), which is expected and safe: the scan then rotates the list over
    # ceil(n/34) cohorts, one per 5-minute scan, and every domain is still queried
    # two orders of magnitude inside its 24h `when:` window.
    #
    # TWO NOISE CLASSES RECUR ACROSS THIS WHOLE COHORT, both bounded (title-only,
    # url-keyed, each lands at most once): (a) PERENNIAL quote / chart / SEC-filing
    # pages on the markets sites, the TradingView pathology -- lethal on
    # marketwatch.com (fixed with a path scope below), mild on barrons.com /
    # foxbusiness.com / businessinsider.com; (b) proper nouns and idioms hitting
    # the exact keywords -- "Brent Spence" (a bridge), "Brent Headrick" (a
    # pitcher), "Vin Diesel", "snake oil salesman", "gas station" crime copy.
    #
    # Associated Press (apnews.com) -- the US wire, and the anchor of this
    # cohort. GNews pass=33/7d (items=100 fresh=100 near=67), ~28 on-beat and
    # broad: the Saudi East-West pipeline drone strike, US diesel past $6,
    # Houthis seizing Red Sea islands, oil above $100, Yemen sliding to war,
    # Cameron Parish LNG litigation, Lula cutting fuel taxes. No RSS: index.rss,
    # hub/business.rss and hub/climate-and-environment.rss all return HTTP 403
    # from the runner (WAF). Articles resolve to apnews.com.
    "apnews.com",
    # The Wall Street Journal (wsj.com) -- paywalled, and its Dow Jones feeds are
    # not blocked but FROZEN, the "answers 200 and has simply stopped moving"
    # class: feeds.a.dj.com/rss/RSSWorldNews.xml items=0; RSSMarketsMain.xml
    # items=20 span=57h fresh=0; WSJcomUSBusiness.xml items=20 span=12h fresh=0
    # (a dated payload whose newest item is older than the 7d window). GNews
    # pass=34/7d (items=100 fresh=100 near=66), ~30 on-beat and squarely the desk
    # we want: "What Is Saudi Arabia's East-West Pipeline and Why Is It Rocking
    # Oil Markets?", record diesel, US natural-gas futures, the Hormuz bypass.
    # Articles resolve to www.wsj.com.
    "wsj.com",
    # The New York Times (nytimes.com) -- GNews pass=22/7d (items=100 fresh=100
    # near=78), essentially 100% on-beat (Saudi oil exports plunging, Gulf tankers
    # running out of export routes, diesel above $6, the North Sea drilling
    # decision, Houthis on the oil route). Its RSS is alive but far thinner and
    # is what loses the comparison: rss.nytimes.com/services/xml/rss/nyt/
    # Business.xml items=49 span=98h fresh=49 pass=6 near=43 no_body=12, and
    # .../EnergyEnvironment.xml items=20 span=313h fresh=11 pass=1 -- that single
    # pass also appears in Business, so the sector feed adds nothing. 6 vs 22.
    # Articles resolve to www.nytimes.com.
    "nytimes.com",
    # The Washington Post (washingtonpost.com) -- the richest of the US
    # mainstream: GNews pass=40/7d (items=100 fresh=100 near=60), ~34 on-beat
    # (how much oil really moves through Hormuz, the Saudi pipeline shutdown,
    # Gulf states racing to bypass Hormuz, $6 diesel, US sanctions on Iranian
    # airlines). Its own feeds are BOTH slow and a stub -- they time out at the
    # 4s default and return 3-11 items at ~9-10s; the full autopsy and the
    # decision not to buy them an override are in FEED_TIMEOUT_OVERRIDES.
    # Bounded noise: the daily "Tracking Trump" roundups, whose blurb mentions
    # oil. Articles resolve to www.washingtonpost.com.
    "washingtonpost.com",
    # The Economist (economist.com) -- paywalled weekly, so a low absolute number
    # was expected and the GNews route still beats its feed 19 to 1. GNews
    # pass=19/7d (items=100 fresh=95 near=76; span reads 21268h because Google
    # surfaced a few archive pages, but 95 of the 100 are inside the window).
    # CAVEAT: ~12 of the 19 are the daily "World in Brief" digest, whose blurb
    # names Brent / the Saudi pipeline / oil routes -- on-beat but one story a
    # day, heavily repetitive. The remaining ~6 are the real thing: "How an
    # oil-supply crisis could bring about an investment boom", "Is traffic
    # through Hormuz really back to normal?", "The Houthis' advance puts oil
    # markets on edge". RSS measured: business/rss.xml items=300 span=6806h
    # fresh=8 pass=1, finance-and-economics fresh=6 pass=0, international fresh=2
    # pass=0 -- 300-item archives with ~8 fresh items a week. Articles resolve to
    # www.economist.com.
    "economist.com",
    # BBC News (bbc.com) -- GNews pass=23/7d (items=100 fresh=100 near=77),
    # ~19 on-beat (the Saudi pipeline drone attack, rising oil prices as winter
    # looms, the Dangote refinery share sale, how the Houthi advance hits trade
    # and oil). Its feeds work but are general-news: feeds.bbci.co.uk/news/
    # business/rss.xml items=57 span=2372h fresh=40 pass=3 (two distinct stories,
    # one repeated) and news/world/rss.xml items=32 fresh=32 pass=3 -- the Gulf
    # News call of 2026-08-18, a keyword-scoped query over the whole site beats
    # downloading a general feed every poll. FP to watch: "'Wannabe Vin Diesel'
    # has Volkswagen seized at Castle Donington car meet" on the substring
    # `diesel`. Articles resolve to www.bbc.com (and bbc.co.uk for UK copy).
    "bbc.com",
    # The Times / The Sunday Times (thetimes.com) -- hard paywall, so Google
    # indexes little: GNews pass=7/7d (items=100 fresh=98 near=91), but 6 of the
    # 7 are on-beat and distinctly British-desk (Ukraine hitting Siberian gas
    # plants in the deepest drone strikes of the war, what the Houthi advance
    # means, oil above $100, the US destroying Iranian tankers, the Saudi
    # pipeline shutdown); the seventh is West Bank sanctions on `sanction`.
    # No RSS: www.thetimes.com/business-money/rss is not a feed at all -- the
    # fetcher gets an HTML page and dies on `<unknown>:2:411: mismatched tag`.
    # Kept at the paywall-source floor. Articles resolve to www.thetimes.com.
    "thetimes.com",
    # The Telegraph (telegraph.co.uk) -- GNews pass=21/7d (items=100 fresh=99
    # near=78), ~15 on-beat and unusually good on the shipping/chokepoint angle
    # ("The shock Houthi mission to strangle global oil", the Saudi pipeline
    # breadcrumb trail, diesel at a four-year high, the North Sea drilling delay,
    # coal demand at a record as the Iran war cuts gas). RSS is effectively
    # frozen: telegraph.co.uk/business/rss.xml items=120 span=7991h fresh=2
    # pass=0. Bounded noise: UK-politics `sanction` copy and one one-word "Oil"
    # section stub. Articles resolve to www.telegraph.co.uk.
    "telegraph.co.uk",
    # Sky News (news.sky.com) -- NOTE THE HOST: articles live on the news.
    # subdomain, not the sky.com apex, and that is the form measured. GNews
    # pass=13/7d (items=72 fresh=72 near=59), ~8 on-beat (Ed Conway's oil-routes
    # analysis, oil at $100 with diesel at all-time highs, Iran retaliating over
    # its tankers, the Saudi pipeline satellite imagery, Trump on North Sea oil).
    # Its feeds are useless for this beat: feeds.skynews.com/feeds/rss/
    # business.xml items=10 fresh=9 pass=0, and world.xml items=8 fresh=8 pass=1
    # where the single pass is a `drone` false positive (an alert that turned out
    # to be a flock of birds). The residual noise here is Ukraine drone copy.
    "news.sky.com",
    # POLITICO (politico.com) -- registered at the floor, and deliberately: its
    # energy desk E&E News (eenews.net) is already in this list from Wave 3 at
    # pass=15/7d and carries the US policy beat, so this apex is only the
    # marginal main-site pickup. GNews pass=6/7d (items=100 fresh=97 near=91) of
    # which 3 are on-beat (the oil benchmark topping $100, CENTCOM destroying
    # five Iranian tankers, the "Out of time on oil" newsletter) and 3 are
    # off-beat (Israel-settlement sanctions x2, plus "Campaigns hit the gas", an
    # idiom on the exact keyword `gas`). RSS measured: rss.politico.com/
    # energy.xml items=30 span=936h fresh=6 pass=1 (the same $100 story) and
    # economy.xml items=3 fresh=0. Articles resolve to www.politico.com.
    "politico.com",
    # POLITICO Europe (politico.eu) -- the EU energy-policy angle. GNews
    # pass=6/7d (items=57 fresh=56 near=50): 2-3 on-beat (Trump asking Ukraine to
    # halt strikes on Russian oil refineries, Houthis seizing a Red Sea port) and
    # 3 EU-sanctions-on-Israel items passing on the substring `sanction` -- a
    # 50-67% off-beat rate, high but inside the rejection bar, and the on-beat
    # items are Brussels-desk copy no other registered source carries. RSS
    # measured: politico.eu/feed/ items=10 span=6h fresh=10 pass=1 (a 10-item
    # front-page stub) and /section/energy-uk/feed/ items=31 span=790h fresh=6
    # pass=2. Articles resolve to www.politico.eu.
    "politico.eu",
    # Axios (axios.com) -- GNews pass=12/7d (items=52 fresh=48 near=36), ~7
    # on-beat and genuinely additive on the US fuels/macro read (record diesel
    # stoking food inflation, the EIA raising its 2027 diesel outlook, "Oil's
    # 'new normal' is looking more expensive", MBS pressing Trump to strike the
    # Houthis, home heating-oil bills). The rest is Axios LOCAL copy ("Vacant
    # Birmingham gas station site getting $8M makeover", coffee in an old gas
    # station) plus the wave's best exact-keyword trap: "The Brent Spence project
    # is finally underway" -- a BRIDGE matching `Brent`. Its own feed is worse,
    # not better: api.axios.com/feed/ items=100 span=250h fresh=75 pass=16
    # near=59 looks richer, but only ~6 of the 16 are on-beat because the feed
    # ships full newsletter blurbs, so unrelated items match on the SUMMARY (a
    # Vance campaign profile matched eight keywords at once). api.axios.com/feed/
    # energy-climate is HTTP 404. Articles resolve to www.axios.com.
    "axios.com",
    # Forbes (forbes.com) -- GNews pass=11/7d (items=100 fresh=100 near=89), ~8
    # on-beat (oil crossing $100 as tankers are struck, Houthi attacks opening a
    # new front, "The Houthis Do Not Need To Close Bab El-Mandeb", four separate
    # diesel-record pieces, the Hormuz escape route under attack). Two proper-noun
    # FPs, both quotable: "Insomniac Explains Wolverine's Already-Infamous 'Fart
    # Gas Trails'" on `gas` and a New York Yankees profile of "Brent Headrick" on
    # the exact `Brent`. RSS: forbes.com/energy/feed/ is HTTP 404 (the energy
    # section has no feed) and forbes.com/business/feed/ is a 2-hour firehose
    # window -- items=25 span=2h fresh=25 pass=1, and that one pass is an EPA
    # greenhouse-`gas` item. Articles resolve to www.forbes.com.
    "forbes.com",
    # Fortune (fortune.com) -- GNews pass=21/7d (items=100 fresh=94 near=73),
    # ~19 on-beat and strong on the US-macro transmission of the oil shock
    # (California diesel overwhelming pump displays, US diesel 60% above pre-war,
    # oil topping $100, the Canada trade-deficit-is-oil explainer, Ryanair on
    # fares above $100 oil, Chevron/Occidental earnings calls). CAVEAT: six of
    # the 21 are the perennial daily series "Current price of oil as of <date>"
    # -- on-beat, but one per day and url-keyed, so it lands once a day forever.
    # RSS: fortune.com/feed/fortune-feeds/?id=3230629 items=10 span=36h fresh=10
    # pass=1. Articles resolve to fortune.com.
    "fortune.com",
    # Business Insider (businessinsider.com) -- pass=36/7d (items=100 fresh=100
    # near=64), ~28 on-beat and the most ADDITIVE domain of the wave: BI's Africa
    # desk is a beat nothing else in the roster carries (the Dangote refinery IPO
    # in five separate stories, Dangote buying 16 Mbbl of Nigerian crude,
    # TotalEnergies' $10bn Angola plan, ExxonMobil's 20th Angola discovery, a new
    # West African licensing round, Algeria/Nigeria revenue exposure), on top of
    # the global beat (the Saudi pipeline, China buying Angolan/Congolese crude).
    # Noise: one perennial quote page ("Palm Oil PRICE Today | Live Price of Palm
    # Oil per Ounce" -- the TradingView pathology, on the substring `oil`) and a
    # few press releases syndicated from markets.businessinsider.com. RSS is a
    # non-starter: markets.businessinsider.com/rss/news items=10 span=0h fresh=10
    # pass=1 (a ten-item, minutes-wide Benzinga syndication window) and
    # www.businessinsider.com/rss items=20 span=5h fresh=20 pass=0. Articles
    # resolve to www.businessinsider.com (and markets.businessinsider.com).
    "businessinsider.com",
    # MarketWatch (marketwatch.com/story) -- PATH-SCOPED, the rigzone.com/news
    # precedent, and the clearest measured case in this wave for doing it. The
    # bare domain looks like the richest source anywhere -- site:marketwatch.com
    # pass=75/7d (items=100 fresh=98 near=23) -- but ~45 of those 75 are PERENNIAL
    # quote / chart / filing pages that trivially carry a ticker keyword
    # ("Download AYXCN31 Data | Mars (Argus) vs. WTI Trade Month Jul 2031 Price
    # Data", "BRNM36 | Brent Crude Jun 2036 Overview", "OSA | Osaka Gas Co. Ltd.
    # Profile", "MUR | Murphy Oil Corp. SEC Filings"), plus robo-posts ("Imperial
    # Oil Ltd. stock rises Tuesday, outperforms market"). Scoped to the article
    # path: site:marketwatch.com/story items=100 span=160h fresh=100 pass=54
    # near=46 -- 54 passes, every one a real article (the Houthi front in the oil
    # war, refining bottlenecks as the next big problem, $6 diesel, Enbridge
    # buying Tallgrass' crude business, the EIA diesel outlook, Venture Global's
    # China LNG deal, US crude stockpiles). RSS is frozen or a stub:
    # feeds.content.dowjones.io/public/rss/mw_topstories items=10 span=2h fresh=10
    # pass=1, mw_marketpulse items=30 span=9433h fresh=0, mw_realtimeheadlines
    # items=10 span=11274h fresh=0. Articles resolve to www.marketwatch.com.
    "marketwatch.com/story",
    # Barron's (barrons.com) -- paywalled markets weekly. GNews pass=20/7d
    # (items=100 fresh=99 near=79), ~17 on-beat and buy-side-shaped: "The Saudi
    # Arabia Pipeline Attacks Just Redefined Middle East Oil Risk", European
    # natural gas at a four-year high lifting energy stocks, $6 diesel simplifying
    # the Fed's decision, 12 energy dividend stocks, Brent back above $100, Baker
    # Hughes' worst day in a year. Noise: ~3 perennial quote pages ("Halliburton
    # Co. Stock Grades | HAL", "Montero Mining & Exploration Advanced Charts") --
    # the same class that forced a path scope on MarketWatch, but 3 of 20 instead
    # of 45 of 75, so the bare domain stands. No RSS: barrons.com/feed/
    # rssheadlines and feeds.a.dj.com/rss/RSSBarronsTopStories.xml are both HTTP
    # 403 from the runner. Articles resolve to www.barrons.com.
    "barrons.com",
    # NPR (npr.org) -- GNews pass=19/7d (items=100 fresh=100 near=81), ~15
    # on-beat (Houthi attacks igniting fires at Saudi oil facilities, why Houthi
    # gains push gas prices higher, crude topping $100 at the pump, diesel past
    # $6, whether the Gulf and Iran can negotiate Hormuz without the US, Iranians
    # losing jobs under sanctions). Noise: NPR's newscast/roundup titles repeat
    # one story across shows ("U.S. military destroys 5 Iranian oil tankers. And,
    # the Smithsonian head resigns" x3, url-keyed so each lands once) and one FP,
    # "Step Off the Gas and Charge : The Academic Minute". RSS: feeds.npr.org/
    # 1006/rss.xml (Business) items=10 span=74h fresh=10 pass=2, and 1025/rss.xml
    # (Energy) items=10 span=346h fresh=2 pass=0 -- ten-item windows. Articles
    # resolve to www.npr.org.
    "npr.org",
    # ABC News (abcnews.com) -- NOTE THE DOMAIN FORM, it is the finding here:
    # site:abcnews.go.com returns items=0 (Google does not index the outlet under
    # the .go.com host), while site:abcnews.com measures pass=52/7d (items=100
    # fresh=100 near=48), ~40 on-beat. abcnews.com is also the host the outlet's
    # OWN feed links to, so both surfaces resolve the same source_domain. RSS was
    # viable and still lost: feeds.abcnews.com/abcnews/businessheadlines items=25
    # span=75h fresh=25 pass=10 near=15 fetch=0.54s (~22/7d once normalised to a
    # 7-day window) with good precision, and /abcnews/topstories items=25 fresh=25
    # pass=3 -- but 52 > 22 and the GNews route also reaches the international
    # desk the business feed never carries.
    "abcnews.com",
    # NBC News (nbcnews.com) -- GNews pass=28/7d (items=100 fresh=100 near=72),
    # ~24 on-beat (the 10-year yield topping 5% as oil surges and diesel hits an
    # all-time high, Saudi Arabia's shutdown of the key pipeline, "Hormuz,
    # Houthis and pipeline strikes", the federal gas tax on Trump's hit list,
    # Iran raising gasoline prices). Noise to watch: two PERENNIAL trackers
    # ("Tracking ship traffic through the Strait of Hormuz. Updated regularly",
    # "Tracking U.S., state and county gas prices, in maps and charts. Updated
    # daily") -- live pages with a moving date, bounded by the url key. RSS:
    # feeds.nbcnews.com/nbcnews/public/business items=25 span=3810h fresh=23
    # pass=7 (all on-beat) and .../public/world items=20 fresh=19 pass=8; 7-8
    # against 28. Articles resolve to www.nbcnews.com.
    "nbcnews.com",
    # Fox Business (foxbusiness.com) -- GNews pass=20/7d (items=100 fresh=99
    # near=79), ~16 on-beat (oil topping $103 on the Saudi pipeline shutdown,
    # soaring diesel threatening the timber industry, "Diesel prices are the ones
    # to watch, not oil", Brent at a six-week high on the Hormuz threat, state
    # blocks on natural-gas pipelines, the US disabling ten Iranian tankers). It
    # is also the roster's most POLITICAL read on prices (Trump's forecasts), so
    # treat headline claims as quotes. Noise: two perennial ticker pages ("united
    # states oil fund - usd acc - USO", "oil refineries ltd - OILRF") and a "snake
    # oil salesman" idiom. RSS measured and rejected: moxie.foxbusiness.com/
    # google-publisher/latest.xml items=25 span=74h fresh=25 pass=2, economy.xml
    # items=25 span=308h fresh=7 pass=1, markets.xml items=25 span=1126h fresh=2
    # pass=0, and feedburner/latest.xml serves the same payload as latest.xml.
    # Articles resolve to www.foxbusiness.com.
    "foxbusiness.com",
    # --- Wave 5A REJECTED (measured 2026-09-14 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # NONE. All 24 candidates cleared the 2026-09 acceptance rule (pass >= 1 over
    # 7d on some surface) and none was >= 80% off-beat. What WAS rejected is a
    # surface per outlet, and each dead surface is written down in its entry
    # above: AP / Barron's RSS (HTTP 403), the WSJ + MarketWatch Dow Jones feeds
    # and Telegraph business (frozen: dated payload, fresh=0 or 2), The Times
    # "feed" (HTML, mismatched tag), forbes.com/energy/feed/ and
    # api.axios.com/feed/energy-climate (HTTP 404), the Washington Post feeds
    # (9-10s for a 3-11 item stub), site:abcnews.go.com (0 items -- use
    # abcnews.com), and the bare site:marketwatch.com (45 of 75 passes are
    # perennial quote pages -- use the /story path).
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- BEGIN
    # =======================================================================
    # US regional, Europe, Canada, Oceania mainstream -- Wave 5B (2026-09-14).
    # Same shape as the Wave 1/2/3/4 GNews blocks above: English-only editions
    # reached via Google News site: (hl=en-US) + the 12-term english_keywords()
    # subset, because their own feed is WAF-blocked, absent, malformed, frozen
    # or simply thinner than Google. Bodies stay unreachable, so items land
    # title-only (empty snippet). Eleven outlets here; the other nine of this
    # wave had a feed that BEAT their GNews surface and are in RSS_FEEDS.
    # Yields are the GNews title-pass count over 7d, measured on the runner
    # (measure_source.yml) against the live 91-keyword set on 2026-09-14.
    #
    # SURFACE PICK (recorded once, applied per entry): higher measured 7-day
    # yield, normalising a short-span feed as pass x 168/span, tie-break to RSS
    # when GNews is under 2x richer. Every entry below therefore carries BOTH
    # numbers -- what its feed did and what GNews did -- so the decision can be
    # re-audited without re-measuring.
    #
    # ACCEPTANCE BAR: this wave ran under the 2026-09-14 rule (pass >= 1 over
    # 7d, or >= 3 over 48h; reject only on zero items on EVERY surface or on a
    # >= 80% off-beat pass set), which is deliberately LOWER than the
    # 2026-08-18 bar of ">= 3 on-beat passes per 7d". Two domains below
    # (houstonchronicle.com, and euractiv.com in the REJECTED sub-block) were
    # already measured under the OLD bar in Wave 3 and are RE-MEASURED here.
    # -----------------------------------------------------------------------
    # Houston Chronicle (houstonchronicle.com) -- Hearst daily of the US oil
    # capital. RE-TEST of the Wave 3 REJECTED note above, under the lowered
    # 2026-09-14 bar: GNews pass=35/7d (items=100 span=164h fresh=100 near=65),
    # nearly 6x the pass=6 measured on 2026-08-18. The 2026-08-18 objection
    # (generic AP wire duplicates) still partly holds -- the Wall-Street-vs-oil
    # tape (~12 of 35) is syndicated AP that CNN / USA Today / LA Times also
    # carry, and it is url-keyed so each copy lands at most once -- but the
    # remainder is genuine Texas energy reporting no other registered source
    # has: a Texas oil CEO resuming a fraud scheme, the EPA killing the
    # power-plant greenhouse-gas rule, Houston pump prices via GasBuddy, US
    # diesel past $6/gal and record diesel hitting farmers. Registered.
    # Path-scoping remains unavailable (site:houstonchronicle.com/business/energy
    # returns 0 items -- Google honours a /news path for Rigzone but ignores
    # this deeper one), and the site RSS is dead from the runner
    # (/rss/feed/Business-Energy-593.php and /rss/feed/Business-287.php both
    # HTTP 403), so the bare domain is the only surface. Articles resolve to
    # www.houstonchronicle.com.
    "houstonchronicle.com",
    # Los Angeles Times (latimes.com) -- US West Coast daily of record. GNews
    # pass=21/7d (items=100 fresh=93 near=72): California-specific energy
    # policy (the city cleaning up its biggest gas power plant, LA cost of
    # living) plus the Middle-East supply beat (Hormuz exclusion zone, Houthis
    # in the Red Sea, US strikes on Iranian tankers, record diesel hitting the
    # harvest). Its OWN feeds are runner-reachable but yield NOTHING on beat:
    # /business/rss2.0.xml items=92 span=633h fresh=22 pass=0 near=22, and
    # /world-nation/rss2.0.xml items=98 span=672h fresh=22 pass=0 near=22 --
    # 44 fresh items, zero passes, so RSS is not a surface here at all. One
    # bounded false-positive class in the GNews set: `gas` as tear gas / a SWAT
    # standoff. Articles resolve to www.latimes.com.
    "latimes.com",
    # USA Today (usatoday.com) -- the US mass-market daily (Gannett). GNews
    # pass=36/7d (items=100 fresh=99 near=63). Its own feed host is BROKEN, not
    # blocked: rssfeeds.usatoday.com/usatoday-NewsTopStories parses as
    # "<unknown>:2:0: syntax error" from the runner (malformed XML), so GNews is
    # the only surface. The pass set is the US pump-price beat this programme
    # wants -- why gas prices are rising and who controls them, oil at $100 and
    # what it means for gasoline, US strikes destroying five Iranian tankers,
    # Houthi/Iranian attacks and oil prices, Trump on keeping Iranian oil.
    # LARGEST NAMED-ENTITY FALSE POSITIVE OF THE WHOLE PROGRAMME, and it must be
    # written down: ~8 of 36 passes are "Brent" the FIRST NAME -- Brent Venables
    # (Oklahoma football coach) and Brent Peterson (NHL) -- matching the exact
    # keyword `Brent`. Plus a "Gas station store Crossword Clue". All bounded
    # (title-only, url-keyed, at most one landing each) and still leaves ~25
    # on-beat, so the domain clears the bar; if this class ever becomes the
    # majority, drop the domain rather than weakening the `Brent` keyword, which
    # every other source depends on. Articles resolve to www.usatoday.com.
    "usatoday.com",
    # Deutsche Welle, English edition (dw.com) -- the German public
    # international broadcaster. GNews pass=23/7d (items=100 fresh=98 near=75):
    # the Saudi pipeline attack from Iraq and its effect on gas prices, oil
    # prices as the Middle East escalates, US strikes on tankers off Iran,
    # Germany targeting the Russian shadow fleet, Houthis taking a Red Sea
    # chokepoint, Trump blaming Ukraine for record US diesel. Its own English
    # BUSINESS feed (rss.dw.com/xml/rss-en-bus) IS runner-reachable and dated
    # but nearly idle: items=20 span=672h fresh=4 pass=2 fetch=1.17s -- four
    # fresh items in 7d against 23 GNews passes, i.e. GNews by >10x, well past
    # the 2x tie-break. Two bounded false positives in the GNews set: "The Day
    # with Brent Goff" (the anchor, on the exact keyword `Brent`) and a palm-oil
    # biotech feature. NOTE this is the ENGLISH edition; dw.com/fa was measured
    # NOT VIABLE for Persian in 2026-08-19 (see the `fa` note in LANGUAGES) --
    # different host path, different verdict, no conflict. Articles resolve to
    # www.dw.com.
    "dw.com",
    # Euronews (euronews.com) -- pan-European news network, English edition, and
    # the best EU-energy-policy read of this wave. GNews pass=23/7d (items=100
    # fresh=100 near=77) and the pass set is ~90% on-beat and genuinely
    # European: oil past $108 after the Hormuz attack and the Saudi pipeline
    # shutdown, which European countries import the most crude, diesel records
    # in Portugal and the Italian excise cut, Europe outspending its 2025
    # Russian Arctic gas bill, Cypriot gas to Europe via Egypt and Greece, EU
    # gas prices as the IEA calls for emergency reserves, Qatar deficit on LNG
    # disruption. Its own business theme feed
    # (/rss?level=theme&name=business) is clean but thin: items=50 span=270h
    # fresh=30 pass=6 fetch=0.14s -- 6/7d against 23/7d, GNews by 3.8x, past the
    # 2x tie-break, so GNews it is (the feed remains the fallback if Google ever
    # de-indexes the domain). Articles resolve to www.euronews.com.
    "euronews.com",
    # Swissinfo (swissinfo.ch) -- SWI, the Swiss public international service,
    # English edition. GNews pass=12/7d (items=62 fresh=54 near=42). NO RSS
    # EXISTS ANY MORE: every WordPress-era feed path now answers HTTP 410 Gone
    # from the runner (/eng/rss, /eng/feed/) and the newer ones 404
    # (/service/rss/all/rss.xml, /eng/rss.xml) -- 410 is the publisher saying
    # "deliberately removed", so do not re-probe. Pass set is mostly the
    # Bloomberg "Markets Wrap" syndication SWI republishes, where oil is named
    # as the day driver (8 of 12) -- the same bounded index-wrap class accepted
    # for Moneycontrol, title-only and url-keyed -- plus the genuinely Swiss and
    # additive items this domain is registered FOR: higher crude prices lifting
    # Swiss import costs, a UBS economist on $200 oil if Hormuz stays shut, and
    # Mercuria / Glencore on a Venezuela deal. Articles resolve to
    # www.swissinfo.ch.
    "swissinfo.ch",
    # The Globe and Mail (theglobeandmail.com) -- Canada national daily and the
    # RICHEST surface of this wave: GNews pass=98/7d (items=100 fresh=100
    # near=2), i.e. essentially every indexed item matches. Pass set spans the
    # Canadian energy file (Alberta mulling a Crown corporation for gas
    # pipelines, the oil-patch M&A boom, Enbridge Line 5 tunnel permits, TC
    # Energy) and the global tape (Brent breaching US$100, the shut Saudi
    # pipeline, Houthis in the Bab el-Mandeb, the IEA on a widening 2026 supply
    # gap). Its Arc business feed is real and reachable -- items=100 span=261h
    # fresh=99 pass=15 fetch=0.98s -- but 15/7d against 98/7d is GNews by 6.5x,
    # far past the 2x tie-break. Volume caveat: at ~14 items/day this is the
    # loudest single domain of the wave (louder than Energy Intelligence at 65);
    # it is url-keyed so nothing duplicates, but if the English feed ever needs
    # thinning, this is the first domain to reconsider -- and the Arc business
    # feed above is the ready-made quieter replacement. Articles resolve to
    # www.theglobeandmail.com.
    "theglobeandmail.com",
    # CBC News (cbc.ca) -- the Canadian public broadcaster. GNews pass=39/7d
    # (items=100 fresh=99 near=60), pass set on-beat and Canada-specific: the
    # oil windfall against tariff losses, skyrocketing diesel reaching grocery
    # prices, the Ottawa 167-project pitch (pipelines to AI), TC Energy on
    # "generational" gas demand, Newfoundland collecting $500M from the oil
    # price, plus the Houthi / Hormuz supply beat. Its own business feed
    # (/webfeed/rss/rss-business) is clean but small: items=20 span=167h
    # fresh=20 pass=5 fetch=0.32s -- 5/7d against 39/7d, GNews by 7.8x. One
    # bounded false positive in the GNews set: a drone sighting that turned out
    # to be a flock of birds. Articles resolve to www.cbc.ca.
    "cbc.ca",
    # The Australian (theaustralian.com.au) -- News Corp national daily, hard
    # paywall. GNews pass=46/7d (items=100 span=166h fresh=100 near=54) and the
    # pass set is the densest Australian-domestic gas coverage of the wave:
    # Santos on the gas-reservation scheme (three separate pieces), an
    # LNG exporter winning a reprieve from it, plus the global tape (Houthis
    # invading a Saudi shipping port, the shut Saudi pipeline, oil spikes vs the
    # ASX, Trump on Venezuelan oil, the diesel war). Its own feed is a SILENT
    # ZERO, which is exactly the class this programme counts rather than
    # trusting a status code: /business/rss answers HTTP 200 and parses, but
    # items=0 span=- fresh=0 -- an empty channel, not a feed. Articles resolve
    # to www.theaustralian.com.au.
    "theaustralian.com.au",
    # Energy News Bulletin (energynewsbulletin.net) -- Aspermont Australian
    # oil & gas trade press, and the highest-PRECISION source of this wave:
    # GNews pass=8/7d (items=34 fresh=33 near=25) with the pass set ~100%
    # on-beat and Australia-upstream, a beat nothing else registered carries --
    # the gas-reservation exposure draft and the ministerial-discretion critique
    # (three pieces), the Slugcatcher column on the Hormuz refinery squeeze,
    # Santos vs LNG competitors, Cutifani leaving the Woodside board, Woodside
    # Goodwyn Alpha maintenance, the Narrabri pipeline plan. Its feed is
    # unreachable from the runner: /rss answers HTTP 404 there (a residential
    # probe gets 403 from the same paths -- an Aspermont WAF that varies by
    # origin), and /feed/ and /rss.xml are 403 residentially too. Low volume by
    # nature (a trade weekly-ish cadence), registered for the beat, not the
    # count. Articles resolve to www.energynewsbulletin.net.
    "energynewsbulletin.net",
    # Norwegian Offshore Directorate (sodir.no) -- the Norwegian upstream
    # regulator (ex-NPD), i.e. an INSTITUTION, not a newsroom: it publishes
    # licensing rounds, production figures and exploration results a few times a
    # week. Measured accordingly: GNews pass=1/7d (items=5 fresh=3 near=2), the
    # single pass being "Growing exploration interest" -- exactly the kind of
    # primary-source item this programme wants. That clears the 2026-09-14 bar
    # (pass >= 1 over 7d) and nothing else; it would have FAILED the 2026-08-18
    # bar of >= 3, which is why it is registered now and was not before. Its own
    # site is behind a Cloudflare interstitial from the runner AND residentially
    # (/en/rss, /en/whats-new/news/rss.xml, /api/rss all HTTP 403 with a "Just a
    # moment" challenge body), so GNews is the only surface. Expect long silent
    # stretches: a zero week here is normal, NOT a dead source -- verify with
    # --persisted before touching it. Articles resolve to www.sodir.no.
    "sodir.no",
    # --- Wave 5B REJECTED (measured 2026-09-14 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # Voice of America (voanews.com): DEAD PUBLISHER, not a blocked one, and
    # the two surfaces agree. GNews site:voanews.com returns items=0 (nothing
    # indexed in the 7d window at all). Its RSS feeds are reachable and parse
    # fine but are FROZEN: the Economy feed (/api/zyboql-vomx-tpetvmi) gives
    # items=20 span=260h fresh=0 and the Middle East feed
    # (/api/zrbopl-vomx-tpeovm_) items=20 span=51h fresh=0 -- i.e. 20 items
    # each, none inside 7 days. Reading the raw pubDates: the newest item on
    # both feeds, and on the USA feed, is from MARCH 2025. Two independent
    # sections frozen at the same date is a shut-down newsroom, and a frozen
    # feed is the silent-zero shape this programme measures for. Re-open only if
    # a fresh pubDate ever appears.
    # "voanews.com",
    # Der Spiegel International (spiegel.de): the English section exists but
    # publishes below this scanner resolution. Its feed is reachable and dated
    # -- /international/index.rss items=20 span=3041h fresh=0 pass=0 -- but the
    # NEWEST English item at measurement was 3 Sep 2026, ELEVEN DAYS old, so
    # nothing falls inside a 7d window. GNews agrees: site:spiegel.de returns
    # items=2 span=17035h fresh=0 pass=0 (Google indexes almost nothing of the
    # English edition; the German edition is out of scope, this programme adds
    # no non-English languages). Zero fresh on BOTH surfaces = reject. Re-open
    # only if Spiegel International returns to a daily English cadence.
    # "spiegel.de",
    # Euractiv (euractiv.com): RE-TEST of the Wave 3 REJECTED note above under
    # the lowered 2026-09-14 bar -- and it fails WORSE than in 2026-08-18, so
    # the rejection is now double-measured and should be considered settled.
    # RSS: https://www.euractiv.com/feed/ is HTTP 403 from the runner (it is
    # HTTP 200 with 100 items from a residential IP -- the classic WAF variant A
    # that makes a home probe lie). GNews: site:euractiv.com items=19 fresh=16
    # pass=0 near=16 -- ZERO passes over 7d, down from pass=2 in 2026-08-18.
    # Sixteen fresh EU-policy items, not one of which matches on title/summary.
    # Path-scoping is still unavailable (Google ignores the deeper path, same as
    # Houston Chronicle). Every surface at zero = reject even under the lower
    # bar.
    # "euractiv.com",
    # The Local Norway (thelocal.no): an English-language expat news site, not
    # an energy source, and it measures like one. RSS
    # (feeds.thelocal.com/rss/builder/no) is reachable and parses: items=20
    # span=38760h fresh=10 pass=0 near=10 -- ten fresh items in 7d, zero
    # passes. GNews: items=1 fresh=1 pass=0. Both surfaces zero over 7d.
    # Norwegian upstream news is covered by Upstream / Offshore Energy /
    # TradeWinds and (from this wave) sodir.no.
    # "thelocal.no",
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- BEGIN
    # =======================================================================
    # Africa, Middle East & North-East Asia mainstream (English) -- Wave 5C
    # (2026-09-14). Eighteen of the wave's 24 candidates are reached through
    # Google News site: (hl=en-US) + the 12-term english_keywords() subset,
    # because their own feed is WAF-blocked from the runner (HTTP 403 while the
    # same URL answers 200 from a residential IP), dateless, stale or absent --
    # the rejected surface is recorded inside each entry so a later wave does
    # not re-test it. Bodies stay unreachable, so these land title-only (empty
    # snippet). Each yield is the GNews title/summary pass count over 7d,
    # measured on the runner (measure_source.yml) against the live 91-keyword
    # set on 2026-09-14. The other six candidates had a usable feed and are in
    # RSS_FEEDS / INTERNATIONAL_RSS_DOMAINS.
    # -----------------------------------------------------------------------
    # --- Africa ---
    # AllAfrica (allafrica.com) -- pan-African aggregator, the widest African
    # net of the wave: GNews pass=36/7d (items=100 fresh=99 near=63) -- the
    # Dangote refinery IPO in every register, Angola's $99bn oil programme and
    # new discoveries, Uganda's first oil slipping to 2027, Nigeria's OPEC
    # output, gas-flare permits. Its own RSS IS runner-reachable and dated but
    # ~9x thinner, because the "energy" topic is Africa-POWER-heavy:
    # /tools/headlines/rdf/energy/headlines.rdf items=30 span=144h pass=4 and
    # .../latest/headlines.rdf items=32 span=143h pass=3. Syndication caveat:
    # AllAfrica republishes Punch / BusinessDay / ThisDay copy, so expect the
    # same story from up to four registered domains (url-keyed, each lands at
    # most once). Articles resolve to allafrica.com.
    "allafrica.com",
    # BusinessDay Nigeria (businessday.ng) -- Nigeria's business daily and the
    # richest African surface of the wave: GNews pass=53/7d (items=100 fresh=91
    # near=38), pass set heavily on-beat (Brent above $100 / $107, the Dangote
    # refinery IPO and its gantry prices, NNPC's 12 Bcf/d 2030 gas target,
    # Nigeria's crude output, deep-offshore tax incentives, India-Nigeria crude
    # talks). Its RSS is reachable but narrow: /category/energy/feed/ measured
    # items=5 span=68h fresh=5 pass=3 near=2 -- 100% on-beat yet only ~5 items
    # per three days, and it misses the markets-desk oil copy GNews finds, so
    # GNews is the surface. Articles resolve to businessday.ng.
    "businessday.ng",
    # Punch (punchng.com) -- Nigeria's highest-circulation daily. GNews
    # pass=54/7d (items=100 fresh=99 near=45). Its site-wide /feed/ IS
    # runner-reachable and dated but is a general-news firehose: items=30
    # span=4h fresh=30 pass=1 near=29, and the single pass is an "Evening
    # recap" digest rather than an energy story. GNews is the surface; the feed
    # is left unregistered so the scanner does not spend a fetch cycle on 30
    # items every four hours for ~1 on-beat item. Articles resolve to
    # punchng.com.
    "punchng.com",
    # ThisDay (thisdaylive.com) -- Nigerian daily, the third of the Lagos set.
    # GNews pass=19/7d (items=100 fresh=92 near=73): oil near $100, the Saudi
    # pipeline outage, Dangote refinery share pricing, cooking-gas supply, an
    # Okrika pipeline incident. Its /feed/ is runner-reachable (items=25
    # span=13h fresh=25 pass=3) but two of those three passes are marginal (an
    # export-promotion story on the substring `oil`, a politics column on
    # `blockade`), so GNews is the surface. Expect duplicates with AllAfrica.
    # Articles resolve to www.thisdaylive.com.
    "thisdaylive.com",
    # The East African (theeastafrican.co.ke) -- Nation Media's regional
    # weekly, the only East-African voice registered. Thin by construction (a
    # weekly): GNews pass=3/7d (items=45 fresh=45 near=42) but all three are
    # on-beat AND unique to the region -- the Lamu vs Uganda refinery race,
    # Dangote's and Alpha's refineries inside EAC trade politics, Uganda's
    # crude going to Asian markets in 2027. Clears the 2026-09 bar (>=1/7d);
    # it would have failed the 2026-08-18 bar. No RSS: /rss and /rss.xml are
    # HTTP 404 and /feed is 403 from the runner. Articles resolve to
    # www.theeastafrican.co.ke.
    "theeastafrican.co.ke",
    # Africa Oil+Gas Report (africaoilgasreport.com) -- African upstream trade
    # press, the purest African pass rate of the wave: GNews items=8 fresh=8
    # pass=8 near=0 -- every single indexed item matched (TotalEnergies in
    # Mozambique, Exxon and TotalEnergies discoveries in Angola, Saipem's
    # $350m Angolan subsea award, Algeria back at 1 Mb/d, NUPRC divestment
    # approvals). Low volume, zero noise. Its WordPress /feed/ answers 200 with
    # 10 dated items from a residential IP and HTTP 403 from the runner (WAF
    # variant A), hence GNews. Articles resolve to africaoilgasreport.com.
    "africaoilgasreport.com",
    # Energy Capital & Power (energycapitalpower.com) -- African energy
    # investment / conference press. The WEAKEST entry of the wave and exactly
    # at the 2026-09 bar: GNews items=1 fresh=1 pass=1 near=0 ("Why Liberia Is
    # Ready for Responsible Upstream Investment") -- Google indexes roughly one
    # item a week of it under en-US. Its /feed/ carries 10 dated items from a
    # residential IP but answers HTTP 403 on the runner, so the thin GNews
    # surface is all there is. Registered per the rule (>=1 pass/7d) and
    # flagged here for the first pruning pass if it stays at 1. Articles
    # resolve to energycapitalpower.com.
    "energycapitalpower.com",
    # --- Middle East ---
    # Al Arabiya English (english.alarabiya.net) -- the English edition of the
    # Saudi broadcaster, a SEPARATE surface from the Arabic alarabiya.net entry
    # in LANGUAGES["ar"] (different host, different keyword funnel): do not
    # fold them. GNews pass=52/7d (items=100 fresh=100 near=48) -- Hormuz
    # diplomacy and the postponed Gulf-Iran meeting, QatarEnergy chasing US LNG
    # to 2031, Trump on keeping Iranian oil, the Yemen/Houthi front. No RSS:
    # /tools/rss answers 403 (Cloudflare). Articles resolve to
    # english.alarabiya.net (a subdomain, no www form).
    "english.alarabiya.net",
    # The Times of Israel (timesofisrael.com) -- Israel's English daily.
    # GNews pass=40/7d (items=100 fresh=100 near=60), pass set dominated by
    # the energy-security beat (the Saudi East-West
    # pipeline hit by Iraqi drones and its multi-week outage, satellite imagery
    # of the fire, the Houthi Red Sea offensive, US sanctions). Its WordPress
    # /feed/ answers 200 with 13 dated items from a residential IP and HTTP 403
    # from the runner (WAF variant A), hence GNews. Articles resolve to
    # www.timesofisrael.com.
    "timesofisrael.com",
    # Haaretz (haaretz.com) -- Israel's English edition, hard paywall. Google
    # still indexes the headlines: GNews pass=19/7d (items=69 fresh=67
    # near=48) -- Houthi disruption of Red Sea shipping, Trump on Iran and the
    # Saudi pipeline, an Iranian ship struck near Hormuz, Falklands drilling
    # charges. No reachable feed (the legacy /srv/... RSS endpoints are 404).
    # Paywalled + title-only means pass=19 IS the ceiling, by construction.
    # Roughly a third of the pass set is West-Bank-sanctions copy that passes
    # on the geopolitics keyword `sanction` rather than on energy. Articles
    # resolve to www.haaretz.com.
    "haaretz.com",
    # Anadolu Agency English (aa.com.tr/en) -- the Turkish state wire, and the
    # single richest GNews surface of the wave: pass=60/7d (items=100 fresh=97
    # near=37) -- strikes on Russian diesel supply, the Iraqi drone platform
    # used against the Saudi pipeline, Qatar buying US LNG, Saudi output at its
    # lowest since 1990, Hormuz navigation diplomacy. The PATH form is
    # registered, same call as www.rigzone.com/news above: site:aa.com.tr and
    # site:aa.com.tr/en both measured pass=60, and the path pins the ENGLISH
    # service of a wire that also publishes in tr/ar/ru (a non-English title
    # persisted under source_lang=en is the failure this avoids). Its English
    # energy RSS (/en/rss/default?cat=energy) is registered-grade in size (30
    # items) but unusable: span=14161h fresh=1 and 3.54s. Articles resolve to
    # www.aa.com.tr.
    "aa.com.tr/en",
    # Ahram Online (english.ahram.org.eg) -- Egypt's English daily. GNews
    # pass=47/7d (items=100 fresh=99 near=52): Brent breaking $100 and the
    # inflation read, Bab al-Mandab as an oil route under Houthi threat, the
    # Saudi pipeline closure, US diesel prices, Egypt's own fuel market. No
    # RSS: /rss.aspx and /Rss/2.aspx answer 403. One recurring false-positive
    # class, bounded and amusing: counterfeit CAR OIL seizures in Egypt match
    # the substring keyword `oil`. Articles resolve to english.ahram.org.eg
    # (a subdomain, no www form).
    "english.ahram.org.eg",
    # Oil & Gas Middle East (oilandgasmiddleeast.com, ITP Media) -- Gulf
    # upstream/downstream trade press with the highest pass RATE of the entire
    # wave: GNews items=38 fresh=37 pass=37 near=0 -- 100% of what Google
    # indexes matches (Dow exiting Sadara with Aramco, Wood's $200m ExxonMobil
    # PNG LNG award, Vallourec-Aramco OCTG, CB&I's all-steel LNG tanks, EPC
    # delivery pressure across the Gulf). The site answers 403 to its homepage
    # and 405 to /rss from the runner, so GNews it is. Articles resolve to
    # www.oilandgasmiddleeast.com.
    "oilandgasmiddleeast.com",
    # AGBI - Arabian Gulf Business Insight (agbi.com) -- Gulf business daily
    # with an energy desk. GNews pass=26/7d (items=100 fresh=66 near=40), pass
    # set ~all on-beat and market-shaped (Saudi shutting the oil pipeline after
    # the Iraq-launched attacks, oil at $100 on the US-Iran escalation, Egypt
    # tripling oil imports, Iraq's export recovery and drained reserves, Brent
    # above $100 as Hormuz traffic dips). Its /feed/ answers 200 with 10 dated
    # items residentially and HTTP 403 from the runner. Articles resolve to
    # www.agbi.com.
    "agbi.com",
    # Trade Arabia (tradearabia.com/news) -- Gulf business wire. The PATH form
    # is registered and the reason is measured: the bare apex returns
    # pass=10/7d (items=100 fresh=74) but HALF of that pass set is PERENNIAL
    # section landing pages ("Gas Prices: business news, updates and
    # analysis", "Downstream Industry: ...", "Renewable Feedstocks: ...") --
    # the TradingView pathology, evergreen URLs that are not news.
    # site:tradearabia.com/news returns items=100 fresh=100 pass=13 with the
    # landing pages gone and the pass set on-beat (ADNOC/XRG/SEFE gas and LNG,
    # Vallourec-Aramco OCTG, Heisco's $359m Kuwait Oil EPC, EIA's record US
    # output, Clariant propane dehydrogenation, Oman refinery output, SLB's 3D
    # seismic survey offshore Brazil, TotalEnergies' Angola success). Residual
    # noise: 2/13 substring false positives (a BMW "refined comfort" review, a
    # drone-delivery mall). No RSS: /rss and /rss/OGN_9.xml are 404. Articles
    # resolve to www.tradearabia.com.
    "tradearabia.com/news",
    # --- North-East Asia ---
    # Nikkei Asia (asia.nikkei.com) -- Japan's business daily, English Asia
    # edition, paywalled. GNews pass=11/7d (items=71 fresh=71 near=60): China
    # buying crude as Hormuz hostilities bite, Iran retaliating after its
    # tankers were destroyed, Brent above $100, Japanese LNG and gas-trading
    # moves. RSS autopsy, and the reason the feed is NOT registered:
    # /rss/feed/nar answers 200 with 50 items but is DATELESS (span=-, all
    # published_at missing), which the scanner cannot persist -- the same class
    # as the World Oil autopsy above. Articles resolve to asia.nikkei.com (a
    # subdomain, no www form).
    "asia.nikkei.com",
    # The Japan Times (japantimes.co.jp) -- Japan's English daily. GNews
    # pass=14/7d (items=100 fresh=91 near=77): the Houthi advance in Yemen, US
    # strikes on Iranian oil tankers, the Saudi pipeline shutdown, Japanese gas
    # safety and refinery policy. RSS unusable on both paths measured:
    # /feed/topstories/ returns 5 items with span=14087h and fresh=0 (a stale
    # stub), /news/business/feed/ answers 403 from the runner. Two bounded
    # false positives in the pass set to expect (`gas` in a Formula One "foot
    # on the gas" headline, `offshore` in an Indonesian wealth-transfer story).
    # Articles resolve to www.japantimes.co.jp.
    "japantimes.co.jp",
    # NHK World (www3.nhk.or.jp/nhkworld) -- the English service of Japan's
    # public broadcaster. The PATH form is registered because the apex
    # www3.nhk.or.jp is the JAPANESE-language NHK host and /nhkworld is the
    # English surface; both measured the same three items, so the path costs
    # nothing and buys the language guarantee. GNews items=25 fresh=23 pass=3
    # near=20, and all three passes are on-beat wire-grade macro: the IEA
    # cutting its global oil supply outlook, NY crude futures at a three-month
    # high, the Houthis tightening their grip on the Red Sea. Clears the
    # 2026-09 bar (>=1/7d); low volume is expected from a broadcaster.
    # English RSS no longer exists (nhkworld/en/news/rss/all.xml and
    # .../feeds/all.xml are 404; /rss/news/cat0.xml is Japanese). Articles
    # resolve to www3.nhk.or.jp.
    "www3.nhk.or.jp/nhkworld",
    # --- Wave 5C REJECTED (measured 2026-09-14 on the runner) ---
    # None. All 24 candidates of this wave cleared the 2026-09 acceptance rule
    # on at least one surface, so there is no rejected-domain list to keep a
    # later wave from re-testing. What WAS rejected is a SURFACE per outlet --
    # every 403 / dateless / stale / thinner feed is written down inside the
    # entry above (and in the RSS_FEEDS Wave 5C header) with its numbers, which
    # is the same guard at the URL level: businesslive.co.za and
    # pipelineoilandgasnews.com have NO GNews index (items=0 both, apex and
    # path), and asia.nikkei.com's /rss/feed/nar is dateless.
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- BEGIN
    # =====================================================================
    # Wave 5D GNews en-US (2026-09-14) -- the candidates of this wave whose own
    # feed is absent (The Jakarta Post, The Edge Malaysia: every /feed, /rss
    # path 404s), WAF-blocked (Kaieteur, Hydrocarbon Engineering: 403 from the
    # runner), paywalled (BNamericas) or DATELESS (Hydrocarbon Processing --
    # see its entry). Bodies stay unreachable on this surface, so items land
    # title-only (empty snippet); each number is the GNews title-pass count over
    # 7d measured on the runner (measure_source.yml, live 91-keyword set) on
    # 2026-09-14. The wave's thirteen RSS outlets are in RSS_FEEDS +
    # INTERNATIONAL_RSS_DOMAINS.
    # =====================================================================
    # The Edge Malaysia (theedgemalaysia.com) -- Malaysia's business daily and
    # the single richest surface of this wave: GNews items=100 fresh=87 pass=69
    # near=18 (span in the raw measure is ~3y because Google surfaced archived
    # pages; 87 of 100 are inside the 7d window). Pass set is heavily on-beat --
    # "PETRONAS sees marginal profit rise ... despite elevated oil prices",
    # "Government could earn RM6.5 bil for every US$10 rise in oil price",
    # "Brent oil surges near US$100 as attacks halt Saudi energy sites",
    # "Emerging Asia stocks slide ... as oil tops US$100", plus the Petronas /
    # upstream-services corporate beat no other roster source carries. Its own
    # site has no feed (theedgemalaysia.com/rss -> HTTP 404). Noise class to
    # watch: index-wrap headlines where oil is named as the market driver
    # ("Rubber glove stocks rally as Brent crude tops US$100") -- bounded,
    # title-only and url-keyed. Articles resolve to theedgemalaysia.com.
    # NOTE: theedgesingapore.com (already registered, 2026-08-18) is a SEPARATE
    # publisher on the same brand; the two do not duplicate each other.
    "theedgemalaysia.com",
    # BNamericas (bnamericas.com) -- LatAm infrastructure / energy business
    # intelligence (Santiago). GNews items=100 fresh=98 pass=25 near=73, and the
    # pass set is the most Brazil-adjacent of the wave: "Petrobras intends to
    # contract up to six FPSOs by 2030", "Petrobras authorized to drill three
    # new wells in Foz do Amazonas", "Brazil moves closer to first natural gas
    # auction", "Why is Brazil's LNG terminal market becoming more
    # fragmented?", plus the regional book -- Vaca Muerta routes, GeoPark /
    # Venezuela, Petroecuador storage tenders, the Bolivian refinery restarts,
    # Barbados offshore round. Hard paywall, so the body is unreachable and the
    # ceiling is the title (no_body is structural here, not a bug); GNews is the
    # only viable surface. Articles resolve to www.bnamericas.com.
    "bnamericas.com",
    # The Jakarta Post (thejakartapost.com) -- Indonesia's English daily
    # (Pertamina, the fuel-subsidy and LPG-import beat). Its own feed is gone
    # (/feed and /rss both HTTP 404 from the runner, no <link rel=alternate> in
    # the homepage head), so GNews: items=100 span=154h fresh=100 pass=12
    # near=88 -- twelve title passes over 7d against a large general-news
    # near-miss tail, which is the expected shape of a national daily measured
    # through a 12-term energy query. Articles resolve to www.thejakartapost.com.
    "thejakartapost.com",
    # Kaieteur News (kaieteurnewsonline.com) -- Guyanese daily, the Stabroek
    # block / ExxonMobil watchdog desk and the closest peer of the Brazilian
    # offshore beat in this wave. Its feed is WAF-blocked (/feed/ HTTP 403 from
    # the runner, apex and www), so GNews: items=100 fresh=47 pass=19 near=28
    # (span ~9y in the raw measure because Google surfaced archived pages; 47
    # items are inside the 7d window). On-beat and specific -- "Guyana earned
    # US$778M from oil in August", "Guyana earned US$2.5B from oil in 2025 --
    # NRF Report", "Dangote refinery turns to Guyana as Nigerian crude supply
    # tightens", "Ram urges Pres. Ali to ring-fence new oil projects to stop
    # Exxon eating into Guyana's profits", "Phillips, Indar visit troubled
    # gas-to-energy project site", "Should Guyana build a small refinery?".
    # FALSE-POSITIVE CLASS, ~5 of 19: Google indexes the site's perennial "Tag
    # Archive: ..." pages, which pass on the tag words themselves and carry no
    # article ("Tag Archive: ExxonMobil FPSO future generations Guyana Oil NRF
    # oil"). They are bounded (url-keyed, land once) but they are the reason
    # this entry is worth ~14-15 real stories a week rather than 19. One more:
    # a "Mobil gas station bombing" crime item on the substring `gas`.
    "kaieteurnewsonline.com",
    # Hydrocarbon Processing (hydrocarbonprocessing.com) -- Gulf Publishing's
    # REFINING / petrochemical trade monthly + daily wire, the downstream anchor
    # of this wave and the reason it exists. Its RSS EXISTS and is rich but is
    # DATELESS, which the scanner cannot persist: /rss?feed=news items=10
    # span=- fresh=10 pass=6 and /rss?topic=refining items=10 span=- fresh=10
    # pass=9 -- every entry arrives with published_at=None (the same defect as
    # the World Oil autopsy elsewhere in this file), so the feed path is
    # unusable and Google supplies the date instead. GNews: items=66 fresh=61
    # pass=61 near=0 -- "IEA: 2026 oil supply gap will widen on delayed return
    # of normal Gulf flows", "Global diesel supply to stay tight through
    # winter", "China independent refiners scramble for oil", "U.S. crude
    # stocks fall, fuel inventories rise on strong refining, EIA says",
    # "YASREF: Fuel oil supply 'under control' despite more refiners switching
    # to middle distillate", Dangote's IPO and expansion, plus the catalyst /
    # licensor project flow (Axens, Clariant, Stamicarbon).
    # WHY pass==fresh, and the caveat that comes with it: the publisher's NAME
    # contains the substring keyword `hydrocarbon`, and Google's item summary
    # carries the publisher name -- so EVERY indexed page of this domain passes
    # by construction. That inflates the count: ~20 of the 61 are author or
    # index pages with content-free titles ("E. S. Al-Zahrani", "V. K. Khanna",
    # "August"), and roughly 40 are real downstream articles. Still far above
    # the bar and far below the >=80%-off-beat reject line, but the pass count
    # of this domain is NOT a quality signal -- judge it by the titles.
    "hydrocarbonprocessing.com",
    # Hydrocarbon Engineering (hydrocarbonengineering.com) -- Palladian
    # Publications' downstream title (refining, gas processing, storage). Its
    # feed is WAF-blocked from the runner (/rss/ HTTP 403, /feed/ HTTP 404), so
    # GNews: items=14 fresh=13 pass=13 near=0 -- "EIA: intensifying diesel
    # squeeze highlights broader oil market strains", "Sapphire Gas Solutions
    # acquires assets owned by Spectrum LNG", "Argent LNG and Albania sign MoU",
    # "Winning the AI race in refining", "Wood secures long-term construction
    # services contract with ExxonMobil PNG", plus the licensor/EPC flow. Same
    # structural caveat as Hydrocarbon Processing above -- the publisher name
    # carries the substring `hydrocarbon`, so pass==fresh by construction -- but
    # here the indexed set is small and almost entirely real articles. Thin
    # (~13/wk) and partly duplicative of Hydrocarbon Processing; kept because it
    # clears the bar and its EPC/vendor items are distinct. Articles resolve to
    # www.hydrocarbonengineering.com.
    "hydrocarbonengineering.com",
    # --- Wave 5D REJECTED (measured 2026-09-14 on the runner under the LOWERED
    #     2026-09 bar; recorded so a future wave does not silently re-test) ---
    # Caixin Global (caixinglobal.com): RE-TEST of the 2026-08-18 Wave 4
    # rejection above (which measured pass=2/7d against the old >=3 bar and
    # noted the outlet was expected to be the strongest of the two Chinese
    # business titles). It got WORSE, not better: 2026-09-14 GNews items=7
    # fresh=7 pass=0 near=7 -- Google still indexes almost nothing of a hard
    # paywall, and this time not one indexed item passed on the title. pass=0
    # fails even the lowered "pass>=1 over 7d" rule. No RSS was attempted: the
    # Wave 4 note already established the paywall is the binding constraint,
    # and yicai.com / jiemian.com cover Chinese business on the zh route.
    # "caixinglobal.com",
    # OilNOW (oilnow.gy): the Guyanese Stabroek-block outlet, and the loss of
    # this wave -- it is the direct peer of the Brazilian offshore beat. BOTH
    # surfaces are shut. RSS: HTTP 403 from the runner on every path tried
    # (https://oilnow.gy/feed/, /feed, /rss and https://www.oilnow.gy/feed/) --
    # a Cloudflare-style block, not a 404. GNews: site:oilnow.gy items=1
    # fresh=1 pass=0, site:www.oilnow.gy items=0 -- Google indexes essentially
    # nothing of the domain either, which is consistent with the same WAF
    # refusing the crawler. Every surface ~0 -> rejected. Guyana coverage is
    # kept through kaieteurnewsonline.com (registered above) and bnamericas.com.
    # Worth re-testing only if the 403 lifts.
    # "oilnow.gy",
    # Stabroek News (stabroeknews.com): the other Guyanese daily. RSS: HTTP 404
    # on every path tried (https://www.stabroeknews.com/feed/,
    # https://stabroeknews.com/feed/, /?feed=rss2, /category/business/feed/,
    # /rss) and the homepage refuses a residential fetch with 403, so no feed
    # could be discovered. GNews: site:stabroeknews.com items=0 and
    # site:www.stabroeknews.com items=0 -- the domain is simply not in the
    # Google News en-US index. Both surfaces 0 items -> rejected under the
    # explicit "every surface returns 0" clause.
    # "stabroeknews.com",
    # Petroleum Economist (petroleum-economist.com): Gulf Publishing's
    # subscription energy monthly. RSS: HTTP 404 on /rss?feed=news (the pattern
    # that works for its sibling hydrocarbonprocessing.com), /rss/, /feed/ and
    # /rss.xml. GNews: items=7 fresh=7 pass=7 -- and the pass count is a
    # MIRAGE: all seven items carry the identical title "Petroleum Economist"
    # (Google indexes the paywall stub, not the article), so every one passes on
    # the `petroleum` keyword while carrying zero information. Seven
    # indistinguishable title-only rows a week is noise, not coverage; the
    # substantive numbers are the ones that matter, and the usable-title count
    # is 0. Rejected. Re-test only if the site starts exposing article titles to
    # Google.
    # "petroleum-economist.com",
    # Oilfield Technology (oilfieldtechnology.com): Palladian's upstream title,
    # sibling of the registered hydrocarbonengineering.com. RSS: HTTP 403 on
    # /rss/ and /rss?feed=news, HTTP 404 on /feed/, and the homepage exposes no
    # <link rel=alternate> feed. GNews: items=2 fresh=2 pass=0 near=2 -- Google
    # indexes two items a week for the domain and neither passed on the title.
    # pass=0 on the only reachable surface -> rejected. Note this is the WEAKER
    # half of the Palladian pair: hydrocarbonengineering.com yields 13/wk on the
    # same surface, so the block is the domain's index footprint, not the
    # publisher.
    # "oilfieldtechnology.com",
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- BEGIN
    # =======================================================================
    # O&G / refining / shipping trade press & institutions -- Wave 5E
    # (2026-09-14), GNews half. Same shape as the Wave 1-4 GNews blocks above:
    # English-only publishers whose own feed is WAF-blocked, absent, DATELESS,
    # credential-walled or frozen, so the surface is Google News site: (hl=en-US)
    # + the 12-term english_keywords() subset. Bodies stay unreachable, so items
    # land title-only (empty snippet). Every yield is the GNews title-pass count
    # over 7d, measured on the runner (measure_source.yml) against the live
    # 91-keyword set on 2026-09-14, under the LOWERED 2026-09 bar (pass >= 1/7d
    # registers; only a 0-item or >=80%-off-beat candidate is rejected). Every
    # feed autopsy quoted below was ALSO run on the runner (measure_source.yml
    # with the feed URL), never from a residential IP.
    # =======================================================================
    # Offshore Magazine (offshore-mag.com) -- Endeavor Business Media's offshore
    # E&P title: same publisher as OGJ (Wave 1a above) and the same WAF story.
    # The Endeavor feed path (/__rss/website-scheduled-content.xml) answers HTTP
    # 400 from the runner, and every /rss* path answers a Cloudflare "Just a
    # moment" interstitial (403) from a residential IP -- no usable feed. GNews
    # pass=20/7d (items=23 fresh=20 near=0), pass set ~100% on-beat and partly
    # Brazil-adjacent: CIMC Raffles completing the hull of Petrobras P-84 (first
    # large all-electric FPSO of its class), an ANPG / ExxonMobil Angola
    # discovery, Eni's Cronos gas tieback off Cyprus, PETRONAS / Ventura
    # drilling, Norway's APA 2026 round, Saipem-Subsea7. ONE offshore-WIND item
    # in the pass set (an advisory-board piece), the `offshore` keyword's
    # standing false positive. Articles resolve to www.offshore-mag.com.
    "offshore-mag.com",
    # Gas Processing & LNG (gasprocessingnews.com) -- Gulf Energy Information's
    # gas-processing title, and a textbook repeat of the World Oil autopsy above.
    # Its feed IS runner-reachable and rich -- /rss?feed=news gave items=10
    # fresh=10 pass=8 fetch=0.77s -- but it is DATELESS: span=- and every item
    # printed at -1.0h, so stage 4 would drop all of it for a null published_at,
    # persisting zero rows. Google News supplies the date instead: GNews
    # pass=19/7d (items=20 fresh=19 near=0), pass set on-beat (TotalEnergies
    # moving to FID on Papua LNG, Cheniere Corpus Christi, NNPC floating LNG,
    # the Nord Stream 2 operator on the US sanctions bill, Petrobras /
    # PetroReconcavo / Brava / Origem expanding the UTG Catu contracts, AltaGas'
    # propane export terminal). One bare "News" section page in the pass set --
    # bounded, url-keyed. Articles resolve to www.gasprocessingnews.com. Do NOT
    # promote this to RSS_FEEDS without first re-checking for a pubDate.
    "gasprocessingnews.com",
    # Quantum Commodity Intelligence (qcintel.com) -- energy / biofuels price
    # reporting agency, and by far the richest surface of this wave: GNews
    # pass=84/7d (items=100 fresh=100 near=16), the pass set price-desk grade and
    # ~100% on-beat (West of Suez / East of Suez daily crude and product wraps,
    # Dubai crude at a five-month high, OPEC+ baselines, Indian refiner runs,
    # Russian refinery outages and gasoline flows, Americas feedstocks / ethanol,
    # Petrobras correcting a gasoline notice as a tax cut replaces the subsidy).
    # Its own RSS is credential-walled: /rss answers "Missing or invalid
    # credentials" behind HTTP 403 from the runner. Articles resolve to
    # www.qcintel.com.
    "qcintel.com",
    # Lloyd's List (lloydslist.com) -- the shipping wire of record (Informa),
    # hard paywall. Feed: /rss answers HTTP 500 from the runner (and a "Layout
    # Not Found" page residentially). GNews pass=22/7d (items=64 fresh=61
    # near=39), pass set ~100% the tanker / chokepoint beat: the VLCC market at a
    # historic high, crude transits into Hormuz down to a trickle, the Yanbu
    # detour via SuMed and Suez, shadow-fleet dilemmas, Bab el Mandeb after
    # Mokha, marine insurers' 80/20 rule. It overlaps TradeWinds on the big
    # stories, but news_articles is url-keyed so each copy lands at most once.
    # Articles resolve to lloydslist.com.
    "lloydslist.com",
    # Riviera Maritime Media (rivieramm.com) -- tanker / offshore-vessel trade
    # press. Feed: /rss answers HTTP 200 with HTML (Affino CMS), which feedparser
    # rejects as not well-formed -- a SILENT ZERO on the feed path, not a 404, so
    # only an item count tells you it is dead. GNews pass=22/7d (items=100
    # fresh=69 near=47): VLCC / VLAC and LNG-carrier newbuild orders, a fatal
    # tanker attack off the UAE, ExxonMobil's Rovuma LNG EPC picks, the first
    # operational ammonia bunker vessel, "insane" VLCC fixtures above US$1M/day.
    # Two bounded caveats: one offshore-WIND item in the pass set, and Riviera's
    # cosmetic "News Content Hub - " title prefix on syndicated items. Articles
    # resolve to www.rivieramm.com.
    "rivieramm.com",
    # Mobility Plaza (mobilityplaza.com) -- fuel-retail / forecourt trade press,
    # and the LIVE home of PetrolPlaza: petrolplaza.com now 301-redirects here
    # (measured 2026-09-14), so the candidate is registered under the new domain
    # and the old one is recorded as rejected below. No feed at all (/feed/,
    # /rss, /rss.xml, /news/feed all 404 or 500 from the runner; no feed <link>
    # in the homepage head). GNews pass=41/7d (items=53 fresh=52 near=11), pass
    # set the service-station / downstream-retail beat no other registered source
    # carries: ENOC transferring 25 Sharjah stations to ADNOC, Oman Oil Marketing
    # adding LPG, Shell stations around Edmonton running dry, Phillips 66 / REG
    # renewable diesel, Brazil's ANP inaugurating an upgraded fuels-analysis
    # centre, CNG/LNG stations in Spain. Articles resolve to
    # www.mobilityplaza.com.
    "mobilityplaza.com",
    # IEA (iea.org) -- the International Energy Agency's news / analysis desk.
    # Feed: /rss/news and /news/rss answer HTTP 403 from the runner (Cloudflare,
    # zone-wide). GNews pass=5/7d (items=26 fresh=13 near=8), every pass on-beat
    # and PRIMARY: Oil Market Report September 2026 (twice, two URLs), the
    # gas-security commentary after the series of supply shocks, Gas Reserve
    # Mechanisms and Flexibility Options, IEA total oil stocks for June 2026.
    # Low volume by construction -- the IEA publishes reports, not a wire -- but
    # these are the documents the market reacts to. Articles resolve to
    # www.iea.org.
    "iea.org",
    # OPEC (opec.org) -- press releases and the Monthly Oil Market Report.
    # RE-TESTED 2026-09-14, reversing the Wave 2 rejection recorded above (which
    # measured pass=4/7d, all four the same research-award press release, and
    # rejected it under the OLD ">=3 DISTINCT on-beat per 7d" bar). New
    # measurement: GNews pass=3/7d (items=4 fresh=3 near=0) and all three passes
    # are the MOMR ("OPEC Digital Publications - Monthly Oil Market Report" x2 +
    # "OPEC Monthly Oil Market Report"), i.e. ~1 distinct document per week.
    # Registered under the LOWERED 2026-09 bar: the MOMR is a primary source the
    # whole desk reads, and OPEC's SPA still has no feed (/rss, /feed, /rss.xml
    # all serve HTML or 403 from the runner). Expect ~1 item a week, sometimes
    # the same report under two URLs; url-keyed, so the duplication is bounded.
    # This entry SUPERSEDES the commented-out "opec.org" line in the Wave 2
    # REJECTED block above -- do not re-test it a third time.
    "opec.org",
    # Wood Mackenzie (woodmac.com) -- consultancy press / insights. No feed
    # (/rss/ and /feed/ both 404 from the runner). GNews pass=21/7d (items=24
    # fresh=21 near=0), but read that number with the S&P-Ratings caveat in mind:
    # only ~5-6 passes are real sector pieces (a promising UAE shale discovery,
    # high oil prices accelerating EV sales, the South Atlantic's next
    # exploration chapter, gas in Indian power markets, EU gas imports reaching
    # 98% by 2050), while the rest are PERENNIAL marketing pages that match on
    # the `Wood Mackenzie` keyword itself ("Data & analytics solutions" x7, an
    # analyst bio, "Wood Mackenzie at APGCE", "Market insights for investors")
    # plus solar / coal / AI-datacentre commentary off our beat. That is ~75%
    # noise -- under the >=80% rejection threshold, and bounded exactly like
    # Conjur and the S&P Ratings releases (title-only, url-keyed, each page lands
    # at most once) -- so it is registered per the numeric bar, with the
    # redundancy recorded here for a future pruning pass. Articles resolve to
    # www.woodmac.com.
    "woodmac.com",
    # Rystad Energy (rystadenergy.com) -- consultancy press / market commentary.
    # No feed (/rss, /news/feed and /feed/ all 404 from the runner). GNews
    # pass=2/7d (items=4 fresh=4 near=2), both on-beat and primary: the
    # oil-demand outlook (a 2030 plateau to a 40M bpd swing by 2050) and the H2
    # 2026 Gas Turbine Report. Thin but clears the 2026-09 floor; expect a couple
    # of items a week. Articles resolve to www.rystadenergy.com.
    "rystadenergy.com",
    # Vortexa (vortexa.com) -- seaborne-flows analytics, Insights desk. Its
    # /feed/ answers HTTP 200 with the Next.js app shell (HTML), which feedparser
    # rejects as not well-formed, and /insights/feed/ is 404 -- another silent
    # zero rather than an honest error. GNews pass=2/7d (items=3 fresh=2 near=0),
    # both on-beat flow analytics (gasoline loadings to Europe at a
    # counter-seasonal record; China's crude purchase priority in a high-freight
    # environment). Low frequency by construction -- a couple of insight posts a
    # week. Compare Kpler, the same kind of house, which DOES have a dated blog
    # feed and is registered in RSS_FEEDS. Articles resolve to www.vortexa.com.
    "vortexa.com",
    # Oxford Institute for Energy Studies (oxfordenergy.org) -- independent
    # energy research institute. It HAS a real, dated WordPress feed
    # (/publications/feed/, 12 items from a residential IP) but that feed answers
    # HTTP 403 from the RUNNER -- the WAF-variant-A residential/datacenter split
    # this programme keeps meeting -- so the feed path would return 0 items in
    # production. GNews pass=1/7d (items=2 fresh=2 near=1): "Italy's 'Liquidity
    # Service' Risks Gas Market Distortion". One pass in 7d is exactly the
    # 2026-09 floor, which is the honest expectation for an institute that
    # publishes research papers rather than news. Articles resolve to
    # www.oxfordenergy.org.
    "oxfordenergy.org",
    # ICIS (icis.com) -- chemicals / energy price wire (RELX), hard paywall.
    # RE-TESTED 2026-09-14, reversing the Wave 3 rejection recorded above (which
    # measured pass=2/7d, both generic Hormuz duplicates, under the old bar). New
    # measurement: GNews pass=3/7d (items=10 fresh=10 near=7) and all three are
    # on-beat AND distinct: oil surging more than $3/bbl as the Saudi East-West
    # pipeline shuts, blocked Middle East oil and gas / LNG exports weighing on
    # the industry, Ukraine bracing for winter on stronger gas stocks. Its own
    # WordPress feed is a SILENT ZERO: /explore/feed/ answers HTTP 200 with a
    # well-formed channel and NO <item> at all from the runner (items=0,
    # fetch=0.91s), and /explore/resources/news/feed/ is 404 -- count items, not
    # status. Registered under the LOWERED 2026-09 bar; SUPERSEDES the
    # commented-out "icis.com" line in the Wave 3 REJECTED block above.
    "icis.com",
    # JPT / SPE (jpt.spe.org) -- the Society of Petroleum Engineers' Journal of
    # Petroleum Technology. RE-TESTED 2026-09-14, reversing the Wave 3 rejection
    # above (pass=2/7d, both duplicates of stories other sources carried). New
    # measurement: GNews pass=1/7d (items=4 fresh=4 near=3) -- a single pass, the
    # ISO 20815:2026 production-assurance / reliability standard piece (it matches
    # on `ammonia` through its CCS / hydrogen scope). /feed and /rss are still
    # HTTP 404 from the runner, as in the 2026-08-18 autopsy. This is the
    # THINNEST entry of the wave and sits exactly on the 2026-09 floor: Google
    # indexes a technical monthly sparsely. Kept for the E&P-engineering angle no
    # wire covers; if a later audit still finds ~1 undistinctive item a week it is
    # the first candidate for a pruning pass. Articles resolve to jpt.spe.org (a
    # subdomain, no www form).
    "jpt.spe.org",
    # --- Wave 5E REJECTED (measured 2026-09-14 on the runner; recorded so a
    #     future wave does not silently re-test) ---
    # Natural Gas World (naturalgasworld.com): BOTH surfaces dead. Its /rss
    # answers HTTP 200 with 20 well-formed, DATED items -- every one of them from
    # February 2025 (items=20 span=530h fresh=0 pass=0 fetch=0.36s). The feed is a
    # frozen archive whose only current timestamp is the channel's own
    # lastBuildDate: the exact "answers 200 and never anything new" failure
    # FEED_STALE_HOURS exists to name, and a silent zero for any freshness window.
    # /feed and /rss.xml serve HTML. And site:naturalgasworld.com on GNews returns
    # items=0 -- Google News does not index the domain under en-US at all. No
    # surface, so no entry.
    # "naturalgasworld.com",
    # OPIS (opisnet.com): fuel-price wire (Dow Jones), hard paywall. Its
    # WordPress /feed/ IS runner-reachable and dated, but it is the marketing blog
    # and publishes roughly monthly: items=10 span=1899h fresh=0 pass=0
    # fetch=0.36s, newest item 19 days old -- nothing inside the 7d window, and
    # nothing inside the 48h alternative either. /news/feed/ and
    # /category/blog/feed/ are 404. And site:opisnet.com on GNews returns items=0:
    # the priced content sits behind the wall and is not indexed. No surface.
    # "opisnet.com",
    # PetrolPlaza (petrolplaza.com): the DOMAIN is retired -- it 301-redirects to
    # mobilityplaza.com, which is registered above with pass=41/7d.
    # site:petrolplaza.com on GNews returns items=1 fresh=1 pass=0 (a single
    # legacy page), and /rss is 404 on the new host. Rejected as a domain, NOT as
    # an outlet: the outlet is alive and is registered under Mobility Plaza.
    # "petrolplaza.com",
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
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
    # Jornal do Comercio (Porto Alegre/RS) — PRIMARY surface for this source.
    # Read the RSS_FEEDS entry above first; it explains why two of its RSS feeds
    # are ALSO registered.
    #
    # /sitemap.xml is a single <urlset> with NO news:news namespace (only <loc>,
    # <lastmod>, <image:image>), so this is the STANDARD_SITEMAPS slot, not a
    # Google News sitemap — and is_sitemap_url() does not match the bare
    # "/sitemap.xml" path either, so putting it in RSS_FEEDS would hand it to
    # feedparser and yield zero. It carries a rolling ~5-DAY window of every
    # section (361 URLs on 2026-08-11, 87 of them /economia/), which is what
    # makes it worth the bytes: the per-editoria RSS feeds hold 5-10 items each
    # and rotate within hours, so by the time a scan runs they have already
    # dropped most of the day.
    #
    # Measured 2026-08-11 on the runner, 48h window, against the LIVE keyword
    # set from Supabase (47 keywords, 15 of them exact — NOT the 25 hardcoded
    # fallback; see the measurement trap in the Gazeta do Povo comment, and
    # scripts/measure_source.py, which now refuses to measure against it):
    #
    #     feed                       items  span  fresh  pass  near  rescued
    #     sitemap.xml                  237   96h    154    10     0     n/a
    #     home                          30   14h     30     5    25       0
    #     economia                       7     -      5     0     5       1
    #     cadernos/jc-logistica          5   99h      3     0     3       1
    #     internacional                  5   27h      5     1     4       0
    #     politica / geral / jornal-cidades / empresas-e-negocios /
    #       opiniao / cultura / esportes                0             0
    #     ultimas-noticias               0     -      0     0     0       0
    #
    # The sitemap's 10 passes contain every one of home's 5 plus five the feeds
    # had already rotated out ("Petróleo fecha em alta", "Gasolina com 32% de
    # etanol", "Quando o risco marítimo chega à bomba de combustível", ...).
    # Precision is unusually high — all 10 are on-beat or beat-adjacent, zero
    # junk — because JC slugs are the headline and the live keyword set is
    # tuned; `near` is structurally 0 because title-less items are never offered
    # the rescue.
    #
    # TRAP WORTH THE LINE: /_conteudo/ultimas-noticias/rss.xml, the obvious
    # "latest news" feed, answers HTTP 200 with a well-formed but EMPTY channel
    # (497 bytes, zero <item>). Registering it would have produced a source that
    # looks healthy forever and publishes nothing — the monitormercantil failure
    # mode with no Cloudflare to blame. The live general feed is
    # /_conteudo/home/rss.xml; it is not registered for the reasons above.
    #
    # COST: this is the only JC surface that pays a fetch per item. Title-less
    # items that clear the slug filter are enriched every scan (~8 within the
    # 24h production window, ~128 KB each); the two RSS feeds are free in
    # fast_mode (title + date present ⇒ no fetch_html) apart from the capped
    # lede rescue. Slugs are ASCII with no accents, so the ACCENTED keywords
    # never fire on them — "petroleo"/"combustivel" match, "petróleo" does not.
    # That is why the unaccented variants in the keyword table earn their keep.
    "www.jornaldocomercio.com": [
        "https://www.jornaldocomercio.com/sitemap.xml",
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


# -----------------------------------------------------------------------------
# Staleness thresholds — "this feed answers 200 but has stopped moving".
#
# A feed that returns ZERO items is already named in the run summary. The nastier
# failure is the one that keeps returning items and never any NEW ones: a CDN
# pinning a cached copy (Poder360, 2026-08-07 -> 2026-08-14), a WordPress cache
# plugin stuck writing the same /feed/ output, an editorial section quietly
# archived. Every counter stays plausible and the source is simply gone.
#
# So we also measure, per feed, the age of its NEWEST item and say it out loud
# once it crosses a threshold. The default is deliberately generous: it exists to
# catch a source that publishes several times a day and froze, not to nag about
# slow ones. Feeds that are legitimately slow declare their own budget below —
# measured 2026-08-14 over one collect of all 47 registered feeds, whose stalest
# healthy entries were jornaldocomercio 95h, ineep 64h and gazeta/agronegocio 48h.
# A source that starts nagging every run is a source whose entry here is wrong,
# or one that really did die; either way it wants a human, not a bigger number.
# -----------------------------------------------------------------------------

FEED_STALE_HOURS_DEFAULT = 48.0

FEED_STALE_HOURS: dict[str, float] = {
    # Research institute — publishes analyses, not news.
    "ineep.org.br": 14 * 24.0,
    # Regional dailies / weekly sections: measured multi-day gaps while healthy.
    "www.jornaldocomercio.com": 10 * 24.0,
    "www.gazetadopovo.com.br": 7 * 24.0,
    "obastidor.com.br": 5 * 24.0,
    "www.theagribiz.com": 5 * 24.0,
}


def feed_stale_hours(domain: str) -> float:
    """Hours a feed may go without a new item before it is called out."""
    return FEED_STALE_HOURS.get(domain, FEED_STALE_HOURS_DEFAULT)


# -----------------------------------------------------------------------------
# Per-host feed timeout overrides — the fix for the "rich-but-slow feed silently
# rejected" class.
#
# fetcher.FEED_TIMEOUT (4s) is what ONE feed request may spend, and it is tight
# on purpose: ~65 feeds share a 22s COLLECT_DEADLINE, so a single slow origin
# must not starve the rest. The failure mode that tightness creates is silent and
# expensive: a feed that answers 200 with a full, dated, on-beat payload in
# 6-13s is recorded as a ReadTimeout error and the outlet reads as dead. Not
# hypothetical — the 2026-08-18 international waves lost three candidates to it
# and had to downgrade them to GNews title-only coverage or reject them:
#     eia.gov                  9-13s   (the US EIA's own feed)
#     intellinews.com          over budget -> GNews
#     globalenergynetwork.net  ~6.6s   -> GNews
# Listing a host here is how a wave says "this feed is worth waiting for". The
# number is the MEASURED fetch time (measure_source prints `fetch=N.NNs`) plus
# headroom — never a guess.
#
# Semantics:
#   * key   = the feed host, the RSS_FEEDS key, same style as FEED_STALE_HOURS.
#             Lookup is www-insensitive both ways, so "eia.gov" also covers
#             "www.eia.gov" and vice-versa: an override that silently does
#             nothing because the registry key carries a www would be the very
#             class of bug this dict exists to kill.
#   * value = seconds for ONE request on that host, replacing FEED_TIMEOUT.
#   * scope = the RSS / Google-News-sitemap feed path (fetcher._fetch_one and
#             _fetch_sitemap), and therefore scripts/measure_source.py, which
#             calls it. STANDARD_SITEMAPS keeps its own STANDARD_SITEMAP_TIMEOUT.
#   * a value at or above fetcher.COLLECT_DEADLINE (22s) buys nothing: the global
#             deadline abandons the feed first.
#
# EMPTY on purpose: any host not listed keeps FEED_TIMEOUT unchanged, so adding
# this dict changed no production behaviour.
# -----------------------------------------------------------------------------

FEED_TIMEOUT_OVERRIDES: dict[str, float] = {
    # =======================================================================
    # WAVE 5 ANCHORS (2026-09-14) — five parallel branches, ONE block each.
    # A wave appends its entries ONLY between its own BEGIN/END markers and
    # leaves every fence line alone: >=4 untouched lines between blocks is what
    # keeps the five diffs in non-overlapping hunks, so the branches merge
    # without conflicts. Empty is the correct state until a wave lands.
    # =======================================================================
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- BEGIN
    # EMPTY ON PURPOSE. Every feed this wave registered is fast: FT 0.34-0.52s,
    # The Guardian 0.49s, CBS News 0.10s -- all well inside the 4s FEED_TIMEOUT,
    # so no host here needs a longer budget.
    # Recorded so a later wave does not re-open it: The Washington Post's feeds
    # (feeds.washingtonpost.com/rss/{business,world,national}) DO time out at the
    # 4s default and DO answer at 9.13-10.13s when re-measured with
    # feed_timeout=12 -- and were still NOT given an override, because what
    # arrives after those ten seconds is a stub: items=3 span=10h pass=0
    # (business), items=11 span=44h pass=3 (world), items=7 span=40h pass=0
    # (national). Spending ~10s of a 22s COLLECT_DEADLINE shared by ~65 feeds to
    # collect three passes, when GNews site:washingtonpost.com measures
    # pass=40/7d, is a bad trade. WaPo is in ENGLISH_NO_RSS_DOMAINS instead.
    # --- Wave 5A (2026-09-14): Global wires & US mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- BEGIN
    # --- Wave 5B (2026-09-14): NO override needed, measured not assumed. The
    #     slowest feed this wave registered is The Irish Times business feed at
    #     fetch=2.42s, inside the 4s FEED_TIMEOUT; every other registered feed
    #     came back in 0.05-0.98s (afr 0.05 / 0.15s, lemonde 0.08s, investing
    #     0.10s, france24 0.31s, abc 0.32s, cityam 0.40s, financialpost 0.44 /
    #     0.51s, calgaryherald 0.70s). The three candidates with no fetch time
    #     at all were BLOCKED, not slow -- euractiv.com HTTP 403, sodir.no HTTP
    #     403, energynewsbulletin.net HTTP 404 -- so a longer budget would not
    #     have helped them and none is registered here.
    # --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- BEGIN
    # Wave 5C (2026-09-14): no override needed. The six feeds this wave
    # registered were all measured well under the 4s default on the runner --
    # BusinessLive (Arc) 1.74s, Korea Herald 1.58s, Pipeline Oil & Gas 1.48s,
    # Tehran Times 0.71s, Daily Sabah 0.45s / 0.37s, Middle East Eye 0.21s. The
    # only slow feed the wave met, Anadolu Agency's English energy RSS (3.54s),
    # is unusable for a different reason (30 items but span=14161h, fresh=1)
    # and its outlet is GNews-covered instead.
    # --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- BEGIN
    # (deliberately empty: no Wave 5D feed needed more than the 4s default.
    #  Slowest measured fetch of the wave, 2026-09-14 on the runner:
    #  bangkokpost.com 1.99s, e.vnexpress.net 1.75s, en.trend.az 1.08s; every
    #  other registered feed answered under 0.7s. The wave's blocked candidates
    #  failed with HTTP 403/404, never with a timeout, so a longer budget would
    #  not have rescued any of them -- re-measured with the default and, for
    #  the 403s, on two host spellings each.)
    # --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- BEGIN
    # --- Wave 5E (2026-09-14): the first two entries this dict has ever had.
    # Both are 2026-08-18 timeout casualties, re-measured on the runner.
    # US EIA -- Today in Energy. Registered in RSS_FEEDS above (items=15 fresh=2
    # pass=2 over 7d) and it needs this budget or it silently returns 0 items
    # again, which is exactly how the outlet was lost the first time.
    # MEASURED fetch=10.14s on the runner with feed_timeout=15 (2026-09-14);
    # 14.0 = that plus ~40% headroom, and still under COLLECT_DEADLINE (22s),
    # past which the global deadline would abandon the feed anyway.
    "eia.gov": 14.0,
    # Global Energy Network (globalenergynetwork.net) -- the second 2026-08-18
    # casualty, re-measured 2026-09-14 with feed_timeout=12: still slow, still
    # rich. items=1000 span=4193h fresh=21 pass=19 near=2 fetch=10.65s -- the
    # feed is the full 1000-entry archive (~6.8 MB), which is WHY it is slow; the
    # host itself is not. 14.0s covers it with headroom.
    # Its GNews entry in ENGLISH_NO_RSS_DOMAINS (pass=18/7d) is deliberately LEFT
    # IN PLACE: this wave only proves the feed is affordable again. Promoting it
    # to RSS (an RSS_FEEDS entry + the apex/www pair in INTERNATIONAL_RSS_DOMAINS
    # + removing the GNews line) touches lines outside this wave's blocks, so it
    # is a follow-up call. Until then this entry is inert -- feed_timeout() is
    # only consulted on the feed path.
    "globalenergynetwork.net": 14.0,
    # bne IntelliNews (intellinews.com) -- the THIRD 2026-08-18 casualty, and the
    # one that needs NO entry here. Re-measured 2026-09-14 with feed_timeout=12,
    # www.intellinews.com/feed/ answered in fetch=0.48s: items=10 span=8h
    # fresh=10 pass=4 near=6. That is ~8x inside the 4s default, so the
    # 2026-08-18 "Read timed out" was a transient / slow-origin episode, not a
    # standing budget problem, and an override would be a no-op. Recorded here as
    # a COMMENT (not an entry) so the next wave does not re-measure it; its GNews
    # entry stays untouched and the RSS promotion is the same follow-up call as
    # globalenergynetwork.net above.
    # --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- END
    #
    # ---- merge fence: keep >=4 lines between wave blocks ----
    #
    #
}


def _www_variants(host: str) -> tuple[str, str]:
    """('eia.gov', 'www.eia.gov') for either spelling of the same host."""
    bare = host[4:] if host.startswith("www.") else host
    return bare, f"www.{bare}"


def feed_timeout(domain: str, default: float, *, host: str | None = None) -> float:
    """Seconds one feed request may take for `domain` (default = FEED_TIMEOUT).

    `domain` is the registry key the fetcher carries; `host` is the netloc of the
    URL actually being fetched, checked as a fallback so an override still bites
    when the two spellings differ. Both are matched www-insensitively.
    """
    for candidate in (domain, host):
        if not candidate:
            continue
        cand = candidate.strip().lower().rstrip("/")
        for key in (cand, *_www_variants(cand)):
            if key in FEED_TIMEOUT_OVERRIDES:
                return FEED_TIMEOUT_OVERRIDES[key]
    return default



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
    # International oil & gas retrieval, ordered by yield. EXACTLY
    # ENGLISH_KEYWORD_CAP (12) terms — the block Google will not truncate.
    "oil", "gas", "diesel", "Brent", "WTI", "OPEC",
    "crude", "LNG", "refinery", "gasoline", "Petrobras", "sanction",
    # Reserve candidates — documented, NOT emitted (the tuple is full at 12).
    # Promote one only by demoting one above, so the block never re-crosses the
    # truncation limit in silence:
    #   Brazilian / regional : Braskem, Cosan, Hormuz, Vibra
    #   prior reserves       : PRIO, Ultrapar, Ipiranga, OceanPact, refit, ANP
)
# NOTE (2026-08-18) — this tuple gates RETRIEVAL, not MATCHING, and the two
# have different failure modes. Do not "fix" a matching problem here.
#
# CROSS-REPO CONTRACT with the dashboard's Supabase table
# `news_hunter_default_keywords`: english_keywords() INTERSECTS the live keyword
# set (that table, plus the per-user `news_hunter_keywords` copies the
# /news-hunter feed scopes on) with this tuple BY EXACT STRING (casefolded).
# Consequences:
#   * Every string here MUST exist verbatim as a DB keyword row, or it is
#     SILENTLY dropped from retrieval — no error, just an absent term. Of the 12
#     above, 7 (oil, gas, diesel, Brent, WTI, OPEC, Petrobras) already have rows;
#     the other 5 (crude, LNG, refinery, gasoline, sanction) are being inserted
#     by a parallel worker_supabase task. Until those rows land, english_keywords
#     degrades gracefully: the missing terms are simply not emitted (see its
#     intersection + empty-set fallback below), so the scanner keeps working.
#   * RETRIEVAL is the funnel: this tuple, capped at ENGLISH_KEYWORD_CAP and
#     emitted in the priority order above, is what we ASK Google for; Google
#     STEMS the terms it receives.
#   * MATCHING is the sieve: filter.matches_keywords runs over the FULL live
#     set, uncapped, as a literal substring (or \b-bounded) test with NO
#     stemming. A matching gap is therefore closed with a DB ROW, never by
#     editing this tuple (a stem added here only burns a retrieval slot — see
#     the 'refinery' case below).
#
# PRIORITY ORDER matters. Unlike the PT `site:` route — whose live set arrives
# ALPHABETICALLY sorted and is truncated by Google at ~13 terms (the still-open
# consequence documented at ~sources.py:892-901) — the English route emits in
# THIS explicit yield order, so the highest-yield terms always survive the
# truncation. `when:` must still precede the OR block on every query (the
# `when:`-before-OR rationale above) or the time filter is the part dropped.
#
# The retrieval/matching asymmetry is why 'refinery' converts to nothing
# WITHOUT its DB row. Measured 2026-08-18, site:www.argusmedia.com, when:7d:
#
#     query term   raw items returned by Google
#     "refinery"          27   <- includes headlines that say "refineries"
#     "refineries"        21
#     "refiner"            6
#
# Google already hands us the "refineries" headlines; our own filter drops them,
# because "refineries" does not contain the string "refinery" ('refiner|y' vs
# 'refiner|ies'). The fix is a keyword ROW in news_hunter_default_keywords (+ the
# per-user copies), NOT an entry here: adding 'refineries' would spend one of the
# 12 retrieval slots to ask for items Google already returns for 'refinery'.
# Keep 'refinery' above for retrieval; let the DB row do the matching.
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


# -----------------------------------------------------------------------------
# Language registry (Wave B1-a — mechanism generalization).
#
# The scanner's one non-Portuguese retrieval path is a LANGUAGE SWITCH on the
# Google News `site:` route (hl/gl/ceid + a per-language keyword subset). It used
# to be a single hard-coded English function (google_news_site_queries_en); it is
# now expressed through this registry so future waves can add languages as data,
# not code. THIS WAVE SEEDS ONLY ENGLISH — English remains the only active
# language, and google_news_site_queries_en (below) is now a shim over
# LANGUAGES["en"], reproducing the historical en URLs byte-for-byte
# (tests/test_multilingual_en_frozen.py pins that).
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class LangConfig:
    """One retrieval-language configuration for the Google News `site:` route.

    Fields:
      code            : canonical language tag ('en'; 'ar'/'ru'/... in later waves).
      hl / gl / ceid  : Google News language/region query params.
      no_rss_domains  : publishers queried via `site:` at this hl.
      keyword_priority: ordered (native_term, canonical_en_concept) pairs. For
                        English the two are identical; foreign vocabularies (later
                        waves) map a native term to the English concept written
                        into matched_keywords. Used by the DEFAULT resolver only.
      cap             : max OR terms per query (Google truncates ~13 terms).
      translate       : foreign languages translate to English for display;
                        en/pt do not. False for English.
      resolve_keywords: hook that turns the live keyword set into the OR-block
                        terms for this language. English overrides it with
                        english_keywords() (intersect-the-live-set semantics,
                        byte-for-byte the historical behaviour). When None, the
                        generic builder falls back to the standalone, curated
                        native vocabulary (native terms of keyword_priority,
                        capped) — the divergence foreign languages will use.
    """

    code: str
    hl: str
    gl: str
    ceid: str
    no_rss_domains: tuple[str, ...]
    keyword_priority: tuple[tuple[str, str], ...]
    cap: int = 12
    translate: bool = True
    resolve_keywords: Callable[["LangConfig", list[str]], list[str]] | None = None


# -----------------------------------------------------------------------------
# Arabic (ar) — the Wave B1-d PILOT that proves the whole foreign-language
# pipeline (retrieve in-language -> match native -> translate-after-filter).
#
# VOCABULARY (native Arabic term -> canonical English concept). The canonical is
# what lands in matched_keywords, so a user whose /news-hunter feed is scoped to
# "oil" sees the Arabic oil story (§2.4). Canonicals are aligned to existing
# English keyword strings where one exists (oil/gas/diesel/gasoline/refinery/
# OPEC/crude/LNG/Brent/sanction/Petrobras) so the feed's keyword scope surfaces
# them; Middle-East-conflict concepts with no DB keyword (Hormuz/Aramco/Iran/
# tanker/Red Sea/Houthi/pipeline) carry a natural English concept token.
#
# RETRIEVAL vs MATCHING (the asymmetry that makes this cheap):
#   * RETRIEVAL uses only the first `cap` (12) native terms — the OR-block Google
#     gets. These are the broad, high-yield energy + conflict words.
#   * MATCHING uses ALL of them via the union built in pipeline.run_search (§2.3),
#     as SUBSTRINGS (never \b-exact: Arabic proclitics ال/و/ب/ل attach with no
#     word boundary, so \b-exact is structurally broken for Arabic — design §2.1).
# Measured on the runner-equivalent GNews `site:` route at hl=ar (2026-08-19):
# every registered domain returns 100 dated items with native energy/conflict
# titles (oil prices, Strait of Hormuz, Aramco, LNG projects, refineries, Houthi,
# Iran sanctions, oil tankers) — all correctly rendered by GoogleTranslator.
AR_KEYWORDS: tuple[tuple[str, str], ...] = (
    # --- retrieval slice (first 12, the OR-block Google receives) ---
    ("نفط", "oil"),          # oil (bare stem; substring of النفط / والنفط etc.)
    ("غاز", "gas"),          # gas
    ("أوبك", "OPEC"),        # OPEC
    ("أرامكو", "Aramco"),    # Saudi Aramco
    ("هرمز", "Hormuz"),      # (Strait of) Hormuz — the core conflict chokepoint
    ("إيران", "Iran"),       # Iran
    ("مصفاة", "refinery"),   # refinery
    ("ديزل", "diesel"),      # diesel
    ("بنزين", "gasoline"),   # gasoline / petrol
    ("عقوبات", "sanction"),  # sanctions (aligned to the English keyword "sanction")
    ("ناقلة", "tanker"),     # (oil) tanker
    ("الحوثي", "Houthi"),    # Houthi (Red Sea / Bab el-Mandeb attacks)
    # --- matching-only slice (beyond cap 12: enrich matched_keywords, not the
    #     retrieval OR-block) ---
    ("خام", "crude"),                # crude
    ("برنت", "Brent"),               # Brent
    ("غاز مسال", "LNG"),             # LNG (liquefied gas)
    ("الغاز المسال", "LNG"),         # LNG (with al- article)
    ("البحر الأحمر", "Red Sea"),     # Red Sea
    ("خط أنابيب", "pipeline"),       # pipeline
    ("أنبوب", "pipeline"),           # pipe / pipeline
    ("بترول", "oil"),                # petroleum -> oil
    ("حقل غاز", "gas"),              # gas field -> gas
    ("حقل نفط", "oil"),             # oil field -> oil
    ("بتروبراس", "Petrobras"),       # Petrobras (Arabic transliteration)
)

# Native Arabic publishers queried via GNews `site:` at hl=ar. Measured
# 2026-08-19: all three return 100 items of on-beat Arabic energy/conflict news.
#   * attaqa.net       — الطاقة, the first Arabic outlet specialised in ENERGY
#                        news (highest O&G precision: LNG projects, oil demand,
#                        oil fields, refineries).
#   * asharqbusiness.com — الشرق بلومبرغ, the Bloomberg-Arabic business/markets
#                        desk (oil prices, refinery licences, Libya oil, Hormuz).
#   * alarabiya.net    — Al Arabiya, broad Gulf news carrying the Middle-East
#                        oil-geopolitics beat (Hormuz, Iran oil exports, Houthi,
#                        sanctions, tankers).
# Bodies of GNews items are unreachable, so these land TITLE-ONLY (empty snippet)
# — exactly the shape translate-after-filter handles best. GNews supplies the
# date, so no RSS/date recovery is needed.
ARABIC_NO_RSS_DOMAINS: tuple[str, ...] = (
    "attaqa.net",
    "asharqbusiness.com",
    "alarabiya.net",
)


# -----------------------------------------------------------------------------
# Russian (ru) — Wave B2a. Config-only, same shape as the Arabic pilot.
#
# VOCABULARY (native Russian term -> canonical English concept). Canonicals are
# aligned to existing English keyword strings where one exists (oil/gas/diesel/
# gasoline/refinery/OPEC/crude/LNG/Brent/sanction) so a user scoped to "oil"/"gas"
# sees the Russian story (§2.4); concepts with no DB keyword (Hormuz/Iran/tanker/
# pipeline/Gazprom/Rosneft/Lukoil/Urals/production) carry a natural English token.
#
# RUSSIAN IS HIGHLY INFLECTED, so the native terms are STEMS, matched as
# SUBSTRINGS (never \b-exact — design §2.1). A bare stem catches every case: нефт
# is a substring of нефть/нефти/нефтяной/нефтепровод; санкц of санкции/санкций/
# санкционный; добыч of добыча/добычи/добычу; дизел of дизель/дизельный. That is
# the whole reason substring beats \b-exact here (a boundary would fall inside the
# declension ending and miss it).
#
# THE ONE COLLISION, and why the domains below are ENERGY-ONLY. The gas stem
# `газ` (essential — цены на газ, экспорт газа) is, case-folded, a substring of
# the Gaza declensions Газа/Газе/Газы/Газу (Russian for the Gaza Strip), which is
# front-page news. Measured 2026-08-19 on the GNews `site:` route: on GENERAL
# outlets `газ` tagged 6-15 Gaza-war headlines per 7-day window as "gas" (rbc.ru
# 7, tass.ru 9, ria.ru 15, kommersant.ru 6) — a real off-beat collision of the
# `crude steel` class. But `газ` is Cyrillic, so it can ONLY ever match text that
# was retrieved from a Russian-language domain; the matching union never sees it
# in PT/EN/AR text. Retrieving exclusively from ENERGY publishers (where Gaza is
# not covered) therefore neutralises it end to end: the same measurement found
# ZERO Gaza matches on neftegaz.ru / oilcapital.ru / eprussia.ru. So `газ` is kept
# at full recall and the collision is closed by source curation, not by crippling
# the stem. Do NOT add a general-news Russian domain here without re-checking this.
#
# RETRIEVAL vs MATCHING: retrieval uses the first `cap` (12) broad energy/conflict
# stems (the OR-block Google gets); MATCHING uses all of them via the union in
# pipeline.run_search. `провод` (wire/conduct — проводить = "to carry out") is
# DELIBERATELY NOT a pipeline stem; the safe forms нефтепровод/газопровод are.
RU_KEYWORDS: tuple[tuple[str, str], ...] = (
    # --- retrieval slice (first 12, the OR-block Google receives) ---
    ("нефт", "oil"),          # oil (stem: нефть/нефти/нефтяной/нефтепровод...)
    ("газ", "gas"),           # gas (stem; Gaza collision closed by energy-only domains)
    ("ОПЕК", "OPEC"),         # OPEC
    ("Газпром", "Gazprom"),   # Gazprom
    ("Роснефт", "Rosneft"),   # Rosneft (leftmost-longest wins over нефт inside it, like gasolina>gas)
    ("Лукойл", "Lukoil"),     # Lukoil
    ("санкц", "sanction"),    # sanctions (stem: санкции/санкций/санкционный)
    ("дизел", "diesel"),      # diesel (stem: дизель/дизельный)
    ("бензин", "gasoline"),   # gasoline / petrol
    ("НПЗ", "refinery"),      # refinery (нефтеперерабатывающий завод)
    ("танкер", "tanker"),     # (oil) tanker
    ("Ормуз", "Hormuz"),      # (Strait of) Hormuz
    # --- matching-only slice (beyond cap 12: enrich matched_keywords) ---
    ("СПГ", "LNG"),               # LNG (сжиженный природный газ)
    ("нефтепровод", "pipeline"),  # oil pipeline (safe compound; not the провод stem)
    ("газопровод", "pipeline"),   # gas pipeline (safe compound)
    ("Брент", "Brent"),           # Brent (Cyrillic; Latin "Brent" left out to keep the vocab non-Latin)
    ("Юралс", "Urals"),           # Urals crude grade (Cyrillic form only, same reason)
    ("добыч", "production"),      # production / extraction (stem: добыча/добычи/добычу)
    ("Иран", "Iran"),             # Iran
    ("эмбарго", "sanction"),      # embargo -> sanction
    ("сырая нефт", "crude"),      # crude oil (nominative phrase; other cases fall to нефт->oil)
)

# Native Russian ENERGY publishers queried via GNews `site:` at hl=ru. Measured
# 2026-08-19: each returns 100 dated items whose titles are on-beat Russian
# energy/markets/geopolitics news, all correctly rendered by GoogleTranslator.
# Deliberately energy-only (see the Gaza note above): general outlets (rbc/tass/
# ria/kommersant/vedomosti/interfax) all carry the Gaza-war beat and were rejected.
#   * neftegaz.ru   — Neftegaz.RU, an O&G industry portal (markets, logistics,
#                     geopolitics; 59 title-passes/100 in the measured window).
#   * oilcapital.ru — Нефть и Капитал (Oil & Capital), pure O&G markets/trade
#                     (81/100 — the densest of the three).
#   * eprussia.ru   — Энергетика и промышленность России (Energy & Industry of
#                     Russia), energy-industry/trade wire (16/100 — thinner but
#                     Gaza-clean and adds resilience against a dropped GNews query).
# Bodies of GNews items are unreachable, so these land TITLE-ONLY (empty snippet)
# — the shape translate-after-filter handles best. GNews supplies the date.
RUSSIAN_NO_RSS_DOMAINS: tuple[str, ...] = (
    "neftegaz.ru",
    "oilcapital.ru",
    "eprussia.ru",
)


# -----------------------------------------------------------------------------
# Chinese (zh, Simplified) — Wave B2a. Config-only, same shape as the pilot.
#
# VOCABULARY (native Chinese term -> canonical English concept). Canonicals align
# to existing English keywords where one exists (oil/gas/crude/diesel/gasoline/
# refinery/OPEC/sanction/tanker/LNG/Brent); the rest carry a natural English token
# (Hormuz/Iran/PetroChina/Sinopec/Aramco/Houthi/Red Sea/pipeline/production).
#
# CHINESE HAS NO WORD BOUNDARIES and no inflection, so terms are whole words
# matched as SUBSTRINGS (\b-exact is meaningless for a script with no spaces —
# design §2.1). No stemming is needed: 石油 ("oil") is a substring of 石油公司
# ("oil company") directly.
#
# OFF-BEAT COLLISION AVOIDED (the `crude steel` lesson): the bare 阿美 ("Aramco",
# clipped from 沙特阿美) is also a common personal name and a substring of 阿美族
# (the Amis people), so it is DROPPED in favour of the full 沙特阿美 (Saudi
# Aramco), which is unambiguous. The generic terms 管道 ("pipeline") and 产量
# ("output/production") are matching-only and, measured 2026-08-19 on the GNews
# `site:` route (yicai/finance.sina/jiemian), produced ZERO stray matches — the
# GNews `site:` AND-block already scopes retrieval to energy pages, so they only
# ever see energy text.
#
# NESTED TERMS resolve by LEFTMOST-LONGEST (the matcher sorts its alternation
# long-first and findall is non-overlapping), exactly like the PT gasolina>gas
# rule: 液化天然气 ("LNG") wins over the 天然气 ("gas") inside it, and 中石油
# ("PetroChina") over 石油 ("oil"). So a headline is tagged with the most specific
# concept present, and the shorter term co-fires ONLY when it also appears in a
# separate position (e.g. 天然气 … 液化天然气 -> gas + LNG).
#
# RETRIEVAL vs MATCHING: retrieval uses the first `cap` (12) broad energy/conflict
# terms; MATCHING uses all of them via the union in pipeline.run_search.
ZH_KEYWORDS: tuple[tuple[str, str], ...] = (
    # --- retrieval slice (first 12, the OR-block Google receives) ---
    ("石油", "oil"),          # oil / petroleum
    ("天然气", "gas"),        # natural gas
    ("原油", "crude"),        # crude oil
    ("柴油", "diesel"),       # diesel
    ("汽油", "gasoline"),     # gasoline / petrol
    ("炼油", "refinery"),     # oil refining / refinery
    ("欧佩克", "OPEC"),       # OPEC
    ("制裁", "sanction"),     # sanction(s)
    ("油轮", "tanker"),       # oil tanker
    ("霍尔木兹", "Hormuz"),   # (Strait of) Hormuz
    ("伊朗", "Iran"),         # Iran
    ("中石油", "PetroChina"), # PetroChina (中国石油; leftmost-longest wins over 石油 inside it)
    # --- matching-only slice (beyond cap 12: enrich matched_keywords) ---
    ("中石化", "Sinopec"),        # Sinopec (中国石化)
    ("液化天然气", "LNG"),        # LNG (leftmost-longest wins over 天然气 inside it)
    ("管道", "pipeline"),         # pipeline
    ("布伦特", "Brent"),          # Brent
    ("沙特阿美", "Aramco"),       # Saudi Aramco (full form; bare 阿美 collides — see note)
    ("胡塞", "Houthi"),           # Houthi (胡塞武装)
    ("红海", "Red Sea"),          # Red Sea (Bab el-Mandeb shipping)
    ("产量", "production"),       # output / production
)

# Native Chinese ENERGY/finance publishers queried via GNews `site:` at
# hl=zh-CN. Measured 2026-08-19: each returns 100 dated items whose titles are
# on-beat Chinese energy/markets news, all correctly rendered by GoogleTranslator.
#   * yicai.com            — 第一财经 (Yicai / China Business Network), top-tier
#                            business media (97 title-passes/100; resolves to
#                            www.yicai.com).
#   * finance.sina.com.cn  — 新浪财经 (Sina Finance), major finance portal (100/100).
#   * jiemian.com          — 界面新闻 (Jiemian News), quality digital business news
#                            (95/100; resolves to www.jiemian.com, some m.jiemian.com).
# Rejected (measured, recorded so a later wave doesn't re-test): cnenergynews.cn
# (GNews indexes 0 items), caixin.com and eastmoney.com (fragment across many
# subdomains — caifuhao.eastmoney.com is user-generated self-media). Bodies of
# GNews items are unreachable, so these land TITLE-ONLY. GNews supplies the date.
CHINESE_NO_RSS_DOMAINS: tuple[str, ...] = (
    "yicai.com",
    "finance.sina.com.cn",
    "jiemian.com",
)


# -----------------------------------------------------------------------------
# Hebrew (iw) — Wave B2b. Config-only, same shape as the pilot.
#
# CODE IS 'iw', NOT 'he'. Google's LEGACY code for Hebrew is iw everywhere that
# matters to us: GoogleTranslator(source='he') raises LanguageNotSupportedException
# (source='iw' works — verified), and hl=iw and hl=he BOTH return the same 100
# items, so iw is the single tag used end to end (LANGUAGES key, source_lang,
# TRANSLATOR_CODE['iw']='iw', hl=iw). Measured 2026-08-19.
#
# VOCABULARY (native Hebrew term -> canonical English concept). Canonicals align
# to existing English keywords where one exists (oil/gas/gasoline/diesel/refinery/
# sanction/tanker/crude/Brent/Iran/Yemen); Red Sea carries a natural English token.
#
# HEBREW HAS ATTACHED PROCLITICS (ה=the, ב=in, ל=to, מ=from, ו=and, ש=that), so
# terms are matched as SUBSTRINGS (never \b-exact — design §2.1): נפט is a
# substring of הנפט / בנפט / והנפט; גז of הגז / וגז / מהגז.
#
# COLLISIONS — measured on the site: route 2026-08-19, and the reason several
# obvious terms are NOT here:
#   * גז ("gas", essential — Israel's Leviathan/Tamar/NewMed/Isramco gas is THE
#     Israeli energy beat) is a substring of מגזר ("sector") and the idiom פול גז
#     ("full throttle"). Unlike the Russian газ⊂Gaza case, this does NOT close by
#     source curation — מגזר is core business vocabulary that appears on the very
#     energy/business desks we retrieve from (measured ~2/11 גז-matches on
#     globes/themarker/calcalist/bizportal). It is KEPT anyway: the canonical is
#     "gas" (an already-heavy existing keyword), the residue is a Hebrew-only
#     business headline occasionally tagged gas, and dropping גז would blind the
#     scanner to the entire Israeli gas sector (recall > a small, Hebrew-scoped,
#     visible noise). Documented rather than hidden.
#   * DROPPED for un-closable substring collisions: צינור ("pipe/pipeline") ⊂
#     drug-money "conduit" / burst water pipes (60% FP measured); the Houthi stem
#     חות ⊂ דוחות ("reports") / לקוחות ("customers") / כוחות ("forces") / שיחות
#     ("talks") — catastrophic, so Houthi is not carried (those refinery-attack
#     stories already match זיקוק/הורמוז); אופ״ק/אופק ("OPEC"/"horizon") collide
#     and OPEC is already matched by the Latin "OPEC" keyword; עיצומים is
#     ambiguous ("labour sanctions"/strikes) so only the loanword סנקציות is used;
#     דלק ("fuel"/Delek) ⊂ דלקת ("inflammation") / מדליק. None of these leaves a
#     coverage hole (Latin OPEC + נפט/גז/זיקוק/בנזין/סולר carry the beat).
# CLEAN (measured, ~0 FP on the business/energy domains): נפט, סולר, זיקוק,
# מכלית, הורמוז, איראן, בנזין, סנקציות (last two carry the same bounded semantic
# breadth as the English gasoline/sanction substrings, by design).
#
# RETRIEVAL vs MATCHING: retrieval uses the first cap (12) terms; MATCHING uses
# all of them via the union in pipeline.run_search. נפט גולמי ("crude oil") wins
# leftmost-longest over נפט ("oil") inside it, tagging the specific concept.
IW_KEYWORDS: tuple[tuple[str, str], ...] = (
    # --- retrieval slice (first 12, the OR-block Google receives) ---
    ("נפט", "oil"),          # oil (bare; substring of הנפט/בנפט/ונפט...)
    ("גז", "gas"),           # gas (kept at full recall; מגזר collision documented above)
    ("איראן", "Iran"),       # Iran (highest-yield geopolitics anchor)
    ("הורמוז", "Hormuz"),    # (Strait of) Hormuz
    ("בנזין", "gasoline"),   # gasoline / petrol
    ("סולר", "diesel"),      # diesel
    ("זיקוק", "refinery"),   # refining / refinery (בית זיקוק = refinery)
    ("מכלית", "tanker"),     # (oil) tanker
    ("סנקציות", "sanction"), # sanctions (loanword; aligned to keyword "sanction")
    ("ברנט", "Brent"),       # Brent (Hebrew spelling; Latin "Brent" left out to keep vocab non-Latin)
    ("הים האדום", "Red Sea"),# Red Sea (Bab el-Mandeb / Houthi shipping)
    ("תימן", "Yemen"),       # Yemen (aligned to keyword "Yemen")
    # --- matching-only slice (beyond cap 12: enrich matched_keywords) ---
    ("נפט גולמי", "crude"),  # crude oil (leftmost-longest wins over נפט->oil inside it)
)

# Native Hebrew ENERGY/business publishers queried via GNews `site:` at hl=iw.
# Measured 2026-08-19: each returns ~100 dated items whose titles are genuine
# Hebrew energy/markets news, all correctly rendered by GoogleTranslator(source=iw).
#   * globes.co.il    — Globes, Israel's main business/financial daily (98/100
#                       genuine-Hebrew title-passes in the measured window).
#   * themarker.com   — TheMarker (Haaretz group) business daily (100/100; carried
#                       the Ashdod refinery + oil&gas + earnings stories).
#   * calcalist.co.il — Calcalist, the other major financial daily (46/47).
# Rejected (measured, recorded so a later wave doesn't re-test): en.globes.co.il /
# biz.walla.co.il / energynews.co.il (GNews indexes 0); ynet.co.il / mako.co.il
# are 100/100 but GENERAL news (more war/politics than O&G) — bizportal.co.il
# (98/100, very energy-dense) is a strong measured alternative if a 4th is wanted.
# Bodies of GNews items are unreachable, so these land TITLE-ONLY. GNews supplies
# the date.
HEBREW_NO_RSS_DOMAINS: tuple[str, ...] = (
    "globes.co.il",
    "themarker.com",
    "calcalist.co.il",
)


# -----------------------------------------------------------------------------
# Spanish (es) — Wave B2b. Config-only, same shape as the pilot, but the FIRST
# Latin-script foreign language — so the union tests these terms against PT/EN
# text too (unlike the Arabic/Russian/Chinese/Hebrew scripts, which can only ever
# match text retrieved in-language). That makes collision control the whole job.
#
# VOCABULARY (native Spanish term -> canonical English concept). Canonicals align
# to existing English keywords where one exists (crude/oil/refinery/diesel/
# pipeline/sanction/tanker); the LatAm oil majors and Vaca Muerta carry their own
# (ASCII) proper-noun token.
#
# THE RULE FOR A LATIN LANGUAGE — only terms that are (a) Spanish-SPECIFIC (the
# shared petróleo/gas/gasolina/OPEP/Brent/WTI/Ormuz already match ES text via the
# existing keyword set, so a Spanish article that only names those needs nothing
# here), and (b) NOT a substring of any common PT/EN word. Both were MEASURED, not
# assumed: every concept term below scored ZERO hits across 40,309 native (pt/en)
# titles in news_articles on 2026-08-19 (crudo/petrolero/petrolera/gasóleo/diésel/
# oleoducto/gasoducto/yacimiento/sanciones/hidrocarburos = 0 each; refinería = 1).
# The five proper nouns DO match native titles — Pemex 62, Ecopetrol 93, PDVSA 5,
# YPF 9, Vaca Muerta 14 — but those are CORRECT tags (a PT/EN article naming
# Ecopetrol should carry "Ecopetrol"), not false positives: they are unambiguous
# entity names, so the canonical rewrite simply enriches those rows.
#
# NOT ADDED, on purpose: refinería is retrieval-useful but its MATCH is already
# covered by the existing substring keyword `refin` (it is kept here so ES
# refinery articles are RETRIEVED and get the cleaner "refinery" canonical);
# OPEP/Ormuz/gas/diesel/Brent are omitted because they are already live keywords.
# petrolero -> oil (not tanker): "petrolero" is far more often adjectival
# (sector/mercado/empresa petrolera = oil-) than the noun "oil tanker", and even
# the tanker sense is oil-related, so "oil" is never badly wrong; the explicit
# "buque petrolero" -> tanker wins leftmost-longest for the vessel sense.
#
# RETRIEVAL vs MATCHING: retrieval uses the first cap (12) terms; MATCHING uses
# all of them via the union in pipeline.run_search. "buque petrolero" wins
# leftmost-longest over "petrolero" inside it.
ES_KEYWORDS: tuple[tuple[str, str], ...] = (
    # --- retrieval slice (first 12, the OR-block Google receives) ---
    ("crudo", "crude"),           # crude oil (ES; distinct from the EN "crude" string)
    ("petrolero", "oil"),         # oil- (adjective/sector) — see note; buque petrolero->tanker
    ("petrolera", "oil"),         # oil company / oil- (feminine)
    ("refinería", "refinery"),    # refinery (retrieval; match also covered by `refin`)
    ("hidrocarburos", "oil"),     # hydrocarbons (PT is hidrocarbonetos — ES-specific)
    ("yacimiento", "oil"),        # oil/gas field / deposit
    ("oleoducto", "pipeline"),    # oil pipeline (PT is oleoduto — the 'c' makes it ES-specific)
    ("gasóleo", "diesel"),        # diesel (Spain/standard; not matched by EN "diesel")
    ("sanciones", "sanction"),    # sanctions (ES; not a substring of EN "sanction")
    ("Pemex", "Pemex"),           # Petróleos Mexicanos
    ("Ecopetrol", "Ecopetrol"),   # Colombia's oil major
    ("PDVSA", "PDVSA"),           # Petróleos de Venezuela
    # --- matching-only slice (beyond cap 12: enrich matched_keywords) ---
    ("diésel", "diesel"),         # diesel (accented ES form the EN "diesel" keyword misses)
    ("gasoducto", "pipeline"),    # gas pipeline (PT is gasoduto)
    ("YPF", "YPF"),               # Argentina's oil major
    ("Vaca Muerta", "Vaca Muerta"), # Argentina's shale play
    ("buque petrolero", "tanker"), # oil tanker (wins leftmost-longest over petrolero->oil)
)

# Native Spanish ENERGY/business publishers queried via GNews `site:` at
# hl=es-419. Measured 2026-08-19: each returns 100 dated items whose titles are
# on-beat Spanish energy/markets news, all correctly rendered by GoogleTranslator.
#   * eleconomista.com.mx — El Economista (Mexico), business daily (Pemex,
#                           fracking, gasolineras, Ecopetrol gas Colombia).
#   * ambito.com          — Ámbito Financiero (Argentina), business daily
#                           (petrolero in Ormuz, Brent record, Russian tanker).
#   * portafolio.co       — Portafolio (Colombia), business daily (Ecopetrol,
#                           Brent > US$91, Ecopetrol takes control of Brava/Brazil).
# Rejected (measured, recorded so a later wave doesn't re-test): lapoliticaonline
# .com returns 100 but ALL off-beat politics; energiaadebate.com is pure energy
# but thin (5 items). eleconomista.es (Spain, 100/100 on-beat) is a strong
# measured alternative if a 4th is wanted. Bodies of GNews items are unreachable,
# so these land TITLE-ONLY. GNews supplies the date.
SPANISH_NO_RSS_DOMAINS: tuple[str, ...] = (
    "eleconomista.com.mx",
    "ambito.com",
    "portafolio.co",
)


LANGUAGES: dict[str, LangConfig] = {
    # English is now a member of the registry; the old _en path is a special case
    # of it. Its resolver is english_keywords() (intersect the live DB set), so
    # its emitted queries are identical to the pre-registry implementation.
    "en": LangConfig(
        code="en",
        hl="en-US",
        gl="US",
        ceid="US:en",
        no_rss_domains=tuple(ENGLISH_NO_RSS_DOMAINS),
        keyword_priority=tuple((k, k) for k in ENGLISH_KEYWORD_PRIORITY),
        cap=ENGLISH_KEYWORD_CAP,
        translate=False,
        resolve_keywords=lambda cfg, live: english_keywords(live),
    ),
    # Arabic — Wave B1-d pilot. translate=True (foreign -> English for display),
    # standalone curated vocabulary (default resolver: no intersection with the
    # live DB set, so users never type Arabic into the keyword box and the PT
    # OR-block is never diluted). hl/gl/ceid measured at EG:ar (100 items/domain).
    "ar": LangConfig(
        code="ar",
        hl="ar",
        gl="EG",
        ceid="EG:ar",
        no_rss_domains=ARABIC_NO_RSS_DOMAINS,
        keyword_priority=AR_KEYWORDS,
        cap=12,
        translate=True,
    ),
    # Russian — Wave B2a. translate=True, standalone curated STEM vocabulary
    # (default resolver, no intersection with the live DB set). hl/gl/ceid measured
    # at RU:ru (100 items/domain). Energy-only sources (Gaza-collision note in
    # RU_KEYWORDS). TRANSLATOR_CODE['ru']='ru' (identity).
    "ru": LangConfig(
        code="ru",
        hl="ru",
        gl="RU",
        ceid="RU:ru",
        no_rss_domains=RUSSIAN_NO_RSS_DOMAINS,
        keyword_priority=RU_KEYWORDS,
        cap=12,
        translate=True,
    ),
    # Chinese (Simplified) — Wave B2a. translate=True, standalone curated
    # substring vocabulary. hl/gl/ceid measured at CN:zh-Hans (100 items/domain);
    # note ceid uses zh-Hans while hl is zh-CN (Google's own convention). The
    # TRANSLATOR_CODE['zh']='zh-CN' gotcha (GoogleTranslator rejects bare 'zh') is
    # already in translate.py.
    "zh": LangConfig(
        code="zh",
        hl="zh-CN",
        gl="CN",
        ceid="CN:zh-Hans",
        no_rss_domains=CHINESE_NO_RSS_DOMAINS,
        keyword_priority=ZH_KEYWORDS,
        cap=12,
        translate=True,
    ),
    # Hebrew — Wave B2b. translate=True, standalone curated substring vocabulary.
    # Key/code/hl/translator all 'iw' (Google's legacy Hebrew tag; source='he'
    # raises — see IW_KEYWORDS + translate.TRANSLATOR_CODE). hl/gl/ceid measured
    # at IL:iw (100 genuine-Hebrew items/domain). Business/energy-only sources.
    "iw": LangConfig(
        code="iw",
        hl="iw",
        gl="IL",
        ceid="IL:iw",
        no_rss_domains=HEBREW_NO_RSS_DOMAINS,
        keyword_priority=IW_KEYWORDS,
        cap=12,
        translate=True,
    ),
    # Spanish — Wave B2b. translate=True, standalone curated vocabulary. The FIRST
    # Latin-script foreign language: its terms are tested by the matching union
    # against PT/EN text too, so the vocab is restricted to ES-specific,
    # collision-free terms (measured 0 hits on 40,309 native titles — see
    # ES_KEYWORDS). hl/gl/ceid measured at US:es-419 (100 items/domain).
    "es": LangConfig(
        code="es",
        hl="es-419",
        gl="US",
        ceid="US:es-419",
        no_rss_domains=SPANISH_NO_RSS_DOMAINS,
        keyword_priority=ES_KEYWORDS,
        cap=12,
        translate=True,
    ),
    # fa (Persian) was MEASURED NOT VIABLE via this mechanism on 2026-08-19 and is
    # deliberately NOT registered: Google News does not index Iranian domains
    # (~30 tested — shana/tasnim/mehr/isna/donya-e-eqtesad/... all 0-2 items/30d),
    # and the Persian-diaspora outlets it does index return their ENGLISH edition
    # (iranintl.com) or empty titles (dw.com/fa). Every path yields 0, off-beat,
    # or non-Persian, so a fa config would be a silent-zero source (and would
    # mistag the occasional English/Arabic leak as fa). Re-open only if a Persian
    # domain becomes GNews-indexed (e.g. a residential/geo-appropriate runner).
}


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


def google_news_site_queries_lang(
    cfg: LangConfig, domains: list[str], keywords: list[str], hours: int
) -> list[str]:
    """One Google News `site:` query per domain, scoped to a LangConfig.

    Generalizes google_news_site_queries_en to any language: it swaps in the
    config's hl/gl/ceid and its keyword resolver. Everything else — the
    `when:`-before-OR shape (do NOT reorder, see the note above _when_clause) and
    the per-domain iteration — is identical across languages.

    The resolver decides the OR-block terms. English supplies english_keywords()
    (intersect the live DB set); a config with no resolver falls back to the
    standalone curated native vocabulary (the divergence foreign languages use in
    later waves). With LANGUAGES["en"] this reproduces the historical en URLs
    byte-for-byte — pinned by tests/test_multilingual_en_frozen.py.
    """
    when = _when_clause(hours)
    resolve = cfg.resolve_keywords or (
        lambda c, live: [nat for nat, _ in c.keyword_priority][: c.cap]
    )
    kw_or = _kw_or(resolve(cfg, keywords))
    out: list[str] = []
    for domain in domains:
        q = quote_plus(f"site:{domain} when:{when} ({kw_or})")
        out.append(
            f"https://news.google.com/rss/search?q={q}&hl={cfg.hl}&gl={cfg.gl}&ceid={cfg.ceid}"
        )
    return out


def google_news_site_queries_en(domains: list[str], keywords: list[str], hours: int) -> list[str]:
    """English Google News `site:` queries (hl=en-US) — now a shim over the
    language-generic builder driven by LANGUAGES["en"].

    Kept as a named function so existing callers (fetcher, tests) are untouched.
    LANGUAGES["en"] reproduces every input to the URL — hl=en-US&gl=US&ceid=US:en,
    the same english_keywords() subset in the same priority order, the same
    `when:`-before-OR shape — so the output is byte-for-byte identical to the
    previous direct implementation (see ENGLISH_KEYWORD_PRIORITY).
    """
    return google_news_site_queries_lang(LANGUAGES["en"], domains, keywords, hours)


# -----------------------------------------------------------------------------
# Google News burst budget — stateless cohort rotation.
#
# THE CONSTRAINT: news.google.com rate-limits a BURST of `site:` queries from one
# IP and silently drops the tail — no error, no 429, just fewer feeds answered.
# That is what stranded the Arabic pilot at 0 rows while measure_source, firing 3
# isolated queries from the SAME runner IP, returned 58-100 fresh items per
# domain (see the note above the priority block in fetcher.iter_collect). The
# per-scan burst today is 15 foreign + 16 PT + 34 EN = 65 `site:` queries. The
# international programme pushes the EN list toward ~120 domains, which would
# roughly double the burst and start dropping its tail — and since the drop is
# silent and position-dependent, the casualties would be whatever sits at the
# end of the list, not the weakest sources.
#
# THE FIX: do not query every EN domain on every scan. Partition the list into
# `cohorts = ceil(len(domains) / per_scan)` contiguous, balanced blocks and query
# exactly ONE block per scan, chosen from a TIME BUCKET rather than from stored
# state (the scanner is a stateless `--once` job fired by cron-job.org every
# 5 min; there is nowhere to keep a rotation pointer, and a crashed scan must not
# skip a cohort forever):
#
#     index = floor(unix_seconds / GNEWS_COHORT_BUCKET_SECONDS) % cohorts
#
# INVARIANT: consecutive 5-minute scans walk consecutive buckets, so every domain
# is queried at least once every `cohorts * 5` minutes — with ~120 EN domains
# that is 4 cohorts = 20 minutes, two orders of magnitude inside the 24h `when:`
# window each query carries (DEFAULT_WINDOW_HOURS / hours_override=24). Nothing
# is lost: a story published in the 20 minutes a domain sits out is still inside
# the window when its cohort comes round, and news_articles is keyed by url so
# re-seeing it costs nothing.
#
# WHAT IS NOT CAPPED: the FOREIGN languages (ar/ru/zh/iw/es). They are GNews-ONLY
# — a dropped query there is total data loss for that language, not a delayed
# one — so they keep being submitted FIRST and in full, on the freshest
# rate-limit budget. Capping EN is precisely what protects them.
#
# TODAY: len(ENGLISH_NO_RSS_DOMAINS) == 34 == EN_GNEWS_QUERIES_PER_SCAN, so
# cohorts == 1 and the emitted query list is BYTE-IDENTICAL to the uncapped one
# (pinned by tests/test_multilingual_en_frozen.py, untouched, plus the explicit
# cohorts==1 identity tests in tests/test_gnews_cohort.py). The mechanism only
# starts moving once a wave pushes the list past the budget.
# -----------------------------------------------------------------------------

# How many EN `site:` queries one scan may submit. Set to today's list length so
# landing the mechanism changed nothing; raising it re-widens the burst.
EN_GNEWS_QUERIES_PER_SCAN = 34
# Same for the PT list (NO_RSS_DOMAINS, 16 today -> cohorts == 1, unchanged).
PT_GNEWS_QUERIES_PER_SCAN = 16
# Bucket width. Matches the cron-job.org scan cadence: one bucket per scan, so
# consecutive scans land on consecutive cohorts.
GNEWS_COHORT_BUCKET_SECONDS = 300


def gnews_cohort_count(n_domains: int, per_scan: int) -> int:
    """How many scans it takes to cover `n_domains` at `per_scan` per scan."""
    if n_domains <= 0 or per_scan <= 0:
        return 1
    return max(1, -(-n_domains // per_scan))


def gnews_cohort_index(
    cohorts: int,
    *,
    now: float | None = None,
    bucket_seconds: int = GNEWS_COHORT_BUCKET_SECONDS,
) -> int:
    """Which cohort this scan owns — derived from the clock, never from state."""
    if cohorts <= 1:
        return 0
    ts = time.time() if now is None else now
    return int(ts // bucket_seconds) % cohorts


def gnews_cohort_slice(domains: list[str], per_scan: int, index: int) -> list[str]:
    """Cohort `index` of `domains`: a contiguous, order-preserving block.

    Blocks are balanced (sizes differ by at most one) so no scan ever exceeds
    `per_scan`, every cohort is non-empty, and concatenating the cohorts in index
    order reproduces `domains` exactly — which is what makes "every domain is
    queried once per `cohorts` scans" a partition property rather than a hope.
    """
    n = len(domains)
    cohorts = gnews_cohort_count(n, per_scan)
    if cohorts <= 1:
        return list(domains)
    i = index % cohorts
    base, rem = divmod(n, cohorts)
    start = i * base + min(i, rem)
    size = base + (1 if i < rem else 0)
    return list(domains[start:start + size])


def gnews_cohort(
    domains: list[str], per_scan: int, *, now: float | None = None
) -> tuple[list[str], int, int]:
    """(this scan's domains, 0-based cohort index, total cohorts)."""
    cohorts = gnews_cohort_count(len(domains), per_scan)
    index = gnews_cohort_index(cohorts, now=now)
    return gnews_cohort_slice(domains, per_scan, index), index, cohorts
