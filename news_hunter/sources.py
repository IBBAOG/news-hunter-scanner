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
