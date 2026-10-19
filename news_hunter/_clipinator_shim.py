"""Partes do clipinator.py necessarias para enriquecimento de artigos.

Em vez de depender do clipinator.py original (que tem CLI + geracao .eml +
cookies locais), extraimos apenas o que `enrich.py` e `fetcher._scrape_homepage`
precisam:

  - fetch_html(url, timeout)  : HTTP client com curl_cffi impersonation
  - EXTRACTORS                : dict dominio -> BS4 extractor
  - SOURCE_NAMES              : dict dominio -> nome legivel
  - _extract(html, domain)    : parse HTML -> (titulo, paragrafos)
  - clean_paragraphs(ps)      : limpa ruido/boilerplate

Sem cookies locais (`cookies/` nao existe no container) — sites paywalled
(Valor, Brasil Energia) caem no fallback natural: feedparser RSS + Google News
indexing. Acceptable trade-off para plug-and-play cloud-only.
"""
from __future__ import annotations

import json
import re
from typing import Callable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag
from curl_cffi import requests as cffi_requests


# =============================================================================
# Dominio -> nome legivel
# =============================================================================

SOURCE_NAMES: dict[str, str] = {
    # Arabic pilot sources (Wave B1-d). normalize_url strips www, so items land
    # under the apex; both forms registered defensively.
    "attaqa.net": "attaqa (الطاقة)",
    "www.attaqa.net": "attaqa (الطاقة)",
    "asharqbusiness.com": "Asharq Business",
    "www.asharqbusiness.com": "Asharq Business",
    "alarabiya.net": "Al Arabiya",
    "www.alarabiya.net": "Al Arabiya",
    # Russian energy sources (Wave B2a). GNews items resolve to the apex
    # (eprussia also serves www); both forms registered defensively.
    "neftegaz.ru": "Neftegaz.RU (Нефтегаз)",
    "www.neftegaz.ru": "Neftegaz.RU (Нефтегаз)",
    "oilcapital.ru": "Oil Capital (Нефть и Капитал)",
    "www.oilcapital.ru": "Oil Capital (Нефть и Капитал)",
    "eprussia.ru": "EPR (Энергетика и промышленность России)",
    "www.eprussia.ru": "EPR (Энергетика и промышленность России)",
    # Chinese energy/finance sources (Wave B2a). GNews items resolve to www for
    # yicai/jiemian (+ some m.jiemian.com) and to the finance subdomain for Sina.
    "yicai.com": "Yicai (第一财经)",
    "www.yicai.com": "Yicai (第一财经)",
    # Yicai serves a separate mobile host and GNews resolves to it for part of
    # the traffic, exactly as it does for m.jiemian.com below. Without the key
    # the outlet renders under its bare domain instead of its name.
    "m.yicai.com": "Yicai (第一财经)",
    "finance.sina.com.cn": "Sina Finance (新浪财经)",
    "jiemian.com": "Jiemian News (界面新闻)",
    "www.jiemian.com": "Jiemian News (界面新闻)",
    "m.jiemian.com": "Jiemian News (界面新闻)",
    # Hebrew business/energy sources (Wave B2b). GNews items resolve to the apex
    # or www; both forms registered defensively.
    "globes.co.il": "Globes (גלובס)",
    "www.globes.co.il": "Globes (גלובס)",
    "themarker.com": "TheMarker (דה מרקר)",
    "www.themarker.com": "TheMarker (דה מרקר)",
    "calcalist.co.il": "Calcalist (כלכליסט)",
    "www.calcalist.co.il": "Calcalist (כלכליסט)",
    # Spanish (LatAm) business/energy sources (Wave B2b). GNews items resolve to
    # the apex or www; both forms registered defensively.
    "eleconomista.com.mx": "El Economista (MX)",
    "www.eleconomista.com.mx": "El Economista (MX)",
    "ambito.com": "Ámbito",
    "www.ambito.com": "Ámbito",
    "portafolio.co": "Portafolio",
    "www.portafolio.co": "Portafolio",
    "valor.globo.com": "Valor Econômico",
    "www.estadao.com.br": "Estadão",
    "estadao.com.br": "Estadão",
    "www1.folha.uol.com.br": "Folha de S. Paulo",
    "folha.uol.com.br": "Folha de S. Paulo",
    "brasilenergia.com.br": "Brasil Energia",
    "www.brasilenergia.com.br": "Brasil Energia",
    "www.metropoles.com": "Metrópoles",
    "metropoles.com": "Metrópoles",
    "www.poder360.com.br": "Poder360",
    "poder360.com.br": "Poder360",
    "www.infomoney.com.br": "InfoMoney",
    "infomoney.com.br": "InfoMoney",
    "www.bloomberglinea.com.br": "Bloomberg Línea",
    "bloomberglinea.com.br": "Bloomberg Línea",
    "noticias.r7.com": "R7",
    "g1.globo.com": "G1",
    "oglobo.globo.com": "O Globo",
    "agencia.petrobras.com.br": "Agência Petrobras",
    "agenciainfra.com": "Agência iNFRA",
    "www.agenciainfra.com": "Agência iNFRA",
    "jota.info": "JOTA",
    "www.jota.info": "JOTA",
    "braziljournal.com": "Brazil Journal",
    "www.braziljournal.com": "Brazil Journal",
    "eixos.com.br": "eixos",
    "www.eixos.com.br": "eixos",
    "monitormercantil.com.br": "Monitor Mercantil",
    "www.monitormercantil.com.br": "Monitor Mercantil",
    "timesbrasil.com.br": "Times Brasil",
    "www.timesbrasil.com.br": "Times Brasil",
    "visaoagro.com.br": "Visão Agro",
    "www.visaoagro.com.br": "Visão Agro",
    "www.theagribiz.com": "Agribiz",
    "theagribiz.com": "Agribiz",
    "aovivo.folha.uol.com.br": "Folha de S. Paulo",
    "estradao.estadao.com.br": "Estradão",
    "pipelinevalor.globo.com": "Pipeline (Valor)",
    "globorural.globo.com": "Globo Rural",
    "cbn.globo.com": "CBN",
    "www.cnnbrasil.com.br": "CNN Brasil",
    "cnnbrasil.com.br": "CNN Brasil",
    "veja.abril.com.br": "Veja",
    "investnews.com.br": "InvestNews",
    "www.investnews.com.br": "InvestNews",
    "neofeed.com.br": "NeoFeed",
    "www.neofeed.com.br": "NeoFeed",
    "www.cnbc.com": "CNBC",
    "exame.com": "Exame",
    "www.exame.com": "Exame",
    "istoedinheiro.com.br": "IstoÉ Dinheiro",
    "www.istoedinheiro.com.br": "IstoÉ Dinheiro",
    "www.brasil247.com": "Brasil 247",
    "brasil247.com": "Brasil 247",
    "observatorio.firjan.com.br": "Observatório Firjan",
    "megawhat.uol.com.br": "MegaWhat",
    "www.reuters.com": "Reuters",
    "reuters.com": "Reuters",
    "br.investing.com": "Investing.com",
    "www.gazetadopovo.com.br": "Gazeta do Povo",
    "gazetadopovo.com.br": "Gazeta do Povo",
    # Apex first and foremost: the /rss/all item links are apex-form, so the
    # apex key is the one that actually resolves at display time. www is here
    # only for completeness (the feed URL itself lives on www).
    "themoscowtimes.com": "The Moscow Times",
    "www.themoscowtimes.com": "The Moscow Times",
    "www.correiobraziliense.com.br": "Correio Braziliense",
    "correiobraziliense.com.br": "Correio Braziliense",
    "www.correiodopovo.com.br": "Correio do Povo",
    "correiodopovo.com.br": "Correio do Povo",
    "www.jornaldocomercio.com": "Jornal do Comércio",
    "jornaldocomercio.com": "Jornal do Comércio",
    # Apex first: normalize_url strips "www.", so the key the enricher looks up
    # is the apex form.
    "itatiaia.com.br": "Rádio Itatiaia",
    "www.itatiaia.com.br": "Rádio Itatiaia",
    "www.atribuna.com.br": "A Tribuna",
    "atribuna.com.br": "A Tribuna",
    "veronoticias.com": "Vero Notícias",
    "www.veronoticias.com": "Vero Notícias",
    "diariodopoder.com.br": "Diário do Poder",
    "www.diariodopoder.com.br": "Diário do Poder",
    "www.conjur.com.br": "Conjur",
    "conjur.com.br": "Conjur",
    "www.argusmedia.com": "Argus Media",
    "argusmedia.com": "Argus Media",
    "operamundi.uol.com.br": "Opera Mundi",
    "claudiodantas.com.br": "Cláudio Dantas",
    "www.claudiodantas.com.br": "Cláudio Dantas",
    "br.tradingview.com": "TradingView",
    # Apex twins of two www.-only registrations, added 2026-09-14 with the
    # Wave 5 pass: both outlets are registered apex-side in sources.py too (the
    # GNews block lists the www. host, but normalize_url hands the enricher the
    # apex), and `news_articles` already holds 120 rows under "cnn.com" that
    # rendered as the bare domain. NOT fixed in the same pass: "cnbc.com", the
    # third case, whose absence is what tests/test_extractor_domain_resolution
    # ::test_host_variants_resolve_to_a_registered_key[m.cnbc.com] asserts on.
    "theedgesingapore.com": "The Edge Singapore",
    "www.theedgesingapore.com": "The Edge Singapore",
    "cnn.com": "CNN",
    "www12.senado.leg.br": "Senado Federal",
    "www.camara.leg.br": "Câmara dos Deputados",
    "camara.leg.br": "Câmara dos Deputados",
    "edition.cnn.com": "CNN",
    "www.cnn.com": "CNN",
    "clickpetroleoegas.com.br": "Click Petróleo e Gás",
    "www.clickpetroleoegas.com.br": "Click Petróleo e Gás",
    "ineep.org.br": "INEEP",
    "www.ineep.org.br": "INEEP",
    "tconline.com.br": "TC Online",
    "www.tconline.com.br": "TC Online",
    "obastidor.com.br": "O Bastidor",
    "www.obastidor.com.br": "O Bastidor",
    "noticias.uol.com.br": "UOL",
    "economia.uol.com.br": "UOL Economia",
    "www.terra.com.br": "Terra",
    "terra.com.br": "Terra",
    "www.moneytimes.com.br": "Money Times",
    "moneytimes.com.br": "Money Times",
    "visnoinvest.com.br": "Visno Invest",
    "www.visnoinvest.com.br": "Visno Invest",
    # International oil & gas / shipping trade press (English) — Wave 1a, RSS.
    # Both apex and www keyed: RSS item links are normalize_url'd (www stripped),
    # so the resolved source_domain is always the apex — but www is kept for
    # completeness and for the GNews-resolved path, which does NOT strip www.
    "oilprice.com": "OilPrice",
    "www.oilprice.com": "OilPrice",
    "gcaptain.com": "gCaptain",
    "www.gcaptain.com": "gCaptain",
    "hellenicshippingnews.com": "Hellenic Shipping News",
    "www.hellenicshippingnews.com": "Hellenic Shipping News",
    "lngprime.com": "LNG Prime",
    "www.lngprime.com": "LNG Prime",
    "oedigital.com": "Offshore Engineer",
    "www.oedigital.com": "Offshore Engineer",
    "splash247.com": "Splash247",
    "www.splash247.com": "Splash247",
    "worldoil.com": "World Oil",
    "www.worldoil.com": "World Oil",
    # International oil & gas trade press covered via Google News (WAF-blocked
    # feeds) — Wave 1a. GNews resolves items to the www article netloc (NOT
    # normalize_url'd), so the www key is the one that renders the name; apex is
    # kept for completeness. Rigzone's ENGLISH_NO_RSS entry is the path-scoped
    # www.rigzone.com/news, but resolved articles are www.rigzone.com.
    "ogj.com": "Oil & Gas Journal",
    "www.ogj.com": "Oil & Gas Journal",
    "energyvoice.com": "Energy Voice",
    "www.energyvoice.com": "Energy Voice",
    "offshore-technology.com": "Offshore Technology",
    "www.offshore-technology.com": "Offshore Technology",
    "rigzone.com": "Rigzone",
    "www.rigzone.com": "Rigzone",
    # Premium / paywalled international wires covered via Google News — Wave 1b.
    # GNews resolves items to the www article netloc (NOT normalize_url'd), so
    # the www key is the one that renders the name; apex is kept for
    # completeness AND because source_name_for's www. fallback used to be
    # charset-buggy (lstrip("www.") strips leading w/. characters, not the
    # "www." prefix), so we never relied on it — both keys are exact matches.
    # The fallback was fixed to removeprefix on 2026-09-14; keying both forms
    # stays the rule anyway, since an exact key needs no fallback at all.
    "bloomberg.com": "Bloomberg",
    "www.bloomberg.com": "Bloomberg",
    "spglobal.com": "S&P Global Commodity Insights",
    "www.spglobal.com": "S&P Global Commodity Insights",
    "upstreamonline.com": "Upstream",
    "www.upstreamonline.com": "Upstream",
    "tradewindsnews.com": "TradeWinds",
    "www.tradewindsnews.com": "TradeWinds",
    "energyintel.com": "Energy Intelligence",
    "www.energyintel.com": "Energy Intelligence",
    "hartenergy.com": "Hart Energy",
    "www.hartenergy.com": "Hart Energy",
    # Russia / CIS + Middle East oil & gas press (English) -- Wave 2
    # (2026-08-18). All GNews-covered except The National (RSS,
    # business-scoped -- see RSS_FEEDS in sources.py). GNews resolves items
    # to the article's real netloc (NOT normalize_url'd), so the www /
    # subdomain key renders the name; apex kept for completeness. The
    # National's RSS links are www, which normalize_url strips to the apex.
    "intellinews.com": "bne IntelliNews",
    "www.intellinews.com": "bne IntelliNews",
    "tass.com": "TASS",
    "www.tass.com": "TASS",
    "interfax.com": "Interfax",
    "www.interfax.com": "Interfax",
    "mees.com": "MEES",
    "www.mees.com": "MEES",
    "arabnews.com": "Arab News",
    "www.arabnews.com": "Arab News",
    "thenationalnews.com": "The National",
    "www.thenationalnews.com": "The National",
    "shana.ir": "Shana",
    "www.shana.ir": "Shana",
    "en.shana.ir": "Shana",
    "gulfnews.com": "Gulf News",
    "www.gulfnews.com": "Gulf News",
    "aljazeera.com": "Al Jazeera",
    "www.aljazeera.com": "Al Jazeera",
    "zawya.com": "Zawya",
    "www.zawya.com": "Zawya",
    "iraqoilreport.com": "Iraq Oil Report",
    "www.iraqoilreport.com": "Iraq Oil Report",
    # US + Europe oil & gas trade press (English) -- Wave 3 (2026-08-18).
    # oilandgas360 / offshore-energy / naturalgasintel are RSS (see RSS_FEEDS);
    # eenews / montelnews / globalenergynetwork are GNews-covered (see
    # ENGLISH_NO_RSS_DOMAINS). RSS item links are normalize_url'd (www stripped)
    # so the resolved source_domain is the apex; GNews resolves to the article's
    # real netloc (www kept). Both keyed for completeness.
    "oilandgas360.com": "Oil & Gas 360",
    "www.oilandgas360.com": "Oil & Gas 360",
    "offshore-energy.biz": "Offshore Energy",
    "www.offshore-energy.biz": "Offshore Energy",
    "naturalgasintel.com": "Natural Gas Intelligence",
    "www.naturalgasintel.com": "Natural Gas Intelligence",
    # Wave 3 GNews-covered (see ENGLISH_NO_RSS_DOMAINS). GNews resolves items to
    # the www article netloc, so the www key renders the name; apex kept for
    # completeness.
    "eenews.net": "E&E News",
    "www.eenews.net": "E&E News",
    "montelnews.com": "Montel News",
    "www.montelnews.com": "Montel News",
    "globalenergynetwork.net": "Global Energy Network",
    "www.globalenergynetwork.net": "Global Energy Network",
    # India oil & gas / business press (English) -- Wave 4 (2026-08-18). ET
    # EnergyWorld / Mint / BusinessLine are RSS (see RSS_FEEDS); Moneycontrol is
    # GNews-covered (see ENGLISH_NO_RSS_DOMAINS). RSS item links are
    # normalize_url'd (www stripped) so the resolved source_domain is the apex
    # (or the energy subdomain for ET, which has no www); GNews resolves to the
    # article's real netloc (www kept). Both keyed for completeness.
    "energy.economictimes.indiatimes.com": "ET EnergyWorld",
    "livemint.com": "Mint",
    "www.livemint.com": "Mint",
    "thehindubusinessline.com": "BusinessLine",
    "www.thehindubusinessline.com": "BusinessLine",
    "moneycontrol.com": "Moneycontrol",
    "www.moneycontrol.com": "Moneycontrol",
    # China state / regional wires (English) -- Wave 4, all GNews-covered (see
    # ENGLISH_NO_RSS_DOMAINS). GNews resolves items to the article's real netloc
    # (www / subdomain kept); apex kept for completeness. Xinhua's English site
    # is the english.news.cn subdomain (no www).
    "english.news.cn": "Xinhua",
    "chinadaily.com.cn": "China Daily",
    "www.chinadaily.com.cn": "China Daily",
    "globaltimes.cn": "Global Times",
    "www.globaltimes.cn": "Global Times",
    "scmp.com": "South China Morning Post",
    "www.scmp.com": "South China Morning Post",
    # =========================================================================
    # --- Wave 5 (2026-09-14) ---
    # The 112 English-language outlets of the 2026-09 international expansion
    # (README "International coverage", tables 5A-5E) shipped with no entry
    # here and none in EXTRACTORS. That costs three things at once: the
    # dashboard renders the bare domain instead of the outlet name,
    # _SITE_SUFFIX_PATTERNS (built from SOURCE_NAMES.values()) cannot strip the
    # " | LNG Industry" style suffix off a title, and the lede-rescue / snippet
    # backfill body fetch skips the outlet because it gates on the extractor
    # registry - the same silent fall-through the 30 foreign-language domains
    # took until 2026-08-18.
    #
    # Keyed per host form the scanner can actually produce: the apex (RSS item
    # links are normalize_url'd, so www is stripped), the www. form (GNews
    # resolves to the article's real netloc and does NOT strip it), the
    # registered subdomain where the outlet has no apex English edition, and
    # the host behind a path-scoped ENGLISH_NO_RSS_DOMAINS entry
    # (marketwatch.com/story -> marketwatch.com, aa.com.tr/en -> aa.com.tr,
    # tradearabia.com/news -> tradearabia.com, www3.nhk.or.jp/nhkworld ->
    # www3.nhk.or.jp). Hosts marked MEASURED were read off `news_articles`
    # after the merge; the rest of the second-host aliases (bbc.co.uk,
    # abcnews.go.com, markets.businessinsider.com, thetimes.co.uk,
    # mercopress.com) are the outlet's other live host, registered so the name
    # survives whichever one Google hands back.
    #
    # Display name = the README "Outlet" column minus the surface/edition
    # parenthesis, one name per outlet so a source filter does not split an
    # outlet in two ("Investing.com (EN edition)" -> "Investing.com", which is
    # what br.investing.com already renders; "Al Arabiya English" -> the "Al
    # Arabiya" of the Arabic keys above). The country survives only where it
    # disambiguates: "Business Day (South Africa)" against "BusinessDay
    # Nigeria".
    # --- Wave 5A: Global wires & US mainstream ---
    "apnews.com": "Associated Press",
    "www.apnews.com": "Associated Press",
    "ft.com": "Financial Times",
    "www.ft.com": "Financial Times",
    "wsj.com": "The Wall Street Journal",
    "www.wsj.com": "The Wall Street Journal",
    "nytimes.com": "The New York Times",
    "www.nytimes.com": "The New York Times",
    "washingtonpost.com": "The Washington Post",
    "www.washingtonpost.com": "The Washington Post",
    "economist.com": "The Economist",
    "www.economist.com": "The Economist",
    # bbc.co.uk serves the same newsroom
    "bbc.com": "BBC News",
    "www.bbc.com": "BBC News",
    "bbc.co.uk": "BBC News",
    "www.bbc.co.uk": "BBC News",
    "theguardian.com": "The Guardian",
    "www.theguardian.com": "The Guardian",
    # co.uk is the legacy host
    "thetimes.com": "The Times",
    "www.thetimes.com": "The Times",
    "thetimes.co.uk": "The Times",
    "www.thetimes.co.uk": "The Times",
    # resolved host seen in news_articles 2026-09-15 (8 rows): the paper's job
    # board, which the scanner reaches because vacancy titles match `oil` / `gas`
    # as plain substrings. Named, NOT filtered -- relevance is the keyword
    # gate's job, and dropping a host here would only hide it from the source
    # filter while the rows kept landing.
    "appointments.thetimes.com": "The Times",
    "telegraph.co.uk": "The Telegraph",
    "www.telegraph.co.uk": "The Telegraph",
    # subdomain-only outlet
    "news.sky.com": "Sky News",
    "www.news.sky.com": "Sky News",
    "politico.com": "POLITICO",
    "www.politico.com": "POLITICO",
    "politico.eu": "POLITICO Europe",
    "www.politico.eu": "POLITICO Europe",
    "axios.com": "Axios",
    "www.axios.com": "Axios",
    "forbes.com": "Forbes",
    "www.forbes.com": "Forbes",
    "fortune.com": "Fortune",
    "www.fortune.com": "Fortune",
    # markets. carries the syndicated wire copy
    "businessinsider.com": "Business Insider",
    "www.businessinsider.com": "Business Insider",
    "markets.businessinsider.com": "Business Insider",
    # resolved host seen in news_articles 2026-09-15 (17 rows): the Africa desk
    "africa.businessinsider.com": "Business Insider",
    # ENGLISH_NO_RSS entry is marketwatch.com/story
    "marketwatch.com": "MarketWatch",
    "www.marketwatch.com": "MarketWatch",
    "barrons.com": "Barron's",
    "www.barrons.com": "Barron's",
    "npr.org": "NPR",
    "www.npr.org": "NPR",
    # articles live on the .go.com host
    "abcnews.com": "ABC News",
    "www.abcnews.com": "ABC News",
    "abcnews.go.com": "ABC News",
    # resolved hosts seen in news_articles 2026-09-15 (9 and 3 rows): CDN and
    # ingest hosts Google hands back instead of the canonical one
    "www-cdn.abcnews.com": "ABC News",
    "ingest.abcnews.com": "ABC News",
    "nbcnews.com": "NBC News",
    "www.nbcnews.com": "NBC News",
    "cbsnews.com": "CBS News",
    "www.cbsnews.com": "CBS News",
    "foxbusiness.com": "Fox Business",
    "www.foxbusiness.com": "Fox Business",
    # --- Wave 5B: US regional, Europe, Canada, Oceania mainstream ---
    # EN edition; br.investing.com above is the pt one
    "investing.com": "Investing.com",
    "www.investing.com": "Investing.com",
    "france24.com": "France 24",
    "www.france24.com": "France 24",
    # /en/ edition
    "lemonde.fr": "Le Monde",
    "www.lemonde.fr": "Le Monde",
    "irishtimes.com": "The Irish Times",
    "www.irishtimes.com": "The Irish Times",
    "cityam.com": "City A.M.",
    "www.cityam.com": "City A.M.",
    "financialpost.com": "Financial Post",
    "www.financialpost.com": "Financial Post",
    "calgaryherald.com": "Calgary Herald",
    "www.calgaryherald.com": "Calgary Herald",
    # kept distinct from ABC News (US)
    "abc.net.au": "ABC News Australia",
    "www.abc.net.au": "ABC News Australia",
    "afr.com": "Australian Financial Review",
    "www.afr.com": "Australian Financial Review",
    "houstonchronicle.com": "Houston Chronicle",
    "www.houstonchronicle.com": "Houston Chronicle",
    "latimes.com": "Los Angeles Times",
    "www.latimes.com": "Los Angeles Times",
    "usatoday.com": "USA Today",
    "www.usatoday.com": "USA Today",
    "dw.com": "Deutsche Welle",
    "www.dw.com": "Deutsche Welle",
    "euronews.com": "Euronews",
    "www.euronews.com": "Euronews",
    "swissinfo.ch": "Swissinfo",
    "www.swissinfo.ch": "Swissinfo",
    "theglobeandmail.com": "The Globe and Mail",
    "www.theglobeandmail.com": "The Globe and Mail",
    "cbc.ca": "CBC News",
    "www.cbc.ca": "CBC News",
    "theaustralian.com.au": "The Australian",
    "www.theaustralian.com.au": "The Australian",
    "energynewsbulletin.net": "Energy News Bulletin",
    "www.energynewsbulletin.net": "Energy News Bulletin",
    "sodir.no": "Norwegian Offshore Directorate",
    "www.sodir.no": "Norwegian Offshore Directorate",
    # --- Wave 5C: Africa, Middle East & North-East Asia mainstream ---
    "allafrica.com": "AllAfrica",
    "www.allafrica.com": "AllAfrica",
    "businessday.ng": "BusinessDay Nigeria",
    "www.businessday.ng": "BusinessDay Nigeria",
    "punchng.com": "Punch",
    "www.punchng.com": "Punch",
    "thisdaylive.com": "ThisDay",
    "www.thisdaylive.com": "ThisDay",
    # businessday.co.za MEASURED in news_articles (8 rows)
    "businesslive.co.za": "Business Day (South Africa)",
    "www.businesslive.co.za": "Business Day (South Africa)",
    "businessday.co.za": "Business Day (South Africa)",
    "www.businessday.co.za": "Business Day (South Africa)",
    "theeastafrican.co.ke": "The East African",
    "www.theeastafrican.co.ke": "The East African",
    "africaoilgasreport.com": "Africa Oil+Gas Report",
    "www.africaoilgasreport.com": "Africa Oil+Gas Report",
    "energycapitalpower.com": "Energy Capital & Power",
    "www.energycapitalpower.com": "Energy Capital & Power",
    # English edition of the ar outlet keyed above; same display name on purpose
    "english.alarabiya.net": "Al Arabiya",
    "middleeasteye.net": "Middle East Eye",
    "www.middleeasteye.net": "Middle East Eye",
    "timesofisrael.com": "The Times of Israel",
    "www.timesofisrael.com": "The Times of Israel",
    "haaretz.com": "Haaretz",
    "www.haaretz.com": "Haaretz",
    # ENGLISH_NO_RSS entry is aa.com.tr/en
    "aa.com.tr": "Anadolu Agency",
    "www.aa.com.tr": "Anadolu Agency",
    "english.ahram.org.eg": "Ahram Online",
    "dailysabah.com": "Daily Sabah",
    "www.dailysabah.com": "Daily Sabah",
    "tehrantimes.com": "Tehran Times",
    "www.tehrantimes.com": "Tehran Times",
    "oilandgasmiddleeast.com": "Oil & Gas Middle East",
    "www.oilandgasmiddleeast.com": "Oil & Gas Middle East",
    "agbi.com": "AGBI",
    "www.agbi.com": "AGBI",
    # ENGLISH_NO_RSS entry is tradearabia.com/news; apex MEASURED (2 rows)
    "tradearabia.com": "Trade Arabia",
    "www.tradearabia.com": "Trade Arabia",
    "pipelineoilandgasnews.com": "Pipeline Oil & Gas Magazine",
    "www.pipelineoilandgasnews.com": "Pipeline Oil & Gas Magazine",
    # The feed host is NOT the article host. www.pipelineoilandgasnews.com
    # links 30/30 of its items to energyconnects.com (measured on the parallel
    # branch intl-post-sources, item A, which registers the article host in
    # INTERNATIONAL_RSS_DOMAINS), so the feed keys above never render anything:
    # `news_articles` holds 16 rows under host energyconnects.com and
    # source_name "energyconnects.com", and none under the magazine's own host.
    # "Energy Connects" is the site's own masthead; Pipeline Oil & Gas Magazine
    # is its sister title, which is why both names live here rather than one
    # being folded into the other.
    "energyconnects.com": "Energy Connects",
    "www.energyconnects.com": "Energy Connects",
    # MEASURED (1 row)
    "asia.nikkei.com": "Nikkei Asia",
    "japantimes.co.jp": "The Japan Times",
    "www.japantimes.co.jp": "The Japan Times",
    # www3 is NOT a www. prefix - it needs its own key
    "www3.nhk.or.jp": "NHK World",
    "koreaherald.com": "The Korea Herald",
    "www.koreaherald.com": "The Korea Herald",
    # --- Wave 5D: South & South-East Asia, Latin America mainstream, downstream trade press I ---
    "straitstimes.com": "The Straits Times",
    "www.straitstimes.com": "The Straits Times",
    "channelnewsasia.com": "CNA (Channel NewsAsia)",
    "www.channelnewsasia.com": "CNA (Channel NewsAsia)",
    "bangkokpost.com": "Bangkok Post",
    "www.bangkokpost.com": "Bangkok Post",
    "e.vnexpress.net": "VnExpress International",
    "timesofindia.indiatimes.com": "The Times of India",
    # energy.economictimes... above is ET EnergyWorld, a different desk
    "economictimes.indiatimes.com": "The Economic Times",
    "business-standard.com": "Business Standard",
    "www.business-standard.com": "Business Standard",
    "dawn.com": "Dawn",
    "www.dawn.com": "Dawn",
    "astanatimes.com": "The Astana Times",
    "www.astanatimes.com": "The Astana Times",
    # apex MEASURED in news_articles (2 rows); feed is the en. subdomain
    "en.trend.az": "Trend News Agency",
    "trend.az": "Trend News Agency",
    "www.trend.az": "Trend News Agency",
    "en.mercopress.com": "MercoPress",
    "mercopress.com": "MercoPress",
    "www.mercopress.com": "MercoPress",
    "batimes.com.ar": "Buenos Aires Times",
    "www.batimes.com.ar": "Buenos Aires Times",
    "mexiconewsdaily.com": "Mexico News Daily",
    "www.mexiconewsdaily.com": "Mexico News Daily",
    "thejakartapost.com": "The Jakarta Post",
    "www.thejakartapost.com": "The Jakarta Post",
    "theedgemalaysia.com": "The Edge Malaysia",
    "www.theedgemalaysia.com": "The Edge Malaysia",
    "kaieteurnewsonline.com": "Kaieteur News",
    "www.kaieteurnewsonline.com": "Kaieteur News",
    "bnamericas.com": "BNamericas",
    "www.bnamericas.com": "BNamericas",
    "hydrocarbonprocessing.com": "Hydrocarbon Processing",
    "www.hydrocarbonprocessing.com": "Hydrocarbon Processing",
    "hydrocarbonengineering.com": "Hydrocarbon Engineering",
    "www.hydrocarbonengineering.com": "Hydrocarbon Engineering",
    # --- Wave 5E: O&G / refining / shipping trade press & institutions ---
    "lngindustry.com": "LNG Industry",
    "www.lngindustry.com": "LNG Industry",
    "worldpipelines.com": "World Pipelines",
    "www.worldpipelines.com": "World Pipelines",
    "tanksterminals.com": "Tanks and Terminals",
    "www.tanksterminals.com": "Tanks and Terminals",
    # IADC
    "drillingcontractor.org": "Drilling Contractor",
    "www.drillingcontractor.org": "Drilling Contractor",
    "rbnenergy.com": "RBN Energy",
    "www.rbnenergy.com": "RBN Energy",
    # F+L Daily
    "fuelsandlubes.com": "Fuels & Lubes",
    "www.fuelsandlubes.com": "Fuels & Lubes",
    "energymonitor.ai": "Energy Monitor",
    "www.energymonitor.ai": "Energy Monitor",
    "shipandbunker.com": "Ship & Bunker",
    "www.shipandbunker.com": "Ship & Bunker",
    "seatrade-maritime.com": "Seatrade Maritime News",
    "www.seatrade-maritime.com": "Seatrade Maritime News",
    # Insights blog
    "kpler.com": "Kpler",
    "www.kpler.com": "Kpler",
    # Today in Energy
    "eia.gov": "US EIA",
    "www.eia.gov": "US EIA",
    "offshore-mag.com": "Offshore Magazine",
    "www.offshore-mag.com": "Offshore Magazine",
    "gasprocessingnews.com": "Gas Processing & LNG",
    "www.gasprocessingnews.com": "Gas Processing & LNG",
    "qcintel.com": "Quantum Commodity Intelligence",
    "www.qcintel.com": "Quantum Commodity Intelligence",
    "lloydslist.com": "Lloyd's List",
    "www.lloydslist.com": "Lloyd's List",
    "rivieramm.com": "Riviera Maritime Media",
    "www.rivieramm.com": "Riviera Maritime Media",
    # ex-PetrolPlaza, which 301s here
    "mobilityplaza.com": "Mobility Plaza",
    "www.mobilityplaza.com": "Mobility Plaza",
    # "IEA" and "OPEC" are the only two names in this dict that are also
    # everyday attribution tokens in a headline. _SITE_SUFFIX_PATTERNS is built
    # from SOURCE_NAMES.values(), so clean_title would read a genuine
    # "Demand revised up - IEA" as a site suffix and cut the attribution off.
    # Inert today, and verified as inert rather than assumed: clean_title has
    # exactly one caller, `_extract`, and its only consumer in enrich.py
    # discards that title (`_, paragrafos = _extract(...)`). The title the
    # dashboard shows comes from the feed / GNews item, or from
    # _extract_title_from_html, neither of which passes through clean_title. If
    # it is ever put on that path, these two values need an opt-out from
    # _SITE_SUFFIX_PATTERNS - not a rewrite of the outlet's name.
    "iea.org": "IEA",
    "www.iea.org": "IEA",
    "opec.org": "OPEC",
    "www.opec.org": "OPEC",
    "woodmac.com": "Wood Mackenzie",
    "www.woodmac.com": "Wood Mackenzie",
    "rystadenergy.com": "Rystad Energy",
    "www.rystadenergy.com": "Rystad Energy",
    "vortexa.com": "Vortexa",
    "www.vortexa.com": "Vortexa",
    "oxfordenergy.org": "Oxford Institute for Energy Studies",
    "www.oxfordenergy.org": "Oxford Institute for Energy Studies",
    "icis.com": "ICIS",
    "www.icis.com": "ICIS",
    "jpt.spe.org": "JPT (SPE)",
}


# =============================================================================
# Extractors por dominio
# =============================================================================

Extractor = Callable[[BeautifulSoup], tuple[str, list[str]]]

NOISE_CLASS_SUBSTRINGS = (
    "advertisement", "publicidade", "newsletter", "related", "relacionad",
    "leia-tambem", "leia-mais", "recomend", "share-", "social-share",
    "tags-list", "author-box", "byline", "sponsor", "subscribe",
    "breadcrumb", "comments", "content-ads", "tag-manager-publicidade",
    "read-more", "mc-read-more", "recommend-theme",
    "box-seja-assinante", "seja-assinante", "assine-", "paywall-wrap",
    "subscription", "premium-content-wall",
    # 2026-10-14 O Globo AMP barrier (mirror of src/lib/clipping): login CTAs,
    # registration walls, AMP paywall chrome and the publication-date line.
    # Do NOT add bare "paywall" or "paywall-section": they wrap REAL bodies
    # (Globo `div.paywall`, Jornal do Comercio `section.paywall-carregando`).
    # "login" / "signin" are NOT substrings: see NOISE_CLASS_TOKEN_PREFIXES.
    "sign-in", "regwall", "barreira", "paywall-amp",
    "paywall-login", "paywall-cta", "paywall-message", "publication-data",
    # 2026-10-16 Eixos: Google "preferred sources" banner
    # (`div.preferred-source-text`), shipped as body.
    "preferred-source",
    # 2026-10-16 Eixos: embedded "you read here" cards of OTHER articles
    # (`div.wp-block-seox-content-feeds > article.feed > p.feed-hat`) leaked
    # their kicker ("comece seu dia", "diálogos da transição") into the body.
    "content-feeds",
    # 2026-10-16 full-registry sweep (r7, 42 domains with boilerplate; shared
    # contract with src/lib/clipping): author bios, "more reads" rails,
    # back-to-top buttons, comment widgets, consent banners, video/embed
    # placeholders, copyright footers and eyebrow labels.
    # Do NOT add bare "comment" (`commentary`, `comment-analysis` wrap real
    # opinion bodies), "teaser" / "kicker" (lede wrappers on several CMSs) or
    # bare "author" (`author-name` sits inside article headers that also hold
    # the standfirst).
    "author-bio", "about-the-author", "author-info", "author-card",
    "contributor-bio", "more-reads", "top-reads", "back-to-top", "disqus",
    "comment-form", "comment-policy", "consent", "video-placeholder",
    "embed-placeholder", "copyright", "eyebrow",
    # 2026-10-19 Energy News Bulletin: research-report promos
    # (`div.research-block`). The TS side strips them so a paywall teaser is
    # not mistaken for a 3-paragraph body; lists stay byte-identical.
    "research-block",
)

#: Words that mark a login widget only when a class token STARTS with them
#: (start of the string or after `-`, `_` or whitespace): `menu-login`,
#: `login-profile`, `paywall-login`, `loginbox` match; `bloginfo`,
#: `cataloginfo`, `designing` do not. As plain substrings they stripped real
#: bodies. Shared contract with src/lib/clipping.
NOISE_CLASS_TOKEN_PREFIXES = ["login", "signin"]
_NOISE_CLASS_TOKEN_RE = re.compile(
    r"(?:^|[-_\s])(?:" + "|".join(NOISE_CLASS_TOKEN_PREFIXES) + r")", re.IGNORECASE
)

#: AMP subscription markup: the not-granted section, the action buttons and the
#: dialog are the wall itself, never the article (the granted preview lives in
#: `[subscriptions-section="content"]`, which is kept).
SUBSCRIPTION_GATE_SELECTORS = (
    '[subscriptions-section="content-not-granted"], '
    '[subscriptions-actions], [subscriptions-dialog]'
)


#: Label-led index group (2026-10-16 Eixos newsletter edition pages; shared
#: contract with src/lib/clipping): a "NESTA EDIÇÃO." label paragraph followed
#: by short teaser paragraphs in the same wrapper is the edition index, not
#: the article.
INDEX_LABEL_RE = re.compile(r"^\s*nesta\s+edi[cç][aã]o\s*[.:\-–—]", re.IGNORECASE)
INDEX_TEASER_MAX_CHARS = 120
#: Upper bound on the DIRECT `<p>` children of a removable group (label + 2-7
#: teasers); a wrapper with more is a body, not an edition index.
INDEX_GROUP_MAX_P = 8


def _collapsed_text(tag: Tag) -> str:
    # No separator: cheerio `.text()` inserts none at inline-tag boundaries,
    # so both sides measure the same string (`<strong>NESTA EDI</strong>ÇÃO.`).
    return re.sub(r"\s+", " ", tag.get_text()).strip()


def _strip_index_groups(container: Tag) -> None:
    """Drop a label-led index group: the label `<p>` and its short teasers.

    For each `<p>` whose collapsed text matches INDEX_LABEL_RE: when its
    parent is not the container itself, has 3..INDEX_GROUP_MAX_P DIRECT `<p>`
    children and every OTHER `<p>` DESCENDANT is under INDEX_TEASER_MAX_CHARS,
    the whole parent goes (label + teasers); otherwise only the label `<p>`.
    """
    for p in list(container.find_all("p")):
        if p.attrs is None or p.parent is None:
            continue  # already removed with a decomposed group
        if not INDEX_LABEL_RE.search(_collapsed_text(p)):
            continue
        parent = p.parent
        direct = parent.find_all("p", recursive=False)
        others = [q for q in parent.find_all("p") if q is not p]
        if (parent is not container and 3 <= len(direct) <= INDEX_GROUP_MAX_P
                and all(len(_collapsed_text(q)) < INDEX_TEASER_MAX_CHARS for q in others)):
            parent.decompose()
        else:
            p.decompose()


#: Related headline (2026-10-16 c, round 8 family 3; byte-identical contract
#: with stripRelatedHeadlines in src/lib/clipping/extract.ts): a `<p>` in the
#: container that IS the text of a link, i.e. another article's headline
#: shipped as a paragraph (moneytimes `ul.single_videos` video teaser; Estadão
#: E-Investidor "Leia mais" list, `<a><div><p class="li-title">`).
#: Runs as the LAST step of _strip_noise (after _strip_index_groups), over the
#: document as it stands then.
#:   LINK KEYS: for every `<a>` in the WHOLE document whose `href` attribute is
#:     present and does not start with "#" (an in-page TOC link names this
#:     article's own intertitle), _dedupe_key(collapsed text) and
#:     _dedupe_key(`title` attribute); empty keys are ignored.
#:   CANDIDATE: a `<p>` in the container whose collapsed text is non-empty and
#:     at most RELATED_HEADLINE_MAX_CHARS (120) code points, whose _dedupe_key
#:     equals the LINK KEY of (i) an ANCESTOR `<a>` of the `<p>` (the card
#:     shape; any final punctuation), or (ii) an `<a>` that is neither ancestor
#:     nor descendant of the `<p>` (a teaser repeated elsewhere on the page),
#:     AND the `<p>` does NOT end in _SENTENCE_END_RE. A descendant `<a>` never
#:     counts (the all-links `<p>` skip in _paragraphs_from owns that case).
#:   BOUND: the candidates are removed only when at least
#:     RELATED_HEADLINE_MIN_KEEP (2) non-empty `<p>` of the container are NOT
#:     candidates; otherwise nothing is removed. A short real body is never
#:     emptied.
RELATED_HEADLINE_MAX_CHARS = 120
RELATED_HEADLINE_MIN_KEEP = 2


def _strip_related_headlines(container: Tag) -> None:
    root = container
    while root.parent is not None:
        root = root.parent
    links_by_key: dict[str, list[Tag]] = {}
    for a in root.find_all("a"):
        href = a.get("href")
        if href is None or href.startswith("#"):
            continue
        for key in (_dedupe_key(_collapsed_text(a)), _dedupe_key(a.get("title") or "")):
            if key:
                links_by_key.setdefault(key, []).append(a)
    if not links_by_key:
        return

    candidates: list[Tag] = []
    kept = 0
    for p in container.find_all("p"):
        text = _collapsed_text(p)
        if not text:
            continue
        links = links_by_key.get(_dedupe_key(text))
        is_candidate = False
        if links and len(text) <= RELATED_HEADLINE_MAX_CHARS:
            # Identity, not ==: bs4 compares Tags by markup.
            ancestors = {id(t) for t in p.parents}
            in_card = any(id(a) in ancestors for a in links)
            elsewhere = any(
                id(a) not in ancestors and not any(t is p for t in a.parents)
                for a in links
            )
            is_candidate = in_card or (elsewhere and not _SENTENCE_END_RE.search(text))
        if is_candidate:
            candidates.append(p)
        else:
            kept += 1
    if not candidates or kept < RELATED_HEADLINE_MIN_KEEP:
        return
    removed = {id(p) for p in candidates}
    for p in candidates:
        # A candidate nested in another candidate goes with it.
        if not any(id(t) in removed for t in p.parents):
            p.decompose()


def _strip_noise(container: Tag) -> None:
    for tag in container.find_all(["figure", "figcaption", "aside", "script", "style", "iframe", "form", "nav"]):
        tag.decompose()
    for el in container.select(SUBSCRIPTION_GATE_SELECTORS):
        if el.parent is not None:
            el.decompose()
    for el in list(container.find_all(True)):
        if el.attrs is None or el.parent is None:
            continue
        classes = el.get("class") or []
        idv = el.get("id") or ""
        combined = " ".join(list(classes) + [idv]).lower()
        if (any(sub in combined for sub in NOISE_CLASS_SUBSTRINGS)
                or _NOISE_CLASS_TOKEN_RE.search(combined)):
            el.decompose()
    # Runs last so that nodes removed above do not count as teasers.
    _strip_index_groups(container)
    # 2026-10-16 c: after the index groups, over what is left (see
    # _strip_related_headlines).
    _strip_related_headlines(container)


def _title_from_meta(soup: BeautifulSoup) -> str:
    for sel in [
        ("meta", {"property": "og:title"}),
        ("meta", {"name": "twitter:title"}),
        ("meta", {"itemprop": "headline"}),
    ]:
        tag = soup.find(*sel)
        if tag and tag.get("content"):
            return tag["content"].strip()
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return ""


def _paragraphs_from(container: Tag | None) -> list[str]:
    if container is None:
        return []
    _strip_noise(container)
    paragraphs: list[str] = []
    for p in container.find_all("p"):
        if p.find_all(recursive=False) and all(child.name == "a" for child in p.find_all(recursive=False)):
            text_only = p.get_text(" ", strip=True)
            link_text = " ".join(a.get_text(" ", strip=True) for a in p.find_all("a"))
            if text_only == link_text:
                continue
        txt = p.get_text(" ", strip=True)
        if txt:
            paragraphs.append(txt)
    return paragraphs


def _first_matching(soup: BeautifulSoup, selectors: list[str]) -> Tag | None:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    return None


def _generic(soup: BeautifulSoup, selectors: list[str]) -> tuple[str, list[str]]:
    title = _title_from_meta(soup)
    container = _first_matching(soup, selectors) or soup.find("article")
    return title, _paragraphs_from(container)


def _make_extractor(selectors: list[str]) -> Extractor:
    return lambda soup: _generic(soup, selectors)


ex_globo = _make_extractor([
    "div.mc-article-body", "article .mc-article-body",
    "article .content-text__container", "article",
])
ex_folha = _make_extractor([
    "div.c-news__body", "article.c-news", "div.c-main-content", "article",
])
ex_estadao = _make_extractor([
    "div.content-wrapper.news-body", "div.news-body",
    "div.template-reportagem",
    "div.n--noticia__content", "div.noticia__conteudo",
    "section.n--noticia__content", "article",
])
ex_brasilenergia = _make_extractor([
    "div.editorial_", "div.descricao-noticia",
    "div.single-content", "div.entry-content", "article",
])
ex_metropoles = _make_extractor([
    "div.m-news__body", "div.texto-materia",
    "article .noticia-conteudo", "article",
])
ex_poder360 = _make_extractor(["div.entry-content", "article .post-content", "article"])
ex_infomoney = _make_extractor([
    "div.article__content", "div.single__content", "div.im-article", "article",
])
ex_bloomberglinea = _make_extractor(["div.article-content", "div.article-body", "article"])
ex_r7 = _make_extractor(["div.b-article__body", "div.article-content", "article"])
ex_agencia_petrobras = _make_extractor(["div.entry-content", "article .post-content", "article"])
# Agencia iNFRA migrated to a Hello Elementor theme (seen 2026-08-20): there is
# no div.entry-content any more, and the eight <article> elements on the page are
# related-post cards holding zero <p>. The three legacy selectors therefore
# matched an empty shell and enrich fell through to the meta description. The
# widget-TYPE class is stable; the elementor-element-<hash> sibling is not.
ex_agenciainfra = _make_extractor([
    ".elementor-widget-theme-post-content .elementor-widget-container",
    "div.entry-content", "article .post-content", "article",
])
# Brazil Journal (2026-10-15): site migrated to Nuxt; body in div.post-body with
# <p><span>; never select on data-v-<hash> or Tailwind arbitrary classes. The
# WordPress containers below are gone and `article` now matches related-post
# teaser cards with no <p>; they stay only as fallbacks.
ex_braziljournal = _make_extractor([
    "div.post-body",
    "div.post-content-text", "section.post-content", "div.entry-content", "article",
])
# OilPrice.com (2026-10-19): body <p> live only in div#news-content
# (= div.wysiwyg.clear); byline, author box, ticker tooltip and share/related/
# comment blocks are outside it. Rejected: singleArticle__content (adds the
# byline + "Join the discussion"), singleArticle__wrapper (adds the author box),
# tableGrid__column--articleContent (adds share/related/comment), inner_content
# (holds no body). Known residual: the "By <Name> for Oilprice.com" sign-off is
# a bare <p> inside the container. Mirrors src/lib/clipping/sources.ts.
ex_oilprice = _make_extractor([
    "div#news-content",
    "div.singleArticle__content div.wysiwyg",
])
ex_eixos = _make_extractor([
    "div.entry-content", "div.post-content", "article .tdb-block-inner", "article",
])
ex_monitormercantil = _make_extractor(["div.td-post-content", "div.entry-content", "article"])
ex_timesbrasil = _make_extractor(["div.article-content", "div.entry-content", "article"])
ex_visaoagro = _make_extractor(["div.entry-content", "div.post-content", "article"])
# Gazeta do Povo runs Next.js with CSS-module class names, so the body wrapper
# is `postBody-module-scss-module__<hash>__postBodyContainer` and the hash
# rotates on every rebuild — hence the substring selector. Without it the
# domain falls through to the meta-description fallback in enrich, which yields
# ~140 chars of standfirst; the container gives the 24 real paragraphs (the
# metered paywall is client-side, so an anonymous fetch gets the whole body).
# `<article>` is NOT a useful fallback here: the tag exists but holds no <p>.
ex_gazetadopovo = _make_extractor([
    'div[class*="postBodyContainer"]', 'div[class*="postBody"]',
    'div[class*="postContent"]',
])
# Argus Media runs Next.js and emits NO <article> and NO itemprop markup at all,
# so every ex_auto selector missed and the domain fell through to the
# meta-description fallback in enrich — title-only matching on a page whose body
# is fully served, unauthenticated (measured 2026-08-28: HTTP 200, 209 KB, 24 <p>
# of which 14 are the body). div#news-body-text matches exactly once on a news
# page and holds exactly the standfirst + body + byline; the sibling
# NewsPost_newsPost__<hash> class is a CSS module and rotates on every build, so
# never select on it. `<article>` is NOT a useful fallback here — the tag does
# not exist on the page. Podcast (/energy-and-commodity-podcasts/) and
# video-insight (/video-insights/) pages carry no #news-body-text and no article
# prose; extracting nothing there is the correct outcome.
ex_argus = _make_extractor([
    "div#news-body-text", 'div[itemprop="articleBody"]', "main article", "article",
])

# Riviera Maritime Media runs Affino, which wraps the body in a PROPRIETARY class
# and emits no <article>, no itemprop and none of ex_auto's containers. Measured
# on the runner 2026-09-15,
# www.rivieramm.com/news-content-hub/vlccs-and-suezmaxes-sold-like-hot-cakes-90002
# (HTTP 200, 108 KB, fetched anonymously):
#
#   div.aos-DS27-WYSEdit          -> 1 match, 22 <p>   <- the body
#   <article>                     -> 0 elements        <- so _generic's implicit
#                                                         fallback finds nothing
#   div[itemprop="articleBody"]   -> 0 elements
#   ex_auto                       -> 0 paragraphs
#
# So every Riviera row was stored with an empty snippet -- all 8 of them on
# 2026-09-15 -- and the outlet degraded to title-only matching. The class is
# Affino's own widget id, NOT a build hash: it is stable across articles, which
# is why it is matched literally and case-sensitively. The dashboard's clipping
# registry carries the SAME selector string; keep the two in step.
#
# Riviera's apex is unreachable (ConnectTimeout on 443), so this extractor only
# ever runs because store.WWW_ONLY_HOSTS keeps the www form in the stored url.
ex_rivieramm = _make_extractor(["div.aos-DS27-WYSEdit"])

def _br_paragraphs(container: Tag | None) -> list[str]:
    """Split a <br>-separated body into paragraphs.

    _paragraphs_from collects <p> tags, which is the near-universal shape. Some
    CMSs ship the whole article as ONE <div> whose paragraph breaks are <br>,
    so <p>-collection returns an empty list on a page that visibly has a body.
    """
    if container is None:
        return []
    _strip_noise(container)
    for br in container.find_all("br"):
        br.replace_with("\n")
    return [seg.strip() for seg in container.get_text(" ").split("\n") if seg.strip()]


def ex_jornaldocomercio(soup: BeautifulSoup) -> tuple[str, list[str]]:
    """Jornal do Comercio (Porto Alegre/RS).

    Two reasons this cannot be ex_auto:
      1. The body lives in `section.paywall-carregando` — none of ex_auto's
         selectors match it, and its `article` fallback is useless here because
         the tag exists but holds no <p> (same trap as Gazeta do Povo).
      2. Inside that section the article is a single <div> with <br> separators
         and NO <p> at all, so even the right container yields zero paragraphs
         through the normal path. Hence _br_paragraphs.
    Without this, enrich falls through to the meta description, which on JC is
    often the SECTION boilerplate ("Noticias sobre negocios e os principais
    setores da economia") rather than the article's own standfirst.
    The class name is matched by SUBSTRING so a Tailwind sibling class or a
    rename to `paywall-*` does not silently break extraction.
    """
    title = _title_from_meta(soup)
    container = _first_matching(soup, [
        'section[class*="paywall-carregando"]',
        'section[class*="paywall"]',
        'div[class*="paywall-carregando"]',
    ])
    return title, _br_paragraphs(container)


def _next_data_html(soup: BeautifulSoup, path: str) -> str | None:
    """Pull an HTML string out of a Next.js `__NEXT_DATA__` JSON island.

    `path` is dotted, resolved from the root of the parsed object. Returns None
    for a missing script tag, unparseable JSON, or a path that does not land on
    a non-empty string — every failure is silent so the caller can fall back to
    its normal selectors.
    """
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag is None:
        return None
    raw = tag.string or tag.get_text() or ""
    if not raw.strip():
        return None
    try:
        cur = json.loads(raw)
    except (ValueError, TypeError):
        return None
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur if isinstance(cur, str) and cur.strip() else None


def ex_jota(soup: BeautifulSoup) -> tuple[str, list[str]]:
    """JOTA (jota.info) — Next.js Pages Router, body NOT in the served DOM.

    The rendered page carries exactly ONE <p> and zero <article>: the article
    lives only in the `__NEXT_DATA__` JSON island, at
    props.pageProps.post.content, as an HTML string (5-8 KB on the three pieces
    measured 2026-08-20 — news, column, opinion). ex_auto returns nothing here,
    which would leave enrich matching on the title alone.

    JOTA PRO pieces expose a truncated `content`; those yield a short paragraph
    list, which is the honest answer — the body genuinely is not served.
    """
    title = _title_from_meta(soup)
    body = _next_data_html(soup, "props.pageProps.post.content")
    if body:
        return title, _paragraphs_from(BeautifulSoup(body, "lxml"))
    # Fallback in case JOTA ever server-renders the body.
    return title, _paragraphs_from(_first_matching(soup, [
        'div[itemprop="articleBody"]', "div.entry-content", "article",
    ]))


ex_auto = _make_extractor([
    'div[itemprop="articleBody"]',
    "div.article-content", "div.article-body", "div.article__content",
    "div.article__body", "div.post-content", "div.post__content",
    "div.post-body", "div.entry-content", "div.entry__content",
    "div.single-content", "div.single__content", "div.content-text",
    "div.news-text", "div.news-content", "div.news__body",
    "div.materia-conteudo", "div.conteudo-materia", "div.texto-materia",
    "div.texto", "div.content", "div.body", "div.main-content",
    "section.article-body", "section.content", "main article",
    "article .content", "article",
])


EXTRACTORS: dict[str, Extractor] = {
    "valor.globo.com": ex_globo,
    "oglobo.globo.com": ex_globo,
    "g1.globo.com": ex_globo,
    "pipelinevalor.globo.com": ex_globo,
    "globorural.globo.com": ex_globo,
    "cbn.globo.com": ex_globo,
    "www1.folha.uol.com.br": ex_folha,
    "folha.uol.com.br": ex_folha,
    "aovivo.folha.uol.com.br": ex_folha,
    "www.estadao.com.br": ex_estadao,
    "estadao.com.br": ex_estadao,
    "estradao.estadao.com.br": ex_estadao,
    "brasilenergia.com.br": ex_brasilenergia,
    "www.brasilenergia.com.br": ex_brasilenergia,
    "www.metropoles.com": ex_metropoles,
    "metropoles.com": ex_metropoles,
    "www.poder360.com.br": ex_poder360,
    "poder360.com.br": ex_poder360,
    "www.infomoney.com.br": ex_infomoney,
    "infomoney.com.br": ex_infomoney,
    "www.bloomberglinea.com.br": ex_bloomberglinea,
    "bloomberglinea.com.br": ex_bloomberglinea,
    "noticias.r7.com": ex_r7,
    "agencia.petrobras.com.br": ex_agencia_petrobras,
    "agenciainfra.com": ex_agenciainfra,
    "www.agenciainfra.com": ex_agenciainfra,
    "jota.info": ex_jota,
    "www.jota.info": ex_jota,
    "braziljournal.com": ex_braziljournal,
    "www.braziljournal.com": ex_braziljournal,
    "eixos.com.br": ex_eixos,
    "www.eixos.com.br": ex_eixos,
    "monitormercantil.com.br": ex_monitormercantil,
    "www.monitormercantil.com.br": ex_monitormercantil,
    "timesbrasil.com.br": ex_timesbrasil,
    "www.timesbrasil.com.br": ex_timesbrasil,
    "visaoagro.com.br": ex_visaoagro,
    "www.visaoagro.com.br": ex_visaoagro,
    "gazetadopovo.com.br": ex_gazetadopovo,
    "www.gazetadopovo.com.br": ex_gazetadopovo,
    # Apex first: sitemap <loc> and feed links are www, but normalize_url strips
    # it, so the domain the enricher looks up is the apex.
    "jornaldocomercio.com": ex_jornaldocomercio,
    "www.jornaldocomercio.com": ex_jornaldocomercio,
    "theagribiz.com": ex_auto,
    "www.theagribiz.com": ex_auto,
    "www.cnnbrasil.com.br": ex_auto,
    "cnnbrasil.com.br": ex_auto,
    "veja.abril.com.br": ex_auto,
    "investnews.com.br": ex_auto,
    "www.investnews.com.br": ex_auto,
    "neofeed.com.br": ex_auto,
    "www.neofeed.com.br": ex_auto,
    "www.cnbc.com": ex_auto,
    "exame.com": ex_auto,
    "www.exame.com": ex_auto,
    "istoedinheiro.com.br": ex_auto,
    "www.istoedinheiro.com.br": ex_auto,
    "www.brasil247.com": ex_auto,
    "brasil247.com": ex_auto,
    "observatorio.firjan.com.br": ex_auto,
    "megawhat.uol.com.br": ex_auto,
    "www.reuters.com": ex_auto,
    "reuters.com": ex_auto,
    "br.investing.com": ex_auto,
    "www.correiobraziliense.com.br": ex_auto,
    "correiobraziliense.com.br": ex_auto,
    "www.correiodopovo.com.br": ex_auto,
    "correiodopovo.com.br": ex_auto,
    "www.atribuna.com.br": ex_auto,
    "atribuna.com.br": ex_auto,
    "veronoticias.com": ex_auto,
    "www.veronoticias.com": ex_auto,
    "diariodopoder.com.br": ex_auto,
    "www.diariodopoder.com.br": ex_auto,
    "www.conjur.com.br": ex_auto,
    "conjur.com.br": ex_auto,
    "www.argusmedia.com": ex_argus,
    "argusmedia.com": ex_argus,
    "operamundi.uol.com.br": ex_auto,
    "claudiodantas.com.br": ex_auto,
    "www.claudiodantas.com.br": ex_auto,
    "br.tradingview.com": ex_auto,
    # Apex twin (see the SOURCE_NAMES comment): this dict is compared to
    # SOURCE_NAMES key by key, so a name without its extractor is a red test.
    "theedgesingapore.com": ex_auto,
    "www.theedgesingapore.com": ex_auto,
    # Radio Itatiaia (Belo Horizonte/MG). Next.js + Tailwind: the body wrapper
    # carries only utility classes ("mx-auto flex w-full max-w-[640px] ..."),
    # so there is no semantic hook and none of ex_auto's NAMED selectors match.
    # What saves it is that the body sits in a real <article>
    # tag, which is ex_auto's LAST fallback, and — unlike Gazeta do Povo and
    # Jornal do Comercio, where <article> exists but holds no <p> — here the
    # <p> tags are inside it. Measured 2026-08-18 on three articles across
    # economia/politica: 8/10/13 paragraphs, 2.3k/3.7k/4.0k chars of real body,
    # against meta descriptions of 148/135/112 chars. Without the registration
    # enrich falls through to that meta description, i.e. the standfirst.
    #
    # WHERE THIS ACTUALLY RUNS, because it is not where you would assume: the
    # periodic scan calls run_search(fast_mode=True), which passes
    # need_snippet=False, and enrich_item then returns BEFORE fetch_html for any
    # item that already has a title and a date — i.e. for every well-formed RSS
    # item. So on the main scan path no extractor runs at all and the stored
    # snippet is just the feed's own <description> (or empty, when that is under
    # SNIPPET_MIN_RSS_CHARS=150). The extractor is exercised by the LEDE RESCUE,
    # which passes need_snippet=True. Measured end to end on 2026-08-18 for two
    # Itatiaia articles whose <description> is 148 and 92 chars: fast path 0 and
    # 0, lede path 357 and 360. That difference is the whole value of this entry
    # — it is what a body-only keyword gets matched against.
    "itatiaia.com.br": ex_auto,
    "www.itatiaia.com.br": ex_auto,
    "www12.senado.leg.br": ex_auto,
    "www.camara.leg.br": ex_auto,
    "camara.leg.br": ex_auto,
    "edition.cnn.com": ex_auto,
    "www.cnn.com": ex_auto,
    "cnn.com": ex_auto,
    "clickpetroleoegas.com.br": ex_auto,
    "www.clickpetroleoegas.com.br": ex_auto,
    "ineep.org.br": ex_auto,
    "www.ineep.org.br": ex_auto,
    "tconline.com.br": ex_auto,
    "www.tconline.com.br": ex_auto,
    "obastidor.com.br": ex_auto,
    "www.obastidor.com.br": ex_auto,
    "noticias.uol.com.br": ex_auto,
    "economia.uol.com.br": ex_auto,
    "www.terra.com.br": ex_auto,
    "terra.com.br": ex_auto,
    "www.moneytimes.com.br": ex_auto,
    "moneytimes.com.br": ex_auto,
    "visnoinvest.com.br": ex_auto,
    "www.visnoinvest.com.br": ex_auto,
    # International oil & gas / shipping trade press (English) — Wave 1a, RSS.
    # ex_auto covers all of these; measured 2026-08-18 the matches land on
    # title+summary so no extractor runs on the main scan path — the extractor
    # only powers the lede rescue (body fetch of a near-miss). World Oil is the
    # exception that always fetches (its feed has no <pubDate>, so enrich_item
    # fetches the article page to read article:published_time).
    "oilprice.com": ex_oilprice,
    "www.oilprice.com": ex_oilprice,
    "gcaptain.com": ex_auto,
    "www.gcaptain.com": ex_auto,
    "hellenicshippingnews.com": ex_auto,
    "www.hellenicshippingnews.com": ex_auto,
    "lngprime.com": ex_auto,
    "www.lngprime.com": ex_auto,
    "oedigital.com": ex_auto,
    "www.oedigital.com": ex_auto,
    "splash247.com": ex_auto,
    "www.splash247.com": ex_auto,
    "worldoil.com": ex_auto,
    "www.worldoil.com": ex_auto,
    # International oil & gas trade press covered via Google News — Wave 1a.
    # Bodies are WAF-blocked for OGJ / Energy Voice / Rigzone, so ex_auto never
    # gets a body to parse (items are title-only) — it is registered for
    # consistency and for the offshore-technology case, whose article pages DO
    # clear via the curl_cffi fallback in fetch_html even though its feed does
    # not, so a lede-rescue body fetch can actually succeed there.
    "ogj.com": ex_auto,
    "www.ogj.com": ex_auto,
    "energyvoice.com": ex_auto,
    "www.energyvoice.com": ex_auto,
    "offshore-technology.com": ex_auto,
    "www.offshore-technology.com": ex_auto,
    "rigzone.com": ex_auto,
    "www.rigzone.com": ex_auto,
    # Premium / paywalled international wires covered via Google News — Wave 1b.
    # Bodies are paywalled/WAF-blocked, so items are title-only and ex_auto
    # never gets a body to parse on the main scan path; registered for
    # consistency and for the lede-rescue path where a body fetch can succeed.
    "bloomberg.com": ex_auto,
    "www.bloomberg.com": ex_auto,
    "spglobal.com": ex_auto,
    "www.spglobal.com": ex_auto,
    "upstreamonline.com": ex_auto,
    "www.upstreamonline.com": ex_auto,
    "tradewindsnews.com": ex_auto,
    "www.tradewindsnews.com": ex_auto,
    "energyintel.com": ex_auto,
    "www.energyintel.com": ex_auto,
    "hartenergy.com": ex_auto,
    "www.hartenergy.com": ex_auto,
    # Russia / CIS + Middle East oil & gas press (English) -- Wave 2
    # (2026-08-18). ex_auto covers all: GNews / paywalled bodies are
    # title-only so no extractor runs on the main scan path; kept for the
    # lede-rescue path and for The National's dated business RSS.
    "intellinews.com": ex_auto,
    "www.intellinews.com": ex_auto,
    "tass.com": ex_auto,
    "www.tass.com": ex_auto,
    "interfax.com": ex_auto,
    "www.interfax.com": ex_auto,
    "mees.com": ex_auto,
    "www.mees.com": ex_auto,
    "arabnews.com": ex_auto,
    "www.arabnews.com": ex_auto,
    "thenationalnews.com": ex_auto,
    "www.thenationalnews.com": ex_auto,
    "shana.ir": ex_auto,
    "www.shana.ir": ex_auto,
    "en.shana.ir": ex_auto,
    "gulfnews.com": ex_auto,
    "www.gulfnews.com": ex_auto,
    "aljazeera.com": ex_auto,
    "www.aljazeera.com": ex_auto,
    "zawya.com": ex_auto,
    "www.zawya.com": ex_auto,
    "iraqoilreport.com": ex_auto,
    "www.iraqoilreport.com": ex_auto,
    # US + Europe oil & gas trade press (English) -- Wave 3 (2026-08-18).
    # ex_auto covers all: RSS matches land on title+summary so no extractor runs
    # on the main scan path; the extractor only powers the lede rescue (body
    # fetch of a near-miss), and GNews bodies are paywalled/WAF-blocked.
    "oilandgas360.com": ex_auto,
    "www.oilandgas360.com": ex_auto,
    "offshore-energy.biz": ex_auto,
    "www.offshore-energy.biz": ex_auto,
    "naturalgasintel.com": ex_auto,
    "www.naturalgasintel.com": ex_auto,
    # Wave 3 GNews-covered (bodies WAF-blocked/paywalled, title-only on the main
    # scan path); ex_auto registered for consistency + the lede-rescue path.
    "eenews.net": ex_auto,
    "www.eenews.net": ex_auto,
    "montelnews.com": ex_auto,
    "www.montelnews.com": ex_auto,
    "globalenergynetwork.net": ex_auto,
    "www.globalenergynetwork.net": ex_auto,
    # India oil & gas / business press (English) -- Wave 4 (2026-08-18). ET
    # EnergyWorld / Mint / BusinessLine RSS matches land on title+summary;
    # Moneycontrol is GNews title-only. ex_auto registered for the lede-rescue
    # path + consistency.
    "energy.economictimes.indiatimes.com": ex_auto,
    "livemint.com": ex_auto,
    "www.livemint.com": ex_auto,
    "thehindubusinessline.com": ex_auto,
    "www.thehindubusinessline.com": ex_auto,
    "moneycontrol.com": ex_auto,
    "www.moneycontrol.com": ex_auto,
    # China state / regional wires (English) -- Wave 4, GNews title-only;
    # ex_auto registered for consistency + the lede-rescue path.
    "english.news.cn": ex_auto,
    "chinadaily.com.cn": ex_auto,
    "www.chinadaily.com.cn": ex_auto,
    "globaltimes.cn": ex_auto,
    "www.globaltimes.cn": ex_auto,
    "scmp.com": ex_auto,
    "www.scmp.com": ex_auto,
    # Foreign-language outlets (ar / ru / zh / iw / es), added to SOURCE_NAMES in
    # waves B1-d and B2a/b but never to EXTRACTORS. The consumer in enrich.py
    # gates on `domain in EXTRACTORS`, so the omission did not raise anything -
    # it silently skipped the extractor for these 30 domains and fell through to
    # the meta description, which is a standfirst rather than a body.
    #
    # ex_auto is the honest choice for all of them: none needs bespoke selectors
    # (its container list covers the article shells these CMSs emit) and a
    # per-domain extractor invented without a measured page would be a guess
    # wearing a specific name.
    #
    # NOT here on purpose: themoscowtimes.com, the other SOURCE_NAMES entry with
    # no extractor. Its RSS carries full bodies, so the body never comes from a
    # page fetch and registering an extractor would suggest a path that is never
    # taken.
    "attaqa.net": ex_auto,
    "www.attaqa.net": ex_auto,
    "asharqbusiness.com": ex_auto,
    "www.asharqbusiness.com": ex_auto,
    "alarabiya.net": ex_auto,
    "www.alarabiya.net": ex_auto,
    "neftegaz.ru": ex_auto,
    "www.neftegaz.ru": ex_auto,
    "oilcapital.ru": ex_auto,
    "www.oilcapital.ru": ex_auto,
    "eprussia.ru": ex_auto,
    "www.eprussia.ru": ex_auto,
    "yicai.com": ex_auto,
    "www.yicai.com": ex_auto,
    "m.yicai.com": ex_auto,
    "finance.sina.com.cn": ex_auto,
    "jiemian.com": ex_auto,
    "www.jiemian.com": ex_auto,
    "m.jiemian.com": ex_auto,
    "globes.co.il": ex_auto,
    "www.globes.co.il": ex_auto,
    "themarker.com": ex_auto,
    "www.themarker.com": ex_auto,
    "calcalist.co.il": ex_auto,
    "www.calcalist.co.il": ex_auto,
    "eleconomista.com.mx": ex_auto,
    "www.eleconomista.com.mx": ex_auto,
    "ambito.com": ex_auto,
    "www.ambito.com": ex_auto,
    "portafolio.co": ex_auto,
    "www.portafolio.co": ex_auto,
    # =========================================================================
    # --- Wave 5 (2026-09-14) ---
    # ex_auto for every Wave 5 host named above. Almost all of these outlets are
    # title-only on the main scan path (GNews) or arrive with an RSS summary, so
    # the extractor powers the lede rescue and the snippet backfill - which is
    # exactly the path that was skipped for 30 domains before 2026-08-18,
    # because the gate is `resolve_extractor_domain(...) is not None`.
    #
    # ex_auto, not a bespoke extractor: none of these pages was measured by this
    # pass, and a per-site selector invented without a fetched page is a guess
    # wearing a specific name. Bespoke extractors are a separate, evidence-led
    # change.
    #
    # Every host named above is keyed literally, www. twin included, even
    # though resolve_extractor_domain would strip a "www." / "m." / "amp." /
    # "mobile." prefix and find the apex. That redundancy is the rule this file
    # already enforces (test_every_named_source_now_has_an_extractor_or_a_
    # stated_reason compares the two dicts key by key, not through the
    # resolver), and "www3.nhk.or.jp" shows why leaning on the resolver is not
    # enough anyway: "www3." is not one of the prefixes it knows.
    # --- Wave 5A: Global wires & US mainstream ---
    "apnews.com": ex_auto,
    "www.apnews.com": ex_auto,
    "ft.com": ex_auto,
    "www.ft.com": ex_auto,
    "wsj.com": ex_auto,
    "www.wsj.com": ex_auto,
    "nytimes.com": ex_auto,
    "www.nytimes.com": ex_auto,
    "washingtonpost.com": ex_auto,
    "www.washingtonpost.com": ex_auto,
    "economist.com": ex_auto,
    "www.economist.com": ex_auto,
    "bbc.com": ex_auto,
    "www.bbc.com": ex_auto,
    "bbc.co.uk": ex_auto,
    "www.bbc.co.uk": ex_auto,
    "theguardian.com": ex_auto,
    "www.theguardian.com": ex_auto,
    "thetimes.com": ex_auto,
    "www.thetimes.com": ex_auto,
    "thetimes.co.uk": ex_auto,
    "www.thetimes.co.uk": ex_auto,
    # resolved host seen in news_articles 2026-09-15 (job board, see SOURCE_NAMES)
    "appointments.thetimes.com": ex_auto,
    "telegraph.co.uk": ex_auto,
    "www.telegraph.co.uk": ex_auto,
    "news.sky.com": ex_auto,
    "www.news.sky.com": ex_auto,
    "politico.com": ex_auto,
    "www.politico.com": ex_auto,
    "politico.eu": ex_auto,
    "www.politico.eu": ex_auto,
    "axios.com": ex_auto,
    "www.axios.com": ex_auto,
    "forbes.com": ex_auto,
    "www.forbes.com": ex_auto,
    "fortune.com": ex_auto,
    "www.fortune.com": ex_auto,
    "businessinsider.com": ex_auto,
    "www.businessinsider.com": ex_auto,
    "markets.businessinsider.com": ex_auto,
    # resolved host seen in news_articles 2026-09-15 (Africa desk)
    "africa.businessinsider.com": ex_auto,
    "marketwatch.com": ex_auto,
    "www.marketwatch.com": ex_auto,
    "barrons.com": ex_auto,
    "www.barrons.com": ex_auto,
    "npr.org": ex_auto,
    "www.npr.org": ex_auto,
    "abcnews.com": ex_auto,
    "www.abcnews.com": ex_auto,
    "abcnews.go.com": ex_auto,
    # resolved hosts seen in news_articles 2026-09-15 (CDN / ingest hosts)
    "www-cdn.abcnews.com": ex_auto,
    "ingest.abcnews.com": ex_auto,
    "nbcnews.com": ex_auto,
    "www.nbcnews.com": ex_auto,
    "cbsnews.com": ex_auto,
    "www.cbsnews.com": ex_auto,
    "foxbusiness.com": ex_auto,
    "www.foxbusiness.com": ex_auto,
    # --- Wave 5B: US regional, Europe, Canada, Oceania mainstream ---
    "investing.com": ex_auto,
    "www.investing.com": ex_auto,
    "france24.com": ex_auto,
    "www.france24.com": ex_auto,
    "lemonde.fr": ex_auto,
    "www.lemonde.fr": ex_auto,
    "irishtimes.com": ex_auto,
    "www.irishtimes.com": ex_auto,
    "cityam.com": ex_auto,
    "www.cityam.com": ex_auto,
    "financialpost.com": ex_auto,
    "www.financialpost.com": ex_auto,
    "calgaryherald.com": ex_auto,
    "www.calgaryherald.com": ex_auto,
    "abc.net.au": ex_auto,
    "www.abc.net.au": ex_auto,
    "afr.com": ex_auto,
    "www.afr.com": ex_auto,
    "houstonchronicle.com": ex_auto,
    "www.houstonchronicle.com": ex_auto,
    "latimes.com": ex_auto,
    "www.latimes.com": ex_auto,
    "usatoday.com": ex_auto,
    "www.usatoday.com": ex_auto,
    "dw.com": ex_auto,
    "www.dw.com": ex_auto,
    "euronews.com": ex_auto,
    "www.euronews.com": ex_auto,
    "swissinfo.ch": ex_auto,
    "www.swissinfo.ch": ex_auto,
    "theglobeandmail.com": ex_auto,
    "www.theglobeandmail.com": ex_auto,
    "cbc.ca": ex_auto,
    "www.cbc.ca": ex_auto,
    "theaustralian.com.au": ex_auto,
    "www.theaustralian.com.au": ex_auto,
    "energynewsbulletin.net": ex_auto,
    "www.energynewsbulletin.net": ex_auto,
    "sodir.no": ex_auto,
    "www.sodir.no": ex_auto,
    # --- Wave 5C: Africa, Middle East & North-East Asia mainstream ---
    "allafrica.com": ex_auto,
    "www.allafrica.com": ex_auto,
    "businessday.ng": ex_auto,
    "www.businessday.ng": ex_auto,
    "punchng.com": ex_auto,
    "www.punchng.com": ex_auto,
    "thisdaylive.com": ex_auto,
    "www.thisdaylive.com": ex_auto,
    "businesslive.co.za": ex_auto,
    "www.businesslive.co.za": ex_auto,
    "businessday.co.za": ex_auto,
    "www.businessday.co.za": ex_auto,
    "theeastafrican.co.ke": ex_auto,
    "www.theeastafrican.co.ke": ex_auto,
    "africaoilgasreport.com": ex_auto,
    "www.africaoilgasreport.com": ex_auto,
    "energycapitalpower.com": ex_auto,
    "www.energycapitalpower.com": ex_auto,
    "english.alarabiya.net": ex_auto,
    "middleeasteye.net": ex_auto,
    "www.middleeasteye.net": ex_auto,
    "timesofisrael.com": ex_auto,
    "www.timesofisrael.com": ex_auto,
    "haaretz.com": ex_auto,
    "www.haaretz.com": ex_auto,
    "aa.com.tr": ex_auto,
    "www.aa.com.tr": ex_auto,
    "english.ahram.org.eg": ex_auto,
    "dailysabah.com": ex_auto,
    "www.dailysabah.com": ex_auto,
    "tehrantimes.com": ex_auto,
    "www.tehrantimes.com": ex_auto,
    "oilandgasmiddleeast.com": ex_auto,
    "www.oilandgasmiddleeast.com": ex_auto,
    "agbi.com": ex_auto,
    "www.agbi.com": ex_auto,
    "tradearabia.com": ex_auto,
    "www.tradearabia.com": ex_auto,
    "pipelineoilandgasnews.com": ex_auto,
    "www.pipelineoilandgasnews.com": ex_auto,
    # Article host of the feed above (see the SOURCE_NAMES comment): without
    # these keys the body fetch is skipped for every item the magazine
    # publishes, since the gate resolves the ARTICLE host, not the feed host.
    "energyconnects.com": ex_auto,
    "www.energyconnects.com": ex_auto,
    "asia.nikkei.com": ex_auto,
    "japantimes.co.jp": ex_auto,
    "www.japantimes.co.jp": ex_auto,
    "www3.nhk.or.jp": ex_auto,
    "koreaherald.com": ex_auto,
    "www.koreaherald.com": ex_auto,
    # --- Wave 5D: South & South-East Asia, Latin America mainstream, downstream trade press I ---
    "straitstimes.com": ex_auto,
    "www.straitstimes.com": ex_auto,
    "channelnewsasia.com": ex_auto,
    "www.channelnewsasia.com": ex_auto,
    "bangkokpost.com": ex_auto,
    "www.bangkokpost.com": ex_auto,
    "e.vnexpress.net": ex_auto,
    "timesofindia.indiatimes.com": ex_auto,
    "economictimes.indiatimes.com": ex_auto,
    "business-standard.com": ex_auto,
    "www.business-standard.com": ex_auto,
    "dawn.com": ex_auto,
    "www.dawn.com": ex_auto,
    "astanatimes.com": ex_auto,
    "www.astanatimes.com": ex_auto,
    "en.trend.az": ex_auto,
    "trend.az": ex_auto,
    "www.trend.az": ex_auto,
    "en.mercopress.com": ex_auto,
    "mercopress.com": ex_auto,
    "www.mercopress.com": ex_auto,
    "batimes.com.ar": ex_auto,
    "www.batimes.com.ar": ex_auto,
    "mexiconewsdaily.com": ex_auto,
    "www.mexiconewsdaily.com": ex_auto,
    "thejakartapost.com": ex_auto,
    "www.thejakartapost.com": ex_auto,
    "theedgemalaysia.com": ex_auto,
    "www.theedgemalaysia.com": ex_auto,
    "kaieteurnewsonline.com": ex_auto,
    "www.kaieteurnewsonline.com": ex_auto,
    "bnamericas.com": ex_auto,
    "www.bnamericas.com": ex_auto,
    "hydrocarbonprocessing.com": ex_auto,
    "www.hydrocarbonprocessing.com": ex_auto,
    "hydrocarbonengineering.com": ex_auto,
    "www.hydrocarbonengineering.com": ex_auto,
    # --- Wave 5E: O&G / refining / shipping trade press & institutions ---
    "lngindustry.com": ex_auto,
    "www.lngindustry.com": ex_auto,
    "worldpipelines.com": ex_auto,
    "www.worldpipelines.com": ex_auto,
    "tanksterminals.com": ex_auto,
    "www.tanksterminals.com": ex_auto,
    "drillingcontractor.org": ex_auto,
    "www.drillingcontractor.org": ex_auto,
    "rbnenergy.com": ex_auto,
    "www.rbnenergy.com": ex_auto,
    "fuelsandlubes.com": ex_auto,
    "www.fuelsandlubes.com": ex_auto,
    "energymonitor.ai": ex_auto,
    "www.energymonitor.ai": ex_auto,
    "shipandbunker.com": ex_auto,
    "www.shipandbunker.com": ex_auto,
    "seatrade-maritime.com": ex_auto,
    "www.seatrade-maritime.com": ex_auto,
    "kpler.com": ex_auto,
    "www.kpler.com": ex_auto,
    "eia.gov": ex_auto,
    "www.eia.gov": ex_auto,
    "offshore-mag.com": ex_auto,
    "www.offshore-mag.com": ex_auto,
    "gasprocessingnews.com": ex_auto,
    "www.gasprocessingnews.com": ex_auto,
    "qcintel.com": ex_auto,
    "www.qcintel.com": ex_auto,
    "lloydslist.com": ex_auto,
    "www.lloydslist.com": ex_auto,
    "rivieramm.com": ex_rivieramm,
    "www.rivieramm.com": ex_rivieramm,
    "mobilityplaza.com": ex_auto,
    "www.mobilityplaza.com": ex_auto,
    "iea.org": ex_auto,
    "www.iea.org": ex_auto,
    "opec.org": ex_auto,
    "www.opec.org": ex_auto,
    "woodmac.com": ex_auto,
    "www.woodmac.com": ex_auto,
    "rystadenergy.com": ex_auto,
    "www.rystadenergy.com": ex_auto,
    "vortexa.com": ex_auto,
    "www.vortexa.com": ex_auto,
    "oxfordenergy.org": ex_auto,
    "www.oxfordenergy.org": ex_auto,
    "icis.com": ex_auto,
    "www.icis.com": ex_auto,
    "jpt.spe.org": ex_auto,
}


# =============================================================================
# Scraper (sem cookies locais — container nao tem cookies/)
# =============================================================================

# Domains handled by a non-default HTTP path in fetch_html. Brasil Energia is
# routed through the authenticated session (brasilenergia_auth) which renews the
# be-auth cookie on expiry; if creds are absent it falls back to curl_cffi
# impersonation (anonymous, paywalled teasers only).
IMPERSONATE_DOMAINS = {"brasilenergia.com.br", "www.brasilenergia.com.br"}

# Tentamos perfis em ordem decrescente de novidade. curl_cffi >= 0.8 traz
# chrome136/chrome131; versoes mais antigas so tem ate chrome124. O loop
# trata ValueError (perfil desconhecido) e erros de rede da mesma forma.
_IMPERSONATE_PROFILES = ("chrome136", "chrome131", "chrome124")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Referer": "https://www.google.com/",
}


def _get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


#: Host prefixes that denote the SAME publication rather than a different one.
_HOST_PREFIXES = ("www.", "m.", "amp.", "mobile.")


def resolve_extractor_domain(domain: str) -> str | None:
    """Return the EXTRACTORS key for a host, or None if the host is unregistered.

    Registration is per literal host string, so `m.yicai.com` and `www.yicai.com`
    each need their own entry or they are treated as unknown outlets. The
    registry cannot keep up with that by hand - every wave has shipped at least
    one host variant nobody thought of - so the lookup normalises instead:
    exact host, then the host with a `www.`/`m.`/`amp.`/`mobile.` prefix removed,
    then the `www.` form of that.

    This must be used by every EXTRACTORS lookup, INCLUDING the membership gate
    in `enrich.py`. A fallback that lives only inside `_extract` is dead code:
    the caller's `domain in EXTRACTORS` test rejects the variant first, and the
    fix never runs.
    """
    if not domain:
        return None
    host = domain.lower()
    if host in EXTRACTORS:
        return host
    for prefix in _HOST_PREFIXES:
        if host.startswith(prefix):
            stripped = host[len(prefix):]
            if stripped in EXTRACTORS:
                return stripped
            if f"www.{stripped}" in EXTRACTORS:
                return f"www.{stripped}"
            break
    return None


def _cffi_get(url: str, timeout: int) -> "cffi_requests.Response":
    """Tenta impersonation em ordem decrescente de novidade."""
    last_exc: Exception = RuntimeError("no profile tried")
    for profile in _IMPERSONATE_PROFILES:
        try:
            resp = cffi_requests.get(
                url, headers=DEFAULT_HEADERS, timeout=timeout, impersonate=profile,
            )
            return resp
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    raise last_exc


def fetch_html(url: str, timeout: int = 25) -> str:
    domain = _get_domain(url)
    # Brasil Energia is a subscriber paywall: fetch it through the authenticated
    # session, which auto-renews the be-auth cookie on expiry. If credentials are
    # absent or login fails, get() returns None and we raise so the caller's
    # try/except skips this URL gracefully (the rest of the scan keeps running).
    if domain in IMPERSONATE_DOMAINS:
        from .brasilenergia_auth import get_auth
        auth = get_auth()
        if auth is not None:
            resp = auth.get(url, timeout=timeout)
            if resp is None:
                raise RuntimeError(f"Brasil Energia: no authenticated response for {url}")
            resp.raise_for_status()
            return resp.text
        # No credentials configured — fall back to impersonation (anonymous,
        # paywalled, but better than nothing for public teasers).
        resp = _cffi_get(url, timeout)
        resp.raise_for_status()
        return resp.text
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    if resp.status_code == 403:
        resp = _cffi_get(url, timeout)
    resp.raise_for_status()
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


# =============================================================================
# Limpeza de titulo e paragrafos
# =============================================================================

_SITE_SUFFIX_PATTERNS = [
    re.compile(r"\s*[\|\u2013\-]\s*" + re.escape(name) + r"\s*$", re.IGNORECASE)
    for name in set(SOURCE_NAMES.values()) | {
        "Valor", "Valor Economico", "Folha", "Estadao", "O Estado de S.Paulo",
        "Brasil Energia - Petróleo e Gás", "oglobo", "Agencia Petrobras",
        "Agencia iNFRA", "Metropoles", "Poder 360", "Visao Agro", "Bloomberg Linea",
    }
]


def clean_title(titulo: str) -> str:
    t = re.sub(r"\s+", " ", titulo).strip()
    changed = True
    while changed:
        changed = False
        for pat in _SITE_SUFFIX_PATTERNS:
            new = pat.sub("", t).strip()
            if new != t and new:
                t = new
                changed = True
    return t


# 2026-10-16 c (round 8): family 4, a relative-age line ("3 months ago", "1
# month ago", "an hour ago"). Listed in _NOISE_PATTERNS below AND read by the
# age-run rule (_RELATIVE_AGE_RE). Byte-identical with RELATIVE_AGE_PATTERN in
# src/lib/clipping/clean.ts (dashboard repo).
RELATIVE_AGE_PATTERN = r"^\s*(\d+|an?)\s+(seconds?|minutes?|mins?|hours?|hrs?|days?|weeks?|months?|years?)\s+ago\s*$"

_NOISE_PATTERNS = [
    r"^\s*leia\s+(tamb[eé]m|mais|tudo\s+sobre)\b",
    r"^\s*leia\s+a\s+(reportagem|mat[eé]ria)\s+completa\b",
    r"^\s*continua\s+(ap[oó]s|depois)\s+(a|da)\s+publicidade",
    r"^\s*assine\b",
    r"^\s*assinar\b",
    r"^\s*publicidade\s*$",
    r"^\s*propaganda\s*$",
    r"^\s*anuncio\s*$",
    r"^\s*newsletter\b",
    r"^\s*siga\s+o\s+",
    r"^\s*siga\s+a\s+",
    r"^\s*assista\b",
    r"^\s*foto:\s",
    r"^\s*imagem:\s",
    r"^\s*cr[eé]dito:\s",
    r"^\s*compartilhe\b",
    r"^\s*veja\s+(tamb[eé]m|mais)\b",
    r"^\s*saiba\s+mais\b",
    r"^\s*por\s+[A-ZÁÉÍÓÚÂÊÔÃÕÇ][\wÀ-ÿ\.\-]+(\s+[A-ZÁÉÍÓÚÂÊÔÃÕÇ][\wÀ-ÿ\.\-]+){0,3}\s*$",
    r"j[aá]\s+[eé]\s+assinante\b",
    r"fa[cç]a\s+seu\s+login\b",
    r"continue\s+lendo\b",
    r"nosso\s+conte[uú]do\s+[eé]\s+exclusivo",
    r"conte[uú]do\s+exclusivo\s+para\s+assinantes",
    r"voc[eê]\s+atingiu\s+o\s+limite",
    r"tr[eê]s\s+mat[eé]rias\s+por\s+m[eê]s",
    r"apoie\s+o\s+jornalismo",
    r"acesse\s+sem\s+limites",
    r"acompanhe\s+os\s+mercados\s+com\s+nossas\s+ferramentas",
    r"tenha\s+acesso\s+a\s+informa[cç][aã]o\s+relevante",
    r"voc[eê]\s+pode\s+ler\s+nosso\s+conte[uú]do\s+exclusivo",
    r"cadastro\s+gratuito",
    r"assine\s+as?\s+newsletters?\b",
    r"receba\s+as?\s+not[ií]cias\s+do\s+dia",
    r"em\s+primeira\s+m[aã]o\s+no\s+e-?mail",
    r"^\s*[\u27f6\u2192\u2794\u279c\u25ba\u25b8\u2023\u00bb]\s*",
    r"^\s*[\u00a9\u00ae]?\s*\d{4}\s+bloomberg\b",
    r"^\s*todos\s+os\s+direitos\s+reservados",
    r"^\s*[\wÀ-ÿ][\wÀ-ÿ\s&'-]{1,40}\s*\|\s*[\wÀ-ÿ][\wÀ-ÿ\s&'-]{1,40}\s*$",
    # 2026-10-14 login / registration CTAs (shared contract with
    # src/lib/clipping; O Globo AMP barrier). Anchored or end-anchored so that
    # prose such as "Quem já possui conta no banco..." or "faça login com sua
    # conta gov.br e siga..." survives.
    r"^\s*j[aá]\s+(possui|tem)\s+(uma\s+)?conta\s*\?",
    r"\bfa[cç]a\s+(o\s+|seu\s+)?login\s*[.!]?\s*$",
    r"\blogin\s+aqui\s*[.!]?\s*$",
    r"acessar\s+a\s+conta\s+da\s+sua\s+assinatura",
    r"^\s*cadastre-se\b",
    r"^\s*crie\s+(sua|uma)\s+conta\b",
    r"^\s*esqueci\s+(a\s+|minha\s+)?senha\b",
    r"^\s*(entrar|login|log\s*in|sign\s*in|iniciar\s+sess[aã]o)\s*[.!]?\s*$",
    r"^\s*already\s+have\s+an\s+account\b",
    r"^\s*create\s+(an|your|a\s+free)\s+account\b",
    r"^\s*¿?\s*ya\s+tienes\s+(una\s+)?cuenta\b",
    r"^\s*inicia(r)?\s+sesi[oó]n\b",
    r"^\s*(conte[uú]do\s+)?exclusivo\s+para\s+assinantes\b",
    # 2026-10-16 Eixos (shared contract with src/lib/clipping): Google
    # "preferred sources" banner, newsletter-edition chrome and the
    # AI-translation note. Anchored / label forms so that prose such as
    # "O Google News mudou o algoritmo...", "Apresentado por executivos da
    # Petrobras, o plano..." and "Nesta edição do relatório anual..." survives.
    r"fontes?\s+preferidas?\s+(do|no)\s+google",
    r"^\s*(coloque|adicione|salve|inclua)\s+(o|a)\s+\S+\s+(nas|entre)\s+(suas|as\s+suas)\s+fontes",
    r"not[ií]cias\s+com\s+mais\s+destaque\s+nas\s+suas\s+buscas",
    r"^\s*(siga|acompanhe|adicione)\b[^.]{0,80}\bgoogle\s+(news|not[ií]cias|discover)\b",
    r"^\s*nesta\s+edi[cç][aã]o\s*[.:\-–—]",
    r"^\s*edi[cç][aã]o\s+(apresentada|oferecida|patrocinada)\s+por\b",
    r"^\s*(apresentad[oa]|oferecid[oa]|patrocinad[oa])\s+por\s*:?\s*\S{0,60}$",
    r"^\s*\*?\s*conte[uú]do\s+traduzido\s+com\s+aux[ií]lio\s+de\s+intelig[eê]ncia\s+artificial",
    r"^\s*comece\s+seu\s+dia\s*[.!:]?\s*$",
    # 2026-10-16 full-registry sweep (shared contract with src/lib/clipping):
    # chrome long enough to escape the short-paragraph list below - the AI
    # translation note (theagribiz), the UOL comment disclaimer, the france24
    # ad-tracking video placeholder and the Opera Mundi "follow us" line.
    r"translated\s+.{0,60}with\s+the\s+(assistance|help)\s+of\s+artificial\s+intelligence",
    r"respons[aá]vel\s+pelo\s+coment[aá]rio",
    r"you\s+must\s+enable\s+advertisement\s+tracking|blocking\s+the\s+video\s+player"
    r"|to\s+display\s+this\s+content\s+from",
    r"^\s*(a\s+)?cobertura\s+no\s+google,\s+no\s+e-?mail\s+e\s+nas\s+redes",
    # 2026-10-16 c (round 8): family 4 (globalenergynetwork "latest news" rail;
    # shared contract with src/lib/clipping, same order). An unrendered
    # WordPress shortcode line ("[mc4wp_form id=2073]", "[/vc_row]",
    # '[contact-form-7 id="123"]') and the relative-age line
    # (RELATIVE_AGE_PATTERN above). Any-length list on purpose: both are
    # anchored at ^ AND $ over a closed shape. The shortcode is an identifier
    # plus optional key=value attributes, so bracketed prose ("[The company
    # declined to comment.]") survives.
    r"^\s*\[\/?[a-z][a-z0-9_\-]*(\s+[a-z0-9_\-]+\s*=[^\]]*)?\]\s*$",
    RELATIVE_AGE_PATTERN,
]
_NOISE_REGEX = re.compile("|".join(_NOISE_PATTERNS), re.IGNORECASE)

#: Short CTA / widget / label lines (2026-10-16 full-registry sweep; shared
#: contract with src/lib/clipping). Tested ONLY on paragraphs under
#: SHORT_NOISE_MAX_CHARS: a short "Subscribe here" is chrome, a long sentence
#: that mentions subscribers is prose.
# DELIBERATE DIVERGENCE: src/lib/clipping exempts lines carrying a PAYWALL_MARKERS phrase
# (a dropped gate CTA turned a paywall shell into `partial` there); the shim has no marker
# list and feeds only bodies/snippets, so dropping a gate CTA is harmless. Lengths are code
# points on both sides (Python len() == TS Array.from(p).length).
SHORT_NOISE_MAX_CHARS = 160
_SHORT_NOISE_PATTERNS = [
    r"\b(subscribe\s+(now|here|today|to\s+(continue|get|receive|read|unlock|gift|our\s+newsletter|the\s+newsletter))|sign\s*up(\s+(for|to|now|here|today)|(?=\s*[.!…]?\s*$))|download\s+the\s+\S+(\s+\S+)?\s+app|gift\s+(this\s+)?articles?|gifted\s+to\s+you)\b",
    r"\b(canal|grupo|channel|join|entre\s+n[oa]|siga|follow|receba|no\s+seu)\b.{0,40}\b(whatsapp|telegram)\b|\b(whatsapp|telegram)\b.{0,40}\b(canal|grupo|channel|clique|click|aqui|here)\b",
    r"\b(receba|siga|acompanhe|assine|inscreva-se|cadastre-se)\b.{0,40}\b(not[ií]cias|newsletters?|boletim|nosso\s+canal|canal\s+(do|no|de)\s+(whatsapp|telegram|youtube))\b",
    r"\bfique\s+(por\s+dentro|informado)\b",
    r"\b(stay\s+in\s+the\s+know|subscribe\s+here)\b|^\s*(join\s+now|become\s+a\s+member)\b",
    r"\bfontes?\s+preferidas?\b|\bcoverage\s+in\s+your\s+search\s+results\b",
    # 2026-10-19: Google preferred-source CTA (City AM: SVG text in an unnamed <a>, no class to catch).
    r"\badd\s+(us\s+)?as\s+a\s+preferred\s+source\b|\bpreferred\s+source\s+on\s+google\s*[.!]?\s*$",
    r"^\s*(leave\s+a\s+comment|back\s+to\s+top(\s+button)?|fetching\s+latest(\s+articles)?|more\s+info|watch\s+now|more\s+top\s+reads(\s+from\s+\S+)?|advt|advertisement)\s*[.!]?\s*$|^\s*what\s+i\s+cover\b",
    r"^\s*disclaimer\s*:?\s*$",
    r"\bto\s+contact\s+the\s+editor\b",
    r"^\s*\S+(\s+\S+)?\s*:\s*$",
    r"^\s*por\s*$",
    r"/autor/",
    r"^\s*(©|copyright\b|\(c\)\s*copyright\b|\(c\)(?=\s*\d{4})).{0,60}\b\d{4}\b|\ball\s+rights\s+reserved\b",
    r"^\s*published\s+in\s+[A-Z][^,]{1,40},\s+\w+\s+\d{1,2}(st|nd|rd|th)?,?\s+\d{4}\s*\.?\s*$",
    r"^\s*\(?\s*(foto|photo|image|imagem)(\s+do\s+destaque)?\s*:",
    # 2026-10-16 c (round 8; shared contract with src/lib/clipping, same
    # order). Family 1, newsletter/social/podcast pitch lines (astanatimes
    # "... stories sent directly to you! ...", 148 code points; cityam "Three
    # daily briefings with ..."; energymonitor "make sure to subscribe",
    # "wherever you get your podcasts"). "daily briefings" is COUNT-led only:
    # "The White House held daily briefings" survives.
    r"\b(stories|news|articles|updates|headlines)\s+sent\s+directly\s+to\s+(you|your\s+inbox)\b|\bmake\s+sure\s+to\s+subscribe\b|\bsubscribe\s+to\s+our\s+(x|facebook|instagram|youtube|podcasts?|channel)\b|\bwherever\s+you\s+get\s+your\s+podcasts\b|^\s*(one|two|three|four|five|\d+)\s+daily\s+briefings?\b",
    # Family 2, advertiser/house promo (GlobalData "... editorial excellence to
    # reach engaged professionals across 36 leading media platforms."); "reach
    # 5,000 professionals across Brazil" is prose and survives.
    r"\b(editorial\s+excellence\s+to\s+reach|reach\s+engaged\s+professionals|across\s+\d+\s+leading\s+media\s+platforms)\b",
    # Family 8, a photo credit at the END of a caption ("... - (Foto: Daniel
    # Torok/Official White House).") and the "Photo credit: X" label.
    r"\(\s*(foto|photo|imagem|image)\s*:[^)]{1,80}\)\s*\.?\s*$|^\s*(photo|foto|image|imagem)\s+(credits?|cr[eé]ditos?)\s*:",
    # Family 7, a bare role line of the author box ("Middle East correspondent").
    # Whole line, ASCII letters and spaces only (`[a-z\s]` under IGNORECASE, NOT
    # `\w`: Python `\w` is Unicode and JS `\w` is ASCII, so "São Paulo
    # correspondent" would split the two sides; with `[a-z]` both keep it).
    # "John Smith, Middle East correspondent" (comma) survives; the (?!\s*by\s)
    # guard keeps "By Gary Dixon Middle East correspondent".
    r"^(?!\s*by\s)\s*[a-z\s]{0,40}\b(correspondent|editor|reporter|columnist)\s*$",
]
_SHORT_NOISE_REGEX = re.compile("|".join(_SHORT_NOISE_PATTERNS), re.IGNORECASE)

#: A short line led by a pictograph / dingbat / geometric glyph ("🔷 Canal 562
#: ClaroTV+", "▶ Assista ao vídeo") is a promo or channel list, not prose -
#: unless it ends like prose (the terminal class of the TS isProseParagraph:
#: . ! ? ellipsis, closing guillemet/quote/paren/bracket, CJK full stops and
#: closing brackets) - a punctuated bullet is kept. Escapes only, no literal glyphs.
GLYPH_LED_MAX_CHARS = 120
_GLYPH_LED_RE = re.compile(r"^\s*[\U0001F300-\U0001FAFF\u2600-\u27BF\u25A0-\u25FF]")
_SENTENCE_END_RE = re.compile(r"[.!?\u2026\u00bb\u201d\u0022\u0027)\]]$|[\u3002\uff01\uff1f\u300d\u300f]$")

# Date-only paragraph test (shared contract with src/lib/clipping and
# scripts/clipping/coverage_probe.py in the dashboard repo). A dateline such as
# "13/10/2026 09h01 Atualizado 13/10/2026" is chrome, not the article.
# Python `re` \b/\d/\s are Unicode-aware while JS without the `u` flag is
# ASCII-only; the two sides differ only on edge cases (e.g. `10há`), accepted.
DATE_ONLY_MAX_CHARS = 80
_DATE_TOKEN_RE = re.compile(
    r"\b(?:\d{1,2}[\/.\-]\d{1,2}[\/.\-]\d{2,4}|\d{4}-\d{2}-\d{2}"
    r"|\d{1,2}\s+de\s+[a-zç]+\s+de\s+\d{4}|\d{1,2}\s+[a-zç]{3,9}\.?\s+\d{4}"
    r"|[a-z]{3,9}\.?\s+\d{1,2},?\s+\d{4})\b",
    re.IGNORECASE,
)
_TIME_TOKEN_RE = re.compile(
    r"\b\d{1,2}(?::|h)\d{2}(?:h|min|\s*[ap]\.?m\.?)?\b|\b\d{1,2}\s*h\b",
    re.IGNORECASE,
)
_DATE_SEPARATORS_RE = re.compile(r"[\s,.;:|•·–—\-()\/]+")
_DATE_WORDS = frozenset("""
    publicado publicada atualizado atualizada publicação publicacao atualização
    atualizacao updated update published modificado modificada modified last em
    às as at on h hs hrs por de do da data date time hora horas min gmt utc brt
    et est pt pst horário horario brasília brasilia segunda terça terca quarta
    quinta sexta sábado sabado domingo feira monday tuesday wednesday thursday
    friday saturday sunday lunes martes miércoles miercoles jueves viernes
""".split())
_DEDUPE_TRAIL_RE = re.compile(r"[\s.,;:!?…»\"”“'\-]+$")


def is_date_only_paragraph(p: str) -> bool:
    """True for a dateline/timestamp paragraph with no prose in it."""
    text = p.strip()
    if not text or len(text) > DATE_ONLY_MAX_CHARS:
        return False
    if not (_DATE_TOKEN_RE.search(text) or _TIME_TOKEN_RE.search(text)):
        return False
    rest = _TIME_TOKEN_RE.sub(" ", _DATE_TOKEN_RE.sub(" ", text))
    return all(
        not tok or tok.lower() in _DATE_WORDS for tok in _DATE_SEPARATORS_RE.split(rest)
    )


def _dedupe_key(p: str) -> str:
    return _DEDUPE_TRAIL_RE.sub("", re.sub(r"\s+", " ", p.lower())).strip()


# Age-run labels (2026-10-16 c, round 8 family 4; byte-identical spec with
# ageRunLabelIndexes in src/lib/clipping/clean.ts). A "latest news" rail
# flattened into the body (globalenergynetwork, Readability path): bare tags
# ("Middle East", "North America") between relative-age lines ("3 months ago").
# No region/category word list: the rule is structural.
# Input: the paragraph list AFTER normalisation (empties removed) and BEFORE
# any drop.
#   AGE line   = matches _RELATIVE_AGE_RE (case-insensitive).
#   LABEL line = not an AGE line, 1..AGE_RUN_LABEL_MAX_WORDS (3) whitespace-
#                separated words, under AGE_RUN_LABEL_MAX_CHARS (40) code
#                points, and NOT ending in _SENTENCE_END_RE.
# Every maximal run of consecutive AGE/LABEL lines that holds at least
# AGE_RUN_MIN_AGE_LINES (2) AGE lines has each of its LABEL lines dropped; the
# AGE lines go through _NOISE_REGEX anyway. A byline next to ONE age line
# ("Dipali Banka" / "2 hours ago") is not a run.
AGE_RUN_LABEL_MAX_WORDS = 3
AGE_RUN_LABEL_MAX_CHARS = 40
AGE_RUN_MIN_AGE_LINES = 2
_RELATIVE_AGE_RE = re.compile(RELATIVE_AGE_PATTERN, re.IGNORECASE)
_WORD_SPLIT_RE = re.compile(r"\s+")


def _words(p: str) -> list[str]:
    return [w for w in _WORD_SPLIT_RE.split(p) if w]


def _age_run_label_indexes(paragraphs: list[str]) -> set[int]:
    """Indexes (into `paragraphs`) of the LABEL lines that sit in an age run."""
    drop: set[int] = set()
    run_labels: list[int] = []
    run_ages = 0
    for i, p in enumerate(paragraphs):
        if _RELATIVE_AGE_RE.search(p):
            run_ages += 1
            continue
        words = len(_words(p))
        if (1 <= words <= AGE_RUN_LABEL_MAX_WORDS
                and len(p) < AGE_RUN_LABEL_MAX_CHARS
                and not _SENTENCE_END_RE.search(p)):
            run_labels.append(i)
            continue
        if run_ages >= AGE_RUN_MIN_AGE_LINES:
            drop.update(run_labels)
        run_labels, run_ages = [], 0
    if run_ages >= AGE_RUN_MIN_AGE_LINES:
        drop.update(run_labels)
    return drop


# Section-label first paragraph (2026-10-16 c, round 8 family 6; byte-identical
# spec with isSectionLabelParagraph in src/lib/clipping/clean.ts): "In-brief
# analysis" (eia.gov), "Summary" (livemint), "EUA" (diariodopoder) opening a
# Readability body. Applied ONCE, after the per-paragraph tests and before the
# global dedupe, to the FIRST surviving paragraph, and only when at least one
# other paragraph survives (a lone paragraph is never dropped). It is dropped
# when it has 1..SECTION_LABEL_MAX_WORDS (3) whitespace-separated words, does
# NOT end in _SENTENCE_END_RE, and is NOT name-shaped. Name-shaped = 2+ words
# that ALL start with an uppercase character (c != c.lower()): a bare byline
# ("Luan Carlos", "By Tsvetana Paraskova", "Da Redação") stays. Known cost: a
# Title Case label ("Oil Markets") is name-shaped and stays too.
SECTION_LABEL_MAX_WORDS = 3


def _starts_uppercase(word: str) -> bool:
    c = word[0]
    return c != c.lower()


def is_section_label_paragraph(p: str) -> bool:
    """True when a (first) paragraph is a bare section label (see above)."""
    words = _words(p)
    if not 1 <= len(words) <= SECTION_LABEL_MAX_WORDS:
        return False
    if _SENTENCE_END_RE.search(p.strip()):
        return False
    name_shaped = len(words) >= 2 and all(_starts_uppercase(w) for w in words)
    return not name_shaped


def clean_paragraphs(paragraphs: list[str]) -> list[str]:
    # Same order as cleanParagraphs in src/lib/clipping/clean.ts (2026-10-16 c).
    # Pass 1 - normalise every paragraph (whitespace collapse, punctuation fix;
    # empties removed). The age-run rule reads the whole normalised sequence
    # before anything is dropped. (The TS side also strips zero-width
    # characters and splits inline "Leia também" markers here; the shim never
    # did either.)
    normalised: list[str] = []
    for p in paragraphs:
        p = re.sub(r"\s+", " ", p).strip()
        p = re.sub(r"\s+([.,;:!?])", r"\1", p)
        if p:
            normalised.append(p)
    age_run_labels = _age_run_label_indexes(normalised)

    # Pass 2 - per-paragraph tests, in order.
    out: list[str] = []
    for index, p in enumerate(normalised):
        if _NOISE_REGEX.search(p):
            continue
        if len(p) < SHORT_NOISE_MAX_CHARS and _SHORT_NOISE_REGEX.search(p):
            continue
        if (len(p) < GLYPH_LED_MAX_CHARS and _GLYPH_LED_RE.search(p)
                and not _SENTENCE_END_RE.search(p.strip())):
            continue
        if is_date_only_paragraph(p):
            continue
        if index in age_run_labels:
            continue
        out.append(p)
    # Section-label first paragraph (2026-10-16 c): once, on the first survivor.
    if len(out) >= 2 and is_section_label_paragraph(out[0]):
        out.pop(0)
    # Global keep-first dedupe: an AMP barrier repeats the same CTA pair
    # 18 times, interleaved, which a consecutive-only check never catches.
    seen: set[str] = set()
    deduped: list[str] = []
    for p in out:
        key = _dedupe_key(p)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return deduped


def _json_ld_article_body(soup: BeautifulSoup) -> list[str]:
    """Paragraphs from a schema.org `articleBody`, if the page publishes one.

    Selector extraction assumes the body is in the markup. A growing share of
    the registry ships an empty article shell and puts the prose in structured
    data instead, where every selector in `ex_auto` misses it and the caller
    falls through to the meta description - a standfirst presented as a body.

    Splitting on blank lines is the shape publishers actually emit; a body that
    arrives as one unbroken string stays one paragraph rather than being cut at
    invented boundaries.
    """
    for script in soup.find_all("script", attrs={"type": True}):
        if "ld+json" not in (script.get("type") or "").lower():
            continue
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                body = node.get("articleBody")
                if isinstance(body, str) and body.strip():
                    parts = [p.strip() for p in re.split(r"\n\s*\n|\n", body) if p.strip()]
                    if parts:
                        return parts
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
    return []


def _extract(html: str, domain: str) -> tuple[str, list[str]]:
    soup = BeautifulSoup(html, "lxml")
    key = resolve_extractor_domain(domain)
    if key is None:
        raise KeyError(domain)
    extractor = EXTRACTORS[key]
    titulo, paragrafos = extractor(soup)
    cleaned = clean_paragraphs(paragrafos)
    if not cleaned:
        cleaned = clean_paragraphs(_json_ld_article_body(soup))
    return clean_title(titulo), cleaned
