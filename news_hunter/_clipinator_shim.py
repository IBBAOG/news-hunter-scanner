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
    "www.correiobraziliense.com.br": "Correio Braziliense",
    "correiobraziliense.com.br": "Correio Braziliense",
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
    "www.theedgesingapore.com": "The Edge Singapore",
    "www12.senado.leg.br": "Senado Federal",
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
    "www.terra.com.br": "Terra",
    "terra.com.br": "Terra",
    "www.moneytimes.com.br": "Money Times",
    "moneytimes.com.br": "Money Times",
    "visnoinvest.com.br": "Visno Invest",
    "www.visnoinvest.com.br": "Visno Invest",
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
)


def _strip_noise(container: Tag) -> None:
    for tag in container.find_all(["figure", "figcaption", "aside", "script", "style", "iframe", "form", "nav"]):
        tag.decompose()
    for el in list(container.find_all(True)):
        if el.attrs is None or el.parent is None:
            continue
        classes = el.get("class") or []
        idv = el.get("id") or ""
        combined = " ".join(list(classes) + [idv]).lower()
        if any(sub in combined for sub in NOISE_CLASS_SUBSTRINGS):
            el.decompose()


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
ex_agenciainfra = _make_extractor(["div.entry-content", "article .post-content", "article"])
ex_braziljournal = _make_extractor([
    "div.post-content-text", "section.post-content", "div.entry-content", "article",
])
ex_eixos = _make_extractor([
    "div.entry-content", "div.post-content", "article .tdb-block-inner", "article",
])
ex_monitormercantil = _make_extractor(["div.td-post-content", "div.entry-content", "article"])
ex_timesbrasil = _make_extractor(["div.article-content", "div.entry-content", "article"])
ex_visaoagro = _make_extractor(["div.entry-content", "div.post-content", "article"])

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
    "veronoticias.com": ex_auto,
    "www.veronoticias.com": ex_auto,
    "diariodopoder.com.br": ex_auto,
    "www.diariodopoder.com.br": ex_auto,
    "www.conjur.com.br": ex_auto,
    "conjur.com.br": ex_auto,
    "www.argusmedia.com": ex_auto,
    "argusmedia.com": ex_auto,
    "operamundi.uol.com.br": ex_auto,
    "claudiodantas.com.br": ex_auto,
    "www.claudiodantas.com.br": ex_auto,
    "br.tradingview.com": ex_auto,
    "www.theedgesingapore.com": ex_auto,
    "www12.senado.leg.br": ex_auto,
    "edition.cnn.com": ex_auto,
    "www.cnn.com": ex_auto,
    "clickpetroleoegas.com.br": ex_auto,
    "www.clickpetroleoegas.com.br": ex_auto,
    "ineep.org.br": ex_auto,
    "www.ineep.org.br": ex_auto,
    "tconline.com.br": ex_auto,
    "www.tconline.com.br": ex_auto,
    "obastidor.com.br": ex_auto,
    "www.obastidor.com.br": ex_auto,
    "noticias.uol.com.br": ex_auto,
    "www.terra.com.br": ex_auto,
    "terra.com.br": ex_auto,
    "www.moneytimes.com.br": ex_auto,
    "moneytimes.com.br": ex_auto,
    "visnoinvest.com.br": ex_auto,
    "www.visnoinvest.com.br": ex_auto,
}


# =============================================================================
# Scraper (sem cookies locais — container nao tem cookies/)
# =============================================================================

IMPERSONATE_DOMAINS = {"brasilenergia.com.br", "www.brasilenergia.com.br"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Referer": "https://www.google.com/",
}


def _get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def fetch_html(url: str, timeout: int = 25) -> str:
    domain = _get_domain(url)
    if domain in IMPERSONATE_DOMAINS:
        resp = cffi_requests.get(
            url, headers=DEFAULT_HEADERS, timeout=timeout, impersonate="chrome124",
        )
        resp.raise_for_status()
        return resp.text
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    if resp.status_code == 403:
        resp = cffi_requests.get(
            url, headers=DEFAULT_HEADERS, timeout=timeout, impersonate="chrome124",
        )
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
]
_NOISE_REGEX = re.compile("|".join(_NOISE_PATTERNS), re.IGNORECASE)


def clean_paragraphs(paragraphs: list[str]) -> list[str]:
    out: list[str] = []
    for p in paragraphs:
        p = re.sub(r"\s+", " ", p).strip()
        p = re.sub(r"\s+([.,;:!?])", r"\1", p)
        if not p:
            continue
        if _NOISE_REGEX.search(p):
            continue
        out.append(p)
    dedup: list[str] = []
    for p in out:
        if not dedup or dedup[-1] != p:
            dedup.append(p)
    return dedup


def _extract(html: str, domain: str) -> tuple[str, list[str]]:
    soup = BeautifulSoup(html, "lxml")
    extractor = EXTRACTORS[domain]
    titulo, paragrafos = extractor(soup)
    return clean_title(titulo), clean_paragraphs(paragrafos)
