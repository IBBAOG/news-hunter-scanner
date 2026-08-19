"""Coleta paralela de RSS/Atom feeds + sitemaps Google News."""
from __future__ import annotations

import calendar
import logging
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import feedparser
import requests
from dateutil import parser as date_parser

from .sources import (
    LANGUAGES,
    NO_RSS_DOMAINS,
    all_homepage_scrapers,
    all_rss_feeds,
    all_standard_sitemaps,
    feed_stale_hours,
    google_news_queries,
    google_news_site_queries,
    google_news_site_queries_lang,
    is_sitemap_url,
)
from .store import normalize_url

log = logging.getLogger(__name__)

# Timeouts curtos. FEED_TIMEOUT limita cada requisicao individual;
# COLLECT_DEADLINE e o teto GLOBAL - depois disso, feeds ainda em voo sao
# abandonados para esta busca e ficam para a proxima.
FEED_TIMEOUT = 4
# Sitemaps WordPress sao lentos e PESADOS: a pagina de posts do visaoagro tem
# 484 KB e o Apache dele nao comprime (sem content-encoding), entao o custo e
# real, nao latencia de handshake. Medido em 2026-08-04, requests em serie:
# visaoagro 1.9-5.8s no indice + 3.0-4.1s na pagina; istoedinheiro 4.9s + 1.8s.
STANDARD_SITEMAP_TIMEOUT = 10
# Teto GLOBAL. Precisa acomodar o pior caso do caminho MAIS LENTO, que e o
# sitemap WordPress paginado: DOIS requests em serie (indice -> pagina de
# posts), logo 2 x STANDARD_SITEMAP_TIMEOUT = 20s. Com o deadline antigo de 12s
# uma coleta fria de visaoagro (9.9s medidos) passava raspando — e trocar um
# zero silencioso por um zero-por-deadline nao e conserto nenhum.
# O custo desse teto so e pago quando alguma fonte esta de fato lenta:
# as_completed drena os feeds a medida que terminam.
COLLECT_DEADLINE = 22.0  # homepage scrapers via curl_cffi levam 6-7s; sitemap paginado, ate 20s

# Segmentos de path que indicam paginas de listagem/categoria, nao artigos.
_NON_ARTICLE_SEGMENTS = frozenset({
    "category", "tag", "tags", "colunista", "dados", "autor", "author",
    "page", "search", "busca", "feed", "amp", "print", "rss",
})

# User-Agent padrao (feedparser usa urllib; passamos via agent=)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class RawItem:
    url: str
    title: str
    summary: str
    published_at: datetime | None
    source_domain: str  # dominio da noticia real (nao do feed)
    feed_domain: str    # dominio do feed que trouxe esse item
    # --- Hints de listagem (homepage scrapers) -------------------------------
    # Preenchidos por _scrape_homepage a partir do proprio HTML da listagem:
    # o texto do link (manchete) e a data impressa ao lado dele.
    #
    # Sao HINTS, nao dados canonicos: ficam FORA de `title`/`published_at` de
    # proposito. Preencher aqueles dois faria `enrich_item(need_snippet=False)`
    # concluir que o item ja esta completo e pular o fetch da pagina — e com
    # isso perderiamos snippet e hora exata em toda fonte raspada. Aqui eles
    # servem so como (a) filtro barato de janela antes de qualquer fetch e
    # (b) fallback honesto quando o enriquecimento falha.
    title_hint: str = ""
    published_hint: datetime | None = None  # granularidade de DIA (00:00 UTC)
    # Retrieval language of the query that fetched this item (§1.4 of the
    # multilingual design). Stamped by iter_collect/collect from the LANGUAGES
    # registry: 'en'/'ar'/... for a per-language GNews `site:` query, None for
    # RSS feeds and the Portuguese `site:` block (native PT / untranslated). It
    # drives Stage 3c translation (only foreign, non-en/pt tags translate) and
    # the source_lang column persisted on the article.
    source_lang: str | None = None


_PT_MONTH_MAP = {
    "jan": "Jan", "fev": "Feb", "mar": "Mar", "abr": "Apr",
    "mai": "May", "jun": "Jun", "jul": "Jul", "ago": "Aug",
    "set": "Sep", "out": "Oct", "nov": "Nov", "dez": "Dec",
}
_PT_DAY_MAP = {
    "seg": "Mon", "ter": "Tue", "qua": "Wed", "qui": "Thu",
    "sex": "Fri", "sab": "Sat", "dom": "Sun",
}
_PT_DATE_RE = re.compile(
    r"^([A-Za-z]{3}),\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d{2}:\d{2}:\d{2})\s*([+\-]\d{4})$"
)


def _parse_ptbr_date(s: str) -> datetime | None:
    """Parse RFC 822-style pubDate with Portuguese locale.

    Handles dates like "Ter, 26 Mai 2026 09:50:56 -0300" produced by UOL feeds,
    which feedparser and dateutil cannot handle because of the pt-BR abbreviations.
    """
    if not s:
        return None
    m = _PT_DATE_RE.match(s.strip())
    if not m:
        return None
    day_abbr, day_num, month_abbr, year, time_str, tz_str = m.groups()
    month_en = _PT_MONTH_MAP.get(month_abbr.lower())
    day_en = _PT_DAY_MAP.get(day_abbr.lower(), day_abbr[:3])
    if not month_en:
        return None
    try:
        return date_parser.parse(f"{day_en}, {day_num} {month_en} {year} {time_str} {tz_str}")
    except (ValueError, TypeError):
        return None


def _parse_entry_date(entry) -> datetime | None:
    # feedparser's *_parsed fields are time.struct_time in UTC. calendar.timegm
    # converts UTC struct -> UTC timestamp; time.mktime would treat the struct
    # as LOCAL time, shifting dates by the machine's tz offset.
    for key in ("published_parsed", "updated_parsed"):
        val = getattr(entry, key, None) or (entry.get(key) if isinstance(entry, dict) else None)
        if val:
            try:
                ts = calendar.timegm(val)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except (TypeError, ValueError, OverflowError):
                continue
    # Fallback: try raw string fields for feeds with pt-BR locale dates (e.g. UOL Economia)
    for key in ("published", "updated"):
        raw = entry.get(key) if isinstance(entry, dict) else getattr(entry, key, None)
        if raw:
            dt = _parse_ptbr_date(raw)
            if dt is not None:
                return dt
    return None


def _unwrap_redirect(url: str) -> str:
    """Folha usa redir.folha.com.br/redir/.../*<destino> nos feeds; extrai o destino."""
    try:
        p = urlparse(url)
    except ValueError:
        return url
    host = p.netloc.lower()
    if host == "redir.folha.com.br":
        # Formato: .../*https://www1.folha.uol.com.br/...
        if "*" in url:
            after = url.split("*", 1)[1]
            if after.startswith(("http://", "https://")):
                return after
        # Fallback: ?url=<destino>
        qs = parse_qs(p.query)
        if "url" in qs and qs["url"]:
            return qs["url"][0]
    return url


def _entry_to_item(entry, feed_domain: str) -> RawItem | None:
    link = (entry.get("link") or "").strip()
    if not link:
        return None
    link = _unwrap_redirect(link)
    link = normalize_url(link)
    title = (entry.get("title") or "").strip()
    summary = (entry.get("summary") or entry.get("description") or "").strip()
    published = _parse_entry_date(entry)
    src_domain = urlparse(link).netloc.lower()

    # Google News: o dominio real vem em entry.source.href (tag <source url="...">)
    if src_domain.endswith("news.google.com"):
        source = entry.get("source") or {}
        source_href = source.get("href") if isinstance(source, dict) else getattr(source, "href", None)
        if source_href:
            src_domain = urlparse(source_href).netloc.lower() or src_domain
        # O titulo do Google News vem como "Titulo - Fonte"; limpa o sufixo
        if " - " in title:
            base, _, tail = title.rpartition(" - ")
            if base and len(tail) < 60:
                title = base.strip()

    if not src_domain:
        src_domain = feed_domain
    return RawItem(
        url=link,
        title=title,
        summary=summary,
        published_at=published,
        source_domain=src_domain,
        feed_domain=feed_domain,
    )


_NS_SITEMAP = "http://www.sitemaps.org/schemas/sitemap/0.9"
_NS_NEWS = "http://www.google.com/schemas/sitemap-news/0.9"


def _fetch_sitemap(feed_url: str, feed_domain: str) -> tuple[list[RawItem], str | None]:
    """Parseia sitemap Google News (urlset com news:news). Sem summary."""
    try:
        r = requests.get(
            feed_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/xml, text/xml, */*",
            },
            timeout=FEED_TIMEOUT,
        )
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:  # noqa: BLE001
        return [], f"{feed_domain}: {e!s}"

    items: list[RawItem] = []
    for url_el in root.findall(f"{{{_NS_SITEMAP}}}url"):
        loc_el = url_el.find(f"{{{_NS_SITEMAP}}}loc")
        if loc_el is None or not (loc_el.text or "").strip():
            continue
        link = normalize_url(loc_el.text.strip())

        news_el = url_el.find(f"{{{_NS_NEWS}}}news")
        title = ""
        published: datetime | None = None
        if news_el is not None:
            title_el = news_el.find(f"{{{_NS_NEWS}}}title")
            if title_el is not None and title_el.text:
                title = title_el.text.strip()
            date_el = news_el.find(f"{{{_NS_NEWS}}}publication_date")
            if date_el is not None and date_el.text:
                try:
                    published = date_parser.parse(date_el.text.strip())
                    if published.tzinfo is None:
                        published = published.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    published = None

        src_domain = urlparse(link).netloc.lower() or feed_domain
        items.append(
            RawItem(
                url=link,
                title=title,
                summary="",
                published_at=published,
                source_domain=src_domain,
                feed_domain=feed_domain,
            )
        )
    return items, None


# Data impressa ao lado do link numa listagem: "29/07/2026".
_LISTING_DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")

# Textos de link que sao call-to-action, nao manchete ("Ler notícia", "Leia mais").
_CTA_TEXTS = frozenset({
    "ler noticia", "ler notícia", "leia mais", "leia", "saiba mais",
    "veja mais", "continue lendo", "confira", "acesse",
})
_LISTING_TITLE_MIN_CHARS = 15


def _listing_hints(anchor) -> tuple[str, datetime | None]:
    """Extrai (manchete, data) do proprio HTML da listagem, se houver.

    Sobe no maximo 2 niveis a partir do <a> procurando UMA data dd/mm/yyyy ou
    um <time datetime>. O guard `len(matches) == 1` evita pegar a data do
    vizinho quando subimos para um container que abriga varios artigos.

    Vale ouro: a listagem de "ultimas noticias" do Brasil Energia imprime a
    data de publicacao junto do link ("Cai oferta de gás da Petrobras
    29/07/2026"), e as 30 entradas dessa pagina cobrem ~7 DIAS, nao 24h. Sem
    ler essa data o scanner nao tem como saber que o item do rodape da listagem
    e velho — e, quando o fetch do artigo falha, acaba carimbando now() nele.
    """
    title = ""
    published: datetime | None = None

    node = anchor
    for _ in range(3):
        if node is None or not hasattr(node, "get_text"):
            break
        text = node.get_text(" ", strip=True)
        if published is None:
            t = node.find("time", attrs={"datetime": True}) if hasattr(node, "find") else None
            if t is not None:
                try:
                    parsed = date_parser.parse(t["datetime"])
                    published = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError, OverflowError):
                    published = None
            if published is None:
                found = _LISTING_DATE_RE.findall(text)
                if len(found) == 1:
                    d, mo, y = (int(g) for g in found[0])
                    try:
                        published = datetime(y, mo, d, tzinfo=timezone.utc)
                    except ValueError:
                        published = None
        if not title and node is anchor:
            candidate = _LISTING_DATE_RE.sub("", text).strip(" -|·–—")
            candidate = re.sub(r"\s+", " ", candidate)
            if (
                len(candidate) >= _LISTING_TITLE_MIN_CHARS
                and candidate.lower() not in _CTA_TEXTS
            ):
                title = candidate[:300]
        if published is not None:
            break
        node = node.parent
    return title, published


def _scrape_homepage(page_url: str, feed_domain: str) -> tuple[list[RawItem], str | None]:
    """Scrapa homepage com curl_cffi e extrai links de artigos.

    Usado para sites que bloqueiam RSS mas permitem acesso via browser
    (Brasil Energia, Agencia Petrobras, etc.). Retorna itens sem titulo/summary
    (enrich_item busca cada pagina individualmente) mas COM os hints que a
    propria listagem exibe — manchete e data — quando existem.
    """
    try:
        from ._clipinator_shim import fetch_html
        html = fetch_html(page_url, timeout=8)
    except Exception as e:  # noqa: BLE001
        return [], f"{feed_domain}: {e!s}"

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:  # noqa: BLE001
        return [], f"{feed_domain}: parse {e!s}"

    base = page_url.rstrip("/")
    # Coleta todos os hrefs e guarda os que parecem artigos (path com 2+ segmentos).
    # Um mesmo artigo costuma aparecer em 2 <a> (a imagem e a manchete); em vez de
    # ficar so com o primeiro, fundimos os hints — o <a> da imagem vem sem texto.
    by_url: dict[str, RawItem] = {}
    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        # Resolve URL relativa
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            parsed_base = urlparse(base)
            href = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
        elif not href.startswith("http"):
            continue
        # Descarta URLs de outros dominios ou de categorias/tags
        parsed = urlparse(href)
        if parsed.netloc.lower() not in (feed_domain, f"www.{feed_domain}", feed_domain.lstrip("www.")):
            continue
        path = parsed.path.rstrip("/")
        segments = [s for s in path.split("/") if s]
        if len(segments) < 2:
            continue
        # Descarta URLs de listagem/categorias: segmentos intermediarios conhecidos
        if any(seg in _NON_ARTICLE_SEGMENTS for seg in segments[:-1]):
            continue
        slug = segments[-1]
        # Slugs de artigo tem titulos com 4+ hifens; navegacao/categoria tem poucos
        if slug.count("-") < 4:
            continue
        link = normalize_url(href)
        title_hint, published_hint = _listing_hints(a)
        existing = by_url.get(link)
        if existing is not None:
            # Segundo <a> para o mesmo artigo: completa o que faltava.
            if not existing.title_hint and title_hint:
                existing.title_hint = title_hint
            if existing.published_hint is None and published_hint is not None:
                existing.published_hint = published_hint
            continue
        by_url[link] = RawItem(
            url=link,
            title="",       # preenchido pelo enrich_item
            summary="",     # preenchido pelo enrich_item
            published_at=None,  # preenchido pelo enrich_item
            source_domain=parsed.netloc.lower(),
            feed_domain=feed_domain,
            title_hint=title_hint,
            published_hint=published_hint,
        )
    return list(by_url.values()), None


def _parse_lastmod(el) -> datetime | None:
    """<lastmod> -> datetime aware, ou None se ausente/ilegivel."""
    if el is None or not (el.text or "").strip():
        return None
    try:
        d = date_parser.parse(el.text.strip())
    except (ValueError, TypeError, OverflowError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


# Nomenclatura de paginacao de sitemap de POSTS, nas duas convencoes que
# aparecem na pratica:
#   WordPress core : wp-sitemap-posts-post-1.xml, wp-sitemap-posts-post-2.xml, ...
#   Yoast SEO      : post-sitemap.xml (= pagina 1), post-sitemap2.xml, ...
#
# Ancorado no NOME DO ARQUIVO inteiro (^...$) de proposito: um `in` solto
# casaria page-sitemap, post_tag-sitemap, category-sitemap, author-sitemap e
# tribe_events-sitemap, que sao listagens/arquivos e nao artigos.
_SITEMAP_POST_PAGE_RE = re.compile(
    r"^(?:wp-sitemap-posts-post-(?P<wp>\d+)|post-sitemap(?P<yoast>\d*))\.xml$",
    re.IGNORECASE,
)

# Folga do cross-check de sanidade da pagina escolhida (ver _fetch_standard_sitemap).
_SITEMAP_STALE_PAGE_SLACK = timedelta(hours=48)


def _post_sitemap_page(loc: str) -> int | None:
    """Numero da pagina, se `loc` e um sitemap paginado de posts. Senao None."""
    try:
        name = urlparse(loc).path.rstrip("/").rsplit("/", 1)[-1]
    except ValueError:
        return None
    m = _SITEMAP_POST_PAGE_RE.match(name)
    if m is None:
        return None
    return int(m.group("wp") or m.group("yoast") or "1")


def _fetch_standard_sitemap(feed_url: str, feed_domain: str) -> tuple[list[RawItem], str | None]:
    """Parseia sitemap WordPress padrao (urlset sem news:news).

    Retorna itens com titulo/summary VAZIOS — serao preenchidos pelo enrich.
    Filtra por <lastmod> para nao enriquecer milhares de posts antigos.

    Se feed_url aponta para um sitemapindex (ex.: /wp-sitemap.xml, Yoast
    /sitemap_index.xml), descobre a pagina de posts mais recente e usa essa.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=96)
    _HDR = {"User-Agent": USER_AGENT, "Accept": "application/xml, text/xml, */*"}
    try:
        r = requests.get(feed_url, headers=_HDR, timeout=STANDARD_SITEMAP_TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:  # noqa: BLE001
        return [], f"{feed_domain}: {e!s}"

    # Detecta sitemapindex: descobre a pagina de posts mais recente.
    _NS_IDX = f"{{{_NS_SITEMAP}}}sitemapindex"
    index_claim: datetime | None = None
    chosen_page: str | None = None
    if root.tag == _NS_IDX or root.tag == "sitemapindex":
        pages: list[tuple[int, str]] = []
        claims: list[datetime] = []
        others: list[str] = []
        for s in root.findall(f"{{{_NS_SITEMAP}}}sitemap"):
            loc_el = s.find(f"{{{_NS_SITEMAP}}}loc")
            loc = (loc_el.text or "").strip() if loc_el is not None else ""
            if not loc:
                continue
            page_no = _post_sitemap_page(loc)
            if page_no is None:
                others.append(loc.rstrip("/").rsplit("/", 1)[-1])
                continue
            pages.append((page_no, loc))
            lm = _parse_lastmod(s.find(f"{{{_NS_SITEMAP}}}lastmod"))
            if lm is not None:
                claims.append(lm)
        if not pages:
            # ANTES isto era `return [], None`: um zero MUDO. O sitemap de
            # visaoagro.com.br (Yoast) nunca casou o padrao "posts-post-" do
            # WordPress core, entao a fonte devolveu 0 itens em 33/33 runs sem
            # aparecer nem em "feeds returning 0 items" nem em "feed errors".
            # Um indice de sitemap que nao expoe nenhuma pagina de posts e um
            # ERRO de configuracao nosso (ou uma mudanca de nomenclatura da
            # fonte) e precisa ser dito em voz alta.
            return [], (
                f"{feed_domain}: sitemapindex sem pagina de posts reconhecida "
                f"({len(others)} filhos: {', '.join(sorted(others)[:8])})"
            )
        # Criterio de escolha: MAIOR NUMERO DE PAGINA.
        #
        # Nao e a ordem do documento (o `post_urls[-1]` anterior): hoje o ultimo
        # elemento calha de ser o certo, e isso quebra em silencio no dia em que
        # o gerador emitir os filhos fora de ordem, ou emitir a pagina 8 antes
        # da 7.
        #
        # E tambem NAO e o <lastmod> do indice, por dois motivos medidos:
        #   1. O WordPress core simplesmente nao emite lastmod nos filhos
        #      (istoedinheiro.com.br: 481 paginas de posts, zero lastmod) — o
        #      criterio seria indefinido.
        #   2. O lastmod do indice MENTE. Em visaoagro.com.br o indice anuncia
        #      post-sitemap.xml com lastmod de hoje, mas o conteudo desse arquivo
        #      vai de 2022-06-01 a 2023-04-28: e a pagina 1, a dos posts MAIS
        #      ANTIGOS. Os posts vivos estao na pagina 7.
        #
        # As duas convencoes paginam do mais ANTIGO para o mais NOVO, entao o
        # numero da pagina e o unico sinal que veio verificado. O lastmod do
        # indice fica so como cross-check de sanidade, abaixo.
        page_no, chosen_page = max(pages, key=lambda p: p[0])
        index_claim = max(claims) if claims else None
        try:
            r2 = requests.get(chosen_page, headers=_HDR, timeout=STANDARD_SITEMAP_TIMEOUT)
            r2.raise_for_status()
            root = ET.fromstring(r2.content)
        except Exception as e:  # noqa: BLE001
            return [], f"{feed_domain}: pagina de posts {page_no}: {e!s}"

    # Coleta em duas passagens para poder filtrar duplicatas WordPress:
    # quando um editor republica um post, o WordPress resolve o conflito de
    # slug appendando "-2" (-3, ...). Resultado: o sitemap lista /slug/ e
    # /slug-2/ como posts distintos, com conteudo identico. Sem titulo no
    # sitemap (e comum IstoE ter fetch_html falhando no enrich), o stage 4
    # cai no fallback de slug e gera titulos "... no pr" e "... no pr 2".
    url_elements = list(root.findall(f"{{{_NS_SITEMAP}}}url"))

    # Passagem 1: coletar todas as paths para dedupe.
    all_paths: set[str] = set()
    for url_el in url_elements:
        loc_el = url_el.find(f"{{{_NS_SITEMAP}}}loc")
        if loc_el is not None and (loc_el.text or "").strip():
            try:
                all_paths.add(urlparse(loc_el.text.strip()).path.rstrip("/"))
            except ValueError:
                continue

    items: list[RawItem] = []
    newest_entry: datetime | None = None
    for url_el in url_elements:
        loc_el = url_el.find(f"{{{_NS_SITEMAP}}}loc")
        if loc_el is None or not (loc_el.text or "").strip():
            continue
        link = normalize_url(loc_el.text.strip())

        # Dedupe WordPress: /slug-N/ e duplicata de /slug/ se ambos existem.
        # Conservador: so age se slug base tem 3+ hifens (padrao de artigo).
        path = urlparse(link).path.rstrip("/")
        m = _WP_DUP_SUFFIX_RE.search(path)
        if m:
            base_path = path[: m.start()]
            base_seg = base_path.rsplit("/", 1)[-1] if "/" in base_path else base_path
            if base_seg.count("-") >= 3 and base_path in all_paths:
                continue  # pula a variante duplicada

        published = _parse_lastmod(url_el.find(f"{{{_NS_SITEMAP}}}lastmod"))
        if published is not None and (newest_entry is None or published > newest_entry):
            newest_entry = published

        # Descarta posts antigos para nao sobrecarregar o enriquecimento
        if published is None or published < cutoff:
            continue

        src_domain = urlparse(link).netloc.lower() or feed_domain
        items.append(RawItem(
            url=link,
            title="",       # preenchido pelo enrich_item
            summary="",     # preenchido pelo enrich_item
            published_at=published,
            source_domain=src_domain,
            feed_domain=feed_domain,
        ))

    # Cross-check de sanidade da pagina escolhida, de graca (sem request extra):
    # a entrada mais recente que ela REALMENTE contem vs. a data mais recente
    # que o indice ANUNCIOU para qualquer pagina de posts. Se a pagina escolhida
    # e muito mais velha do que o indice promete, escolhemos a pagina errada —
    # exatamente o que acontecia em visaoagro (pagina 1: max 2023-04-28; indice
    # anunciando hoje). Isso vira log, nao um terceiro fetch: o custo de um
    # request a mais por scan e maior que o de um aviso que um humano le.
    if index_claim is not None and chosen_page is not None:
        if newest_entry is None or newest_entry < index_claim - _SITEMAP_STALE_PAGE_SLACK:
            log.warning(
                "%s: pagina de posts escolhida (%s) parece a errada — entrada mais "
                "recente %s, mas o sitemapindex anuncia %s. A nomenclatura de "
                "paginacao da fonte mudou?",
                feed_domain, chosen_page.rsplit("/", 1)[-1],
                newest_entry.isoformat() if newest_entry else "nenhuma",
                index_claim.isoformat(),
            )
    return items, None


# "-N" no fim do path, onde N tem 1-2 digitos. Ancora na barra para evitar
# capturar substring no meio (ex.: "/em-10-anos-algo" nao casa).
_WP_DUP_SUFFIX_RE = re.compile(r"-\d{1,2}$")

# site: operand inside a quote_plus'd Google News query (site%3Adominio.com).
_GNEWS_SITE_RE = re.compile(r"site%3A([^\s+&]+)", re.IGNORECASE)

# Max entries rendered in the per-feed observability lines. A mass outage must
# not turn one log line into 10 KB.
_LOG_LIST_CAP = 15


def _lang_from_query_url(url: str) -> str | None:
    """Recover a Google News query's source_lang from its ``hl=`` param.

    Defensive twin of the ``lang_by_url`` map built in iter_collect/collect: it
    maps ``hl=`` back to a LANGUAGES code, so a refactor that forgets to populate
    the map can't silently UNLABEL foreign items. Only registry languages are
    recoverable — Portuguese (``hl=pt-BR``) is not in LANGUAGES, so it correctly
    returns None (native PT / untranslated), and RSS feeds (no ``hl=``) too.
    """
    try:
        qs = parse_qs(urlparse(url).query)
    except ValueError:
        return None
    hl = (qs.get("hl") or [None])[0]
    if not hl:
        return None
    for cfg in LANGUAGES.values():
        if cfg.hl == hl:
            return cfg.code
    return None


def feed_label(domain: str, url: str) -> str:
    """Short, stable identifier for a feed task, for logs.

    A domain can register several feeds (g1, folha, exame), so the domain alone
    is ambiguous; the full URL is too long, especially for Google News queries.
    Renders as "domain/last-path-segment", or "gnews:<site operand>" for the
    Google News site: queries.
    """
    if domain == "news.google.com":
        m = _GNEWS_SITE_RE.search(url)
        return f"gnews:{m.group(1)}" if m else "gnews:query"
    try:
        path = urlparse(url).path.rstrip("/")
    except ValueError:
        return domain
    tail = path.rsplit("/", 1)[-1] if path else ""
    return f"{domain}/{tail}" if tail else domain


def _strip_domain_prefix(domain: str, msg: str) -> str:
    """Fetchers prefix errors with "<domain>: "; the label already carries it."""
    prefix = f"{domain}: "
    return msg[len(prefix):] if msg.startswith(prefix) else msg


def _join_capped(labels: list[str]) -> str:
    ordered = sorted(labels)
    if len(ordered) <= _LOG_LIST_CAP:
        return ", ".join(ordered)
    shown = ", ".join(ordered[:_LOG_LIST_CAP])
    return f"{shown} (+{len(ordered) - _LOG_LIST_CAP} more)"


def _newest_published(items: list[RawItem]) -> datetime | None:
    """Most recent real publication date among a feed's items, if any.

    Only `published_at` counts — the date the SOURCE stated. Listing hints and
    fabricated stamps are exactly what a staleness check must not trust.
    """
    dates = [it.published_at for it in items if it.published_at is not None]
    return max(dates) if dates else None


def _stale_feeds(
    newest: dict[tuple[str, str], datetime],
    now: datetime,
) -> list[tuple[str, float, float]]:
    """Feeds whose newest item is older than the domain's staleness budget.

    Returns (label, age_hours, budget_hours), stalest first. A feed that returned
    no dated item at all is NOT reported here: that is either a zero (already
    named in the summary) or a source whose dates only exist after enrichment.
    """
    out: list[tuple[str, float, float]] = []
    for (dom, url), ts in newest.items():
        # Google News queries carry their own `when:` window, so their recency
        # says nothing about the health of a feed we registered.
        if dom == "news.google.com":
            continue
        budget = feed_stale_hours(dom)
        age_h = (now - ts).total_seconds() / 3600.0
        if age_h > budget:
            out.append((feed_label(dom, url), age_h, budget))
    out.sort(key=lambda t: t[1], reverse=True)
    return out


def _render_stale(stale: list[tuple[str, float, float]]) -> str:
    rendered = [
        f"{label} (newest item {age / 24:.1f}d ago, budget {budget / 24:.0f}d)"
        for label, age, budget in stale[:_LOG_LIST_CAP]
    ]
    extra = len(stale) - len(rendered)
    return ", ".join(rendered) + (f" (+{extra} more)" if extra > 0 else "")


def _fetch_one(feed_url: str, feed_domain: str) -> tuple[list[RawItem], str | None]:
    """Baixa e parseia um feed. Retorna (items, erro_ou_None)."""
    if is_sitemap_url(feed_url):
        return _fetch_sitemap(feed_url, feed_domain)

    # Baixa via requests (com timeout real) e passa o bytes ao feedparser.
    # feedparser.parse(url) usa urllib sem timeout - causa travas de 10-30s
    # em feeds lentos, estourando o orcamento global.
    try:
        r = requests.get(
            feed_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.5",
            },
            timeout=FEED_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001
        return [], f"{feed_domain}: {e!s}"
    if r.status_code >= 400:
        return [], f"{feed_domain}: HTTP {r.status_code}"

    try:
        # When the HTTP Content-Type declares a non-UTF-8 charset (e.g. ISO-8859-1)
        # and the XML body has no <?xml?> declaration, feedparser defaults to UTF-8
        # for XML, mangling accented characters. Pre-decode with the declared charset
        # so feedparser receives a proper str instead of raw bytes.
        feed_input: bytes | str = r.content
        ct = r.headers.get("content-type", "")
        if "charset=" in ct.lower() and "utf-8" not in ct.lower() and not r.content.lstrip()[:5].startswith(b"<?xml"):
            charset = ct.lower().split("charset=")[-1].split(";")[0].strip()
            try:
                feed_input = r.content.decode(charset)
            except (LookupError, UnicodeDecodeError):
                feed_input = r.content
        parsed = feedparser.parse(feed_input)
    except Exception as e:  # noqa: BLE001
        return [], f"{feed_domain}: {e!s}"

    bozo = getattr(parsed, "bozo", 0)
    entries = parsed.get("entries") or []
    if not entries:
        if bozo:
            exc = parsed.get("bozo_exception")
            return [], f"{feed_domain}: {exc}"
        return [], None

    items: list[RawItem] = []
    for e in entries:
        it = _entry_to_item(e, feed_domain)
        if it:
            items.append(it)
    # Erros cosmeticos (bozo=1 mas entries vieram) nao sao reportados.
    return items, None


def iter_collect(
    keywords: list[str],
    hours: int,
    *,
    max_workers: int = 48,
    include_google_news: bool = True,
    deadline: float | None = None,
):
    """Versao streaming de collect: yield (dom, items, err) a medida que feeds
    terminam. Permite overlap com enriquecimento.
    """
    tasks: list[tuple[str, str]] = list(all_rss_feeds())
    # Sitemaps WordPress padrao (istoedinheiro, visaoagro, etc.)
    std_tasks: list[tuple[str, str]] = list(all_standard_sitemaps())
    # Query URL -> retrieval language (§1.4). Only per-language GNews `site:`
    # queries are entered; RSS feeds and the PT block stay absent (=> None =
    # native/untranslated). Items are stamped from this map in the drain loop.
    lang_by_url: dict[str, str] = {}
    if include_google_news:
        if NO_RSS_DOMAINS:
            for url in google_news_site_queries(NO_RSS_DOMAINS, keywords, hours):
                tasks.append(("news.google.com", url))
        # Per-language site: queries from the LANGUAGES registry (en today; +ar
        # in B1-d; +5 in B2+). English reproduces the historical direct call
        # exactly (test_multilingual_en_frozen.py); each query URL is tagged with
        # its language code so the drain loop can stamp the items it returns.
        for cfg in LANGUAGES.values():
            if cfg.no_rss_domains:
                for url in google_news_site_queries_lang(
                    cfg, list(cfg.no_rss_domains), keywords, hours
                ):
                    tasks.append(("news.google.com", url))
                    lang_by_url[url] = cfg.code

    import time as _time
    t_start = _time.time()
    dl = deadline if deadline is not None else COLLECT_DEADLINE

    # Agrega todos os tipos de fonte: RSS/sitemaps, sitemaps padrao, homepage scrapers
    # Cada tipo tem seu fetcher mas todos compartilham o mesmo deadline global.
    home_tasks: list[tuple[str, str]] = list(all_homepage_scrapers())

    def _make_fetcher(is_std: bool, is_home: bool):
        if is_home:
            return _scrape_homepage
        if is_std:
            return _fetch_standard_sitemap
        return _fetch_one

    all_tasks = (
        [(dom, url, False, False) for dom, url in tasks] +
        [(dom, url, True, False) for dom, url in std_tasks] +
        [(dom, url, False, True) for dom, url in home_tasks]
    )

    ex = ThreadPoolExecutor(max_workers=max_workers)
    futs = {
        ex.submit(_make_fetcher(is_std, is_home), url, dom): (dom, url)
        for dom, url, is_std, is_home in all_tasks
    }
    # Per-feed observability: count items returned by (domain, feed_url) so
    # silent gaps (HTTP 200 + zero items, e.g. exame.com /feed/ dropping the
    # Revista Exame section) surface in logs instead of failing silently.
    # Aggregated and logged at INFO once iter_collect finishes draining.
    _feed_counts: dict[tuple[str, str], int] = {}
    _feed_errors: dict[tuple[str, str], str] = {}
    # Newest item date per feed. A feed that answers 200 with the SAME items
    # forever is as dead as one returning zero, and nothing above notices it.
    _feed_newest: dict[tuple[str, str], datetime] = {}
    try:
        for fut in as_completed(futs.keys(), timeout=dl):
            dom, _url = futs[fut]
            try:
                got, err = fut.result()
            except Exception as e:  # noqa: BLE001
                _feed_errors[(dom, _url)] = str(e)
                yield dom, [], f"{dom}: {e!s}"
                continue
            _feed_counts[(dom, _url)] = len(got or [])
            # Stamp the retrieval language on each item (§1.4). lang_by_url is
            # the source of truth; _lang_from_query_url is a defensive fallback
            # that recovers the tag from hl= if the map ever misses a lang query.
            lang = lang_by_url.get(_url) or _lang_from_query_url(_url)
            if lang and got:
                for it in got:
                    if it.source_lang is None:
                        it.source_lang = lang
            newest = _newest_published(got or [])
            if newest is not None:
                _feed_newest[(dom, _url)] = newest
            if err:
                _feed_errors[(dom, _url)] = err
            yield dom, got, err
            if _time.time() - t_start >= dl:
                break
    except (FuturesTimeout, TimeoutError):
        pass
    finally:
        # Mark feeds that never completed (deadline hit) as -1 for visibility.
        for fut, (dom, url) in futs.items():
            if not fut.done():
                fut.cancel()
                _feed_counts.setdefault((dom, url), -1)
        ex.shutdown(wait=False, cancel_futures=True)
        if _feed_counts or _feed_errors:
            total = sum(c for c in _feed_counts.values() if c > 0)
            # A REGISTERED feed returning 0 items is always anomalous: these
            # sites publish daily and their feeds carry the last N items
            # regardless of date. A Google News site: query returning 0 is
            # routine (no article matched the keywords inside the window), so
            # those are counted apart instead of drowning the real signal.
            zero_feeds: list[str] = []
            gnews_zero = 0
            timed_out: list[str] = []
            for (d, u), c in _feed_counts.items():
                if c == 0:
                    if d == "news.google.com":
                        gnews_zero += 1
                    else:
                        zero_feeds.append(feed_label(d, u))
                elif c == -1:
                    timed_out.append(feed_label(d, u))
            log.info(
                "feed summary: %d items total, %d feeds returned 0 (+%d gnews queries), "
                "%d feeds timed out, %d feeds errored",
                total, len(zero_feeds), gnews_zero, len(timed_out), len(_feed_errors),
            )
            # INFO, not DEBUG: production runs at INFO, so anything below it is
            # invisible where it matters. A source can die silently for months
            # while the run stays green and the counters stay plausible — that
            # is exactly how monitormercantil.com.br went unnoticed from
            # 2026-04-29 to 2026-07-30 (Cloudflare 403 on every datacenter IP).
            # Naming the dead feeds is the whole point of the summary.
            if zero_feeds:
                log.info("feeds returning 0 items: %s", _join_capped(zero_feeds))
            if timed_out:
                log.info("feeds timed out (deadline %.1fs): %s", dl, _join_capped(timed_out))
            # Same argument one step further: a feed can keep answering 200 with
            # a full set of dated entries and still have stopped moving. That is
            # how www.poder360.com.br disappeared from the feed for six days in
            # 2026-08 — Cloudflare pinned its /feed/ object at 2026-08-07 19:55
            # UTC and every counter above stayed green. An item count says the
            # feed answered; only the date of its newest item says it is alive.
            stale = _stale_feeds(_feed_newest, datetime.now(timezone.utc))
            if stale:
                log.info("feeds stale (no new item): %s", _render_stale(stale))
            if _feed_errors:
                log.info(
                    "feed errors: %s",
                    _join_capped([
                        f"{feed_label(d, u)}: {_strip_domain_prefix(d, msg)}"
                        for (d, u), msg in _feed_errors.items()
                    ]),
                )


def collect(
    keywords: list[str],
    hours: int,
    *,
    max_workers: int = 48,
    include_google_news: bool = True,
) -> tuple[list[RawItem], list[str]]:
    """Busca todos os feeds registrados + Google News. Retorna (itens, erros).

    A filtragem por keyword e data nao e feita aqui - cabe ao chamador.
    Mas ja passamos keywords para construir queries do Google News.

    As queries GERAIS do Google News (por keyword em qualquer site) sao
    pesadas: trazem centenas de URLs com wrapper que nao tem snippet sem
    decode. Por padrao usamos apenas queries 'site:' para dominios sem
    RSS/sitemap proprio. Isso mantem o orcamento <10s.
    """
    tasks: list[tuple[str, str]] = list(all_rss_feeds())
    lang_by_url: dict[str, str] = {}

    if include_google_news:
        if NO_RSS_DOMAINS:
            for url in google_news_site_queries(NO_RSS_DOMAINS, keywords, hours):
                tasks.append(("news.google.com", url))
        # Per-language site: queries from the LANGUAGES registry (en + ar today),
        # each tagged with its language code (parity with iter_collect, §1.4).
        for cfg in LANGUAGES.values():
            if cfg.no_rss_domains:
                for url in google_news_site_queries_lang(
                    cfg, list(cfg.no_rss_domains), keywords, hours
                ):
                    tasks.append(("news.google.com", url))
                    lang_by_url[url] = cfg.code

    items: list[RawItem] = []
    errors: list[str] = []

    # Deadline global duro (~6s): o que nao chegou a tempo fica para a proxima
    # busca. Isso mantem a latencia de /search bounded sem depender da boa
    # vontade de cada feed.
    ex = ThreadPoolExecutor(max_workers=max_workers)
    futs = {ex.submit(_fetch_one, url, dom): (dom, url) for dom, url in tasks}
    try:
        done, not_done = wait(futs.keys(), timeout=COLLECT_DEADLINE)
        for fut in done:
            dom, url = futs[fut]
            try:
                got, err = fut.result()
            except Exception as e:  # noqa: BLE001
                errors.append(f"{dom}: {e!s}")
                continue
            if err:
                errors.append(err)
            lang = lang_by_url.get(url) or _lang_from_query_url(url)
            if lang and got:
                for it in got:
                    if it.source_lang is None:
                        it.source_lang = lang
            items.extend(got)
        for fut in not_done:
            dom, _ = futs[fut]
            fut.cancel()
            errors.append(f"{dom}: excedeu deadline de {COLLECT_DEADLINE}s")
    finally:
        # Nao bloqueia no shutdown - futures pendentes continuam rodando
        # mas nao bloqueiam o retorno de collect().
        ex.shutdown(wait=False, cancel_futures=True)

    # Dedupe por URL normalizada (mantem o primeiro com data nao-nula quando possivel)
    by_url: dict[str, RawItem] = {}
    for it in items:
        cur = by_url.get(it.url)
        if cur is None:
            by_url[it.url] = it
        elif cur.published_at is None and it.published_at is not None:
            by_url[it.url] = it

    return list(by_url.values()), errors
