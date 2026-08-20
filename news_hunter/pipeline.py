"""Orquestracao: coletar -> filtrar -> enriquecer -> persistir."""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

from .enrich import _resolve_google_news_url, enrich_item, source_name_for
from .fetcher import RawItem, iter_collect

from .filter import matches_keywords, strip_related, within_window
from .sources import HOMEPAGE_SCRAPERS, LANGUAGES, RECENT_ONLY_SCRAPERS
from .store import (
    Article,
    finish_run,
    get_cached_snippets,
    get_config,
    normalize_url,
    start_run,
    upsert_articles,
    urls_with_snippet,
)

log = logging.getLogger(__name__)

ENRICH_WORKERS = 24
# Workers de resolucao separados: gnewsdecoder sofre rate-limit acima de
# ~6 chamadas paralelas ao Google; com 6 workers cada chamada leva ~1.5s.
RESOLVE_WORKERS = 6
# Deadline de enrich sobre URLs ja resolvidas (fetch_html = 6s/item max).
ENRICH_DEADLINE = 18.0
# Deadline adicional para aguardar resolucoes pendentes apos collect.
# 56 itens / 6 workers * 1.75s/item ≈ 16s para cobrir quase tudo.
RESOLVE_EXTRA = 16.0

# --- Lede rescue (added 2026-06-09) -----------------------------------------
# Em fast_mode (o unico modo do scanner cloud) so casavamos keyword contra
# titulo + summary do RSS. Fontes editoriais (ex.: eixos "Comece seu dia")
# trazem titulos espertos e descriptions curtas (< SNIPPET_MIN_RSS_CHARS) cujo
# texto nao contem a keyword, mesmo quando o LEDE do corpo e claramente sobre
# petroleo/gas. Resultado: artigos relevantes sumiam silenciosamente.
#
# A correcao: para itens de RSS (titulo+data ja presentes) que passaram a
# janela mas NAO casaram no titulo nem no summary, buscamos o corpo (lede,
# primeiros paragrafos) de um subconjunto LIMITADO por scan e re-validamos a
# keyword. Mantem o caminho rapido rapido: so paga fetch_html nos near-miss,
# com cap global + por dominio + deadline proprio.
LEDE_RESCUE_WORKERS = 12
LEDE_RESCUE_DEADLINE = 14.0   # teto para a fase de lede (fetch_html = 6s/item)
LEDE_RESCUE_CAP = 40          # max fetches de lede por scan (global)
LEDE_RESCUE_CAP_DOMAIN = 8    # max fetches de lede por dominio por scan

# --- Stage 3d: snippet backfill (added 2026-08-20) --------------------------
# O lede rescue busca o corpo de quem NAO casou keyword. O espelho disso nunca
# existiu: um item que casou NO TITULO era guardado com snippet vazio e ninguem
# mais ia buscar o corpo dele. Como fontes cobertas so por Google News (Brasil
# 247, Agencia iNFRA, Reuters...) chegam sem description nenhuma, elas entravam
# SEMPRE assim — 1.026 das 1.383 linhas de 24h (74%) estavam sem snippet quando
# isto foi medido, e o feed mostrava so manchetes.
#
# Esta fase e o espelho: pega os itens JA APROVADOS que ainda estao sem snippet,
# do mais novo para o mais velho, e busca o corpo de um subconjunto capado.
# Ordenar por published_at DESC importa: artigo novo e sempre o mais novo, entao
# ele ganha o cap mesmo quando o backlog e grande.
#
# NAO re-valida keyword (o item ja passou) — o snippet aqui e display. E NAO
# custa um fetch por artigo por scan: urls_with_snippet() pergunta ao banco
# quais ja tem texto gravado, senao o scanner (stateless) re-baixaria os mesmos
# artigos a cada 5 minutos para sempre.
SNIPPET_BACKFILL_WORKERS = 12
SNIPPET_BACKFILL_DEADLINE = 12.0   # teto da fase (fetch_html = 6s/item)
SNIPPET_BACKFILL_CAP = 25          # max fetches por scan (global)
SNIPPET_BACKFILL_CAP_DOMAIN = 6
# Quantos candidatos (os mais novos) entram na pergunta ao banco. Uma consulta
# por scan, resposta so de urls: ~9 KB.
SNIPPET_BACKFILL_LOOKUP = 75
# Sentinela de ordenacao: artigo sem data vai para o fim da fila do backfill.
_OLDEST_TS = datetime.min.replace(tzinfo=timezone.utc)


# --- Stage 3c: translation (added Wave B1-d) --------------------------------
# POST-FILTER translation of the kept FOREIGN items only (source_lang not in
# en/pt/None): the multilingual design translates for DISPLAY, never in the
# sieve, and never the firehose (§2.5/§3). Small pool on purpose — Google's free
# endpoint rate-limits per IP, and the datacenter runner shares that pool. Every
# failure/timeout is fail-soft: the native title stays, title_en/snippet_en NULL.
TRANSLATE_WORKERS = 6
TRANSLATE_DEADLINE = 12.0    # teto da fase inteira (deep-translator ~0.4-2.6s/call)
TRANSLATE_CAP = 40           # max itens traduzidos por scan (espelha LEDE_RESCUE_CAP)


# Sentinela: item de RSS que passou a janela mas nao casou keyword no titulo
# nem no summary. Candidato a "lede rescue" (fetch do corpo + re-check).
LEDE_RESCUE_MARKER = "#lede"

# Folga aplicada a janela quando filtramos por `published_hint` (a data que a
# LISTAGEM imprime ao lado do link). O hint tem granularidade de DIA: um artigo
# publicado 23:00 vira 00:00 do mesmo dia, ou seja, ate 24h MAIS VELHO do que a
# realidade. Somamos 24h para nunca descartar um item que de fato esta dentro
# da janela — o filtro definitivo continua sendo o do stage 4, com a data real.
HINT_WINDOW_SLACK_HOURS = 24


def _keep_candidate(
    item: RawItem,
    keywords: list[str],
    hours: int,
    exact_keywords: set[str] | None = None,
    *,
    allow_lede_rescue: bool = False,
) -> list[str] | None:
    """Filtragem barata pre-enriquecimento.

    Retorna a lista de keywords que casaram, ou None se deve descartar.
    Itens sem titulo E sem summary (sitemaps WordPress padrao) recebem
    ["#pending"] — keyword check real ocorre no stage 4 apos enriquecimento.

    `exact_keywords` propagado para `matches_keywords`. Default None (=
    todas substring), garantindo compat com chamadores antigos.

    `allow_lede_rescue`: quando True, itens de RSS bem-formados (titulo +
    data) que NAO casaram no titulo nem no summary retornam o sentinela
    [LEDE_RESCUE_MARKER] em vez de None. O pipeline busca o corpo de um
    subconjunto limitado desses itens e re-valida a keyword contra o lede.
    Sem esse flag o comportamento e identico ao anterior (descarta).
    """
    # Janela primeiro: filtra a maioria dos itens sem pagar custo de regex.
    if item.published_at is not None and not within_window(item.published_at, hours):
        return None
    # Sem data propria, mas a listagem imprimiu uma: usa como filtro barato.
    # Isto e o que impede o rodape de uma listagem de arquivo (a de
    # "ultimas noticias" do Brasil Energia cobre ~7 DIAS, nao 24h) de ser
    # buscado a cada scan e — quando o fetch falha — persistido como se fosse
    # de agora. Descarta ANTES de qualquer fetch_html.
    if (
        item.published_at is None
        and item.published_hint is not None
        and not within_window(item.published_hint, hours + HINT_WINDOW_SLACK_HOURS)
    ):
        return None
    # Homepage scrapers apontam para paginas ja topicas (ex.:
    # brasilenergia.com.br/petroleoegas/ultimasnoticias). Aceita tudo sem
    # filtro de keyword — a pagina em si garante relevancia.
    if item.feed_domain in HOMEPAGE_SCRAPERS:
        return ["#topic"]
    # Items sem titulo nem summary:
    if not item.title and not item.summary:
        # Pre-filtra via URL: slug (sitemaps) ou path completo (homepage scrapers).
        # Reduz drasticamente o numero de fetches de enriquecimento.
        from urllib.parse import urlparse as _up
        path = _up(item.url).path
        if item.published_at is None:
            # Homepage scrapers: usa path inteiro (section + slug) como hint
            path_text = path.replace("-", " ").replace("/", " ")
            path_match = matches_keywords(path_text, keywords, exact_keywords)
            return path_match if path_match else None
        # Sitemaps WordPress padrao: usa apenas o slug (ultimo segmento)
        slug = path.rstrip("/").rsplit("/", 1)[-1].replace("-", " ")
        slug_match = matches_keywords(slug, keywords, exact_keywords)
        return slug_match if slug_match else None
    # Title-first: titulos sao curtos. Se casou, retorna imediatamente.
    matched = matches_keywords(item.title, keywords, exact_keywords)
    if matched:
        return matched
    clean_summary = strip_related(item.summary)
    summary_match = (
        matches_keywords(clean_summary, keywords, exact_keywords)
        if clean_summary
        else None
    )
    if summary_match:
        return summary_match
    # Near-miss: titulo + summary nao casaram. Item de RSS bem-formado
    # (titulo + data presentes) vira candidato a lede rescue; o resto descarta.
    if allow_lede_rescue and item.title and item.published_at is not None:
        return [LEDE_RESCUE_MARKER]
    return None


def _run_lede_rescue(
    candidates: list[RawItem],
    keywords: list[str],
    exact_keywords: set[str],
    enriched: list,
    errors: list[str],
) -> int:
    """Busca o corpo (lede) de near-miss de RSS e re-valida keyword.

    Para cada item near-miss (titulo + data presentes, mas keyword nem no
    titulo nem no summary), busca a pagina via enrich_item(need_snippet=True)
    — que roda fetch_html + extractor do dominio + meta description — e
    re-checa keyword contra titulo + snippet (lede). Os que casam sao
    anexados a `enriched` com os keywords reais; o stage 4 segue normalmente.

    Capado em tres niveis para nao estourar o orcamento do scan (~5 min,
    8 min de timeout GHA):
      - LEDE_RESCUE_CAP        : max total de fetches por scan
      - LEDE_RESCUE_CAP_DOMAIN : max por dominio (evita martelar uma fonte)
      - LEDE_RESCUE_DEADLINE   : teto de tempo da fase inteira

    Retorna o numero de itens resgatados (keyword casou no lede).
    """
    # Aplica caps ANTES de submeter (nao paga fetch alem do orcamento).
    selected: list[RawItem] = []
    per_domain: dict[str, int] = {}
    for it in candidates:
        if len(selected) >= LEDE_RESCUE_CAP:
            break
        dom = it.source_domain
        cnt = per_domain.get(dom, 0)
        if cnt >= LEDE_RESCUE_CAP_DOMAIN:
            continue
        per_domain[dom] = cnt + 1
        selected.append(it)

    if not selected:
        return 0

    log.info(
        "lede rescue: %d near-miss, %d selecionados (cap %d/%d-dom)",
        len(candidates), len(selected), LEDE_RESCUE_CAP, LEDE_RESCUE_CAP_DOMAIN,
    )

    rescued = 0
    ex = ThreadPoolExecutor(max_workers=LEDE_RESCUE_WORKERS)
    try:
        # need_snippet=True força fetch_html + extractor mesmo com title/published
        # ja presentes (caminho que o fast_mode normalmente pula).
        futs = {
            ex.submit(
                enrich_item, it,
                resolve_google_news=False,
                need_snippet=True,
            ): it
            for it in selected
        }
        done, not_done = wait(futs.keys(), timeout=LEDE_RESCUE_DEADLINE)
        for fut in done:
            it = futs[fut]
            try:
                snippet, published, resolved_url, resolved_domain, ext_title = fut.result()
            except Exception as e:  # noqa: BLE001
                errors.append(f"lede {it.url}: {e!s}")
                continue
            if not snippet:
                continue
            # Re-valida keyword contra titulo + lede. So segue se casar de fato.
            hay = f"{it.title} \n {snippet}"
            final_match = matches_keywords(hay, keywords, exact_keywords)
            if not final_match:
                continue
            rescued += 1
            enriched.append((
                it, final_match, snippet,
                published or it.published_at,
                resolved_url, resolved_domain, ext_title,
            ))
        for fut in not_done:
            fut.cancel()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    log.info("lede rescue: %d artigos resgatados", rescued)
    return rescued


def _backfill_order(empty: list[Article], *, oldest_first: bool = False) -> list[Article]:
    """Ordena os candidatos a backfill: round-robin entre dominios, recencia dentro.

    Recencia global pura parece obvia e aloca o orcamento pessimamente: quem
    publica mais leva tudo. Medido 2026-08-20, com o backfill ja no ar,
    finance.sina.com.cn respondia por 367 das ~994 linhas sem corpo em 24h — mais
    de um terco — e junto com alarabiya/yicai/asharq enchia a janela dos mais
    novos, deixando as fontes brasileiras (Brasil 247 com TRES artigos no dia)
    esperando atras de uma mangueira de fogo estrangeira.

    Round-robin conserta a alocacao sem nenhuma lista de prioridade para manter:
    cada dominio manda seu artigo mais novo, depois o segundo, e assim por
    diante. A ordem entre dominios e pela materia mais nova de cada um, entao a
    fonte que acabou de publicar continua na frente.

    O efeito colateral que importa: um dominio que NUNCA vai responder (Reuters
    da 401 de DataDome no runner) gasta uma vaga por scan em vez de seis.
    """
    newest = not oldest_first
    by_domain: dict[str, list[Article]] = {}
    for a in empty:
        by_domain.setdefault(a.domain, []).append(a)
    for items in by_domain.values():
        items.sort(key=lambda a: a.published_at or _OLDEST_TS, reverse=newest)
    # Abre a rodada o dominio com a materia mais nova (ou mais velha, no passe
    # de drenagem).
    queues = sorted(
        by_domain.values(),
        key=lambda items: items[0].published_at or _OLDEST_TS,
        reverse=newest,
    )
    ordered: list[Article] = []
    for rank in range(max(len(q) for q in queues)):
        for q in queues:
            if rank < len(q):
                ordered.append(q[rank])
    return ordered


def _backfill_candidates(empty: list[Article]) -> list[Article]:
    """Intercala o passe de RECENCIA com o passe de DRENAGEM.

    Recencia sozinha nunca alcanca o backlog, e o motivo e circular: os artigos
    cronicamente sem corpo sao, por definicao, os que ninguem preencheu — logo
    sao os mais VELHOS, e toda materia nova passa na frente deles para sempre.
    Medido 2026-08-20: os 10 artigos do Brasil 247 (o mais novo com 14h) ficavam
    fora da janela de 75 candidatos porque dezenas de dominios tinham materia
    mais fresca, e o dominio inteiro seguia em branco mesmo com o fetch
    funcionando de fato a partir do runner (diagnose_snippet: 357 chars).

    Metade do orcamento vai para o mais novo (uma materia recem-publicada tem que
    ganhar corpo no mesmo scan) e metade para o mais velho ainda vazio, que e o
    que faz o backlog realmente drenar. Ambos os passes sao round-robin por
    dominio, entao nenhum dos lados vira mangueira de fogo de um so dominio.
    """
    fresh = _backfill_order(empty)
    stale = _backfill_order(empty, oldest_first=True)
    out: list[Article] = []
    seen: set[str] = set()
    for a, b in zip(fresh, stale):
        for cand in (a, b):
            if cand.url not in seen:
                seen.add(cand.url)
                out.append(cand)
    return out


def _run_snippet_backfill(articles: list[Article], errors: list[str]) -> int:
    """Busca o corpo dos artigos aprovados que ainda estao sem snippet.

    Roda sobre o conjunto FINAL (to_persist), depois do stage 4 e antes da
    traducao — assim um item estrangeiro backfillado ainda ganha snippet_en no
    mesmo scan.

    Reusa enrich_item(need_snippet=True), o mesmo caminho do lede rescue, em vez
    de repetir fetch+extractor aqui: quando um extractor de dominio melhora, as
    duas fases melhoram juntas.

    Retorna quantos snippets foram preenchidos. Fail-soft: qualquer erro deixa o
    artigo exatamente como estava (sem snippet), que e o comportamento de hoje.
    """
    empty = [a for a in articles if not (a.snippet or "").strip()]
    if not empty:
        return 0
    head = _backfill_candidates(empty)[:SNIPPET_BACKFILL_LOOKUP]

    known = urls_with_snippet([a.url for a in head])
    if known is None:
        # Banco mudo: nao da para saber o que ja foi feito. Em vez de gastar o
        # cap inteiro re-baixando artigos possivelmente ja enriquecidos, atende
        # so os poucos mais novos — quase certamente novos de verdade.
        todo_pool = head[:SNIPPET_BACKFILL_CAP_DOMAIN]
    else:
        todo_pool = [a for a in head if a.url not in known]

    todo: list[Article] = []
    per_domain: dict[str, int] = {}
    for a in todo_pool:
        if len(todo) >= SNIPPET_BACKFILL_CAP:
            break
        n = per_domain.get(a.domain, 0)
        if n >= SNIPPET_BACKFILL_CAP_DOMAIN:
            continue
        per_domain[a.domain] = n + 1
        todo.append(a)
    if not todo:
        return 0

    filled = 0
    ex = ThreadPoolExecutor(max_workers=SNIPPET_BACKFILL_WORKERS)
    try:
        pending = {
            ex.submit(
                enrich_item,
                RawItem(
                    url=a.url,
                    title=a.title,
                    summary="",
                    published_at=a.published_at,
                    source_domain=a.domain,
                    feed_domain=a.domain,
                ),
                resolve_google_news=False,
                need_snippet=True,
            ): a
            for a in todo
        }
        done, not_done = wait(pending.keys(), timeout=SNIPPET_BACKFILL_DEADLINE)
        for fut in done:
            a = pending[fut]
            try:
                snippet, _pub, _url, _dom, _title = fut.result()
            except Exception as e:  # noqa: BLE001
                errors.append(f"snippet backfill {a.url}: {e!s}")
                continue
            if snippet and snippet.strip():
                a.snippet = snippet
                filled += 1
        for fut in not_done:
            fut.cancel()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    log.info(
        "snippet backfill: %d sem snippet, %d buscados (cap %d/%d-dom), %d preenchidos",
        len(empty), len(todo), SNIPPET_BACKFILL_CAP, SNIPPET_BACKFILL_CAP_DOMAIN, filled,
    )
    return filled


def _translate_article(a) -> bool:
    """Translate one foreign Article's title (+snippet) to English, in place.

    Fail-soft per field: a failed translation leaves that _en field None and the
    native title/snippet intact. Returns True if anything was translated.
    """
    from .translate import translate_to_en

    title_en = translate_to_en(a.title, a.source_lang)
    if title_en:
        a.title_en = title_en
    if a.snippet:
        snippet_en = translate_to_en(a.snippet, a.source_lang)
        if snippet_en:
            a.snippet_en = snippet_en
    return bool(a.title_en or a.snippet_en)


def _run_translation(articles: list, errors: list[str]) -> int:
    """Stage 3c: translate the kept FOREIGN items' title/snippet to English.

    Operates on the FINAL kept set (to_persist), after enrich + lede rescue, so
    it never touches the discarded firehose. Only foreign items (source_lang not
    in en/pt/None) are considered; `title_original` is stamped for every foreign
    row (native string, decoupled from `title` per §4) even when a translation is
    deferred over the cap or fails. Bounded by TRANSLATE_CAP + TRANSLATE_DEADLINE
    + a small pool. NEVER drops a row: on any failure/timeout the native title
    survives with title_en NULL, and the next scan re-tries (idempotent upsert).
    """
    foreign = [
        a for a in articles
        if a.source_lang and a.source_lang.lower() not in ("en", "pt")
    ]
    if not foreign:
        return 0
    # Record the native original for EVERY foreign row (cheap; makes title_original
    # populated even for rows whose translation is deferred over the cap).
    for a in foreign:
        a.title_original = a.title

    # Spend the per-scan cap on genuinely-untranslated rows only. The scanner is
    # stateless (no local cache), so a re-seen foreign row arrives with title_en
    # None even when the DB already holds its English — one batched lookup tells
    # us which URLs are done, and we skip them. Without this the cap is re-spent
    # every scan on the same front rows (which may already be translated) and the
    # deferred tail NEVER drains (~155 foreign/scan vs cap 40 = permanent churn).
    # Fail-soft: None/empty => skip nothing (old behaviour); the write-once guard
    # in supabase_sync still prevents any NULL clobber.
    from . import supabase_sync
    already_done = supabase_sync.already_translated_urls([a.url for a in foreign])
    pending = (
        [a for a in foreign if a.url not in already_done]
        if already_done else foreign
    )
    skipped = len(foreign) - len(pending)

    selected = pending[:TRANSLATE_CAP]
    over_cap = len(pending) - len(selected)
    translated = 0
    timed_out = 0
    ex = ThreadPoolExecutor(max_workers=TRANSLATE_WORKERS)
    try:
        futs = {ex.submit(_translate_article, a): a for a in selected}
        done, not_done = wait(futs.keys(), timeout=TRANSLATE_DEADLINE)
        timed_out = len(not_done)
        for fut in done:
            a = futs[fut]
            try:
                if fut.result():
                    translated += 1
            except Exception as e:  # noqa: BLE001
                # translate_to_en is already fail-soft; this only guards the
                # future machinery. Native title stays, title_en NULL.
                errors.append(f"translate {a.url}: {e!s}")
        for fut in not_done:
            fut.cancel()  # deadline: native title stays, title_en NULL (fail-soft)
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    log.info(
        "translate: %d/%d foreign items translated (cap %d, %d already done skipped, "
        "%d over cap deferred, %d timed out)",
        translated, len(selected), TRANSLATE_CAP, skipped, over_cap, timed_out,
    )
    return translated


def run_search(
    *,
    include_google_news: bool = True,
    fast_mode: bool = False,
    hours_override: int | None = None,
) -> dict:
    """Executa uma busca completa. Retorna estatisticas.

    Pipeline em 3 fases paralelas:
    1. Collect streamed: feeds retornam assim que disponivel.
    2. Pre-resolucao GNews (paralela ao collect): URLs news.google.com sao
       decodificadas com resolve_ex (RESOLVE_WORKERS=6) para nao causar
       rate-limit no Google. Itens nao-GNews vao direto para o enrich.
    3. Enrich: fetch_html nas URLs reais (48 workers, 12s deadline).

    fast_mode=True (usado pela pagina /headlines): passa need_snippet=False
    ao enrich_item, que retorna sem fetch_html quando title+published ja
    vieram do RSS. Em itens "bem formados" do feed, isso elimina a fase 3
    inteira; o que sobra e o proprio collect + resolucao GNews. Os cards
    da pagina de headlines so mostram titulo, fonte e link — snippet vazio
    nao prejudica a UI daquela rota.

    hours_override: quando nao-None, ignora cfg["window_hours"]. Usado pelo
    auto-refresh (sempre 24h leve) — mantem a janela maior do dropdown
    apenas como filtro de exibicao, sem sobrecarregar scans periodicos.
    """
    cfg = get_config()
    keywords: list[str] = cfg["keywords"]
    # Whole-word match opt-in (added 2026-05-20). Keywords flagged 'exact' in
    # news_hunter_keywords.match_type are matched with \\b{kw}\\b; the rest are
    # substring (legacy behaviour). Empty set means "all substring" — same as
    # before the feature shipped.
    exact_keywords: set[str] = set(cfg.get("exact_keywords") or set())
    hours: int = (
        hours_override if hours_override is not None else int(cfg["window_hours"])
    )

    # --- Multilingual matching union (§2.3) ---------------------------------
    # RETRIEVAL stays per-language (iter_collect asks Google for the live PT set
    # per PT domain and each LangConfig's curated native vocab per foreign
    # domain). MATCHING, however, must see every foreign native term or a foreign
    # item can never pass the sieve — so _keep_candidate / Stage 4 / lede rescue
    # all run over this UNION. Foreign terms are non-Latin substrings, so testing
    # them against PT/EN text adds zero false positives; exact_keywords is
    # unchanged (all foreign terms are substring — \b-exact is broken for Arabic).
    native_terms = [
        nat
        for c in LANGUAGES.values() if c.translate
        for nat, _canon in c.keyword_priority
    ]
    match_keywords: list[str] = list(keywords) + native_terms
    # native term -> canonical English concept, written into matched_keywords so
    # the /news-hunter feed's keyword scope surfaces a foreign story (§2.4). en
    # maps to itself (identity), so including it is a harmless no-op.
    native_to_canon: dict[str, str] = {
        nat: canon
        for c in LANGUAGES.values()
        for nat, canon in c.keyword_priority
    }

    run_id = start_run()
    errors: list[str] = []
    n_new = 0
    n_upserted = 0
    n_lede_ok = 0
    n_translated = 0
    n_backfilled = 0

    try:
        t0 = time.time()
        # (item, matched, snippet, published, resolved_url, resolved_domain, extracted_title)
        enriched: list[tuple[RawItem, list[str], str, datetime | None, str, str, str]] = []
        seen_urls: set[str] = set()
        pending: dict = {}          # future -> (item, matched)  — enrich_ex
        pending_resolve: dict = {}  # future -> (item, matched)  — resolve_ex
        # Itens near-miss (titulo+summary nao casaram) candidatos a lede rescue.
        lede_candidates: list[RawItem] = []

        enrich_ex = ThreadPoolExecutor(max_workers=ENRICH_WORKERS)
        resolve_ex = ThreadPoolExecutor(max_workers=RESOLVE_WORKERS)
        n_raw = 0
        n_cand = 0
        n_cache = 0
        n_resolved_ok = 0
        # Limita enriquecimentos por dominio.
        # Homepage scrapers (published_at=None) usam cap maior: sao paginas ja
        # topicas, entao vale a pena aceitar muito de cada uma.
        _enrich_count: dict[str, int] = {}
        _ENRICH_CAP = 20           # RSS, GNews, sitemaps
        _ENRICH_CAP_HOMEPAGE = 40  # homepage scrapers (sites topicos, aceita muito)
        # Contador SEPARADO para a fase 2a (resolucao de wrappers do Google
        # News). Antes as duas fases dividiam `_enrich_count`, e isso gastava o
        # cap DUAS VEZES para o mesmo artigo: uma ao submeter a resolucao e
        # outra ao submeter o enrich da URL ja resolvida. Consequencia: um
        # dominio coberto SO por Google News que produzisse >= 20 candidatos
        # esgotava o cap so com resolucoes e nao sobrava nenhuma vaga de enrich
        # — ou seja, quanto MAIS o dominio rendia, MENOS artigos ele
        # persistia, ate zero. Medido em 2026-08-04: br.investing.com devolvia
        # 100 itens frescos por scan e persistia ZERO; www.reuters.com caiu de
        # 10 para 1 assim que a query passou a devolver 100 frescos em vez de
        # 20. As duas fases limitam recursos diferentes (chamadas ao
        # gnewsdecoder vs. fetch_html), entao tem contadores diferentes.
        _resolve_count: dict[str, int] = {}
        _RESOLVE_CAP = 20
        try:
            for dom, items, err in iter_collect(
                keywords, hours, include_google_news=include_google_news
            ):
                if err:
                    errors.append(err)
                if not items:
                    continue
                # Filtra items novos e aplica keyword match
                batch_keys: list[str] = []
                batch: list[tuple[RawItem, list[str], str]] = []
                for it in items:
                    n_raw += 1
                    key = normalize_url(it.url)
                    if key in seen_urls:
                        continue
                    seen_urls.add(key)
                    matched = _keep_candidate(
                        it, match_keywords, hours, exact_keywords,
                        allow_lede_rescue=True,
                    )
                    if matched is None:
                        continue
                    # Near-miss: nao casou titulo/summary mas e RSS bem-formado.
                    # Desviado para a fase de lede rescue (capada) mais abaixo.
                    if matched == [LEDE_RESCUE_MARKER]:
                        lede_candidates.append(it)
                        continue
                    n_cand += 1
                    batch_keys.append(key)
                    batch.append((it, matched, key))

                if not batch_keys:
                    continue
                # Lookup cache para o batch
                cached = get_cached_snippets(batch_keys)
                for it, matched, key in batch:
                    hit = cached.get(key)
                    if hit is not None and hit[0]:
                        n_cache += 1
                        sn, pub, domres, cached_title = hit
                        enriched.append((it, matched, sn, pub or it.published_at, key, domres, cached_title))
                    elif it.url.startswith("https://news.google.com/"):
                        # Fase 2a: resolucao separada (max 6 paralelas → sem rate-limit).
                        # Cap PROPRIO — ver _resolve_count acima.
                        _cnt = _resolve_count.get(it.source_domain, 0)
                        if _cnt >= _RESOLVE_CAP:
                            continue
                        _resolve_count[it.source_domain] = _cnt + 1
                        fut = resolve_ex.submit(_resolve_google_news_url, it.url)
                        pending_resolve[fut] = (it, matched)
                    else:
                        # Fase 2b: enrich direto (URL real, sem gnewsdecoder)
                        _cap = _ENRICH_CAP_HOMEPAGE if it.published_at is None else _ENRICH_CAP
                        _cnt = _enrich_count.get(it.source_domain, 0)
                        if _cnt >= _cap:
                            continue
                        _enrich_count[it.source_domain] = _cnt + 1
                        fut = enrich_ex.submit(
                            enrich_item, it,
                            resolve_google_news=False,
                            need_snippet=not fast_mode,
                        )
                        pending[fut] = (it, matched)

            # --- Fase 2a: aguarda resolucoes restantes ---
            done_resolve, nd_resolve = wait(
                pending_resolve.keys(), timeout=RESOLVE_EXTRA
            )
            for fut in done_resolve:
                it, matched = pending_resolve[fut]
                try:
                    resolved_url, resolved_domain = fut.result()
                except Exception as e:  # noqa: BLE001
                    errors.append(f"resolve {it.url}: {e!s}")
                    resolved_url, resolved_domain = it.url, it.source_domain
                if resolved_url.startswith("https://news.google.com/"):
                    continue  # nao resolvido — descarta
                _cnt = _enrich_count.get(resolved_domain, 0)
                if _cnt >= _ENRICH_CAP:
                    continue
                _enrich_count[resolved_domain] = _cnt + 1
                n_resolved_ok += 1
                resolved_it = RawItem(
                    url=resolved_url,
                    title=it.title,
                    summary=it.summary,
                    published_at=it.published_at,
                    source_domain=resolved_domain,
                    feed_domain=it.feed_domain,
                    # Carry the language tag across the GNews resolve step, or
                    # every foreign item (which ALWAYS arrives via the GNews
                    # `site:` route) would lose it here and never translate.
                    source_lang=it.source_lang,
                )
                fut2 = enrich_ex.submit(
                    enrich_item, resolved_it,
                    resolve_google_news=False,
                    need_snippet=not fast_mode,
                )
                pending[fut2] = (resolved_it, matched)
            for fut in nd_resolve:
                fut.cancel()

            log.info(
                "Candidatos: %d (brutos %d) | cache_hit=%d resolve=%d(ok=%d) enrich=%d",
                n_cand, n_raw, n_cache, len(pending_resolve), n_resolved_ok, len(pending),
            )

            # --- Fase 3: aguarda enrich ---
            done, not_done = wait(pending.keys(), timeout=ENRICH_DEADLINE)
            for fut in done:
                it, matched = pending[fut]
                try:
                    snippet, published, resolved_url, resolved_domain, ext_title = fut.result()
                except Exception as e:  # noqa: BLE001
                    errors.append(f"enrich {it.url}: {e!s}")
                    snippet = ""
                    published = it.published_at
                    resolved_url = it.url
                    resolved_domain = it.source_domain
                    ext_title = ""
                enriched.append((it, matched, snippet, published, resolved_url, resolved_domain, ext_title))
            # Items que nao terminaram a tempo sao descartados. Antes salvavamos
            # homepage scrapers com published=now, mas isso fazia artigos VELHOS
            # (fixados em secoes) aparecerem como "agora".
            for fut in not_done:
                fut.cancel()

            # --- Fase 3b: lede rescue (added 2026-06-09) -----------------
            # Near-miss de RSS (titulo + summary nao casaram). Busca o corpo
            # (lede) de um subconjunto CAPADO e re-valida keyword. Capta
            # artigos cuja keyword vive so no primeiro paragrafo — caso eixos
            # "Comece seu dia", titulo editorial + description < 150 chars.
            if lede_candidates:
                n_lede_ok = _run_lede_rescue(
                    lede_candidates,
                    match_keywords,
                    exact_keywords,
                    enriched,
                    errors,
                )
        finally:
            resolve_ex.shutdown(wait=False, cancel_futures=True)
            enrich_ex.shutdown(wait=False, cancel_futures=True)

        # Stage 4: janela final + validacao de titulo/snippet.
        # Homepage scrapers apontam para paginas topicas — nao exigem snippet
        # nem re-validacao de keyword (a pagina em si garante relevancia).
        now = datetime.now(timezone.utc)
        to_persist: list[Article] = []
        n_dropped_blind = 0
        for it, matched, snippet, published, resolved_url, resolved_domain, ext_title in enriched:
            is_topic = it.feed_domain in HOMEPAGE_SCRAPERS
            # Titulo real, em ordem de confianca: feed > pagina > listagem.
            # (O fallback de slug NAO conta como titulo real — ver abaixo.)
            real_title = it.title or ext_title or it.title_hint

            # Itens de scraper de listagem chegam aqui sabendo APENAS que existe
            # um link numa pagina. Titulo da pagina ou corpo sao a prova de que
            # o artigo foi de fato alcancado. Sem nenhum dos dois o fetch falhou
            # (404 de slug-variante, timeout, sessao caida) e nao ha noticia —
            # so um slug. Descarta; o item segue na listagem e o proximo scan
            # (~5 min) tenta de novo.
            if is_topic and not (snippet or ext_title):
                n_dropped_blind += 1
                continue

            # Data, em ordem de confianca:
            #   1. a real (feed ou meta/JSON-LD da pagina)
            #   2. a que a LISTAGEM imprimiu (dia exato, verificavel, estavel)
            #   3. now(), so para paginas "ultimas noticias" e so como carimbo
            #      de PRIMEIRA DESCOBERTA (published_is_approx -> o sink nunca
            #      re-carimba uma linha que ja existe).
            published_is_approx = False
            if published is None:
                if it.published_hint is not None:
                    published = it.published_hint
                elif it.feed_domain in RECENT_ONLY_SCRAPERS and real_title:
                    # Caso legitimo: paywall/bot-detection impediu ler a data,
                    # mas sabemos o titulo e a pagina so lista material recente.
                    published = now
                    published_is_approx = True
                else:
                    # Sem titulo real e sem data nao ha noticia: o que sobraria
                    # e um slug carimbado com a hora de agora. Descarta.
                    continue
            if not within_window(published, hours):
                continue
            # Wrapper Google News nao resolvido = link quebrado, descarta.
            if resolved_url.startswith("https://news.google.com/"):
                continue
            # Para fontes NAO topicas, snippet e obrigatorio (garante relevancia
            # via re-validacao de keyword). Para homepage scrapers, aceita sem.
            # Em fast_mode (headlines), aceita sem snippet: a relevancia ja foi
            # validada no _keep_candidate via titulo ou summary do RSS.
            if not snippet and not is_topic and not fast_mode:
                continue

            # Titulo real (feed > pagina > listagem) ou, em ultimo caso, o slug
            # da URL. O slug so e aceitavel porque, chegando aqui, a data e
            # REAL (nao fabricada): e o caso dos sitemaps WordPress, cujo
            # <lastmod> da a data mesmo quando o fetch da pagina falha.
            if real_title:
                display_title = real_title
            else:
                # Fallback: slug da URL. urldecode percent-escapes (%C3%A1 -> á)
                # e capitaliza como sentence case — senao fica "projeto cine petrobras...".
                slug = resolved_url.rstrip("/").rsplit("/", 1)[-1]
                raw = unquote(slug).replace("-", " ").strip()
                display_title = raw[:1].upper() + raw[1:] if raw else resolved_url

            # Clampa published_at no futuro: alguns sites (ex.: agencia.petrobras)
            # publicam metadata com timestamp agendado. Evita "há -51 min".
            # Tambem e um valor fabricado — marca como aproximado para nao ser
            # re-carimbado a cada scan numa linha que ja existe.
            if published and published > now + timedelta(minutes=5):
                published = now
                published_is_approx = True

            final_hay = f"{display_title} \n {snippet}"
            final_match = matches_keywords(final_hay, match_keywords, exact_keywords)
            if is_topic:
                # Site ja e topico — se nao casou keyword especifica, marca como #topic.
                if not final_match:
                    final_match = ["#topic"]
            elif not final_match:
                # Sem snippet (fast_mode) items so casam via titulo; se _keep_candidate
                # aprovou apenas pelo summary RSS, mantem os keywords originais.
                if fast_mode and matched:
                    final_match = matched
                else:
                    # Re-validacao estrita para fontes genericas.
                    continue
            # Rewrite native foreign terms to their canonical English concept so
            # the /news-hunter feed's keyword scope surfaces the item (§2.4):
            # 'نفط' -> 'oil'. Sentinels (#topic/#pending) and Latin keywords map
            # to themselves. Dedupe preserving order (both غاز and حقل غاز -> gas).
            canon_match: list[str] = []
            for k in final_match:
                c = native_to_canon.get(k, k)
                if c not in canon_match:
                    canon_match.append(c)
            final_match = canon_match
            to_persist.append(Article(
                url=normalize_url(resolved_url),
                domain=resolved_domain,
                source_name=source_name_for(resolved_domain),
                title=display_title,
                snippet=snippet,
                published_at=published,
                found_at=now,
                matched_keywords=final_match,
                published_is_approx=published_is_approx,
                source_lang=it.source_lang,
            ))

        if n_dropped_blind:
            # Observabilidade: um scraper de listagem que passa a falhar por
            # completo (sessao expirada, WAF novo) agora some do feed em vez de
            # entupi-lo com slugs. Sem esta linha isso seria silencioso.
            log.info(
                "%d itens de listagem descartados sem titulo/corpo/data "
                "(enrich falhou; serao re-tentados no proximo scan)",
                n_dropped_blind,
            )

        # --- Stage 3d: snippet backfill (added 2026-08-20) -------------------
        # Espelho do lede rescue para quem casou no TITULO: busca o corpo dos
        # aprovados que continuam sem snippet. Antes da traducao de proposito,
        # para que um item estrangeiro backfillado ja saia com snippet_en.
        n_backfilled = _run_snippet_backfill(to_persist, errors)

        # --- Stage 3c: translate the kept FOREIGN items to English (§3) ------
        # Runs on the final kept set, after enrich + lede — never the firehose.
        # Fail-soft: leaves title_en NULL and keeps the native row on any error.
        n_translated = _run_translation(to_persist, errors)

        n_new = upsert_articles(to_persist)
        n_upserted = len(to_persist)

    except Exception as e:  # noqa: BLE001
        log.exception("Falha na busca")
        errors.append(f"fatal: {e!s}")
    finally:
        finish_run(run_id, n_upserted, errors[:200])  # cap de erros no historico
        # finish_run is a no-op in cloud mode (no runs table), so without this
        # the errors list dies here and the service only ever prints its LENGTH
        # ("errors=2"), which names nothing and is impossible to act on.
        if errors:
            head = "; ".join(errors[:10])
            log.info(
                "errors (%d): %s%s",
                len(errors), head, " ..." if len(errors) > 10 else "",
            )

    return {
        "run_id": run_id,
        "n_new": n_new,
        "n_total": n_upserted,
        "errors": errors,
        "window_hours": hours,
        "keywords_count": len(keywords),
        "exact_keywords_count": len(exact_keywords),
        "lede_rescued": n_lede_ok,
        "snippets_backfilled": n_backfilled,
        "translated": n_translated,
    }


# Janela fixa do auto-scan: scans sempre usam 24h. O frontend filtra por
# janela menor no cliente quando necessario.
AUTO_SCAN_HOURS = 24
