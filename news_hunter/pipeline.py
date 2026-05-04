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
from .sources import HOMEPAGE_SCRAPERS, RECENT_ONLY_SCRAPERS
from .store import (
    Article,
    finish_run,
    get_cached_snippets,
    get_config,
    normalize_url,
    start_run,
    upsert_articles,
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


def _keep_candidate(item: RawItem, keywords: list[str], hours: int) -> list[str] | None:
    """Filtragem barata pre-enriquecimento.

    Retorna a lista de keywords que casaram, ou None se deve descartar.
    Itens sem titulo E sem summary (sitemaps WordPress padrao) recebem
    ["#pending"] — keyword check real ocorre no stage 4 apos enriquecimento.
    """
    # Janela primeiro: filtra a maioria dos itens sem pagar custo de regex.
    if item.published_at is not None and not within_window(item.published_at, hours):
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
            path_match = matches_keywords(path_text, keywords)
            return path_match if path_match else None
        # Sitemaps WordPress padrao: usa apenas o slug (ultimo segmento)
        slug = path.rstrip("/").rsplit("/", 1)[-1].replace("-", " ")
        slug_match = matches_keywords(slug, keywords)
        return slug_match if slug_match else None
    # Title-first: titulos sao curtos. Se casou, retorna imediatamente.
    matched = matches_keywords(item.title, keywords)
    if matched:
        return matched
    clean_summary = strip_related(item.summary)
    if not clean_summary:
        return None
    return matches_keywords(clean_summary, keywords) or None


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
    hours: int = (
        hours_override if hours_override is not None else int(cfg["window_hours"])
    )

    run_id = start_run()
    errors: list[str] = []
    n_new = 0
    n_upserted = 0

    try:
        t0 = time.time()
        # (item, matched, snippet, published, resolved_url, resolved_domain, extracted_title)
        enriched: list[tuple[RawItem, list[str], str, datetime | None, str, str, str]] = []
        seen_urls: set[str] = set()
        pending: dict = {}          # future -> (item, matched)  — enrich_ex
        pending_resolve: dict = {}  # future -> (item, matched)  — resolve_ex

        enrich_ex = ThreadPoolExecutor(max_workers=ENRICH_WORKERS)
        resolve_ex = ThreadPoolExecutor(max_workers=RESOLVE_WORKERS)
        n_raw = 0
        n_cand = 0
        n_cache = 0
        n_resolved_ok = 0
        # Limita enriquecimentos por dominio.
        # Homepage scrapers (published_at=None) usam cap menor: servidores lentos
        # podem travar threads por 15s+ em fetchs sem retorno rapido.
        _enrich_count: dict[str, int] = {}
        _ENRICH_CAP = 20           # RSS, GNews, sitemaps
        _ENRICH_CAP_HOMEPAGE = 40  # homepage scrapers (sites topicos, aceita muito)
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
                    matched = _keep_candidate(it, keywords, hours)
                    if matched is None:
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
                    else:
                        _cap = _ENRICH_CAP_HOMEPAGE if it.published_at is None else _ENRICH_CAP
                        _cnt = _enrich_count.get(it.source_domain, 0)
                        if _cnt >= _cap:
                            continue
                        _enrich_count[it.source_domain] = _cnt + 1
                        if it.url.startswith("https://news.google.com/"):
                            # Fase 2a: resolucao separada (max 6 paralelas → sem rate-limit)
                            fut = resolve_ex.submit(_resolve_google_news_url, it.url)
                            pending_resolve[fut] = (it, matched)
                        else:
                            # Fase 2b: enrich direto (URL real, sem gnewsdecoder)
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
        finally:
            resolve_ex.shutdown(wait=False, cancel_futures=True)
            enrich_ex.shutdown(wait=False, cancel_futures=True)

        # Stage 4: janela final + validacao de titulo/snippet.
        # Homepage scrapers apontam para paginas topicas — nao exigem snippet
        # nem re-validacao de keyword (a pagina em si garante relevancia).
        now = datetime.now(timezone.utc)
        to_persist: list[Article] = []
        for it, matched, snippet, published, resolved_url, resolved_domain, ext_title in enriched:
            is_topic = it.feed_domain in HOMEPAGE_SCRAPERS
            # Data real obrigatoria para a maioria das fontes.
            # Excecao: scrapers de paginas "ultimas noticias" (RECENT_ONLY_SCRAPERS,
            # ex.: brasilenergia.com.br/petroleoegas/ultimasnoticias) so listam
            # artigos recentes — quando enrich falha por paywall/bot-detection,
            # usamos now() como aproximacao em vez de descartar.
            if published is None:
                if it.feed_domain in RECENT_ONLY_SCRAPERS:
                    published = now
                else:
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

            # Titulo real: item original > extraido pela pagina > slug da URL
            if it.title:
                display_title = it.title
            elif ext_title:
                display_title = ext_title
            else:
                # Fallback: slug da URL. urldecode percent-escapes (%C3%A1 -> á)
                # e capitaliza como sentence case — senao fica "projeto cine petrobras...".
                slug = resolved_url.rstrip("/").rsplit("/", 1)[-1]
                raw = unquote(slug).replace("-", " ").strip()
                display_title = raw[:1].upper() + raw[1:] if raw else resolved_url

            # Clampa published_at no futuro: alguns sites (ex.: agencia.petrobras)
            # publicam metadata com timestamp agendado. Evita "há -51 min".
            if published and published > now + timedelta(minutes=5):
                published = now

            final_hay = f"{display_title} \n {snippet}"
            final_match = matches_keywords(final_hay, keywords)
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
            to_persist.append(Article(
                url=normalize_url(resolved_url),
                domain=resolved_domain,
                source_name=source_name_for(resolved_domain),
                title=display_title,
                snippet=snippet,
                published_at=published,
                found_at=now,
                matched_keywords=final_match,
            ))

        n_new = upsert_articles(to_persist)
        n_upserted = len(to_persist)

    except Exception as e:  # noqa: BLE001
        log.exception("Falha na busca")
        errors.append(f"fatal: {e!s}")
    finally:
        finish_run(run_id, n_upserted, errors[:200])  # cap de erros no historico

    return {
        "run_id": run_id,
        "n_new": n_new,
        "n_total": n_upserted,
        "errors": errors,
        "window_hours": hours,
        "keywords_count": len(keywords),
    }


# Janela fixa do auto-scan: scans sempre usam 24h. O frontend filtra por
# janela menor no cliente quando necessario.
AUTO_SCAN_HOURS = 24
