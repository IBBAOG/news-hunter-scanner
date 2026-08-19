"""Push de artigos novos para uma tabela `news_articles` no Supabase.

Integracao opcional: sem SUPABASE_URL + SUPABASE_SERVICE_KEY no ambiente, o
scanner segue funcionando 100% em modo local (SQLite). Falhas de rede/auth
tambem sao silenciosas — nunca derrubam o pipeline local.

Bandwidth budget (Supabase free = 5 GB/mes):
- So pushamos artigos NOVOS (flag `is_new` devolvido pelo upsert SQLite).
- Atualizacoes de titulo/snippet em artigos ja persistidos nao re-pushamos
  (raras apos o primeiro enrich — economia relevante em polls de 30s).
- Batch upsert: uma requisicao por scan, independente do tamanho da lista.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import Article

log = logging.getLogger(__name__)

# Cap de seguranca: nunca pusha mais que isso de uma vez. Defensivo contra
# primeiro-scan-da-vida (DB vazio) que pode produzir 200+ artigos novos.
MAX_BATCH = 100

_lock = threading.Lock()
_sink: "_SupabaseSink | None" = None
_tried_init = False


class _SupabaseSink:
    """Cliente Supabase lazy. Silencioso quando nao configurado."""

    def __init__(self) -> None:
        self.client = None
        self.table = "news_articles"
        url = os.environ.get("SUPABASE_URL", "").strip()
        key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
        if not url or not key:
            log.info("Supabase desabilitado (SUPABASE_URL/SUPABASE_SERVICE_KEY ausentes)")
            return
        try:
            from supabase import create_client  # type: ignore[import-untyped]
            self.client = create_client(url, key)
            log.info("Supabase habilitado (target: %s)", url)
        except ImportError:
            log.warning("Pacote 'supabase' nao instalado — pip install supabase")
        except Exception as e:  # noqa: BLE001
            log.warning("Supabase init falhou: %s", e)

    def _existing_published_at(self, urls: list[str]) -> dict[str, str] | None:
        """Le published_at das linhas que JA existem. None se a consulta falhar.

        Devolve o valor cru (ISO string) — vai direto de volta no payload do
        upsert, sem round-trip por datetime.
        """
        if self.client is None or not urls:
            return {}
        out: dict[str, str] = {}
        for i in range(0, len(urls), 100):
            chunk = urls[i:i + 100]
            try:
                res = (
                    self.client.table(self.table)
                    .select("url, published_at")
                    .in_("url", chunk)
                    .execute()
                )
            except Exception as e:  # noqa: BLE001
                log.warning("lookup de published_at falhou (%d urls): %s", len(chunk), e)
                return None
            for r in res.data or []:
                url, pub = r.get("url"), r.get("published_at")
                if url and pub:
                    out[url] = pub
        return out

    def _freeze_approx_dates(
        self, articles: list["Article"]
    ) -> tuple[list["Article"], dict[str, str]]:
        """Write-once para datas fabricadas.

        Um published_at aproximado (now() de primeira descoberta, ou clamp de
        timestamp futuro) e um CHUTE, nao um fato. Re-aplica-lo a cada scan faz
        o artigo se auto-renovar para sempre: como a data e sempre "agora", ele
        nunca sai da janela de 24h, logo continua sendo re-descoberto e
        re-carimbado — foi assim que noticias do Brasil Energia de 28-29/07
        apareceram no topo do feed em 04/08 como "13m ago".

        Regra: se a linha ja existe com uma data, ela vence. Se e nova, o
        carimbo aproximado entra (uma unica vez). Uma data REAL obtida num scan
        futuro sobrescreve normalmente — o item continua curavel.

        Se o lookup falhar, os aproximados sao deixados de fora deste push (o
        proximo scan tenta de novo em ~5 min): perder uma insercao e reversivel,
        re-carimbar nao e.
        """
        approx = [a for a in articles if a.published_is_approx]
        if not approx:
            return articles, {}
        known = self._existing_published_at([a.url for a in approx])
        if known is None:
            log.warning(
                "push: %d artigos com data aproximada adiados (lookup indisponivel)",
                len(approx),
            )
            return [a for a in articles if not a.published_is_approx], {}
        overrides = {a.url: known[a.url] for a in approx if known.get(a.url)}
        if overrides:
            log.info(
                "push: %d/%d datas aproximadas preservadas da linha existente",
                len(overrides), len(approx),
            )
        return articles, overrides

    def _existing_translations(
        self, urls: list[str]
    ) -> dict[str, dict] | None:
        """Le o overlay multilingue das linhas que JA existem.

        Devolve {url: {source_lang, title_original, title_en, snippet_en}} para
        as urls encontradas (dicts crus do PostgREST). {} quando o cliente nao
        esta configurado. None quando a consulta FALHA — o chamador entao adia
        as linhas em risco, igual _freeze_approx_dates faz com datas.
        """
        if self.client is None or not urls:
            return {}
        out: dict[str, dict] = {}
        for i in range(0, len(urls), 100):
            chunk = urls[i:i + 100]
            try:
                res = (
                    self.client.table(self.table)
                    .select("url, source_lang, title_original, title_en, snippet_en")
                    .in_("url", chunk)
                    .execute()
                )
            except Exception as e:  # noqa: BLE001
                log.warning("lookup de traducoes falhou (%d urls): %s", len(chunk), e)
                return None
            for r in res.data or []:
                u = r.get("url")
                if u:
                    out[u] = r
        return out

    def _preserve_translations(
        self, articles: list["Article"]
    ) -> tuple[list["Article"], dict[str, dict]]:
        """Write-once para traducoes: nunca sobrescreve uma traducao com NULL.

        Uma linha estrangeira traduzida no scan N, re-vista no scan N+1 enquanto
        ACIMA do cap de traducao (nao selecionada), chega aqui com title_en/
        snippet_en None. Escrita crua, ela ANULA o overlay ingles ja gravado — a
        manchete do feed /news-hunter volta a piscar no idioma nativo. Espelha o
        _freeze_approx_dates: le o overlay gravado e faz COALESCE por baixo dos
        campos vazios do candidato (o valor gravado vence quando o novo e vazio;
        uma re-traducao legitima, com title_en preenchido, ainda sobrescreve).

        So linhas que PODEM regredir sao consultadas: estrangeiras ou de tag
        legada (nunca pt/en, que jamais carregam overlay ingles) cujo title_en
        OU snippet_en de entrada esteja vazio.

        Devolve (articles_a_enviar, overrides), overrides[url] = so os campos a
        restaurar do banco. Em falha de lookup, as linhas em risco (title_en de
        entrada vazio) sao adiadas DESTE push e re-tentadas no proximo scan —
        adiar um update e reversivel, regredir uma manchete nao e.
        """
        at_risk = [a for a in articles if _may_regress_translation(a)]
        if not at_risk:
            return articles, {}
        stored = self._existing_translations([a.url for a in at_risk])
        if stored is None:
            deferred = {
                a.url for a in at_risk if not (a.title_en or "").strip()
            }
            if deferred:
                log.warning(
                    "push: %d traducoes adiadas (lookup indisponivel — nunca "
                    "sobrescrever com NULL)",
                    len(deferred),
                )
            return [a for a in articles if a.url not in deferred], {}
        overrides: dict[str, dict] = {}
        for a in at_risk:
            row = stored.get(a.url)
            if not row:
                continue  # url nova: nada gravado a preservar, escreve como esta
            keep: dict = {}
            for f in _OVERLAY_FIELDS:
                incoming = getattr(a, f, None)
                is_empty = incoming is None or (
                    isinstance(incoming, str) and not incoming.strip()
                )
                if is_empty and row.get(f):
                    keep[f] = row[f]
            if keep:
                overrides[a.url] = keep
        if overrides:
            n_titles = sum(1 for v in overrides.values() if "title_en" in v)
            log.info(
                "push: %d traducoes preservadas da linha existente "
                "(%d manchetes protegidas de regressao)",
                len(overrides), n_titles,
            )
        return articles, overrides

    def push(self, articles: list["Article"]) -> int:
        """Faz upsert em chunks de MAX_BATCH. Retorna total de rows enviadas.

        Dedup por URL é obrigatório: Postgres rejeita ON CONFLICT DO UPDATE com
        erro 21000 quando a mesma constraint key (url) aparece duas vezes no
        mesmo INSERT — o batch inteiro é abortado. Mantemos o último candidato
        de cada URL (mais recente em `found_at`).
        """
        if self.client is None or not articles:
            return 0
        deduped: dict[str, "Article"] = {}
        for a in articles:
            existing = deduped.get(a.url)
            if existing is None or a.found_at >= existing.found_at:
                deduped[a.url] = a
        unique, date_overrides = self._freeze_approx_dates(list(deduped.values()))
        unique, tx_overrides = self._preserve_translations(unique)
        total = 0
        for i in range(0, len(unique), MAX_BATCH):
            chunk = unique[i:i + MAX_BATCH]
            rows = [
                _article_to_row(
                    a, date_overrides.get(a.url), tx_overrides.get(a.url)
                )
                for a in chunk
            ]
            try:
                self.client.table(self.table).upsert(rows, on_conflict="url").execute()
                total += len(rows)
            except Exception as e:  # noqa: BLE001
                log.warning("Supabase push falhou (%d rows): %s", len(rows), e)
                return total
        return total


# Overlay multilingue (§4). Estes campos NUNCA podem regredir para NULL por cima
# de um valor gravado — write-once, igual ao published_at fabricado. title_en e o
# que um leitor VE piscar de volta ao idioma nativo, mas os quatro sao protegidos
# em bloco via _preserve_translations.
_OVERLAY_FIELDS = ("source_lang", "title_original", "title_en", "snippet_en")


def _may_regress_translation(a: "Article") -> bool:
    """True se escrever esta linha como esta puder ANULAR um overlay ja gravado.

    So linhas com tag ESTRANGEIRA (source_lang setado e != pt/en) carregam
    overlay ingles. Nativas pt/en e untagged/legadas (source_lang None) sao
    isentas — jamais tem traducao gravada, entao consulta-las por scan seria
    varrer o firehose PT inteiro (a maioria das linhas) a toa. Uma linha
    estrangeira com title_en OU snippet_en de entrada vazio e candidata: o banco
    pode ter uma traducao que o candidato (re-visto, acima do cap ou que falhou)
    nao traz.
    """
    lang = (a.source_lang or "").strip().lower()
    if not lang or lang in ("pt", "en"):
        return False
    return (
        not (a.title_en and a.title_en.strip())
        or not (a.snippet_en and a.snippet_en.strip())
    )


def already_translated_urls(urls: list[str]) -> set[str] | None:
    """URLs de `urls` que JA carregam title_en nao-nulo no banco.

    Deixa _run_translation pular re-traducoes: o cap por scan drena o backlog de
    itens ainda-nao-traduzidos monotonicamente, em vez de gastar cada scan nos
    mesmos primeiros N (que podem ja estar prontos) e nunca alcancar a cauda.
    Fail-soft: None quando o Supabase esta indisponivel ou o lookup falha — o
    chamador trata None (e {} vazio) como "nao pular nada" (comportamento antigo;
    o guard de _preserve_translations impede regressao de qualquer forma).
    """
    if not urls:
        return set()
    try:
        stored = get_sink()._existing_translations(urls)
    except Exception as e:  # noqa: BLE001
        log.warning("already_translated_urls falhou: %s", e)
        return None
    if stored is None:
        return None
    return {u for u, r in stored.items() if (r.get("title_en") or "").strip()}


def _article_to_row(
    a: "Article",
    published_override: str | None = None,
    tx_override: dict | None = None,
) -> dict:
    """Serializa Article para o schema da tabela `news_articles`.

    `published_override`: data ja gravada na linha existente, usada quando a
    data do candidato e aproximada (write-once — ver _freeze_approx_dates).
    `tx_override`: overlay multilingue ja gravado, a restaurar por baixo dos
    campos vazios do candidato (write-once — ver _preserve_translations). So
    contem os campos onde o candidato e vazio e o banco tem valor, entao o
    `tx.get(campo, valor_do_candidato)` faz o COALESCE (gravado vence o vazio;
    valor novo nao-vazio do candidato vence — re-traducao continua possivel).
    """
    if published_override:
        published = published_override
    else:
        published = a.published_at.isoformat() if a.published_at else None
    tx = tx_override or {}
    return {
        "url": a.url,
        "domain": a.domain,
        "source_name": a.source_name,
        "title": a.title,
        "snippet": a.snippet,
        "published_at": published,
        "found_at": a.found_at.isoformat(),
        "matched_keywords": list(a.matched_keywords),
        # Multilingual overlay (§4). None serializa para SQL NULL, entao PT/EN e
        # scans legados seguem inalterados. O write-once vive em
        # _preserve_translations: uma linha re-vista acima do cap chega com
        # title_en=None e SERIA gravada como NULL por cima da traducao existente
        # — o COALESCE via tx_override impede exatamente essa regressao.
        "source_lang": tx.get("source_lang", a.source_lang),
        "title_original": tx.get("title_original", a.title_original),
        "title_en": tx.get("title_en", a.title_en),
        "snippet_en": tx.get("snippet_en", a.snippet_en),
    }


def get_sink() -> _SupabaseSink:
    """Singleton: inicializa o cliente na primeira chamada."""
    global _sink, _tried_init
    if _sink is not None:
        return _sink
    with _lock:
        if _sink is None and not _tried_init:
            _tried_init = True
            _sink = _SupabaseSink()
    return _sink  # type: ignore[return-value]


def push_new(articles: list["Article"]) -> int:
    """Helper de alto nivel: pega o sink e envia. Usado por store.upsert_articles.

    Envelopado em try/except — nunca aborta o pipeline local.
    """
    if not articles:
        return 0
    try:
        return get_sink().push(articles)
    except Exception as e:  # noqa: BLE001
        log.warning("push_new falhou: %s", e)
        return 0
