"""Stateless shim sobre Supabase. Substitui o SQLite local do Clipinator.

O scanner cloud e efemero — nenhum estado persistido localmente. Toda a
dedup e cache vem do Supabase:

  - news_articles.url (PRIMARY KEY)      -> dedupe automatico via UPSERT
  - news_hunter_keywords (UNION de users) -> lista de keywords para scanear

Mantemos as mesmas exports que `pipeline.py` importa para nao precisar
refatorar a pipeline: Article, normalize_url, get_config, start_run,
finish_run, get_cached_snippets, upsert_articles.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .config import DEFAULT_KEYWORDS, DEFAULT_WINDOW_HOURS
from . import supabase_sync

log = logging.getLogger(__name__)


TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src",
    "__twitter_impression",
}


def normalize_url(url: str) -> str:
    """Remove fragment, tracking params e 'www.' do host para dedupe estavel."""
    try:
        p = urlparse(url)
    except ValueError:
        return url
    netloc = p.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in TRACKING_PARAMS]
    return urlunparse((p.scheme, netloc, p.path.rstrip("/") or p.path, p.params, urlencode(query), ""))


@dataclass
class Article:
    url: str
    domain: str
    source_name: str
    title: str
    snippet: str
    published_at: datetime | None
    found_at: datetime
    matched_keywords: list[str] = field(default_factory=list)

    @property
    def published_iso(self) -> str | None:
        return self.published_at.isoformat() if self.published_at else None


def get_config() -> dict:
    """Returns {'keywords': [...], 'exact_keywords': {...}, 'window_hours': 24}.

    Keywords sao a UNION de todos os usuarios autenticados (SELECT keyword,
    match_type FROM news_hunter_keywords). Cada linha pode marcar a keyword
    como:
      - 'substring' (default): match substring case-insensitive (comportamento
        legado). Fica em `keywords` mas NAO em `exact_keywords`.
      - 'exact': match whole-word, regex `\\b{kw}\\b` case-insensitive. Fica em
        ambos os sets para que o filter compile uma alternation \\b-bounded.

    Se a mesma keyword aparece em multiplos users com match_types diferentes
    (ex: user A 'ANS' substring, user B 'ANS' exact), o agregado promove para
    'exact' (regra "qualquer usuario pediu exact -> respeita"). Isso e
    conservador: produz menos artigos do que se promovesse para substring.

    Fallback para DEFAULT_KEYWORDS (todas substring) quando:
      - Supabase nao configurado (dev local)
      - Query falha
      - Tabela vazia (nenhum usuario fez login ainda)
    Coluna `match_type` pode nao existir ainda (scanner deployado antes da
    migration) — neste caso `r.get('match_type')` retorna None e tratamos
    como 'substring' (zero impacto pre-migration).
    """
    sink = supabase_sync.get_sink()
    if sink.client is None:
        return {
            "keywords": list(DEFAULT_KEYWORDS),
            "exact_keywords": set(),
            "window_hours": DEFAULT_WINDOW_HOURS,
        }

    kws: list[str]
    exact: set[str] = set()
    try:
        # Supabase-py nao tem DISTINCT nativo; pega tudo e dedup aqui.
        # Volume esperado: <100 usuarios * ~30 keywords = <3000 rows.
        # A cada scan e ~50ms; aceitavel para o loop de 30s.
        try:
            res = sink.client.table("news_hunter_keywords").select(
                "keyword, match_type"
            ).execute()
        except Exception as col_err:  # noqa: BLE001
            # Provavel `column "match_type" does not exist` se o scanner subir
            # antes da migration. Cai para o SELECT antigo (so keyword).
            log.info(
                "select(keyword,match_type) falhou (%s) — caindo para select(keyword)",
                col_err,
            )
            res = sink.client.table("news_hunter_keywords").select("keyword").execute()
        rows = res.data or []
        kw_set: set[str] = set()
        for r in rows:
            kw = r.get("keyword")
            if not kw:
                continue
            kw_set.add(kw)
            if r.get("match_type") == "exact":
                exact.add(kw)
        kws = sorted(kw_set)
        if not kws:
            log.info("news_hunter_keywords vazio — usando DEFAULT_KEYWORDS")
            kws = list(DEFAULT_KEYWORDS)
            exact = set()
    except Exception as e:  # noqa: BLE001
        log.warning("get_config falhou, caindo em DEFAULT_KEYWORDS: %s", e)
        kws = list(DEFAULT_KEYWORDS)
        exact = set()

    return {
        "keywords": kws,
        "exact_keywords": exact,
        "window_hours": DEFAULT_WINDOW_HOURS,
    }


def get_cached_snippets(urls: Iterable[str]) -> dict[str, tuple[str, datetime | None, str, str]]:
    """Stateless: sempre vazio.

    O container cloud nao persiste — cada scan re-enriquece todos os candidatos.
    Custo: fetch_html em ate ~50 URLs/scan (cap ENRICH_CAP). Com 30s de
    intervalo e 24 workers, fica bem dentro do orcamento.
    """
    return {}


def start_run() -> int:
    """No-op: sem tabela `runs` no Supabase. Retorna 0 como placeholder."""
    return 0


def finish_run(run_id: int, n_found: int, errors: list[str]) -> None:
    """No-op: sem tabela `runs`. Estatisticas sao emitidas via stdout no service."""
    return


def upsert_articles(articles: list[Article]) -> int:
    """Push em lote para Supabase. Retorna numero de rows enviadas.

    Diferente do original SQLite que retornava `n_new` (row que nao existia):
    aqui retornamos `n_pushed` porque em modo stateless nao sabemos o que ja
    existe antes do UPSERT. Operacionalmente equivalente para monitoring.
    """
    if not articles:
        return 0
    try:
        return supabase_sync.push_new(articles)
    except Exception as e:  # noqa: BLE001
        log.warning("upsert_articles falhou: %s", e)
        return 0
