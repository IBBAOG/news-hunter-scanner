"""Stateless shim sobre Supabase. Substitui o SQLite local do Clipinator.

O scanner cloud e efemero - nenhum estado persistido localmente. Toda a
dedup e cache vem do Supabase:

  - news_articles.url (PRIMARY KEY)      -> dedupe automatico via UPSERT
  - news_hunter_default_keywords          -> keywords-padrao globais (com match_type)
  - news_hunter_keywords (UNION de users) -> keywords per-user (com match_type)

Mantemos as mesmas exports que pipeline.py importa para nao precisar
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


def _fetch_default_keywords(sink) -> dict:
    """Fetch global default keywords with match_type from Supabase.

    Calls RPC get_default_news_keywords_with_flags() which returns rows of
    (keyword text, match_type text).  Falls back to direct table SELECT if the
    RPC call fails (e.g. scanner deployed before the RPC was created).

    Returns a dict mapping keyword -> match_type ('substring' | 'exact').
    Empty dict on any failure (caller merges with per-user keywords).
    """
    if sink.client is None:
        return {}
    try:
        res = sink.client.rpc("get_default_news_keywords_with_flags").execute()
        rows = res.data or []
        result: dict = {}
        for r in rows:
            kw = r.get("keyword")
            mt = r.get("match_type") or "substring"
            if kw:
                result[kw] = mt
        log.info("default keywords loaded via RPC: %d entries", len(result))
        return result
    except Exception as rpc_err:  # noqa: BLE001
        log.info(
            "get_default_news_keywords_with_flags RPC failed (%s) -- falling back to direct table SELECT",
            rpc_err,
        )
    try:
        res = sink.client.table("news_hunter_default_keywords").select(
            "keyword, match_type"
        ).execute()
        rows = res.data or []
        result = {}
        for r in rows:
            kw = r.get("keyword")
            mt = r.get("match_type") or "substring"
            if kw:
                result[kw] = mt
        log.info("default keywords loaded via direct table: %d entries", len(result))
        return result
    except Exception as tbl_err:  # noqa: BLE001
        log.warning("default keywords table SELECT also failed: %s", tbl_err)
        return {}


def get_config() -> dict:
    """Returns {'keywords': [...], 'exact_keywords': {...}, 'window_hours': 24}.

    Keywords are the UNION of:
      1. Global defaults from news_hunter_default_keywords (via RPC
         get_default_news_keywords_with_flags, with match_type per row).
      2. Per-user keywords from news_hunter_keywords (UNION of all users,
         with match_type per row).

    match_type aggregation rule: if the same keyword appears in multiple
    sources/users with different match_types, the result is promoted to
    'exact' (conservative: fewer false positives).

    The exact_keywords set contains every keyword whose effective
    match_type is 'exact'.  The keywords list contains all keywords
    regardless of match_type (consumed by the filter as the full set).

    Fallback to DEFAULT_KEYWORDS (all substring, hardcoded in config.py) when:
      - Supabase is not configured (local dev)
      - Both default-keyword fetches fail
      - Both tables are empty

    Accent handling (critical fix -- 2026-05-26):
    Keywords are stored in the DB with their canonical form (accents
    preserved).  The scanner must NOT strip diacritics before matching
    because that turns the Iran keyword (with tilde-n) into 'ira', causing it
    to hit 'diretoria', 'irma-with-tilde', etc. -- generating thousands of
    false positives.  filter.py now applies re.IGNORECASE directly on the
    original text without any NFD normalisation.  The DB ships BOTH
    'petroleo' and 'petroleo-accented' as separate entries for sources that
    omit accents.
    """
    sink = supabase_sync.get_sink()
    if sink.client is None:
        return {
            "keywords": list(DEFAULT_KEYWORDS),
            "exact_keywords": set(),
            "window_hours": DEFAULT_WINDOW_HOURS,
        }

    # Aggregated map: keyword -> effective match_type ('substring' | 'exact').
    # 'exact' wins over 'substring' when the same keyword appears in both.
    aggregated: dict = {}

    # --- Source 1: global default keywords ---
    for kw, mt in _fetch_default_keywords(sink).items():
        if mt == "exact":
            aggregated[kw] = "exact"
        else:
            aggregated.setdefault(kw, "substring")

    # --- Source 2: per-user keywords (UNION of all authenticated users) ---
    try:
        try:
            res = sink.client.table("news_hunter_keywords").select(
                "keyword, match_type"
            ).execute()
        except Exception as col_err:  # noqa: BLE001
            # Likely column "match_type" does not exist if scanner deployed before
            # the migration that added the column. Fall back to keyword-only SELECT.
            log.info(
                "select(keyword,match_type) failed (%s) -- falling back to select(keyword)",
                col_err,
            )
            res = sink.client.table("news_hunter_keywords").select("keyword").execute()
        rows = res.data or []
        for r in rows:
            kw = r.get("keyword")
            if not kw:
                continue
            mt = r.get("match_type") or "substring"
            if mt == "exact":
                aggregated[kw] = "exact"
            else:
                aggregated.setdefault(kw, "substring")
    except Exception as e:  # noqa: BLE001
        log.warning("per-user keywords fetch failed: %s", e)

    if not aggregated:
        log.info("All keyword sources empty -- using DEFAULT_KEYWORDS (all substring)")
        return {
            "keywords": list(DEFAULT_KEYWORDS),
            "exact_keywords": set(),
            "window_hours": DEFAULT_WINDOW_HOURS,
        }

    kws = sorted(aggregated)
    exact: set = {kw for kw, mt in aggregated.items() if mt == "exact"}

    log.info(
        "keywords loaded: total=%d exact=%d substring=%d",
        len(kws),
        len(exact),
        len(kws) - len(exact),
    )
    return {
        "keywords": kws,
        "exact_keywords": exact,
        "window_hours": DEFAULT_WINDOW_HOURS,
    }


def get_cached_snippets(urls) -> dict:
    """Stateless: always returns empty dict.

    The cloud container does not persist state -- each scan re-enriches all
    candidates.  Cost: fetch_html on up to ~50 URLs/scan (ENRICH_CAP).
    With a 30 s interval and 24 workers this stays well within budget.
    """
    return {}


def start_run() -> int:
    """No-op: no runs table in Supabase.  Returns 0 as a placeholder."""
    return 0


def finish_run(run_id: int, n_found: int, errors) -> None:
    """No-op: no runs table.  Stats are emitted to stdout by the service."""
    return


def upsert_articles(articles) -> int:
    """Batch-push to Supabase.  Returns number of rows sent.

    Unlike the original SQLite implementation that returned n_new (rows that
    did not previously exist), we return n_pushed because in stateless mode
    we do not know what already exists before the UPSERT.  Operationally
    equivalent for monitoring purposes.
    """
    if not articles:
        return 0
    try:
        return supabase_sync.push_new(articles)
    except Exception as e:  # noqa: BLE001
        log.warning("upsert_articles failed: %s", e)
        return 0
