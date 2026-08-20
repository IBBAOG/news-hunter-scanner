"""Repair rows that are ALREADY STORED without a body.

Stage 3d (pipeline._run_snippet_backfill) can only reach articles the current
scan still sees, and that is a real ceiling: a Google-News-only source is
re-collected only while Google keeps returning it. Measured 2026-08-20, Brasil
247's newest articles filled within minutes of Stage 3d shipping while its
day-old ones had already dropped out of the `site:` result set and became
unreachable to the live pipeline forever — nothing was wrong with the fetch
(diagnose_snippet pulls 357 chars from that domain without trouble), the
collector simply never offered those rows again.

So this job asks the DATABASE instead of the collector: give me the rows with an
empty snippet, fetch their bodies, write back the snippet and nothing else. It
is the repair pass; Stage 3d is the live one.

Only `snippet` is ever written, with an UPDATE keyed on the url — never an
upsert. A row that exists keeps its date, its matched_keywords and its
translation overlay untouched, so this job cannot regress any of the write-once
guarantees the sink protects.

Usage:
    python -m scripts.backfill_snippets                     # 400 newest empty rows
    python -m scripts.backfill_snippets --limit 1000 --hours 168
    python -m scripts.backfill_snippets --domain www.brasil247.com
    python -m scripts.backfill_snippets --dry-run           # fetch, report, write nothing
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from news_hunter.enrich import enrich_item
from news_hunter.fetcher import RawItem
from news_hunter.pipeline import _backfill_order
from news_hunter.supabase_sync import get_sink

log = logging.getLogger("backfill_snippets")

# Pagination unit for the read. PostgREST caps a response at the project's
# max-rows; asking in pages keeps the job honest about how much it actually saw.
PAGE = 1000


@dataclass
class Row:
    """Minimal shape _backfill_order needs (it only reads .domain/.published_at)."""

    url: str
    domain: str
    published_at: datetime
    snippet: str = ""


def _fetch_empty_rows(sink, hours: int, domain: str | None, limit: int) -> list[Row]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    out: list[Row] = []
    offset = 0
    while len(out) < limit:
        q = (
            sink.client.table(sink.table)
            .select("url, domain, published_at")
            .eq("snippet", "")
            .gt("published_at", cutoff)
            .order("published_at", desc=True)
            .range(offset, offset + PAGE - 1)
        )
        if domain:
            q = q.eq("domain", domain)
        rows = q.execute().data or []
        for r in rows:
            out.append(
                Row(
                    url=r["url"],
                    domain=r["domain"],
                    published_at=datetime.fromisoformat(
                        r["published_at"].replace("Z", "+00:00")
                    ),
                )
            )
        if len(rows) < PAGE:
            break
        offset += PAGE
    return out


def _select(rows: list[Row], limit: int, per_domain: int) -> list[Row]:
    """Round-robin by domain so one firehose cannot consume the whole run.

    Same rule as Stage 3d and for the same reason: on the day this was written
    finance.sina.com.cn held over a third of all bodyless rows, and a plain
    ordering would have spent the entire budget there while the Brazilian
    sources — a handful of articles each — waited behind it.
    """
    picked: list[Row] = []
    seen: Counter = Counter()
    for r in _backfill_order(rows, oldest_first=False):
        if len(picked) >= limit:
            break
        if seen[r.domain] >= per_domain:
            continue
        seen[r.domain] += 1
        picked.append(r)
    return picked


def run(hours: int, limit: int, per_domain: int, domain: str | None,
        workers: int, deadline: float, dry_run: bool) -> int:
    sink = get_sink()
    if sink.client is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY ausentes — nada a fazer")
        return 2

    t0 = time.time()
    rows = _fetch_empty_rows(sink, hours, domain, limit * 4)
    log.info("linhas sem corpo na janela de %dh: %d (%d dominios)",
             hours, len(rows), len({r.domain for r in rows}))
    if not rows:
        return 0

    todo = _select(rows, limit, per_domain)
    log.info("selecionadas: %d de %d dominios (cap %d, %d/dominio)",
             len(todo), len({r.domain for r in todo}), limit, per_domain)

    filled: dict[str, str] = {}
    verdicts: Counter = Counter()
    by_domain_ok: Counter = Counter()
    by_domain_fail: Counter = Counter()

    ex = ThreadPoolExecutor(max_workers=workers)
    try:
        pending = {
            ex.submit(
                enrich_item,
                RawItem(url=r.url, title="", summary="", published_at=r.published_at,
                        source_domain=r.domain, feed_domain=r.domain),
                resolve_google_news=False,
                need_snippet=True,
            ): r
            for r in todo
        }
        done, not_done = wait(pending.keys(), timeout=deadline)
        for fut in done:
            r = pending[fut]
            try:
                snippet, _pub, _u, _d, _t = fut.result()
            except Exception as e:  # noqa: BLE001
                verdicts["fetch_error"] += 1
                by_domain_fail[r.domain] += 1
                log.debug("falhou %s: %s", r.url, e)
                continue
            if snippet and snippet.strip():
                filled[r.url] = snippet
                by_domain_ok[r.domain] += 1
            else:
                verdicts["sem_corpo"] += 1
                by_domain_fail[r.domain] += 1
        for fut in not_done:
            fut.cancel()
            verdicts["timeout"] += 1
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    log.info("corpos obtidos: %d/%d (%s)", len(filled), len(todo),
             ", ".join(f"{k}={v}" for k, v in sorted(verdicts.items())) or "sem falhas")

    written = 0
    if dry_run:
        log.info("--dry-run: nada gravado")
    else:
        for url, snippet in filled.items():
            try:
                # UPDATE, nunca upsert: escreve APENAS o snippet, numa linha que
                # ja existe. Data, matched_keywords e o overlay de traducao ficam
                # exatamente como estao.
                sink.client.table(sink.table).update({"snippet": snippet}).eq("url", url).execute()
                written += 1
            except Exception as e:  # noqa: BLE001
                log.warning("update falhou em %s: %s", url, e)

    # Por dominio: e aqui que se ve QUEM esta estruturalmente inalcancavel
    # (403/WAF) em vez de so atrasado na fila.
    domains = sorted(set(by_domain_ok) | set(by_domain_fail),
                     key=lambda d: -(by_domain_ok[d] + by_domain_fail[d]))
    for d in domains[:25]:
        ok, bad = by_domain_ok[d], by_domain_fail[d]
        log.info("  %-32s ok=%-4d falhou=%-4d", d, ok, bad)

    log.info("gravados: %d | dt=%.1fs", written, time.time() - t0)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hours", type=int, default=168, help="janela de published_at (default 168 = 7d)")
    ap.add_argument("--limit", type=int, default=400, help="max de artigos buscados nesta rodada")
    ap.add_argument("--per-domain", type=int, default=40, help="max por dominio nesta rodada")
    ap.add_argument("--domain", default=None, help="restringe a um dominio")
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--deadline", type=float, default=240.0, help="teto da fase de fetch, em segundos")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY ausentes — abortando")
        return 2

    return run(args.hours, args.limit, args.per_domain, args.domain,
               args.workers, args.deadline, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
