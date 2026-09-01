"""Re-translate foreign rows whose English overlay is missing.

WHY THIS EXISTS. Between 2026-08-19 and 2026-09-01 the translate backend
answered HTTP 500 on a large share of calls, and deep-translator returned the
visible text of Google's error page as an ordinary successful string instead of
raising. `translate_to_en` accepted it (non-empty was its only test) and the
sink stored it: 2,670 production rows (34% of every row carrying a `title_en`)
whose English headline read

    Error 500 (Server Error)!!1500.That's an error. ...

The producer is now guarded (news_hunter.translate.looks_like_error_page), so no
NEW row can be poisoned. That fixes nothing already stored, and nothing already
stored can self-heal: the sink's overlay is WRITE-ONCE, so a later scan will not
overwrite a non-empty `title_en`, and an old article is not re-scanned at all. A
database sweep NULLs the poisoned values; this job is what puts real English
back, otherwise those rows display their native Arabic/Chinese/Russian/Hebrew
headline forever.

Same shape as scripts/backfill_snippets.py — that job asks the DATABASE which
rows lack a body; this one asks which rows lack a translation.

  * Selection: a FOREIGN source_lang (set, and not en/pt), a non-empty
    `title_original`, and a NULL `title_en`.
  * It translates through the SAME GUARDED `translate_to_en`, so a repair run
    launched during a translate outage writes nothing rather than re-poisoning
    the rows it was meant to fix. That is the whole point of routing the repair
    through the producer instead of calling deep-translator directly.
  * It writes with an UPDATE keyed on url, filtered `title_en is null`, and only
    ever fills a field that was empty. It can never overwrite a translation.
  * Idempotent and safe to re-run: a repaired row stops matching the predicate,
    so consecutive runs walk down the backlog. Nothing is ever deleted.

Usage:
    python -m scripts.repair_foreign_titles                  # 500 newest, write
    python -m scripts.repair_foreign_titles --limit 2000
    python -m scripts.repair_foreign_titles --lang ar
    python -m scripts.repair_foreign_titles --dry-run        # translate, report, write nothing
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from news_hunter.supabase_sync import get_sink
from news_hunter.translate import translate_to_en

log = logging.getLogger("repair_foreign_titles")

# Pagination unit for the read. PostgREST caps a response at the project's
# max-rows; asking in pages keeps the job honest about how much it actually saw.
PAGE = 1000

# Tags the pipeline never translates. Kept here as the literal negation of the
# selection predicate rather than derived from sources.LANGUAGES: this job must
# also reach rows tagged by a language that has since been retired from the
# registry, which a registry-derived allowlist would silently skip.
NATIVE_TAGS = ("en", "pt")


@dataclass
class Row:
    url: str
    source_lang: str
    title_original: str
    snippet: str = ""
    snippet_en: str = ""


class _GuardCounter(logging.Handler):
    """Counts error-page rejections raised inside news_hunter.translate.

    The guard's WARNING carries `extra={"error_page": True}`, so this counts the
    attribute and not a message substring — the count survives a reworded log
    line. Without it, "translated nothing" and "the endpoint is down" look
    identical in the summary, which is exactly the ambiguity that let the
    original outage run for two weeks unnoticed.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "error_page", False):
            self.count += 1


def _fetch_candidates(sink, limit: int, lang: str | None) -> list[Row]:
    """Rows with a foreign source_lang, a native title, and no English title."""
    out: list[Row] = []
    offset = 0
    while len(out) < limit:
        q = (
            sink.client.table(sink.table)
            .select("url, source_lang, title_original, snippet, snippet_en")
            .is_("title_en", "null")
            .not_.is_("source_lang", "null")
            .not_.is_("title_original", "null")
            .order("published_at", desc=True)
            .range(offset, offset + PAGE - 1)
        )
        if lang:
            q = q.eq("source_lang", lang)
        else:
            q = q.not_.in_("source_lang", NATIVE_TAGS)
        rows = q.execute().data or []
        for r in rows:
            # The "non-empty title_original" half of the predicate is tested
            # HERE and not in the query on purpose: PostgREST spells it
            # `title_original=neq.` (an empty right-hand side), whose parsing is
            # a detail of the gateway rather than a documented contract, and a
            # selection predicate that a server-side quirk could silently widen
            # is not worth the round-trip it saves. NULLs are excluded above,
            # where the syntax is unambiguous.
            title_original = (r.get("title_original") or "").strip()
            if not title_original:
                continue
            out.append(
                Row(
                    url=r["url"],
                    source_lang=(r.get("source_lang") or "").strip(),
                    title_original=title_original,
                    snippet=r.get("snippet") or "",
                    snippet_en=r.get("snippet_en") or "",
                )
            )
        if len(rows) < PAGE:
            break
        offset += PAGE
    return out


def _select(rows: list[Row], limit: int, per_lang: int) -> list[Row]:
    """Round-robin by language so one language cannot consume the whole run.

    Same rule as backfill_snippets' per-domain round-robin and for the same
    reason: the poisoned set is not evenly spread across ar/zh/ru/iw/es, and a
    plain ordering would spend an entire run inside the largest one while the
    others waited behind it.
    """
    picked: list[Row] = []
    seen: Counter = Counter()
    # Interleave: one pass per rank, taking at most one row per language.
    by_lang: dict[str, list[Row]] = {}
    for r in rows:
        by_lang.setdefault(r.source_lang, []).append(r)
    rank = 0
    while len(picked) < limit:
        progressed = False
        for lang in sorted(by_lang):
            bucket = by_lang[lang]
            if rank >= len(bucket) or seen[lang] >= per_lang:
                continue
            picked.append(bucket[rank])
            seen[lang] += 1
            progressed = True
            if len(picked) >= limit:
                break
        if not progressed:
            break
        rank += 1
    return picked


def _repair(row: Row) -> dict:
    """Translate what is missing on one row. Returns the UPDATE payload (maybe {}).

    Fail-soft per field, exactly like the live Stage 3c: a field the backend
    could not translate — including because the guard rejected an error page —
    is simply left out of the payload and stays NULL for the next run.
    """
    payload: dict = {}
    title_en = translate_to_en(row.title_original, row.source_lang)
    if title_en:
        payload["title_en"] = title_en
    # Only fill a snippet_en that is actually empty; a stored one is never touched.
    if row.snippet and not row.snippet_en.strip():
        snippet_en = translate_to_en(row.snippet, row.source_lang)
        if snippet_en:
            payload["snippet_en"] = snippet_en
    return payload


def run(limit: int, per_lang: int, lang: str | None, workers: int,
        batch: int, pause: float, dry_run: bool) -> int:
    sink = get_sink()
    if sink.client is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY ausentes — nada a fazer")
        return 2

    guard = _GuardCounter()
    translate_log = logging.getLogger("news_hunter.translate")
    translate_log.addHandler(guard)
    try:
        return _run(sink, guard, limit, per_lang, lang, workers, batch, pause, dry_run)
    finally:
        # Always detach: run() is callable more than once in a process (tests,
        # and a future looped driver), and a leaked handler would double-count.
        translate_log.removeHandler(guard)


def _run(sink, guard: _GuardCounter, limit: int, per_lang: int, lang: str | None,
         workers: int, batch: int, pause: float, dry_run: bool) -> int:
    t0 = time.time()
    rows = _fetch_candidates(sink, limit * 4, lang)
    langs = Counter(r.source_lang for r in rows)
    log.info("candidatas (foreign + title_original + title_en NULL): %d  [%s]",
             len(rows), ", ".join(f"{k}={v}" for k, v in sorted(langs.items())) or "-")
    if not rows:
        return 0

    todo = _select(rows, limit, per_lang)
    log.info("selecionadas: %d (cap %d, %d/idioma)  [%s]", len(todo), limit, per_lang,
             ", ".join(f"{k}={v}" for k, v in
                       sorted(Counter(r.source_lang for r in todo).items())))

    repaired: list[tuple[Row, dict]] = []
    attempted = 0
    # Batched with a pause between batches: courtesy to a free, unauthenticated
    # endpoint. A 2,670-row backlog is several runs by design, not one burst.
    for start in range(0, len(todo), batch):
        chunk = todo[start:start + batch]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            payloads = list(ex.map(_repair, chunk))
        attempted += len(chunk)
        for row, payload in zip(chunk, payloads):
            if payload:
                repaired.append((row, payload))
        log.info("  lote %d-%d: %d/%d traduzidos (guard ate agora: %d)",
                 start + 1, start + len(chunk), sum(1 for p in payloads if p),
                 len(chunk), guard.count)
        if pause and start + batch < len(todo):
            time.sleep(pause)

    written = 0
    if dry_run:
        log.info("--dry-run: nada gravado")
    else:
        for row, payload in repaired:
            try:
                # UPDATE, nunca upsert, e ainda filtrado por title_en IS NULL:
                # se um scan concorrente preencheu a linha nesse meio-tempo, ele
                # vence. Esta rotina so preenche vazio, nunca sobrescreve.
                (
                    sink.client.table(sink.table)
                    .update(payload)
                    .eq("url", row.url)
                    .is_("title_en", "null")
                    .execute()
                )
                written += 1
            except Exception as e:  # noqa: BLE001
                log.warning("update falhou em %s: %s", row.url, e)

    skipped = attempted - len(repaired)
    log.info(
        "candidatas=%d selecionadas=%d tentadas=%d reparadas=%d gravadas=%d "
        "puladas=%d (das quais guard/error-page=%d) dt=%.1fs",
        len(rows), len(todo), attempted, len(repaired), written,
        skipped, guard.count, time.time() - t0,
    )

    if attempted and not repaired and guard.count:
        # Every attempt came back as an error page: the backend is down, not the
        # data. Nothing was written (that is the guard working), but a manually
        # dispatched repair that repaired nothing must not look like a success.
        log.error("nenhuma traducao aceita e o guard disparou %d vez(es) — "
                  "backend de traducao indisponivel; rode de novo mais tarde",
                  guard.count)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=500, help="max de linhas reparadas nesta rodada")
    ap.add_argument("--per-lang", type=int, default=400, help="max por idioma nesta rodada")
    ap.add_argument("--lang", default=None, help="restringe a um source_lang (ar/ru/zh/iw/es)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch", type=int, default=50, help="linhas por lote antes da pausa")
    ap.add_argument("--pause", type=float, default=2.0, help="pausa entre lotes, em segundos")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY ausentes — abortando")
        return 2

    return run(args.limit, args.per_lang, args.lang, args.workers,
               args.batch, args.pause, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
