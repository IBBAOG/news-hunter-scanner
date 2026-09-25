"""TEMPORARY (measurement only, removed before handover).

Which empty-snippet articles does the snippet backfill skip because this scan
already read their page, who read it, and which articles does it fill? One full
scan, every write door closed. `--off` = the A side (date credibility off).
"""
from __future__ import annotations

import logging
import sys
import traceback

from news_hunter import date_credibility as dc
from news_hunter import enrich, pipeline, supabase_sync, translation_retry


def main() -> int:
    off = "--off" in sys.argv
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s", stream=sys.stdout)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    def _no_write(*_a, **_k):
        raise RuntimeError("audit must never write")

    supabase_sync.push_new = _no_write
    supabase_sync._SupabaseSink.push = _no_write
    translation_retry.fill_missing = _no_write
    captured: list = []
    pipeline.upsert_articles = lambda arts: captured.extend(arts) or len(arts)
    pipeline._run_translation_retry = lambda *a, **k: 0

    stage = {"now": "enrich"}
    recorder: dict[str, tuple[str, str, int]] = {}
    real_record = enrich.record_page

    def _rec(url, soup):
        via = ("fetch_page_evidence"
               if any(f.name == "fetch_page_evidence" for f in traceback.extract_stack()) else "enrich_item")
        ev = real_record(url, soup)
        prev = recorder.get(url)
        recorder[url] = (stage["now"], via, (prev[2] + 1) if prev else 1)
        return ev

    if off:
        pipeline._run_date_credibility = lambda articles, *a, **k: articles
        pipeline._title_date_evidence = lambda it: False
        enrich.record_page = lambda url, soup: dc.PageEvidence()
    else:
        enrich.record_page = _rec

    def _tag(name: str, tag: str) -> None:
        real = getattr(pipeline, name)

        def wrapped(*a, **k):
            stage["now"] = tag
            return real(*a, **k)

        setattr(pipeline, name, wrapped)

    _tag("_run_lede_rescue", "lede")
    _tag("_run_date_credibility", "date_check")

    real_bf = pipeline._run_snippet_backfill

    def _bf(articles, errors, redated=None):
        stage["now"] = "backfill"
        empty = [a for a in articles if not (a.snippet or "").strip()]
        skipped = [(a, dc.page_seen(a.url)) for a in empty if dc.page_seen(a.url) is not None]
        before = {a.url: (a.snippet or "") for a in articles}
        n = real_bf(articles, errors, redated)
        filled = [a for a in articles if not before.get(a.url, "").strip() and (a.snippet or "").strip()]
        print(f"BACKFILL AUDIT ({'OFF' if off else 'ON'}): empty={len(empty)} "
              f"skipped_as_read={len(skipped)} filled={len(filled)} returned={n}")
        for a, ev in skipped:
            pd = ev.page_date.value.isoformat() if ev.page_date else None
            print(f"  SKIPPED {a.url} domain={a.domain} read={ev.read} challenge={ev.challenge} "
                  f"snippet_len={len(ev.snippet)} page_date={pd} recorded_by={recorder.get(a.url)}")
        for a in filled:
            print(f"  FILLED  {a.url} domain={a.domain} by={recorder.get(a.url)} :: {a.snippet[:90]!r}")
        return n

    pipeline._run_snippet_backfill = _bf
    pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=24)
    return 0


if __name__ == "__main__":
    sys.exit(main())
