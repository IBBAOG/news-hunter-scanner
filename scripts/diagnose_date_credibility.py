"""Measure the date-credibility rules on the LIVE feeds, before trusting a threshold.

The rules live in news_hunter/date_credibility.py (R2 page date, R3 verify or
defer, title dates) and pipeline._run_date_credibility. This script answers the
two questions that decide whether they are safe to ship:

  R2 -- for how many CURRENT items of each registered feed would the page's own
        publication date (or the date the feed prints in its title) move the
        date earlier than the feed says, by more than the tolerance? A domain
        above 50% is either re-stamping (the bug R2 exists for) or serving a
        TEMPLATE date -- the same date on every page, which R2 must never be
        allowed to turn into a silent zero. `const` tells them apart: a template
        shows as one page date repeated across items with different feed dates.
  R3 -- which feeds re-date urls news_articles already stores (or print older
        dates in their own titles), and what would happen to their never-seen
        fresh items: verified, dropped as older, or deferred?

Read-only: it never writes news_articles. Run it on the scanner's runner
(.github/workflows/diagnose_date_credibility.yml) so fetches see the same WAFs
and the lookups see the live table; locally, --stored-json replaces the lookup
with a snapshot and the keyword set falls back to config.DEFAULT_KEYWORDS
(labelled).

Usage:
    python -m scripts.diagnose_date_credibility                      # every registered feed
    python -m scripts.diagnose_date_credibility --pages 10 --hours 24
    python -m scripts.diagnose_date_credibility --feeds https://www.kpler.com/blog/rss.xml
    python -m scripts.diagnose_date_credibility --dry-run https://www.kpler.com/blog/rss.xml \
        --cleanup-after 2026-09-25T10:40:00Z
    python -m scripts.diagnose_date_credibility --titles
    python -m scripts.diagnose_date_credibility --full-scan [--no-date-credibility]

--dry-run runs the REAL pipeline (run_search, fast mode, no Google News) over
one feed with the upsert replaced by a capture, and prints what would be
persisted, dropped as old and deferred. --cleanup-after treats rows created at
or after that instant as never stored (what the database will look like once a
burst of wrongly inserted rows is deleted). --titles runs the title cleaner
over every stored title containing "|" and prints what would change.
--full-scan runs one complete scan (Google News included) with every write
closed and prints its wall time and the Stage 4b cost; --no-date-credibility
switches Stage 4b off for the A side of an A/B.

Columns (feed mode):
    items    entries the fetcher returned        fresh  feed date inside --hours
    match    fresh entries passing the keyword filter (live keywords)
    smp      item pages fetched for R2 (fresh first, newest first)
    fail     page fetch failed                    nodate page without a usable date
    older    page date older than feed - tol      oldF   ... among fresh sampled items
    later    page date LATER than feed + tol (R2 ignores it; informational)
    const    largest group of sampled items sharing one page date while their
             feed dates differ by more than the tolerance (template suspicion)
    tdate    items whose title prints a date     tOld   ... older than the feed date
    stored   fresh items already in news_articles
    dbEv     stored fresh MATCHED items re-dated (feed > min(pub, created) + tol),
             as count/largest 10-minute batch
    tEv      fresh items whose title date is older, count/largest batch
    FLAG     R3's batch rule holds (date_credibility.is_batch on dbEv or tEv)
    new      never-seen fresh matched items
    r3v/o/d/a  their fate if FLAGGED: verified / dropped as older / deferred
             (page read, no date) / admitted unverified (page not fetched,
             or a WAF challenge)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from urllib.parse import urlparse

from news_hunter import date_credibility as dc
from news_hunter.config import DEFAULT_KEYWORDS
from news_hunter.enrich import source_name_for
from news_hunter.fetcher import RawItem, _fetch_one, _fetch_standard_sitemap
from news_hunter.filter import within_window
from news_hunter.sources import LANGUAGES, all_rss_feeds, all_standard_sitemaps

PAGE_WORKERS = 24
PAGE_TIMEOUT = 8


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _keywords() -> tuple[list[str], set[str], str]:
    from news_hunter import supabase_sync
    from news_hunter.store import get_config

    cfg = get_config()
    keywords: list[str] = cfg["keywords"]
    exact: set[str] = set(cfg.get("exact_keywords") or set())
    if supabase_sync.get_sink().client is None:
        prov = "FALLBACK (no Supabase client)"
    elif set(keywords) == set(DEFAULT_KEYWORDS) and not exact:
        prov = "FALLBACK (keyword sources empty)"
    else:
        prov = "LIVE (Supabase)"
    native = [nat for c in LANGUAGES.values() if c.translate for nat, _ in c.keyword_priority]
    return list(keywords) + native, exact, prov


def _matched(it: RawItem, keywords: list[str], exact: set[str], hours: int) -> bool:
    from news_hunter.pipeline import LEDE_RESCUE_MARKER, _keep_candidate

    verdict = _keep_candidate(it, keywords, hours, exact, allow_lede_rescue=True)
    return verdict is not None and verdict != [LEDE_RESCUE_MARKER]


def _stored_lookup(urls: list[str], snapshot: dict | None) -> dc.StoredLookup:
    """Stored dates from the live table (store.existing_dates), or a snapshot."""
    if snapshot is not None:
        return dc.StoredLookup(found={u: snapshot[u] for u in urls if u in snapshot})
    from news_hunter.store import existing_dates

    return existing_dates(urls)


def _load_snapshot(path: str | None, cleanup_after: datetime | None):
    snap = None
    if path:
        raw = json.load(open(path, encoding="utf-8"))
        rows = raw if isinstance(raw, list) else [dict(url=k, **v) for k, v in raw.items()]
        snap = {
            r["url"]: dc.StoredDates(
                published_at=dc.parse_stored_timestamp(r.get("published_at")),
                created_at=dc.parse_stored_timestamp(r.get("created_at")),
            )
            for r in rows
        }
    return snap


def _apply_cleanup(lk: dc.StoredLookup, cutoff: datetime | None) -> dc.StoredLookup:
    """Rows created at/after `cutoff` read as never stored (a simulated cleanup)."""
    if cutoff is None:
        return lk
    return dc.StoredLookup(
        found={u: s for u, s in lk.found.items() if s.created_at is None or s.created_at < cutoff},
        failed=set(lk.failed),
    )


def _page_signals(url: str) -> dict:
    """Fetch one page and read every date signal separately (for the table)."""
    from bs4 import BeautifulSoup

    from news_hunter._clipinator_shim import fetch_html

    out: dict = {"fetched": False, "error": "", "page_date": None, "signals": {}}
    t0 = time.time()
    try:
        html = fetch_html(url, timeout=PAGE_TIMEOUT)
    except Exception as e:  # noqa: BLE001
        out["error"] = f"{type(e).__name__}: {str(e)[:80]}"
        out["secs"] = time.time() - t0
        return out
    out["secs"] = time.time() - t0
    soup = BeautifulSoup(html, "lxml")
    sigs = dc.read_page_signals(soup)
    out["fetched"] = True
    out["page_date"] = sigs.published
    out["challenge"] = out["page_date"] is None and dc.looks_like_challenge(soup)
    out["signals"] = {
        name: value for name, value in (
            ("meta", sigs.meta_published), ("jsonld", sigs.jsonld_article),
            ("itemprop", sigs.itemprop), ("meta_date", sigs.meta_date),
            ("jsonld_page", sigs.jsonld_page),
        ) if value is not None
    }
    out["headlines"] = sigs.headlines
    return out


def _fmt_dt(d) -> str:
    if d is None:
        return "-"
    if isinstance(d, dc.ParsedDate):
        return d.value.strftime("%Y-%m-%d") + ("" if d.date_only else d.value.strftime(" %H:%M"))
    return d.strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------------------
# feed mode
# ---------------------------------------------------------------------------

def _targets(feeds: list[str], include_sitemaps: bool) -> list[tuple[str, str, bool]]:
    registry = [(d, u, False) for d, u in all_rss_feeds()]
    if include_sitemaps:
        registry += [(d, u, True) for d, u in all_standard_sitemaps()]
    if not feeds:
        return registry
    by_url = {u: (d, u, s) for d, u, s in registry}
    out = []
    for u in feeds:
        out.append(by_url.get(u) or (urlparse(u).netloc.lower(), u, False))
    return out


def run_feeds(args) -> int:
    keywords, exact, prov = _keywords()
    print(f"keyword set : {prov} ({len(keywords)} terms)", flush=True)
    snapshot = _load_snapshot(args.stored_json, None)
    cutoff = _parse_cutoff(args.cleanup_after)
    targets = _targets(args.feeds, not args.no_sitemaps)
    print(f"feeds       : {len(targets)}  window={args.hours}h  pages/feed={args.pages}"
          f"  tolerance={dc.TOLERANCE}", flush=True)

    t0 = time.time()
    fetched: dict[tuple[str, str], tuple[list[RawItem], str | None]] = {}
    with ThreadPoolExecutor(max_workers=32) as ex:
        futs = {
            ex.submit(_fetch_standard_sitemap if sm else _fetch_one, u, d): (d, u)
            for d, u, sm in targets
        }
        for fut in futs:
            d, u = futs[fut]
            try:
                fetched[(d, u)] = fut.result(timeout=60)
            except Exception as e:  # noqa: BLE001
                fetched[(d, u)] = ([], f"{type(e).__name__}: {e}")
    print(f"feeds fetched in {time.time() - t0:.1f}s", flush=True)

    # group by feed domain (a domain can register several feeds; dedupe by url)
    per_dom: dict[str, dict] = {}
    for (d, u), (items, err) in fetched.items():
        slot = per_dom.setdefault(d, {"items": {}, "errors": []})
        if err:
            slot["errors"].append(f"{u}: {err}")
        for it in items or []:
            slot["items"].setdefault(it.url, it)

    jobs: dict[str, str] = {}   # url -> feed domain, for page fetches
    rows: dict[str, dict] = {}
    for d, slot in sorted(per_dom.items()):
        items = list(slot["items"].values())
        dated = [it for it in items if it.published_at is not None]
        fresh = [it for it in dated if within_window(it.published_at, args.hours)]
        match = [it for it in fresh if _matched(it, keywords, exact, args.hours)]
        fresh_urls = {it.url for it in fresh}
        tdate = tolder = 0
        tev_dates: list = []     # feed dates of fresh items whose title date is older
        for it in items:
            if not it.title or "|" not in it.title:
                continue
            _, td = dc.split_source_suffix(it.title, source_name_for(it.source_domain))
            if td is None:
                continue
            tdate += 1
            if dc.is_older(td, it.published_at):
                tolder += 1
                if it.url in fresh_urls:
                    tev_dates.append(it.published_at)
        stored = _stored_lookup([it.url for it in fresh], snapshot) if fresh else {}
        lookup_ok = stored is not None
        stored = _apply_cleanup(stored or {}, cutoff)
        db_ev = [it.url for it in match if dc.restamps(it.published_at, stored.get(it.url))]
        db_dates = [it.published_at for it in match if it.url in set(db_ev)]
        db_ev_all = [it.url for it in fresh if dc.restamps(it.published_at, stored.get(it.url))]
        new = [it for it in match if it.url not in stored]
        # the pipeline's rule: a BATCH of re-dates (date_credibility.is_batch)
        flagged = dc.is_batch(db_dates) or dc.is_batch(tev_dates)
        tev = len(tev_dates)
        sample = sorted(fresh, key=lambda i: i.published_at, reverse=True)
        rest = sorted([i for i in dated if i.url not in fresh_urls], key=lambda i: i.published_at, reverse=True)
        sample = (sample + rest)[: args.pages]
        if flagged:
            in_sample = {i.url for i in sample}
            extra = sorted(new, key=lambda i: i.published_at, reverse=True)[: args.r3_pages]
            sample += [i for i in extra if i.url not in in_sample]
        for it in sample:
            jobs[it.url] = d
        rows[d] = {
            "items": items, "fresh": fresh, "match": match, "sample": sample,
            "tdate": tdate, "tolder": tolder, "tev": tev, "stored": stored,
            "lookup_ok": lookup_ok, "db_ev": db_ev, "db_ev_all": db_ev_all,
            "db_batch": dc.largest_batch(db_dates), "tev_batch": dc.largest_batch(tev_dates),
            "new": new, "flagged": flagged, "errors": slot["errors"],
        }

    print(f"page fetches: {len(jobs)} (workers {PAGE_WORKERS})", flush=True)
    t1 = time.time()
    pages: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as ex:
        futs = {ex.submit(_page_signals, u): u for u in jobs}
        done, not_done = wait(futs.keys(), timeout=args.page_deadline)
        for fut in done:
            try:
                pages[futs[fut]] = fut.result()
            except Exception as e:  # noqa: BLE001
                pages[futs[fut]] = {"fetched": False, "error": str(e), "page_date": None, "signals": {}}
        for fut in not_done:
            fut.cancel()
            pages[futs[fut]] = {"fetched": False, "error": "deadline", "page_date": None, "signals": {}}
    print(f"pages fetched in {time.time() - t1:.1f}s", flush=True)

    hdr = (f"{'feed':34} {'items':>5} {'fresh':>5} {'match':>5} {'smp':>3} {'fail':>4} "
           f"{'nodt':>4} {'older':>5} {'oldF':>4} {'later':>5} {'const':>5} {'tdate':>5} "
           f"{'tOld':>4} {'strd':>4} {'dbEv':>5} {'tEv':>5} {'FLAG':>4} {'new':>4} "
           f"{'r3v':>3} {'r3o':>3} {'r3d':>3} {'r3a':>3}")
    print("\n" + hdr)
    totals = Counter()
    notes: list[str] = []
    for d, r in sorted(rows.items()):
        fresh_urls = {i.url for i in r["fresh"]}
        smp = r["sample"]
        fail = nodate = older = older_f = later = 0
        smp_fresh = 0
        by_date: dict[str, list] = {}
        sig_disagree = 0
        for it in smp:
            pg = pages.get(it.url) or {}
            if it.url in fresh_urls:
                smp_fresh += 1
            if not pg.get("fetched"):
                fail += 1
                continue
            pdt = pg.get("page_date")
            if pdt is None:
                nodate += 1
                continue
            by_date.setdefault(pdt.raw, []).append(it.published_at)
            if dc.is_older(pdt, it.published_at):
                older += 1
                if it.url in fresh_urls:
                    older_f += 1
            elif pdt.value > it.published_at + dc.TOLERANCE:
                later += 1
            sig = pg.get("signals") or {}
            vals = [v for v in sig.values() if v is not None]
            if len(vals) >= 2 and max(v.value for v in vals) - min(v.value for v in vals) > dc.TOLERANCE + dc.DAY_SPAN:
                sig_disagree += 1
        const = 0
        for raw, feeds_dates in by_date.items():
            if len(feeds_dates) >= 2 and max(feeds_dates) - min(feeds_dates) > dc.TOLERANCE:
                const = max(const, len(feeds_dates))
        # The pipeline's verdicts for a flagged feed's never-seen items:
        # verified / older / deferred (fetched, no page date) / admitted (we
        # could not fetch the page: transport error, HTTP >= 400, a challenge).
        r3v = r3o = r3d = r3a = 0
        if r["flagged"]:
            for it in r["new"]:
                pg = pages.get(it.url)
                if pg is None:
                    continue
                pdt = pg.get("page_date") if pg.get("fetched") else None
                if not pg.get("fetched") or pg.get("challenge"):
                    r3a += 1
                elif pdt is None:
                    r3d += 1
                elif dc.is_older(pdt, it.published_at):
                    r3o += 1
                else:
                    r3v += 1
        db_col = f"{len(r['db_ev'])}/{r['db_batch']}"
        tev_col = f"{r['tev']}/{r['tev_batch']}"
        print(f"{d[:34]:34} {len(r['items']):5} {len(r['fresh']):5} {len(r['match']):5} "
              f"{len(smp):3} {fail:4} {nodate:4} {older:5} {older_f:4} {later:5} {const:5} "
              f"{r['tdate']:5} {r['tolder']:4} {len(r['stored']):4} {db_col:>5} "
              f"{tev_col:>5} {'YES' if r['flagged'] else '':>4} {len(r['new']):4} "
              f"{r3v:3} {r3o:3} {r3d:3} {r3a:3}")
        totals.update(feeds=1, items=len(r["items"]), fresh=len(r["fresh"]), match=len(r["match"]),
                      smp=len(smp), fail=fail, nodate=nodate, older=older, older_f=older_f,
                      smp_fresh=smp_fresh, later=later, flagged=int(r["flagged"]),
                      new=len(r["new"]), r3v=r3v, r3o=r3o, r3d=r3d, r3a=r3a)
        if smp_fresh and older_f * 2 > smp_fresh:
            notes.append(f"R2>50%   {d}: {older_f}/{smp_fresh} fresh sampled items have an older page date")
        if const >= 3:
            notes.append(f"TEMPLATE? {d}: {const} sampled items share one page date across different feed dates")
        if sig_disagree:
            notes.append(f"SIGNALS  {d}: {sig_disagree} pages whose date signals disagree by > tol+day")
        if r["db_ev"] or r["tev"]:
            notes.append(
                f"{'FLAGGED ' if r['flagged'] else 'ISOLATED'} {d}: db_evidence={len(r['db_ev'])} "
                f"(largest 10-min batch {r['db_batch']}, fresh-all {len(r['db_ev_all'])}) "
                f"title_evidence={r['tev']} (batch {r['tev_batch']}) new={len(r['new'])}"
                + (f" -> verified {r3v}, older {r3o}, deferred {r3d}, admitted unverified {r3a}"
                   if r["flagged"] else " -> not flagged")
            )
            for u in r["db_ev"][:3]:
                s = r["stored"].get(u)
                it = next(i for i in r["match"] if i.url == u)
                notes.append(
                    f"           evidence {u}  feed={_fmt_dt(it.published_at)}"
                    f" stored_pub={_fmt_dt(s.published_at)} created={_fmt_dt(s.created_at)}"
                )
        if not r["lookup_ok"]:
            notes.append(f"LOOKUP   {d}: news_articles lookup FAILED")
        if r["errors"]:
            notes.append(f"ERROR    {d}: {'; '.join(r['errors'])[:160]}")
        if args.verbose:
            for it in smp:
                pg = pages.get(it.url) or {}
                pdt = pg.get("page_date")
                sig = ",".join(f"{k}={_fmt_dt(v)}" for k, v in (pg.get("signals") or {}).items())
                print(f"    feed={_fmt_dt(it.published_at)} page={_fmt_dt(pdt)} "
                      f"src={(pdt.source if pdt else '-')} [{sig}] "
                      f"{'FAIL ' + pg.get('error', '') if not pg.get('fetched') else ''} {it.url[:90]}")
    print("\n=== totals ===")
    print(dict(totals))
    print("\n=== notes ===")
    for n in notes:
        print(n)
    return 0


# ---------------------------------------------------------------------------
# dry-run mode: the real pipeline over one feed, upsert captured
# ---------------------------------------------------------------------------

def _parse_cutoff(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return dc.parse_stored_timestamp(raw)


def run_dry(args) -> int:
    from news_hunter import pipeline, supabase_sync

    url = args.dry_run
    domain = next((d for d, u in all_rss_feeds() if u == url), urlparse(url).netloc.lower())
    items, err = _fetch_one(url, domain)
    print(f"feed        : {url} ({domain}) items={len(items)} err={err}", flush=True)
    snapshot = _load_snapshot(args.stored_json, None)
    cutoff = _parse_cutoff(args.cleanup_after)

    real_lookup = supabase_sync.existing_dates

    def _lookup(urls):
        if snapshot is not None:
            got = {u: snapshot[u] for u in urls if u in snapshot}
        else:
            got = real_lookup(urls)
        return _apply_cleanup(got, cutoff)

    def _no_write(*_a, **_k):
        raise RuntimeError("diagnose_date_credibility --dry-run must never write")

    supabase_sync.existing_dates = _lookup
    # Read-only, belt and braces: every write door is closed, not just the one
    # run_search uses today.
    supabase_sync.push_new = _no_write
    supabase_sync._SupabaseSink.push = _no_write
    from news_hunter import translation_retry
    translation_retry.fill_missing = _no_write
    captured: list = []
    pipeline.iter_collect = lambda *a, **k: iter([(domain, items, err)])
    pipeline.upsert_articles = lambda arts: captured.extend(arts) or len(arts)
    pipeline._run_translation_retry = lambda *a, **k: 0
    holder: dict = {}
    base = pipeline._DateStats

    class _Tap(base):  # type: ignore[misc, valid-type]
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            holder["stats"] = self

    pipeline._DateStats = _Tap
    enriched_urls: set[str] = set()
    real_enrich = pipeline.enrich_item

    def _enrich_tap(it, **kw):
        enriched_urls.add(it.url)
        return real_enrich(it, **kw)

    pipeline.enrich_item = _enrich_tap
    res = pipeline.run_search(include_google_news=False, fast_mode=True, hours_override=args.hours)
    stats = holder.get("stats")

    keywords, exact, prov = _keywords()
    fresh = [it for it in items if it.published_at and within_window(it.published_at, args.hours)]
    match = [it for it in fresh if _matched(it, keywords, exact, args.hours)]
    stored = _lookup([it.url for it in fresh]) or {}
    persisted = {a.url: a for a in captured}
    trace = dict(stats.trace) if stats else {}
    print(f"keyword set : {prov}")
    print(f"items={len(items)} fresh(<{args.hours}h)={len(fresh)} keyword-matched={len(match)} "
          f"already-stored(fresh)={len(stored)}"
          + (f" [cleanup: rows created >= {cutoff.isoformat()} treated as never stored]" if cutoff else ""))
    print(stats.log_line() if stats else "no stats")
    outcome = Counter()
    lines = []
    for it in sorted(match, key=lambda i: i.published_at, reverse=True):
        t = trace.get(it.url, "")
        if it.url in persisted:
            if it.url in stored:
                what = "PERSIST-existing"
            elif t == "verified":
                what = "PERSIST-verified"
            elif t.startswith("admitted_"):
                what = "PERSIST-" + t.upper()
            else:
                what = "PERSIST-new"
        elif t:
            what = t.upper()
        elif it.url not in enriched_urls:
            # _ENRICH_CAP: at most 20 items of one domain reach stage 4 per scan
            what = "NOT-ENRICHED(cap 20)"
        else:
            what = "NOT-PERSISTED(other)"
        outcome[what] += 1
        a = persisted.get(it.url)
        kws = ",".join(a.matched_keywords) if a else ""
        lines.append(f"  {what:28} feed={_fmt_dt(it.published_at)} {it.url.rsplit('/', 1)[-1][:56]:56} "
                     f"[{kws[:30]}] | {(a.title if a else it.title)[:60]}")
    print("\n=== outcome of the keyword-matched fresh items ===")
    for k, v in sorted(outcome.items()):
        print(f"  {k:28} {v}")
    print("\n".join(lines))

    # The two things this dry run exists to prove, checked item by item on what
    # WOULD be written: no old post persisted as new, no row kept on the
    # source's own name alone.
    from news_hunter import date_credibility as _dc
    from news_hunter.keyword_senses import drop_own_name

    old_new = own_only = 0
    for a in captured:
        if a.url in stored:
            continue                       # an existing row, not a new one
        it = next((i for i in items if i.url == a.url), None)
        if it is not None and it.title:
            _, td = _dc.split_source_suffix(it.title, source_name_for(it.source_domain))
            if _dc.is_older(td, it.published_at):
                old_new += 1
        ev = _dc.page_seen(a.url)
        if ev is not None and it is not None and _dc.is_older(ev.page_date, it.published_at):
            old_new += 1
        if not drop_own_name(a.matched_keywords, a.domain):
            own_only += 1
    print(f"\nnew rows that are old posts (title or page date > 24 h older): {old_new}")
    print(f"rows kept on the source's own name alone: {own_only}")
    print(f"\nrun_search: n_total={res.get('n_total')} date_page_older={res.get('date_page_older')} "
          f"date_deferred={res.get('date_deferred')} "
          f"unverified_admitted={res.get('date_unverified_admitted')} "
          f"own_name_dropped={res.get('own_name_dropped')} restamp={res.get('date_restamp_domains')}")
    return 0


def run_full_scan(args) -> int:
    """One complete scan (Google News included), every write door closed.

    Prints the scan's wall time and the date-credibility line, i.e. what Stage
    4b costs on top of a production-shaped scan. Compare `dt` with the "scan
    done ... dt=" lines of the News Hunter scan workflow at the same hour (they
    also pay ~3 s of upsert POSTs, which this run skips).
    """
    from news_hunter import pipeline, supabase_sync, translation_retry

    def _no_write(*_a, **_k):
        raise RuntimeError("diagnose_date_credibility --full-scan must never write")

    supabase_sync.push_new = _no_write
    supabase_sync._SupabaseSink.push = _no_write
    translation_retry.fill_missing = _no_write
    captured: list = []
    pipeline.upsert_articles = lambda arts: captured.extend(arts) or len(arts)
    pipeline._run_translation_retry = lambda *a, **k: 0
    holder: dict = {}
    base = pipeline._DateStats

    class _Tap(base):  # type: ignore[misc, valid-type]
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            holder["stats"] = self

    pipeline._DateStats = _Tap
    label = "date credibility ON"
    if args.no_date_credibility:
        # The A side of an A/B: Stage 4b and the title evidence switched off,
        # everything else identical.
        label = "date credibility OFF"
        pipeline._run_date_credibility = lambda articles, *a, **k: articles
        pipeline._title_date_evidence = lambda it: False
    t0 = time.time()
    res = pipeline.run_search(include_google_news=True, fast_mode=True, hours_override=args.hours)
    dt = time.time() - t0
    stats = holder.get("stats")
    print(f"\nfull scan (no writes, {label}): dt={dt:.1f}s would_upsert={len(captured)} "
          f"stage4b={stats.seconds if stats else -1:.2f}s errors={len(res.get('errors', []))}")
    print(stats.log_line() if stats else "no stats")
    return 0


def run_titles(args) -> int:
    """T regression check: clean_display_title over every stored title containing "|".

    Two passes per title: as the scanner runs it without a page (suffix strip
    and the "X X" collapse), and a worst case for the h1 rule where the page
    <h1> is assumed to be each " | "-separated segment of the title itself. A
    title that changes is printed with its domain; the counts are the evidence
    that nothing regresses outside the suffix the rule is meant to remove.
    """
    from news_hunter import supabase_sync

    sink = supabase_sync.get_sink()
    if sink.client is None:
        print("titles: no Supabase client", flush=True)
        return 2
    rows: list[dict] = []
    start = 0
    while True:
        res = (
            sink.client.table(sink.table)
            .select("domain, source_name, title")
            .like("title", "%|%")
            .order("url")
            .range(start, start + 999)
            .execute()
        )
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        start += 1000
    changed: dict[str, list[tuple[str, str]]] = {}
    changed_h1: dict[str, list[tuple[str, str]]] = {}
    for r in rows:
        title, name, dom = r.get("title") or "", r.get("source_name") or "", r.get("domain") or ""
        new = dc.clean_display_title(title, name)
        if new != title:
            changed.setdefault(dom, []).append((title, new))
        segs = [x.strip() for x in title.split("|") if x.strip()]
        new_h1 = dc.clean_display_title(title, name, segs)
        if new_h1 != new:
            changed_h1.setdefault(dom, []).append((title, new_h1))
    print(f"titles with '|': {len(rows)} rows, {len({r.get('domain') for r in rows})} domains", flush=True)
    for label, bucket in (("no page (suffix / X X)", changed), ("worst-case h1 = a segment", changed_h1)):
        n = sum(len(v) for v in bucket.values())
        print(f"\n=== changed, {label}: {n} rows ===")
        for dom, pairs in sorted(bucket.items(), key=lambda kv: -len(kv[1])):
            print(f"  {dom}: {len(pairs)}")
            for old, new in pairs[:3]:
                print(f"      {old[:110]!r}\n   -> {new[:110]!r}")
    return 0


def main(argv: list[str] | None = None) -> int:
    import logging

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--feeds", nargs="*", default=[], help="only these feed URLs (default: every registered feed)")
    ap.add_argument("--no-sitemaps", action="store_true", help="skip STANDARD_SITEMAPS")
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--pages", type=int, default=8, help="item pages fetched per feed for R2")
    ap.add_argument("--r3-pages", type=int, default=12, help="extra never-seen pages per FLAGGED feed")
    ap.add_argument("--page-deadline", type=float, default=240.0)
    ap.add_argument("--stored-json", help="snapshot {url: {published_at, created_at}} instead of the live lookup")
    ap.add_argument("--cleanup-after", help="treat rows created at/after this ISO instant as never stored")
    ap.add_argument("--dry-run", metavar="FEED_URL", help="run the real pipeline over one feed (no writes)")
    ap.add_argument("--titles", action="store_true", help="T regression check over every stored title with '|'")
    ap.add_argument("--full-scan", action="store_true", help="one complete scan, no writes: dt + Stage 4b cost")
    ap.add_argument("--no-date-credibility", action="store_true",
                    help="with --full-scan: switch Stage 4b off (the A side of an A/B timing)")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every sampled page")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s", stream=sys.stdout)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.titles:
        return run_titles(args)
    if args.full_scan:
        return run_full_scan(args)
    if args.dry_run:
        return run_dry(args)
    return run_feeds(args)


if __name__ == "__main__":
    sys.exit(main())
