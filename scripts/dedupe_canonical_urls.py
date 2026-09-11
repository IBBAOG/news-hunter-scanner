"""Consolidate news_articles rows that are the same article under different urls.

WHY THIS EXISTS. Until 2026-09-11 the url canonicalizer (store.normalize_url)
only stripped utm-style params and 'www.'. Sina Finance's per-slot tracking
query (`?cre=tianyi&mod=pchp&loc=NN...`, `?finpagefr=p_108`) and AMP mirrors
(`/amp/<path>`, `<path>/amp`, `?amp=1`) therefore produced several rows per
article: one Sina headline was stored under 8 urls, and on 09-11 the zh feed had
260 rows for 149 distinct titles. Each copy needed its own translation, so some
copies of a story showed English and others the native headline.

The canonicalizer is fixed (no NEW variant can be stored). This job folds the
rows already stored onto the canonical url:

  * group every row by normalize_url(url);
  * keep ONE row per group — the one already at the canonical url when it
    exists, otherwise the best copy (has title_en > has snippet > oldest);
  * fill the keeper's EMPTY fields from its siblings (title_en, snippet_en,
    title_original, source_lang, snippet), union matched_keywords — a field the
    keeper already has is never overwritten, so no translation is lost;
  * move the keeper to the canonical url if it is not there yet;
  * only then delete the siblings.

Order matters: the keeper is written BEFORE any sibling is deleted, so a run
that dies half-way never loses data — the worst case is a group left partially
merged, which the next run finishes. Idempotent: a second run finds 0 groups.

Usage:
    python -m scripts.dedupe_canonical_urls --dry-run    # report only
    python -m scripts.dedupe_canonical_urls              # apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict

from news_hunter.store import normalize_url
from news_hunter.supabase_sync import _chunk_urls_for_query, get_sink

log = logging.getLogger("dedupe_canonical_urls")

PAGE = 1000
FILL_FIELDS = ("title_en", "snippet_en", "title_original", "source_lang", "snippet")
FULL_COLS = (
    "url, domain, title, snippet, published_at, found_at, created_at, "
    "matched_keywords, source_lang, title_original, title_en, snippet_en"
)


def _empty(v) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def plan_groups(urls: list[str]) -> dict[str, list[str]]:
    """canonical url -> stored urls, for every group that needs work.

    A group needs work when it has more than one row, or when its single row is
    not yet at the canonical url (a later scan would otherwise create the
    canonical row next to it — a fresh duplicate).
    """
    groups: dict[str, list[str]] = defaultdict(list)
    for u in urls:
        groups[normalize_url(u)].append(u)
    return {
        canon: members for canon, members in groups.items()
        if len(members) > 1 or members[0] != canon
    }


def _rank(row: dict) -> tuple:
    return (
        0 if not _empty(row.get("title_en")) else 1,
        0 if not _empty(row.get("snippet")) else 1,
        row.get("created_at") or "",
        row.get("url") or "",
    )


def merge_group(canon: str, rows: list[dict]) -> tuple[dict, dict, list[str]]:
    """Decide one group. Returns (keeper_row, update_payload, urls_to_delete).

    `update_payload` holds only what changes on the keeper: a url move and/or
    empty fields filled from siblings, plus the matched_keywords union.
    """
    at_canon = [r for r in rows if r["url"] == canon]
    ordered = sorted(rows, key=_rank)
    keeper = at_canon[0] if at_canon else ordered[0]
    siblings = [r for r in ordered if r["url"] != keeper["url"]]

    payload: dict = {}
    if keeper["url"] != canon:
        payload["url"] = canon
    for f in FILL_FIELDS:
        if _empty(keeper.get(f)):
            for s in siblings:
                if not _empty(s.get(f)):
                    payload[f] = s[f]
                    break
    kws = list(keeper.get("matched_keywords") or [])
    for s in siblings:
        for k in s.get("matched_keywords") or []:
            if k not in kws:
                kws.append(k)
    if kws != list(keeper.get("matched_keywords") or []):
        payload["matched_keywords"] = kws
    return keeper, payload, [s["url"] for s in siblings]


def _all_urls(sink) -> list[str]:
    out: list[str] = []
    offset = 0
    while True:
        rows = (
            sink.client.table(sink.table).select("url")
            .order("url").range(offset, offset + PAGE - 1).execute().data or []
        )
        out.extend(r["url"] for r in rows if r.get("url"))
        if len(rows) < PAGE:
            return out
        offset += PAGE


def _rows_for(sink, urls: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for chunk in _chunk_urls_for_query(urls):
        rows = (
            sink.client.table(sink.table).select(FULL_COLS)
            .in_("url", chunk).execute().data or []
        )
        for r in rows:
            out[r["url"]] = r
    return out


def _is_pk_conflict(e: Exception) -> bool:
    s = str(e)
    return "23505" in s or "duplicate key" in s


def _apply(sink, canon: str, keeper: dict, payload: dict, delete: list[str]) -> str:
    """Write one group. Returns 'ok', 'raced' (canonical appeared meanwhile) or 'failed'."""
    t = sink.table
    if payload:
        try:
            sink.client.table(t).update(payload).eq("url", keeper["url"]).execute()
        except Exception as e:  # noqa: BLE001
            if not (payload.get("url") and _is_pk_conflict(e)):
                log.warning("update failed on %s: %s", keeper["url"], e)
                return "failed"
            # A scan inserted the canonical row between our read and this write.
            # Merge INTO it instead: fill its empty fields from everything we
            # hold, then the keeper becomes one more sibling to delete.
            current = _rows_for(sink, [canon]).get(canon)
            if current is None:
                return "failed"
            others = [keeper] + [r for r in _rows_for(sink, delete).values()]
            _k, fill, _d = merge_group(canon, [current] + others)
            fill.pop("url", None)
            if fill:
                try:
                    sink.client.table(t).update(fill).eq("url", canon).execute()
                except Exception as e2:  # noqa: BLE001
                    log.warning("fill of raced canonical %s failed: %s", canon, e2)
                    return "failed"
            delete = delete + [keeper["url"]]
            for chunk in _chunk_urls_for_query(delete):
                sink.client.table(t).delete().in_("url", chunk).execute()
            return "raced"
    for chunk in _chunk_urls_for_query(delete):
        if chunk:
            sink.client.table(t).delete().in_("url", chunk).execute()
    return "ok"


def run(dry_run: bool, max_groups: int | None) -> int:
    sink = get_sink()
    if sink.client is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing")
        return 2
    urls = _all_urls(sink)
    groups = plan_groups(urls)
    multi = sum(1 for m in groups.values() if len(m) > 1)
    surplus = sum(len(m) - 1 for m in groups.values())
    log.info("rows=%d groups_needing_work=%d (multi-row=%d, rename-only=%d) surplus_rows=%d",
             len(urls), len(groups), multi, len(groups) - multi, surplus)
    if max_groups is not None:
        groups = dict(list(groups.items())[:max_groups])

    items = list(groups.items())
    n_del = n_move = n_title = n_ok = n_race = n_fail = 0
    by_domain: dict[str, int] = defaultdict(int)
    for i in range(0, len(items), 200):
        batch = items[i:i + 200]
        rows = _rows_for(sink, [u for _c, members in batch for u in members])
        for canon, members in batch:
            present = [rows[u] for u in members if u in rows]
            if not present:
                continue
            keeper, payload, delete = merge_group(canon, present)
            n_del += len(delete)
            n_move += 1 if "url" in payload else 0
            n_title += 1 if "title_en" in payload else 0
            by_domain[keeper.get("domain") or "?"] += len(delete)
            if dry_run:
                continue
            res = _apply(sink, canon, keeper, payload, delete)
            n_ok += res == "ok"
            n_race += res == "raced"
            n_fail += res == "failed"
        log.info("  %d/%d groups processed", min(i + 200, len(items)), len(items))

    top = sorted(by_domain.items(), key=lambda kv: -kv[1])[:12]
    log.info("rows to delete=%d  keepers moved to canonical url=%d  keepers gaining title_en=%d",
             n_del, n_move, n_title)
    log.info("deleted rows by domain: %s", ", ".join(f"{d}={n}" for d, n in top) or "-")
    if dry_run:
        log.info("--dry-run: nothing written")
        return 0
    log.info("groups ok=%d raced=%d failed=%d", n_ok, n_race, n_fail)
    return 1 if n_fail else 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-groups", type=int, default=None)
    args = ap.parse_args(argv)
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing - aborting")
        return 2
    return run(args.dry_run, args.max_groups)


if __name__ == "__main__":
    sys.exit(main())
