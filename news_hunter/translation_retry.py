"""Retry pass for foreign rows stored without an English title.

WHY. The live translation stage (pipeline._run_translation) only ever sees the
articles the CURRENT scan collected. A foreign row whose translation failed —
a throttled backend, the per-scan cap, the stage deadline — is left with
`title_en IS NULL`, and it is only retried if a later scan re-sees it in the
source feed. The busiest sources (Sina Finance, Al Arabiya) rotate their
listings in hours, so most such rows drop out of the feed and stay native
forever. Measured 2026-09-11: NULL rows from 09-08 were still NULL, and the
untranslated rows were the NEWEST ones, i.e. what sits on top of /home.

This pass asks the DATABASE instead of the feed. Each scan it takes a small,
bounded batch of foreign rows from the last few days whose `title_en` is still
NULL and translates them through the same guarded `translate_to_en`.

Write shape (identical to scripts/repair_foreign_titles.py, and for the same
reasons): an UPDATE keyed on url AND filtered `title_en IS NULL`, carrying only
the fields that were empty. It can never overwrite a translation, never write
an error page (the translator rejects those), never touch a native row, never
delete anything.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

NATIVE_TAGS = ("en", "pt")


def _query(sink, since_iso: str, limit: int, newest_first: bool):
    return (
        sink.client.table(sink.table)
        .select("url, source_lang, title_original, snippet, snippet_en")
        .is_("title_en", "null")
        .not_.is_("source_lang", "null")
        .not_.in_("source_lang", NATIVE_TAGS)
        .not_.is_("title_original", "null")
        .gte("published_at", since_iso)
        .order("published_at", desc=newest_first)
        .limit(limit)
        .execute()
    )


def select_retry_rows(
    sink,
    *,
    budget: int,
    days: int,
    exclude: set[str] | frozenset[str] = frozenset(),
    now: datetime | None = None,
) -> list[dict]:
    """Pick up to `budget` untranslated foreign rows from the last `days` days.

    Half the budget goes to the NEWEST rows (a headline that just failed is the
    one on top of the feed) and half to the OLDEST still inside the window, so a
    row that keeps failing at the head can never starve the tail. Rows in
    `exclude` (the ones the live stage already attempted this scan) are skipped,
    as are rows with a blank native title. Returns [] on any lookup failure.
    """
    if budget <= 0 or sink is None or getattr(sink, "client", None) is None:
        return []
    now = now or datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).isoformat()
    fetch = budget + len(exclude)
    try:
        newest = _query(sink, since, fetch, newest_first=True).data or []
        oldest = _query(sink, since, fetch, newest_first=False).data or []
    except Exception as e:  # noqa: BLE001
        log.warning("translation retry: lookup failed: %s", e)
        return []

    picked: list[dict] = []
    seen: set[str] = set()

    def _take(row: dict) -> None:
        url = row.get("url")
        if not url or url in seen or url in exclude:
            return
        if not (row.get("title_original") or "").strip():
            return
        seen.add(url)
        picked.append(row)

    for i in range(max(len(newest), len(oldest))):
        for side in (newest, oldest):
            if i < len(side) and len(picked) < budget:
                _take(side[i])
        if len(picked) >= budget:
            break
    return picked


def translate_missing(row: dict) -> dict:
    """UPDATE payload for one row: only fields that are empty and translated OK."""
    from .translate import translate_to_en  # noqa: PLC0415

    lang = (row.get("source_lang") or "").strip()
    payload: dict = {}
    title_en = translate_to_en((row.get("title_original") or "").strip(), lang)
    if title_en:
        payload["title_en"] = title_en
    snippet = row.get("snippet") or ""
    if snippet.strip() and not (row.get("snippet_en") or "").strip():
        snippet_en = translate_to_en(snippet, lang)
        if snippet_en:
            payload["snippet_en"] = snippet_en
    return payload


def fill_missing(sink, url: str, payload: dict) -> bool:
    """Write `payload` onto `url` only while its title_en is still NULL."""
    if not payload:
        return False
    try:
        (
            sink.client.table(sink.table)
            .update(payload)
            .eq("url", url)
            .is_("title_en", "null")
            .execute()
        )
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("translation retry: update failed on %s: %s", url, e)
        return False
