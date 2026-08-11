"""Measure what a candidate feed would actually yield, against the LIVE keywords.

Why this exists: registering a source is a MEASUREMENT, not a guess. A site
usually publishes several feeds (per-editoria RSS, a general one, a sitemap) and
the right pick is the one that returns the most sector stories per byte — which
you cannot know without running the real fetcher and the real filter over each
candidate.

THE TRAP THIS TOOL EXISTS TO CLOSE: `store.get_config()` falls back SILENTLY to
the ~25 hardcoded DEFAULT_KEYWORDS (all substring) whenever the Supabase service
key is unreachable — the normal state of a dev machine. Production runs a much
larger set with a substantial `exact` subset, so a local measurement against the
fallback is plausible-but-wrong: it invents noise ("gas" matching "gastos") that
production never sees, and misses the exact-only hits production does see. This
script therefore REFUSES to print a yield table when the keyword set it loaded is
the fallback, unless you pass --allow-fallback-keywords and accept the label.

Run it on the SAME runner the scanner uses (workflow: measure_source.yml), so the
numbers also account for how the source behaves from a datacenter IP.

Usage:
    python -m scripts.measure_source https://site.com/feed/ https://site.com/economia/rss.xml
    python -m scripts.measure_source https://site.com/sitemap.xml --standard-sitemap
    python -m scripts.measure_source https://site.com/feed/ --lede --hours 48

Columns:
    items      raw entries the fetcher returned
    span       oldest -> newest published_at inside the feed
    fresh      entries inside the window (--hours)
    pass       entries the pipeline would keep on title/summary (or slug, for
               sitemaps) — i.e. what lands WITHOUT paying a body fetch
    near       fresh entries that missed and would become lede-rescue candidates
    rescued    of those, how many match once the body is fetched (--lede only;
               the real scan caps this at LEDE_RESCUE_CAP_DOMAIN=8 per domain)
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from news_hunter.config import DEFAULT_KEYWORDS
from news_hunter.enrich import enrich_item
from news_hunter.fetcher import (
    RawItem,
    _fetch_one,
    _fetch_standard_sitemap,
)
from news_hunter.filter import matches_keywords, within_window
from news_hunter.pipeline import LEDE_RESCUE_MARKER, _keep_candidate
from news_hunter.sources import is_sitemap_url
from news_hunter.store import get_config


def _keyword_provenance(keywords: list[str], exact: set[str]) -> str:
    """LIVE (from Supabase) vs FALLBACK (config.DEFAULT_KEYWORDS)."""
    from news_hunter import supabase_sync

    if supabase_sync.get_sink().client is None:
        return "FALLBACK (no Supabase client — SUPABASE_URL/SERVICE_KEY missing)"
    if set(keywords) == set(DEFAULT_KEYWORDS) and not exact:
        return "FALLBACK (Supabase reachable but every keyword source came back empty)"
    return "LIVE (Supabase)"


def _fetch(url: str, *, standard_sitemap: bool) -> tuple[list[RawItem], str | None, float]:
    domain = urlparse(url).netloc.lower()
    t0 = time.time()
    if standard_sitemap:
        items, err = _fetch_standard_sitemap(url, domain)
    else:
        items, err = _fetch_one(url, domain)
    return items, err, time.time() - t0


def _label(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    parts = [p for p in path.split("/") if p and p not in ("rss.xml", "feed", "rss")]
    return "/".join(parts[-2:]) or urlparse(url).netloc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("urls", nargs="+", help="candidate feed / sitemap URLs")
    ap.add_argument("--hours", type=int, default=48, help="window in hours (default 48)")
    ap.add_argument(
        "--standard-sitemap",
        action="store_true",
        help="treat every URL as a plain sitemap (urlset without news:news), i.e. "
             "the STANDARD_SITEMAPS registry slot — items arrive title-less and "
             "the pipeline pre-filters them on the URL slug",
    )
    ap.add_argument(
        "--lede",
        action="store_true",
        help="fetch the body of near-miss items and re-check (slow; mirrors the "
             "pipeline's lede-rescue phase)",
    )
    ap.add_argument("--lede-cap", type=int, default=25, help="max body fetches per feed (default 25)")
    ap.add_argument(
        "--allow-fallback-keywords",
        action="store_true",
        help="print the table even when the keyword set is the hardcoded fallback",
    )
    args = ap.parse_args(argv)

    cfg = get_config()
    keywords: list[str] = cfg["keywords"]
    exact: set[str] = set(cfg.get("exact_keywords") or set())
    provenance = _keyword_provenance(keywords, exact)

    print(f"keyword set   : {provenance}", flush=True)
    print(
        f"keywords      : {len(keywords)} total, {len(exact)} exact, "
        f"{len(keywords) - len(exact)} substring",
        flush=True,
    )
    print(f"exact         : {', '.join(sorted(exact))}", flush=True)
    print(f"substring     : {', '.join(k for k in keywords if k not in exact)}", flush=True)
    print(f"window        : {args.hours}h", flush=True)

    if provenance.startswith("FALLBACK") and not args.allow_fallback_keywords:
        print(
            "\nREFUSING to measure against the fallback keyword set — the numbers "
            "would not describe production.\nRun this on the scanner's runner "
            "(measure_source.yml, which passes the Supabase secrets), or pass "
            "--allow-fallback-keywords and label the table accordingly.",
            flush=True,
        )
        return 2

    now = datetime.now(timezone.utc)
    rows: list[tuple[str, int, str, int, int, int, int, int]] = []

    for url in args.urls:
        items, err, elapsed = _fetch(url, standard_sitemap=args.standard_sitemap)
        label = _label(url)
        if err:
            print(f"\n### {label}\nERROR: {err}", flush=True)
            continue

        dates = [i.published_at for i in items if i.published_at]
        span = (
            f"{(max(dates) - min(dates)).total_seconds() / 3600:.0f}h" if dates else "-"
        )
        fresh = passed = near = 0
        matched_rows: list[tuple[RawItem, list[str]]] = []
        near_items: list[RawItem] = []
        for it in items:
            if it.published_at is not None and not within_window(it.published_at, args.hours):
                continue
            fresh += 1
            verdict = _keep_candidate(
                it, keywords, args.hours, exact, allow_lede_rescue=True
            )
            if verdict is None:
                continue
            if verdict == [LEDE_RESCUE_MARKER]:
                near += 1
                near_items.append(it)
                continue
            passed += 1
            matched_rows.append((it, verdict))

        rescued = 0
        rescued_rows: list[tuple[RawItem, list[str], str]] = []
        if args.lede and near_items:
            for it in near_items[: args.lede_cap]:
                try:
                    snippet, _pub, _u, _d, _t = enrich_item(
                        it, resolve_google_news=False, need_snippet=True
                    )
                except Exception as e:  # noqa: BLE001
                    print(f"  lede error {it.url}: {e}", flush=True)
                    continue
                if not snippet:
                    continue
                hit = matches_keywords(f"{it.title} \n {snippet}", keywords, exact)
                if hit:
                    rescued += 1
                    rescued_rows.append((it, hit, snippet))

        rows.append((label, len(items), span, fresh, passed, near, rescued, int(elapsed * 1000)))

        print(f"\n### {label}  ({url})", flush=True)
        print(
            f"items={len(items)} span={span} fresh={fresh} pass={passed} "
            f"near={near} rescued={rescued} fetch={elapsed:.2f}s",
            flush=True,
        )
        for it, kws in matched_rows:
            age = (now - it.published_at).total_seconds() / 3600 if it.published_at else -1
            title = it.title or it.url.rstrip("/").rsplit("/", 1)[-1]
            print(f"  PASS [{age:5.1f}h] {','.join(kws):28} {title[:95]}", flush=True)
        for it, kws, snippet in rescued_rows:
            print(f"  LEDE {','.join(kws):28} {it.title[:80]}", flush=True)
            print(f"       {snippet[:200]}", flush=True)

    print("\n=== summary ===", flush=True)
    print(f"{'feed':38} {'items':>5} {'span':>7} {'fresh':>5} {'pass':>5} {'near':>5} {'resc':>5} {'ms':>6}")
    for label, n, span, fresh, passed, near, rescued, ms in rows:
        print(f"{label:38} {n:5} {span:>7} {fresh:5} {passed:5} {near:5} {rescued:5} {ms:6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
