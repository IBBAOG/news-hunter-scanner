"""Diagnose a single feed URL from wherever this runs (locally or on a GHA runner).

Why this exists: a feed can fail from a datacenter IP in ways that are invisible
from a residential IP (WAF challenge, 403, CDN cache-miss latency). When a source
silently stops producing articles, run this workflow on the same runner the
scanner uses and read the actual status / latency / body.

Usage:
    python -m scripts.diagnose_feed https://example.com/feed/
    python -m scripts.diagnose_feed https://example.com/feed/ --timeout 15

Reports, for the given URL:
  1. requests GET with the scanner's exact headers at the scanner's FEED_TIMEOUT
  2. the same GET with a generous timeout (isolates "slow" from "blocked")
  3. curl_cffi GET with browser impersonation (isolates "bot-blocked" from "down")
  4. feedparser result for whatever body came back (entry count, bozo, dates)
"""
from __future__ import annotations

import argparse
import sys
import time

import feedparser
import requests

from news_hunter.fetcher import FEED_TIMEOUT, USER_AGENT, _fetch_one

ACCEPT = "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.5"


def _headline(label: str) -> None:
    print(f"\n=== {label} ===", flush=True)


def _report_response(r: requests.Response, elapsed: float) -> None:
    print(f"status        : {r.status_code}", flush=True)
    print(f"elapsed       : {elapsed:.2f}s", flush=True)
    print(f"bytes         : {len(r.content)}", flush=True)
    interesting = (
        "content-type", "content-encoding", "server", "cf-cache-status",
        "cf-ray", "cf-mitigated", "retry-after", "x-cache", "age", "via",
    )
    for h in interesting:
        if h in r.headers:
            print(f"hdr {h:<13}: {r.headers[h]}", flush=True)
    head = r.content[:400]
    print(f"first bytes   : {head!r}", flush=True)


def _try_requests(url: str, timeout: float, label: str) -> requests.Response | None:
    _headline(f"{label} (requests, timeout={timeout}s)")
    t0 = time.time()
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": ACCEPT},
            timeout=timeout,
        )
    except Exception as e:  # noqa: BLE001
        print(f"EXCEPTION     : {type(e).__name__}: {e}", flush=True)
        print(f"elapsed       : {time.time() - t0:.2f}s", flush=True)
        return None
    _report_response(r, time.time() - t0)
    return r


def _try_curl_cffi(url: str, timeout: float) -> None:
    _headline(f"curl_cffi impersonation (timeout={timeout}s)")
    t0 = time.time()
    try:
        from curl_cffi import requests as cffi_requests

        r = cffi_requests.get(url, impersonate="chrome", timeout=timeout)
    except Exception as e:  # noqa: BLE001
        print(f"EXCEPTION     : {type(e).__name__}: {e}", flush=True)
        print(f"elapsed       : {time.time() - t0:.2f}s", flush=True)
        return
    elapsed = time.time() - t0
    print(f"status        : {r.status_code}", flush=True)
    print(f"elapsed       : {elapsed:.2f}s", flush=True)
    body = r.content or b""
    print(f"bytes         : {len(body)}", flush=True)
    for h in ("content-type", "content-encoding", "server", "cf-cache-status", "cf-mitigated"):
        val = r.headers.get(h)
        if val:
            print(f"hdr {h:<13}: {val}", flush=True)
    print(f"first bytes   : {body[:400]!r}", flush=True)
    parsed = feedparser.parse(body)
    print(f"feedparser    : {len(parsed.get('entries') or [])} entries, bozo={parsed.get('bozo')}", flush=True)


def _report_parse(body: bytes) -> None:
    _headline("feedparser on the last successful body")
    parsed = feedparser.parse(body)
    entries = parsed.get("entries") or []
    print(f"bozo          : {parsed.get('bozo')}", flush=True)
    if parsed.get("bozo"):
        print(f"bozo_exception: {parsed.get('bozo_exception')}", flush=True)
    print(f"entries       : {len(entries)}", flush=True)
    for e in entries[:5]:
        print(
            f"  - {(e.get('published') or e.get('updated') or 'NO-DATE')!s:<40} "
            f"{(e.get('title') or '')[:70]}",
            flush=True,
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("url", help="feed URL to diagnose")
    ap.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="generous timeout for the second attempt (default 20s)",
    )
    ap.add_argument("--repeat", type=int, default=2, help="how many scanner-timeout attempts (default 2)")
    args = ap.parse_args(argv)

    print(f"diagnosing    : {args.url}", flush=True)
    print(f"scanner FEED_TIMEOUT = {FEED_TIMEOUT}s", flush=True)

    # 1) Exactly what the scanner does, repeated — a CDN cache MISS on the first
    #    call and a HIT on the second is the signature of a latency problem.
    body = b""
    for i in range(max(1, args.repeat)):
        r = _try_requests(args.url, FEED_TIMEOUT, f"scanner-equivalent attempt {i + 1}")
        if r is not None and r.content:
            body = r.content

    # 2) Same request, generous timeout — separates "slow" from "blocked".
    r_slow = _try_requests(args.url, args.timeout, "generous timeout")
    if r_slow is not None and r_slow.content:
        body = r_slow.content

    # 3) Browser impersonation — separates "bot-blocked" from "down".
    _try_curl_cffi(args.url, args.timeout)

    if body:
        _report_parse(body)

    # 4) End-to-end: the real _fetch_one the collector calls.
    _headline("news_hunter.fetcher._fetch_one (end to end)")
    t0 = time.time()
    items, err = _fetch_one(args.url, args.url.split("/")[2])
    print(f"elapsed       : {time.time() - t0:.2f}s", flush=True)
    print(f"items         : {len(items)}", flush=True)
    print(f"error         : {err}", flush=True)
    for it in items[:5]:
        print(f"  - {it.published_at} {it.title[:70]}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
