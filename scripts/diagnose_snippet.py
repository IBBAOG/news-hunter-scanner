"""Diagnose why an ARTICLE lands in the feed with no body, from the runner.

Companion to diagnose_feed.py, which answers "does this FEED answer 200 from a
datacenter IP?". This one answers the next question down: given one article URL,
does the snippet path produce text — and if not, which half failed, the fetch or
the extractor?

That split is the whole point. "Source has no snippet" has two completely
different remedies:

  * fetch failed (403 / WAF / timeout)  -> the domain needs the Google News
    route, a cookie, or nothing at all can be done; the extractor is irrelevant.
  * fetch fine, extractor returned zero paragraphs -> the site changed its
    layout (page-builder migration, body moved into a JSON island) and the fix
    is a selector in _clipinator_shim.EXTRACTORS.

Reading only the final "snippet: (empty)" cannot tell those apart, and guessing
wrong costs a whole debugging session.

Usage:
    python -m scripts.diagnose_snippet https://exemplo.com.br/materia [...]
"""
from __future__ import annotations

import argparse
import sys
import time

from news_hunter._clipinator_shim import EXTRACTORS, _extract, fetch_html
from news_hunter.enrich import enrich_item
from news_hunter.fetcher import RawItem
from news_hunter.store import normalize_url


def _domain(url: str) -> str:
    from urllib.parse import urlparse

    return urlparse(url).netloc.lower()


def diagnose(url: str) -> None:
    url = normalize_url(url)
    domain = _domain(url)
    print(f"\n=== {url} ===", flush=True)
    print(f"domain          : {domain}", flush=True)
    print(f"in EXTRACTORS   : {domain in EXTRACTORS}", flush=True)

    # 1. Raw fetch — separates "blocked" from "layout changed".
    t0 = time.time()
    try:
        html = fetch_html(url, timeout=10)
        err = None
    except Exception as e:  # noqa: BLE001
        html, err = "", e
    dt = time.time() - t0
    if err is not None:
        print(f"fetch_html      : FAILED after {dt:.2f}s -- {err!r}", flush=True)
        print("verdict         : FETCH — the runner cannot reach this article.", flush=True)
        return
    print(f"fetch_html      : {len(html)} bytes in {dt:.2f}s", flush=True)
    if not html.strip():
        print("verdict         : FETCH — empty body (blocked or redirected).", flush=True)
        return

    # 2. Extractor — how much prose the registered selectors actually find.
    if domain in EXTRACTORS:
        try:
            title, paragraphs = _extract(html, domain)
        except Exception as e:  # noqa: BLE001
            print(f"extractor       : RAISED {e!r}", flush=True)
            paragraphs, title = [], ""
        print(f"extractor title : {title[:90]!r}", flush=True)
        print(f"extractor paras : {len(paragraphs)}", flush=True)
        if paragraphs:
            print(f"  first         : {paragraphs[0][:160]!r}", flush=True)
    else:
        print("extractor       : domain not registered (enrich falls back to meta)", flush=True)

    # 3. The real path, end to end — this is what the scanner would store.
    item = RawItem(
        url=url, title="", summary="", published_at=None,
        source_domain=domain, feed_domain=domain,
    )
    snippet, published, _u, _d, ext_title = enrich_item(item, need_snippet=True)
    print(f"enrich snippet  : {len(snippet)} chars", flush=True)
    if snippet:
        print(f"  text          : {snippet[:200]!r}", flush=True)
    print(f"enrich date     : {published}", flush=True)
    print(f"enrich title    : {ext_title[:90]!r}", flush=True)

    if snippet:
        print("verdict         : OK — this article would land with a body.", flush=True)
    elif domain in EXTRACTORS:
        print(
            "verdict         : EXTRACTOR — page fetched, no prose found. Check the "
            "selectors for this domain in _clipinator_shim.EXTRACTORS.",
            flush=True,
        )
    else:
        print(
            "verdict         : EXTRACTOR — domain unregistered and meta description "
            "empty. Add it to EXTRACTORS.",
            flush=True,
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("urls", nargs="+", help="article URLs to diagnose")
    args = ap.parse_args(argv)
    for url in args.urls:
        diagnose(url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
