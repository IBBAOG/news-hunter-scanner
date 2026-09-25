"""End to end: run_search over Kpler-shaped feed items (the 2026-09-25 re-stamp burst).

What each test pins, in the words of the fix:
  * a fresh feed <pubDate> on a page whose datePublished is "Apr 01, 2026" is
    not persisted (R2 through R3, through the backfill fetch, and through the
    date the feed prints in its own title);
  * an empty page datePublished in a feed with re-stamp evidence is DEFERRED;
  * a genuinely new item whose page date matches is persisted, headline clean;
  * an item with no page date in a feed WITHOUT re-stamp evidence is persisted
    with its feed date -- the behaviour before the fix;
plus the guard rails: lookup failure defers, Google News is exempt, the flag
needs fresh evidence, the budget rotates, and one INFO line is always logged.

The collector, the page fetch and the news_articles lookup are stubbed; the
pipeline, the enrich path and the date rules are the real ones.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import enrich, pipeline, supabase_sync  # noqa: E402
from news_hunter.date_credibility import StoredDates  # noqa: E402
from news_hunter.fetcher import RawItem  # noqa: E402

UTC = timezone.utc
NOW = datetime.now(UTC)
TODAY = NOW.strftime("%b %d, %Y")
KWS = ["Hormuz", "crude", "tanker", "shadow fleet", "LNG", "oil"]
FEED = "www.kpler.com"
BLOG = "https://kpler.com/blog/"

# Kpler's RSS <description>: long enough to be the snippet, so in fast mode the
# enrich path never fetches the page -- exactly how the burst got through.
SUMMARY = (
    "For shipping and commodity desks, Hormuz transit data and crude tanker flows "
    "are the structural signal to watch this quarter, as freight and LNG markets "
    "reprice around the Strait and shadow fleet exposure keeps rising."
)


def _page(h1: str, date_published: str) -> str:
    ld = {"@context": "https://schema.org", "@graph": [
        {"@type": "BlogPosting", "headline": h1, "datePublished": date_published,
         "dateModified": "Sep 25, 2026", "publisher": {"@type": "Organization", "name": "Kpler"}},
        {"@type": "BreadcrumbList", "itemListElement": []},
    ]}
    return (f"<html><head><title>{h1} | Kpler - {date_published}</title>"
            f'<script type="application/ld+json">{json.dumps(ld)}</script></head>'
            f'<body><h1 class="single-blog-heading">{h1}</h1>'
            f"<p>{SUMMARY}</p><p>{SUMMARY}</p></body></html>")


def _item(slug: str, title: str, *, age_h: float = 1.0, summary: str = SUMMARY,
          feed: str = FEED, url: str | None = None) -> RawItem:
    return RawItem(
        url=url or BLOG + slug, title=title, summary=summary,
        published_at=NOW - timedelta(hours=age_h),
        source_domain="kpler.com", feed_domain=feed, source_lang="en",
    )


# The stored post the feed re-dates: first seen 11 days ago, now "fresh".
STORED_SLUG = "crude-tanker-rates-hit-new-highs-as-hormuz-risk-escalates"
STORED = _item(
    STORED_SLUG,
    "Crude tanker rates hit new highs as Hormuz risk escalates Crude tanker rates hit new highs "
    "as Hormuz risk escalates | Kpler -",
    age_h=1.2,
)
STORED_ROW = {BLOG + STORED_SLUG: StoredDates(published_at=NOW - timedelta(days=11),
                                              created_at=NOW - timedelta(days=11))}

OLD_SLUG = "how-to-build-a-risk-tree-to-assess-shadow-fleet-exposure-in-your-network"
OLD_H1 = "How to build a risk tree to assess shadow fleet exposure in your network"
# The page-date path: this title carries no printed date (the "| Kpler -" shape).
OLD = _item(OLD_SLUG, f"How to build a risk tree to assess shadow fleet exposure {OLD_H1} | Kpler -")

DATELESS_SLUG = "russian-barrels-to-fill-chinese-crude-stocks"
DATELESS = _item(DATELESS_SLUG, "Russian barrels to fill Chinese crude stocks? Russian barrels to fill "
                                "Chinese crude stocks? | Kpler -")

NEW_SLUG = "hormuz-tanker-flows-rebound-as-transits-normalise"
NEW_H1 = "Hormuz tanker flows rebound as transits normalise"
NEW = _item(NEW_SLUG, f"Tanker Traffic Returns to Hormuz {NEW_H1} | Kpler - {TODAY}", age_h=0.5)

PAGES = {
    BLOG + OLD_SLUG: _page(OLD_H1, "Apr 01, 2026"),
    BLOG + DATELESS_SLUG: _page("Russian barrels to fill Chinese crude stocks?", ""),
    BLOG + NEW_SLUG: _page(NEW_H1, TODAY),
    BLOG + STORED_SLUG: _page("Crude tanker rates hit new highs as Hormuz risk escalates", ""),
}


class _Run:
    def __init__(self):
        self.persisted: list = []
        self.fetched: list[str] = []
        self.lookups: list[list[str]] = []
        self.stats = None


def _drive(monkeypatch, items, *, stored=None, lookup_fails=False, pages=None, feed=FEED):
    """run_search over `items` with the collector, page fetch and lookup stubbed."""
    run = _Run()
    pages = PAGES if pages is None else pages

    monkeypatch.setattr(pipeline, "get_config",
                        lambda: {"keywords": KWS, "exact_keywords": set(), "window_hours": 24})
    monkeypatch.setattr(pipeline, "iter_collect", lambda *a, **k: iter([(feed, list(items), None)]))
    monkeypatch.setattr(pipeline, "upsert_articles",
                        lambda arts: run.persisted.extend(arts) or len(arts))
    monkeypatch.setattr(pipeline, "urls_with_snippet", lambda urls: set())
    monkeypatch.setattr(pipeline, "_run_translation_retry", lambda *a, **k: 0)

    def _fetch_html(url, timeout=6):  # noqa: ARG001
        run.fetched.append(url)
        if url not in pages:
            raise RuntimeError("HTTP 403")
        return pages[url]

    monkeypatch.setattr(enrich, "fetch_html", _fetch_html)

    def _existing_dates(urls):
        run.lookups.append(list(urls))
        if lookup_fails:
            return None
        return {u: s for u, s in (stored or {}).items() if u in urls}

    monkeypatch.setattr(supabase_sync, "existing_dates", _existing_dates)

    base = pipeline._DateStats

    class _Tap(base):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            run.stats = self

    monkeypatch.setattr(pipeline, "_DateStats", _Tap)
    pipeline.run_search(include_google_news=False, fast_mode=True, hours_override=24)
    return run


def _by_url(run):
    return {a.url: a for a in run.persisted}


def _outcome(run, url):
    return dict(run.stats.trace).get(url)


# ---------------------------------------------------------------------------
# The four behaviours the fix is about
# ---------------------------------------------------------------------------

def test_fresh_feed_date_with_page_date_apr_01_is_not_persisted(monkeypatch):
    """R3 flags the feed (a stored post re-dated), the page says Apr 01 -> dropped."""
    run = _drive(monkeypatch, [STORED, OLD], stored=STORED_ROW)
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + OLD_SLUG) == "older_page"
    assert run.stats.restamp_db == {FEED: [BLOG + STORED_SLUG]}
    assert BLOG + OLD_SLUG in run.fetched        # the early return was bypassed
    # The stored row itself is not new: it is re-sent, never deferred.
    assert BLOG + STORED_SLUG in _by_url(run)


def test_page_date_apr_01_also_drops_the_item_through_the_backfill_fetch(monkeypatch):
    """No re-stamp evidence at all: R2 still applies whenever a page is fetched.

    An item without an RSS description lands in the snippet backfill, whose
    fetch reads the page date too.
    """
    bare = _item(OLD_SLUG, OLD.title, summary="")
    run = _drive(monkeypatch, [bare])
    assert run.stats.flagged() == []
    assert BLOG + OLD_SLUG in run.fetched
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert run.stats.n_page_older == 1


def test_date_printed_in_the_feed_title_drops_the_item_without_a_fetch(monkeypatch):
    """ "... | Kpler - Mar 31, 2026" on a fresh <pubDate> is an old post, no page needed."""
    titled = _item(OLD_SLUG, f"How to build a risk tree to assess shadow fleet exposure {OLD_H1} "
                             "| Kpler - Mar 31, 2026")
    run = _drive(monkeypatch, [titled], pages={})
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + OLD_SLUG) == "older_title"
    assert run.fetched == []
    assert run.stats.title_evidence == {FEED: 1}


def test_empty_page_date_with_restamp_evidence_is_deferred(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [STORED, DATELESS], stored=STORED_ROW)
    assert BLOG + DATELESS_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"
    assert run.stats.deferred == {FEED: {"no_page_date": 1}}
    line = next(r.getMessage() for r in caplog.records if "date credibility:" in r.getMessage())
    assert "deferred=1" in line and "no_page_date=1" in line and FEED in line


def test_genuinely_new_item_whose_page_date_matches_is_persisted(monkeypatch):
    run = _drive(monkeypatch, [STORED, NEW], stored=STORED_ROW)
    got = _by_url(run).get(BLOG + NEW_SLUG)
    assert got is not None
    assert _outcome(run, BLOG + NEW_SLUG) == "verified"
    assert got.published_at == NEW.published_at          # the feed date, untouched
    assert got.title == NEW_H1                           # T: the page <h1>


def test_no_page_date_without_restamp_evidence_keeps_the_feed_date(monkeypatch):
    """Unchanged behaviour: no evidence, no verification, the feed date stands."""
    run = _drive(monkeypatch, [DATELESS])
    got = _by_url(run).get(BLOG + DATELESS_SLUG)
    assert got is not None and got.published_at == DATELESS.published_at
    assert run.stats.flagged() == [] and run.stats.n_deferred == 0
    assert run.fetched == []                             # fast mode + RSS snippet: no fetch
    assert got.title == "Russian barrels to fill Chinese crude stocks?"   # T without a page


def test_no_page_date_on_a_fetched_page_without_evidence_keeps_the_feed_date(monkeypatch):
    bare = _item(DATELESS_SLUG, DATELESS.title, summary="")
    run = _drive(monkeypatch, [bare])
    got = _by_url(run).get(BLOG + DATELESS_SLUG)
    assert BLOG + DATELESS_SLUG in run.fetched           # the backfill did read the page
    assert got is not None and got.published_at == bare.published_at


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------

def test_the_burst_shape_end_to_end(monkeypatch):
    """Stored post re-dated + old + dateless + genuinely new, in one scan."""
    run = _drive(monkeypatch, [STORED, OLD, DATELESS, NEW], stored=STORED_ROW)
    urls = set(_by_url(run))
    assert urls == {BLOG + STORED_SLUG, BLOG + NEW_SLUG}
    assert run.stats.verified == 1 and run.stats.n_page_older == 1 and run.stats.n_deferred == 1


def test_lookup_failure_defers_every_feed_candidate(monkeypatch):
    run = _drive(monkeypatch, [STORED, NEW], stored=STORED_ROW, lookup_fails=True)
    assert run.persisted == []
    assert run.stats.lookup_failed is True
    assert run.stats.deferred == {FEED: {"lookup_failed": 2}}


def test_google_news_items_are_exempt_from_r3(monkeypatch):
    gnews_old = _item(OLD_SLUG, "How to build a risk tree", feed="news.google.com")
    gnews_stored = _item(STORED_SLUG, "Crude tanker rates hit new highs", feed="news.google.com")
    run = _drive(monkeypatch, [gnews_stored, gnews_old], stored=STORED_ROW, feed="news.google.com")
    assert run.stats.flagged() == []
    assert run.lookups == []                             # nothing to check: no lookup at all
    assert {BLOG + OLD_SLUG, BLOG + STORED_SLUG} <= set(_by_url(run))


def test_stored_published_at_already_pushed_forward_still_testifies(monkeypatch):
    """An earlier re-stamp moved published_at; created_at still exposes the next."""
    stored = {BLOG + STORED_SLUG: StoredDates(published_at=STORED.published_at,
                                              created_at=NOW - timedelta(days=11))}
    run = _drive(monkeypatch, [STORED, DATELESS], stored=stored)
    assert run.stats.restamp_db == {FEED: [BLOG + STORED_SLUG]}
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"


def test_evidence_beyond_the_enrich_cap_still_flags_the_feed(monkeypatch):
    """The stored post sits below the first 20 items of the feed (Kpler, 11:54 UTC)."""
    newest_dateless = _item(DATELESS_SLUG, DATELESS.title, age_h=0.05)
    fillers = [_item(f"hormuz-note-{n}", f"Hormuz crude note number {n}", age_h=0.2 + n / 100)
               for n in range(pipeline_cap())]
    run = _drive(monkeypatch, [newest_dateless, *fillers, STORED], stored=STORED_ROW,
                 pages={**PAGES, **{BLOG + f"hormuz-note-{n}": _page(f"Hormuz crude note number {n}", TODAY)
                                    for n in range(pipeline_cap())}})
    # STORED never reached stage 4 (21st item of the feed) and still testified.
    assert BLOG + STORED_SLUG not in _by_url(run)
    assert run.stats.restamp_db == {FEED: [BLOG + STORED_SLUG]}
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"


def pipeline_cap() -> int:
    # _ENRICH_CAP is a local of run_search (20); mirror it rather than import it.
    return 20


def test_title_evidence_counts_only_items_the_feed_presents_as_fresh(monkeypatch):
    # A three-day-old item whose title prints an even older date: not today's
    # problem, and it must not flag the feed for a week.
    stale = _item("old-note", "Old note on Hormuz | Kpler - Jun 30, 2026", age_h=72)
    run = _drive(monkeypatch, [stale, DATELESS])
    assert run.stats.flagged() == []
    assert BLOG + DATELESS_SLUG in _by_url(run)


def test_verification_budget_defers_the_rest_and_rotates(monkeypatch):
    many = [_item(f"hormuz-brief-{n}", f"Hormuz brief number {n} on crude flows", age_h=0.1 + n / 60)
            for n in range(12)]
    pages = {BLOG + f"hormuz-brief-{n}": _page(f"Hormuz brief number {n} on crude flows", TODAY)
             for n in range(12)}
    run = _drive(monkeypatch, [STORED, *many], stored=STORED_ROW, pages=pages)
    assert run.stats.verified == pipeline.DATE_VERIFY_CAP_DOMAIN
    assert run.stats.deferred == {FEED: {"over_budget": 12 - pipeline.DATE_VERIFY_CAP_DOMAIN}}
    kept = set(_by_url(run)) - {BLOG + STORED_SLUG}
    # the newest half of the budget always goes to the newest items
    newest = {BLOG + f"hormuz-brief-{n}" for n in range(pipeline.DATE_VERIFY_CAP_DOMAIN // 2)}
    assert newest <= kept


def test_date_credibility_line_is_logged_with_zeros(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    _drive(monkeypatch, [DATELESS])
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("date credibility:")]
    assert len(lines) == 1
    assert lines[0].startswith("date credibility: page_older=0 [] (title_date=0) restamp_domains=[]"
                               " verified=0 deferred=0 []")


def test_approximate_dates_are_never_candidates(monkeypatch):
    """A clamped future date is fabricated (published_is_approx): R3 leaves it to
    _freeze_approx_dates, which must keep working exactly as before."""
    future = _item("scheduled-hormuz-post", "Scheduled Hormuz crude post", age_h=-3)
    run = _drive(monkeypatch, [STORED, future], stored=STORED_ROW)
    got = _by_url(run).get(BLOG + "scheduled-hormuz-post")
    assert got is not None and got.published_is_approx is True
    assert _outcome(run, BLOG + "scheduled-hormuz-post") is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
