"""End to end: run_search over Kpler-shaped feed items (the 2026-09-25 re-stamp burst).

What each test pins, in the words of the fix:
  * a fresh feed <pubDate> on a page whose datePublished is "Apr 01, 2026" is
    not persisted (R2 through R3, through the backfill fetch, and through the
    date the feed prints in its own title);
  * an empty page datePublished in a feed with re-stamp evidence is DEFERRED;
  * a genuinely new item whose page date matches is persisted, headline clean;
  * an item with no page date in a feed WITHOUT re-stamp evidence is persisted
    with its feed date -- the behaviour before the fix;
  * R3 flags on BATCH evidence only (3 re-dates within 10 minutes): one or two
    isolated updates, or three spread over hours, flag nothing;
  * on a flagged feed a page we could not fetch (403, a WAF challenge) is
    admitted with the feed date and counted, never deferred;
  * `Kpler` does not count as a keyword on kpler.com;
plus the guard rails: lookup failure defers, Google News is exempt, the budget
rotates, and one INFO line is always logged.

The collector, the page fetch and the news_articles lookup are stubbed; the
pipeline, the enrich path and the date rules are the real ones.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import enrich, pipeline, supabase_sync  # noqa: E402
from news_hunter.date_credibility import StoredDates  # noqa: E402
from news_hunter.fetcher import RawItem  # noqa: E402

UTC = timezone.utc
NOW = datetime.now(UTC)
TODAY = NOW.strftime("%b %d, %Y")
# The live keyword set carries `Kpler` (meant for what OTHER outlets write about
# the company); keeping it here is what makes the own-name tests meaningful.
KWS = ["Hormuz", "crude", "tanker", "shadow fleet", "LNG", "oil", "Kpler"]
FEED = "www.kpler.com"
BLOG = "https://kpler.com/blog/"

# Kpler's RSS <description>: long enough to be the snippet, so in fast mode the
# enrich path never fetches the page -- exactly how the burst got through.
SUMMARY = (
    "For shipping and commodity desks, Hormuz transit data and crude tanker flows "
    "are the structural signal to watch this quarter, as freight and LNG markets "
    "reprice around the Strait and shadow fleet exposure keeps rising."
)


def _page(h1: str, date_published: str, body: str = SUMMARY) -> str:
    ld = {"@context": "https://schema.org", "@graph": [
        {"@type": "BlogPosting", "headline": h1, "datePublished": date_published,
         "dateModified": "Sep 25, 2026", "publisher": {"@type": "Organization", "name": "Kpler"}},
        {"@type": "BreadcrumbList", "itemListElement": []},
    ]}
    return (f"<html><head><title>{h1} | Kpler - {date_published}</title>"
            f'<script type="application/ld+json">{json.dumps(ld)}</script></head>'
            f'<body><h1 class="single-blog-heading">{h1}</h1>'
            f"<p>{body}</p><p>{body}</p></body></html>")


# What Cloudflare answers with HTTP 200 while it challenges a bot.
CHALLENGE_PAGE = (
    "<html><head><title>Just a moment...</title>"
    '<script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1?ray=8c"></script>'
    "</head><body><h1>www.kpler.com</h1><p>Verifying you are human. This may take a few seconds.</p>"
    "</body></html>"
)


def _item(slug: str, title: str, *, age_h: float = 1.0, summary: str = SUMMARY,
          feed: str = FEED, url: str | None = None, source: str = "kpler.com") -> RawItem:
    return RawItem(
        url=url or BLOG + slug, title=title, summary=summary,
        published_at=NOW - timedelta(hours=age_h),
        source_domain=source, feed_domain=feed, source_lang="en",
    )


def _stored(slugs, days: float = 11) -> dict[str, StoredDates]:
    first_seen = NOW - timedelta(days=days)
    return {BLOG + s: StoredDates(published_at=first_seen, created_at=first_seen) for s in slugs}


# Three stored posts the feed re-dates within 3 minutes -- the batch of
# 2026-09-25 (10:41:49 .. 10:45:08). Their title dates are left out here so the
# batch is DB evidence alone.
BATCH_SLUGS = [
    "crude-tanker-rates-hit-new-highs-as-hormuz-risk-escalates",
    "galveston-didnt-just-get-busier-its-port-call-pattern-changed-shape",
    "irans-long-reach-saudis-east-west-pipeline-under-fire",
]
BATCH = [
    _item(BATCH_SLUGS[0], "Crude tanker rates hit new highs as Hormuz risk escalates Crude tanker "
                          "rates hit new highs as Hormuz risk escalates | Kpler -", age_h=1.25),
    _item(BATCH_SLUGS[1], "Galveston didn't just get busier. Its port call data changed for LNG "
                          "tankers | Kpler -", age_h=1.23),
    _item(BATCH_SLUGS[2], "East-West Pipeline Attack Threatens Saudi Oil Exports Iran's long reach: "
                          "Saudi's east-west pipeline under fire | Kpler -", age_h=1.20),
]
BATCH_ROWS = _stored(BATCH_SLUGS)

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

BLOCKED_SLUG = "lng-freight-spikes-as-hormuz-queues-build"
BLOCKED = _item(BLOCKED_SLUG, "LNG freight spikes as Hormuz queues build | Kpler -", age_h=0.4)

PAGES = {
    BLOG + OLD_SLUG: _page(OLD_H1, "Apr 01, 2026"),
    BLOG + DATELESS_SLUG: _page("Russian barrels to fill Chinese crude stocks?", ""),
    BLOG + NEW_SLUG: _page(NEW_H1, TODAY),
    **{BLOG + s: _page("stored post", "") for s in BATCH_SLUGS},
}


class _Run:
    def __init__(self):
        self.persisted: list = []
        self.fetched: list[str] = []
        self.lookups: list[list[str]] = []
        self.stats = None


def _drive(monkeypatch, items, *, stored=None, lookup_fails=False, pages=None, feed=FEED,
           slow: float = 0.0):
    """run_search over `items` with the collector, page fetch and lookup stubbed.

    A url missing from `pages` answers like a WAF: HTTP 403.
    """
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
        if slow:
            time.sleep(slow)
        if url not in pages:
            raise RuntimeError("403 Client Error: Forbidden")
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


def _line(caplog, prefix):
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith(prefix)]
    assert len(lines) == 1, lines
    return lines[0]


# ---------------------------------------------------------------------------
# The four behaviours the fix is about
# ---------------------------------------------------------------------------

def test_fresh_feed_date_with_page_date_apr_01_is_not_persisted(monkeypatch):
    """R3 flags the feed (a batch of stored posts re-dated), the page says Apr 01."""
    run = _drive(monkeypatch, [*BATCH, OLD], stored=BATCH_ROWS)
    assert run.stats.flagged() == [FEED]
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + OLD_SLUG) == "older_page"
    assert BLOG + OLD_SLUG in run.fetched        # the early return was bypassed
    # The stored rows themselves are not new: re-sent, never deferred.
    assert {BLOG + s for s in BATCH_SLUGS} <= set(_by_url(run))


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
    # One contradiction is evidence, not a batch: the feed is not flagged.
    assert len(run.stats.title_evidence[FEED]) == 1 and run.stats.flagged() == []


def test_empty_page_date_with_restamp_evidence_is_deferred(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [*BATCH, DATELESS], stored=BATCH_ROWS)
    assert BLOG + DATELESS_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"
    assert run.stats.deferred == {FEED: {"no_page_date": 1}}
    line = _line(caplog, "date credibility:")
    assert "restamp_domains=[www.kpler.com(db=3/3)]" in line
    assert "deferred=1 [www.kpler.com: no_page_date=1]" in line


def test_genuinely_new_item_whose_page_date_matches_is_persisted(monkeypatch):
    run = _drive(monkeypatch, [*BATCH, NEW], stored=BATCH_ROWS)
    got = _by_url(run).get(BLOG + NEW_SLUG)
    assert got is not None
    assert _outcome(run, BLOG + NEW_SLUG) == "verified"
    assert got.published_at == NEW.published_at          # the feed date, untouched
    assert got.title == NEW_H1                           # T: the page <h1>
    assert "Kpler" not in got.matched_keywords           # own name never counts


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
# R3 is batch evidence only
# ---------------------------------------------------------------------------

def test_one_isolated_redate_flags_nothing(monkeypatch, caplog):
    """A feed re-dating ONE stored article is updating it (wsj, estadao...)."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [BATCH[0], DATELESS], stored=_stored(BATCH_SLUGS[:1]))
    assert run.stats.flagged() == []
    assert run.fetched == []
    assert BLOG + DATELESS_SLUG in _by_url(run)
    assert "isolated=[www.kpler.com(db=1/1)]" in _line(caplog, "date credibility:")


def test_two_redates_are_still_not_a_batch(monkeypatch):
    run = _drive(monkeypatch, [*BATCH[:2], DATELESS], stored=_stored(BATCH_SLUGS[:2]))
    assert run.stats.flagged() == [] and BLOG + DATELESS_SLUG in _by_url(run)


def test_three_redates_spread_over_hours_flag_nothing(monkeypatch):
    """estadao's shape: six unrelated updates in 2.2 h, never three in 10 minutes."""
    spread = [_item(s, BATCH[i].title, age_h=1.0 + 1.5 * i) for i, s in enumerate(BATCH_SLUGS)]
    run = _drive(monkeypatch, [*spread, DATELESS], stored=BATCH_ROWS)
    assert len(run.stats.restamp_db[FEED]) == 3
    assert run.stats.flagged() == []
    assert BLOG + DATELESS_SLUG in _by_url(run)


def test_a_batch_of_title_date_contradictions_flags_the_feed(monkeypatch):
    titled = [
        _item(f"old-hormuz-note-{n}", f"Old Hormuz crude note {n} | Kpler - Jul 2{n}, 2026",
              age_h=1.0 + n / 60)
        for n in range(3)
    ]
    run = _drive(monkeypatch, [*titled, DATELESS])
    assert run.stats.flagged() == [FEED]
    assert all(_outcome(run, BLOG + f"old-hormuz-note-{n}") == "older_title" for n in range(3))
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"


def test_two_title_date_contradictions_do_not_flag(monkeypatch):
    titled = [
        _item(f"old-hormuz-note-{n}", f"Old Hormuz crude note {n} | Kpler - Jul 2{n}, 2026",
              age_h=1.0 + n / 60)
        for n in range(2)
    ]
    run = _drive(monkeypatch, [*titled, DATELESS])
    assert run.stats.flagged() == [] and BLOG + DATELESS_SLUG in _by_url(run)


# ---------------------------------------------------------------------------
# A page we could not fetch is admitted, never deferred
# ---------------------------------------------------------------------------

def test_fetch_failure_on_a_flagged_feed_admits_the_item_unverified(monkeypatch, caplog):
    """investing.com fails 8/8 from the runner: flagged, it must not become a zero."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [*BATCH, BLOCKED], stored=BATCH_ROWS)   # BLOCKED -> HTTP 403
    got = _by_url(run).get(BLOG + BLOCKED_SLUG)
    assert got is not None and got.published_at == BLOCKED.published_at
    assert _outcome(run, BLOG + BLOCKED_SLUG) == "admitted_fetch_failed"
    assert run.stats.unverified == {FEED: {"fetch_failed": 1}} and run.stats.n_deferred == 0
    assert "unverified_admitted=1 [www.kpler.com: fetch_failed=1]" in _line(caplog, "date credibility:")


def test_a_waf_challenge_page_is_admitted_unverified(monkeypatch):
    pages = {**PAGES, BLOG + BLOCKED_SLUG: CHALLENGE_PAGE}
    run = _drive(monkeypatch, [*BATCH, BLOCKED], stored=BATCH_ROWS, pages=pages)
    assert BLOG + BLOCKED_SLUG in _by_url(run)
    assert _outcome(run, BLOG + BLOCKED_SLUG) == "admitted_challenge"


def test_our_own_deadline_defers_to_the_next_scan(monkeypatch):
    monkeypatch.setattr(pipeline, "DATE_VERIFY_DEADLINE", 0.05)
    run = _drive(monkeypatch, [*BATCH, NEW], stored=BATCH_ROWS, slow=0.4)
    assert BLOG + NEW_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + NEW_SLUG) == "deferred_deadline"
    # The abandoned fetch still finishes in its thread: let it, so it cannot
    # record its page into the next test's registry.
    time.sleep(0.5)


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------

def test_the_burst_shape_end_to_end(monkeypatch):
    """Stored posts re-dated in a batch + old + dateless + blocked + genuinely new."""
    run = _drive(monkeypatch, [*BATCH, OLD, DATELESS, BLOCKED, NEW], stored=BATCH_ROWS)
    urls = set(_by_url(run))
    assert urls == {*(BLOG + s for s in BATCH_SLUGS), BLOG + NEW_SLUG, BLOG + BLOCKED_SLUG}
    assert run.stats.verified == 1 and run.stats.n_page_older == 1
    assert run.stats.n_deferred == 1 and run.stats.n_unverified == 1


def test_lookup_failure_defers_every_feed_candidate(monkeypatch):
    run = _drive(monkeypatch, [*BATCH, NEW], stored=BATCH_ROWS, lookup_fails=True)
    assert run.persisted == []
    assert run.stats.lookup_failed is True
    assert run.stats.deferred == {FEED: {"lookup_failed": 4}}


def test_google_news_items_are_exempt_from_r3(monkeypatch):
    gnews = [_item(s, BATCH[i].title, feed="news.google.com", age_h=1.2 + i / 100)
             for i, s in enumerate(BATCH_SLUGS)]
    gnews_old = _item(OLD_SLUG, "How to build a risk tree", feed="news.google.com")
    run = _drive(monkeypatch, [*gnews, gnews_old], stored=BATCH_ROWS, feed="news.google.com")
    assert run.stats.flagged() == []
    assert run.lookups == []                             # nothing to check: no lookup at all
    assert {BLOG + OLD_SLUG, *(BLOG + s for s in BATCH_SLUGS)} <= set(_by_url(run))


def test_stored_published_at_already_pushed_forward_still_testifies(monkeypatch):
    """An earlier re-stamp moved published_at; created_at still exposes the next."""
    stored = {BLOG + s: StoredDates(published_at=it.published_at, created_at=NOW - timedelta(days=11))
              for s, it in zip(BATCH_SLUGS, BATCH)}
    run = _drive(monkeypatch, [*BATCH, DATELESS], stored=stored)
    assert run.stats.flagged() == [FEED]
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"


def test_evidence_beyond_the_enrich_cap_still_flags_the_feed(monkeypatch):
    """The stored posts sit below the first 20 items of the feed (Kpler, 11:54 UTC)."""
    newest_dateless = _item(DATELESS_SLUG, DATELESS.title, age_h=0.05)
    fillers = [_item(f"hormuz-note-{n}", f"Hormuz crude note number {n}", age_h=0.2 + n / 100)
               for n in range(20)]
    pages = {**PAGES, **{BLOG + f"hormuz-note-{n}": _page(f"Hormuz crude note number {n}", TODAY)
                         for n in range(20)}}
    run = _drive(monkeypatch, [newest_dateless, *fillers, *BATCH], stored=BATCH_ROWS, pages=pages)
    # BATCH never reached stage 4 (items 22-24 of the feed) and still testified.
    assert not ({BLOG + s for s in BATCH_SLUGS} & set(_by_url(run)))
    assert run.stats.flagged() == [FEED]
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"


def test_title_evidence_counts_only_items_the_feed_presents_as_fresh(monkeypatch):
    # Three-day-old items whose titles print even older dates: not today's
    # problem, and they must not flag the feed for a week.
    stale = [_item(f"old-note-{n}", f"Old note on Hormuz {n} | Kpler - Jun 30, 2026", age_h=72 + n / 60)
             for n in range(3)]
    run = _drive(monkeypatch, [*stale, DATELESS])
    assert run.stats.flagged() == []
    assert BLOG + DATELESS_SLUG in _by_url(run)


def test_verification_budget_defers_the_rest_and_rotates(monkeypatch):
    many = [_item(f"hormuz-brief-{n}", f"Hormuz brief number {n} on crude flows", age_h=0.1 + n / 60)
            for n in range(12)]
    pages = {**PAGES, **{BLOG + f"hormuz-brief-{n}": _page(f"Hormuz brief number {n} on crude flows", TODAY)
                         for n in range(12)}}
    run = _drive(monkeypatch, [*BATCH, *many], stored=BATCH_ROWS, pages=pages)
    assert run.stats.verified == pipeline.DATE_VERIFY_CAP_DOMAIN
    assert run.stats.deferred == {FEED: {"over_budget": 12 - pipeline.DATE_VERIFY_CAP_DOMAIN}}
    kept = set(_by_url(run)) - {BLOG + s for s in BATCH_SLUGS}
    # the newest half of the budget always goes to the newest items
    newest = {BLOG + f"hormuz-brief-{n}" for n in range(pipeline.DATE_VERIFY_CAP_DOMAIN // 2)}
    assert newest <= kept


def test_date_credibility_line_is_logged_with_zeros(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    _drive(monkeypatch, [DATELESS])
    assert _line(caplog, "date credibility:").startswith(
        "date credibility: page_older=0 [] (title_date=0) restamp_domains=[] isolated=[]"
        " verified=0 unverified_admitted=0 [] deferred=0 []"
    )
    assert _line(caplog, "own-name keywords:") == (
        "own-name keywords: 0 items matched only their source's own name"
    )


def test_approximate_dates_are_never_candidates(monkeypatch):
    """A clamped future date is fabricated (published_is_approx): R3 leaves it to
    _freeze_approx_dates, which must keep working exactly as before."""
    future = _item("scheduled-hormuz-post", "Scheduled Hormuz crude post", age_h=-3)
    run = _drive(monkeypatch, [*BATCH, future], stored=BATCH_ROWS)
    got = _by_url(run).get(BLOG + "scheduled-hormuz-post")
    assert got is not None and got.published_is_approx is True
    assert _outcome(run, BLOG + "scheduled-hormuz-post") is None


# ---------------------------------------------------------------------------
# `Kpler` is not a keyword on kpler.com (real titles, 2026-09-25)
# ---------------------------------------------------------------------------

MARKETING_SLUG = "how-to-choose-ship-tracking-software-for-your-business"
MARKETING = _item(
    MARKETING_SLUG,
    "How to choose ship tracking software for your business How to choose the right ship "
    f"tracking software for your business | Kpler - {TODAY}",
    summary=("This guide explains how ship tracking works, who uses it, what data powers it, "
             "and how to choose the right solution for your maritime operations with Kpler."),
)
LNG_SLUG = "hormuz-risk-and-winter-restocking-keep-lng-bid"
LNG_POST = _item(
    LNG_SLUG,
    "Hormuz risk and winter restocking keep LNG bid Hormuz risk and winter restocking keep LNG "
    f"bid | Kpler - {TODAY}",
)


def test_marketing_post_that_matched_only_kpler_is_dropped_and_counted(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    pages = {BLOG + MARKETING_SLUG: _page("How to choose the right ship tracking software for your "
                                          "business", TODAY, body="Kpler helps maritime teams track ships.")}
    run = _drive(monkeypatch, [MARKETING], pages=pages)
    assert run.persisted == []
    assert _line(caplog, "own-name keywords:") == (
        "own-name keywords: 1 items matched only their source's own name (kpler.com=1)"
    )


def test_crude_lng_post_keeps_its_other_keywords(monkeypatch):
    run = _drive(monkeypatch, [LNG_POST])
    got = _by_url(run).get(BLOG + LNG_SLUG)
    assert got is not None
    assert "Kpler" not in got.matched_keywords
    assert {"Hormuz", "LNG"} <= set(got.matched_keywords)
    assert got.title == "Hormuz risk and winter restocking keep LNG bid"


def test_kpler_still_counts_on_every_other_domain(monkeypatch):
    other = _item("kpler-data-shows-record-lng-exports", "Kpler data shows record exports",
                  url="https://www.reuters.com/markets/kpler-data-shows-record-lng-exports",
                  source="www.reuters.com", feed="www.reuters.com")
    run = _drive(monkeypatch, [other], feed="www.reuters.com")
    got = _by_url(run).get("https://reuters.com/markets/kpler-data-shows-record-lng-exports")
    assert got is not None and "Kpler" in got.matched_keywords


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
