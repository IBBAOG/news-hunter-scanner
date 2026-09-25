"""End to end: run_search over Kpler-shaped feed items (the 2026-09-25 re-stamp burst).

What each test pins, in the words of the fix:
  * a fresh feed <pubDate> on a page whose datePublished is "Apr 01, 2026" is
    not persisted (R2 through the spot check, the verify phase, the backfill
    fetch, and the date the feed prints in its own title);
  * an empty page datePublished in a FLAGGED feed is DEFERRED;
  * a genuinely new item whose page date matches is persisted, headline clean;
  * an item with no page date in a feed WITHOUT re-stamp evidence is persisted
    with its feed date;
  * R3 flags on BATCH evidence (3 within 10 minutes: stored re-dates, title
    contradictions and page-proven old items together); one or two isolated
    updates, or three spread over hours, flag nothing;
  * a never-seen item of a NON-flagged feed gets a spot check that never
    defers, and whose old pages escalate the feed in the same scan;
  * on a flagged feed, a page we could not read defers the item while other
    pages of the same site were read this scan, and admits it (counted) only
    when the whole site blocks us;
  * a failed lookup costs only its own urls;
  * `Kpler` does not count as a keyword on kpler.com.

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

from news_hunter import enrich, pipeline  # noqa: E402
from news_hunter.date_credibility import StoredDates, StoredLookup  # noqa: E402
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


def _stored(urls, days: float = 11) -> dict[str, StoredDates]:
    first_seen = NOW - timedelta(days=days)
    return {(u if u.startswith("http") else BLOG + u): StoredDates(published_at=first_seen,
                                                                   created_at=first_seen)
            for u in urls}


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


def _drive(monkeypatch, items=None, *, stored=None, pages=None, feed=FEED, slow: float = 0.0,
           fail_lookup=(), feeds=None, hours=24):
    """run_search with the collector, page fetch and lookup stubbed.

    A url missing from `pages` answers like a WAF: HTTP 403; a page given as
    an exception raises it (a timeout, a 5xx). A url in `fail_lookup` (or
    every url, with fail_lookup="all") fails its lookup; fail_lookup="down"
    fails every url with the database unavailable.
    `feeds` = [(feed_domain, items), ...] to collect several feeds.
    """
    run = _Run()
    pages = PAGES if pages is None else pages
    batches = feeds if feeds is not None else [(feed, list(items or []))]

    monkeypatch.setattr(pipeline, "get_config",
                        lambda: {"keywords": KWS, "exact_keywords": set(), "window_hours": hours})
    monkeypatch.setattr(pipeline, "iter_collect",
                        lambda *a, **k: iter([(fd, list(its), None) for fd, its in batches]))
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
        if isinstance(pages[url], BaseException):
            raise pages[url]
        return pages[url]

    monkeypatch.setattr(enrich, "fetch_html", _fetch_html)

    def _existing_dates(urls):
        run.lookups.append(list(urls))
        down = fail_lookup == "down"
        failed = set(urls) if fail_lookup in ("all", "down") else {u for u in urls if u in fail_lookup}
        found = {u: s for u, s in (stored or {}).items() if u in urls and u not in failed}
        return StoredLookup(found=found, failed=failed,
                            unavailable="ConnectTimeout: database down" if down else "")

    monkeypatch.setattr(pipeline, "existing_dates", _existing_dates)

    base = pipeline._DateStats

    class _Tap(base):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            run.stats = self

    monkeypatch.setattr(pipeline, "_DateStats", _Tap)
    pipeline.run_search(include_google_news=False, fast_mode=True, hours_override=hours)
    return run


def _by_url(run):
    return {a.url: a for a in run.persisted}


def _outcome(run, url):
    return dict(run.stats.trace).get(url)


def _line(caplog, prefix):
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith(prefix)]
    assert len(lines) == 1, lines
    return lines[0]


def _kinds(run, fd=FEED):
    out: dict[str, int] = {}
    for _date, kind in run.stats.evidence.get(fd, {}).values():
        out[kind] = out.get(kind, 0) + 1
    return out


# ---------------------------------------------------------------------------
# The four behaviours the fix is about
# ---------------------------------------------------------------------------

def test_fresh_feed_date_with_page_date_apr_01_is_not_persisted(monkeypatch):
    """A flagged feed (a batch of stored posts re-dated); the page says Apr 01."""
    run = _drive(monkeypatch, [*BATCH, OLD], stored=BATCH_ROWS)
    assert run.stats.flagged() == [FEED]
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + OLD_SLUG) == "older_page"
    assert BLOG + OLD_SLUG in run.fetched        # the early return was bypassed
    # The stored rows themselves are not new: re-sent, never checked or deferred.
    assert {BLOG + s for s in BATCH_SLUGS} <= set(_by_url(run))
    assert not {BLOG + s for s in BATCH_SLUGS} & set(run.fetched)


def test_page_date_apr_01_is_caught_by_the_spot_check_without_any_evidence(monkeypatch):
    """No history, no batch: the spot check alone reads the page and drops it."""
    run = _drive(monkeypatch, [OLD])
    assert run.stats.flagged() == []
    assert BLOG + OLD_SLUG in run.fetched
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + OLD_SLUG) == "older_page"
    assert _kinds(run) == {"page": 1}            # one piece of evidence, not a batch


def test_date_printed_in_the_feed_title_drops_the_item_without_a_fetch(monkeypatch):
    """ "... | Kpler - Mar 31, 2026" on a fresh <pubDate> is an old post, no page needed."""
    titled = _item(OLD_SLUG, f"How to build a risk tree to assess shadow fleet exposure {OLD_H1} "
                             "| Kpler - Mar 31, 2026")
    run = _drive(monkeypatch, [titled], pages={})
    assert BLOG + OLD_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + OLD_SLUG) == "older_title"
    assert run.fetched == []
    assert _kinds(run) == {"title": 1} and run.stats.flagged() == []


def test_empty_page_date_on_a_flagged_feed_is_deferred(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [*BATCH, DATELESS], stored=BATCH_ROWS)
    assert BLOG + DATELESS_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"
    assert run.stats.deferred == {FEED: {"no_page_date": 1}}
    line = _line(caplog, "date credibility:")
    assert "restamp_domains=[www.kpler.com(db=3;batch=3)]" in line
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
    """No evidence: the spot check reads a dateless page and ADMITS the item."""
    run = _drive(monkeypatch, [DATELESS])
    got = _by_url(run).get(BLOG + DATELESS_SLUG)
    assert got is not None and got.published_at == DATELESS.published_at
    assert _outcome(run, BLOG + DATELESS_SLUG) == "spot_dateless"
    assert run.stats.flagged() == [] and run.stats.n_deferred == 0
    assert got.title == "Russian barrels to fill Chinese crude stocks?"


# ---------------------------------------------------------------------------
# R3 is batch evidence only
# ---------------------------------------------------------------------------

def test_one_isolated_redate_flags_nothing(monkeypatch, caplog):
    """A feed re-dating ONE stored article is updating it (wsj, estadao...)."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [BATCH[0], DATELESS], stored=_stored(BATCH_SLUGS[:1]))
    assert run.stats.flagged() == []
    assert BLOG + DATELESS_SLUG in _by_url(run)
    assert "isolated=[www.kpler.com(db=1;batch=1)]" in _line(caplog, "date credibility:")


def test_two_redates_are_still_not_a_batch(monkeypatch):
    run = _drive(monkeypatch, [*BATCH[:2], DATELESS], stored=_stored(BATCH_SLUGS[:2]))
    assert run.stats.flagged() == [] and BLOG + DATELESS_SLUG in _by_url(run)


def test_three_redates_spread_over_hours_flag_nothing(monkeypatch):
    """estadao's shape: six unrelated updates in 2.2 h, never three in 10 minutes."""
    spread = [_item(s, BATCH[i].title, age_h=1.0 + 1.5 * i) for i, s in enumerate(BATCH_SLUGS)]
    run = _drive(monkeypatch, [*spread, DATELESS], stored=BATCH_ROWS)
    assert _kinds(run) == {"db": 3}
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


def test_title_dates_equal_to_the_feed_date_are_not_evidence(monkeypatch):
    """Three fresh posts printing today's date: not contradictions, no flag."""
    today = [_item(f"hormuz-brief-today-{n}", f"Hormuz brief {n} | Kpler - {TODAY}", age_h=0.2 + n / 60)
             for n in range(3)]
    run = _drive(monkeypatch, [*today, DATELESS])
    assert run.stats.evidence == {} and run.stats.flagged() == []
    assert BLOG + DATELESS_SLUG in _by_url(run)


def test_evidence_kinds_add_up_to_one_batch(monkeypatch):
    """Two stored re-dates and one title contradiction within minutes: a batch."""
    titled = _item("old-hormuz-note-9", "Old Hormuz crude note 9 | Kpler - Jul 29, 2026", age_h=1.22)
    run = _drive(monkeypatch, [*BATCH[:2], titled, DATELESS], stored=_stored(BATCH_SLUGS[:2]))
    assert _kinds(run) == {"db": 2, "title": 1}
    assert run.stats.flagged() == [FEED]


# ---------------------------------------------------------------------------
# The spot check (never-seen items of a feed that is NOT flagged)
# ---------------------------------------------------------------------------

def test_spot_check_never_defers(monkeypatch):
    blocked = _item("blocked-post", "LNG blocked post", age_h=0.3)       # page 403
    run = _drive(monkeypatch, [DATELESS, blocked])
    assert {BLOG + DATELESS_SLUG, BLOG + "blocked-post"} <= set(_by_url(run))
    assert _outcome(run, BLOG + DATELESS_SLUG) == "spot_dateless"
    assert _outcome(run, BLOG + "blocked-post") == "spot_unread"
    assert run.stats.n_deferred == 0


def _republish(n, *, minutes_apart, feed="www.newsource.com", host="newsource.com", same_date=""):
    items, pages = [], {}
    for i in range(n):
        slug = f"old-crude-analysis-{i}"
        url = f"https://{host}/analysis/{slug}"
        items.append(_item(slug, f"Old crude oil analysis number {i}", url=url, source=host, feed=feed,
                           age_h=0.2 + i * minutes_apart / 60))
        # Each old post keeps its own date (Kpler's burst: Mar 31 .. Sep 10).
        pages[url] = _page(f"Old crude oil analysis number {i}", same_date or f"Mar {i + 1:02d}, 2026")
    return items, pages


def test_small_burst_in_a_feed_with_no_history_escalates_and_leaks_nothing(monkeypatch):
    """5 old posts re-published within 5 minutes, no title dates, never stored.

    The spot check reads 4 pages, all older: 4 page-evidence items in one
    10-minute window flag the feed in the SAME scan, and the 5th is checked in
    round 2. Nothing leaks.
    """
    items, pages = _republish(5, minutes_apart=1)
    run = _drive(monkeypatch, feed="www.newsource.com", items=items, pages=pages)
    assert run.stats.flagged() == ["www.newsource.com"]
    assert _kinds(run, "www.newsource.com") == {"page": 5}
    assert run.persisted == []
    assert run.stats.n_page_older == 5


def test_a_spread_burst_leaks_at_most_the_items_beyond_the_spot_budget(monkeypatch):
    """The same 5 posts re-published 30 minutes apart: never a 10-minute batch.

    The spot check still drops every page it reads (4); the 5th is admitted
    unchecked -- the leak is bounded by N - DATE_SPOT_CAP_DOMAIN per scan.
    """
    items, pages = _republish(5, minutes_apart=30)
    run = _drive(monkeypatch, feed="www.newsource.com", items=items, pages=pages)
    assert run.stats.flagged() == []
    assert run.stats.n_page_older == pipeline.DATE_SPOT_CAP_DOMAIN
    assert len(run.persisted) == 5 - pipeline.DATE_SPOT_CAP_DOMAIN
    assert run.stats.spot == {"unchecked": 1}


def test_a_burst_whose_old_posts_share_one_date_reads_as_a_template(monkeypatch, caplog):
    """The template guard's accepted residual, pinned: five old posts that all
    carry the SAME page date look like a CMS constant, so the feed's page dates
    are ignored this scan -- no page evidence, no flag, the posts are admitted
    as dateless. Counted and named in the log (template_date=[...])."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    items, pages = _republish(5, minutes_apart=1, same_date="Mar 03, 2026")
    run = _drive(monkeypatch, feed="www.newsource.com", items=items, pages=pages)
    assert run.stats.template == {"www.newsource.com": "Mar 03, 2026"}
    assert run.stats.flagged() == [] and run.stats.n_page_older == 0
    assert run.stats.spot == {"dateless": 4, "unchecked": 1}
    assert "template_date=[www.newsource.com(Mar 03, 2026)]" in _line(caplog, "date credibility:")


def test_a_template_date_never_drops_genuinely_new_articles(monkeypatch):
    """A CMS that prints one fixed old date on every page: three new articles in
    one scan would all be re-dated to it and dropped. They are kept instead."""
    host, feed = "template.example.com", "www.template.example.com"
    items, pages = [], {}
    for n in range(3):
        url = f"https://{host}/news/crude-story-{n}"
        items.append(_item(f"crude-story-{n}", f"Crude story number {n} today", url=url, source=host,
                           feed=feed, age_h=0.1 + n / 60))
        pages[url] = _page(f"Crude story number {n} today", "2020-01-01T00:00:00Z")
    run = _drive(monkeypatch, feed=feed, items=items, pages=pages)
    assert len(run.persisted) == 3
    assert all(a.published_at > NOW - timedelta(hours=1) for a in run.persisted)
    assert feed in run.stats.template and run.stats.n_page_older == 0


def test_two_items_sharing_an_old_date_are_not_a_template(monkeypatch):
    host, feed = "pair.example.com", "www.pair.example.com"
    items, pages = [], {}
    for n in range(2):
        url = f"https://{host}/news/crude-story-{n}"
        items.append(_item(f"crude-story-{n}", f"Crude story number {n} today", url=url, source=host,
                           feed=feed, age_h=0.1 + n / 60))
        pages[url] = _page(f"Crude story number {n} today", "Mar 03, 2026")
    run = _drive(monkeypatch, feed=feed, items=items, pages=pages)
    assert run.persisted == [] and run.stats.template == {} and run.stats.n_page_older == 2


def test_a_smaller_spot_budget_leaks_the_rest(monkeypatch):
    """The bound, measured: with 2 spot checks per feed a clustered 5-post burst
    cannot reach the 3-item batch, so 5 - 2 = 3 posts get through."""
    monkeypatch.setattr(pipeline, "DATE_SPOT_CAP_DOMAIN", 2)
    items, pages = _republish(5, minutes_apart=1)
    run = _drive(monkeypatch, feed="www.newsource.com", items=items, pages=pages)
    assert run.stats.flagged() == [] and len(run.persisted) == 3


# ---------------------------------------------------------------------------
# A page we could not read: deferred unless the whole site blocks us
# ---------------------------------------------------------------------------

def test_a_403_on_an_old_post_of_a_readable_site_is_deferred(monkeypatch):
    """The QA probe: flagged www.kpler.com, the old post's page answers 403 while
    another Kpler page was read this scan -> deferred, never published."""
    run = _drive(monkeypatch, [*BATCH, NEW, BLOCKED], stored=BATCH_ROWS)   # NEW reads, BLOCKED 403
    assert BLOG + BLOCKED_SLUG not in _by_url(run)
    assert _outcome(run, BLOG + BLOCKED_SLUG) == "deferred_unread"
    assert BLOG + NEW_SLUG in _by_url(run)


def test_a_site_blocked_as_a_whole_admits_its_items_unverified(monkeypatch, caplog):
    """investing.com fails 8/8 from the runner: flagged, it must not become a zero."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [*BATCH, BLOCKED], stored=BATCH_ROWS)   # no Kpler page reads
    got = _by_url(run).get(BLOG + BLOCKED_SLUG)
    assert got is not None and got.published_at == BLOCKED.published_at
    assert _outcome(run, BLOG + BLOCKED_SLUG) == "admitted_site_blocked"
    assert run.stats.unverified == {FEED: {"site_blocked": 1}} and run.stats.n_deferred == 0
    assert "unverified_admitted=1 [www.kpler.com: site_blocked=1]" in _line(caplog, "date credibility:")


def test_a_waf_challenge_follows_the_same_site_rule(monkeypatch):
    pages = {**PAGES, BLOG + BLOCKED_SLUG: CHALLENGE_PAGE}
    alone = _drive(monkeypatch, [*BATCH, BLOCKED], stored=BATCH_ROWS, pages=pages)
    assert _outcome(alone, BLOG + BLOCKED_SLUG) == "admitted_site_blocked"


def test_a_waf_challenge_on_a_readable_site_is_deferred(monkeypatch):
    pages = {**PAGES, BLOG + BLOCKED_SLUG: CHALLENGE_PAGE}
    run = _drive(monkeypatch, [*BATCH, NEW, BLOCKED], stored=BATCH_ROWS, pages=pages)
    assert _outcome(run, BLOG + BLOCKED_SLUG) == "deferred_unread"


def test_a_check_cut_by_our_deadline_defers_on_a_flagged_feed(monkeypatch):
    """No answer is not a block: a check that did not complete (our deadline,
    cancelled, never started) counts as NOT CHECKED -- deferred, never admitted,
    even with no page of the site read."""
    monkeypatch.setattr(pipeline, "DATE_CHECK_DEADLINE", 0.05)
    run = _drive(monkeypatch, [*BATCH, NEW], stored=BATCH_ROWS, slow=0.4)
    assert run.stats.flagged() == [FEED]
    assert _outcome(run, BLOG + NEW_SLUG) == "deferred_incomplete"
    assert BLOG + NEW_SLUG not in _by_url(run) and run.stats.n_unverified == 0
    time.sleep(0.5)   # let the abandoned fetch finish before the next test


def test_a_check_cut_by_our_deadline_is_admitted_on_a_feed_that_is_not_flagged(monkeypatch):
    monkeypatch.setattr(pipeline, "DATE_CHECK_DEADLINE", 0.05)
    run = _drive(monkeypatch, [NEW], slow=0.4)
    assert _outcome(run, BLOG + NEW_SLUG) == "spot_incomplete"
    assert BLOG + NEW_SLUG in _by_url(run)
    time.sleep(0.5)


@pytest.mark.parametrize("failure", [
    TimeoutError("HTTPSConnectionPool(host='kpler.com'): Read timed out. (read timeout=5)"),
    RuntimeError("503 Server Error: Service Unavailable for url"),
    ConnectionError("Connection aborted."),
])
def test_only_a_4xx_or_a_challenge_counts_as_a_site_blocking_us(monkeypatch, failure):
    """A completed check that timed out, got a 5xx or lost its connection is no
    refusal: on a flagged feed the item is deferred even with no page read."""
    run = _drive(monkeypatch, [*BATCH, BLOCKED], stored=BATCH_ROWS,
                 pages={**PAGES, BLOG + BLOCKED_SLUG: failure})
    assert _outcome(run, BLOG + BLOCKED_SLUG) == "deferred_unread"
    assert run.stats.n_unverified == 0


# ---------------------------------------------------------------------------
# Lookups fail per url, never for the whole scan
# ---------------------------------------------------------------------------

def test_a_url_the_lookup_cannot_answer_is_checked_as_never_seen(monkeypatch, caplog):
    """A url bisection isolates as persistently bad is never deferred forever:
    it gets its page check like any new item (the database trigger protects a
    stored date anyway)."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [*BATCH, NEW], stored=BATCH_ROWS, fail_lookup={BLOG + NEW_SLUG})
    assert _outcome(run, BLOG + NEW_SLUG) == "verified"
    assert BLOG + NEW_SLUG in _by_url(run) and BLOG + NEW_SLUG in run.fetched
    assert run.stats.flagged() == [FEED]
    assert "lookup=PARTIAL(candidates=1,evidence=0)" in _line(caplog, "date credibility:")


def test_a_failed_evidence_lookup_loses_only_that_evidence(monkeypatch, caplog):
    """The batch sits beyond the enrich cap (evidence-only urls) and its lookup
    fails: no flag, but every candidate is still persisted -- lookup=PARTIAL."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    fillers = [_item(f"hormuz-note-{n}", f"Hormuz crude note number {n}", age_h=0.2 + n / 100)
               for n in range(20)]
    pages = {**PAGES, **{BLOG + f"hormuz-note-{n}": _page(f"Hormuz crude note number {n}", TODAY)
                         for n in range(20)}}
    run = _drive(monkeypatch, [*fillers, *BATCH], stored=BATCH_ROWS, pages=pages,
                 fail_lookup={BLOG + s for s in BATCH_SLUGS})
    assert run.stats.flagged() == []
    assert len(run.persisted) == 20
    assert "lookup=PARTIAL(candidates=0,evidence=3)" in _line(caplog, "date credibility:")


def test_a_lookup_that_fails_for_every_url_says_so_and_blocks_nothing(monkeypatch, caplog):
    """The database does not answer: every candidate counts as never seen and is
    judged by its page -- no url is deferred for the lookup's sake."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    run = _drive(monkeypatch, [*BATCH, NEW], stored=BATCH_ROWS, fail_lookup="all")
    assert set(_by_url(run)) == {*(BLOG + s for s in BATCH_SLUGS), BLOG + NEW_SLUG}
    assert run.stats.n_deferred == 0
    assert "lookup=FAILED" in _line(caplog, "date credibility:")


def test_lookup_failed_when_every_url_failed_evidence_included(monkeypatch, caplog):
    """LOW 7: FAILED whenever failed == looked_up, evidence-only urls included."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    fillers = [_item(f"hormuz-note-{n}", f"Hormuz crude note number {n}", age_h=0.2 + n / 100)
               for n in range(20)]
    pages = {**PAGES, **{BLOG + f"hormuz-note-{n}": _page(f"Hormuz crude note number {n}", TODAY)
                         for n in range(20)}}
    run = _drive(monkeypatch, [*fillers, *BATCH], pages=pages, fail_lookup="all")
    assert run.stats.looked_up == 23
    assert " lookup=FAILED " in _line(caplog, "date credibility:")


# ---------------------------------------------------------------------------
# The guard never stops the scan (QA round 2, HIGH 1)
# ---------------------------------------------------------------------------

def _generic_feed(n_stored=20, n_redated=3):
    """The QA probe: a feed's first 20 matched items are stored (not re-dated);
    3 more stored urls, below the enrich cap, are re-dated together."""
    host, feed = "generic.example.com", "www.generic.example.com"
    items, stored, pages = [], {}, {}
    for n in range(n_stored):
        url = f"https://{host}/news/crude-brief-{n}"
        it = _item(f"crude-brief-{n}", f"Crude brief number {n}", url=url, source=host, feed=feed,
                   age_h=0.2 + n / 100)
        items.append(it)
        stored[url] = StoredDates(published_at=it.published_at, created_at=it.published_at)
    for n in range(n_redated):
        url = f"https://{host}/news/old-crude-brief-{n}"
        items.append(_item(f"old-crude-brief-{n}", f"Old crude brief number {n}", url=url, source=host,
                           feed=feed, age_h=0.3 + n / 100))
        stored[url] = StoredDates(published_at=NOW - timedelta(days=11), created_at=NOW - timedelta(days=11))
    return feed, items, stored, pages


def test_a_feed_flagged_only_by_evidence_beyond_the_enrich_cap_does_not_crash_the_scan(monkeypatch, caplog):
    """HIGH 1 (QA round 2): the feed has no never-seen item at all, yet its
    evidence-only lookup flags it in round 1. That used to raise KeyError out of
    Stage 4b and cost every source its inserts for the scan."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    feed, items, stored, pages = _generic_feed()
    gnews = _item("google-news-item", "Crude tanker story via Google News", feed="news.google.com",
                  url="https://www.reuters.com/markets/crude-tanker-story", source="www.reuters.com")
    run = _drive(monkeypatch, feeds=[(feed, items), ("news.google.com", [gnews])], stored=stored,
                 pages=pages)
    assert run.stats.flagged() == [feed]
    assert len(run.persisted) == 21                     # the 20 stored rows + Google News
    assert "https://reuters.com/markets/crude-tanker-story" in _by_url(run)   # normalised
    assert not [r for r in caplog.records if r.getMessage().startswith("Falha na busca")]
    assert " error=0 [] " in _line(caplog, "date credibility:")


def test_an_exception_in_one_feed_defers_only_that_feeds_items(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    other_host = "other.example.com"
    other = _item("crude-other", "Crude story from another feed", url=f"https://{other_host}/crude-other",
                  source=other_host, feed=f"www.{other_host}")
    pages = {**PAGES, f"https://{other_host}/crude-other": _page("Crude story from another feed", TODAY)}
    real = pipeline._feed_verdicts

    def _boom(plan, **kw):
        if plan.fd == FEED:
            raise ValueError("unexpected page shape")
        return real(plan, **kw)

    monkeypatch.setattr(pipeline, "_feed_verdicts", _boom)
    run = _drive(monkeypatch, feeds=[(FEED, [NEW, DATELESS]), (f"www.{other_host}", [other])], pages=pages)
    assert set(_by_url(run)) == {f"https://{other_host}/crude-other"}
    assert run.stats.deferred == {FEED: {"error": 2}}
    errs = [r for r in caplog.records if r.levelname == "ERROR" and FEED in r.getMessage()]
    assert errs and errs[0].exc_info is not None                  # with the traceback
    assert f" error=1 [{FEED}] " in _line(caplog, "date credibility:")


def test_an_exception_outside_the_feeds_never_stops_the_upsert(monkeypatch, caplog):
    """Anything Stage 4b cannot pin on one feed: logged at ERROR, counted in the
    line, and the scan persists exactly as it would have without the stage."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")

    def _boom(*a, **k):
        raise RuntimeError("scheduler exploded")

    monkeypatch.setattr(pipeline, "_fetch_order", _boom)
    gnews = _item("google-news-item", "Crude tanker story via Google News", feed="news.google.com",
                  url="https://www.reuters.com/markets/crude-tanker-story", source="www.reuters.com")
    run = _drive(monkeypatch, feeds=[(FEED, [*BATCH, OLD, NEW]), ("news.google.com", [gnews])],
                 stored=BATCH_ROWS)
    assert {BLOG + OLD_SLUG, BLOG + NEW_SLUG, "https://reuters.com/markets/crude-tanker-story"} <= set(_by_url(run))
    errs = [r for r in caplog.records if r.levelname == "ERROR" and "(Stage 4b) failed" in r.getMessage()]
    assert errs and errs[0].exc_info is not None
    assert " error=1 [stage] " in _line(caplog, "date credibility:")
    assert not [r for r in caplog.records if r.getMessage().startswith("Falha na busca")]


# ---------------------------------------------------------------------------
# One malformed item never aborts the scan (QA round 3, HIGH 1)
# ---------------------------------------------------------------------------

def test_a_title_date_with_an_impossible_offset_is_no_date(monkeypatch, caplog):
    """The QA probe: `+99:00` used to raise out of the collect loop -- 0 rows
    saved, from every source."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    odd = _item("hormuz-crude-note", "Hormuz crude note | Kpler - 2026-09-25T10:00:00+99:00", age_h=0.3)
    pages = {**PAGES, BLOG + "hormuz-crude-note": _page("Hormuz crude note", TODAY)}
    run = _drive(monkeypatch, [odd, NEW], pages=pages)
    assert {BLOG + "hormuz-crude-note", BLOG + NEW_SLUG} <= set(_by_url(run))
    assert not [r for r in caplog.records if r.getMessage().startswith("Falha na busca")]
    assert " error=0 [] " in _line(caplog, "date credibility:")


def test_a_title_evidence_step_that_raises_skips_only_that_item(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    real = pipeline._title_date_evidence

    def _boom(it):
        if it.url == BLOG + NEW_SLUG:
            raise ValueError("offset must be a timedelta strictly between ...")
        return real(it)

    monkeypatch.setattr(pipeline, "_title_date_evidence", _boom)
    run = _drive(monkeypatch, [NEW, DATELESS])
    assert {BLOG + NEW_SLUG, BLOG + DATELESS_SLUG} <= set(_by_url(run))      # the item goes on
    errs = [r for r in caplog.records if r.levelname == "ERROR" and BLOG + NEW_SLUG in r.getMessage()]
    assert errs and errs[0].exc_info is not None
    assert " error=1 [item] " in _line(caplog, "date credibility:")


def test_a_title_date_or_cleaning_that_raises_keeps_the_item_as_it_came(monkeypatch, caplog):
    """Stage 4's title date and title cleaning, each guarded per item."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")

    def _boom(*_a, **_k):
        raise ValueError("malformed title")

    monkeypatch.setattr(pipeline, "split_source_suffix", _boom)    # title evidence + title date
    monkeypatch.setattr(pipeline, "clean_display_title", _boom)    # stage 4 + Stage 4b cleaning
    run = _drive(monkeypatch, [NEW])
    got = _by_url(run)[BLOG + NEW_SLUG]
    assert got.title == NEW.title                                   # uncleaned, but saved
    line = _line(caplog, "date credibility:")
    assert " error=4 [item=4] " in line                             # evidence, date, 2 cleanings


# ---------------------------------------------------------------------------
# The candidate lookup is down (QA round 3, MEDIUM 2)
# ---------------------------------------------------------------------------

def test_lookup_down_a_feed_with_evidence_defers_what_it_cannot_confirm(monkeypatch, caplog):
    """P13b: the database is down during a burst. The stored posts it re-dates
    look never-seen and their pages are dateless; one title contradiction is
    the only evidence, below the batch. They are held back, not admitted."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    titled = _item("old-hormuz-note", "Old Hormuz crude note | Kpler - Jun 30, 2026", age_h=1.1)
    run = _drive(monkeypatch, [*BATCH, titled, NEW], stored=BATCH_ROWS, fail_lookup="down")
    assert run.stats.flagged() == [] and run.stats.cautious == {FEED}
    assert set(_by_url(run)) == {BLOG + NEW_SLUG}
    assert run.stats.deferred == {FEED: {"no_page_date": 3}}
    assert f"cautious=[{FEED}]" in _line(caplog, "date credibility:")


def test_lookup_down_unread_and_unchecked_items_are_held_back_too(monkeypatch):
    """P13d: evidence below the batch (one title contradiction); a 403 and the
    items beyond the spot budget are deferred instead of admitted."""
    titled = _item("old-hormuz-note", "Old Hormuz crude note | Kpler - Jun 30, 2026", age_h=1.1)
    more = [_item(f"hormuz-more-{n}", f"Hormuz crude more {n}", age_h=0.1 + n / 60) for n in range(5)]
    pages = {**PAGES, **{BLOG + f"hormuz-more-{n}": _page(f"Hormuz crude more {n}", TODAY) for n in range(5)}}
    run = _drive(monkeypatch, [titled, BLOCKED, *more], pages=pages, fail_lookup="down")
    assert run.stats.cautious == {FEED} and run.stats.flagged() == []
    outcomes = dict(run.stats.trace)
    assert BLOG + BLOCKED_SLUG not in _by_url(run)
    assert outcomes[BLOG + BLOCKED_SLUG] in ("deferred_unread", "deferred_over_budget")
    assert run.stats.deferred[FEED].get("over_budget", 0) == 6 - pipeline.DATE_SPOT_CAP_DOMAIN
    assert run.stats.n_unverified == 0
    assert not [o for o in outcomes.values() if o in ("spot_unchecked", "spot_unread", "spot_dateless")]


def test_lookup_down_a_feed_without_evidence_still_admits(monkeypatch):
    """P13c, the control: no evidence at all, so the spot check admits as usual."""
    run = _drive(monkeypatch, [DATELESS, BLOCKED, NEW], fail_lookup="down")
    assert run.stats.cautious == set()
    assert {BLOG + DATELESS_SLUG, BLOG + BLOCKED_SLUG, BLOG + NEW_SLUG} <= set(_by_url(run))
    assert run.stats.n_deferred == 0


def test_a_lookup_that_only_isolates_bad_urls_is_no_outage(monkeypatch):
    titled = _item("old-hormuz-note", "Old Hormuz crude note | Kpler - Jun 30, 2026", age_h=1.1)
    run = _drive(monkeypatch, [titled, DATELESS], fail_lookup={BLOG + DATELESS_SLUG})
    assert run.stats.cautious == set()
    assert BLOG + DATELESS_SLUG in _by_url(run)


# ---------------------------------------------------------------------------
# feed_fresh_spike: a bulk re-stamp read off the feed alone (QA P13e)
# ---------------------------------------------------------------------------

def _spike_feed(fresh: int, total: int, *, fresh_within_h: float, span_days: float,
                host: str = "spike.example.com", dated_pages: bool = False):
    """`fresh` items dated within `fresh_within_h` hours, the rest spread back
    to `span_days` days. Never stored, no title date; pages dateless."""
    feed = f"www.{host}"
    items, pages = [], {}
    for n in range(total):
        if n < fresh:
            age = 0.05 + (fresh_within_h - 0.1) * n / max(1, fresh - 1)
        else:
            age = 26.0 + (span_days * 24 - 26.0) * (n - fresh) / max(1, total - fresh - 1)
        url = f"https://{host}/posts/crude-post-{n}"
        items.append(_item(f"crude-post-{n}", f"Crude post number {n}", url=url, source=host,
                           feed=feed, age_h=age))
        pages[url] = _page(f"Crude post number {n}", TODAY if dated_pages else "")
    return feed, items, pages


def test_a_bulk_restamp_of_never_stored_posts_flags_the_feed(monkeypatch, caplog):
    """P13e: 81 of 100 items dated within the hour in a feed spanning 26 days,
    none stored, no title date, no page date. Flagged from the feed alone; the
    undated never-seen items are deferred instead of saved in full."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    feed, items, pages = _spike_feed(81, 100, fresh_within_h=1.0, span_days=26)
    run = _drive(monkeypatch, feed=feed, items=items, pages=pages)
    assert run.stats.flagged() == [feed]
    assert run.stats.spike[feed].fresh == 81 and run.stats.spike[feed].total == 100
    assert run.persisted == []
    assert set(run.stats.deferred[feed]) <= {"no_page_date", "over_budget"}
    assert f"{feed}(feed_fresh_spike=81/100,26d;batch=0)" in _line(caplog, "date credibility:")


def test_a_high_volume_feed_is_not_a_spike(monkeypatch):
    """50 items in the last 3 hours, spanning one day: dense, but not a re-stamp."""
    feed, items, pages = _spike_feed(35, 50, fresh_within_h=2.0, span_days=1, dated_pages=True)
    for it in items[35:]:
        it.published_at = NOW - timedelta(hours=2.5)
    items[-1].published_at = NOW - timedelta(days=1)
    run = _drive(monkeypatch, feed=feed, items=items, pages=pages)
    assert run.stats.spike == {} and run.stats.flagged() == []
    assert run.stats.n_deferred == 0 and len(run.persisted) > 0


def test_a_quiet_feed_on_a_busy_day_is_not_a_spike(monkeypatch):
    """A 12-item weekly-ish feed with 5 posts this morning: 42 % fresh."""
    feed, items, pages = _spike_feed(5, 12, fresh_within_h=1.5, span_days=10, dated_pages=True)
    run = _drive(monkeypatch, feed=feed, items=items, pages=pages)
    assert run.stats.spike == {} and run.stats.n_deferred == 0


# ---------------------------------------------------------------------------
# Budgets, caps and page reuse
# ---------------------------------------------------------------------------

def test_verification_budget_holds_the_rest_back_and_rotates(monkeypatch):
    many = [_item(f"hormuz-brief-{n}", f"Hormuz brief number {n} on crude flows", age_h=0.1 + n / 60)
            for n in range(12)]
    pages = {**PAGES, **{BLOG + f"hormuz-brief-{n}": _page(f"Hormuz brief number {n} on crude flows", TODAY)
                         for n in range(12)}}
    run = _drive(monkeypatch, [*BATCH, *many], stored=BATCH_ROWS, pages=pages)
    kept = set(_by_url(run)) - {BLOG + s for s in BATCH_SLUGS}
    assert len(kept) == pipeline.DATE_VERIFY_CAP_DOMAIN              # the rest is held back
    assert run.stats.verified == pipeline.DATE_VERIFY_CAP_DOMAIN
    assert run.stats.deferred == {FEED: {"over_budget": 12 - pipeline.DATE_VERIFY_CAP_DOMAIN}}
    newest = {BLOG + f"hormuz-brief-{n}" for n in range(pipeline.DATE_VERIFY_CAP_DOMAIN // 2)}
    assert newest <= kept                                            # newest half first


def test_the_global_cap_is_shared_round_robin(monkeypatch):
    """10 feeds x 4 never-seen items = 40 spot checks wanted, 32 allowed: every
    feed gets at least 3, and the 8 over the cap are admitted unchecked."""
    feeds, pages = [], {}
    for f in range(10):
        host = f"feed{f}.example.com"
        items = []
        for n in range(4):
            url = f"https://{host}/news/crude-note-{n}"
            items.append(_item(f"crude-note-{n}", f"Crude note {n} from feed {f}", url=url,
                               source=host, feed=host, age_h=0.2 + n / 60))
            pages[url] = _page(f"Crude note {n} from feed {f}", TODAY)
        feeds.append((host, items))
    run = _drive(monkeypatch, feeds=feeds, pages=pages)
    assert len(run.fetched) == pipeline.DATE_CHECK_CAP
    assert run.stats.spot == {"ok": pipeline.DATE_CHECK_CAP, "unchecked": 40 - pipeline.DATE_CHECK_CAP}
    per_feed = {}
    for url in run.fetched:
        host = url.split("/")[2]
        per_feed[host] = per_feed.get(host, 0) + 1
    assert min(per_feed.values()) >= 3 and len(run.persisted) == 40


def _plans(spec):
    """{feed: n chosen items} -> _FeedPlan objects for _fetch_order."""
    plans = {}
    for fd, n in spec.items():
        chosen = [(_item(f"{fd}-{i}", f"t {i}", url=f"https://{fd}/{i}", feed=fd), None) for i in range(n)]
        plans[fd] = pipeline._FeedPlan(fd=fd, items=list(chosen), chosen=list(chosen))
    return plans


def test_flagged_feeds_are_checked_first_and_the_rest_rotates():
    spec = {"a.example": 4, "b.example": 4, "c.example": 4, "z-flagged.example": 3}
    got = pipeline._fetch_order(_plans(spec), {"z-flagged.example"}, bucket=0, cap=5)
    assert [a.url for a, _o in got[:3]] == [f"https://z-flagged.example/{i}" for i in range(3)]
    # The two places left go to a different feed as the 5-minute bucket turns.
    firsts = set()
    for bucket in range(3):
        order = pipeline._fetch_order(_plans(spec), {"z-flagged.example"}, bucket=bucket, cap=5)
        firsts.add(order[3][0].url.split("/")[2])
    assert firsts == {"a.example", "b.example", "c.example"}


def test_the_cut_goes_back_to_the_over_budget_list():
    plans = _plans({"a.example": 4, "b.example": 4})
    got = pipeline._fetch_order(plans, set(), bucket=0, cap=6)
    assert len(got) == 6
    assert sum(len(p.chosen) for p in plans.values()) == 6
    assert sum(len(p.unchosen) for p in plans.values()) == 2


def test_pages_read_earlier_in_the_scan_are_judged_for_free(monkeypatch):
    """Five near-misses the lede rescue read + four plain items: the spot budget
    (4) goes to the four; the five are judged without a fetch -- none unchecked."""
    body = ("Crude tanker flows through Hormuz rose again this week as buyers raced to secure "
            "cargoes ahead of the winter restocking season.")
    near = [_item(f"weekly-wrap-{n}", f"Weekly wrap number {n}", summary="Short teaser.", age_h=0.3 + n / 60)
            for n in range(5)]
    plain = [_item(f"hormuz-plain-{n}", f"Hormuz plain crude note {n}", age_h=0.2 + n / 60) for n in range(4)]
    pages = {**PAGES,
             **{BLOG + f"weekly-wrap-{n}": _page(f"Weekly wrap number {n}", TODAY, body=body) for n in range(5)},
             **{BLOG + f"hormuz-plain-{n}": _page(f"Hormuz plain crude note {n}", TODAY) for n in range(4)}}
    run = _drive(monkeypatch, [*near, *plain], pages=pages)
    assert run.stats.reused == 5 and run.stats.fetched == 4
    assert run.stats.spot == {"ok": 9} and len(run.persisted) == 9


def test_a_page_the_lede_rescue_read_still_drops_an_old_post(monkeypatch):
    """R2 now happens once, in Stage 4b: the lede rescue's page proves the
    near-miss is an old post, and it is dropped without a second fetch."""
    body = ("Crude tanker flows through Hormuz rose again this week as buyers raced to secure "
            "cargoes ahead of the winter restocking season.")
    old = _item("weekly-wrap-old", "Weekly wrap from spring", summary="Short teaser.", age_h=0.3)
    pages = {**PAGES, BLOG + "weekly-wrap-old": _page("Weekly wrap from spring", "Apr 01, 2026", body=body)}
    run = _drive(monkeypatch, [old], pages=pages)
    assert run.persisted == [] and _outcome(run, BLOG + "weekly-wrap-old") == "older_page"
    assert run.fetched.count(BLOG + "weekly-wrap-old") == 1


def test_a_page_enrich_already_read_is_not_fetched_again(monkeypatch):
    """A near-miss the lede rescue fetched (and saved) reaches the date check,
    which reuses that page instead of downloading it twice."""
    nearmiss = _item("weekly-market-wrap", "Weekly market wrap", summary="Short teaser.", age_h=0.3)
    pages = {**PAGES, BLOG + "weekly-market-wrap": _page(
        "Weekly market wrap", TODAY, body="Crude tanker flows through Hormuz rose again this week "
        "as buyers raced to secure cargoes ahead of the winter restocking season.")}
    run = _drive(monkeypatch, [nearmiss], pages=pages)
    assert BLOG + "weekly-market-wrap" in _by_url(run)
    assert run.fetched.count(BLOG + "weekly-market-wrap") == 1
    assert run.stats.reused == 1 and run.stats.fetched == 0


def test_the_snippet_backfill_does_not_refetch_a_page_the_date_check_read(monkeypatch):
    bare = _item("lng-queue-update", "LNG queue update", summary="", age_h=0.3)
    pages = {**PAGES, BLOG + "lng-queue-update": _page(
        "LNG queue update", TODAY, body="LNG carriers queued off the Strait of Hormuz for a third "
        "day as insurers repriced war risk on every transit.")}
    run = _drive(monkeypatch, [bare], pages=pages)
    got = _by_url(run)[BLOG + "lng-queue-update"]
    assert run.fetched.count(BLOG + "lng-queue-update") == 1
    assert got.snippet.startswith("LNG carriers queued")          # from the date check's fetch


def test_a_page_read_without_a_snippet_is_not_fetched_again_by_the_backfill(monkeypatch):
    """The date check read the page and found no text to show: the backfill,
    which would fetch the very same page, leaves it alone this scan."""
    bare = _item("lng-chart-of-the-day", "LNG chart of the day", summary="", age_h=0.3)
    empty_page = ('<html><head><script type="application/ld+json">{"@type": "BlogPosting", '
                  f'"datePublished": "{TODAY}"}}</script></head><body><h1>LNG chart of the day</h1>'
                  '<img src="chart.png"/></body></html>')
    run = _drive(monkeypatch, [bare], pages={**PAGES, BLOG + "lng-chart-of-the-day": empty_page})
    assert BLOG + "lng-chart-of-the-day" in _by_url(run)
    assert run.fetched.count(BLOG + "lng-chart-of-the-day") == 1


def test_a_page_older_but_still_inside_the_window_keeps_its_snippet_and_headline(monkeypatch):
    """Re-dated by the spot check yet inside a 72 h window: persisted with the
    page's date, and with the text and <h1> of the page the check already read
    (the backfill skips a page this scan has read)."""
    h1 = "Hormuz queues ease as insurers cut war risk premiums"
    page_date = NOW - timedelta(hours=30)
    late = _item("hormuz-queues-ease", f"Tanker queues shrink {h1} | Kpler -", summary="", age_h=1)
    pages = {**PAGES, BLOG + "hormuz-queues-ease": _page(
        h1, page_date.isoformat(), body="War risk premiums for Hormuz transits fell for a second "
        "week as tanker queues off Fujairah shrank to their lowest since June.")}
    run = _drive(monkeypatch, [late], pages=pages, hours=72)
    got = _by_url(run)[BLOG + "hormuz-queues-ease"]
    assert _outcome(run, BLOG + "hormuz-queues-ease") == "older_page"
    assert abs((got.published_at - page_date).total_seconds()) < 1
    assert got.snippet.startswith("War risk premiums")
    assert got.title == h1
    assert run.fetched.count(BLOG + "hormuz-queues-ease") == 1


def test_a_date_check_that_finished_late_still_hands_its_snippet_to_the_backfill(monkeypatch):
    """Stage 4b stopped waiting (its deadline), the fetch finished afterwards and
    recorded the page with its snippet: the backfill takes it, no second fetch."""
    from bs4 import BeautifulSoup

    from news_hunter.date_credibility import record_page
    from news_hunter.store import Article

    url = BLOG + "late-date-check"
    ev = record_page(url, BeautifulSoup(_page("Late date check", TODAY), "lxml"))
    ev.snippet = "LNG carriers queued off the Strait of Hormuz for a third day."
    a = Article(url=url, domain="kpler.com", source_name="Kpler", title="Late date check",
                snippet="", published_at=NOW, found_at=NOW)
    fetched: list[str] = []
    monkeypatch.setattr(pipeline, "urls_with_snippet", lambda urls: set())
    monkeypatch.setattr(enrich, "fetch_html", lambda u, timeout=6: fetched.append(u) or "")
    pipeline._run_snippet_backfill([a], [])
    assert a.snippet == ev.snippet
    assert fetched == []


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------

def test_the_burst_shape_end_to_end(monkeypatch):
    """Stored posts re-dated in a batch + old + dateless + a 403 + genuinely new."""
    run = _drive(monkeypatch, [*BATCH, OLD, DATELESS, BLOCKED, NEW], stored=BATCH_ROWS)
    urls = set(_by_url(run))
    assert urls == {*(BLOG + s for s in BATCH_SLUGS), BLOG + NEW_SLUG}
    assert run.stats.verified == 1 and run.stats.n_page_older == 1
    assert run.stats.deferred == {FEED: {"no_page_date": 1, "unread": 1}}


def test_google_news_items_are_exempt(monkeypatch):
    gnews = [_item(s, BATCH[i].title, feed="news.google.com", age_h=1.2 + i / 100)
             for i, s in enumerate(BATCH_SLUGS)]
    gnews_old = _item(OLD_SLUG, "How to build a risk tree", feed="news.google.com")
    run = _drive(monkeypatch, [*gnews, gnews_old], stored=BATCH_ROWS, feed="news.google.com")
    assert run.stats.flagged() == [] and run.lookups == [] and run.fetched == []
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
    assert not ({BLOG + s for s in BATCH_SLUGS} & set(_by_url(run)))
    assert run.stats.flagged() == [FEED]
    assert _outcome(run, BLOG + DATELESS_SLUG) == "deferred_no_page_date"


def test_title_evidence_counts_only_items_the_feed_presents_as_fresh(monkeypatch):
    stale = [_item(f"old-note-{n}", f"Old note on Hormuz {n} | Kpler - Jun 30, 2026", age_h=72 + n / 60)
             for n in range(3)]
    run = _drive(monkeypatch, [*stale, DATELESS])
    assert run.stats.flagged() == []
    assert BLOG + DATELESS_SLUG in _by_url(run)


def test_date_credibility_line_is_logged_with_zeros(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    _drive(monkeypatch, [])
    assert _line(caplog, "date credibility:").startswith(
        "date credibility: page_older=0 [] (title_date=0) template_date=[] restamp_domains=[]"
        " isolated=[] cautious=[] verified=0 spot=[] unverified_admitted=0 [] deferred=0 []"
        " pages=0+0reused looked_up=0 lookup=ok error=0 [] in "
    )
    assert _line(caplog, "own-name keywords:") == (
        "own-name keywords: 0 items matched only their source's own name"
    )


def test_approximate_dates_are_never_candidates(monkeypatch):
    """A clamped future date is fabricated (published_is_approx): the date check
    leaves it to _freeze_approx_dates, which must keep working exactly as before."""
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


def test_an_own_name_near_miss_the_lede_saves_is_not_counted(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    # A thin description, so the lede rescue reads the body (a 150+ char RSS
    # description would be the lede itself).
    teaser = _item(MARKETING_SLUG, MARKETING.title, summary="Short teaser from Kpler.")
    pages = {BLOG + MARKETING_SLUG: _page(
        "How to choose the right ship tracking software", TODAY,
        body="Kpler tracks every crude tanker leaving the Gulf, Hormuz included, in real time.")}
    run = _drive(monkeypatch, [teaser], pages=pages)
    got = _by_url(run).get(BLOG + MARKETING_SLUG)
    assert got is not None and "Kpler" not in got.matched_keywords
    assert _line(caplog, "own-name keywords:").startswith("own-name keywords: 0 items")


def test_a_lede_that_only_says_kpler_is_counted(monkeypatch, caplog):
    """No keyword in title or summary at all, and the body only names Kpler."""
    caplog.set_level(logging.INFO, logger="news_hunter.pipeline")
    plain = _item("product-update-september", "Product update for September", summary="Short teaser.")
    pages = {BLOG + "product-update-september": _page(
        "Product update for September", TODAY, body="Kpler customers can now export vessel lists.")}
    run = _drive(monkeypatch, [plain], pages=pages)
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
