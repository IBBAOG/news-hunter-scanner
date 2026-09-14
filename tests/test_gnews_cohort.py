"""The Google News burst budget: cohort rotation over the `site:` query lists.

news.google.com silently drops the tail of a burst of `site:` queries from one
IP. Today's scan submits 65 (15 foreign + 16 PT + 34 EN); the international
programme pushes the EN list toward ~120, which would double the burst and start
losing whatever sits at the END of the list — silently, and position-dependently,
so the casualties would be chosen by list order rather than by merit.

The fix is a stateless rotation: partition the EN list into
``ceil(n / EN_GNEWS_QUERIES_PER_SCAN)`` contiguous blocks and query one per scan,
picked from a 5-minute time bucket (the scan cadence). What has to be true:

  * the blocks PARTITION the list — every domain in exactly one, so `cohorts`
    consecutive scans cover the roster and no scan asks for a domain twice;
  * no scan exceeds the budget;
  * order is preserved, inside a cohort and across them;
  * with today's 34 domains ``cohorts == 1`` and the emitted query list is the
    uncapped one, byte for byte (tests/test_multilingual_en_frozen.py is the
    other half of that guarantee and is deliberately untouched);
  * the foreign languages are NOT capped — they are GNews-only, so a dropped
    query there is data loss, not delay.

Run from repo root: python -m pytest tests/test_gnews_cohort.py -v
"""
from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import fetcher  # noqa: E402
from news_hunter.sources import (  # noqa: E402
    EN_GNEWS_QUERIES_PER_SCAN,
    ENGLISH_NO_RSS_DOMAINS,
    GNEWS_COHORT_BUCKET_SECONDS,
    LANGUAGES,
    NO_RSS_DOMAINS,
    PT_GNEWS_QUERIES_PER_SCAN,
    gnews_cohort,
    gnews_cohort_count,
    gnews_cohort_index,
    gnews_cohort_slice,
    google_news_site_queries,
    google_news_site_queries_en,
)

_KWS = [
    "oil", "gas", "diesel", "Brent", "WTI", "OPEC",
    "crude", "LNG", "refinery", "gasoline", "Petrobras", "sanction",
    "combustível", "Raízen",
]


def _domains(n: int) -> list[str]:
    return [f"d{i:03d}.example" for i in range(n)]


# --------------------------------------------------------------------------
# The partition
# --------------------------------------------------------------------------

def test_consecutive_buckets_cover_every_domain_exactly_once():
    """`cohorts` consecutive scans query the whole roster, each domain once.

    This is the load-bearing property: it is what makes a capped scan a DELAY
    (bounded by cohorts x 5 min) instead of a loss. Checked over a list far past
    the budget (120 domains -> 4 cohorts), from an arbitrary starting bucket.
    """
    domains = _domains(120)
    cohorts = gnews_cohort_count(len(domains), EN_GNEWS_QUERIES_PER_SCAN)
    assert cohorts == 4

    start_bucket = 123_456  # arbitrary; the property must not depend on phase
    seen: list[str] = []
    for step in range(cohorts):
        now = (start_bucket + step) * GNEWS_COHORT_BUCKET_SECONDS
        subset, index, total = gnews_cohort(
            domains, EN_GNEWS_QUERIES_PER_SCAN, now=now
        )
        assert total == cohorts
        assert index == (start_bucket + step) % cohorts
        assert len(subset) == len(set(subset)), "a domain twice in one scan"
        seen.extend(subset)

    assert sorted(seen) == sorted(domains)          # covered
    assert len(seen) == len(set(seen))              # exactly once


def test_no_cohort_exceeds_the_per_scan_budget():
    """Across many roster sizes, every cohort fits the budget and none is empty."""
    for n in list(range(1, 60)) + [100, 120, 121, 200]:
        domains = _domains(n)
        cohorts = gnews_cohort_count(n, EN_GNEWS_QUERIES_PER_SCAN)
        sizes = [
            len(gnews_cohort_slice(domains, EN_GNEWS_QUERIES_PER_SCAN, i))
            for i in range(cohorts)
        ]
        assert sum(sizes) == n, (n, sizes)
        assert max(sizes) <= EN_GNEWS_QUERIES_PER_SCAN, (n, sizes)
        assert min(sizes) >= 1, (n, sizes)
        assert max(sizes) - min(sizes) <= 1, (n, sizes)  # balanced


def test_cohorts_preserve_list_order_and_concatenate_to_the_original():
    """Order inside a cohort is the registry order, and cohort 0..N-1 rebuilds it.

    Order matters because a wave appends to the END of ENGLISH_NO_RSS_DOMAINS and
    the per-domain comment next to each entry is the audit trail: a reshuffled
    rotation would make "which cohort is my source in" unanswerable by reading
    sources.py.
    """
    domains = _domains(97)
    cohorts = gnews_cohort_count(len(domains), EN_GNEWS_QUERIES_PER_SCAN)
    rebuilt: list[str] = []
    for i in range(cohorts):
        subset = gnews_cohort_slice(domains, EN_GNEWS_QUERIES_PER_SCAN, i)
        assert subset == sorted(subset, key=domains.index)  # order preserved
        rebuilt.extend(subset)
    assert rebuilt == domains


def test_bucket_index_walks_forward_and_wraps():
    """Consecutive 5-minute buckets select consecutive cohorts, modulo N."""
    cohorts = 4
    indices = [
        gnews_cohort_index(cohorts, now=b * GNEWS_COHORT_BUCKET_SECONDS)
        for b in range(1000, 1009)
    ]
    assert indices == [(1000 + k) % 4 for k in range(9)]
    # A scan landing anywhere inside a bucket picks the same cohort.
    for offset in (0, 1, 149, GNEWS_COHORT_BUCKET_SECONDS - 1):
        assert gnews_cohort_index(
            cohorts, now=1000 * GNEWS_COHORT_BUCKET_SECONDS + offset
        ) == 1000 % 4


# --------------------------------------------------------------------------
# Today: cohorts == 1, nothing changes
# --------------------------------------------------------------------------

def test_single_cohort_reproduces_the_full_list_unchanged():
    """At or below the budget there is ONE cohort and it is the whole list."""
    for n in range(1, EN_GNEWS_QUERIES_PER_SCAN + 1):
        domains = _domains(n)
        assert gnews_cohort_count(n, EN_GNEWS_QUERIES_PER_SCAN) == 1
        subset, index, cohorts = gnews_cohort(domains, EN_GNEWS_QUERIES_PER_SCAN)
        assert (subset, index, cohorts) == (domains, 0, 1)


def test_live_english_roster_is_one_cohort_and_its_queries_are_unchanged():
    """The EN query list the scanner emits TODAY is byte-identical to the
    uncapped one — at any clock time, so landing the mechanism changed nothing."""
    domains = list(ENGLISH_NO_RSS_DOMAINS)
    assert len(domains) <= EN_GNEWS_QUERIES_PER_SCAN, (
        "the EN roster outgrew the budget: cohorts>1 is now live — expected, but "
        "the scan log must show 'gnews en cohort k/N' and coverage is now "
        "N x 5 min"
    )
    legacy = google_news_site_queries_en(domains, _KWS, 24)
    for now in (0, 1_000_000, 1_757_000_000, 2_000_000_123):
        subset, _index, cohorts = gnews_cohort(
            domains, EN_GNEWS_QUERIES_PER_SCAN, now=now
        )
        assert cohorts == 1
        assert subset == domains
        assert google_news_site_queries_en(subset, _KWS, 24) == legacy


def test_live_pt_roster_is_one_cohort_and_its_queries_are_unchanged():
    domains = list(NO_RSS_DOMAINS)
    legacy = google_news_site_queries(domains, _KWS, 24)
    subset, _index, cohorts = gnews_cohort(domains, PT_GNEWS_QUERIES_PER_SCAN)
    assert cohorts == 1
    assert google_news_site_queries(subset, _KWS, 24) == legacy


# --------------------------------------------------------------------------
# The scan's task list
# --------------------------------------------------------------------------

def test_gnews_tasks_caps_english_but_never_the_foreign_languages(monkeypatch):
    """With an EN roster 3x over budget, the scan submits one EN cohort — and
    still every foreign query, in the priority block.

    The asymmetry is the whole point: EN has RSS fallbacks and redundant sources,
    so a delayed query costs nothing; ar/ru/zh/iw/es are GNews-ONLY, so a dropped
    query is that language's entire scan.
    """
    import dataclasses

    big_en = _domains(120)  # ceil(120/34) = 4 cohorts of exactly 30
    patched = dict(fetcher.LANGUAGES)
    patched["en"] = dataclasses.replace(
        fetcher.LANGUAGES["en"], no_rss_domains=tuple(big_en)
    )
    monkeypatch.setattr(fetcher, "LANGUAGES", patched)

    priority, tasks, lang_by_url = fetcher._gnews_tasks(_KWS, 24)

    en_urls = [u for _d, u in tasks if lang_by_url.get(u) == "en"]
    assert len(en_urls) == 30
    assert len(en_urls) <= EN_GNEWS_QUERIES_PER_SCAN

    foreign_expected = sum(
        len(c.no_rss_domains) for c in LANGUAGES.values() if c.translate
    )
    assert len(priority) == foreign_expected
    assert all(d == "news.google.com" for d, _u in priority)
    foreign_codes = {c.code for c in LANGUAGES.values() if c.translate}
    tagged = {lang_by_url[u] for _d, u in priority}
    assert tagged == foreign_codes


def test_gnews_tasks_today_is_the_full_uncapped_burst():
    """Nothing is withheld at today's roster sizes: 15 foreign + 16 PT + 34 EN."""
    priority, tasks, lang_by_url = fetcher._gnews_tasks(_KWS, 24)
    assert len(priority) == 15
    assert len(tasks) == len(NO_RSS_DOMAINS) + len(ENGLISH_NO_RSS_DOMAINS)

    en_urls = [u for _d, u in tasks if lang_by_url.get(u) == "en"]
    assert en_urls == google_news_site_queries_en(list(ENGLISH_NO_RSS_DOMAINS), _KWS, 24)
    pt_urls = [u for _d, u in tasks if u not in lang_by_url]
    assert pt_urls == google_news_site_queries(list(NO_RSS_DOMAINS), _KWS, 24)


def test_scan_logs_one_cohort_line(caplog):
    """The rotation must be visible in the run log, or a silently shrinking burst
    is exactly as undiagnosable as the dropped queries it replaces."""
    with caplog.at_level(logging.INFO, logger="news_hunter.fetcher"):
        fetcher._gnews_tasks(_KWS, 24)
    lines = [r.getMessage() for r in caplog.records if "cohort" in r.getMessage()]
    assert lines == [f"gnews en cohort 1/1 ({len(ENGLISH_NO_RSS_DOMAINS)} domains)"]
