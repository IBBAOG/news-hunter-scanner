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
import re
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


def _queries_over_a_full_rotation(domains, per_scan, builder, start_bucket=0):
    """Every query `cohorts` consecutive scans emit, in scan order."""
    cohorts = gnews_cohort_count(len(domains), per_scan)
    out = []
    for step in range(cohorts):
        subset, _i, _n = gnews_cohort(
            domains, per_scan, now=(start_bucket + step) * GNEWS_COHORT_BUCKET_SECONDS
        )
        out.extend(builder(subset, _KWS, 24))
    return out


def test_a_full_rotation_emits_exactly_the_uncapped_english_query_list():
    """The queries a full rotation sends are the uncapped list, in order.

    Deliberately NOT written as "cohorts == 1": that would be a tripwire that
    goes red the moment a wave grows the roster past the budget, which is when
    the mechanism starts WORKING. Stated this way the invariant holds on both
    sides of the line — today it collapses to "one scan emits the whole list,
    byte for byte", and after a wave it reads "N scans do".
    """
    domains = list(ENGLISH_NO_RSS_DOMAINS)
    legacy = google_news_site_queries_en(domains, _KWS, 24)

    # From bucket 0 the cohorts come out in index order, so the concatenation is
    # the uncapped list verbatim.
    assert _queries_over_a_full_rotation(
        domains, EN_GNEWS_QUERIES_PER_SCAN, google_news_site_queries_en
    ) == legacy
    # From any other phase the rotation starts mid-list, so the ORDER rotates
    # while the SET emitted per full rotation does not: still every query, still
    # exactly once. (A scan does not care which cohort it gets, only that the
    # roster is covered.)
    for start in (1, 7, 123_457):
        rotated = _queries_over_a_full_rotation(
            domains, EN_GNEWS_QUERIES_PER_SCAN, google_news_site_queries_en, start
        )
        assert sorted(rotated) == sorted(legacy)
        assert len(rotated) == len(set(rotated)) == len(legacy)

    if gnews_cohort_count(len(domains), EN_GNEWS_QUERIES_PER_SCAN) == 1:
        # Today. Any clock time must give the same, complete list.
        for now in (0, 1_000_000, 1_757_000_000, 2_000_000_123):
            subset, _i, cohorts = gnews_cohort(
                domains, EN_GNEWS_QUERIES_PER_SCAN, now=now
            )
            assert (subset, cohorts) == (domains, 1)
            assert google_news_site_queries_en(subset, _KWS, 24) == legacy


def test_a_full_rotation_emits_exactly_the_uncapped_pt_query_list():
    domains = list(NO_RSS_DOMAINS)
    assert _queries_over_a_full_rotation(
        domains, PT_GNEWS_QUERIES_PER_SCAN, google_news_site_queries
    ) == google_news_site_queries(domains, _KWS, 24)


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


def test_gnews_tasks_emits_this_scan_cohort_of_each_live_roster():
    """What ONE live scan submits: every foreign query, plus this scan's PT and
    EN cohorts — and each cohort's queries are the legacy builder's output for
    exactly those domains (no query is reshaped by the capping)."""
    priority, tasks, lang_by_url = fetcher._gnews_tasks(_KWS, 24)

    assert len(priority) == sum(
        len(c.no_rss_domains) for c in LANGUAGES.values() if c.translate
    )

    en_cohort, _i, _n = gnews_cohort(
        list(ENGLISH_NO_RSS_DOMAINS), EN_GNEWS_QUERIES_PER_SCAN
    )
    pt_cohort, _i, _n = gnews_cohort(list(NO_RSS_DOMAINS), PT_GNEWS_QUERIES_PER_SCAN)
    assert len(tasks) == len(en_cohort) + len(pt_cohort)

    en_urls = [u for _d, u in tasks if lang_by_url.get(u) == "en"]
    assert en_urls == google_news_site_queries_en(en_cohort, _KWS, 24)
    pt_urls = [u for _d, u in tasks if u not in lang_by_url]
    assert pt_urls == google_news_site_queries(pt_cohort, _KWS, 24)


def test_scan_logs_one_cohort_line(caplog):
    """The rotation must be visible in the run log, or a silently shrinking burst
    is exactly as undiagnosable as the dropped queries it replaces."""
    with caplog.at_level(logging.INFO, logger="news_hunter.fetcher"):
        _priority, tasks, lang_by_url = fetcher._gnews_tasks(_KWS, 24)
    lines = [r.getMessage() for r in caplog.records if "cohort" in r.getMessage()]
    en_lines = [ln for ln in lines if ln.startswith("gnews en cohort ")]
    assert len(en_lines) == 1, lines

    cohorts = gnews_cohort_count(
        len(ENGLISH_NO_RSS_DOMAINS), EN_GNEWS_QUERIES_PER_SCAN
    )
    n_en = len([u for _d, u in tasks if lang_by_url.get(u) == "en"])
    # The line must describe the burst that was actually submitted, not a
    # plausible-looking constant.
    assert re.fullmatch(
        rf"gnews en cohort ([1-9]\d*)/{cohorts} \({n_en} domains\)", en_lines[0]
    ), en_lines[0]
