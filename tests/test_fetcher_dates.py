"""The fetcher never hands the pipeline a date that raises when used.

dateutil builds a UTC offset of 24 h or more ("+9999", "+99:00") without
complaint; the datetime raises only when first USED -- in within_window, in
the middle of the collect loop, taking every source of the scan down. Found by
QA on the date-credibility branch (round 3); the four parse sites of the
fetcher were exposed on main already.

Run from repo root: python -m pytest tests/test_fetcher_dates.py -v
"""
from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import fetcher  # noqa: E402
from news_hunter.filter import within_window  # noqa: E402


def test_a_ptbr_pubdate_with_an_impossible_offset_is_no_date():
    assert fetcher._parse_ptbr_date("Ter, 26 Mai 2026 09:50:56 +9999") is None
    ok = fetcher._parse_ptbr_date("Ter, 26 Mai 2026 09:50:56 -0300")
    assert ok == datetime(2026, 5, 26, 12, 50, 56, tzinfo=timezone.utc)


def test_an_entry_with_that_pubdate_reaches_the_window_check_safely():
    got = fetcher._parse_entry_date({"published": "Ter, 26 Mai 2026 09:50:56 +9999"})
    assert got is None
    assert within_window(got, 24) is False           # None: never raises


@pytest.mark.parametrize("raw", ["2026-09-25T10:00:00+99:00", "2026-09-25T10:00:00+2400"])
def test_a_sitemap_lastmod_with_an_impossible_offset_is_no_date(raw):
    el = ET.fromstring(f"<lastmod>{raw}</lastmod>")
    assert fetcher._parse_lastmod(el) is None
    good = ET.fromstring("<lastmod>2026-09-25T10:00:00-03:00</lastmod>")
    assert fetcher._parse_lastmod(good) == datetime(2026, 9, 25, 13, 0, tzinfo=timezone.utc)


def test_a_listing_time_element_with_an_impossible_offset_is_no_date():
    html = ('<div><h2><a href="https://example.com/news/a">Crude tanker rates rise</a></h2>'
            '<time datetime="2026-09-25T10:00:00+99:00">25/09</time></div>')
    anchor = BeautifulSoup(html, "lxml").find("a")
    _title, published = fetcher._listing_hints(anchor)
    assert published is None


def test_usable_offset_keeps_good_dates_and_naive_ones():
    naive = datetime(2026, 9, 25, 10, 0)
    aware = datetime(2026, 9, 25, 10, 0, tzinfo=timezone.utc)
    assert fetcher._usable_offset(naive) is naive and fetcher._usable_offset(aware) is aware
    assert fetcher._usable_offset(None) is None
