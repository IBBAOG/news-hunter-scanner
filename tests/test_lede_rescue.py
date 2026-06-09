"""Tests for the lede-rescue near-miss routing (added 2026-06-09).

Background: the cloud scanner runs in fast_mode, which matches keywords only
against the RSS title + summary and never fetches the article body. Editorial
sources (e.g. eixos "Comece seu dia") ship clever titles with no keyword and a
short description (< SNIPPET_MIN_RSS_CHARS) whose text also lacks the keyword,
even when the body lede is squarely about petroleo/gas. Such articles were
dropped silently.

These tests cover only the cheap, deterministic routing decision in
`_keep_candidate` (no network). The full `_run_lede_rescue` path is exercised
manually against live pages — it requires fetch_html and is not unit-tested
here to keep the suite offline.

Run from repo root: python -m pytest tests/test_lede_rescue.py -v
Or with stdlib only: python tests/test_lede_rescue.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import RawItem
from news_hunter.pipeline import LEDE_RESCUE_MARKER, _keep_candidate

_NOW = datetime.now(timezone.utc)
_OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)
_KWS = ["petróleo", "gás", "Petrobras"]


def _item(title="", summary="", published=_NOW, url="https://eixos.com.br/a/x/"):
    return RawItem(
        url=url,
        title=title,
        summary=summary,
        published_at=published,
        source_domain="eixos.com.br",
        feed_domain="eixos.com.br",
    )


def test_near_miss_returns_sentinel_when_rescue_allowed():
    # Editorial title + short summary, neither carries a keyword.
    it = _item(
        title="Riscos novos, infraestrutura velha",
        summary="Ásia está ampliando geração termelétrica a carvão",
    )
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) == [
        LEDE_RESCUE_MARKER
    ]


def test_near_miss_discarded_without_rescue_flag_backcompat():
    # Default (flag off) must behave exactly as before: discard near-miss.
    it = _item(
        title="Riscos novos, infraestrutura velha",
        summary="Ásia está ampliando geração termelétrica a carvão",
    )
    assert _keep_candidate(it, _KWS, 24, set()) is None


def test_title_match_short_circuits_no_rescue():
    it = _item(title="Petrobras abre edital", summary="")
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) == [
        "Petrobras"
    ]


def test_summary_match_short_circuits_no_rescue():
    it = _item(title="Título sem termo", summary="negociação sobre gás natural")
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) == ["gás"]


def test_no_published_date_not_rescued():
    # Items without a date (homepage scrape / standard sitemap) take the
    # pre-existing slug/path branch and never become lede candidates.
    it = _item(title="Título sem data", summary="", published=None)
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) is None


def test_out_of_window_not_rescued():
    it = _item(title="Riscos novos", summary="", published=_OLD)
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) is None


if __name__ == "__main__":
    test_near_miss_returns_sentinel_when_rescue_allowed()
    test_near_miss_discarded_without_rescue_flag_backcompat()
    test_title_match_short_circuits_no_rescue()
    test_summary_match_short_circuits_no_rescue()
    test_no_published_date_not_rescued()
    test_out_of_window_not_rescued()
    print("all lede-rescue tests passed")
