"""Guard Stage 3d — the body fetch for articles that matched on the TITLE.

The lede rescue only ever ran on near-misses (title + summary did not match),
so an item that DID match on its title was persisted with an empty snippet and
nobody ever went back for it. Sources covered only by Google News — Brasil 247,
Agencia iNFRA, Reuters — arrive with no description at all, so that was their
permanent state.

Fetches are mocked here: this pins the SELECTION and the fail-soft contract, the
two things that decide whether the budget is spent on the right articles. Real
extraction quality is covered by the per-domain extractor tests.
"""
from datetime import datetime, timedelta, timezone

import pytest

from news_hunter import pipeline
from news_hunter.store import Article

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def _article(url: str, domain: str = "exemplo.com", minutes_old: int = 0, snippet: str = "") -> Article:
    return Article(
        url=url,
        domain=domain,
        source_name=domain,
        title=f"Titulo de {url}",
        snippet=snippet,
        published_at=NOW - timedelta(minutes=minutes_old),
        found_at=NOW,
        matched_keywords=["petroleo"],
    )


@pytest.fixture
def fetched(monkeypatch):
    """Records which URLs were fetched; returns a body for each."""
    calls: list[str] = []

    def fake_enrich(item, *, resolve_google_news=False, need_snippet=True):
        calls.append(item.url)
        return (f"corpo de {item.url}", item.published_at, item.url, item.source_domain, "")

    monkeypatch.setattr(pipeline, "enrich_item", fake_enrich)
    monkeypatch.setattr(pipeline, "urls_with_snippet", lambda urls: set())
    return calls


def test_fills_an_empty_snippet(fetched):
    arts = [_article("https://a/1")]
    assert pipeline._run_snippet_backfill(arts, []) == 1
    assert arts[0].snippet == "corpo de https://a/1"
    assert fetched == ["https://a/1"]


def test_leaves_an_article_that_already_has_a_snippet_alone(fetched):
    arts = [_article("https://a/1", snippet="ja tenho texto")]
    assert pipeline._run_snippet_backfill(arts, []) == 0
    assert arts[0].snippet == "ja tenho texto"
    assert fetched == []


def test_newest_first_wins_the_cap(monkeypatch, fetched):
    """A brand-new article is the only one nobody can have enriched before, so
    it must beat a backlog of older bodyless ones for the scarce budget."""
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 2)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 99)
    arts = [
        _article("https://old/1", domain="d1", minutes_old=900),
        _article("https://new/1", domain="d2", minutes_old=1),
        _article("https://old/2", domain="d3", minutes_old=800),
        _article("https://new/2", domain="d4", minutes_old=5),
    ]
    pipeline._run_snippet_backfill(arts, [])
    assert sorted(fetched) == ["https://new/1", "https://new/2"]


def test_skips_urls_the_database_already_has_a_snippet_for(monkeypatch, fetched):
    """Without this the stateless scanner re-downloads the same articles every
    five minutes, forever, and the budget never reaches a new one."""
    monkeypatch.setattr(
        pipeline, "urls_with_snippet", lambda urls: {"https://a/1", "https://a/2"}
    )
    arts = [_article("https://a/1"), _article("https://a/2"), _article("https://a/3")]
    assert pipeline._run_snippet_backfill(arts, []) == 1
    assert fetched == ["https://a/3"]


def test_per_domain_cap_spreads_the_budget(monkeypatch, fetched):
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 10)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 2)
    arts = [_article(f"https://x/{i}", domain="hog.com", minutes_old=i) for i in range(6)]
    arts += [_article("https://y/1", domain="outro.com", minutes_old=99)]
    pipeline._run_snippet_backfill(arts, [])
    assert sum(1 for u in fetched if u.startswith("https://x/")) == 2
    assert "https://y/1" in fetched


def test_lookup_failure_narrows_the_phase_instead_of_guessing(monkeypatch, fetched):
    """A mute database means we cannot tell done from new. Spending the whole
    cap blind would mostly re-download finished work, so serve only the few
    newest — the ones almost certainly new."""
    monkeypatch.setattr(pipeline, "urls_with_snippet", lambda urls: None)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 25)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 2)
    arts = [_article(f"https://a/{i}", domain=f"d{i}.com", minutes_old=i) for i in range(20)]
    pipeline._run_snippet_backfill(arts, [])
    assert len(fetched) == 2


def test_one_broken_fetch_does_not_sink_the_phase(monkeypatch):
    def flaky(item, *, resolve_google_news=False, need_snippet=True):
        if item.url.endswith("/boom"):
            raise RuntimeError("403")
        return ("corpo", item.published_at, item.url, item.source_domain, "")

    monkeypatch.setattr(pipeline, "enrich_item", flaky)
    monkeypatch.setattr(pipeline, "urls_with_snippet", lambda urls: set())
    arts = [_article("https://a/boom", domain="d1"), _article("https://a/ok", domain="d2")]
    errors: list[str] = []
    assert pipeline._run_snippet_backfill(arts, errors) == 1
    assert arts[0].snippet == ""      # unchanged, exactly as before the phase
    assert arts[1].snippet == "corpo"
    assert any("boom" in e for e in errors)


def test_an_empty_body_is_not_written_as_a_snippet(monkeypatch):
    monkeypatch.setattr(
        pipeline, "enrich_item",
        lambda item, **kw: ("   ", item.published_at, item.url, item.source_domain, ""),
    )
    monkeypatch.setattr(pipeline, "urls_with_snippet", lambda urls: set())
    arts = [_article("https://a/1")]
    assert pipeline._run_snippet_backfill(arts, []) == 0
    assert arts[0].snippet == ""


def test_article_without_a_date_sorts_last_rather_than_crashing(monkeypatch, fetched):
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 1)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 1)
    dateless = _article("https://a/none", domain="d1")
    dateless.published_at = None
    arts = [dateless, _article("https://a/dated", domain="d2", minutes_old=600)]
    pipeline._run_snippet_backfill(arts, [])
    assert fetched == ["https://a/dated"]
