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


def test_the_newest_article_is_always_served(monkeypatch, fetched):
    """A brand-new article is the only one nobody can have enriched before, so
    it must never queue behind the backlog — even a budget of one serves it."""
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 1)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 99)
    arts = [
        _article("https://old/1", domain="d1", minutes_old=900),
        _article("https://new/1", domain="d2", minutes_old=1),
        _article("https://old/2", domain="d3", minutes_old=800),
    ]
    pipeline._run_snippet_backfill(arts, [])
    assert fetched == ["https://new/1"]


def test_half_the_budget_drains_the_oldest(monkeypatch, fetched):
    """Recency alone never reaches the backlog, and the reason is circular: the
    chronically bodyless articles ARE the old ones, so every new story overtakes
    them forever. Measured 2026-08-20: Brasil 247's ten articles (freshest 14h
    old) sat outside the 75-candidate window while dozens of domains had fresher
    news — and the domain stayed blank even though the fetch demonstrably worked
    from the runner."""
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 4)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 99)
    arts = [_article(f"https://a/{i}", domain=f"d{i}.com", minutes_old=i * 60) for i in range(12)]
    pipeline._run_snippet_backfill(arts, [])
    assert "https://a/0" in fetched    # freshest
    assert "https://a/11" in fetched   # oldest still bodyless


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


# --- budget allocation across domains ---------------------------------------


def test_a_firehose_domain_cannot_starve_a_small_one(monkeypatch, fetched):
    """The shape measured in production on the day this shipped.

    finance.sina.com.cn accounted for 367 of the ~994 bodyless rows in 24h — more
    than a third — and its articles are also the freshest, because it publishes
    constantly. Under pure global recency it swallowed the window and Brasil 247,
    with THREE articles that day, waited behind a foreign firehose.
    """
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 10)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 10)
    arts = [_article(f"https://sina/{i}", domain="finance.sina.com.cn", minutes_old=i) for i in range(60)]
    arts += [_article(f"https://b247/{i}", domain="www.brasil247.com", minutes_old=200 + i) for i in range(3)]
    pipeline._run_snippet_backfill(arts, [])
    assert sum(1 for u in fetched if "b247" in u) == 3
    assert sum(1 for u in fetched if "sina" in u) == 7


def test_the_freshest_domain_still_opens_the_round(monkeypatch, fetched):
    """Round-robin must not cost recency: whoever just published goes first."""
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 2)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 10)
    arts = [
        _article("https://stale/1", domain="velho.com", minutes_old=900),
        _article("https://stale/2", domain="velho.com", minutes_old=910),
        _article("https://fresh/1", domain="novo.com", minutes_old=1),
    ]
    pipeline._run_snippet_backfill(arts, [])
    assert fetched[0] == "https://fresh/1"


def test_order_keeps_every_candidate_exactly_once():
    arts = [_article(f"https://a/{i}", domain=f"d{i % 4}.com", minutes_old=i) for i in range(20)]
    ordered = pipeline._backfill_order(arts)
    assert sorted(a.url for a in ordered) == sorted(a.url for a in arts)
    assert len(ordered) == len(arts)


def test_a_permanently_blocked_domain_burns_one_slot_not_six(monkeypatch, fetched):
    """Reuters answers 401 (DataDome) from the runner and can never fill. It is
    also always among the freshest, so under per-domain-cap-only selection it
    took six slots per scan, forever, for nothing."""
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP", 6)
    monkeypatch.setattr(pipeline, "SNIPPET_BACKFILL_CAP_DOMAIN", 6)
    arts = [_article(f"https://reuters/{i}", domain="www.reuters.com", minutes_old=i) for i in range(24)]
    arts += [_article(f"https://outra/{i}", domain=f"outra{i}.com.br", minutes_old=100 + i) for i in range(5)]
    pipeline._run_snippet_backfill(arts, [])
    assert sum(1 for u in fetched if "reuters" in u) == 1
    assert sum(1 for u in fetched if "outra" in u) == 5
