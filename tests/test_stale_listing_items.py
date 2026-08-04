"""Regression tests for the "old article shown as fresh" incident (2026-08-04).

Symptom: Brasil Energia articles from 28-29/07 sat at the top of the News
Hunter feed on 04/08 labelled "13m ago", with an empty snippet and a title
derived from the URL slug ("Cai oferta de gas da petrobras").

Mechanics, in order:

  1. `_scrape_homepage` discovers the "ultimas noticias" listing. That page is
     NOT a last-24h list — it carries 30 links spanning ~7 days — but the
     scraper threw away the date the page prints next to every link.
  2. Items therefore reached the pipeline with `published_at=None` and were
     fetched one by one. When that fetch failed (paywalled 404 slug variant,
     timeout, dropped session) `enrich_item` returned no title, no body and no
     date.
  3. Stage 4 stamped `published = now()` because the domain is in
     RECENT_ONLY_SCRAPERS, and the UPSERT re-applied that stamp to a row that
     ALREADY existed. Since the fabricated date is always "now", the item never
     left the 24h window, so it was re-discovered and re-stamped on the next
     scan — forever.

The three tests below pin the three fixes: read the listing date, refuse to
persist an item we could not reach, and never re-apply a fabricated date to an
existing row.

Run from repo root: python -m pytest tests/test_stale_listing_items.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import pipeline, supabase_sync
from news_hunter.fetcher import RawItem, _listing_hints, _scrape_homepage
from news_hunter.pipeline import _keep_candidate
from news_hunter.store import Article

_NOW = datetime.now(timezone.utc)
_KWS = ["petróleo", "gás", "Petrobras"]
_BE = "www.brasilenergia.com.br"

# Shape of the real listing markup (div.row.archives > div.col-md-12 > a),
# captured 2026-08-04: headline text and dd/mm/yyyy inside the anchor.
_LISTING_HTML = """
<html><body><main class="container"><div class="editorial_">
<section><div class="row archives">
  <div class="col-md-12">
    <a href="/petroleoegas/ep/producao-nacional-bate-recorde-de-5842-mi-boed">
      Produção nacional bate recorde de 5,842 milhões de boe/d em junho
      {today}
    </a>
  </div>
  <div class="col-md-12">
    <a href="/petroleoegas/gas/cai-oferta-de-gas-da-petrobras">
      Cai oferta de gás da Petrobras 29/07/2026
    </a>
  </div>
</div></section>
</div></main></body></html>
"""


def _fake_fetch_html(html: str):
    def _inner(url, timeout=8):  # noqa: ARG001
        return html
    return _inner


def _listing_item(url="https://brasilenergia.com.br/petroleoegas/gas/x-y-z-w-k",
                  title_hint="Cai oferta de gás da Petrobras",
                  published_hint=None):
    """An item as `_scrape_homepage` produces it: no title/summary/date."""
    return RawItem(
        url=url,
        title="",
        summary="",
        published_at=None,
        source_domain="brasilenergia.com.br",
        feed_domain=_BE,
        title_hint=title_hint,
        published_hint=published_hint,
    )


# ---------------------------------------------------------------------------
# 1. The listing date is read (and is what classifies the item)
# ---------------------------------------------------------------------------

def test_scrape_homepage_reads_headline_and_date_from_the_listing(monkeypatch):
    today = datetime.now(timezone.utc).strftime("%d/%m/%Y")
    monkeypatch.setattr(
        "news_hunter._clipinator_shim.fetch_html",
        _fake_fetch_html(_LISTING_HTML.format(today=today)),
    )
    items, err = _scrape_homepage(
        "https://brasilenergia.com.br/petroleoegas/ultimasnoticias", _BE
    )
    assert err is None
    by_slug = {i.url.rsplit("/", 1)[-1]: i for i in items}

    fresh = by_slug["producao-nacional-bate-recorde-de-5842-mi-boed"]
    assert fresh.published_hint == datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    assert fresh.title_hint.startswith("Produção nacional bate recorde")

    stale = by_slug["cai-oferta-de-gas-da-petrobras"]
    assert stale.published_hint == datetime(2026, 7, 29, tzinfo=timezone.utc)
    # The date must not leak into the headline.
    assert stale.title_hint == "Cai oferta de gás da Petrobras"

    # Hints stay OUT of the canonical fields: filling `title`/`published_at`
    # would make enrich_item(need_snippet=False) skip the article fetch, and we
    # would lose the snippet and the exact publication time on every scrape.
    assert stale.title == "" and stale.published_at is None


def test_listing_hint_ignores_a_container_holding_several_dates():
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(
        '<div><a href="/a/b-c-d-e-f">Manchete sem data aqui</a>'
        "<span>01/01/2026</span><span>02/01/2026</span></div>",
        "lxml",
    )
    title, published = _listing_hints(soup.find("a"))
    assert title == "Manchete sem data aqui"
    assert published is None  # ambiguous -> no guess


def test_stale_listing_item_is_dropped_before_any_fetch():
    six_days_ago = datetime.now(timezone.utc) - timedelta(days=6)
    it = _listing_item(published_hint=six_days_ago.replace(hour=0, minute=0))
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) is None


def test_fresh_listing_item_survives_the_hint_filter():
    it = _listing_item(published_hint=_NOW.replace(hour=0, minute=0, second=0))
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) == ["#topic"]


def test_hint_slack_keeps_an_item_published_late_yesterday():
    # A 23:00 article has a 00:00 hint, i.e. the hint reads up to 24h older
    # than reality. The slack must not let the cheap filter drop it.
    it = _listing_item(
        published_hint=(_NOW - timedelta(hours=20)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    )
    assert _keep_candidate(it, _KWS, 24, set(), allow_lede_rescue=True) == ["#topic"]


# ---------------------------------------------------------------------------
# 2. End to end through run_search: what a failed enrichment now does
# ---------------------------------------------------------------------------

def _drive_run_search(monkeypatch, items, enrich_result):
    """Run the real pipeline over `items` with a stubbed collector/enricher."""
    captured: list[list[Article]] = []

    monkeypatch.setattr(
        pipeline, "get_config",
        lambda: {"keywords": _KWS, "exact_keywords": set(), "window_hours": 24},
    )
    monkeypatch.setattr(
        pipeline, "iter_collect",
        lambda *a, **k: iter([(_BE, items, None)]),
    )
    monkeypatch.setattr(pipeline, "enrich_item", enrich_result)
    monkeypatch.setattr(pipeline, "upsert_articles",
                        lambda arts: captured.append(list(arts)) or len(arts))
    pipeline.run_search(include_google_news=False, fast_mode=True, hours_override=24)
    return captured[0] if captured else []


def test_unreachable_listing_item_is_not_persisted(monkeypatch):
    """enrich_item fails -> no title, no body, no date -> nothing persisted.

    This is the whole incident in one assertion: the item used to land with
    published_at = now(), which then got re-applied on every scan.
    """
    def _failed_enrich(it, **kwargs):  # noqa: ARG001
        return "", None, it.url, it.source_domain, ""

    persisted = _drive_run_search(
        monkeypatch,
        [_listing_item(published_hint=_NOW.replace(hour=0, minute=0, second=0))],
        _failed_enrich,
    )
    assert persisted == []


def test_reachable_listing_item_keeps_the_real_page_date(monkeypatch):
    real_date = _NOW - timedelta(hours=3)

    def _ok_enrich(it, **kwargs):  # noqa: ARG001
        return "corpo do artigo sobre a Petrobras", real_date, it.url, it.source_domain, "Título real"

    persisted = _drive_run_search(
        monkeypatch,
        [_listing_item(published_hint=_NOW.replace(hour=0, minute=0, second=0))],
        _ok_enrich,
    )
    assert len(persisted) == 1
    assert persisted[0].published_at == real_date
    assert persisted[0].title == "Título real"
    assert persisted[0].published_is_approx is False


def test_paywalled_item_without_a_date_still_lands_but_flagged(monkeypatch):
    """Requirement: the legitimate now() fallback must keep working.

    Page reached (title + body), but the date is unreadable. The item is kept
    with now() — flagged approximate so the sink never re-stamps it.
    """
    def _dateless_enrich(it, **kwargs):  # noqa: ARG001
        return "corpo do artigo sobre a Petrobras", None, it.url, it.source_domain, "Título real"

    persisted = _drive_run_search(
        monkeypatch, [_listing_item(published_hint=None)], _dateless_enrich
    )
    assert len(persisted) == 1
    assert persisted[0].published_is_approx is True
    assert persisted[0].published_at is not None


def test_dateless_item_prefers_the_listing_date_over_now(monkeypatch):
    """A real (day-granular) date beats a fabricated one."""
    hint = (_NOW - timedelta(hours=5)).replace(hour=0, minute=0, second=0, microsecond=0)

    def _dateless_enrich(it, **kwargs):  # noqa: ARG001
        return "corpo do artigo sobre a Petrobras", None, it.url, it.source_domain, "Título real"

    persisted = _drive_run_search(monkeypatch, [_listing_item(published_hint=hint)],
                                  _dateless_enrich)
    assert len(persisted) == 1
    assert persisted[0].published_at == hint
    assert persisted[0].published_is_approx is False


# ---------------------------------------------------------------------------
# 3. A fabricated date is write-once
# ---------------------------------------------------------------------------

class _FakeSelect:
    def __init__(self, rows, fail=False):
        self._rows, self._fail = rows, fail

    def select(self, _cols):
        return self

    def in_(self, _col, urls):
        self._urls = set(urls)
        return self

    def execute(self):
        if self._fail:
            raise RuntimeError("PostgREST down")
        return type("R", (), {"data": [r for r in self._rows if r["url"] in self._urls]})


class _FakeClient:
    def __init__(self, rows, fail=False):
        self.rows, self.fail = rows, fail
        self.upserted: list[list[dict]] = []

    def table(self, _name):
        client = self

        class _T(_FakeSelect):
            def upsert(self, rows, on_conflict=None):  # noqa: ARG002
                client.upserted.append(rows)
                return type("E", (), {"execute": staticmethod(lambda: None)})

        return _T(self.rows, self.fail)


def _approx_article(url, when):
    return Article(
        url=url, domain="brasilenergia.com.br", source_name="Brasil Energia",
        title="Cai oferta de gás da Petrobras", snippet="", published_at=when,
        found_at=when, matched_keywords=["#topic"], published_is_approx=True,
    )


def _sink_with(client):
    sink = supabase_sync._SupabaseSink.__new__(supabase_sync._SupabaseSink)
    sink.client = client
    sink.table = "news_articles"
    return sink


def test_approximate_date_never_overwrites_an_existing_row():
    url = "https://brasilenergia.com.br/petroleoegas/gas/cai-oferta-de-gas-da-petrobras"
    stored = "2026-07-29T15:41:04.137823+00:00"
    client = _FakeClient([{"url": url, "published_at": stored}])
    _sink_with(client).push([_approx_article(url, _NOW)])

    assert client.upserted[0][0]["published_at"] == stored  # NOT now()


def test_approximate_date_is_written_on_first_discovery():
    url = "https://brasilenergia.com.br/petroleoegas/gas/novissima-materia-de-hoje"
    client = _FakeClient([])  # row does not exist yet
    _sink_with(client).push([_approx_article(url, _NOW)])

    assert client.upserted[0][0]["published_at"] == _NOW.isoformat()


def test_real_date_still_overwrites_so_a_poisoned_row_stays_curable():
    url = "https://brasilenergia.com.br/petroleoegas/gas/cai-oferta-de-gas-da-petrobras"
    client = _FakeClient([{"url": url, "published_at": "2026-08-04T11:16:02+00:00"}])
    real = datetime(2026, 7, 29, 9, 50, tzinfo=timezone.utc)
    article = _approx_article(url, real)
    article.published_is_approx = False  # date read from the page this time

    _sink_with(client).push([article])
    assert client.upserted[0][0]["published_at"] == real.isoformat()


def test_lookup_failure_defers_approximate_rows_instead_of_restamping():
    url = "https://brasilenergia.com.br/petroleoegas/gas/cai-oferta-de-gas-da-petrobras"
    client = _FakeClient([], fail=True)
    sent = _sink_with(client).push([_approx_article(url, _NOW)])

    assert sent == 0 and client.upserted == []


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
