"""Wave B2a — multilingual translation convergence + write-once guard.

Two structural defects surfaced by the live run (~155 foreign items/scan vs
TRANSLATE_CAP=40):

  1. NULL clobber (CORRECTNESS). supabase_sync wrote title_en UNCONDITIONALLY, so
     a foreign row translated in scan N, re-seen in scan N+1 while OVER the cap
     (title_en=None in memory — the scanner is stateless), had its stored English
     nulled out and the /news-hunter headline flickered back to native. Fixed by
     _preserve_translations (write-once, mirroring _freeze_approx_dates for dates).

  2. Churn instead of convergence (LIVENESS). _run_translation re-considered the
     whole kept foreign set every scan, so the cap was re-spent on the same front
     rows (possibly already translated) and the deferred tail never drained. Fixed
     by skipping URLs that already carry title_en in the DB, so each scan's budget
     goes to untranslated items and the backlog drains monotonically.

The English golden stays byte-identical (test_multilingual_en_frozen.py); the
ar/ru/zh mechanism is unchanged (test_multilingual_{ar,ru,zh}.py). These tests pin
the two fixes and prove native paths are untouched.

Run from repo root: python -m pytest tests/test_multilingual_translation_convergence.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import supabase_sync  # noqa: E402
from news_hunter import translate as translate_mod  # noqa: E402
from news_hunter.store import Article  # noqa: E402
from news_hunter.supabase_sync import (  # noqa: E402
    _article_to_row,
    _may_regress_translation,
)

_NOW = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)


# --- fake Supabase client (same shape as tests/test_stale_listing_items.py) ---
class _FakeSelect:
    def __init__(self, rows, fail=False):
        self._rows, self._fail = rows, fail
        self._urls: set[str] = set()

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


def _sink_with(client):
    sink = supabase_sync._SupabaseSink.__new__(supabase_sync._SupabaseSink)
    sink.client = client
    sink.table = "news_articles"
    return sink


def _foreign_article(url, *, title_en=None, snippet_en=None, source_lang="ar",
                     title="أسعار النفط", snippet="نص عربي"):
    """A kept foreign Article as it reaches the push layer.

    title_original is stamped for EVERY foreign row by _run_translation, so it is
    present here even when the translation is deferred (title_en None).
    """
    return Article(
        url=url, domain="attaqa.net", source_name="attaqa",
        title=title, snippet=snippet, published_at=_NOW, found_at=_NOW,
        matched_keywords=["oil"], source_lang=source_lang,
        title_original=title, title_en=title_en, snippet_en=snippet_en,
    )


def _upserted_row(client, url):
    for chunk in client.upserted:
        for row in chunk:
            if row["url"] == url:
                return row
    return None


# ============================================================================
# 1. THE regression pin — a re-seen foreign row OVER the cap keeps title_en
# ============================================================================
def test_over_cap_reseen_row_keeps_its_stored_title_en():
    """Scan N translated it; scan N+1 re-sees it over the cap (title_en=None).

    The upsert MUST NOT null the stored English — the exact flicker-back bug.
    """
    url = "https://attaqa.net/oil-prices-rise"
    stored = {
        "url": url, "source_lang": "ar", "title_original": "أسعار النفط",
        "title_en": "Oil prices rise", "snippet_en": "Arabic body in English",
    }
    client = _FakeClient([stored])

    # over the cap this scan -> not translated -> English overlay empty in memory
    reseen = _foreign_article(url, title_en=None, snippet_en=None)
    sent = _sink_with(client).push([reseen])

    row = _upserted_row(client, url)
    assert sent == 1 and row is not None
    assert row["title_en"] == "Oil prices rise"     # NOT nulled — preserved
    assert row["snippet_en"] == "Arabic body in English"
    assert row["source_lang"] == "ar"
    # native columns still carry the as-scraped values
    assert row["title"] == "أسعار النفط"
    assert row["title_original"] == "أسعار النفط"


def test_over_cap_reseen_row_still_updates_native_columns():
    """Preserving the overlay must not freeze the row: native title/snippet and
    matched_keywords still update on the re-seen upsert."""
    url = "https://attaqa.net/opec-meets"
    client = _FakeClient([{"url": url, "title_en": "OPEC meets", "source_lang": "ar"}])

    reseen = _foreign_article(url, title_en=None, title="أوبك تجتمع اليوم",
                              snippet="نص محدث")
    reseen.matched_keywords = ["oil", "OPEC"]
    _sink_with(client).push([reseen])

    row = _upserted_row(client, url)
    assert row["title_en"] == "OPEC meets"          # overlay preserved
    assert row["title"] == "أوبك تجتمع اليوم"        # native title updated
    assert row["matched_keywords"] == ["oil", "OPEC"]


# ============================================================================
# 2. New rows and fresh re-translations are written as-is (no over-preservation)
# ============================================================================
def test_new_foreign_row_writes_its_fresh_translation():
    """First discovery: nothing stored, so the freshly-translated overlay is
    written unchanged (the guard must not blank a brand-new translation)."""
    url = "https://attaqa.net/brand-new"
    client = _FakeClient([])  # url does not exist yet
    fresh = _foreign_article(url, title_en="Fresh EN title", snippet_en="Fresh EN body")
    sent = _sink_with(client).push([fresh])

    row = _upserted_row(client, url)
    assert sent == 1
    assert row["title_en"] == "Fresh EN title"
    assert row["snippet_en"] == "Fresh EN body"


def test_fresh_retranslation_overwrites_the_stored_value():
    """COALESCE gives precedence to a NON-empty incoming value: a genuine
    re-translation still overwrites, so a row stays curable (mirrors the
    write-once-date 'real date still overwrites' guarantee)."""
    url = "https://attaqa.net/updated"
    client = _FakeClient([{"url": url, "source_lang": "ar", "title_en": "Old EN"}])
    incoming = _foreign_article(url, title_en="New EN")   # translated again this scan
    _sink_with(client).push([incoming])

    assert _upserted_row(client, url)["title_en"] == "New EN"   # incoming wins


def test_partial_overlay_preserves_only_the_missing_field():
    """Incoming has title_en but not snippet_en; DB has both -> keep the stored
    snippet_en, keep the incoming title_en (per-field COALESCE)."""
    url = "https://attaqa.net/partial"
    client = _FakeClient([{
        "url": url, "source_lang": "ar",
        "title_en": "Stored title", "snippet_en": "Stored snippet",
    }])
    incoming = _foreign_article(url, title_en="Fresh title", snippet_en=None)
    _sink_with(client).push([incoming])

    row = _upserted_row(client, url)
    assert row["title_en"] == "Fresh title"        # incoming (non-empty) wins
    assert row["snippet_en"] == "Stored snippet"   # missing field preserved


# ============================================================================
# 3. Lookup failure defers the at-risk row rather than clobbering
# ============================================================================
def test_lookup_failure_defers_untranslated_foreign_row():
    """If the overlay lookup fails we cannot rule out a stored translation, so an
    incoming foreign row whose title_en is empty is DROPPED from this push and
    retried next scan — deferring is reversible, a headline regression is not."""
    url = "https://attaqa.net/lookup-down"
    client = _FakeClient([], fail=True)
    reseen = _foreign_article(url, title_en=None)
    sent = _sink_with(client).push([reseen])

    assert sent == 0 and client.upserted == []     # never written as NULL


def test_lookup_failure_still_writes_a_freshly_translated_row():
    """On the same failure, a row that HAS a fresh title_en is safe to write
    (setting title_en can't regress it), so it is not deferred."""
    url = "https://attaqa.net/lookup-down-but-fresh"
    client = _FakeClient([], fail=True)
    fresh = _foreign_article(url, title_en="Fresh despite outage")
    sent = _sink_with(client).push([fresh])

    assert sent == 1
    assert _upserted_row(client, url)["title_en"] == "Fresh despite outage"


# ============================================================================
# 4. Native (pt/en/untagged) rows are exempt — never looked up, never preserved
# ============================================================================
def test_native_rows_are_not_regression_candidates():
    assert _may_regress_translation(_foreign_article("u", source_lang="pt")) is False
    assert _may_regress_translation(_foreign_article("u", source_lang="en")) is False
    assert _may_regress_translation(_foreign_article("u", source_lang=None)) is False
    # a foreign row fully translated is also not at risk (nothing to restore)
    assert _may_regress_translation(
        _foreign_article("u", title_en="t", snippet_en="s")
    ) is False
    # a foreign row missing either English field IS a candidate
    assert _may_regress_translation(_foreign_article("u", title_en=None)) is True
    assert _may_regress_translation(
        _foreign_article("u", title_en="t", snippet_en=None)
    ) is True


def test_pt_firehose_is_never_looked_up_even_with_a_poison_row_present():
    """A PT row is written as-is and no lookup override is applied to it, even if
    (bizarrely) a stored title_en existed under its url — native rows are exempt,
    which is also what keeps the push from scanning the whole PT firehose."""
    url = "https://valor.globo.com/petrobras"
    client = _FakeClient([{"url": url, "title_en": "should be ignored"}])
    pt = Article(
        url=url, domain="valor.globo.com", source_name="Valor",
        title="Petrobras sobe diesel", snippet="corpo", published_at=_NOW,
        found_at=_NOW, matched_keywords=["diesel"], source_lang="pt",
    )
    _sink_with(client).push([pt])

    row = _upserted_row(client, url)
    assert row["title_en"] is None                 # native overlay stays NULL
    assert row["title"] == "Petrobras sobe diesel"


# ============================================================================
# 5. _run_translation spends the cap on UNTRANSLATED rows (Fix 2)
# ============================================================================
def test_run_translation_skips_urls_already_translated_in_db(monkeypatch):
    """With cap=1 and [already-done, new], the skip advances the cap PAST the
    done row so the NEW row is the one translated — without the skip the cap
    would be spent re-translating the done row and the new one would starve."""
    import news_hunter.pipeline as pipe

    done_url = "https://attaqa.net/already-done"
    new_url = "https://attaqa.net/needs-work"

    monkeypatch.setattr(pipe, "TRANSLATE_CAP", 1)
    monkeypatch.setattr(
        supabase_sync, "already_translated_urls", lambda urls: {done_url}
    )
    monkeypatch.setattr(
        translate_mod, "translate_to_en",
        lambda text, src: ("EN:" + text) if src == "ar" else None,
    )

    done = _foreign_article(done_url, title="نص منجز")   # comes first in the list
    new = _foreign_article(new_url, title="نص جديد")
    n = pipe._run_translation([done, new], errors=[])

    assert n == 1                                  # exactly the one untranslated row
    assert new.title_en == "EN:نص جديد"            # the cap was spent here
    assert done.title_en is None                   # skipped, not re-translated
    # title_original is still stamped for the skipped row (cheap, in-memory)
    assert done.title_original == "نص منجز"


def test_run_translation_falls_back_to_translate_all_when_lookup_unavailable(monkeypatch):
    """Fail-soft: a None from already_translated_urls (Supabase down) reverts to
    the old behaviour — translate up to the cap, skipping nothing. The write-once
    guard in supabase_sync still prevents any NULL clobber downstream."""
    import news_hunter.pipeline as pipe

    monkeypatch.setattr(pipe, "TRANSLATE_CAP", 40)
    monkeypatch.setattr(supabase_sync, "already_translated_urls", lambda urls: None)
    monkeypatch.setattr(
        translate_mod, "translate_to_en",
        lambda text, src: ("EN:" + text) if src == "ar" else None,
    )

    a = _foreign_article("https://attaqa.net/a", title="نص أ")
    b = _foreign_article("https://attaqa.net/b", title="نص ب")
    n = pipe._run_translation([a, b], errors=[])

    assert n == 2                                  # both translated (nothing skipped)
    assert a.title_en == "EN:نص أ" and b.title_en == "EN:نص ب"


def test_run_translation_queries_only_the_foreign_urls(monkeypatch):
    """The Fix-2 lookup is passed the FOREIGN urls only — never the pt/en/None
    firehose (which cannot carry a translation and would waste the query)."""
    import news_hunter.pipeline as pipe

    seen: dict = {}

    def _spy(urls):
        seen["urls"] = list(urls)
        return set()

    monkeypatch.setattr(supabase_sync, "already_translated_urls", _spy)
    monkeypatch.setattr(
        translate_mod, "translate_to_en", lambda text, src: "EN:" + text
    )

    ar = _foreign_article("https://attaqa.net/ar", source_lang="ar")
    pt = Article(url="https://valor.globo.com/x", domain="valor.globo.com",
                 source_name="Valor", title="Preço", snippet="", published_at=_NOW,
                 found_at=_NOW, matched_keywords=["diesel"], source_lang="pt")
    en = Article(url="https://reuters.com/y", domain="reuters.com",
                 source_name="Reuters", title="Oil", snippet="", published_at=_NOW,
                 found_at=_NOW, matched_keywords=["oil"], source_lang="en")
    pipe._run_translation([ar, pt, en], errors=[])

    assert seen["urls"] == ["https://attaqa.net/ar"]   # foreign only


# ============================================================================
# 6. _article_to_row COALESCE contract (unit-level)
# ============================================================================
def test_article_to_row_tx_override_coalesces_per_field():
    a = _foreign_article("https://attaqa.net/z", title_en=None, snippet_en=None)
    # no override -> incoming values (None) serialize to SQL NULL, unchanged
    assert _article_to_row(a)["title_en"] is None
    # override supplies only the fields to restore
    row = _article_to_row(a, None, {"title_en": "Restored", "snippet_en": "Restored s"})
    assert row["title_en"] == "Restored"
    assert row["snippet_en"] == "Restored s"
    assert row["source_lang"] == "ar"              # not in override -> incoming


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
