"""Guard the repair pass's selection and, above all, its write shape.

The job re-translates foreign rows whose English overlay is missing, after a
database sweep NULLs the values the 2026-08 error-page incident wrote. Three
properties matter and none is obvious from reading the script:

1. It writes with an UPDATE keyed on url AND filtered `title_en is null`. An
   upsert would rewrite the whole row and could regress a fabricated
   published_at; an unfiltered update could stomp a translation a concurrent
   scan just wrote. This job only ever fills something empty.
2. It never touches a `snippet_en` that already has content.
3. It routes through the GUARDED translate_to_en, so a run launched during a
   translate outage writes nothing rather than re-poisoning the very rows it
   was dispatched to repair — and says so by exiting non-zero.
"""
from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scripts.repair_foreign_titles as mod  # noqa: E402
from scripts.repair_foreign_titles import Row, _select  # noqa: E402

POISON = (
    "Error 500 (Server Error)!!1500.That's an error.There was an error. "
    "Please try again later.That's all we know."
)


def _row(url: str, lang: str, *, snippet: str = "", snippet_en: str = "") -> Row:
    return Row(url=url, source_lang=lang, title_original=f"native-{lang}",
               snippet=snippet, snippet_en=snippet_en)


# --- fake PostgREST write surface -------------------------------------------
class _FakeTable:
    def __init__(self, writes, filters):
        self._writes, self._filters = writes, filters
        self._payload = None
        self._keys: list[str] = []

    def update(self, payload):
        self._payload = payload
        return self

    def eq(self, col, val):
        self._keys.append(f"{col}={val}")
        return self

    def is_(self, col, val):
        self._keys.append(f"{col}.is.{val}")
        return self

    def execute(self):
        self._writes.append(self._payload)
        self._filters.append(list(self._keys))
        return None

    def upsert(self, *a, **kw):  # pragma: no cover - must never be called
        raise AssertionError("the repair pass must never upsert")


class _FakeClient:
    def __init__(self):
        self.writes: list[dict] = []
        self.filters: list[list[str]] = []

    def table(self, _name):
        return _FakeTable(self.writes, self.filters)


class _FakeSink:
    def __init__(self):
        self.client = _FakeClient()
        self.table = "news_articles"


def _wire(monkeypatch, rows, translator):
    sink = _FakeSink()
    monkeypatch.setattr(mod, "get_sink", lambda: sink)
    monkeypatch.setattr(mod, "_fetch_candidates", lambda *a, **kw: rows)
    monkeypatch.setattr(mod, "translate_to_en", translator)
    return sink


# ============================================================================
# 1. Selection
# ============================================================================
def test_one_language_cannot_consume_the_run():
    rows = [_row(f"https://ar/{i}", "ar") for i in range(200)]
    rows += [_row(f"https://iw/{i}", "iw") for i in range(4)]
    rows += [_row(f"https://es/{i}", "es") for i in range(3)]
    picked = _select(rows, limit=30, per_lang=400)
    assert sum(1 for r in picked if r.source_lang == "iw") == 4
    assert sum(1 for r in picked if r.source_lang == "es") == 3
    assert len(picked) == 30


def test_per_lang_cap_and_limit_are_respected():
    rows = [_row(f"https://ar/{i}", "ar") for i in range(50)]
    assert len(_select(rows, limit=50, per_lang=3)) == 3
    assert len(_select(rows, limit=7, per_lang=40)) == 7


def test_nothing_is_selected_twice():
    rows = [_row(f"https://x/{i}", ["ar", "ru", "zh"][i % 3]) for i in range(30)]
    picked = _select(rows, limit=30, per_lang=40)
    assert len({r.url for r in picked}) == len(picked)


def test_a_blank_title_original_is_not_a_candidate():
    """Half the predicate is enforced in Python, so it needs its own pin.

    `title_original <> ''` would be `title_original=neq.` on the wire — an empty
    right-hand side whose parsing is a gateway detail, not a contract — so the
    emptiness test lives here instead. A row with nothing native to translate
    must not consume a slot.
    """
    class _Q:
        def __init__(self, rows):
            self._rows = rows

        @property
        def not_(self):
            return self

        def __getattr__(self, _name):
            return lambda *a, **kw: self

        def execute(self):
            return type("R", (), {"data": self._rows})

    class _Sink:
        table = "news_articles"

        def __init__(self, rows):
            self.client = type("C", (), {"table": lambda _s, _n: _Q(rows)})()

    rows = [
        {"url": "https://a/1", "source_lang": "ar", "title_original": "عنوان"},
        {"url": "https://a/2", "source_lang": "ar", "title_original": "   "},
        {"url": "https://a/3", "source_lang": "ar", "title_original": None},
    ]
    got = mod._fetch_candidates(_Sink(rows), limit=10, lang=None)
    assert [r.url for r in got] == ["https://a/1"]


# ============================================================================
# 2. Write shape
# ============================================================================
def test_update_is_keyed_on_url_and_filtered_on_null_title_en(monkeypatch):
    sink = _wire(monkeypatch, [_row("https://a/1", "ar")], lambda t, s: "EN:" + t)
    assert mod.run(limit=10, per_lang=10, lang=None, workers=2,
                   batch=10, pause=0.0, dry_run=False) == 0
    assert sink.client.writes == [{"title_en": "EN:native-ar"}]
    # both the key AND the race guard must be on the request
    assert sink.client.filters == [["url=https://a/1", "title_en.is.null"]]


def test_a_stored_snippet_en_is_never_overwritten(monkeypatch):
    row = _row("https://a/1", "ar", snippet="نص", snippet_en="already English")
    sink = _wire(monkeypatch, [row], lambda t, s: "EN:" + t)
    assert mod.run(limit=10, per_lang=10, lang=None, workers=2,
                   batch=10, pause=0.0, dry_run=False) == 0
    assert sink.client.writes == [{"title_en": "EN:native-ar"}]  # no snippet_en key


def test_an_empty_snippet_en_is_filled(monkeypatch):
    row = _row("https://a/1", "ar", snippet="نص", snippet_en="")
    sink = _wire(monkeypatch, [row], lambda t, s: "EN:" + t)
    assert mod.run(limit=10, per_lang=10, lang=None, workers=2,
                   batch=10, pause=0.0, dry_run=False) == 0
    assert sink.client.writes == [{"title_en": "EN:native-ar", "snippet_en": "EN:نص"}]


def test_dry_run_writes_nothing(monkeypatch):
    sink = _wire(monkeypatch, [_row("https://a/1", "ar")], lambda t, s: "EN:" + t)
    assert mod.run(limit=10, per_lang=10, lang=None, workers=2,
                   batch=10, pause=0.0, dry_run=True) == 0
    assert sink.client.writes == []


def test_a_row_that_could_not_be_translated_is_not_written(monkeypatch):
    sink = _wire(monkeypatch, [_row("https://a/1", "ar")], lambda t, s: None)
    assert mod.run(limit=10, per_lang=10, lang=None, workers=2,
                   batch=10, pause=0.0, dry_run=False) == 0
    assert sink.client.writes == []


# ============================================================================
# 3. A translate outage repairs nothing, writes nothing, and is LOUD
# ============================================================================
def test_outage_writes_nothing_and_exits_nonzero(monkeypatch):
    """The real end-to-end property: the guard inside translate_to_en holds.

    translate_to_en is NOT stubbed here — only the deep-translator class beneath
    it is, and it returns the exact production error page. So this exercises the
    producer's guard through the repair path, which is the reason the repair
    routes through it instead of calling deep-translator itself.
    """
    from news_hunter import translate as translate_mod

    class _ErrorPageTranslator:
        def __init__(self, source=None, target=None):
            pass

        def translate(self, text):  # noqa: ARG002
            return POISON

    monkeypatch.setattr(translate_mod, "_translator_cls", lambda: _ErrorPageTranslator)

    sink = _FakeSink()
    monkeypatch.setattr(mod, "get_sink", lambda: sink)
    monkeypatch.setattr(mod, "_fetch_candidates", lambda *a, **kw: [_row("https://a/1", "ar")])
    monkeypatch.setattr(mod, "translate_to_en", translate_mod.translate_to_en)

    rc = mod.run(limit=10, per_lang=10, lang=None, workers=2,
                 batch=10, pause=0.0, dry_run=False)

    assert sink.client.writes == []   # nothing re-poisoned
    assert rc == 1                    # a repair that repaired nothing is not a success


def test_the_guard_counter_is_detached_after_a_run(monkeypatch):
    """A leaked handler would double-count on the next run in the same process."""
    before = list(logging.getLogger("news_hunter.translate").handlers)
    _wire(monkeypatch, [_row("https://a/1", "ar")], lambda t, s: "EN:" + t)
    mod.run(limit=10, per_lang=10, lang=None, workers=2,
            batch=10, pause=0.0, dry_run=False)
    assert logging.getLogger("news_hunter.translate").handlers == before
