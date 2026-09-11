"""2026-09-11: foreign headlines left untranslated on /home.

Three defects, one symptom (Arabic/Chinese headlines shown native):

  1. Google throttling the scraped translate.google.com/m endpoint (verified on
     the runner AND from a residential IP): the error-page guard correctly
     refused the 500 page, but there was no other backend, so nothing got
     translated. Fix: breaker + clients5 + MyMemory fallbacks.
  2. No retry: a NULL row only healed if a later scan re-saw it in the feed.
     Fix: Stage 3e, a bounded fill-only retry over the database.
  3. One article stored under many urls (Sina tracking query strings, AMP
     mirrors), each needing its own translation. Fix: canonical urls + a
     one-off consolidation job.

Run from repo root: python -m pytest tests/test_translation_throttle_retry_canonical.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import pipeline as pipe  # noqa: E402
from news_hunter import supabase_sync, translation_retry  # noqa: E402
from news_hunter import translate as translate_mod  # noqa: E402
from news_hunter.store import normalize_url  # noqa: E402
from news_hunter.translate import (  # noqa: E402
    _mymemory_call as REAL_MYMEMORY,
    _parse_clients5,
    looks_like_error_page,
    translate_to_en,
)
from scripts.dedupe_canonical_urls import merge_group, plan_groups  # noqa: E402

# Byte for byte what the scan runner logged on 2026-09-11 (typographic apostrophes).
RUNNER_ERROR_PAGE = (
    "Error 500 (Server Error)!!1500.That’s an error.There was an error. "
    "Please try again later.That’s all we know."
)
# What clients5 returned for 海湾国家下周或将与伊朗会晤 商讨霍尔木兹海峡前景 the same day.
REAL_TRANSLATION = "Gulf countries may meet with Iran next week to discuss prospects for the Strait of Hormuz"
ZH = "海湾国家下周或将与伊朗会晤 商讨霍尔木兹海峡前景"
AR = "أقول للحوثيين: لا تراهنوا على إيران"


class _Scripted:
    replies: list = []
    sources: list = []

    def __init__(self, source=None, target=None):  # noqa: ARG002
        type(self).sources.append(source)

    def translate(self, text):  # noqa: ARG002
        r = type(self).replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _google(monkeypatch, replies):
    _Scripted.replies = list(replies)
    _Scripted.sources = []
    monkeypatch.setattr(translate_mod, "_translator_cls", lambda: _Scripted)


# ============================================================================
# 1. The reject heuristic: a real translation vs the runner's error page
# ============================================================================
def test_real_translation_is_not_an_error_page():
    assert looks_like_error_page(REAL_TRANSLATION) is False
    assert looks_like_error_page("I say to the Houthis: Do not bet on Iran") is False
    assert looks_like_error_page(
        "International Energy Agency: OECD refining capacity under pressure, "
        "diesel market will tighten further"
    ) is False


def test_runner_error_page_is_an_error_page():
    assert looks_like_error_page(RUNNER_ERROR_PAGE) is True


def test_error_page_from_google_falls_through_to_clients5(monkeypatch):
    """The 2026-09-11 shape: google_web 500s on both attempts, clients5 works."""
    _google(monkeypatch, [RUNNER_ERROR_PAGE, RUNNER_ERROR_PAGE])
    monkeypatch.setattr(translate_mod, "_clients5_call", lambda p, c: REAL_TRANSLATION)
    assert translate_to_en(ZH, "zh") == REAL_TRANSLATION
    assert _Scripted.sources == ["zh-CN", "auto"]


def test_everything_throttled_returns_none_never_the_error_page(monkeypatch):
    _google(monkeypatch, [RUNNER_ERROR_PAGE, RUNNER_ERROR_PAGE])
    monkeypatch.setattr(translate_mod, "_clients5_call", lambda p, c: RUNNER_ERROR_PAGE)
    monkeypatch.setattr(
        translate_mod, "_mymemory_call",
        lambda p, c: "MYMEMORY WARNING: YOU USED ALL AVAILABLE FREE TRANSLATIONS FOR TODAY",
    )
    assert translate_to_en(AR, "ar") is None


def test_a_fallback_that_echoes_the_input_is_not_a_translation(monkeypatch):
    _google(monkeypatch, [RuntimeError("blocked"), RuntimeError("blocked")])
    monkeypatch.setattr(translate_mod, "_clients5_call", lambda p, c: p)
    assert translate_to_en(AR, "ar") is None


def test_breaker_skips_a_throttled_google_after_three_failures(monkeypatch):
    """Back-off: a throttled endpoint costs 3 calls per process, not 2 per item."""
    _google(monkeypatch, [RUNNER_ERROR_PAGE] * 10)
    monkeypatch.setattr(translate_mod, "_clients5_call", lambda p, c: "EN " + p)
    for _ in range(4):
        assert translate_to_en(ZH, "zh") == "EN " + ZH
    # call 1: 2 google attempts; call 2: 1 more opens the breaker; then none.
    assert len(_Scripted.sources) == 3
    stats = translate_mod.pop_backend_stats()
    assert stats["google_web:fail"] == 3
    assert stats["clients5:ok"] == 4
    assert stats["google_web:skipped"] >= 2


def test_breaker_half_opens_after_cooldown(monkeypatch):
    _google(monkeypatch, [RUNNER_ERROR_PAGE] * 3 + ["Recovered"])
    monkeypatch.setattr(translate_mod, "_clients5_call", lambda p, c: "EN")
    for _ in range(2):
        translate_to_en(ZH, "zh")
    assert len(_Scripted.sources) == 3
    monkeypatch.setattr(translate_mod, "_BREAKER_COOLDOWN", 0.0)
    translate_mod._open_until["google_web"] = 0.0     # cooldown elapsed
    assert translate_to_en(ZH, "zh") == "Recovered"


def test_clients5_response_shapes():
    assert _parse_clients5([REAL_TRANSLATION]) == REAL_TRANSLATION
    assert _parse_clients5([["I say to the Houthis", "ar"]]) == "I say to the Houthis"
    assert _parse_clients5([]) is None
    assert _parse_clients5({"error": 1}) is None


def test_mymemory_quota_warning_raises_so_it_is_never_stored(monkeypatch):
    import requests

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"responseStatus": 200, "quotaFinished": False,
                    "responseData": {"translatedText": "MYMEMORY WARNING: YOU USED ALL AVAILABLE FREE TRANSLATIONS"}}

    monkeypatch.setattr(requests, "get", lambda *a, **kw: _Resp())
    try:
        REAL_MYMEMORY(AR, "ar")
    except RuntimeError:
        return
    raise AssertionError("a MyMemory quota warning must not come back as a translation")


# ============================================================================
# 2. Retry selection + write shape
# ============================================================================
class _Chain:
    def __init__(self, sink):
        self.sink = sink
        self.desc = None

    @property
    def not_(self):
        return self

    def order(self, _col, desc=False):
        self.desc = desc
        return self

    def update(self, payload):
        self.sink.updates.append({"payload": payload, "filters": []})
        return self

    def eq(self, col, val):
        if self.sink.updates:
            self.sink.updates[-1]["filters"].append(f"{col}=eq.{val}")
        return self

    def is_(self, col, val):
        if self.sink.updates:
            self.sink.updates[-1]["filters"].append(f"{col}=is.{val}")
        return self

    def __getattr__(self, _name):
        return lambda *a, **kw: self

    def execute(self):
        if self.sink.fail:
            raise RuntimeError("PostgREST down")
        data = self.sink.newest if self.desc else self.sink.oldest
        return type("R", (), {"data": data})


class _Sink:
    table = "news_articles"

    def __init__(self, newest=(), oldest=(), fail=False):
        self.newest, self.oldest, self.fail = list(newest), list(oldest), fail
        self.updates: list[dict] = []
        self.client = type("C", (), {"table": lambda _s, _n: _Chain(self)})()


def _r(url, title="عنوان", lang="ar", snippet="", snippet_en=None):
    return {"url": url, "source_lang": lang, "title_original": title,
            "snippet": snippet, "snippet_en": snippet_en}


def test_retry_selection_interleaves_newest_and_oldest():
    newest = [_r(f"n{i}") for i in range(10)]
    oldest = [_r(f"o{i}") for i in range(10)]
    got = translation_retry.select_retry_rows(_Sink(newest, oldest), budget=4, days=7)
    assert [r["url"] for r in got] == ["n0", "o0", "n1", "o1"]


def test_retry_selection_respects_exclude_budget_and_blank_titles():
    newest = [_r("a"), _r("b", title="  "), _r("c"), _r("d")]
    got = translation_retry.select_retry_rows(
        _Sink(newest, []), budget=2, days=7, exclude={"a"},
    )
    assert [r["url"] for r in got] == ["c", "d"]


def test_retry_selection_dedupes_rows_seen_from_both_ends():
    rows = [_r("x"), _r("y")]
    got = translation_retry.select_retry_rows(_Sink(rows, list(reversed(rows))), budget=5, days=7)
    assert sorted(r["url"] for r in got) == ["x", "y"]


def test_retry_selection_is_fail_soft():
    assert translation_retry.select_retry_rows(_Sink(fail=True), budget=5, days=7) == []
    assert translation_retry.select_retry_rows(_Sink([_r("a")]), budget=0, days=7) == []


def test_retry_write_is_fill_only():
    sink = _Sink()
    assert translation_retry.fill_missing(sink, "u1", {"title_en": "EN"}) is True
    assert sink.updates == [{"payload": {"title_en": "EN"},
                             "filters": ["url=eq.u1", "title_en=is.null"]}]
    assert translation_retry.fill_missing(sink, "u2", {}) is False
    assert len(sink.updates) == 1


def test_translate_missing_never_touches_a_stored_snippet_en(monkeypatch):
    monkeypatch.setattr(translate_mod, "translate_to_en", lambda t, s: "EN:" + t)
    row = _r("u", title="t", snippet="s", snippet_en="kept")
    assert translation_retry.translate_missing(row) == {"title_en": "EN:t"}
    row = _r("u", title="t", snippet="s", snippet_en=None)
    assert translation_retry.translate_missing(row) == {"title_en": "EN:t", "snippet_en": "EN:s"}


def test_retry_stage_uses_what_the_live_stage_left_and_writes_fill_only(monkeypatch):
    sink = _Sink([_r(f"n{i}") for i in range(30)], [])
    monkeypatch.setattr(supabase_sync, "get_sink", lambda: sink)
    monkeypatch.setattr(translate_mod, "translate_to_en", lambda t, s: "EN")
    monkeypatch.setattr(pipe, "TRANSLATE_CAP", 40)
    monkeypatch.setattr(pipe, "TRANSLATE_RETRY_CAP", 20)
    attempted = {f"live{i}" for i in range(35)}          # live stage used 35 of 40
    n = pipe._run_translation_retry(attempted, errors=[])
    assert n == 5
    assert all(u["filters"][-1] == "title_en=is.null" for u in sink.updates)


def test_retry_stage_does_nothing_when_the_cap_is_spent(monkeypatch):
    sink = _Sink([_r("n0")], [])
    monkeypatch.setattr(supabase_sync, "get_sink", lambda: sink)
    monkeypatch.setattr(pipe, "TRANSLATE_CAP", 40)
    assert pipe._run_translation_retry({f"x{i}" for i in range(40)}, errors=[]) == 0
    assert sink.updates == []


def test_retry_stage_leaves_rows_null_when_translation_fails(monkeypatch):
    sink = _Sink([_r("n0"), _r("n1")], [])
    monkeypatch.setattr(supabase_sync, "get_sink", lambda: sink)
    monkeypatch.setattr(translate_mod, "translate_to_en", lambda t, s: None)
    assert pipe._run_translation_retry(set(), errors=[]) == 0
    assert sink.updates == []


# ============================================================================
# 3. URL canonicalizer + consolidation
# ============================================================================
SINA = "https://finance.sina.com.cn/world/2026-09-11/doc-inirnais6775522.shtml"
SINA_VARIANTS = [
    SINA,
    SINA + "?finpagefr=p_108",
    SINA + "?cre=tianyi&mod=pchp&loc=10&r=0&rfunc=30&tj=cxvertical_pc_hp&tr=12",
    SINA + "?cre=tianyi&mod=pchp&loc=12&r=0&rfunc=33&tj=cxvertical_pc_hp&tr=12",
    SINA + "?cre=tianyi&mod=pchp&loc=9&r=0&rfunc=87&tj=cxvertical_pc_hp&tr=12",
    "https://www.finance.sina.com.cn/world/2026-09-11/doc-inirnais6775522.shtml?cre=tianyi#top",
]


def test_sina_tracking_variants_collapse_to_one_url():
    assert {normalize_url(u) for u in SINA_VARIANTS} == {SINA}


def test_sina_dynamic_page_keeps_its_id():
    u = "https://stock.finance.sina.com.cn/stock/view/paper.php?symbol=sh000001&reportid=841485729478"
    assert normalize_url(u) == u


def test_amp_prefix_collapses_onto_the_canonical_path():
    asharq = "https://asharqbusiness.com/economics/146914/%D9%82%D8%B7%D8%B1"
    assert normalize_url("https://asharqbusiness.com/amp/economics/146914/%D9%82%D8%B7%D8%B1") == asharq
    arabiya = "https://alarabiya.net/aswaq/oil-and-gas/2026/09/11/%D8%A7%D9%84%D8%B7"
    assert normalize_url("https://www.alarabiya.net/amp/aswaq/oil-and-gas/2026/09/11/%D8%A7%D9%84%D8%B7") == arabiya
    assert normalize_url("https://www.alarabiya.net/aswaq/oil-and-gas/2026/09/11/%D8%A7%D9%84%D8%B7") == arabiya


def test_amp_suffix_and_query_switches():
    assert normalize_url("https://tass.com/economy/2186047/amp") == "https://tass.com/economy/2186047"
    assert normalize_url("https://www.arabnews.com/node/2656503/amp") == "https://arabnews.com/node/2656503"
    assert normalize_url("https://globalenergynetwork.net/news-item/first-steel-cut?amp=1") == \
        "https://globalenergynetwork.net/news-item/first-steel-cut"
    assert normalize_url("https://www.bloomberglinea.com.br/mercados/x?outputType=amp") == \
        "https://bloomberglinea.com.br/mercados/x"


def test_things_that_are_not_tracking_are_kept():
    # the article id lives in the query string
    assert normalize_url("https://www.globes.co.il/news/article.aspx?did=1001555334") == \
        "https://globes.co.il/news/article.aspx?did=1001555334"
    # Quintype AMP has no plain counterpart at the de-AMP'd path
    g = "https://gulfnews.com/amp/story/world%2Fmena%2Fjordan-armed-forces"
    assert normalize_url(g) == g
    # outputType with a non-amp value is left alone
    assert normalize_url("https://example.com/a?outputType=json") == "https://example.com/a?outputType=json"
    # `source` is only tracking on intellinews
    assert normalize_url("https://example.com/a?source=12") == "https://example.com/a?source=12"
    assert normalize_url("https://www.intellinews.com/x-466674?source=moldova") == "https://intellinews.com/x-466674"


def test_normalize_url_is_idempotent():
    for u in SINA_VARIANTS + ["https://tass.com/economy/2186047/amp",
                              "https://www.aljazeera.com/amp/news/2026/9/10/x?traffic_source=rss"]:
        once = normalize_url(u)
        assert normalize_url(once) == once


def test_plan_groups_finds_duplicates_and_rename_only_rows():
    urls = SINA_VARIANTS[1:4] + ["https://tass.com/economy/1/amp", "https://ok.com/a"]
    groups = plan_groups(urls)
    assert set(groups) == {SINA, "https://tass.com/economy/1"}
    assert len(groups[SINA]) == 3


def _row(url, **kw):
    base = {"url": url, "domain": "finance.sina.com.cn", "title": "t", "snippet": "",
            "created_at": "2026-09-11T00:00:00+00:00", "matched_keywords": ["oil"],
            "source_lang": "zh", "title_original": "t", "title_en": None, "snippet_en": None}
    base.update(kw)
    return base


def test_merge_keeps_the_canonical_row_and_fills_only_empty_fields():
    rows = [
        _row(SINA, title_en=None, matched_keywords=["oil"]),
        _row(SINA + "?finpagefr=p_108", title_en="Gulf states may meet Iran", snippet="body",
             matched_keywords=["gas"]),
        _row(SINA + "?cre=tianyi", title_en="Other EN", snippet_en="SEN"),
    ]
    keeper, payload, delete = merge_group(SINA, rows)
    assert keeper["url"] == SINA
    assert "url" not in payload                         # already canonical
    assert payload["title_en"] == "Gulf states may meet Iran"   # best sibling first
    assert payload["snippet_en"] == "SEN"
    assert payload["matched_keywords"] == ["oil", "gas"]
    assert sorted(delete) == sorted([SINA + "?finpagefr=p_108", SINA + "?cre=tianyi"])


def test_merge_never_overwrites_the_keepers_translation():
    rows = [_row(SINA, title_en="Keeper EN"), _row(SINA + "?cre=x", title_en="Sibling EN")]
    _keeper, payload, _delete = merge_group(SINA, rows)
    assert "title_en" not in payload


def test_merge_moves_the_best_copy_when_no_row_is_canonical():
    rows = [_row(SINA + "?cre=a"), _row(SINA + "?cre=b", title_en="EN")]
    keeper, payload, delete = merge_group(SINA, rows)
    assert keeper["url"] == SINA + "?cre=b"            # the translated copy survives
    assert payload["url"] == SINA
    assert delete == [SINA + "?cre=a"]


def test_retry_window_uses_published_at(monkeypatch):
    """The retry query is bounded by published_at >= now - days."""
    seen = {}

    class _Spy(_Chain):
        def gte(self, col, val):
            seen[col] = val
            return self

    sink = _Sink([_r("a")], [])
    sink.client = type("C", (), {"table": lambda _s, _n: _Spy(sink)})()
    now = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
    translation_retry.select_retry_rows(sink, budget=1, days=7, now=now)
    assert seen["published_at"].startswith("2026-09-04T12:00")
