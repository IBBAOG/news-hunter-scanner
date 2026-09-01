"""The 200-shaped success: a Google error page accepted as a translation.

Measured on production `news_articles` 2026-09-01 — 2,670 rows (34% of every row
carrying a `title_en`) whose English headline read, verbatim and identically:

    Error 500 (Server Error)!!1500.That's an error.There was an error.
    Please try again later.That's all we know.

deep-translator SCRAPES translate.google.com and parses whatever HTML comes
back, so Google answering 5xx raises nothing and returns nothing empty: the
parser extracts the visible text of the error page and hands it back as an
ordinary successful string. The old loop's only acceptance test was "non-empty",
so it accepted it, and because the sink's translation overlay is write-once
those rows could never heal. Same defect class as a fabricated timestamp written
on a failure path.

These tests pin BOTH directions, because a guard that merely stops alarming is
worse than no guard:

  * it FIRES on the production string (two source langs), and says so at WARNING;
  * it does NOT fire on a real translation, nor on prose containing "error",
    "server", a three-digit number, or one of Google's sign-off sentences alone;
  * a rejection CONTINUES down the backend chain — an error page on attempt 1
    must not cost the item its 'auto' retry.

Run from repo root: python -m pytest tests/test_multilingual_error_page_guard.py -v
"""
from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import translate as translate_mod  # noqa: E402
from news_hunter.translate import (  # noqa: E402
    looks_like_error_page,
    translate_to_en,
)

# The exact string found in production, byte for byte (ASCII apostrophes, as
# stored). Anything that stops rejecting THIS has regressed the incident.
POISON = (
    "Error 500 (Server Error)!!1500.That's an error.There was an error. "
    "Please try again later.That's all we know."
)

# The same family with Google's typographic apostrophe and without the `!!1`
# marker — caught by the sign-off conjunction rather than the title pattern.
POISON_NO_MARKER = (
    "502.That’s an error.The server encountered a temporary error. "
    "Please try again later.That’s all we know."
)


# --- test doubles: never touch the network ----------------------------------
class _ScriptedTranslator:
    """Returns a queued reply per call; records the source code it was built with."""

    replies: list = []
    sources: list = []

    def __init__(self, source=None, target=None):  # noqa: ARG002
        type(self).sources.append(source)

    def translate(self, text):  # noqa: ARG002
        return type(self).replies.pop(0)

    @classmethod
    def load(cls, replies):
        cls.replies = list(replies)
        cls.sources = []


def _use(monkeypatch, replies):
    _ScriptedTranslator.load(replies)
    monkeypatch.setattr(translate_mod, "_translator_cls", lambda: _ScriptedTranslator)


# ============================================================================
# 1. The guard FIRES on the production payload
# ============================================================================
def test_production_error_page_is_rejected_arabic(monkeypatch):
    # both attempts (mapped code, then 'auto') hand back the error page
    _use(monkeypatch, [POISON, POISON])
    assert translate_to_en("أسعار النفط ترتفع", "ar") is None
    assert _ScriptedTranslator.sources == ["ar", "auto"]  # chain fully walked


def test_production_error_page_is_rejected_chinese(monkeypatch):
    _use(monkeypatch, [POISON, POISON])
    assert translate_to_en("原油价格上涨", "zh") is None
    assert _ScriptedTranslator.sources == ["zh-CN", "auto"]


def test_error_page_without_the_marker_is_rejected(monkeypatch):
    """A variant that drops `!!1` is still caught, by the sign-off conjunction."""
    _use(monkeypatch, [POISON_NO_MARKER, POISON_NO_MARKER])
    assert translate_to_en("Нефть дорожает", "ru") is None


def test_rejection_is_logged_at_warning_with_lang_and_payload(monkeypatch, caplog):
    """A silently discarded translation is the next silent bug.

    Before this guard, an outage of the translate endpoint was invisible in the
    scan log and permanent in the database. The WARNING is the only place it
    becomes observable, so its content is part of the contract.
    """
    _use(monkeypatch, [POISON, POISON])
    with caplog.at_level(logging.WARNING, logger="news_hunter.translate"):
        assert translate_to_en("מחירי הנפט עולים", "iw") is None

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2                       # one per attempt
    first = warnings[0]
    assert getattr(first, "error_page", False) is True
    msg = first.getMessage()
    assert "iw" in msg                              # the source language
    assert "Error 500 (Server Error)" in msg        # a truncated payload
    assert "error page" in msg.lower()


# ============================================================================
# 2. The guard does NOT fire — a real translation, and real prose
# ============================================================================
def test_a_normal_translation_passes_through_unchanged(monkeypatch):
    _use(monkeypatch, ["Oil prices rise as OPEC+ holds output"])
    assert translate_to_en("أسعار النفط ترتفع", "ar") == "Oil prices rise as OPEC+ holds output"


def test_headline_mentioning_error_server_and_a_number_passes(monkeypatch):
    """No false positive on prose.

    Every ingredient of the poisoned string appears here — the word "error", the
    word "server", a three-digit number — and this is a legitimate headline. The
    detector keys on Google's template shape, not on this vocabulary.
    """
    legit = "Server error halts Brent trading for 500 minutes, exchange says"
    _use(monkeypatch, [legit])
    assert translate_to_en("خطأ في الخادم يوقف تداول برنت", "ar") == legit


def test_one_signoff_sentence_alone_is_not_enough(monkeypatch):
    """It takes BOTH of Google's closing sentences. Either alone is English."""
    quoted = "“That’s an error,” the minister said of the 500 bpd figure"
    _use(monkeypatch, [quoted])
    assert translate_to_en("قال الوزير", "ar") == quoted

    other = "That’s all we know about the Petrobras 500 km pipeline, analyst says"
    _use(monkeypatch, [other])
    assert translate_to_en("قال المحلل", "ar") == other


def test_detector_unit_cases():
    """The predicate itself, away from the call chain."""
    assert looks_like_error_page(POISON) is True
    assert looks_like_error_page(POISON_NO_MARKER) is True
    assert looks_like_error_page("Error 404 (Not Found)!!1") is True
    assert looks_like_error_page("Oil prices rise as OPEC+ holds output") is False
    assert looks_like_error_page("Server error 500 hits Petrobras trading desk") is False
    assert looks_like_error_page("Error in the 500 kbpd estimate, ANP admits") is False
    assert looks_like_error_page("") is False
    assert looks_like_error_page(None) is False


# ============================================================================
# 3. A rejection falls through to the next backend, it does not abort
# ============================================================================
def test_error_page_on_first_attempt_falls_through_to_auto(monkeypatch):
    """A transient 500 on the mapped code still deserves the 'auto' retry."""
    _use(monkeypatch, [POISON, "Oil prices rise"])
    assert translate_to_en("أسعار النفط ترتفع", "ar") == "Oil prices rise"
    assert _ScriptedTranslator.sources == ["ar", "auto"]


def test_good_first_attempt_never_reaches_the_second(monkeypatch):
    _use(monkeypatch, ["Oil prices rise", "SHOULD NOT BE USED"])
    assert translate_to_en("أسعار النفط ترتفع", "ar") == "Oil prices rise"
    assert _ScriptedTranslator.sources == ["ar"]


def test_empty_then_error_page_still_returns_none(monkeypatch):
    """Both legacy and new failure modes in one chain — still fail-soft."""
    _use(monkeypatch, ["   ", POISON])
    assert translate_to_en("أسعار النفط", "ar") is None
