"""Foreign headline/snippet -> English translation (Stage 3c of the pipeline).

Primary backend is `deep-translator`'s GoogleTranslator: free, no API key, pure
Python. The pipeline calls `translate_to_en` ONLY on the handful of items the
keyword filter KEPT whose source_lang is foreign (never the raw firehose — see
the multilingual design §2.5). Everything here is FAIL-SOFT: any exception,
block, timeout, empty result, OR A RESPONSE THAT IS AN ERROR PAGE RATHER THAN A
TRANSLATION returns None, and the caller keeps the native title with
`title_en`/`snippet_en` left NULL. A translation outage is a display regression,
never data loss or a crash.

That last clause is not decoration. deep-translator SCRAPES translate.google.com
and parses whatever HTML comes back, so a Google 5xx is neither an exception nor
an empty string — it is the visible text of Google's error page, handed back as
an ordinary successful result. Between 2026-08-19 and 2026-09-01 that string was
written to 2,670 production rows (34% of every row carrying a `title_en`) and,
because the sink's overlay is write-once, not one of them could ever heal. See
`looks_like_error_page` below for the detector and why it cannot fire on prose.

The runner-block risk (deep-translator scrapes translate.google.com, which the
shared datacenter IP pool can throttle) was validated absent by
scripts/translate_probe.py on the ubuntu-latest runner before this shipped.
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

# Our source_lang tag -> GoogleTranslator source code. Two gotchas, both
# measured (design §3.1 + scripts/translate_probe.py):
#   * Hebrew's tag IS 'iw' (Google's legacy code), NOT 'he' —
#     GoogleTranslator(source='he') raises LanguageNotSupportedException, so the
#     LANGUAGES key, source_lang and this code are ALL 'iw' (identity), never 'he'.
#   * Chinese is 'zh-CN'.
# ar/ru/es/iw are identity. Kept as a table so a new language is a data change,
# not a code change. A tag missing here falls back to 'auto'. (fa is pre-listed
# for a possible future Persian wave; no fa source is registered today — see the
# fa note in sources.LANGUAGES.)
TRANSLATOR_CODE: dict[str, str] = {
    "ar": "ar",
    "fa": "fa",
    "iw": "iw",
    "ru": "ru",
    "zh": "zh-CN",
    "es": "es",
}

# Never translate these: native English / Portuguese / untagged items are
# already display-ready. Keeping the guard here (not only in the caller) means a
# stray call can never spend a translate slot on a native row.
_NATIVE_TAGS = frozenset({"", "en", "pt"})

# GoogleTranslator's free endpoint caps a single call near 5000 chars. Titles
# are short; snippets are <= SNIPPET_MAX_CHARS (360) upstream. This is a pure
# safety clamp so an unexpectedly long body can't raise NotValidLength.
_MAX_CHARS = 4500

# --- The 200-shaped success that is really an error page --------------------
#
# Google's HTTP error pages are a small, fixed template family. The one that
# poisoned production reads, in full:
#
#   Error 500 (Server Error)!!1500.That's an error.There was an error.
#   Please try again later.That's all we know.
#
# The detector below keys on the SHAPE OF THAT TEMPLATE, never on a vocabulary
# word, because "error", "server" and a three-digit number are all ordinary
# content in an energy headline ("Server error halts Brent trading for 500
# minutes") and must keep passing.
#
#   * _GOOGLE_ERROR_TITLE requires the literal sequence
#         Error <3 digits> (<short reason>)!!1
#     The trailing `!!1` is Google's own boilerplate marker — it ships inside the
#     <title> of every page in this family and exists nowhere in written prose.
#     It is the token that makes the match identify THE PAGE rather than the
#     topic: a real headline about a 500 writes "500 error", never
#     "Error 500 (Server Error)!!1".
#
#   * _GOOGLE_ERROR_SIGNOFF is a CONJUNCTION of the template's two closing
#     sentences, both of which must appear in the same field. It exists only so
#     a variant that drops the `!!1` marker is still caught. Either sentence on
#     its own is ordinary English and does NOT fire — it takes both, verbatim,
#     inside one title or snippet.
#
# Both apostrophe forms are accepted (Google serves U+2019; what came back on
# the poisoned rows is ASCII) — a spelling detail, not a loosening.
_GOOGLE_ERROR_TITLE = re.compile(r"Error\s+\d{3}\s*\([^()\n]{0,60}\)\s*!!1")
_GOOGLE_ERROR_SIGNOFF = (
    re.compile(r"That[’']s an error", re.IGNORECASE),
    re.compile(r"That[’']s all we know", re.IGNORECASE),
)


def looks_like_error_page(text: str | None) -> bool:
    """True when the backend handed back Google's error page, not a translation.

    Deliberately tight: see the comment above the two patterns for what is
    matched and, more importantly, for what is NOT (the words "error"/"server",
    a bare status code, or one of the two sign-off sentences alone).
    """
    if not text:
        return False
    if _GOOGLE_ERROR_TITLE.search(text):
        return True
    return all(p.search(text) for p in _GOOGLE_ERROR_SIGNOFF)


def _translator_cls():
    """Import GoogleTranslator lazily.

    Lazy so the pipeline module imports with zero hard dependency on
    deep-translator — if the package is somehow absent on a runner, translation
    degrades to native instead of breaking the whole scan at import time.
    """
    from deep_translator import GoogleTranslator  # noqa: PLC0415

    return GoogleTranslator


def translate_to_en(text: str | None, src_lang: str | None) -> str | None:
    """Translate `text` from `src_lang` to English, or None (fail-soft).

    Returns None — meaning "keep the native text" — when:
      * text is empty,
      * src_lang is native/untagged (en/pt/None) — we never translate those,
      * every backend attempt fails (block, timeout, quota, empty result, or a
        reply that is an error page rather than a translation).

    Fallback chain (all caught): the mapped source code, then source='auto'
    (which handled Hebrew fine in testing and covers a missing mapping), then
    None. Never raises.

    A rejected error page CONTINUES to the next backend rather than aborting: a
    transient 500 on the first call still deserves the second attempt.
    """
    if not text or not text.strip():
        return None
    src = (src_lang or "").strip().lower()
    if src in _NATIVE_TAGS:
        return None

    payload = text[:_MAX_CHARS]
    try:
        GoogleTranslator = _translator_cls()
    except Exception as e:  # noqa: BLE001
        log.debug("deep-translator unavailable: %s", e)
        return None

    code = TRANSLATOR_CODE.get(src, "auto")
    attempts = [code] if code == "auto" else [code, "auto"]
    for attempt_code in attempts:
        try:
            out = GoogleTranslator(source=attempt_code, target="en").translate(payload)
        except Exception as e:  # noqa: BLE001
            log.debug("translate(src=%s) failed: %s", attempt_code, e)
            continue
        if not out or not out.strip():
            # empty result: try the next backend in the chain
            continue
        out = out.strip()
        if looks_like_error_page(out):
            # WARNING, not debug, and it carries the payload: before this guard a
            # translate-endpoint outage was invisible in the scan log AND
            # permanent in the database. Discarding silently would only move the
            # bug — this line is the only place the outage becomes observable.
            log.warning(
                "translate(src=%s): backend returned an error page, not a "
                "translation - discarded, keeping native: %r",
                attempt_code, out[:160],
                extra={"error_page": True},
            )
            # ... and fall through to the next backend: attempt 1 hitting a
            # transient 500 must not cost the item its 'auto' retry.
            continue
        return out
    return None
