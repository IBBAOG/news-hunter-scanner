"""Foreign headline/snippet -> English translation (Stage 3c of the pipeline).

Primary backend is `deep-translator`'s GoogleTranslator: free, no API key, pure
Python. The pipeline calls `translate_to_en` ONLY on the handful of items the
keyword filter KEPT whose source_lang is foreign (never the raw firehose — see
the multilingual design §2.5). Everything here is FAIL-SOFT: any exception,
block, timeout or empty result returns None, and the caller keeps the native
title with `title_en`/`snippet_en` left NULL. A translation outage is a display
regression, never data loss or a crash.

The runner-block risk (deep-translator scrapes translate.google.com, which the
shared datacenter IP pool can throttle) was validated absent by
scripts/translate_probe.py on the ubuntu-latest runner before this shipped.
"""
from __future__ import annotations

import logging

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
      * every backend attempt fails (block, timeout, quota, empty).

    Fallback chain (all caught): the mapped source code, then source='auto'
    (which handled Hebrew fine in testing and covers a missing mapping), then
    None. Never raises.
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
        if out and out.strip():
            return out.strip()
        # empty result: try the next backend in the chain
    return None
