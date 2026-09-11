"""Foreign headline/snippet -> English translation (Stage 3c of the pipeline).

Backends, tried in order for every call (each one fail-soft):

  1. `google_web`  deep-translator's GoogleTranslator, which SCRAPES
                   translate.google.com/m — first the mapped source code, then
                   source='auto'.
  2. `clients5`    Google's JSON endpoint used by the Chrome dictionary extension
                   (clients5.google.com/translate_a/t?client=dict-chrome-ex).
                   Same translations, but a JSON API rather than scraped HTML.
  3. `mymemory`    api.mymemory.translated.net — a different provider entirely,
                   the last resort when Google as a whole is refusing us.

The pipeline calls `translate_to_en` ONLY on the handful of items the keyword
filter KEPT whose source_lang is foreign (never the raw firehose — see the
multilingual design §2.5). Everything here is FAIL-SOFT: any exception, block,
timeout, empty result, OR A RESPONSE THAT IS AN ERROR PAGE RATHER THAN A
TRANSLATION returns None, and the caller keeps the native title with
`title_en`/`snippet_en` left NULL. A translation outage is a display regression,
never data loss or a crash.

That error-page clause is not decoration. deep-translator parses whatever HTML
comes back, so a Google 5xx is neither an exception nor an empty string — it is
the visible text of Google's error page, handed back as an ordinary successful
result. Between 2026-08-19 and 2026-09-01 that string was written to 2,670
production rows. See `looks_like_error_page` below.

WHY THERE IS MORE THAN ONE BACKEND (2026-09-11). From 2026-09-01 the
translate.google.com/m endpoint started answering our calls with that error
page more and more often — on the GitHub runner AND from a residential IP, so
it is Google throttling the scraped endpoint, not a datacenter block. Scan logs
went from 10/10 foreign items translated (09-01) to 4/15 (09-07) to 0-1/15
(09-11), and the untranslated rows piled up exactly at the top of the feed.
Measured the same day from the same residential IP that got the 500 page:
clients5 translated 25/25 in a burst and MyMemory translated all probes.

THE BREAKER. A backend that fails `_BREAKER_THRESHOLD` times in a row is skipped
for `_BREAKER_COOLDOWN` seconds, so a throttled Google costs two or three wasted
calls per process instead of two per item — which also stops us hammering an
endpoint that is telling us to back off. After the cooldown one call is let
through (half-open); a success closes the breaker, a failure re-opens it.
"""
from __future__ import annotations

import html
import logging
import re
import threading
import time
from collections import Counter
from urllib.parse import quote

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

# MyMemory speaks ISO codes, so Hebrew is 'he' there (the opposite of Google).
# It needs an explicit source language: 'auto' is not sent to it.
_MYMEMORY_CODE: dict[str, str] = {"iw": "he", "zh-CN": "zh-CN"}

# Never translate these: native English / Portuguese / untagged items are
# already display-ready. Keeping the guard here (not only in the caller) means a
# stray call can never spend a translate slot on a native row.
_NATIVE_TAGS = frozenset({"", "en", "pt"})

# GoogleTranslator's free endpoint caps a single call near 5000 chars. Titles
# are short; snippets are <= SNIPPET_MAX_CHARS (360) upstream. This is a pure
# safety clamp so an unexpectedly long body can't raise NotValidLength.
_MAX_CHARS = 4500

# The HTTP fallbacks send the text in a GET query string. A CJK character costs
# 9 bytes once percent-encoded, so cap the ENCODED length rather than the text
# length; a payload over the cap skips that backend (it is never truncated — a
# half-translated snippet would be worse than the native one).
_CLIENTS5_MAX_ENCODED = 7000
# MyMemory rejects queries over 500 bytes.
_MYMEMORY_MAX_BYTES = 500

_HTTP_TIMEOUT = 8.0
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)
_CLIENTS5_URL = "https://clients5.google.com/translate_a/t"
_MYMEMORY_URL = "https://api.mymemory.translated.net/get"

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

# MyMemory's equivalent of the error page: when the anonymous daily quota runs
# out it returns HTTP 200 with the WARNING as the "translation". Same defect
# class, same answer — it is never written as English.
_MYMEMORY_WARNING = re.compile(r"MYMEMORY WARNING|QUERY LENGTH LIMIT", re.IGNORECASE)


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


# --- Circuit breaker + per-backend counters ---------------------------------
_BREAKER_THRESHOLD = 3
_BREAKER_COOLDOWN = 600.0   # longer than a scan: a throttled backend sits out the run

_state_lock = threading.Lock()
_failures: Counter = Counter()          # backend -> consecutive failures
_open_until: dict[str, float] = {}      # backend -> monotonic deadline
_stats: Counter = Counter()             # "<backend>:ok" / "<backend>:fail" / "<backend>:skipped"


def reset_breakers() -> None:
    """Close every breaker and zero the counters (tests; a fresh process)."""
    with _state_lock:
        _failures.clear()
        _open_until.clear()
        _stats.clear()


def pop_backend_stats() -> dict[str, int]:
    """Return and clear the per-backend counters — the scan logs one summary line."""
    with _state_lock:
        out = dict(_stats)
        _stats.clear()
    return out


def _breaker_allows(name: str) -> bool:
    with _state_lock:
        until = _open_until.get(name)
        if until is None:
            return True
        if time.monotonic() >= until:
            # Half-open: let this call through. _record() decides what happens next.
            _open_until.pop(name, None)
            return True
        _stats[f"{name}:skipped"] += 1
        return False


def _record(name: str, ok: bool) -> None:
    with _state_lock:
        if ok:
            _failures[name] = 0
            _stats[f"{name}:ok"] += 1
            return
        _failures[name] += 1
        _stats[f"{name}:fail"] += 1
        if _failures[name] >= _BREAKER_THRESHOLD and name not in _open_until:
            _open_until[name] = time.monotonic() + _BREAKER_COOLDOWN
            opened = True
        else:
            opened = False
    if opened:
        log.warning(
            "translate backend %s failed %d times in a row - skipped for %.0fs",
            name, _BREAKER_THRESHOLD, _BREAKER_COOLDOWN,
        )


# --- Backends ----------------------------------------------------------------
def _translator_cls():
    """Import GoogleTranslator lazily.

    Lazy so the pipeline module imports with zero hard dependency on
    deep-translator — if the package is somehow absent on a runner, translation
    degrades to native instead of breaking the whole scan at import time.
    """
    from deep_translator import GoogleTranslator  # noqa: PLC0415

    return GoogleTranslator


def _parse_clients5(data) -> str | None:
    """clients5 answers ["text"] with an explicit source, [["text", "ar"]] with auto."""
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, str):
            return first
        if isinstance(first, list) and first and isinstance(first[0], str):
            return first[0]
    return None


def _clients5_call(payload: str, code: str) -> str | None:
    """One call to Google's JSON dictionary endpoint. Raises on HTTP failure."""
    if len(quote(payload, safe="")) > _CLIENTS5_MAX_ENCODED:
        return None
    import requests  # noqa: PLC0415

    r = requests.get(
        _CLIENTS5_URL,
        params={"client": "dict-chrome-ex", "sl": code, "tl": "en", "q": payload},
        headers={"User-Agent": _UA},
        timeout=_HTTP_TIMEOUT,
    )
    if r.status_code != 200:
        raise RuntimeError(f"clients5 HTTP {r.status_code}")
    return _parse_clients5(r.json())


def _mymemory_call(payload: str, code: str) -> str | None:
    """One call to MyMemory. Raises on HTTP failure, quota exhaustion or a warning."""
    if code == "auto" or len(payload.encode("utf-8")) > _MYMEMORY_MAX_BYTES:
        return None
    import requests  # noqa: PLC0415

    src = _MYMEMORY_CODE.get(code, code)
    r = requests.get(
        _MYMEMORY_URL,
        params={"q": payload, "langpair": f"{src}|en"},
        headers={"User-Agent": _UA},
        timeout=_HTTP_TIMEOUT,
    )
    if r.status_code != 200:
        raise RuntimeError(f"mymemory HTTP {r.status_code}")
    j = r.json()
    if j.get("quotaFinished") or str(j.get("responseStatus")) != "200":
        raise RuntimeError(f"mymemory status {j.get('responseStatus')}")
    text = ((j.get("responseData") or {}).get("translatedText") or "")
    if _MYMEMORY_WARNING.search(text):
        raise RuntimeError("mymemory returned a quota/limit warning as the translation")
    return html.unescape(text)


def _reject(out: str | None, payload: str, backend: str, code: str) -> str | None:
    """Return the cleaned translation, or None when it must not be stored."""
    if not out or not out.strip():
        return None
    out = out.strip()
    if looks_like_error_page(out) or _MYMEMORY_WARNING.search(out):
        # WARNING, not debug, and it carries the payload: before this guard a
        # translate-endpoint outage was invisible in the scan log AND
        # permanent in the database. Discarding silently would only move the
        # bug — this line is the only place the outage becomes observable.
        log.warning(
            "translate(src=%s): backend returned an error page, not a "
            "translation - discarded, keeping native: %r",
            code, out[:160],
            extra={"error_page": True, "backend": backend},
        )
        return None
    if backend != "google_web" and out == payload.strip():
        # The HTTP fallbacks echo the input when they cannot translate it.
        return None
    return out


def translate_to_en(text: str | None, src_lang: str | None) -> str | None:
    """Translate `text` from `src_lang` to English, or None (fail-soft).

    Returns None — meaning "keep the native text" — when:
      * text is empty,
      * src_lang is native/untagged (en/pt/None) — we never translate those,
      * every backend attempt fails (block, timeout, quota, empty result, or a
        reply that is an error page rather than a translation).

    Chain (all caught): google_web with the mapped code, google_web with 'auto',
    clients5, mymemory. A backend whose breaker is open is skipped. Never raises.

    A rejected error page CONTINUES to the next attempt rather than aborting: a
    transient 500 on the first call still deserves the rest of the chain.
    """
    if not text or not text.strip():
        return None
    src = (src_lang or "").strip().lower()
    if src in _NATIVE_TAGS:
        return None

    payload = text[:_MAX_CHARS]
    code = TRANSLATOR_CODE.get(src, "auto")

    # 1. google_web (deep-translator): mapped code, then 'auto'.
    try:
        GoogleTranslator = _translator_cls()
    except Exception as e:  # noqa: BLE001
        log.debug("deep-translator unavailable: %s", e)
        GoogleTranslator = None
    if GoogleTranslator is not None:
        attempts = [code] if code == "auto" else [code, "auto"]
        for attempt_code in attempts:
            if not _breaker_allows("google_web"):
                break
            try:
                raw = GoogleTranslator(source=attempt_code, target="en").translate(payload)
            except Exception as e:  # noqa: BLE001
                log.debug("translate(src=%s) failed: %s", attempt_code, e)
                _record("google_web", ok=False)
                continue
            out = _reject(raw, payload, "google_web", attempt_code)
            _record("google_web", ok=out is not None)
            if out is not None:
                return out
            # ... and fall through: attempt 1 hitting a transient 500 must not
            # cost the item the rest of the chain.

    # 2./3. HTTP fallbacks.
    for name, call in (("clients5", _clients5_call), ("mymemory", _mymemory_call)):
        if not _breaker_allows(name):
            continue
        try:
            raw = call(payload, code)
        except Exception as e:  # noqa: BLE001
            log.debug("translate %s(src=%s) failed: %s", name, code, e)
            _record(name, ok=False)
            continue
        if raw is None:
            continue  # backend declined the payload (size/auto) — not a failure
        out = _reject(raw, payload, name, code)
        _record(name, ok=out is not None)
        if out is not None:
            return out
    return None
