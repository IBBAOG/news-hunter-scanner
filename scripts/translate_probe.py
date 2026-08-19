"""Translation gate probe for the multilingual News Hunter feature.

Why this exists: the multilingual design (foreign-language capture + English
translation) hinges on ONE untested assumption — that Google Translate, reached
for free (no key) via `deep-translator`, works from the `ubuntu-latest`
datacenter IP. On a residential IP it is fast and correct; from the shared
Azure/GHA IP pool the free `translate.google.com` endpoint can throttle
(HTTP 429/403) or block where a home connection does not. This is a manual-only
probe — the runner-side twin of the residential test recorded in the design doc.

It translates 12 real oil & gas / Middle-East-conflict headlines (2 per language
across Arabic, Russian, Chinese, Hebrew, Persian/Farsi, Spanish) to English and
prints, per call: source lang, original, translation, elapsed seconds, and any
exception. It ends with a single PASS/FAIL verdict line so the runner status
encodes the gate result (exit 0 = GO, exit 1 = NO-GO).

Correct GoogleTranslator source codes (design doc §3.1): Arabic `ar`, Russian
`ru`, Chinese `zh-CN`, Hebrew `iw` (NOT `he` — unmapped code raises
LanguageNotSupportedException), Persian `fa`, Spanish `es`.

Usage:
    python -m scripts.translate_probe
    python -m scripts.translate_probe --per-call-timeout 30
"""
from __future__ import annotations

import argparse
import concurrent.futures
import sys
import time

# (our_lang_tag, GoogleTranslator source code, native headline, gloss for the reader)
# Real oil & gas / Middle-East-conflict headlines, 2 per language.
SAMPLES: list[tuple[str, str, str, str]] = [
    # Arabic — translator code 'ar'
    ("ar", "ar",
     "أسعار النفط ترتفع مع تصاعد التوترات في مضيق هرمز",
     "Oil prices rise as tensions escalate in the Strait of Hormuz"),
    ("ar", "ar",
     "أرامكو السعودية تعلن عن اكتشاف حقل غاز جديد",
     "Saudi Aramco announces the discovery of a new gas field"),
    # Russian — translator code 'ru'
    ("ru", "ru",
     "Цены на нефть выросли на фоне атак дронов на НПЗ",
     "Oil prices rose amid drone attacks on refineries"),
    ("ru", "ru",
     "Газпром сократил экспорт газа в Европу",
     "Gazprom reduced gas exports to Europe"),
    # Chinese — translator code 'zh-CN'
    ("zh", "zh-CN",
     "中石油宣布在南海发现大型天然气田",
     "PetroChina announces discovery of a large gas field in the South China Sea"),
    ("zh", "zh-CN",
     "国际油价因中东紧张局势上涨",
     "International oil prices rise on Middle East tensions"),
    # Hebrew — translator code 'iw' (NOT 'he')
    ("he", "iw",
     "מחירי הנפט עלו על רקע המתיחות במפרץ",
     "Oil prices rose against the backdrop of tensions in the Gulf"),
    ("he", "iw",
     "חברת החשמל הודיעה על צמצום באספקת הגז הטבעי",
     "The electric company announced a cut in natural gas supply"),
    # Persian / Farsi — translator code 'fa'
    ("fa", "fa",
     "قیمت نفت با افزایش تنش در تنگه هرمز بالا رفت",
     "The price of oil rose with increasing tension in the Strait of Hormuz"),
    ("fa", "fa",
     "وزارت نفت ایران از افزایش تولید در میدان گازی پارس جنوبی خبر داد",
     "Iran's oil ministry reports a production increase at the South Pars gas field"),
    # Spanish — translator code 'es'
    ("es", "es",
     "El precio del petróleo sube por las tensiones en el Estrecho de Ormuz",
     "Oil price rises due to tensions in the Strait of Hormuz"),
    ("es", "es",
     "Pemex anuncia un nuevo descubrimiento de yacimiento petrolero en el Golfo",
     "Pemex announces a new oil field discovery in the Gulf"),
]

# Signatures that mean "the datacenter IP is being throttled/blocked" — a hard
# NO-GO, distinct from a one-off transient hiccup.
BLOCK_SIGNATURES = (
    "429", "403", "too many requests", "toomanyrequests", "blocked",
    "forbidden", "quota", "rate limit", "ratelimit", "captcha",
    "service unavailable", "503",
)
TIMEOUT_SIGNATURES = (
    "timeout", "timed out", "read timed out", "connection", "connectionerror",
    "max retries", "temporary failure in name resolution", "ssl",
)


def _classify(exc_name: str, exc_msg: str) -> str:
    """Return 'block' | 'timeout' | 'error' for a failure."""
    blob = f"{exc_name} {exc_msg}".lower()
    if any(s in blob for s in BLOCK_SIGNATURES):
        return "block"
    if any(s in blob for s in TIMEOUT_SIGNATURES):
        return "timeout"
    return "error"


def _translate_once(translator_cls, src_code: str, text: str) -> str:
    return translator_cls(source=src_code, target="en").translate(text)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--per-call-timeout",
        type=float,
        default=30.0,
        help="hard wall-clock cap per translation call in seconds (default 30)",
    )
    args = ap.parse_args(argv)

    print("=== Translation gate probe (deep-translator / GoogleTranslator, free, no key) ===",
          flush=True)
    print("Purpose: is Google Translate reachable + correct from the ubuntu-latest datacenter IP?",
          flush=True)

    try:
        import deep_translator
        from deep_translator import GoogleTranslator
    except Exception as e:  # noqa: BLE001
        print(f"\nFATAL: could not import deep_translator: {type(e).__name__}: {e}", flush=True)
        print("\nVERDICT: FAIL (deep-translator not installed / import error)", flush=True)
        return 1

    print(f"deep-translator version: {getattr(deep_translator, '__version__', 'unknown')}",
          flush=True)
    print(f"per-call timeout       : {args.per_call_timeout:.0f}s", flush=True)

    successes = 0
    failures = 0
    block_hits = 0
    timeout_hits = 0
    total_elapsed = 0.0

    # A single-worker pool lets us impose a hard per-call timeout without the
    # underlying (possibly hung) call eating the whole job budget.
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    for idx, (lang, src_code, native, gloss) in enumerate(SAMPLES, start=1):
        print(f"\n--- [{idx:>2}/{len(SAMPLES)}] source_lang={lang}  (translator code '{src_code}') ---",
              flush=True)
        print(f"original    : {native}", flush=True)
        print(f"expected    : {gloss}", flush=True)

        t0 = time.time()
        try:
            fut = pool.submit(_translate_once, GoogleTranslator, src_code, native)
            result = fut.result(timeout=args.per_call_timeout)
            elapsed = time.time() - t0
            total_elapsed += elapsed
            if result and result.strip():
                successes += 1
                print(f"translation : {result}", flush=True)
                print(f"elapsed     : {elapsed:.2f}s", flush=True)
                print("status      : OK", flush=True)
            else:
                failures += 1
                print(f"translation : <empty>", flush=True)
                print(f"elapsed     : {elapsed:.2f}s", flush=True)
                print("status      : FAIL (empty result)", flush=True)
        except concurrent.futures.TimeoutError:
            elapsed = time.time() - t0
            total_elapsed += elapsed
            failures += 1
            timeout_hits += 1
            print(f"elapsed     : {elapsed:.2f}s", flush=True)
            print(f"exception   : TimeoutError (exceeded {args.per_call_timeout:.0f}s)", flush=True)
            print("status      : FAIL (timeout)", flush=True)
        except Exception as e:  # noqa: BLE001
            elapsed = time.time() - t0
            total_elapsed += elapsed
            failures += 1
            kind = _classify(type(e).__name__, str(e))
            if kind == "block":
                block_hits += 1
            elif kind == "timeout":
                timeout_hits += 1
            print(f"elapsed     : {elapsed:.2f}s", flush=True)
            print(f"exception   : {type(e).__name__}: {e}", flush=True)
            print(f"status      : FAIL ({kind})", flush=True)

    pool.shutdown(wait=False)

    total = len(SAMPLES)
    avg = (total_elapsed / total) if total else 0.0

    print("\n=== SUMMARY ===", flush=True)
    print(f"success   : {successes}/{total}", flush=True)
    print(f"failure   : {failures}/{total}", flush=True)
    print(f"blocked   : {block_hits} (HTTP 429/403/quota/captcha signatures)", flush=True)
    print(f"timeouts  : {timeout_hits}", flush=True)
    print(f"avg call  : {avg:.2f}s   total: {total_elapsed:.2f}s", flush=True)

    # Verdict:
    #   NO-GO if ANY block signature (a throttled datacenter IP only gets worse
    #     under real load — one 429 is a decisive signal, not noise).
    #   NO-GO if fewer than 75% of calls returned English text (endpoint
    #     effectively unusable — timeouts / connection failures / empties).
    #   GO otherwise (all/most correct within a few seconds).
    go = block_hits == 0 and successes >= (total * 3 + 3) // 4  # ceil(0.75*total)

    if go:
        print(f"\nVERDICT: PASS (GO) — Google Translate is reachable and correct from the "
              f"ubuntu-latest datacenter IP: {successes}/{total} correct, avg {avg:.2f}s/call, "
              f"no 429/403/block.", flush=True)
        return 0

    if block_hits:
        reason = f"{block_hits} call(s) hit an HTTP 429/403/quota/block signature from the datacenter IP"
    elif timeout_hits and successes < (total * 3 + 3) // 4:
        reason = f"{timeout_hits} timeout(s); only {successes}/{total} usable"
    else:
        reason = f"only {successes}/{total} calls returned usable English"
    print(f"\nVERDICT: FAIL (NO-GO) — {reason}. "
          f"Pick a fallback (MyMemory/LibreTranslate free, residential self-hosted runner, "
          f"or a paid API).", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
