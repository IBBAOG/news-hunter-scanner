"""News Hunter scanner — modo `--once` (1 scan e sai) para GitHub Actions.

Sem flag: roda em loop com intervalo SCAN_INTERVAL_SECONDS (modo daemon).
Com `--once`: roda um scan e retorna. Usado pelo workflow GHA scan.yml.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("news_hunter_service")

SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL_SECONDS", "30"))

_shutdown = False


def _sigterm(_sig, _frame):
    global _shutdown
    log.info("SIGTERM/SIGINT recebido — saindo apos o scan atual")
    _shutdown = True


signal.signal(signal.SIGTERM, _sigterm)
signal.signal(signal.SIGINT, _sigterm)


def _run_once() -> int:
    from news_hunter.pipeline import run_search

    t0 = time.time()
    try:
        result = run_search(
            include_google_news=True,
            fast_mode=True,
            hours_override=24,
        )
        log.info(
            "scan done: n_new=%s n_total=%s errors=%s keywords=%s dt=%.1fs",
            result.get("n_new"),
            result.get("n_total"),
            len(result.get("errors", [])),
            result.get("keywords_count"),
            time.time() - t0,
        )
        return 0
    except Exception:
        log.exception("scan crashed")
        return 1


def main() -> int:
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY ausentes — abortando")
        return 2

    once = "--once" in sys.argv[1:]
    if once:
        log.info("scanner: single-shot mode")
        return _run_once()

    log.info("scanner up — interval=%ss", SCAN_INTERVAL)
    while not _shutdown:
        t0 = time.time()
        _run_once()
        if _shutdown:
            break
        sleep_for = max(0.0, SCAN_INTERVAL - (time.time() - t0))
        if sleep_for > 0:
            time.sleep(sleep_for)

    log.info("bye")
    return 0


if __name__ == "__main__":
    sys.exit(main())
