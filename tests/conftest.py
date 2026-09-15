"""Suite-wide isolation for the translation module.

Two pieces of process-global state in news_hunter.translate must not leak
between tests:

  * the HTTP fallback backends (clients5, mymemory) would make real network
    calls from any test that exercises translate_to_en with a scripted
    deep-translator. They are stubbed to "declined" (None) by default; a test
    that wants them re-patches `_clients5_call` / `_mymemory_call` itself.
  * the circuit breaker counts consecutive failures across calls, so a test
    that feeds error pages would open it and change the next test's chain.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter import translate as _translate  # noqa: E402


def registered_hosts() -> set[str]:
    """Every host the scanner can attribute an English-language item to.

    A plain function, not a fixture: the international registry tests feed it to
    `pytest.mark.parametrize`, which runs at collection time, before any fixture
    exists.

    RSS entries arrive normalize_url'd (www stripped) and are registered apex +
    www in `INTERNATIONAL_RSS_DOMAINS`; a Google News entry may be path-scoped
    (`marketwatch.com/story`, `aa.com.tr/en`, `www3.nhk.or.jp/nhkworld`) and what
    reaches the enricher is the host part of it, with or without www. Derived
    from `sources.py` on every call so a wave that registers a feed and forgets
    the name or the extractor turns the tests red on its own.
    """
    from news_hunter.sources import (
        ENGLISH_NO_RSS_DOMAINS,
        INTERNATIONAL_RSS_DOMAINS,
    )

    hosts = set(INTERNATIONAL_RSS_DOMAINS)
    for entry in ENGLISH_NO_RSS_DOMAINS:
        host = entry.split("/")[0].lower()
        hosts.add(host)
        if host.startswith("www."):
            hosts.add(host.removeprefix("www."))
    return hosts


@pytest.fixture(autouse=True)
def _isolate_translate_backends(monkeypatch):
    _translate.reset_breakers()
    monkeypatch.setattr(_translate, "_clients5_call", lambda payload, code: None)
    monkeypatch.setattr(_translate, "_mymemory_call", lambda payload, code: None)
    yield
    _translate.reset_breakers()
