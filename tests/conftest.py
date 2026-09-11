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


@pytest.fixture(autouse=True)
def _isolate_translate_backends(monkeypatch):
    _translate.reset_breakers()
    monkeypatch.setattr(_translate, "_clients5_call", lambda payload, code: None)
    monkeypatch.setattr(_translate, "_mymemory_call", lambda payload, code: None)
    yield
    _translate.reset_breakers()
