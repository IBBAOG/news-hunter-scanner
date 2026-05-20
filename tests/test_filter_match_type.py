"""Tests for the substring vs exact match_type feature (added 2026-05-20).

Run from repo root: `python -m pytest tests/test_filter_match_type.py -v`
Or with stdlib only: `python tests/test_filter_match_type.py`.
"""
from __future__ import annotations

import sys
import os

# Allow running directly: `python tests/test_filter_match_type.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.filter import matches_keywords


def _eq(actual, expected, label):
    ok = sorted(actual) == sorted(expected)
    print(f"{'PASS' if ok else 'FAIL'} — {label}: got={actual} want={expected}")
    return ok


def test_substring_default_matches_inside_word():
    """Without exact_keywords, ANS hits trANSporte (substring fallback)."""
    assert _eq(
        matches_keywords("trANSporte de cargas", ["ANS"]),
        ["ANS"],
        "substring default: ANS matches inside trANSporte",
    )


def test_exact_does_not_match_inside_word():
    """ANS with exact_keywords={ANS} does NOT match trANSporte."""
    assert _eq(
        matches_keywords("trANSporte de cargas", ["ANS"], exact_keywords={"ANS"}),
        [],
        "exact: ANS does not match inside trANSporte",
    )


def test_exact_matches_whole_word():
    """ANS exact matches when standalone."""
    assert _eq(
        matches_keywords("ANS divulga relatorio anual", ["ANS"], exact_keywords={"ANS"}),
        ["ANS"],
        "exact: ANS matches 'ANS divulga relatorio'",
    )


def test_exact_multi_token():
    """Multi-token exact ('saude suplementar') matches the full sequence."""
    assert _eq(
        matches_keywords(
            "Agencia Nacional de Saude Suplementar publicou nota",
            ["saude suplementar"],
            exact_keywords={"saude suplementar"},
        ),
        ["saude suplementar"],
        "exact multi-token: 'saude suplementar' matches as a sequence",
    )


def test_exact_with_accents():
    """Keyword and text are both normalized (accent-stripped + lowercased)."""
    assert _eq(
        matches_keywords(
            "A ANS, agencia regulatoria, anunciou.",
            ["ANS"],
            exact_keywords={"ANS"},
        ),
        ["ANS"],
        "exact + accents: ANS surrounded by comma is matched",
    )


def test_exact_with_hyphen_keyword():
    """Hyphenated keyword matches with hyphen in text but not without."""
    assert _eq(
        matches_keywords(
            "Producao do pre-sal cresceu",
            ["pre-sal"],
            exact_keywords={"pre-sal"},
        ),
        ["pre-sal"],
        "exact pre-sal: matches 'pre-sal' in text",
    )
    assert _eq(
        matches_keywords(
            "Producao do pre sal cresceu",
            ["pre-sal"],
            exact_keywords={"pre-sal"},
        ),
        [],
        "exact pre-sal: does NOT match 'pre sal' (no hyphen)",
    )


def test_mixed_substring_and_exact():
    """petroleo substring + ANS exact: both labels emitted on relevant text."""
    assert _eq(
        matches_keywords(
            "Petroleo cresce; ANS publicou nota",
            ["petroleo", "ANS"],
            exact_keywords={"ANS"},
        ),
        ["petroleo", "ANS"],
        "mixed: petroleo (substring) + ANS (exact) match",
    )
    # ANS exact NOT triggered by 'trANSporte', but petroleo substring still hits.
    assert _eq(
        matches_keywords(
            "Petroleo cresce; trANSporte rodoviario lento",
            ["petroleo", "ANS"],
            exact_keywords={"ANS"},
        ),
        ["petroleo"],
        "mixed: ANS exact suppressed inside trANSporte while petroleo survives",
    )


def test_substring_leftmost_longest_invariant():
    """gas vs gasolina alternation order should not strip 'gasolina' to 'gas'."""
    # Both keywords substring — the longer must report when present.
    result = matches_keywords("preco da gasolina hoje", ["gas", "gasolina"])
    assert "gasolina" in result, f"expected gasolina in {result}"
    print(f"PASS — leftmost-longest: gas/gasolina => {result}")


def test_compat_no_exact_keywords():
    """Legacy callers (no exact_keywords kwarg) get all-substring behaviour."""
    # This is the 'before' state: every keyword treated as substring.
    result = matches_keywords("trANSporte rodoviario", ["ANS"])
    assert _eq(result, ["ANS"], "compat: no exact_keywords -> all substring")


if __name__ == "__main__":
    fns = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    failures = 0
    for fn in fns:
        try:
            fn()
        except AssertionError as e:
            failures += 1
            print(f"  ASSERT FAILED in {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failures += 1
            print(f"  ERROR in {fn.__name__}: {e}")
    sys.exit(1 if failures else 0)
