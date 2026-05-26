"""Tests for the substring vs exact match_type feature (added 2026-05-20).

Includes regression tests for the NFD-strip bug fixed on 2026-05-26:
the old filter._normalize() stripped diacritics before matching, turning
keywords like 'Ira-with-tilde' into 'ira' and hitting 'diretoria', etc.
The fix removes NFD stripping entirely; re.IGNORECASE handles case folding
without destroying accents.

Run from repo root: python -m pytest tests/test_filter_match_type.py -v
Or with stdlib only: python tests/test_filter_match_type.py
"""
from __future__ import annotations

import sys
import os

# Allow running directly: python tests/test_filter_match_type.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.filter import matches_keywords


def _eq(actual, expected, label):
    ok = sorted(actual) == sorted(expected)
    status = "PASS" if ok else "FAIL"
    print(f"{status} -- {label}: got={actual} want={expected}")
    return ok


# ---------------------------------------------------------------------------
# Regression tests for the NFD-strip / diacritic-preservation bug
# ---------------------------------------------------------------------------

def test_ira_with_tilde_exact_does_not_match_diretoria():
    """'Ira-with-tilde' exact must NOT match 'diretoria' (old NFD bug: ira -> ira)."""
    iran = "Irã"  # U+00E3 = a with tilde
    assert _eq(
        matches_keywords("a diretoria decidiu", [iran], exact_keywords={iran}),
        [],
        "exact Iran: does NOT match 'diretoria'",
    )


def test_ira_with_tilde_substring_does_not_match_diretoria():
    """'Ira-with-tilde' substring must NOT match 'diretoria' (old NFD bug)."""
    iran = "Irã"
    assert _eq(
        matches_keywords("a diretoria decidiu", [iran]),
        [],
        "substring Iran: does NOT match 'diretoria'",
    )


def test_ira_with_tilde_substring_matches_correct_context():
    """'Ira-with-tilde' substring DOES match text that contains it."""
    iran = "Irã"
    assert _eq(
        matches_keywords("No Irã, hoje foi anunciado", [iran]),
        [iran],
        "substring Iran: matches 'No Ira-with-tilde, hoje'",
    )


def test_ira_with_tilde_exact_matches_correct_context():
    """'Ira-with-tilde' exact DOES match standalone keyword in text."""
    iran = "Irã"
    assert _eq(
        matches_keywords("a diretoria do Irã decidiu", [iran], exact_keywords={iran}),
        [iran],
        "exact Iran: matches 'a diretoria do Ira-with-tilde decidiu'",
    )


def test_gas_exact_does_not_match_pegasus():
    """'gas' exact must NOT match 'Pegasus' (word-boundary check)."""
    assert _eq(
        matches_keywords("Pegasus voa pelo mundo", ["gas"], exact_keywords={"gas"}),
        [],
        "exact gas: does NOT match inside Pegasus",
    )


def test_gas_exact_matches_standalone():
    """'gas' exact DOES match when the word appears standalone."""
    assert _eq(
        matches_keywords("Vazamento de gas no centro", ["gas"], exact_keywords={"gas"}),
        ["gas"],
        "exact gas: matches standalone 'gas'",
    )


def test_accented_and_unaccented_variants_are_independent():
    """'petroleo' (no accent) must NOT match 'petróleo' (with accent) and vice-versa.

    The default-keywords table ships both variants intentionally so the
    scanner covers sources that omit accents.  When one variant is present
    the other should not fire.
    """
    petroleo_plain = "petroleo"
    petroleo_accented = "petróleo"  # U+00F3 = o with acute
    # Plain keyword in accented text -> no match
    assert _eq(
        matches_keywords("Preço do petróleo sobe", [petroleo_plain]),
        [],
        "plain 'petroleo' does NOT match accented 'petróleo'",
    )
    # Accented keyword in plain text -> no match
    assert _eq(
        matches_keywords("Preco do petroleo sobe", [petroleo_accented]),
        [],
        "accented 'petróleo' does NOT match plain 'petroleo'",
    )
    # Each matches its own form
    assert _eq(
        matches_keywords("Preco do petroleo sobe", [petroleo_plain]),
        [petroleo_plain],
        "plain 'petroleo' matches plain text",
    )
    assert _eq(
        matches_keywords("Preço do petróleo sobe", [petroleo_accented]),
        [petroleo_accented],
        "accented 'petróleo' matches accented text",
    )


# ---------------------------------------------------------------------------
# Original feature tests (kept from 2026-05-20 -- must still pass)
# ---------------------------------------------------------------------------

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
    result = matches_keywords("preco da gasolina hoje", ["gas", "gasolina"])
    assert "gasolina" in result, f"expected gasolina in {result}"
    print(f"PASS -- leftmost-longest: gas/gasolina => {result}")


def test_compat_no_exact_keywords():
    """Legacy callers (no exact_keywords kwarg) get all-substring behaviour."""
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
    print()
    if failures:
        print(f"FAILED: {failures}/{len(fns)} tests failed")
    else:
        print(f"ALL PASSED: {len(fns)} tests")
    sys.exit(1 if failures else 0)
