"""Google preferred-source CTA, City AM wording (2026-10-19).

City AM renders the "Add City A.M. as a preferred source on Google" banner as
SVG text inside an unnamed <a>, so no noise class can catch it; the short-CTA
list (paragraphs under SHORT_NOISE_MAX_CHARS) carries the wording instead, in
lockstep with `src/lib/clipping` in the dashboard repo.
"""
from __future__ import annotations

import pytest

from news_hunter._clipinator_shim import SHORT_NOISE_MAX_CHARS, clean_paragraphs

PROSE = (
    "City AM reported that Brent crude rose above $90 a barrel on Monday as "
    "traders priced in fresh supply cuts from OPEC+ producers."
)


@pytest.mark.parametrize("text", [
    "Add as a preferred source on Google",
    "Add City A.M. as a preferred source on Google",
])
def test_preferred_source_cta_is_dropped(text: str) -> None:
    assert len(text) < SHORT_NOISE_MAX_CHARS
    assert clean_paragraphs([PROSE, text]) == [PROSE]


def test_preferred_source_on_google_scholar_survives() -> None:
    # The `preferred source on Google` alternative is end-anchored: a sentence
    # that keeps going after "Google" is prose.
    text = "A preferred source on Google Scholar lists the 2026 study."
    assert clean_paragraphs([text]) == [text]


def test_prose_about_google_survives() -> None:
    text = "The company prefers Google Cloud for its data."
    assert clean_paragraphs([text]) == [text]
