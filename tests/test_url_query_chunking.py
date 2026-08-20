"""Guard the size of a `url=in.(...)` lookup against the PostgREST query limit.

A PostgREST `in.()` filter travels in the QUERY STRING, so the real limit is the
percent-encoded request length — and the encoding is not uniform: a Latin URL
costs ~150 characters, an Arabic or Chinese one ~257, because every character
becomes %XX%XX. Fixed 100-item chunks passed on Latin sources and blew up the
moment a batch caught attaqa / alarabiya / yicai.

Measured against the live project on 2026-08-20 with real Arabic URLs:

    n= 25  req_len= 6480  -> 200
    n= 50  req_len=12880  -> 200
    n= 75  req_len=19280  -> 200
    n= 95  req_len=24400  -> 200
    n=100  req_len=25680  -> 400   {"message": "JSON could not be generated"}

The failure was silent and expensive: `_existing_translations` returned None, so
`_preserve_translations` took its defensive branch and DEFERRED the rows at risk
— 133 per scan in the logs — meaning the write-once protection for translations
was running degraded, and the same trap was waiting for every new lookup.
"""
from urllib.parse import quote

from news_hunter.supabase_sync import (
    _MAX_QUERY_URL_CHARS,
    _MAX_QUERY_URLS,
    _chunk_urls_for_query,
)

# ~150 encoded chars each.
LATIN = [f"https://www.exemplo.com.br/economia/materia-de-teste-numero-{i:04d}" for i in range(300)]
# ~257 encoded chars each — the shape that broke production.
ARABIC = [f"https://attaqa.net/2026/08/20/غاز-النفط-المسال-الجزائري-يقترب-من-صفق-{i:04d}" for i in range(300)]


def _encoded_size(chunk):
    return sum(len(quote(u, safe="")) + 3 for u in chunk)


def test_every_chunk_fits_the_budget():
    for pool in (LATIN, ARABIC, LATIN[:150] + ARABIC[:150]):
        for chunk in _chunk_urls_for_query(pool):
            assert _encoded_size(chunk) <= _MAX_QUERY_URL_CHARS


def test_non_latin_urls_produce_smaller_chunks_than_latin_ones():
    """The whole point: cut by encoded WEIGHT, not by item count."""
    latin_chunks = _chunk_urls_for_query(LATIN)
    arabic_chunks = _chunk_urls_for_query(ARABIC)
    assert len(latin_chunks[0]) > len(arabic_chunks[0])
    # And the Arabic batch that used to be one 100-item request is now split.
    assert len(arabic_chunks) > len(latin_chunks)


def test_nothing_is_dropped_or_duplicated():
    pool = LATIN[:120] + ARABIC[:120]
    flat = [u for chunk in _chunk_urls_for_query(pool) for u in chunk]
    assert flat == pool


def test_item_cap_still_applies_to_cheap_urls():
    """Short URLs must not build an unbounded row count into one request."""
    tiny = [f"https://a.co/{i}" for i in range(500)]
    for chunk in _chunk_urls_for_query(tiny):
        assert len(chunk) <= _MAX_QUERY_URLS


def test_a_single_oversized_url_still_gets_its_turn():
    """Better to try one doomed request than to silently drop a row from the
    lookup — a missing row reads as 'not stored', which is the wrong answer."""
    monster = "https://x.co/" + "á" * _MAX_QUERY_URL_CHARS
    chunks = _chunk_urls_for_query([monster, "https://a.co/1"])
    assert [monster] in chunks
    assert any("https://a.co/1" in c for c in chunks)


def test_empty_input():
    assert _chunk_urls_for_query([]) == []


def test_budget_leaves_real_headroom_under_the_measured_wall():
    """25.680 encoded chars answered 400 in production; 24.400 answered 200.
    Halving that wall is what keeps a gateway tweak from reopening the bug."""
    assert _MAX_QUERY_URL_CHARS <= 13000
