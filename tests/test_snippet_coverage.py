"""Guard the three independent reasons an article reached the feed with no body.

Measured 2026-08-20: 1.026 of the 1.383 rows published in the previous 24h
(74%) carried an EMPTY snippet, so the dashboard showed headlines and nothing
else. Three separate causes, each fixed here and each with its own test:

1. The feed HAD the body and we never read it. WordPress publishes the whole
   article in <content:encoded> while <description> is a short teaser (JOTA:
   96 chars of text against 5.264). The item then entered with no snippet AND
   matched keywords on the title alone.
2. Nobody ever fetched the body of an item that matched on the TITLE. The lede
   rescue only runs on near-misses, so a Google-News-only source (Brasil 247,
   Agencia iNFRA, Reuters) — which arrives with no description at all — was
   guaranteed to land bodyless.
3. The upsert ERASED snippets it had already stored. The scanner is stateless
   and re-pushes the same row every scan (~355 rows / 5 min against ~7 genuinely
   new articles); a row whose body was fetched in scan N came back empty in
   scan N+1 and overwrote the stored text. Same class as the write-once rules
   already protecting published_at (fabricated dates) and title_en.
"""
import feedparser
import pytest

from news_hunter.fetcher import (
    RSS_THIN_SUMMARY_CHARS,
    _entry_to_item,
    _lede_from_content_html,
    _plain_len,
)
from news_hunter.enrich import SNIPPET_MIN_RSS_CHARS, _snippet_from_rss
from news_hunter.supabase_sync import _split_on_snippet


# --- 1. content:encoded -----------------------------------------------------

LEDE = (
    "A manutencao da cobranca de 12% do Imposto de Exportacao sobre o petroleo "
    "bruto nao e apenas uma decisao tributaria. E uma escolha que afeta "
    "competitividade, investimentos e a capacidade do Brasil de transformar sua "
    "producao em emprego, renda, divisas e arrecadacao publica."
)

FEED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
  <title>Fonte</title>
  <item>
    <title>Imposto de Exportacao pune quem produz</title>
    <link>https://www.exemplo.com.br/materia</link>
    <pubDate>Thu, 20 Aug 2026 08:00:09 +0000</pubDate>
    <description><![CDATA[<img width="799" height="533" src="https://img.exemplo/wp-content/uploads/2026/08/foto-da-materia-em-destaque.jpg" class="attachment-large size-large wp-post-image" alt="petroleo" />Cobranca ameaca competitividade]]></description>
    <content:encoded><![CDATA[<p>%s</p><p>Os numeros ajudam a dimensionar o debate: os royalties distribuidos no pais alcancaram R$ 36,5 bilhoes no primeiro semestre.</p>]]></content:encoded>
  </item>
</channel>
</rss>
""" % LEDE


def _first_item():
    return _entry_to_item(feedparser.parse(FEED_XML).entries[0], "www.exemplo.com.br")


def test_thin_description_is_measured_as_text_not_bytes():
    """An <img>-heavy description is long in bytes and short in words."""
    raw = feedparser.parse(FEED_XML).entries[0].get("summary")
    assert len(raw) > RSS_THIN_SUMMARY_CHARS      # bytes say "we have text"
    assert _plain_len(raw) < RSS_THIN_SUMMARY_CHARS  # the text says otherwise


def test_lede_comes_from_content_encoded_when_description_is_thin():
    item = _first_item()
    assert item.summary.startswith("A manutencao da cobranca de 12%")
    # And it is now long enough to survive the snippet threshold.
    assert _snippet_from_rss(item.summary).startswith("A manutencao")


def test_the_two_thresholds_are_one_number():
    """A drift here reopens the bug: the fetcher would fill a summary the
    enricher then rejects as thin, and the row lands empty with nothing erroring."""
    assert SNIPPET_MIN_RSS_CHARS == RSS_THIN_SUMMARY_CHARS


def test_a_fat_description_is_left_alone():
    """content:encoded must not override a description that already works."""
    xml = FEED_XML.replace(
        '<img width="799" height="533" src="https://img.exemplo/wp-content/uploads/2026/08/foto-da-materia-em-destaque.jpg" class="attachment-large size-large wp-post-image" alt="petroleo" />Cobranca ameaca competitividade',
        "Descricao propria do feed, " + "longa o bastante para servir de snippet. " * 5,
    )
    item = _entry_to_item(feedparser.parse(xml).entries[0], "www.exemplo.com.br")
    assert item.summary.startswith("Descricao propria do feed")


def test_lede_is_bounded_to_the_first_paragraphs():
    """The free body must be the SAME text the lede rescue would have fetched.

    Widening it here would silently widen the keyword-matching surface of every
    full-text feed — a body-wide match, not a lede match.
    """
    html = "".join(f"<p>Paragrafo numero {i} com texto suficiente para contar como prosa.</p>" for i in range(10))
    lede = _lede_from_content_html(html)
    assert "Paragrafo numero 0" in lede
    assert "Paragrafo numero 2" in lede
    assert "Paragrafo numero 3" not in lede


def test_lede_drops_captions_and_furniture():
    html = (
        "<figure><figcaption>Foto: Divulgacao</figcaption></figure>"
        "<p>Credito</p>"
        "<script>var x = 1;</script>"
        "<p>Este e o primeiro paragrafo de verdade da materia, com prosa suficiente.</p>"
    )
    lede = _lede_from_content_html(html)
    assert lede.startswith("Este e o primeiro paragrafo")
    assert "Divulgacao" not in lede
    assert "var x" not in lede


def test_no_content_encoded_leaves_the_item_untouched():
    xml = FEED_XML.replace("content:encoded", "content:unused")
    item = _entry_to_item(feedparser.parse(xml).entries[0], "www.exemplo.com.br")
    assert "Cobranca ameaca competitividade" in item.summary


# --- 3. upsert must not erase a stored snippet ------------------------------


def test_rows_without_snippet_are_sent_without_the_column():
    rows = [
        {"url": "a", "title": "A", "snippet": "texto real"},
        {"url": "b", "title": "B", "snippet": ""},
    ]
    with_s, without_s = _split_on_snippet(rows)
    assert [r["url"] for r in with_s] == ["a"]
    assert [r["url"] for r in without_s] == ["b"]
    # The whole point: the key is GONE, so PostgREST leaves the column out of
    # the ON CONFLICT ... DO UPDATE SET list and the stored text survives.
    assert "snippet" not in without_s[0]
    assert with_s[0]["snippet"] == "texto real"


@pytest.mark.parametrize("blank", ["", "   ", "\n"])
def test_whitespace_only_snippet_counts_as_empty(blank):
    with_s, without_s = _split_on_snippet([{"url": "a", "snippet": blank}])
    assert with_s == []
    assert "snippet" not in without_s[0]


def test_payloads_are_key_uniform():
    """PostgREST normalises a heterogeneous payload to the UNION of its keys and
    fills the gaps — dropping the key on SOME rows of one batch would put the
    empty write right back. Each call must carry uniform keys."""
    rows = [
        {"url": "a", "title": "A", "snippet": "x"},
        {"url": "b", "title": "B", "snippet": ""},
        {"url": "c", "title": "C", "snippet": "y"},
    ]
    for payload in _split_on_snippet(rows):
        keysets = {frozenset(r) for r in payload}
        assert len(keysets) == 1
