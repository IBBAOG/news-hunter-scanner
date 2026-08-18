"""Guard the Radio Itatiaia article layout and the feed's registration shape.

Itatiaia is a Next.js + Tailwind site: the body wrapper carries only utility
classes ("mx-auto flex w-full max-w-[640px] ..."), so none of ex_auto's NAMED
selectors match and extraction rests entirely on its LAST fallback, the
<article> tag. That is the opposite of the Gazeta do Povo / Jornal do Comercio
trap, where <article> exists but holds no <p> — here it holds all of them, and
the failure mode to guard is a future page rewrite that moves the <p> out of
<article>, which would silently drop enrich back to the ~140-char meta
description (i.e. the standfirst) with no error anywhere.

Fixture, not network: this asserts the shape. A real layout change is caught by
measure_source / the `no_body` counter, not here.
"""
from news_hunter._clipinator_shim import _extract
from news_hunter.sources import RSS_FEEDS, is_sitemap_url

ARTICLE_HTML = """
<html><head>
  <meta property="og:title" content="Petrobras já gastou US$ 300 milhões em poço na Margem Equatorial">
  <meta property="og:description" content="Apesar do alto custo, a empresa ainda não sabe o potencial produtivo da área">
</head><body>
  <main class="relative z-base">
    <div class="mx-auto max-w-7xl px-4 md:px-6 relative my-6 flex flex-col">
      <article class="mx-auto flex w-full max-w-[640px] flex-col xl:col-start-2">
        <h1>Petrobras já gastou US$ 300 milhões em poço na Margem Equatorial</h1>
        <div class="text-lg text-primary [&amp;_strong]:font-bold">
          <p>A diretora executiva de assuntos corporativos da Petrobras informou nesta
             segunda-feira que a companhia já desembolsou cerca de US$ 300 milhões na
             perfuração do poço Morpho, no bloco FZA-M-59, na Margem Equatorial.</p>
          <p>O local está sendo perfurado desde outubro e custa aproximadamente US$ 1
             milhão por dia, com presença de hidrocarbonetos já identificada.</p>
        </div>
      </article>
    </div>
  </main>
</body></html>
"""


def test_body_comes_from_the_article_tag():
    title, paragraphs = _extract(ARTICLE_HTML, "itatiaia.com.br")
    assert title == "Petrobras já gastou US$ 300 milhões em poço na Margem Equatorial"
    assert len(paragraphs) == 2
    assert paragraphs[0].startswith("A diretora executiva")


def test_extracted_body_beats_the_meta_description():
    """Why the registration is worth having at all.

    Unregistered, enrich falls through to og:description — measured at 92-148
    chars on real Itatiaia articles. The extracted lede is several times that,
    and it is what the lede rescue matches a keyword against.
    """
    _title, paragraphs = _extract(ARTICLE_HTML, "itatiaia.com.br")
    body = " ".join(paragraphs)
    meta_len = len("Apesar do alto custo, a empresa ainda não sabe o potencial produtivo da área")
    assert len(body) > 2 * meta_len


def test_www_and_apex_share_the_extractor():
    """normalize_url strips www, so the apex key is the one enrich looks up."""
    for domain in ("itatiaia.com.br", "www.itatiaia.com.br"):
        _title, paragraphs = _extract(ARTICLE_HTML, domain)
        assert len(paragraphs) == 2


def test_feed_is_registered_as_rss_not_as_a_sitemap():
    """The feed lives on the WP backend host, not on the canonical www host.

    Two things a future edit could plausibly break: pointing the entry at
    www.itatiaia.com.br (which serves a 218 KB Next.js error page under an HTTP
    404 for every feed path), or swapping in sitemap-news.xml, which
    is_sitemap_url() would route to the news-sitemap parser and which was
    rejected on precision grounds — see the comment in sources.py.
    """
    feeds = RSS_FEEDS["www.itatiaia.com.br"]
    assert feeds == ["https://admin.itatiaia.com.br/feed/?cat=68828"]
    assert not any(is_sitemap_url(u) for u in feeds)
