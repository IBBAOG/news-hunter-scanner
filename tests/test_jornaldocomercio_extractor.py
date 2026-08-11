"""Guard the two independent traps in the Jornal do Comercio article layout.

1. The body lives in `section.paywall-carregando`. The page DOES have an
   <article> tag, so ex_auto "succeeds" and returns nothing — the failure is
   silent and looks like a source with no body.
2. Inside that section the article is a single <div> with <br> separators and
   no <p> at all, so even the correct container yields zero paragraphs through
   normal <p>-collection.

Both were live on 2026-08-11. Fixture, not network: this asserts the shape of
the extractor, and a real layout change is caught by measure_source / the
`no_body` counter, not here.
"""
from news_hunter._clipinator_shim import _extract

ARTICLE_HTML = """
<html><head>
  <meta property="og:title" content="Plataforma P-33 já está em Rio Grande">
  <meta name="description" content="Notícias sobre negócios e os principais setores da economia">
</head><body>
  <article><h1>Plataforma P-33 já está em Rio Grande</h1></article>
  <section class="paywall-carregando relative">
    <div>
      A plataforma P-33, da Petrobras, já está na região oceânica de Rio Grande.<br/>
      A estrutura será desmantelada no município pela Ecovix.<br/>
      A sucata metálica deverá ser destinada à Gerdau.
    </div>
    <div class="relative"></div>
  </section>
</body></html>
"""


def test_body_is_extracted_from_the_paywall_section():
    title, paragraphs = _extract(ARTICLE_HTML, "jornaldocomercio.com")
    assert title == "Plataforma P-33 já está em Rio Grande"
    # <br>-separated, so this must be 3 paragraphs and not 1 blob or 0.
    assert len(paragraphs) == 3
    assert paragraphs[0].startswith("A plataforma P-33, da Petrobras")


def test_keyword_lives_only_in_the_body():
    """The whole point of extracting JC bodies: the lede carries the keyword.

    Title and slug say "Plataforma P-33 ... Rio Grande" — no keyword. Only the
    first body paragraph says "Petrobras". If the extractor regresses to the
    meta description, this article stops matching (the description on JC is
    frequently the SECTION boilerplate, not the article standfirst).
    """
    title, paragraphs = _extract(ARTICLE_HTML, "jornaldocomercio.com")
    assert "Petrobras" not in title
    assert "Petrobras" in " ".join(paragraphs)


def test_www_and_apex_share_the_extractor():
    """normalize_url strips www, so the apex key is the one enrich looks up."""
    for domain in ("jornaldocomercio.com", "www.jornaldocomercio.com"):
        _title, paragraphs = _extract(ARTICLE_HTML, domain)
        assert len(paragraphs) == 3
