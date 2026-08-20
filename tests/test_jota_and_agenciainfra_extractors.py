"""Guard the two layouts that made these outlets invisible to the scanner.

Both were live on 2026-08-20, and both fail the SAME way — silently. ex_auto
"succeeds" (the page has an <article>), returns zero paragraphs, and enrich
falls through to the meta description, so the outlet quietly degrades to
title-only matching instead of erroring.

1. JOTA (jota.info) serves NO article <p> at all: the body exists only inside
   the Next.js `__NEXT_DATA__` JSON island at props.pageProps.post.content.
2. Agencia iNFRA moved to a Hello Elementor theme: div.entry-content is gone
   and the <article> elements on the page are related-post cards holding zero
   <p>.

Fixture, not network: this asserts the shape of each extractor. A real layout
change is caught by measure_source / the `no_body` counter, not here.
"""
import json

from news_hunter._clipinator_shim import _extract

# --- JOTA -------------------------------------------------------------------

_JOTA_NEXT_DATA = {
    "props": {
        "pageProps": {
            "post": {
                "title": "Imposto de Exportação pune quem produz",
                "content": (
                    "<p>A manutenção da cobrança de 12% do Imposto de Exportação sobre o "
                    "petróleo bruto não é apenas uma decisão tributária.</p>"
                    "<p>No primeiro semestre de 2026, os royalties distribuídos no país "
                    "alcançaram R$ 36,5 bilhões.</p>"
                    "<p>O IBP seguirá aberto ao diálogo com o governo e o Congresso.</p>"
                ),
            }
        }
    }
}

JOTA_HTML = """
<html><head>
  <meta property="og:title" content="Imposto de Exportação pune quem produz">
  <meta name="description" content="Cobrança ameaça competitividade">
</head><body>
  <div id="__next"><p>Carregando…</p></div>
  <script id="__NEXT_DATA__" type="application/json">%s</script>
</body></html>
""" % json.dumps(_JOTA_NEXT_DATA)


def test_jota_body_comes_out_of_the_next_data_island():
    title, paragraphs = _extract(JOTA_HTML, "www.jota.info")
    assert title == "Imposto de Exportação pune quem produz"
    assert len(paragraphs) == 3
    assert paragraphs[0].startswith("A manutenção da cobrança de 12%")


def test_jota_keyword_lives_only_in_the_body():
    """The reason this extractor has to exist.

    The title carries no tracked keyword; "petróleo" appears only in the body,
    so without the island the article can never be rescued on its lede.
    """
    title, paragraphs = _extract(JOTA_HTML, "www.jota.info")
    assert "petróleo" not in title.lower()
    assert "petróleo" in " ".join(paragraphs).lower()


def test_jota_degrades_quietly_when_the_island_is_absent():
    """A JOTA PRO page or a future rewrite must not raise — just yield nothing."""
    title, paragraphs = _extract(
        '<html><head><meta property="og:title" content="X"></head>'
        "<body><div>no island here</div></body></html>",
        "www.jota.info",
    )
    assert title == "X"
    assert paragraphs == []


def test_jota_survives_unparseable_island():
    title, paragraphs = _extract(
        '<html><head><meta property="og:title" content="X"></head><body>'
        '<script id="__NEXT_DATA__" type="application/json">{not json</script>'
        "</body></html>",
        "www.jota.info",
    )
    assert title == "X"
    assert paragraphs == []


# --- Agencia iNFRA ----------------------------------------------------------

INFRA_HTML = """
<html><head>
  <meta property="og:title" content="Vibra lança diesel para setor de mineração">
</head><body>
  <div class="elementor-element elementor-element-3f725a70 elementor-widget elementor-widget-theme-post-content">
    <div class="elementor-widget-container">
      <p>Maior distribuidora de combustíveis do país, a Vibra lançou um diesel aditivado.</p>
      <p>O produto foi testado na operação da Vale, no Pará.</p>
      <p>A Vibra descreve o Supera Plus como o primeiro diesel premium para minas.</p>
    </div>
  </div>
  <article class="elementor-post"><h3>Outra matéria</h3></article>
  <article class="elementor-post"><h3>Mais uma</h3></article>
</body></html>
"""


def test_agenciainfra_body_comes_from_the_elementor_widget():
    title, paragraphs = _extract(INFRA_HTML, "agenciainfra.com")
    assert title == "Vibra lança diesel para setor de mineração"
    # The two <article> cards must not win, and must not contribute paragraphs.
    assert len(paragraphs) == 3
    assert paragraphs[0].startswith("Maior distribuidora de combustíveis")
    assert "Outra matéria" not in " ".join(paragraphs)


def test_agenciainfra_legacy_layout_still_works():
    """The pre-Elementor selectors stay as fallbacks, not replaced."""
    legacy = """
    <html><head><meta property="og:title" content="Antiga"></head><body>
      <div class="entry-content"><p>Corpo no layout antigo.</p></div>
    </body></html>
    """
    title, paragraphs = _extract(legacy, "agenciainfra.com")
    assert title == "Antiga"
    assert paragraphs == ["Corpo no layout antigo."]
