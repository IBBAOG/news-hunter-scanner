"""Guard the EXTRACTORS host resolution and the JSON-LD body fallback.

Two failures were shipping together, and both were silent by construction:

1. **Host-variant misses.** EXTRACTORS is keyed per literal host, and the caller
   in `enrich.py` gated on `domain in EXTRACTORS`. `m.yicai.com` was therefore
   an unknown outlet even with `yicai.com` registered - no error, no log line,
   just a fall-through to the meta description, which is a standfirst dressed as
   a body. The fix normalises the host, and it has to be applied at the GATE as
   well as inside `_extract`: a fallback that only lives in `_extract` never runs
   because the gate rejects the variant first.

2. **Thirty foreign-language domains registered in SOURCE_NAMES only.** Every
   ar / ru / zh / iw / es outlet added in waves B1-d and B2a/b had a display name
   and no extractor, so all of them took the same silent fall-through.

The assertions below are two-sided on purpose: the resolver must still REFUSE an
unregistered host, and the JSON-LD fallback must NOT fire when the selectors
already found prose - a fallback that always runs would mask a broken extractor
rather than compensate for a missing body.
"""
from news_hunter._clipinator_shim import (
    EXTRACTORS,
    SOURCE_NAMES,
    _extract,
    ex_auto,
    resolve_extractor_domain,
)

import pytest


# ── Host resolution ──────────────────────────────────────────────────────────
def test_exact_host_resolves_to_itself():
    assert resolve_extractor_domain("valor.globo.com") == "valor.globo.com"


@pytest.mark.parametrize(
    "host,expected",
    [
        ("m.yicai.com", "m.yicai.com"),        # now registered outright
        ("amp.yicai.com", "yicai.com"),        # prefix stripped
        ("mobile.yicai.com", "yicai.com"),
        ("m.exame.com", "exame.com"),          # apex form registered
        # cnbc is registered ONLY as www., so this exercises the third step -
        # strip the prefix, then re-add www. Without it the whole www.-only
        # slice of the registry (cnbc, cnn, theedgesingapore) stays unreachable
        # from any mobile or AMP host.
        ("m.cnbc.com", "www.cnbc.com"),
    ],
)
def test_host_variants_resolve_to_a_registered_key(host, expected):
    assert resolve_extractor_domain(host) == expected


def test_resolver_still_refuses_an_unregistered_host():
    # The guard must keep firing: a resolver that answers for everything would
    # make every unknown domain look registered and silently apply ex_auto.
    assert resolve_extractor_domain("nao-existe-mesmo.example") is None
    assert resolve_extractor_domain("m.nao-existe-mesmo.example") is None
    assert resolve_extractor_domain("") is None


def test_extract_raises_for_an_unregistered_domain():
    with pytest.raises(KeyError):
        _extract("<html><body><p>x</p></body></html>", "nao-existe-mesmo.example")


# ── Foreign-language registry parity ─────────────────────────────────────────
FOREIGN_DOMAINS = [
    "attaqa.net", "www.attaqa.net",
    "asharqbusiness.com", "www.asharqbusiness.com",
    "alarabiya.net", "www.alarabiya.net",
    "neftegaz.ru", "www.neftegaz.ru",
    "oilcapital.ru", "www.oilcapital.ru",
    "eprussia.ru", "www.eprussia.ru",
    "yicai.com", "www.yicai.com", "m.yicai.com",
    "finance.sina.com.cn",
    "jiemian.com", "www.jiemian.com", "m.jiemian.com",
    "globes.co.il", "www.globes.co.il",
    "themarker.com", "www.themarker.com",
    "calcalist.co.il", "www.calcalist.co.il",
    "eleconomista.com.mx", "www.eleconomista.com.mx",
    "ambito.com", "www.ambito.com",
    "portafolio.co", "www.portafolio.co",
]


@pytest.mark.parametrize("domain", FOREIGN_DOMAINS)
def test_foreign_domains_have_an_extractor(domain):
    assert EXTRACTORS.get(domain) is ex_auto


def test_m_yicai_is_named():
    assert SOURCE_NAMES.get("m.yicai.com") == SOURCE_NAMES["yicai.com"]


def test_the_moscow_times_is_deliberately_extractorless():
    """Its RSS carries full bodies, so no page fetch ever reaches an extractor.

    Registering one would advertise a path that is never taken. This test exists
    so a future "let's finish the list" pass has to read that reason first.
    """
    assert "themoscowtimes.com" in SOURCE_NAMES
    assert "themoscowtimes.com" not in EXTRACTORS


def test_every_named_source_now_has_an_extractor_or_a_stated_reason():
    allowed_without = {"themoscowtimes.com", "www.themoscowtimes.com"}
    missing = {d for d in SOURCE_NAMES if d not in EXTRACTORS} - allowed_without
    assert not missing, (
        "these domains render a publication name but fall through to the meta "
        f"description on every fetch: {sorted(missing)}"
    )


# ── JSON-LD body fallback ────────────────────────────────────────────────────
_JSON_LD_ONLY = """
<html><head>
  <meta property="og:title" content="Refinaria amplia producao no trimestre">
  <script type="application/ld+json">
  {"@type":"NewsArticle","headline":"Refinaria amplia producao no trimestre",
   "articleBody":"A refinaria elevou a producao no trimestre, segundo o balanco divulgado nesta terca.\\n\\nO volume processado subiu ante o mesmo periodo do ano passado, informou a companhia em nota.\\n\\nAnalistas avaliam que o resultado sustenta a projecao anual da empresa para o segmento."}
  </script>
</head><body>
  <div class="wrapper"><article></article></div>
</body></html>
"""

_MARKUP_BODY = """
<html><head>
  <meta property="og:title" content="Titulo real">
  <script type="application/ld+json">
  {"@type":"NewsArticle","articleBody":"NAO DEVE APARECER no resultado porque os seletores acharam o corpo."}
  </script>
</head><body>
  <article>
    <p>Primeiro paragrafo do corpo servido no HTML, com tamanho suficiente para valer.</p>
    <p>Segundo paragrafo do corpo servido no HTML, tambem com tamanho suficiente.</p>
  </article>
</body></html>
"""


def test_json_ld_body_is_used_when_the_selectors_find_nothing():
    title, paragraphs = _extract(_JSON_LD_ONLY, "yicai.com")
    assert title == "Refinaria amplia producao no trimestre"
    assert len(paragraphs) == 3
    assert paragraphs[0].startswith("A refinaria elevou a producao")


def test_json_ld_body_does_not_override_a_working_extractor():
    _title, paragraphs = _extract(_MARKUP_BODY, "yicai.com")
    assert len(paragraphs) == 2
    assert all("NAO DEVE APARECER" not in p for p in paragraphs)


def test_json_ld_fallback_is_inert_without_structured_data():
    _title, paragraphs = _extract(
        "<html><head><title>t</title></head><body><div></div></body></html>",
        "yicai.com",
    )
    assert paragraphs == []
