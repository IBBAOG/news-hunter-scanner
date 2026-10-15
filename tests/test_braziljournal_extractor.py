"""Brazil Journal extractor after the Nuxt migration (seen 2026-10-15).

braziljournal.com moved from WordPress to Nuxt (Vue SSR, Tailwind, scoped
`data-v-<hash>` attributes). The WordPress containers (`div.post-content-text`,
`section.post-content`, `div.entry-content`) are gone, and the `article`
fallback now matches related-post teaser cards that hold no `<p>`, so the
extractor returned zero paragraphs. The body is server-rendered in exactly one
`div.post-body` holding one `<p><span>` per paragraph.

Mirrors the contract in `src/lib/clipping/sources.ts` of the dashboard repo.
"""
from __future__ import annotations

import pytest

from news_hunter._clipinator_shim import _extract

PARA_1 = (
    "A Petrobras anunciou nesta quarta-feira um novo plano de investimentos "
    "para o segmento de exploração e produção na Margem Equatorial."
)
PARA_2 = "O gás."
PARA_3 = (
    "Segundo a companhia, os recursos serão direcionados prioritariamente a "
    "projetos de águas profundas com retorno acima do custo de capital."
)
TEASER = "Teaser"
FOOTER = "© 2016-2026 Brazil Journal - Todos os direitos reservados"

NUXT_PAGE = (
    "<html><body>"
    '<div id="__nuxt">'
    '<article class="boxarticle boxarticle--row"><h3>' + TEASER + "</h3></article>"
    '<div class="post-body flow-root" data-font-scale="normal">'
    f"<p><span>{PARA_1}</span></p>"
    f"<p><span>{PARA_2}</span></p>"
    f"<p><span>{PARA_3}</span></p>"
    "</div>"
    f"<footer><p>{FOOTER}</p></footer>"
    "</div>"
    "</body></html>"
)


@pytest.mark.parametrize("host", ["braziljournal.com", "www.braziljournal.com"])
def test_nuxt_body_is_extracted_from_post_body(host: str) -> None:
    _, paragraphs = _extract(NUXT_PAGE, host)
    assert paragraphs == [PARA_1, PARA_2, PARA_3]


@pytest.mark.parametrize("host", ["braziljournal.com", "www.braziljournal.com"])
def test_nuxt_teaser_and_footer_are_not_body(host: str) -> None:
    _, paragraphs = _extract(NUXT_PAGE, host)
    joined = "\n".join(paragraphs)
    assert TEASER not in joined
    assert "Todos os direitos reservados" not in joined
