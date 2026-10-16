"""Eixos body chrome in the clipping shim (2026-10-16).

Two Eixos clippings shipped chrome as the body: the Google "preferred sources"
banner (`div.preferred-source-text`), newsletter-edition labels and an
AI-translation disclaimer. These tests mirror the contract shared with
`src/lib/clipping` in the dashboard repo; each family has negative controls
(real prose that mentions the same words) that must survive.
"""
from __future__ import annotations

import pytest

from news_hunter._clipinator_shim import (
    NOISE_CLASS_SUBSTRINGS,
    _extract,
    clean_paragraphs,
)

PROSE = (
    "A Petrobras anunciou nesta quinta-feira a contratação de duas novas "
    "plataformas para o campo de Búzios, na Bacia de Santos."
)

POSITIVES = [
    "Coloque a eixos nas suas fontes preferidas do Google",
    "Veja nossas notícias com mais destaque nas suas buscas.",
    "Adicione o eixos às suas fontes preferidas no Google",
    "Siga a eixos no Google News",
    "Acompanhe nossas notícias pelo Google Discover",
    "NESTA EDIÇÃO. Petróleo, gás e transição energética",
    "Nesta edição: Petrobras, ANP e leilões",
    "EDIÇÃO APRESENTADA POR:",
    "Edição patrocinada por Shell",
    "Apresentado por: Petrobras",
    "Oferecida por",
    "* Conteúdo traduzido com auxílio de Inteligência Artificial, revisado e "
    "editado pela Redação do Broadcast, que se responsabiliza pelas "
    "informações divulgadas.",
    "comece seu dia",
    "Comece seu dia.",
]

NEGATIVES = [
    "O Google News mudou o algoritmo de ranqueamento em 2026, segundo a empresa.",
    "Apresentado por executivos da Petrobras, o plano prevê 10 novos poços até 2030.",
    "Com informações da Dow Jones Newswires",
    "Nesta edição do relatório anual, a ANP revisou as reservas provadas.",
    "Comece seu dia com as principais notícias do setor, disse o diretor ao lançar o produto.",
    PROSE,
]


@pytest.mark.parametrize("text", POSITIVES)
def test_eixos_chrome_is_dropped(text: str) -> None:
    assert clean_paragraphs([PROSE, text]) == [PROSE]


@pytest.mark.parametrize("text", NEGATIVES)
def test_prose_with_the_same_words_survives(text: str) -> None:
    assert clean_paragraphs([text]) == [text]


def test_preferred_source_is_a_noise_class() -> None:
    assert "preferred-source" in NOISE_CLASS_SUBSTRINGS


def test_preferred_source_banner_block_is_stripped_from_the_dom() -> None:
    # A banner line the text patterns do not know must still go, by class.
    html = (
        "<html><head>"
        '<meta property="og:title" content="Petrobras contrata plataformas">'
        "</head><body><article>"
        f"<p>{PROSE}</p>"
        '<div class="preferred-source-text"><p>Texto genérico do banner de busca</p></div>'
        "</article></body></html>"
    )
    _title, paragraphs = _extract(html, "eixos.com.br")
    assert paragraphs == [PROSE]
