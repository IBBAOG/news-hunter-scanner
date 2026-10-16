"""Label-led index groups and content-feed cards (2026-10-16 Eixos).

Eixos newsletter edition pages open with a "NESTA EDIÇÃO." label followed by
short teasers of the edition, and embed "you read here" cards of other
articles (`div.wp-block-seox-content-feeds`) whose kicker leaked into the
body. Same three index-group shapes as the TS side (src/lib/clipping).
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from news_hunter._clipinator_shim import (
    INDEX_TEASER_MAX_CHARS,
    NOISE_CLASS_SUBSTRINGS,
    _paragraphs_from,
)

REAL = (
    "A Petrobras anunciou nesta quinta-feira a contratação de duas novas "
    "plataformas para o campo de Búzios, na Bacia de Santos, com entrada em "
    "operação prevista para 2029."
)
LONG_SIBLING = (
    "A ANP aprovou nesta semana a revisão do plano de desenvolvimento do campo, "
    "que passa a prever a perfuração de doze poços produtores e oito injetores, "
    "além de um novo gasoduto de escoamento até a costa paulista em 2030."
)
LABEL = "NESTA EDIÇÃO. Resumo da semana"


def _body(inner: str) -> list[str]:
    soup = BeautifulSoup(f"<html><body><article>{inner}</article></body></html>", "html.parser")
    return _paragraphs_from(soup.find("article"))


def test_lengths_of_the_fixtures() -> None:
    assert len(REAL) > INDEX_TEASER_MAX_CHARS
    assert len(LONG_SIBLING) >= 200


def test_a_label_led_group_of_teasers_is_removed_whole() -> None:
    paragraphs = _body(
        f'<div class="wp-block-group"><p>{LABEL}</p><p>Teaser um.</p><p>Teaser dois.</p></div>'
        f"<p>{REAL}</p>"
    )
    assert paragraphs == [REAL]


def test_a_group_with_a_real_paragraph_loses_only_the_label() -> None:
    paragraphs = _body(
        f'<div class="wp-block-group"><p>{LABEL}</p><p>Teaser um.</p><p>{LONG_SIBLING}</p></div>'
        f"<p>{REAL}</p>"
    )
    assert LABEL not in paragraphs
    assert LONG_SIBLING in paragraphs and REAL in paragraphs


def test_a_label_directly_in_the_container_loses_only_the_label() -> None:
    paragraphs = _body(f"<p>{LABEL}</p><p>Teaser um.</p><p>{REAL}</p>")
    assert LABEL not in paragraphs
    assert REAL in paragraphs


def test_prose_starting_with_nesta_edicao_is_not_a_label() -> None:
    prose = "Nesta edição do relatório anual, a ANP revisou as reservas provadas do país."
    assert _body(f'<div class="wp-block-group"><p>{prose}</p><p>Teaser.</p></div>') == [prose, "Teaser."]


def test_content_feed_cards_are_stripped() -> None:
    assert "content-feeds" in NOISE_CLASS_SUBSTRINGS
    paragraphs = _body(
        f"<p>{REAL}</p>"
        '<div class="wp-block-seox-content-feeds"><article class="feed">'
        '<p class="feed-hat">diálogos da transição</p>'
        "<p>Título de outra matéria do portal</p></article></div>"
    )
    assert paragraphs == [REAL]
