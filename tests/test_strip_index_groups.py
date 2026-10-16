"""Label-led index groups and content-feed cards (2026-10-16 Eixos).

Eixos newsletter edition pages open with a "NESTA EDIÇÃO." label followed by
short teasers of the edition, and embed "you read here" cards of other
articles (`div.wp-block-seox-content-feeds`) whose kicker leaked into the
body. Same three index-group shapes as the TS side (src/lib/clipping).
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from news_hunter._clipinator_shim import (
    INDEX_GROUP_MAX_P,
    INDEX_TEASER_MAX_CHARS,
    _collapsed_text,
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


def _group(*inner: str) -> str:
    return '<div class="wp-block-group">' + "".join(inner) + "</div>" + f"<p>{REAL}</p>"


def test_a_wrapper_with_too_many_direct_paragraphs_loses_only_the_label() -> None:
    teasers = [f"<p>Teaser {i}.</p>" for i in range(INDEX_GROUP_MAX_P)]
    assert 1 + len(teasers) > INDEX_GROUP_MAX_P
    paragraphs = _body(_group(f"<p>{LABEL}</p>", *teasers))
    assert LABEL not in paragraphs
    assert "Teaser 0." in paragraphs and REAL in paragraphs


def test_eight_direct_paragraphs_is_still_a_group() -> None:
    teasers = [f"<p>Teaser {i}.</p>" for i in range(INDEX_GROUP_MAX_P - 1)]
    assert _body(_group(f"<p>{LABEL}</p>", *teasers)) == [REAL]


def test_a_long_nested_paragraph_keeps_the_group() -> None:
    paragraphs = _body(_group(f"<p>{LABEL}</p>", "<p>Teaser um.</p>", "<p>Teaser dois.</p>",
                              f"<div><p>{LONG_SIBLING}</p></div>"))
    assert LABEL not in paragraphs
    assert LONG_SIBLING in paragraphs and "Teaser um." in paragraphs


def test_label_plus_one_teaser_loses_only_the_label() -> None:
    paragraphs = _body(_group(f"<p>{LABEL}</p>", "<p>Teaser um.</p>"))
    assert paragraphs == ["Teaser um.", REAL]


def test_a_label_split_across_inline_tags_still_matches() -> None:
    soup = BeautifulSoup("<p><strong>NESTA EDI</strong>ÇÃO. Resumo</p>", "html.parser")
    assert _collapsed_text(soup.p) == "NESTA EDIÇÃO. Resumo"
    paragraphs = _body(_group("<p><strong>NESTA EDI</strong>ÇÃO. Resumo</p>",
                              "<p>Teaser um.</p>", "<p>Teaser dois.</p>"))
    assert paragraphs == [REAL]
