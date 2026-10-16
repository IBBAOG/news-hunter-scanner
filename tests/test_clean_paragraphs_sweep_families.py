"""Boilerplate families from the dashboard's full-registry sweep (2026-10-16).

Round 7 of the coverage harness (109 domains, 322 urls) found 104 `ok` bodies
with boilerplate in 42 domains, in 12 families. These tests mirror the
contract shared with `src/lib/clipping` in the dashboard repo: the short-CTA
list (paragraphs under SHORT_NOISE_MAX_CHARS only), the long-form noise
patterns, glyph-led short lines and the new noise classes. Every family has
negative controls - real prose that must survive.
"""
from __future__ import annotations

import pytest

from news_hunter._clipinator_shim import (
    GLYPH_LED_MAX_CHARS,
    NOISE_CLASS_SUBSTRINGS,
    SHORT_NOISE_MAX_CHARS,
    _extract,
    clean_paragraphs,
)

PROSE = (
    "A Petrobras anunciou nesta quinta-feira a contratação de duas novas "
    "plataformas para o campo de Búzios, na Bacia de Santos."
)

SHORT_POSITIVES = [
    "SUBSCRIBE HERE",
    "Receba gratuitamente as newsletters do Money Times",
    "Clique AQUI, entre no canal do WhatsApp da Visão Agro e receba notícias em tempo real.",
    "Receba notícias do Metrópoles no seu Telegram e fique por dentro de tudo!",
    "In:",
    "acessibilidade:",
    "Advt",
    "(Foto: Reprodução)",
    "Foto do destaque: Pedro França/Agência Senado",
    "© 2026 CBS Interactive Inc. All Rights Reserved.",
    "(c) Copyright Thomson Reuters 2026.",
    "(c) 2026 Example Media",
    "Sign up.",
    "Sign up!",
    "Published in São Paulo, October 16th, 2026.",
    "Ship & Bunker News Team To contact the editor responsible for this story email us at [email protected]",
    "Published in Dawn, October 16th, 2026",
    "Leave a comment",
    "Back to top button",
    "watch now",
    "More Top Reads From Oilprice.com",
    "What I Cover",
    "Por",
    "Stay in the know",
    "Join Now",
    "Fetching latest articles",
    "More Info",
    "Disclaimer",
    "Fique por dentro das notícias que importam para você!",
    "Fique informado com a RedeTV!",
    "Ponha o jornal entre suas fontes preferidas.",
    "See more ABC coverage in your search results",
    "Subscribe to gift this article",
    "Gift articles to anyone you choose each month when you subscribe.",
    "This is a Mint Premium article gifted to you.Subscribe to enjoy similar stories.",
    "Sign up for gCaptain’s newsletter and never miss an update",
    "Da redaçãoi Da redação https://istoedinheiro.com.br/autor/da-redacao",
]

LONG_POSITIVES = [
    "This story was translated from the original Portuguese with the assistance of "
    "artificial intelligence and reviewed by The AgriBiz editorial team before "
    "publication, following our editorial standards.",
    "O autor da mensagem, e não o UOL, é o responsável pelo comentário. Leia as Regras "
    "de Uso do UOL.",
    "To display this content from YouTube, you must enable advertisement tracking and "
    "audience measurement.",
    "One of your browser extensions seems to be blocking the video player from loading. "
    "To watch this content, you may need to disable it on this site.",
    "A cobertura no Google, no e-mail e nas redes.",
]

NEGATIVES = [
    "A disputa entre o Telegram e o STF continua.",
    "O acordo entre a Meta e o WhatsApp foi fechado.",
    "More info on the field will be released Friday.",
    "Watch now as Brent crosses $100.",
    "▶ Ele disse: “vamos rever.”",
    "Investors may subscribe to the offering until Friday.",
    "Brazil will become a member of OPEC+ in January.",
    "(c) a produção de 2025 será revista pela ANP.",
    "Telegram foi bloqueado pelo STF.",
    "A Meta, dona do WhatsApp, anunciou a mudança.",
    "É preciso que o governo acompanhe o canal de Suez de perto.",
    "Signup bonuses for drillers rose 10% in 2025.",
    "Disclaimer of liability was rejected by the court.",
    "▶ A Petrobras elevou o diesel em 5%.",
    "O governo publicou o decreto na sexta-feira (14).",
    "Resultado: a produção caiu 3% no trimestre, segundo a ANP, com o foco na manutenção.",
    "Por que a Petrobras mudou o plano?",
    PROSE,
]

LONG_SUBSCRIBE_PROSE = (
    "The company said the number of customers who subscribe to its premium freight "
    "data service rose 12% in the quarter, driven by tanker owners and commodity "
    "traders tracking the Red Sea diversions and the new Brazilian export routes."
)


@pytest.mark.parametrize("text", SHORT_POSITIVES)
def test_short_chrome_is_dropped(text: str) -> None:
    assert len(text) < SHORT_NOISE_MAX_CHARS
    assert clean_paragraphs([PROSE, text]) == [PROSE]


@pytest.mark.parametrize("text", LONG_POSITIVES)
def test_long_form_chrome_is_dropped(text: str) -> None:
    assert clean_paragraphs([PROSE, text]) == [PROSE]


def test_the_ai_translation_note_is_dropped_at_any_length() -> None:
    text = LONG_POSITIVES[0]
    assert len(text) >= SHORT_NOISE_MAX_CHARS
    assert clean_paragraphs([text]) == []


@pytest.mark.parametrize("text", NEGATIVES)
def test_prose_survives(text: str) -> None:
    assert clean_paragraphs([text]) == [text]


def test_a_long_sentence_mentioning_subscribe_survives() -> None:
    assert len(LONG_SUBSCRIBE_PROSE) >= SHORT_NOISE_MAX_CHARS
    assert clean_paragraphs([LONG_SUBSCRIBE_PROSE]) == [LONG_SUBSCRIBE_PROSE]


@pytest.mark.parametrize("text", [
    "🔷 Canal 562 ClaroTV+",
    "🔷 TV SINAL ABERTO: parabólicas canal 562",
    "▶ Assista ao vídeo",
    "☎ Central do assinante",
])
def test_glyph_led_short_lines_are_dropped(text: str) -> None:
    assert len(text) < GLYPH_LED_MAX_CHARS
    assert clean_paragraphs([PROSE, text]) == [PROSE]


def test_a_glyph_inside_prose_survives() -> None:
    text = "A Petrobras (▲ 2%) subiu na abertura do pregão desta quinta-feira."
    assert clean_paragraphs([text]) == [text]


def test_a_long_glyph_led_paragraph_survives() -> None:
    text = "▶ " + PROSE + " " + PROSE
    assert len(text) >= GLYPH_LED_MAX_CHARS
    assert clean_paragraphs([text]) == [text]


NEW_CLASSES = (
    "author-bio", "about-the-author", "author-info", "author-card",
    "contributor-bio", "more-reads", "top-reads", "back-to-top", "disqus",
    "comment-form", "comment-policy", "consent", "video-placeholder",
    "embed-placeholder", "copyright", "eyebrow",
)


def test_the_new_noise_classes_are_registered() -> None:
    assert set(NEW_CLASSES) <= set(NOISE_CLASS_SUBSTRINGS)
    for forbidden in ("comment", "teaser", "kicker", "author"):
        assert forbidden not in NOISE_CLASS_SUBSTRINGS


@pytest.mark.parametrize("cls", NEW_CLASSES)
def test_a_new_noise_class_block_is_stripped_from_the_dom(cls: str) -> None:
    html = (
        "<html><head>"
        '<meta property="og:title" content="Petrobras contrata plataformas">'
        "</head><body><article>"
        f"<p>{PROSE}</p>"
        f'<div class="{cls}"><p>Texto genérico que nenhum padrão de texto conhece</p></div>'
        "</article></body></html>"
    )
    _title, paragraphs = _extract(html, "eixos.com.br")
    assert paragraphs == [PROSE]
