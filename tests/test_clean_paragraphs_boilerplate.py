"""Body boilerplate constraints in the clipping shim (2026-10-14 O Globo AMP).

The AMP mirror of an O Globo blog post served, inside `<article>`, a
`content-publication-data__updated` date line, the granted preview, and 18
`div.amp-barreira[subscriptions-actions]` blocks inside the not-granted section,
each with the two login CTAs "Já possui conta? Faça Login" / "Quer acessar a
conta da sua assinatura? Login aqui". The extractor kept all of it and the
clipping email rendered the barrier as the body.

These tests mirror the contract shared with `src/lib/clipping` in the dashboard
repo: AMP subscription-gate selectors, login-CTA noise patterns, the date-only
paragraph drop and the global keep-first dedupe. Each positive is paired with a
negative control that must survive.
"""
from __future__ import annotations

import pytest

from news_hunter._clipinator_shim import (
    _extract,
    clean_paragraphs,
    is_date_only_paragraph,
)

PREVIEW = (
    "A Lazard reduziu sua participação na Vibra Energia, segundo comunicado "
    "enviado ao mercado nesta segunda-feira pela distribuidora de combustíveis."
)
LOGIN_A = "Já possui conta? Faça Login"
LOGIN_B = "Quer acessar a conta da sua assinatura? Login aqui"

_BARRIER = (
    '<div class="amp-barreira" subscriptions-actions>'
    f'<p class="paywall-amp-section2-login">{LOGIN_A}</p>'
    f'<p class="paywall-amp-section2-login">{LOGIN_B}</p>'
    "</div>"
)

GLOBO_AMP_PAGE = (
    "<html amp><head>"
    '<meta property="og:title" content="Lazard diminui participação na Vibra">'
    "</head><body><article>"
    '<p class="content-publication-data__updated">13/10/2026 09h01 Atualizado 13/10/2026</p>'
    f'<section subscriptions-section="content"><p>{PREVIEW}</p></section>'
    '<section subscriptions-section="content-not-granted">'
    f"{_BARRIER}{_BARRIER}"
    "</section>"
    "</article></body></html>"
)


def test_globo_amp_page_yields_only_the_preview() -> None:
    title, paragraphs = _extract(GLOBO_AMP_PAGE, "oglobo.globo.com")
    assert paragraphs == [PREVIEW]
    assert title == "Lazard diminui participação na Vibra"


def test_clean_paragraphs_dedupes_globally_keep_first() -> None:
    a = "Primeiro parágrafo real da matéria sobre a Vibra."
    b = "Segundo parágrafo real da matéria sobre a Lazard."
    assert clean_paragraphs([a, b, a, b]) == [a, b]


def test_dedupe_ignores_case_spacing_and_trailing_punctuation() -> None:
    a = "Primeiro parágrafo real da matéria."
    assert clean_paragraphs([a, "primeiro  parágrafo real da matéria!"]) == [a]


@pytest.mark.parametrize("text", [
    "Já possui conta?",
    "Faça seu login",
    "Clique e faça login.",
    "Login aqui",
    "Quer acessar a conta da sua assinatura?",
    "Cadastre-se gratuitamente",
    "Crie sua conta para continuar",
    "Esqueci minha senha",
    "Entrar",
    "Already have an account? Sign in",
    "Create a free account to keep reading",
    "¿Ya tienes una cuenta? Inicia sesión",
    "Inicia sesión para continuar",
    "Conteúdo exclusivo para assinantes",
])
def test_login_cta_patterns_are_dropped(text: str) -> None:
    keep = "Parágrafo real que precisa sobreviver à limpeza do corpo."
    assert clean_paragraphs([keep, text]) == [keep]


@pytest.mark.parametrize("text", [
    "Quem já possui conta no banco poderá migrar o saldo.",
    "Para acessar o sistema, faça login com sua conta gov.br e siga as instruções.",
])
def test_negative_controls_survive(text: str) -> None:
    assert clean_paragraphs([text]) == [text]


@pytest.mark.parametrize("text", [
    "13/10/2026 09h01 Atualizado 13/10/2026",
    "Publicado em 13 de outubro de 2026",
    "2026-09-02",
])
def test_date_only_positives(text: str) -> None:
    assert is_date_only_paragraph(text)
    assert clean_paragraphs([text]) == []


@pytest.mark.parametrize("text", [
    "13/10/2026 — sessão plenária às 10h",
    "A produção de 2026 superou a de 2025.",
])
def test_date_only_negatives(text: str) -> None:
    assert not is_date_only_paragraph(text)
    assert clean_paragraphs([text]) == [text]
