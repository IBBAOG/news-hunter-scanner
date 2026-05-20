"""Matching de keywords + janela temporal."""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from functools import lru_cache


# Marcadores de blocos de "matérias relacionadas" dentro de summaries RSS.
# Cortamos tudo apartir deles para evitar que o título de uma recomendação
# externa (ex.: "... posto de combustíveis") gere falso positivo.
_RELATED_MARKERS = re.compile(
    r"\b(?:leia\s+tamb[eé]m|veja\s+tamb[eé]m|saiba\s+mais|leia\s+mais|"
    r"veja\s+mais|confira\s+tamb[eé]m|mais\s+not[ií]cias|"
    r"not[ií]cias\s+relacionadas|mat[eé]rias\s+relacionadas)\b",
    re.IGNORECASE,
)

# Boilerplate "O post X apareceu primeiro em Y." injetado pelo WordPress em
# RSS feeds. Quando o nome do site contem termos do vocabulario O&G
# (ex.: "CPG Click Petróleo e Gás"), todo artigo casa "petróleo"+"gás"
# no filtro de keyword, mesmo sendo sobre NASA/celular/carro elétrico.
# Usamos [\s\S] em vez de . porque o input pode vir como HTML cru
# (<a href="..."> contem dots dentro da URL que quebrariam [^.]+).
_WP_CROSSPOST_FOOTER = re.compile(
    r"O\s+post\s+[\s\S]+?apareceu\s+primeiro\s+em\s+[\s\S]+?</a>\s*\.",
    re.IGNORECASE,
)
# Variante para texto ja sem HTML (snippet limpo): termina no primeiro ponto.
_WP_CROSSPOST_FOOTER_PLAIN = re.compile(
    r"O\s+post\s+.+?\s+apareceu\s+primeiro\s+em\s+[^.<]+\.",
    re.IGNORECASE | re.DOTALL,
)
# Tags HTML — stripadas antes do regex PLAIN para lidar com <a> aninhado.
_HTML_TAG = re.compile(r"<[^>]+>")


def strip_wp_footer(text: str) -> str:
    """Remove 'O post X apareceu primeiro em Y.' (WordPress crosspost).

    Tenta primeiro o padrao HTML (com </a>); se nao casar, strippa tags e
    tenta a variante plain. Robusto a input HTML ou texto ja limpo.
    """
    if not text:
        return text
    cleaned = _WP_CROSSPOST_FOOTER.sub(" ", text)
    if cleaned != text:
        return cleaned.strip()
    no_tags = _HTML_TAG.sub(" ", text)
    return _WP_CROSSPOST_FOOTER_PLAIN.sub(" ", no_tags).strip()


def strip_related(text: str) -> str:
    """Remove blocos 'LEIA TAMBÉM' / 'VEJA MAIS' + boilerplate WordPress."""
    if not text:
        return text
    text = strip_wp_footer(text)
    m = _RELATED_MARKERS.search(text)
    if m:
        return text[: m.start()]
    return text


def _normalize(s: str) -> str:
    """Lowercase + remove acentos para comparacao."""
    nfkd = unicodedata.normalize("NFKD", s)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower()


@lru_cache(maxsize=4)
def _compile_matcher(
    keywords_key: tuple[str, ...],
    exact_keywords_key: tuple[str, ...] = (),
) -> tuple[
    re.Pattern[str] | None,
    re.Pattern[str] | None,
    dict[str, str],
]:
    """Compila dois regex (substring + exact) + mapa normalizado->original.

    Returns:
      (substring_pat, exact_pat, normalized_to_original)

    - substring_pat: alternation SEM \\b — para keywords match_type='substring'
      (default ate o opt-in da feature 2026-05-20).
    - exact_pat: alternation COM \\b{kw}\\b — para keywords match_type='exact',
      casa apenas palavra inteira (case-insensitive).
    - normalized_to_original: dict {form_normalizada: form_original_user_input}
      para reconstruir o label original ao reportar matches.

    Cacheado por tupla de keywords; chamado ~5000x por busca.
    """
    normalized_to_original: dict[str, str] = {}
    for k in keywords_key:
        normalized_to_original.setdefault(_normalize(k), k)

    exact_normalized: set[str] = {_normalize(k) for k in exact_keywords_key if k}
    sub_only: list[str] = [
        n for n in normalized_to_original if n and n not in exact_normalized
    ]
    ex_only: list[str] = [n for n in exact_normalized if n]

    sub_pat: re.Pattern[str] | None = None
    if sub_only:
        # Sort por comprimento desc — alternation eh leftmost-longest; sem
        # ordenacao, "gas" casaria antes de "gasolina" em hay="gasolina".
        sub_only.sort(key=len, reverse=True)
        sub_pat = re.compile(
            "(?:" + "|".join(re.escape(k) for k in sub_only) + ")",
            re.IGNORECASE,
        )

    ex_pat: re.Pattern[str] | None = None
    if ex_only:
        ex_only.sort(key=len, reverse=True)
        ex_pat = re.compile(
            rf"\b(?:{'|'.join(re.escape(k) for k in ex_only)})\b",
            re.IGNORECASE,
        )

    return sub_pat, ex_pat, normalized_to_original


def matches_keywords(
    text: str,
    keywords: list[str],
    exact_keywords: set[str] | None = None,
) -> list[str]:
    """Retorna a lista de keywords que casam com o texto (vazia se nenhuma).

    `keywords`         — lista completa (substring + exact).
    `exact_keywords`   — subset de `keywords` que devem casar apenas como
                         palavra inteira (\\b{kw}\\b case-insensitive). As demais
                         seguem regra de substring case-insensitive.

    Casos especiais:
    - "ANS" como substring casa "trANSporte" (compat 2026-05-20+).
    - "ANS" como exact casa "ANS divulga relatorio" mas NAO "trANSporte".
    - "saude suplementar" multi-token como exact: a alternation contem o
      espaco interno como literal; `\\b` casa entre word-char e non-word-char
      nas pontas; texto "Agencia Nacional de Saude Suplementar" matcha.
    - "pre-sal" com hifen como exact: hifen e non-word; texto "pre-sal" casa,
      "pre sal" (sem hifen) NAO casa — semantica intencional: "este token
      exato, com esta pontuacao".

    Compat: chamadas sem `exact_keywords` (codigo antigo) caem na branch
    substring para todas as keys.
    """
    if not text:
        return []
    pat_sub, pat_exact, normalized_to_original = _compile_matcher(
        tuple(keywords),
        tuple(sorted(exact_keywords)) if exact_keywords else (),
    )
    hay = _normalize(text)
    hits: list[str] = []
    if pat_sub is not None:
        hits.extend(pat_sub.findall(hay))
    if pat_exact is not None:
        hits.extend(pat_exact.findall(hay))
    if not hits:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for h in hits:
        orig = normalized_to_original.get(h, h)
        if orig not in seen:
            seen.add(orig)
            out.append(orig)
    return out


def within_window(published_at: datetime | None, hours: int) -> bool:
    """True se published_at esta dentro da janela (default: 24h)."""
    if published_at is None:
        return False
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - published_at) <= timedelta(hours=hours)
