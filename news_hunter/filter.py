"""Matching de keywords + janela temporal."""
from __future__ import annotations

import re
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


@lru_cache(maxsize=4)
def _compile_matcher(
    keywords_key: tuple[str, ...],
    exact_keywords_key: tuple[str, ...] = (),
) -> tuple[
    re.Pattern[str] | None,
    re.Pattern[str] | None,
    dict[str, str],
]:
    """Compile two regex patterns (substring + exact) and a lowercased->original map.

    IMPORTANT — no NFD/diacritic stripping here or in the match phase.
    Accents are meaningful in Portuguese: 'Irã' must NOT match 'diretoria'
    (which would happen if both were stripped to 'ira').  Case folding is
    handled by re.IGNORECASE so 'petróleo' matches 'PETRÓLEO' but not
    'petroleo' (a separate keyword entry for that variant).

    Returns:
        (substring_pat, exact_pat, lower_to_original)

    - substring_pat: plain alternation (no \b) for match_type='substring'.
    - exact_pat: \b-bounded alternation for match_type='exact'.
    - lower_to_original: maps keyword.lower() → original user-supplied form
      for label reconstruction after a match.
    """
    lower_to_original: dict[str, str] = {}
    for k in keywords_key:
        lower_to_original.setdefault(k.lower(), k)

    exact_lower: set[str] = {k.lower() for k in exact_keywords_key if k}
    sub_only: list[str] = [
        lo for lo in lower_to_original if lo and lo not in exact_lower
    ]
    ex_only: list[str] = [lo for lo in exact_lower if lo]

    sub_pat: re.Pattern[str] | None = None
    if sub_only:
        # Sort longest-first so 'gasolina' takes priority over 'gas' in
        # an alternation (leftmost-longest in Python re).
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

    return sub_pat, ex_pat, lower_to_original


def matches_keywords(
    text: str,
    keywords: list[str],
    exact_keywords: set[str] | None = None,
) -> list[str]:
    """Return list of keywords matching the text (empty if none match).

    `keywords`        — full list (substring + exact combined).
    `exact_keywords`  — subset of `keywords` that must match as whole words
                        (\b{kw}\b, case-insensitive).  The rest use plain
                        substring case-insensitive matching.

    Accent preservation (critical for Portuguese):
    - 'Irã' (substring) matches only text that contains 'irã' / 'IRÃ',
      NOT 'diretoria' (which the old NFD-strip approach would hit).
    - 'petroleo' (no accent) matches only 'petroleo', not 'petróleo'.
      The default-keywords table ships BOTH variants intentionally.
    - Case is folded via re.IGNORECASE; diacritics are preserved as-is.

    Compatibility: callers that omit `exact_keywords` get all-substring
    behaviour (same as before the match_type feature shipped).
    """
    if not text:
        return []
    pat_sub, pat_exact, lower_to_original = _compile_matcher(
        tuple(keywords),
        tuple(sorted(exact_keywords)) if exact_keywords else (),
    )
    # Match directly on the original text — NO NFD stripping.
    # re.IGNORECASE handles case folding without destroying diacritics.
    hits: list[str] = []
    if pat_sub is not None:
        hits.extend(pat_sub.findall(text))
    if pat_exact is not None:
        hits.extend(pat_exact.findall(text))
    if not hits:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for h in hits:
        orig = lower_to_original.get(h.lower(), h)
        if orig not in seen:
            seen.add(orig)
            out.append(orig)
    return out


def within_window(published_at: datetime | None, hours: int) -> bool:
    """True if published_at is within the given window (default: 24 h)."""
    if published_at is None:
        return False
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - published_at) <= timedelta(hours=hours)
