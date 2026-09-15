"""Keyword sense exclusion: drop a keyword hit when the text uses the word in an
off-topic sense.

WHY THIS EXISTS. A keyword is a string, a reader's interest is a sense. The
default keyword 'Compass' is meant to follow Compass Gas e Energia (Cosan group:
Comgas, Edge, biomethane, gas release). Measured on news_articles 2026-09-15:
of 571 rows tagged 'Compass', dozens were about the Jeep Compass SUV and many
more about homonymous companies (Vinci Compass, Compass Group, Compass
Diversified, Compass Point, Compass Pathways...). Whole-word matching
(match_type 'exact') removes 'compasso'/'compassivo'/'compassion'; it cannot
tell a car or another company from the Cosan business. This module can.

HOW A HIT IS JUDGED (evaluate_keyword_sense):

  1. Find every occurrence of the keyword with the SAME match semantics the
     matcher used (\\b-bounded for 'exact', plain substring otherwise).
     Accents are preserved: no NFD stripping, only re.IGNORECASE (see filter.py).
  2. On-topic context anywhere in the document (Cosan, Comgas, GNV, biometano,
     gas release, "empresa de gas", the state gas distributors...) always wins:
     nothing is excluded.
  3. An occurrence matching a PROTECTED sense is a valid hit ('Nefte Compass',
     the Energy Intelligence oil newsletter): the keyword is kept.
  4. Each occurrence is then explained, or not, by an excluded sense:
       * occurrence patterns explain ONLY the occurrence they touch
         ('Jeep Compass', 'Renegade, Compass e Commander', 'Vinci Compass',
         'Compass Group'). "Vinci Compass e Compass divulgam balanco" keeps
         the label: the bare second 'Compass' is not explained;
       * document vocabulary explains EVERY occurrence in the text ('Jeep',
         'SUV', 'km/l' for automotive; 'psilocibina', 'mercado imobiliario',
         '$0,02' for other entities). This is the one spill-over: a bare
         'Compass' in a text that says 'SUV' anywhere is read as the car.
  5. The keyword is dropped only when it occurs at least once and every
     occurrence is explained by an excluded sense. Zero occurrences is "no
     evidence", never a drop.

The matcher (filter.matches_keywords) applies this to each hit whose keyword has
an entry in KEYWORD_SENSE_EXCLUSIONS, so every pipeline stage honours it. The
retro-purge (scripts/purge_keyword_sense_exclusions.py) imports the same
function for stored rows: one rule, two callers.

Adding a keyword: add a SenseExclusion under its lowercased form and tune the
patterns against real rows before merging. Keep vocabulary narrow and specific:
a vocabulary term removes every story that mentions it next to the keyword
(2026-09-15 QA: 'veiculos' + 'rodovias', 'montadora' and 'Stellantis' would have
dropped Compass's own natural-gas-for-vehicles news).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

# How far before an occurrence a `before` pattern may look. The longest context
# we match ('Corolla Cross, ') is ~20 chars; 80 leaves room for spacing.
_BEFORE_WINDOW = 80


@dataclass(frozen=True)
class KeywordSense:
    """One sense of a keyword, recognised by context.

    before     -- regex that must END exactly where the occurrence starts
                  (compiled with a trailing \\Z; searched in a short window).
    after      -- regex that must START exactly where the occurrence ends.
    vocabulary -- document-level terms; one hit explains every occurrence.

    For a protected sense only `before`/`after` are used: protection is an
    occurrence-level statement ("this occurrence is the newsletter").
    """

    name: str
    before: re.Pattern[str] | None = None
    after: re.Pattern[str] | None = None
    vocabulary: re.Pattern[str] | None = None


@dataclass(frozen=True)
class SenseExclusion:
    """Off-topic senses of one keyword plus the context that overrides them."""

    excluded: tuple[KeywordSense, ...]
    keep_context: re.Pattern[str] | None = None
    protected: tuple[KeywordSense, ...] = ()


@dataclass(frozen=True)
class SenseVerdict:
    """Outcome of judging one keyword against one document.

    status:
      'no_rule'        -- the keyword has no SenseExclusion entry
      'no_occurrence'  -- the keyword does not occur in the text
      'keep_context'   -- on-topic context found; nothing excluded
      'protected'      -- an occurrence is in a protected sense (e.g. nefte_compass)
      'excluded'       -- every occurrence is in an excluded sense -> drop the hit
      'no_evidence'    -- at least one occurrence is unexplained -> keep
                          (`evidence` may still list what explained the others)
    """

    keyword: str
    status: str
    occurrences: int = 0
    senses: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()
    unexplained: int = 0

    @property
    def excluded(self) -> bool:
        return self.status == "excluded"


# ---------------------------------------------------------------------------
# Pattern helpers
# ---------------------------------------------------------------------------

def _before(*alternatives: str) -> re.Pattern[str]:
    return re.compile(r"(?:" + "|".join(alternatives) + r")\Z", re.IGNORECASE)


def _terms(*alternatives: str) -> re.Pattern[str]:
    """Alternation used both as an `after` pattern (.match) and as vocabulary."""
    return re.compile(r"(?:" + "|".join(alternatives) + r")", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Compass (Compass Gas e Energia, Cosan group)
# ---------------------------------------------------------------------------

# Car models that share a list with the Jeep Compass in Brazilian car press
# ("Renegade, Compass e Commander", "contra Compass, Corolla Cross"). Case-
# sensitive on purpose: several are ordinary words in lower case.
_CAR_MODELS = (
    r"(?-i:Renegade|Commander|Avenger|Wrangler|Gladiator|Grand\s+Cherokee|Cherokee"
    r"|Corolla\s+Cross|Haval\s+H6|Tiguan|SW4|Hilux|Hillux|Taos|Tucson|Kicks|Creta"
    r"|HR-V|T-Cross|Nivus|Fastback|Tiggo(?:\s*\d+X?)?|Sportage|RAV4|Outlander|CR-V"
    r"|Duster|Jaecoo(?:\s*\d+)?|Omoda(?:\s*\d+)?|Song\s+(?:Pro|Plus))"
)
_LIST_SEP = r"(?:\s*[,/]\s*|\s+(?:e|and|ou|or|x|vs\.?|versus|contra)\s+)"

COMPASS_AUTOMOTIVE = KeywordSense(
    name="automotive",
    before=_before(
        r"\bJeep(?:®|'s|’s)?\s+",
        # 'um veículo Jeep Compass', 'veículo modelo Compass', 'um carro Compass'.
        r"\b(?:ve[íi]culo|carro|autom[óo]vel|SUV)(?:\s+(?:modelo|da\s+marca))?\s+",
        _CAR_MODELS + _LIST_SEP,
    ),
    after=_terms(
        # Trim levels (case-sensitive: 'Sport', 'Limited' are ordinary words).
        r"\s+(?-i:Longitude|Limited|Sport|S[ée]rie\s+S|Blackhawk|Overland|Trailhawk"
        r"|Upland|Altitude|Night\s+Eagle)\b",
        # Powertrains and engine codes. No bare '1.3' / '2.0': 'Compass 1.3 bi em
        # debentures' is the company.
        r"\s+(?:4xe|e-?Hybrid|T270|TD380|Hurricane|1[.,]3\s*turbo|2\.0\s*(?:turbo|diesel))\b",
        # Used-car listing year 'Compass 2021/2022'. A bare year is NOT evidence:
        # 'Compass 2026: guidance preve Ebitda...' is the company.
        r"\s+20\d\d/(?:20)?\d\d\b",
        _LIST_SEP + r"(?:Jeep\s+)?" + _CAR_MODELS + r"\b",
    ),
    # Narrow on purpose. Removed after QA 2026-09-15 because Compass sells natural
    # gas FOR vehicles: 'montadora', 'Stellantis' (a gas customer) and the weak
    # pair terms ('veiculos' + 'rodovias', 'carros' + 'motoristas').
    vocabulary=_terms(
        r"\bJeep\b", r"\bSUVs?\b", r"\btest[\s-]?drives?\b", r"\bkm/l\b",
        r"\b\d{2,3}\s?cv\b", r"\bc[âa]mbio\s+(?:autom[áa]tico|manual|CVT)\b",
        r"\bpicapes?\b",
        # 'sedã', never 'seda' (silk): accents are meaningful, see filter.py.
        r"\bsedãs?\b", r"\bsedans?\b", r"\bemplacamentos?\b", r"\bautomakers?\b",
        r"\bcarmakers?\b", r"\bhorsepower\b",
    ),
)

# Homonymous companies and products seen in news_articles rows tagged 'Compass'
# (2026-09-15). Names are case-sensitive: 'compass point' is also a noun.
COMPASS_OTHER_ENTITY = KeywordSense(
    name="other_entity",
    before=_before(
        r"(?-i:\bVinci)\s+",                # Vinci Compass (asset manager)
        r"(?-i:\bSurvey)\s+",               # ClassNK Survey Compass (AI assistant)
        r"(?-i:\bSmart\s+Reversal)\s+",     # TradingView indicator
        # Bosch Tech Compass (survey); also its url slug 'bosch-tech-compass-2026'.
        r"\bBosch[\s-]+Tech[\s-]+",
    ),
    after=_terms(
        r"\s+(?-i:Group|Diversified|Minerals|Pathways|PATHWAYS|Therapeutics|Point"
        r"|Containers|Lite)\b",
        r",?\s+(?-i:Inc)\b",               # Compass Inc. (US real-estate brokerage)
        # Ticker within the same parenthetical: 'Compass Noticias (COMP)'.
        r"[^()\n]{0,40}\(\s*(?:(?:NYSE|NASDAQ|Nasdaq|LSE)\s*:\s*)?"
        r"(?-i:COMP|CMPS|CMPX|CODI|CMP|CPG)\s*\)",
    ),
    vocabulary=_terms(
        r"\bpsilocibina\b", r"\bpsilocybin\b", r"\bCOMP360\b", r"(?-i:\bFDA\b)",
        r"\bmercado\s+imobili[áa]rio\b", r"\breal\s+estate\b", r"\bZillow\b",
        r"(?-i:\b(?:com|a|da|e)\s+Anywhere\b)", r"\bEnergyX\b",
        r"\bGreat\s+Salt\s+Lake\b",
        # US-issuer EPS template ('superou projecoes por $0,02'): a bare dollar
        # sign glued to a comma-decimal amount. Not 'R$ 0,57', 'R $ 0,57',
        # 'US$ 15', '$560 million' or 'a $62 mil'.
        r"(?<![A-Za-z$])(?<!(?-i:R) )(?<!(?-i:US) )\$\d+,\d{2}\b",
    ),
)

COMPASS_NEFTE = KeywordSense(
    name="nefte_compass",
    before=_before(r"(?-i:\bNefte)\s+"),   # Energy Intelligence oil newsletter
)

# Cosan-group context. Always wins over the excluded senses.
COMPASS_COMPANY_CONTEXT = _terms(
    # 'Comgás' without a space: 'com gás' ('with gas') is an ordinary phrase.
    r"\bCosan\b", r"\bCSAN3\b", r"\bPASS3\b", r"\bComg[áa]s\b", r"\bRa[íi]zen\b",
    r"\bCompass\s+G[áa]s\b", r"(?-i:\bCompass\s+ON\b)",
    r"\bg[áa]s\s+natural\b", r"\bnatural\s+gas\b", r"\bgas\s+distribut(?:ion|or)\b",
    r"\bbiometano\b", r"\bbiomethane\b", r"\bbiog[áa]s\b", r"\bgas\s+release\b",
    # Natural gas for vehicles: what Compass sells to fleets and ride-hailing cars.
    r"(?-i:\bGNV\b)",
    r"\b(?:distribuidora|distribui[çc][ãa]o|empresa|companhia|bra[çc]o"
    r"|comercializadora|comercializa[çc][ãa]o|concession[áa]rias?)\s+de\s+g[áa]s\b",
    r"\bRota\s+3\b", r"(?-i:\bTRSP\b)", r"\bterminal\s+de\s+regaseifica[çc][ãa]o\b",
    # Gas distributors of the group (Comgas, Necta, Sulgas, Commit's stakes) and
    # the other state distributors that show up in the same coverage.
    r"\bSulg[áa]s\b", r"(?-i:\bNecta\b)", r"\bCommit\s+G[áa]s\b",
    r"\bG[áa]s\s+Brasiliano\b", r"\bCompag[áa]s\b", r"\bMSG[áa]s\b", r"\bSCG[áa]s\b",
    r"\bCoperg[áa]s\b", r"\bGasmig\b", r"\bBahiag[áa]s\b", r"\bPotig[áa]s\b",
    r"\bPBG[áa]s\b", r"\bSerg[áa]s\b",
    # Accent required: 'algas' (seaweed) and 'cegas' (blind) are words.
    r"\bAlgás\b", r"\bCegás\b",
    r"(?-i:\bGNL\b)", r"(?-i:\bLNG\b)",
    r"\bAnt[ôo]nio\s+Sim[õo]es\b",
    # Edge, Compass's gas trading/LNG unit, written with a Portuguese article
    # ('a Edge', 'da Edge', 'Edge, da Compass'); English 'the edge' never counts.
    r"(?-i:(?:\b[AaÀà]|\b[Dd]a|\b[Nn]a|\b[Pp]ela|\b[Cc]om\s+a)\s+Edge\b)",
    r"(?-i:\bEdge\s*,\s*(?:da|do|empresa|subsidi[áa]ria)\b)",
)

#: keyword (lowercased) -> its off-topic senses. Only Compass today.
KEYWORD_SENSE_EXCLUSIONS: dict[str, SenseExclusion] = {
    "compass": SenseExclusion(
        excluded=(COMPASS_AUTOMOTIVE, COMPASS_OTHER_ENTITY),
        keep_context=COMPASS_COMPANY_CONTEXT,
        protected=(COMPASS_NEFTE,),
    ),
}


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def rule_for(keyword: str) -> SenseExclusion | None:
    return KEYWORD_SENSE_EXCLUSIONS.get((keyword or "").lower())


@lru_cache(maxsize=64)
def _occurrence_pattern(keyword_lower: str, exact: bool) -> re.Pattern[str]:
    body = re.escape(keyword_lower)
    return re.compile(rf"\b{body}\b" if exact else body, re.IGNORECASE)


def _occurrence_sense(text: str, start: int, end: int, sense: KeywordSense) -> str | None:
    """Evidence string when the occurrence [start, end) reads in `sense`."""
    if sense.before is not None:
        m = sense.before.search(text, max(0, start - _BEFORE_WINDOW), start)
        if m:
            return (m.group(0) + text[start:end]).strip()
    if sense.after is not None:
        m = sense.after.match(text, end)
        if m:
            return (text[start:end] + m.group(0)).strip()
    return None


def _short(s: str, limit: int = 60) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    return s if len(s) <= limit else s[: limit - 3] + "..."


def evaluate_keyword_sense(text: str, keyword: str, *, exact: bool) -> SenseVerdict:
    """Judge whether `keyword` is used in an excluded sense in `text`.

    `exact` must be the keyword's effective match_type ('exact' -> True), so the
    occurrences judged are exactly the ones the matcher counted.
    """
    rule = rule_for(keyword)
    if rule is None:
        return SenseVerdict(keyword, "no_rule")
    text = text or ""
    occ = [(m.start(), m.end()) for m in _occurrence_pattern(keyword.lower(), exact).finditer(text)]
    if not occ:
        return SenseVerdict(keyword, "no_occurrence")
    n = len(occ)

    if rule.keep_context is not None:
        m = rule.keep_context.search(text)
        if m:
            return SenseVerdict(
                keyword, "keep_context", n, (), (f"context: {_short(m.group(0))}",)
            )

    for s, e in occ:
        for sense in rule.protected:
            ev = _occurrence_sense(text, s, e, sense)
            if ev:
                return SenseVerdict(keyword, "protected", n, (sense.name,), (f"{sense.name}: {_short(ev)}",))

    # Occurrence patterns explain only the occurrence they touch.
    explained: list[str | None] = [None] * n
    evidence: dict[str, str] = {}
    for i, (s, e) in enumerate(occ):
        for sense in rule.excluded:
            ev = _occurrence_sense(text, s, e, sense)
            if ev:
                explained[i] = sense.name
                evidence.setdefault(sense.name, f"{sense.name}: {_short(ev)}")
                break

    # Document vocabulary explains every occurrence still unexplained.
    for sense in rule.excluded:
        if sense.vocabulary is None or all(explained):
            continue
        m = sense.vocabulary.search(text)
        if m:
            for i in range(n):
                if explained[i] is None:
                    explained[i] = sense.name
            evidence.setdefault(sense.name, f"{sense.name}: {_short(m.group(0))}")

    senses = tuple(dict.fromkeys(x for x in explained if x))
    shown = tuple(evidence[name] for name in senses if name in evidence)
    unexplained = sum(1 for x in explained if x is None)
    if unexplained:
        return SenseVerdict(keyword, "no_evidence", n, senses, shown, unexplained)
    return SenseVerdict(keyword, "excluded", n, senses, shown)


def drop_sense_excluded(
    labels: Iterable[str],
    text: str,
    exact_keywords: Iterable[str] | None = None,
) -> list[str]:
    """Return `labels` without the ones whose every occurrence in `text` is off-topic.

    Labels without a SenseExclusion (and sentinels such as '#topic') pass
    through untouched, as do labels with no occurrence in `text` (no evidence).
    """
    labels = list(labels)
    if not labels or not any(rule_for(k) for k in labels):
        return labels
    exact_lower = {k.lower() for k in (exact_keywords or ()) if k}
    out: list[str] = []
    for k in labels:
        if rule_for(k) is not None:
            verdict = evaluate_keyword_sense(text, k, exact=k.lower() in exact_lower)
            if verdict.excluded:
                continue
        out.append(k)
    return out
