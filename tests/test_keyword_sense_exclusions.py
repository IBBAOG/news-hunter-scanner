"""Keyword sense exclusion (news_hunter/keyword_senses.py), wired into the matcher.

'Compass' is meant to follow Compass Gas e Energia (Cosan group). Every title
and snippet below is a real news_articles row tagged 'Compass' (production,
2026-09-15) unless marked synthetic. The scanner matches 'Compass' as
match_type 'exact' since 2026-09-15, so most cases run on that path; the
substring path is covered where its behaviour differs.

Run from repo root: python -m pytest tests/test_keyword_sense_exclusions.py -v
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_hunter.fetcher import RawItem  # noqa: E402
from news_hunter.filter import matches_keywords  # noqa: E402
from news_hunter.keyword_senses import (  # noqa: E402
    KEYWORD_SENSE_EXCLUSIONS,
    drop_sense_excluded,
    evaluate_keyword_sense,
)
from news_hunter.pipeline import _keep_candidate  # noqa: E402

KW = "Compass"
EXACT = {KW}
KEYWORDS = [KW, "gás", "gasolina", "pipeline", "Cosan"]


def _match(text: str, *, exact: bool = True, keywords=None, context: str = "") -> list[str]:
    return matches_keywords(
        text, keywords or KEYWORDS, EXACT if exact else set(), sense_context=context
    )


# ---------------------------------------------------------------------------
# Car (Jeep Compass) -> dropped
# ---------------------------------------------------------------------------

CAR_ROWS = [
    # estadao.com.br
    ("Nova geração do Jeep Compass manterá plataforma, mas pode receber "
     "motorização da Leapmotor", ""),
    # clickpetroleoegas.com.br
    ("Proprietária de Jeep Compass Longitude T270 com 37,9 mil km percebe vazamento "
     "menos de 2 meses após revisão e leva SUV à concessionária em São Paulo", ""),
    # estadao.com.br
    ("Xiaomi Skynomad N90 Max tem preço de Jeep Compass e barraca integrada no teto", ""),
    # cnnbrasil.com.br: the keyword only in the snippet, in a Jeep model list
    ("Estreia do Avenger impulsiona melhor mês de vendas da Jeep em 5 anos",
     "A fabricante somou 9.027 emplacamentos no mês entre os modelos Renegade, "
     "Compass e Commander, somados a mais de 5 mil pedidos do novo SUV compacto"),
    # valor.globo.com: bare 'Compass', document vocabulary ('SUV')
    ("Leapmotor B10 é SUV elétrico mais barato que Compass", ""),
    # noticias.r7.com: trim + model year
    ("Teste com o Jeep Compass Sport 2026: ainda vale a pena?", ""),
    # g1.globo.com: model list, no 'Jeep'
    ("SW4, Compass e Hillux: 60 automóveis são apreendidos em operação contra o CV "
     "que prendeu 28 pessoas", ""),
    # estadao.com.br: Compass only inside a car-review teaser
    ("Nissan Sentra sai de linha no Brasil após três anos; marca confirma fim do ciclo do sedã",
     "testamos o SUV híbrido chinês que quer derrubar Corolla Cross e Compass Omoda 5 "
     "SHS entrega 224 cv, faz até 19 km/l"),
]


@pytest.mark.parametrize("title,snippet", CAR_ROWS)
def test_car_rows_are_dropped(title, snippet):
    assert _match(f"{title} \n {snippet}") == []


@pytest.mark.parametrize("title,snippet", CAR_ROWS)
def test_car_rows_are_dropped_on_the_substring_path_too(title, snippet):
    assert _match(f"{title} \n {snippet}", exact=False) == []


@pytest.mark.parametrize("body", [
    # g1.globo.com article bodies, found by the purge refetch
    "além do mesmo apartamento de R$ 900 mil e do veículo Compass de R$ 190 mil.",
    "já ajudou a profissional cedendo um veículo modelo Compass e pagando custos de aluguel",
])
def test_car_noun_before_compass_is_car_evidence(body):
    assert _match(body) == []


def test_car_title_judged_with_the_summary_as_sense_context():
    # Synthetic: a bare title is only readable as the car next to its summary.
    title = "Compass ganha nova central multimídia"
    assert _match(title) == [KW]
    assert _match(title, context="A Jeep atualizou o SUV médio mais vendido do país") == []


# ---------------------------------------------------------------------------
# Company (Compass Gas e Energia) -> kept
# ---------------------------------------------------------------------------

COMPANY_ROWS = [
    ("Lucro da Compass no 2º trimestre recua 19%",
     "A Compass, empresa de gás e energia controlada pela Cosan, registrou um lucro "
     "líquido de R$ 287,2 milhões no segundo trimestre"),
    ("Fitch reafirma nota de crédito da Compass e retira observação negativa",
     "As ações replicam aquelas realizadas na análise da Cosan, controladora da Compass."),
    ("Compass se posiciona em biometano no mercado livre e no cativo com nova "
     "regulação em São Paulo", ""),
    ("Edge mais que dobra contratos e vira plataforma de crescimento da Compass",
     "Carreta de GNL da Edge, do grupo Compass A Edge se tornou a principal "
     "plataforma de crescimento da Compass"),
    # Titles alone, no context word: no evidence either way -> kept.
    ("Lucro da Compass no 2º trimestre recua 19%", ""),
    ("Compass contrata BTG para atuar como formador de mercado", ""),
    ("Compass lucra R$ 287,2 milhões entre abril e junho, queda de 19%", ""),
    ("Nefte Compass Market Trends: Sep. 9, 2026", "Current market highlights in charts."),
    # QA 2026-09-15 (synthetic): Compass sells natural gas FOR vehicles.
    ("Compass leva GNV a veículos pesados em rodovias de São Paulo", ""),
    ("Compass quer GNV em 100 mil carros de motoristas de aplicativo", ""),
    ("Compass fecha acordo com montadora para caminhões movidos a GNV", ""),
    ("Compass vai fornecer gás para fábrica da Stellantis em Betim", ""),
    ("Compass conclui compra da MSGás; carros a GNV crescem em rodovias do MS", ""),
    # QA 2026-09-15 (synthetic): a year or amount right after the name.
    ("Compass 2026: guidance prevê Ebitda de R$ 6 bilhões", ""),
    ("Compass 2025 results beat estimates as gas volumes grow", ""),
    ("Compass 1.3 bi em debêntures", ""),
    # QA 2026-09-15 (synthetic): dollar amounts that are not a US EPS template.
    ("Brazil's Compass prices IPO at 28 reais per share, raising $560 million", ""),
]


@pytest.mark.parametrize("title,snippet", COMPANY_ROWS)
def test_company_rows_keep_the_label(title, snippet):
    assert KW in _match(f"{title} \n {snippet}")


# Each removed signal on its own, with no company context to hide it (so reverting
# that one signal turns exactly these red).
@pytest.mark.parametrize("text", [
    # weak pair terms ('veículos' + 'rodovias', 'carros' + 'motoristas')
    "Compass leva gás a veículos pesados em rodovias de São Paulo",
    "Compass amplia oferta para carros de motoristas de aplicativo",
    # 'montadora' / 'Stellantis'
    "Compass fecha acordo com montadora para frota de caminhões",
    "Compass vai fornecer gás para fábrica da Stellantis em Betim",
    # bare model year / bare '1.3'
    "Compass 2026: guidance prevê Ebitda de R$ 6 bilhões",
    "Compass 1.3 bi em debêntures",
])
def test_removed_signals_are_not_evidence(text):
    v = evaluate_keyword_sense(text, KW, exact=True)
    assert v.status == "no_evidence" and _match(text, keywords=[KW]) == [KW]


@pytest.mark.parametrize("context", [
    "GNV", "MSGás", "SCGás", "Copergás", "Gasmig", "Bahiagás", "Potigás", "Sergás",
])
def test_gas_for_vehicles_context_beats_car_vocabulary(context):
    # Synthetic: 'SUV' alone would be car evidence; the gas context wins.
    text = f"Compass e {context}: frota de SUVs de locadoras troca de combustível"
    assert _match(text, keywords=[KW]) == [KW]


def test_com_gas_phrase_is_not_comgas_context():
    # 'com gás' ('with gas') is an ordinary phrase, not the distributor Comgás.
    assert _match("Jeep Compass roda com gás e etanol", keywords=[KW]) == []
    assert _match("Jeep Compass roda com Comgás", keywords=[KW]) == [KW]


def test_accented_distributor_names_do_not_match_ordinary_words():
    # 'algas' (seaweed) and 'cegas' (blind) must not read as Algás / Cegás.
    assert _match("Compass Point cita algas e pessoas cegas", keywords=[KW]) == []
    assert _match("Compass Point, Algás e Cegás", keywords=[KW]) == [KW]


def test_used_car_listing_year_is_still_car_evidence():
    assert _match("Vendo Compass 2021/2022 blindado, único dono") == []


def test_company_context_beats_automotive_vocabulary():
    # valor.globo.com, plus a synthetic 'SUV' that alone would be car evidence.
    text = ("Edge, da Compass, vai fornecer GNL e biometano para frota de caminhões "
            "pesados e SUVs da JAC Motors")
    assert _match(text) == [KW]


def test_nefte_compass_is_protected_even_next_to_car_words():
    v = evaluate_keyword_sense("Nefte Compass: Russian SUV sales slump", KW, exact=True)
    assert v.status == "protected" and not v.excluded


# ---------------------------------------------------------------------------
# Other companies named Compass -> dropped
# ---------------------------------------------------------------------------

OTHER_ENTITY_ROWS = [
    "A nova ambição da Vinci Compass na área de alimentação",
    "Compass Group India cuts food waste to 3% using AI-led kitchen operations",
    "COO da Compass Diversified adquire US$ 211.146 em ações - Investing.com Brasil",
    "ClassNK launches ‘Survey Compass,’ an AI assistant supporting survey-related operations",
    "Compass Point inicia cobertura da Gorilla Tech com compra pelo crescimento em GPUs",
    "JPMorgan rebaixa Compass Minerals por preocupações com volume",
    "Compass Therapeutics, Notícias (CMPX) - Investing.com Brasil",
    "Compass Notícias (COMP) - Investing.com Brasil",
    "Análise SWOT da Compass Inc.: ação mira crescimento após aquisição",
    "BTIG eleva preço-alvo da Compass para US$ 15 com base em dados do mercado imobiliário",
    "FDA concede voucher prioritário à Compass para tratamento com psilocibina",
    # terra.com.br article body, found by the purge refetch
    "A quinta edição do Bosch Tech Compass, estudo global que analisa expectativas",
]


@pytest.mark.parametrize("title", OTHER_ENTITY_ROWS)
def test_other_entity_rows_are_dropped(title):
    assert _match(title) == []


def test_us_eps_dollar_template_is_other_entity_evidence():
    # br.investing.com, real rows (Compass Inc. / Compass Minerals templates).
    assert _match("Lucro da Compass superou projeções por $0,02; receita supera estimativas") == []
    assert _match("Lucro da Compass veio abaixo das projeções por $0,08") == []


@pytest.mark.parametrize("amount", [
    "levanta $560 million", "tem preço-alvo de US$ 15", "paga R$ 0,57 por ação",
    "paga R $ 0,57 por ação", "paga R $0,57 por ação", "vale US $0,57", "vê bitcoin a $62 mil",
])
def test_other_dollar_amounts_are_not_evidence(amount):
    assert _match(f"Compass {amount}") == [KW]


def test_dollar_in_the_summary_does_not_drop_a_title_hit():
    assert _match("Compass sobe 5% na B3", context="bitcoin opera a $62 mil") == [KW]
    it = _item("Compass sobe 5% na B3", "bitcoin opera a $62 mil")
    assert _keep_candidate(it, KEYWORDS, 24, EXACT) == [KW]


def test_homonym_pattern_explains_only_its_own_occurrence():
    # QA 2026-09-15 (synthetic): the bare second 'Compass' is not explained.
    text = "Agenda de resultados: Vinci Compass e Compass divulgam balanço hoje"
    v = evaluate_keyword_sense(text, KW, exact=True)
    assert v.status == "no_evidence" and v.occurrences == 2 and v.unexplained == 1
    assert v.evidence == ("other_entity: Vinci Compass",)
    assert _match(text) == [KW]
    # Same shape: title names Compass Pathways, the lede says just "a Compass".
    lede = ("Por que a ação da Compass Pathways está subindo hoje? \n "
            "A Compass informou dados positivos do estudo de fase 3.")
    assert _match(lede) == [KW]


def test_document_vocabulary_is_the_one_spill_over():
    # Vocabulary explains every occurrence, bare ones included.
    text = ("Por que a ação da Compass Pathways está subindo hoje? \n "
            "A Compass informou que o FDA aceitou o pedido.")
    v = evaluate_keyword_sense(text, KW, exact=True)
    assert v.excluded and v.occurrences == 2 and v.senses == ("other_entity",)
    assert _match("Compass lidera vendas entre os SUVs médios") == []


# ---------------------------------------------------------------------------
# Mixed articles: only the excluded keyword goes
# ---------------------------------------------------------------------------

def test_mixed_article_other_entity_only_compass_removed():
    title = ("Compass Point eleva recomendação do FII Strawberry Fields com base no "
             "pipeline - Investing.com Brasil")
    assert _match(title) == ["pipeline"]


def test_mixed_article_car_only_compass_removed():
    # clickpetroleoegas.com.br title + a synthetic second keyword in the snippet.
    text = ("Casal de idosos compra o primeiro carro 0 km e celebra conquista com "
            "Jeep Compass dos sonhos \n O modelo roda com gasolina ou etanol.")
    assert _match(text) == ["gasolina"]


def test_company_occurrence_next_to_a_car_mention_keeps_the_label():
    # Synthetic: a Jeep Compass mention plus the company in the same article.
    text = "Jeep Compass lidera vendas \n A Compass, dona da Comgás, anunciou dividendos."
    assert _match(text, keywords=[KW]) == [KW]


def test_protected_occurrence_next_to_an_excluded_one_keeps_the_label():
    # Synthetic: the newsletter quoted in a roundup that also names Vinci Compass.
    text = "Vinci Compass cuts oil exposure, Nefte Compass reports Urals discount"
    assert _match(text, keywords=[KW]) == [KW]


# ---------------------------------------------------------------------------
# Exact vs substring paths, accents, no-occurrence
# ---------------------------------------------------------------------------

def test_exact_path_ignores_compasso_through_config_not_the_rule():
    text = "O mercado segue em compasso de espera"
    assert _match(text, exact=True) == []
    # Substring path: noise stays (whole-word matching is the config fix).
    assert _match(text, exact=False) == [KW]


def test_substring_noise_inside_a_car_document_is_dropped():
    text = "Montadoras em descompasso: SUV mais vendido perde espaço"
    assert _match(text, exact=False) == []


def test_accent_is_meaningful_seda_is_not_seda_car():
    # 'seda' (silk) must not read as 'sedã'; 'sedã' alone is car evidence.
    assert _match("Compass lança linha de camisas de seda") == [KW]
    assert _match("Compass ou sedã: qual comprar") == []


def test_no_occurrence_is_never_an_exclusion():
    v = evaluate_keyword_sense("Jeep lança SUV", KW, exact=True)
    assert v.status == "no_occurrence" and not v.excluded


def test_keywords_without_a_rule_pass_through():
    assert "gás" not in KEYWORD_SENSE_EXCLUSIONS
    assert _match("Jeep Compass a gás \n SUV", keywords=["gás", KW]) == ["gás"]


def test_drop_sense_excluded_keeps_sentinels_and_is_case_insensitive():
    assert drop_sense_excluded(["#pending", "COMPASS"], "Jeep Compass", {"compass"}) == ["#pending"]


# ---------------------------------------------------------------------------
# Pipeline stage: _keep_candidate honours the rule (title + summary context)
# ---------------------------------------------------------------------------

def _item(title: str, summary: str = "") -> RawItem:
    return RawItem(
        url="https://example.com/a/1",
        title=title,
        summary=summary,
        published_at=datetime.now(timezone.utc),
        source_domain="example.com",
        feed_domain="example.com",
    )


def test_keep_candidate_drops_a_car_title():
    it = _item("Jeep Compass tem desconto de R$ 27 mil e versão de entrada passa a custar menos")
    assert _keep_candidate(it, KEYWORDS, 24, EXACT) is None


def test_keep_candidate_uses_the_summary_to_judge_a_bare_title():
    it = _item("Compass ganha nova versão", "A Stellantis renovou o SUV da Jeep.")
    assert _keep_candidate(it, KEYWORDS, 24, EXACT) is None


def test_keep_candidate_keeps_other_keywords_of_a_mixed_title():
    it = _item("Compass Point eleva recomendação do FII com base no pipeline")
    assert _keep_candidate(it, KEYWORDS, 24, EXACT) == ["pipeline"]


def test_keep_candidate_keeps_the_company():
    it = _item("Compass (PASS3) aprova dividendos de R$ 0,57 por ação")
    assert _keep_candidate(it, KEYWORDS, 24, EXACT) == [KW]


# ---------------------------------------------------------------------------
# End to end through run_search (stubbed collector / enricher / sink)
# ---------------------------------------------------------------------------

def _drive_run_search(monkeypatch, items, snippet_for):
    from news_hunter import pipeline

    captured: list = []
    monkeypatch.setattr(
        pipeline, "get_config",
        lambda: {"keywords": [KW, "pipeline"], "exact_keywords": set(EXACT), "window_hours": 24},
    )
    monkeypatch.setattr(
        pipeline, "iter_collect", lambda *a, **k: iter([("example.com", items, None)])
    )

    def _enrich(it, **kwargs):  # noqa: ARG001
        return snippet_for(it), it.published_at, it.url, it.source_domain, ""

    monkeypatch.setattr(pipeline, "enrich_item", _enrich)
    monkeypatch.setattr(
        pipeline, "upsert_articles", lambda arts: captured.extend(arts) or len(arts)
    )
    pipeline.run_search(include_google_news=False, fast_mode=True, hours_override=24)
    return {a.title: a.matched_keywords for a in captured}


def test_run_search_persists_company_drops_car_and_strips_mixed(monkeypatch):
    items = [
        _item("Leapmotor B10 é SUV elétrico mais barato que Compass"),
        _item("Compass Point eleva recomendação do FII com base no pipeline"),
        _item("Compass (PASS3) aprova dividendos de R$ 0,57 por ação"),
    ]
    for i, it in enumerate(items):
        it.url = f"https://example.com/a/{i}"
    got = _drive_run_search(monkeypatch, items, lambda it: "")
    assert got == {
        "Compass Point eleva recomendação do FII com base no pipeline": ["pipeline"],
        "Compass (PASS3) aprova dividendos de R$ 0,57 por ação": [KW],
    }


def test_fast_mode_fallback_does_not_resurrect_an_excluded_hit(monkeypatch):
    # Bare title passes the cheap stage; the enriched body shows the car. Before
    # the fix the fast-mode fallback re-used the candidate-stage ['Compass'].
    it = _item("Compass ganha nova versão")
    got = _drive_run_search(
        monkeypatch, [it], lambda _it: "A Stellantis renovou o SUV médio da Jeep."
    )
    assert got == {}
