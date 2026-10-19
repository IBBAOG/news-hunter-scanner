"""Boilerplate families from the dashboard's coverage round 8 (2026-10-16 c).

Round 8 (`clipping_coverage_20261016T132529Z`) left boilerplate in `ok` bodies,
mostly on the Readability path where the noise classes never run. These tests
mirror the round-8 cases of the dashboard's `src/lib/clipping/__tests__/
clean.test.ts` and `extract.test.ts` (the TS side is the source of truth; the
patterns and constants are byte-identical): pitch lines, house promo,
shortcode / relative-age lines and age-run rail tags, the section-label first
paragraph, bare role lines, caption credits and the related-headline DOM rule.
Every family has negative controls - real prose that must survive.
"""
from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from news_hunter import _clipinator_shim as shim
from news_hunter._clipinator_shim import (
    SHORT_NOISE_MAX_CHARS,
    _paragraphs_from,
    clean_paragraphs,
)

# Same two paragraphs as clean.test.ts (`A`, `B`) and its round-8 `C`.
A = "A Petrobras anunciou nesta terça-feira um novo plano de investimentos para o refino."
B = "Segundo a companhia, os recursos serão aplicados ao longo dos próximos cinco anos."
C = "Brent crude rose 2% on Friday as traders weighed fresh supply risks in the Middle East."
ASTANA = (
    "Get The Astana Times stories sent directly to you! Sign up via the website or "
    "subscribe to our X, Facebook, Instagram, Telegram, YouTube and Tiktok!"
)


def drops_to(inp: list[str], expected: list[str]) -> None:
    assert clean_paragraphs(inp) == expected, inp


def test_the_round8_constants_match_the_ts_side() -> None:
    assert shim.AGE_RUN_LABEL_MAX_WORDS == 3
    assert shim.AGE_RUN_LABEL_MAX_CHARS == 40
    assert shim.AGE_RUN_MIN_AGE_LINES == 2
    assert shim.SECTION_LABEL_MAX_WORDS == 3
    assert shim.RELATED_HEADLINE_MAX_CHARS == 120
    assert shim.RELATED_HEADLINE_MIN_KEEP == 2
    assert SHORT_NOISE_MAX_CHARS == 160
    # RELATIVE_AGE_PATTERN is a named constant listed in _NOISE_PATTERNS.
    assert shim.RELATIVE_AGE_PATTERN in shim._NOISE_PATTERNS


# ── Family 1: newsletter/social/podcast pitch lines (short list) ────────────
def test_the_astanatimes_line_is_under_the_short_cap() -> None:
    assert len(ASTANA) == 148
    assert len(ASTANA) < SHORT_NOISE_MAX_CHARS


@pytest.mark.parametrize("line", [
    ASTANA,
    "Three daily briefings with the key market moves and top business and political stories.",
    "If you like our podcast, make sure to subscribe and be notified of new episodes.",
    "Listen to Energy Technology: industry insights on Spotify, Apple Podcasts, Amazon, "
    "or wherever you get your podcasts.",
    "Subscribe to our YouTube channel for daily market videos.",  # synthetic
    "Get the latest headlines sent directly to your inbox.",  # synthetic
])
def test_family1_pitch_lines_are_dropped(line: str) -> None:
    drops_to([A, line], [A])


def test_family1_negative_controls_survive() -> None:
    long_count_led = (
        "Three daily briefings a week were held by the energy ministry during the blackout, "
        "officials said, as grid operators struggled to restore supply to 4 million homes "
        "in the north."
    )
    assert len(long_count_led) >= SHORT_NOISE_MAX_CHARS
    keep = [
        "The White House held daily briefings throughout the crisis.",
        "Daily briefings on the spill continued until Friday.",
        "The cargo was sent directly to the refinery in Rotterdam.",
        "Make sure the tanks are full before the storm, the ministry said.",
        "Investors may subscribe to our offering until Friday, the CEO said.",
        long_count_led,
    ]
    drops_to(keep, keep)


def test_a_short_count_led_daily_briefings_sentence_is_dropped_accepted_cost() -> None:
    drops_to([A, "Three daily briefings were held last week."], [A])


# ── Family 2: advertiser/house promo (short list) ───────────────────────────
def test_family2_house_promo_is_dropped() -> None:
    drops_to([
        A,
        "Combine business intelligence and editorial excellence to reach engaged "
        "professionals across 36 leading media platforms.",
    ], [A])


def test_family2_negative_controls_survive() -> None:
    keep = [
        "The program aims to reach 5,000 professionals across Brazil by 2027.",
        "Meta and Google remain the leading media platforms for political ads.",
    ]
    drops_to(keep, keep)


# ── Family 4: shortcodes, relative-age lines, age-run labels ────────────────
@pytest.mark.parametrize("line", [
    "[mc4wp_form id=2073]",
    '[contact-form-7 id="123" title="Contact"]',  # synthetic
    "[/vc_row]",  # synthetic
    "3 months ago",
    "1 month ago",
    "an hour ago",  # synthetic
    "2 days ago",  # synthetic
])
def test_family4_whole_line_shortcodes_and_age_lines_are_dropped(line: str) -> None:
    drops_to([A, line], [A])


def test_family4_negative_controls_survive() -> None:
    keep = [
        "[The company declined to comment.]",
        "[Editor's note: this story was updated.]",
        "Brent hit $100 three months ago.",
        "3 months ago, the company said it would drill two more wells.",
        "The rig was contracted 3 months ago.",
    ]
    drops_to(keep, keep)


def test_the_bare_tags_of_a_latest_news_rail_are_dropped() -> None:
    drops_to(
        [A, B, "1 month ago", "Middle East", "3 months ago", "Europe", "North America",
         "5 months ago", "Asia"],
        [A, B],
    )


def test_a_byline_next_to_one_age_line_survives() -> None:
    drops_to([A, "Dipali Banka", "2 hours ago", B], [A, "Dipali Banka", B])


def test_labels_separated_by_prose_are_not_a_run() -> None:
    drops_to(
        [A, "2 hours ago", "Brent update", B, "3 hours ago", "Opening bell", C],
        [A, "Brent update", B, "Opening bell", C],
    )


def test_a_punctuated_line_breaks_the_run() -> None:
    drops_to([A, "1 month ago", "Oil rose.", "3 months ago", B], [A, "Oil rose.", B])


def test_the_age_run_reads_the_sequence_before_any_drop() -> None:
    # A dropped line (date-only / CTA) between labels does not split the run in
    # the TS order either: indexes come from the normalised list before pass 2.
    drops_to(
        [A, "1 month ago", "Europe", "Leave a comment", "Asia", "3 months ago", B],
        [A, B],
    )


# ── Family 6: section-label first paragraph (structural) ────────────────────
def test_a_short_unpunctuated_first_paragraph_is_dropped() -> None:
    drops_to(["In-brief analysis", A, B], [A, B])
    drops_to(["Summary", A, B], [A, B])
    drops_to(["EUA", A, B], [A, B])


def test_the_section_label_applies_to_the_first_surviving_paragraph() -> None:
    drops_to(["13/10/2026 09h01 Atualizado 13/10/2026", "Summary", A, B], [A, B])


def test_only_the_first_label_goes() -> None:
    drops_to(["Summary", "In-brief analysis", A], ["In-brief analysis", A])
    drops_to([A, "In-brief analysis", B], [A, "In-brief analysis", B])
    drops_to([A, "Distillate dynamics", B], [A, "Distillate dynamics", B])


def test_section_label_negative_controls() -> None:
    drops_to(["Oil prices fell.", A], ["Oil prices fell.", A])
    drops_to(["Why now?", A], ["Why now?", A])
    drops_to(["Summary"], ["Summary"])
    drops_to(["By Tsvetana Paraskova", A], ["By Tsvetana Paraskova", A])
    drops_to(["Luan Carlos", A], ["Luan Carlos", A])
    drops_to(["Da Redação", A], ["Da Redação", A])


def test_bylines_are_untouched_by_every_round8_rule() -> None:
    keep = ["By Tsvetana Paraskova for Oilprice.com", "Por Fulano Silva, de Brasília", A]
    drops_to(keep, keep)
    drops_to([A, "By Tsvetana Paraskova for Oilprice.com"],
             [A, "By Tsvetana Paraskova for Oilprice.com"])


def test_the_section_label_runs_before_the_dedupe() -> None:
    # TS order: drop the first survivor, THEN keep-first dedupe; a later copy of
    # the label is therefore the one that stays.
    drops_to(["Summary", A, "Summary"], [A, "Summary"])


# ── Family 7: bare role lines (short list) ──────────────────────────────────
@pytest.mark.parametrize("line", [
    "Middle East correspondent", "Senior energy reporter", "Contributing Editor",
])
def test_family7_bare_role_lines_are_dropped(line: str) -> None:
    drops_to([A, line], [A])


def test_family7_negative_controls_survive() -> None:
    keep = [
        "John Smith, Middle East correspondent",
        "By Gary Dixon, Middle East correspondent",
        "By Gary Dixon Middle East correspondent",
        "A Reuters correspondent in Beirut said the strike hit a fuel depot.",
    ]
    drops_to([A, *keep], [A, *keep])


# ── Family 8: photo credit at the END of a caption (short list) ─────────────
@pytest.mark.parametrize("line", [
    "Presidente dos Estados Unidos, Donald Trump - (Foto: Daniel Torok/Official White House).",
    "Brent crude storage tanks in Rotterdam (Photo: Reuters)",  # synthetic
    "Photo credit: CNBC",
    "Image credits: Shutterstock",  # synthetic
])
def test_family8_caption_credits_are_dropped(line: str) -> None:
    drops_to([A, line], [A])


def test_family8_negative_controls_survive() -> None:
    long_credit = (
        "O presidente dos Estados Unidos, Donald Trump, afirmou nesta sexta-feira que vai "
        "começar uma grande exploração de petróleo e gás na Groenlândia, segundo a Casa "
        "Branca (Foto: Daniel Torok)."
    )
    assert len(long_credit) >= SHORT_NOISE_MAX_CHARS
    keep = [
        long_credit,
        "A foto: uma plataforma da Petrobras no pré-sal, segundo a ANP.",
        "O ministro (foto) visitou a plataforma P-78 nesta sexta-feira.",
    ]
    drops_to([A, *keep], [A, *keep])


# ── CJK / RTL controls (behaviour PINNED for TS parity, not endorsed) ───────
# Both structural rules count whitespace-separated words and read the terminal
# class of _SENTENCE_END_RE (no fullwidth `）`, no digits). Chinese prose has no
# spaces (one "word"), Arabic has no case (never name-shaped). These tests pin
# what the shim does today, identical to the TS rules; the revisor is checking
# the TS side for the same question. If the rule changes there, flip them here.
ZH_FULLWIDTH_PAREN = "中国石油天然气集团公司周五表示第三季度原油产量同比增长百分之三（新华社）"
ZH_DIGIT = "中国海油表示今年前三季度油气净产量同比增长7"
ZH_FULL_STOP = "中国石油天然气集团公司周五表示第三季度原油产量同比增长百分之三。"
AR_THREE_WORDS = "أسعار النفط ترتفع"


@pytest.mark.parametrize("first", [ZH_FULLWIDTH_PAREN, ZH_DIGIT])
def test_cjk_first_paragraph_without_spaces_is_taken_for_a_section_label(first: str) -> None:
    assert len(first.split()) == 1
    drops_to([first, A, B], [A, B])


def test_cjk_first_paragraph_ending_in_a_cjk_full_stop_is_kept() -> None:
    drops_to([ZH_FULL_STOP, A, B], [ZH_FULL_STOP, A, B])


def test_arabic_three_word_first_paragraph_is_taken_for_a_section_label() -> None:
    assert len(AR_THREE_WORDS.split()) == 3
    drops_to([AR_THREE_WORDS, A, B], [A, B])


@pytest.mark.parametrize("line", [ZH_DIGIT, AR_THREE_WORDS])
def test_cjk_and_arabic_short_lines_inside_an_age_run_are_taken_for_labels(line: str) -> None:
    assert len(line) < shim.AGE_RUN_LABEL_MAX_CHARS
    drops_to([A, "1 month ago", line, "3 months ago", B], [A, B])


def test_a_cjk_line_over_the_label_cap_inside_an_age_run_is_kept() -> None:
    assert len(ZH_FULLWIDTH_PAREN) >= shim.AGE_RUN_LABEL_MAX_CHARS
    drops_to([A, "1 month ago", ZH_FULLWIDTH_PAREN, "3 months ago", B],
             [A, ZH_FULLWIDTH_PAREN, B])


# ── Family 3: related headline, a <p> that IS the text of a link (DOM) ──────
# Inline pages, container = the <article> (as the TS extract.test.ts cases).
H = "O risco escondido na economia dos EUA que começa a preocupar Wall Street"
R1 = ("O Dow Jones hoje teve baixa, assim como as demais bolsas de Nova York nesta "
      "sexta-feira, com investidores atentos ao Fed.")
R2 = ("O WTI para dezembro avançou 2,61% na Nymex, a US$ 99,02 o barril, enquanto o "
      "Brent subiu 3,72% na ICE.")
R3 = "Na semana, o Brent e o WTI avançaram 4,9% e 7,8%, respectivamente, segundo a ICE."


def _body(inner: str, outside: str = "") -> list[str]:
    html = (f"<html><head><title>Dow Jones hoje</title></head><body>{outside}"
            f"<article>{inner}</article></body></html>")
    soup = BeautifulSoup(html, "lxml")
    return clean_paragraphs(_paragraphs_from(soup.find("article")))


def test_a_card_headline_inside_its_ancestor_link_is_removed() -> None:
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><ul><li><a href="/x"><img src="a.png">'
        f'<p class="li-title">{H}</p></a></li></ul>'
    ) == [R1, R2]


def test_a_headline_linked_elsewhere_on_the_page_is_removed() -> None:
    assert _body(f"<p>{R1}</p><p>{H}</p><p>{R2}</p>",
                 f'<aside><a href="/x">{H}</a></aside>') == [R1, R2]


def test_the_link_title_attribute_matches_too_normalised() -> None:
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><div><a href="/v" title="{H.upper()}  ">'
        f'<img src="v.png"></a><p>{H}</p></div>'
    ) == [R1, R2]


def test_a_question_headline_inside_its_own_card_goes_too() -> None:
    q = "Quem ganha e quem perde com a entrada em vigor do Acordo União Europeia-Mercosul?"
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><ul><li><a href="/q"><div>'
        f'<p class="li-title">{q}</p></div></a></li></ul>'
    ) == [R1, R2]


def test_a_punctuated_headline_linked_elsewhere_is_prose_and_kept() -> None:
    punct = f"{H}."
    assert _body(f"<p>{R1}</p><p>{punct}</p><p>{R2}</p>",
                 f'<aside><a href="/x">{punct}</a></aside>') == [R1, punct, R2]


def test_a_link_equal_paragraph_over_120_code_points_is_kept() -> None:
    long = (f"{H} e o que isso significa para os juros, o dólar e as bolsas nas próximas "
            "semanas segundo analistas")
    assert len(long) > shim.RELATED_HEADLINE_MAX_CHARS
    assert _body(f"<p>{R1}</p><p>{long}</p><p>{R2}</p>",
                 f'<aside><a href="/x">{long}</a></aside>') == [R1, long, R2]


def test_a_descendant_link_is_not_this_rule() -> None:
    # The <p> whose own child link carries the text is the all-links skip in
    # _paragraphs_from (isMostlyLinks on the TS side), not the related headline.
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><p><a href="/x">{H}</a></p><p>{R3}</p>'
    ) == [R1, R2, R3]


def test_nothing_is_removed_unless_two_other_paragraphs_stay() -> None:
    h2 = "Alcoa supera previsão de lucro no 3º trimestre e aumenta produção de alumínio"
    assert _body(
        f'<p>{R1}</p><a href="/x"><p>{H}</p></a><a href="/y"><p>{h2}</p></a>'
    ) == [R1, H, h2]


def test_a_short_real_line_that_matches_no_link_is_kept() -> None:
    assert _body(f"<p>{R1}</p><p>Oil production and export trends</p><p>{R2}</p>",
                 '<nav><a href="/">Home</a></nav>') == [R1, "Oil production and export trends", R2]


@pytest.mark.parametrize("href", ["#section-2", None])
def test_an_in_page_or_hrefless_link_never_counts(href: str | None) -> None:
    attr = f' href="{href}"' if href is not None else ""
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><a{attr}><p>{H}</p></a>'
    ) == [R1, R2, H]
