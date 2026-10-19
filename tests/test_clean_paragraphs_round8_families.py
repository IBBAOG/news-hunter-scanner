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


def test_an_empty_entry_neither_counts_nor_ends_a_run() -> None:
    drops_to([A, "1 month ago", "", "Europe", "3 months ago", B], [A, B])
    drops_to([A, "1 month ago", "   ", "Europe", "3 months ago", B], [A, B])


def test_a_40_code_point_label_is_not_a_label() -> None:
    label = "Abcdefghijklm Nopqrstuvwxyzab Cdefghijkl"
    assert len(label) == shim.AGE_RUN_LABEL_MAX_CHARS == 40
    drops_to([A, "1 month ago", label, "3 months ago", B], [A, label, B])
    drops_to([A, "1 month ago", label[:-1], "3 months ago", B], [A, B])


def test_a_real_list_just_before_the_rail_survives() -> None:
    # QA (TS f69ab1ce8): labels count only AFTER the run's first age line.
    drops_to(
        [A, "The consortium includes:", "Petrobras", "Shell", "TotalEnergies",
         "3 months ago", "Middle East", "4 months ago"],
        [A, "The consortium includes:", "Petrobras", "Shell", "TotalEnergies"],
    )
    drops_to(
        [A, "Key figures", "Brent: $90", "WTI: $86", "2 days ago", "3 days ago", B],
        [A, "Key figures", "Brent: $90", "WTI: $86", B],
    )


def test_a_caseless_script_line_is_never_an_age_run_label() -> None:
    zh, ar = "中东", "الشرق الأوسط"
    drops_to([A, "1 month ago", zh, "3 months ago", ar, "5 months ago", B], [A, zh, ar, B])


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


def test_the_section_label_runs_after_the_dedupe() -> None:
    # QA (TS f69ab1ce8): the section label is the LAST step, after the dedupe,
    # so a repeated label does not survive as the new first line.
    drops_to(["Summary", "Summary", A], [A])
    drops_to(["Summary", A, "Summary"], [A])


def test_name_shape_reads_the_first_code_point() -> None:
    # Deseret capitals are astral (outside the BMP): same answer as TS, which
    # reads Array.from(word)[0].
    deseret = "\U00010414\U0001042f\U00010445 \U00010406\U0001042e"
    drops_to([deseret, A], [deseret, A])


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
        # QA: ASCII letters only (`[a-z\s]`, not `\w`), same answer as TS
        "São Paulo correspondent",
        "G20 correspondent",
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


# ── CJK / RTL controls (QA 2026-10-16 c: the caseless-script guard) ─────────
# Unspaced Chinese is one "word" and Arabic/Hebrew have no case, so neither the
# word count nor the name shape can tell a label from a lead or a byline there.
# Both structural rules skip any paragraph holding a caseless-script character
# (CASELESS_SCRIPT_PATTERN, byte-identical with TS f69ab1ce8). Accepted cost:
# a caseless label stays.
ZH_FULLWIDTH_PAREN = (
    "中国石油天然气集团公司周五表示，受国内需求回升和新油田投产推动，"
    "第三季度原油产量同比增长百分之三（新华社）"
)
ZH_DIGIT = "中国海油表示今年前三季度油气净产量同比增长7"
ZH_FULL_STOP = "中国石油天然气集团公司周五表示第三季度原油产量同比增长百分之三。"
AR_THREE_WORDS = "أسعار النفط ترتفع"


@pytest.mark.parametrize("first", [ZH_FULLWIDTH_PAREN, ZH_DIGIT, ZH_FULL_STOP, AR_THREE_WORDS])
def test_a_cjk_or_arabic_first_paragraph_is_never_a_section_label(first: str) -> None:
    drops_to([first, A, B], [first, A, B])


@pytest.mark.parametrize("first", [
    # the TS QA list (clean.test.ts, f69ab1ce8)
    "前三季度原油产量同比增长3.2%，天然气产量增长6.1%",
    "国际油价16日收于每桶90.5美元",
    "中国石油发布公告称：",
    "新华社北京10月16日电（记者张三）",
    "他表示，这是‘历史性的一步’",
    "国际油价连续三日上涨——",
    "布伦特原油上涨2%；",
    "原油価格が上昇",
    "هل ترتفع الأسعار؟",
    "דני זקן",
    "بقلم أحمد علي",
    "摘要",  # accepted cost: a Chinese section label stays
])
def test_the_ts_caseless_first_paragraphs_are_kept(first: str) -> None:
    drops_to([first, A, B], [first, A, B])


@pytest.mark.parametrize("line", [ZH_DIGIT, AR_THREE_WORDS])
def test_cjk_and_arabic_short_lines_inside_an_age_run_are_kept(line: str) -> None:
    assert len(line) < shim.AGE_RUN_LABEL_MAX_CHARS
    drops_to([A, "1 month ago", line, "3 months ago", B], [A, line, B])


def test_a_cjk_line_over_the_label_cap_inside_an_age_run_is_kept() -> None:
    assert len(ZH_FULLWIDTH_PAREN) >= shim.AGE_RUN_LABEL_MAX_CHARS
    drops_to([A, "1 month ago", ZH_FULLWIDTH_PAREN, "3 months ago", B],
             [A, ZH_FULLWIDTH_PAREN, B])


# ── Family 3: related headline, a <p> that IS the text of its link card ─────
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


def test_a_wordpress_lazy_load_card_is_removed() -> None:
    # <noscript><img></noscript> copy of the card image (TS: the card key
    # ignores noscript/template/script/style text).
    h = "O risco escondido na economia dos EUA"
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><ul><li><a href="/x"><img data-src="a.png">'
        f'<noscript><img src="a.png"></noscript><p>{h}</p></a></li></ul>'
    ) == [R1, R2]


def test_noscript_template_script_style_text_is_not_card_text() -> None:
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><a href="/x"><noscript>Ative o JavaScript</noscript>'
        f'<template>tpl</template><script>var x = 1;</script><style>.a{{}}</style>'
        f'<p>{H}</p></a>'
    ) == [R1, R2]


def test_the_card_text_is_the_collapsed_text_without_skipped_tags() -> None:
    soup = BeautifulSoup(f'<a href="/x"><span> 12:30 </span>\n<b>{H}</b></a>', "lxml")
    a = soup.find("a")
    assert shim._card_text(a) == shim._collapsed_text(a)


@pytest.mark.parametrize("line, outside", [
    # in-page TOC link written as a full URL with a fragment
    ("Production outlook",
     '<nav><a href="https://site.com/article#production-outlook">Production outlook</a></nav>'),
    # empty href
    (H, f'<aside><a href="">{H}</a></aside>'),
    # share link whose title= is the deck
    ("Investidores avaliam a manutenção dos juros pelo Fed e a alta do petróleo",
     '<div><a href="https://twitter.com/intent/tweet?url=x" title="Investidores avaliam a '
     'manutenção dos juros pelo Fed e a alta do petróleo"><img src="t.png"></a></div>'),
    # one-word intertitle equal to a tag link
    ("Petrobras", '<div class="tags"><a href="/tag/petrobras">Petrobras</a></div>'),
    # the plain sidebar case of the first draft
    (H, f'<aside><a href="/x">{H}</a></aside>'),
    (f"{H}.", f'<aside><a href="/x">{H}.</a></aside>'),
])
def test_a_link_elsewhere_with_the_same_text_is_not_a_card(line: str, outside: str) -> None:
    assert _body(f"<p>{R1}</p><p>{line}</p><p>{R2}</p>", outside) == [R1, line, R2]


def test_the_card_title_attribute_is_a_key_too_normalised() -> None:
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><a href="/v" title="{H.upper()}  "><img src="v.png">'
        f'<span>12:30</span><p>{H}</p></a>'
    ) == [R1, R2]


@pytest.mark.parametrize("anchor", [
    '<a href="#production-outlook">', '<a href="">', '<a href="  ">', '<a name="x">',
])
def test_the_card_needs_a_usable_href(anchor: str) -> None:
    assert _body(f"<p>{R1}</p><p>{R2}</p>{anchor}<p>{H}</p></a>") == [R1, R2, H]


def test_a_question_headline_inside_its_own_card_goes_too() -> None:
    q = "Quem ganha e quem perde com a entrada em vigor do Acordo União Europeia-Mercosul?"
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><ul><li><a href="/q"><div>'
        f'<p class="li-title">{q}</p></div></a></li></ul>'
    ) == [R1, R2]


def test_a_card_paragraph_over_120_code_points_is_kept() -> None:
    long = (f"{H} e o que isso significa para os juros, o dólar e as bolsas nas próximas "
            "semanas segundo analistas")
    assert len(long) > shim.RELATED_HEADLINE_MAX_CHARS
    assert _body(f'<p>{R1}</p><p>{R2}</p><a href="/x"><p>{long}</p></a>') == [R1, R2, long]


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


LABEL = "NESTA EDIÇÃO. Resumo da semana"


def test_an_index_group_whose_teaser_is_a_card_goes_as_a_group() -> None:
    assert _body(
        f'<div class="wp-block-group"><p>{LABEL}</p><p>Teaser um.</p><p>Teaser dois.</p>'
        f'<a href="/t3"><p>Teaser três</p></a></div><p>{R1}</p><p>{R2}</p>'
    ) == [R1, R2]


def test_related_headlines_run_after_the_index_groups() -> None:
    # The bound counts what is left AFTER the index group went: one real <p>
    # stays, so the card is kept. Run first, the group's 3 <p> would have
    # counted and the card would have gone.
    assert _body(
        f'<p>{R1}</p><div class="wp-block-group"><p>{LABEL}</p><p>Teaser um.</p>'
        f'<p>Teaser dois.</p></div><a href="/x"><p>{H}</p></a>'
    ) == [R1, H]


# ── Declared divergences from the TS side (pinned, see the shim comment) ────
def test_divergence_ascii_class_under_ignorecase_matches_long_s() -> None:
    drops_to([A, "[ſhortcode]"], [A])  # TS keeps it


@pytest.mark.parametrize("age", ["٣ months ago", "３ hours ago"])
def test_divergence_unicode_digits_make_an_age_line(age: str) -> None:
    drops_to([A, age], [A])  # TS keeps it


def test_divergence_no_inline_split_a_pipe_list_ends_an_age_run() -> None:
    pipes = "Mercados | Energia | Petróleo | Gás"
    drops_to([A, "1 month ago", pipes, "3 months ago", B], [A, pipes, B])  # TS: [A, B]
    drops_to([pipes, "Summary", A], [pipes, "Summary", A])  # TS: [A]


def test_divergence_a_span_wrapped_link_paragraph_survives() -> None:
    assert _body(
        f'<p>{R1}</p><p>{R2}</p><p><span><a href="/x">{H}</a></span></p>'
    ) == [R1, R2, H]  # TS isMostlyLinks drops it
