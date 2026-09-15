"""Retro-purge of sense-excluded labels (scripts/purge_keyword_sense_exclusions.py).

Offline: rows are dicts shaped like news_articles, the Supabase client is a stub
that records every call, and the refetcher is a stub returning canned pages.
Titles are real rows tagged 'Compass' (production, 2026-09-15) unless marked
synthetic.

Run from repo root: python -m pytest tests/test_purge_keyword_sense_exclusions.py -v
"""
from __future__ import annotations

import csv
import glob
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import purge_keyword_sense_exclusions as P  # noqa: E402


# ---------------------------------------------------------------------------
# Stub Supabase client
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, client, table):
        self.client, self.table = client, table
        self.op, self.cols, self.payload = "select", None, None
        self.filters: list[tuple] = []
        self.span: tuple[int, int] | None = None

    def select(self, cols):
        self.op, self.cols = "select", cols
        return self

    def filter(self, col, op, val):
        self.filters.append((col, op, val))
        return self

    def eq(self, col, val):
        return self.filter(col, "eq", val)

    def gte(self, col, val):
        return self.filter(col, "gte", val)

    def neq(self, col, val):
        return self.filter(col, "neq", val)

    def in_(self, col, vals):
        return self.filter(col, "in", list(vals))

    def order(self, *_a, **_k):
        return self

    def range(self, a, b):
        self.span = (a, b)
        return self

    def update(self, payload):
        self.op, self.payload = "update", payload
        return self

    def delete(self):
        self.op = "delete"
        return self

    def upsert(self, payload, on_conflict=None):
        self.op, self.payload = "upsert", payload
        return self

    def execute(self):
        self.client.calls.append(self)
        return _Result(self.client.respond(self))


class _Client:
    def __init__(self, rows=None, changed_urls=(), on_mutation=None):
        self.rows = rows or []
        self.changed_urls = set(changed_urls)
        self.on_mutation = on_mutation
        self.calls: list[_Query] = []

    def table(self, name):
        return _Query(self, name)

    def respond(self, q):
        if q.op == "select":
            lo, hi = q.span or (0, len(self.rows) - 1)
            return self.rows[lo:hi + 1]
        if q.op in ("update", "delete"):
            if self.on_mutation:
                self.on_mutation(q)
            url = dict((c, v) for c, op, v in q.filters if op == "eq").get("url")
            return [] if url in self.changed_urls else [{"url": url}]
        return []


class _Sink:
    def __init__(self, client):
        self.client = client
        self.table = "news_articles"


def _row(url, title, snippet="", keywords=("Compass",), **extra):
    r = {
        "url": url, "domain": url.split("/")[2], "source_name": "x", "title": title,
        "snippet": snippet, "matched_keywords": list(keywords),
        "found_at": "2026-09-14T10:00:00+00:00", "published_at": "2026-09-14T09:00:00+00:00",
        "source_lang": None, "title_original": None, "title_en": None, "snippet_en": None,
    }
    r.update(extra)
    return r


CAR = _row("https://estadao.com.br/car/1",
           "Nova geração do Jeep Compass manterá plataforma, mas pode receber motorização da Leapmotor")
OTHER_MIXED = _row("https://br.investing.com/news/2",
                   "Compass Point eleva recomendação do FII Strawberry Fields com base no pipeline",
                   keywords=("Compass", "pipeline"))
COMPANY = _row("https://valor.globo.com/empresas/3", "Lucro da Compass no 2º trimestre recua 19%",
               "A Compass, empresa de gás e energia controlada pela Cosan, registrou lucro")
NEFTE = _row("https://www.energyintel.com/4", "Nefte Compass Market Trends: Sep. 9, 2026")
BARE = _row("https://agenciainfra.com/5", "Compass contrata BTG para atuar como formador de mercado")
NOISE = _row("https://g1.globo.com/politica/6",
             "Crise no STF pode colocar nova indicação de Lula para Corte na geladeira",
             "O Senado segue em compasso de espera", keywords=("Compass", "gás"))
NOISE_ONLY = _row("https://cnnbrasil.com.br/7", "Cohen: crise no STF atrasa Secex Consenso")
UNRELATED = _row("https://example.com/8", "Petrobras reajusta diesel", keywords=("Petrobras",))

LONG_BODY = "Texto do artigo sobre a crise institucional. " * 20


# ---------------------------------------------------------------------------
# decide_row on stored text
# ---------------------------------------------------------------------------

def test_car_row_with_only_compass_is_deleted():
    d = P.decide_row(CAR)
    assert d.action == P.DELETE and d.new_keywords == [] and d.labels_removed == ["Compass"]
    assert "automotive: Jeep Compass" in d.reason


def test_mixed_row_only_strips_the_label():
    d = P.decide_row(OTHER_MIXED)
    assert d.action == P.STRIP and d.new_keywords == ["pipeline"]
    assert "other_entity: Compass Point" in d.reason


def test_company_nefte_and_bare_rows_are_kept():
    assert P.decide_row(COMPANY).action == P.KEEP_COMPANY
    assert P.decide_row(NEFTE).action == "keep_nefte_compass"
    bare = P.decide_row(BARE)
    assert bare.action == P.KEEP_NO_EVIDENCE and not bare.pending_refetch


def test_no_wholeword_row_is_kept_and_flagged_without_refetch():
    d = P.decide_row(NOISE)
    assert d.action == P.KEEP_NO_EVIDENCE and d.pending_refetch
    assert "substring: compasso" in d.reason and d.new_keywords == ["Compass", "gás"]


def test_row_without_a_rule_label_is_ignored():
    assert P.decide_row(UNRELATED) is None


def test_translated_fields_are_part_of_the_stored_text():
    # Synthetic foreign row: only the English overlay names the car.
    row = _row("https://example.es/9", "El nuevo modelo llega a Brasil",
               title_en="The new Jeep Compass arrives in Brazil")
    assert P.decide_row(row).action == P.DELETE


# ---------------------------------------------------------------------------
# decide_row with a refetcher (rows with no whole-word occurrence)
# ---------------------------------------------------------------------------

def _page(body="", full="", title=None, ok=True, reason=""):
    return P.FetchedPage(ok, reason, page_title=title if title is not None else NOISE["title"],
                         article_text=body, full_text=full or body)


def test_refetched_page_without_whole_word_strips_or_deletes():
    page = _page(LONG_BODY + " O Senado segue em compasso de espera.")
    mixed = P.decide_row(NOISE, refetch=lambda r: page)
    assert mixed.action == P.STRIP_NW and mixed.new_keywords == ["gás"]
    assert "substring on page: compasso" in mixed.reason
    only = P.decide_row(dict(NOISE, matched_keywords=["Compass"]), refetch=lambda r: page)
    assert only.action == P.DELETE_NW


def test_refetched_body_with_only_excluded_senses_is_stripped():
    page = _page(LONG_BODY + " O ministro chegou num Jeep Compass blindado.")
    assert P.decide_row(NOISE, refetch=lambda r: page).action == P.STRIP_NW


def test_refetched_body_with_company_context_is_kept():
    page = _page(LONG_BODY + " A Compass, dona da Comgás, comentou o tema.")
    assert P.decide_row(NOISE, refetch=lambda r: page).action == P.KEEP_COMPANY


def test_whole_word_outside_the_extracted_paragraphs_is_judged_by_its_surroundings():
    # g1 asset declaration (real): the car sits in a list the extractor skips.
    car = _page(LONG_BODY, full=LONG_BODY + " Os bens declarados em 2022 foram: Apartamento "
                "em Natal: R$ 547.270,85 Carro Jeep Compass: R$ 79.318,77 " + LONG_BODY)
    d = P.decide_row(NOISE, refetch=lambda r: car)
    assert d.action == P.STRIP_NW and "outside the extracted paragraphs: automotive" in d.reason
    # Synthetic market-wrap bullet: company context around it -> kept.
    wrap = _page(LONG_BODY, full=LONG_BODY + " Maiores altas: Compass (PASS3) +3,2%")
    assert P.decide_row(NOISE, refetch=lambda r: wrap).action == P.KEEP_COMPANY
    # A bare mention with nothing around it, even with no body: not proof -> kept.
    bare = _page("curto", full="curto Leia também: Compass anuncia dividendos")
    assert P.decide_row(NOISE, refetch=lambda r: bare).action == P.KEEP_NO_EVIDENCE


def test_failed_fetch_wrong_page_and_thin_body_are_kept():
    failed = P.decide_row(NOISE, refetch=lambda r: _page(ok=False, reason="HTTPError: 403"))
    assert failed.action == P.KEEP_FETCH_FAILED and "403" in failed.reason
    wrong = P.decide_row(NOISE, refetch=lambda r: _page(LONG_BODY, title="Página não encontrada"))
    assert wrong.action == P.KEEP_FETCH_FAILED and "not the stored article" in wrong.reason
    thin = P.decide_row(NOISE, refetch=lambda r: _page("Assine para continuar lendo."))
    assert thin.action == P.KEEP_FETCH_FAILED and "too thin" in thin.reason


def test_rows_with_stored_evidence_never_hit_the_network():
    calls = []

    def refetch(row):
        calls.append(row["url"])
        return _page(LONG_BODY)

    for row in (CAR, OTHER_MIXED, COMPANY, NEFTE, BARE):
        P.decide_row(row, refetch=refetch)
    assert calls == []


# ---------------------------------------------------------------------------
# run(): dry-run vs apply
# ---------------------------------------------------------------------------

ROWS = [CAR, OTHER_MIXED, COMPANY, NEFTE, BARE, NOISE, UNRELATED]


def test_dry_run_writes_csv_and_never_touches_the_database(tmp_path):
    client = _Client()
    csv_path = str(tmp_path / "out.csv")
    decisions, outcomes, backup = P.run(
        ROWS, keywords=None, refetch=None, apply=False, sink=_Sink(client),
        backup_dir=str(tmp_path / "bk"), csv_path=csv_path,
    )
    assert client.calls == [] and outcomes == {} and backup is None
    assert not os.path.exists(tmp_path / "bk")
    with open(csv_path, encoding="utf-8") as fh:
        got = list(csv.DictReader(fh))
    assert list(got[0].keys()) == list(P.CSV_COLUMNS)
    assert {r["url"]: r["action"] for r in got} == {
        CAR["url"]: P.DELETE, OTHER_MIXED["url"]: P.STRIP, COMPANY["url"]: P.KEEP_COMPANY,
        NEFTE["url"]: "keep_nefte_compass", BARE["url"]: P.KEEP_NO_EVIDENCE,
        NOISE["url"]: P.KEEP_NO_EVIDENCE,
    }
    assert next(r for r in got if r["url"] == OTHER_MIXED["url"])["matched_keywords"] == "Compass|pipeline"


def test_apply_backs_up_first_then_mutates_guarded_on_the_read_array(tmp_path):
    backup_dir = tmp_path / "bk"
    seen_backup_before_mutation = []

    def on_mutation(_q):
        files = glob.glob(str(backup_dir / "*.json"))
        seen_backup_before_mutation.append(bool(files))

    client = _Client(changed_urls={OTHER_MIXED["url"]}, on_mutation=on_mutation)
    page = _page(LONG_BODY + " em compasso de espera")
    decisions, outcomes, backup = P.run(
        ROWS, keywords=["Compass"], refetch=lambda r: page, apply=True, sink=_Sink(client),
        backup_dir=str(backup_dir), now=datetime(2026, 9, 15, 12, tzinfo=timezone.utc),
    )
    assert seen_backup_before_mutation and all(seen_backup_before_mutation)
    with open(backup) as fh:
        saved = json.load(fh)
    assert {s["row"]["url"] for s in saved} == {CAR["url"], OTHER_MIXED["url"], NOISE["url"]}
    assert all("snippet" in s["row"] and "found_at" in s["row"] for s in saved)

    muts = {dict((c, v) for c, op, v in q.filters if op == "eq")["url"]: q for q in client.calls}
    assert set(muts) == {CAR["url"], OTHER_MIXED["url"], NOISE["url"]}
    assert muts[CAR["url"]].op == "delete"
    assert ("matched_keywords", "eq", '{"Compass"}') in muts[CAR["url"]].filters
    assert muts[NOISE["url"]].op == "update" and muts[NOISE["url"]].payload == {"matched_keywords": ["gás"]}
    assert ("matched_keywords", "eq", '{"Compass","gás"}') in muts[NOISE["url"]].filters
    assert outcomes == {CAR["url"]: "ok", OTHER_MIXED["url"]: "changed", NOISE["url"]: "ok"}


def test_summary_lists_counts_and_every_row():
    decisions, _o, _b = P.run(ROWS, keywords=None, refetch=None, apply=False)
    md = P.render_summary(decisions, applied=False, outcomes={CAR["url"]: "changed"})
    assert "dry-run (nothing written)" in md
    assert "| delete | 1 |" in md and "| keep_no_evidence | 2 |" in md
    assert all(d.url[:60] in md for d in decisions)
    assert "delete (changed)" in md


def test_main_refuses_apply_on_an_offline_file(tmp_path):
    path = tmp_path / "rows.json"
    path.write_text(json.dumps(ROWS), encoding="utf-8")
    assert P.main(["--apply", "--input-json", str(path)]) == 2


def test_main_offline_dry_run_writes_step_summary(tmp_path, monkeypatch):
    path = tmp_path / "rows.json"
    path.write_text(json.dumps(ROWS), encoding="utf-8")
    summary = tmp_path / "step.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert P.main(["--input-json", str(path), "--output-dir", str(tmp_path / "o")]) == 0
    assert "| delete | 1 |" in summary.read_text(encoding="utf-8")
    assert os.path.exists(tmp_path / "o" / "decisions.csv")


# ---------------------------------------------------------------------------
# Database helpers and throttling
# ---------------------------------------------------------------------------

def test_fetch_tagged_rows_pages_and_filters(monkeypatch):
    monkeypatch.setattr(P, "PAGE", 2)
    rows = [_row(f"https://e.com/{i}", "Jeep Compass") for i in range(5)]
    client = _Client(rows=rows)
    got = P.fetch_tagged_rows(_Sink(client), ["COMPASS", "Compass", "compass"], "2026-09-12T00:00:00+00:00")
    assert [r["url"] for r in got] == [r["url"] for r in rows]
    assert len(client.calls) == 3
    q = client.calls[0]
    assert q.cols == "*"
    assert ("matched_keywords", "ov", '{"COMPASS","Compass","compass"}') in q.filters
    assert ("found_at", "gte", "2026-09-12T00:00:00+00:00") in q.filters


def test_pg_array_literal_quotes_every_element():
    assert P.pg_array_literal(["Compass", 'a "b"', "c,d"]) == '{"Compass","a \\"b\\"","c,d"}'


def test_label_variants_cover_case_and_configured_forms():
    assert set(P.label_variants(["compass"], ["Compass", "PETROBRAS"])) == {"compass", "COMPASS", "Compass"}


def test_throttled_refetcher_spaces_same_domain_calls_and_caps():
    t = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        t[0] += s

    fetched = []
    r = P.ThrottledRefetcher(lambda url: fetched.append(url) or _page(LONG_BODY),
                             per_domain_s=3.0, cap=3, sleep=sleep, clock=lambda: t[0])
    r({"url": "https://g1.globo.com/a"})
    r({"url": "https://valor.globo.com/b"})
    r({"url": "https://g1.globo.com/c"})
    capped = r({"url": "https://g1.globo.com/d"})
    assert slept == [3.0]
    assert len(fetched) == 3 and not capped.ok and "cap" in capped.reason


def test_domain_round_robin_interleaves():
    rows = [{"url": u} for u in ("https://a.com/1", "https://a.com/2", "https://a.com/3", "https://b.com/1")]
    assert [r["url"] for r in P.domain_round_robin(rows)] == [
        "https://a.com/1", "https://b.com/1", "https://a.com/2", "https://a.com/3"
    ]


# ---------------------------------------------------------------------------
# The next scan cannot put a purged label back
# ---------------------------------------------------------------------------

def test_scanner_upsert_writes_the_fresh_match_and_never_reads_stored_keywords():
    from news_hunter import supabase_sync
    from news_hunter.store import Article

    stored = [{"url": "https://e.com/x", "published_at": "2026-09-14T09:00:00+00:00",
               "source_lang": "es", "title_original": "t", "title_en": "T", "snippet_en": "S",
               "matched_keywords": ["Compass", "gás"]}]
    client = _Client(rows=stored)
    sink = supabase_sync._SupabaseSink.__new__(supabase_sync._SupabaseSink)
    sink.client, sink.table = client, "news_articles"
    now = datetime.now(timezone.utc)
    art = Article(url="https://e.com/x", domain="e.com", source_name="E", title="t",
                  snippet="s", published_at=now, found_at=now, matched_keywords=["gás"],
                  published_is_approx=True, source_lang="es")
    assert sink.push([art]) == 1
    upserts = [q for q in client.calls if q.op == "upsert"]
    assert upserts and upserts[0].payload[0]["matched_keywords"] == ["gás"]
    selects = [q.cols or "" for q in client.calls if q.op == "select"]
    assert selects and not any("matched_keywords" in c for c in selects)
