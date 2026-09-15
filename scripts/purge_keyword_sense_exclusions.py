"""Retro-purge: remove sense-excluded keyword labels from stored news_articles rows.

WHY THIS EXISTS. news_hunter/keyword_senses.py stops NEW hits such as the Jeep
Compass or Vinci Compass from being tagged 'Compass'. Rows already stored keep
their label: the scanner only rewrites matched_keywords for articles it sees
again inside its scan window, and an article whose only keyword is now excluded
is not written at all. This job re-applies the SAME rule (imported, never
copied) to the stored rows.

WHAT IT DOES, per row tagged with a keyword that has a SenseExclusion:

  * Judge the keyword on the stored text (title, title_original, title_en,
    snippet, snippet_en) with WHOLE-WORD occurrences.
      - every occurrence off-topic      -> remove the label  (strip_label)
        ... and the array becomes empty -> delete the row    (delete)
      - company context found           -> keep_company
      - protected occurrence            -> keep_<sense>      (keep_nefte_compass)
      - whole word, no sense evidence   -> keep_no_evidence
  * No whole-word occurrence in the stored text (the hit came from the article
    body under substring matching: 'compasso', 'descompasso', 'compassivo'):
      - without --refetch-missing-evidence -> keep_no_evidence
      - with it, re-fetch the article with the scanner's own fetch_html +
        extractor (throttled per domain) and strip the label only when the
        fetched page has no whole-word occurrence, or only excluded-sense ones
        (strip_label_no_wholeword / delete_no_wholeword). A failed fetch, a page
        that is not the stored article, or a body too thin to prove absence ->
        keep_fetch_failed.

SAFETY. Dry-run by default. With --apply: every affected full row is written to
a JSON backup FIRST, then each mutation is guarded on the matched_keywords value
that was read (a row the scanner rewrote meanwhile is skipped and counted), then
a summary goes to stdout and $GITHUB_STEP_SUMMARY.

WHY THE NEXT SCAN CANNOT PUT THE LABEL BACK. supabase_sync writes
matched_keywords straight from the fresh match on every upsert; there is no
write-once / union for that column. The fresh match runs through the same sense
rule, so an excluded label is never re-computed, and an article whose only hit
is excluded is not upserted at all. Run this AFTER the 'Compass' match_type is
'exact' in the database (done 2026-09-15), otherwise a substring 'compasso' hit
still inside the 24 h scan window would be re-tagged.

Usage:
    python -m scripts.purge_keyword_sense_exclusions                       # dry-run, last 3 days
    python -m scripts.purge_keyword_sense_exclusions --all-history --refetch-missing-evidence
    python -m scripts.purge_keyword_sense_exclusions --apply --days 3
    python -m scripts.purge_keyword_sense_exclusions --input-json rows.json --csv out.csv   # offline
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable
from urllib.parse import urlparse

from news_hunter.keyword_senses import (
    KEYWORD_SENSE_EXCLUSIONS,
    evaluate_keyword_sense,
    rule_for,
)

log = logging.getLogger("purge_keyword_sense_exclusions")

PAGE = 500
TEXT_FIELDS = ("title", "title_original", "title_en", "snippet", "snippet_en")
CSV_COLUMNS = ("url", "domain", "title", "matched_keywords", "action", "reason")

STRIP = "strip_label"
DELETE = "delete"
STRIP_NW = "strip_label_no_wholeword"
DELETE_NW = "delete_no_wholeword"
KEEP_NO_EVIDENCE = "keep_no_evidence"
KEEP_COMPANY = "keep_company"
KEEP_FETCH_FAILED = "keep_fetch_failed"
MUTATING = (STRIP, DELETE, STRIP_NW, DELETE_NW)

# A refetched page proves ABSENCE of a whole-word occurrence only when it is the
# stored article and carries enough of it. Below this body length we also need
# the substring that produced the original hit ('descompasso') to be on the page.
MIN_BODY_CHARS = 400
MIN_TITLE_OVERLAP = 0.6


# ---------------------------------------------------------------------------
# Pure decision logic (unit-tested with stub rows and a stub fetcher)
# ---------------------------------------------------------------------------

@dataclass
class FetchedPage:
    ok: bool
    reason: str = ""
    page_title: str = ""
    article_text: str = ""
    full_text: str = ""


@dataclass
class RowDecision:
    url: str
    domain: str
    title: str
    matched_keywords: list[str]
    action: str
    reason: str
    new_keywords: list[str] = field(default_factory=list)
    labels_removed: list[str] = field(default_factory=list)
    # True when decided WITHOUT a refetcher and a label had no whole-word
    # occurrence in the stored text: a refetch could still change the outcome.
    pending_refetch: bool = False

    @property
    def mutates(self) -> bool:
        return self.action in MUTATING


def stored_document(row: dict) -> str:
    parts: list[str] = []
    for f in TEXT_FIELDS:
        v = (row.get(f) or "").strip()
        if v and v not in parts:
            parts.append(v)
    return " \n ".join(parts)


def _substring_forms(text: str, keyword: str) -> list[str]:
    pat = re.compile(r"\w*" + re.escape(keyword) + r"\w*", re.IGNORECASE)
    return sorted({m.group(0).lower() for m in pat.finditer(text or "")})


def _occurrence_windows(text: str, keyword: str, radius: int = 250) -> str:
    """The text within `radius` chars of each whole-word occurrence, merged."""
    spans: list[list[int]] = []
    pat = re.compile(r"\b" + re.escape(keyword) + r"\b", re.IGNORECASE)
    for m in pat.finditer(text or ""):
        lo, hi = max(0, m.start() - radius), m.end() + radius
        if spans and lo <= spans[-1][1]:
            spans[-1][1] = max(spans[-1][1], hi)
        else:
            spans.append([lo, hi])
    return " \n ... \n ".join(text[lo:hi] for lo, hi in spans)


def _title_overlap(title: str, haystack: str) -> float:
    tokens = {t for t in re.findall(r"\w{4,}", (title or "").lower())}
    if not tokens:
        return 1.0
    hay = (haystack or "").lower()
    return sum(1 for t in tokens if t in hay) / len(tokens)


def _keep_action_for(status: str, senses: tuple[str, ...]) -> str:
    if status == "keep_context":
        return KEEP_COMPANY
    if status == "protected":
        return f"keep_{senses[0]}" if senses else "keep_protected"
    return KEEP_NO_EVIDENCE


def _judge_refetched(row: dict, label: str, page: FetchedPage) -> tuple[bool, str, str]:
    """(remove?, keep_action_if_not, reason) for a row with no stored whole word."""
    if not page.ok:
        return False, KEEP_FETCH_FAILED, f"refetch failed: {page.reason}"
    overlap = _title_overlap(row.get("title") or "", f"{page.page_title} \n {page.full_text[:4000]}")
    if overlap < MIN_TITLE_OVERLAP:
        return False, KEEP_FETCH_FAILED, (
            f"refetched page is not the stored article (title overlap {overlap:.0%})"
        )
    fetched = f"{page.page_title} \n {page.article_text} \n {page.full_text}"
    whole = evaluate_keyword_sense(fetched, label, exact=True)
    thin_body = len(page.article_text) < MIN_BODY_CHARS
    if whole.status == "no_occurrence":
        forms = _substring_forms(fetched, label)
        if thin_body and not forms:
            return False, KEEP_FETCH_FAILED, (
                f"refetched body too thin to prove absence ({len(page.article_text)} chars, "
                f"no '{label.lower()}' substring on the page)"
            )
        seen = f"; substring on page: {', '.join(forms)}" if forms else ""
        return True, "", f"refetched page has no whole-word '{label}'{seen}"
    # Whole word on the page: judge the article body (not the page chrome) with
    # the stored text, exactly like the scanner judges title + lede.
    body_doc = f"{stored_document(row)} \n {page.page_title} \n {page.article_text}"
    verdict = evaluate_keyword_sense(body_doc, label, exact=True)
    if verdict.status == "no_occurrence":
        # The whole word sits outside the extracted paragraphs: a list of cars,
        # an asset table, a market-wrap bullet, a related-links rail. Absence is
        # not proven, so judge the text AROUND those occurrences instead.
        near_doc = (
            f"{stored_document(row)} \n {page.page_title} \n "
            f"{_occurrence_windows(page.full_text, label)}"
        )
        near = evaluate_keyword_sense(near_doc, label, exact=True)
        where = "refetched page, outside the extracted paragraphs: "
        if near.excluded:
            return True, "", where + "; ".join(near.evidence)
        keep = _keep_action_for(near.status, near.senses)
        return False, keep, where + (
            "; ".join(near.evidence) or f"whole-word '{label}', no sense evidence"
        )
    if verdict.excluded:
        return True, "", "refetched body: " + "; ".join(verdict.evidence)
    keep = _keep_action_for(verdict.status, verdict.senses)
    detail = "; ".join(verdict.evidence) or f"whole-word '{label}', no sense evidence"
    return False, keep, "refetched body: " + detail


def decide_row(
    row: dict,
    *,
    keywords: Iterable[str] | None = None,
    refetch: Callable[[dict], FetchedPage] | None = None,
) -> RowDecision | None:
    """Decide one stored row. None when it carries no label with a sense rule."""
    only = {k.lower() for k in keywords} if keywords else None
    labels = list(row.get("matched_keywords") or [])
    targets = [
        k for k in labels
        if rule_for(k) is not None and (only is None or k.lower() in only)
    ]
    if not targets:
        return None
    doc = stored_document(row)
    removed: list[str] = []
    reasons: list[str] = []
    keep_actions: list[str] = []
    via_refetch = False
    pending = False
    page: FetchedPage | None = None
    for label in targets:
        verdict = evaluate_keyword_sense(doc, label, exact=True)
        if verdict.excluded:
            removed.append(label)
            reasons.append("stored text: " + "; ".join(verdict.evidence))
            continue
        if verdict.status != "no_occurrence":
            keep_actions.append(_keep_action_for(verdict.status, verdict.senses))
            reasons.append(
                "stored text: " + ("; ".join(verdict.evidence)
                                   or f"whole-word '{label}', no sense evidence")
            )
            continue
        if refetch is None:
            pending = True
            forms = _substring_forms(doc, label)
            seen = f" (substring: {', '.join(forms)})" if forms else ""
            keep_actions.append(KEEP_NO_EVIDENCE)
            reasons.append(f"no whole-word '{label}' in stored text{seen}")
            continue
        if page is None:
            page = refetch(row)
        remove, keep, reason = _judge_refetched(row, label, page)
        reasons.append(reason)
        if remove:
            removed.append(label)
            via_refetch = True
        else:
            keep_actions.append(keep)

    new_keywords = [k for k in labels if k not in removed]
    if removed:
        if new_keywords:
            action = STRIP_NW if via_refetch else STRIP
        else:
            action = DELETE_NW if via_refetch else DELETE
    else:
        action = keep_actions[0]
    return RowDecision(
        url=row.get("url") or "",
        domain=row.get("domain") or "",
        title=row.get("title") or "",
        matched_keywords=labels,
        action=action,
        reason=" | ".join(reasons),
        new_keywords=new_keywords,
        labels_removed=removed,
        pending_refetch=pending,
    )


# ---------------------------------------------------------------------------
# Refetch (scanner's own fetcher + extractor), throttled per domain
# ---------------------------------------------------------------------------

def fetch_article_page(url: str, *, timeout: int = 15) -> FetchedPage:
    from bs4 import BeautifulSoup

    from news_hunter._clipinator_shim import (
        _extract,
        _json_ld_article_body,
        clean_paragraphs,
        fetch_html,
        resolve_extractor_domain,
    )

    try:
        html = fetch_html(url, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        return FetchedPage(False, f"{type(e).__name__}: {str(e)[:120]}")
    if not html or len(html) < 500:
        return FetchedPage(False, f"empty page ({len(html or '')} bytes)")
    domain = urlparse(url).netloc.lower()
    paragraphs: list[str] = []
    if resolve_extractor_domain(domain) is not None:
        try:
            _t, paragraphs = _extract(html, domain)
        except Exception as e:  # noqa: BLE001
            log.debug("extractor failed on %s: %s", url, e)
    soup = BeautifulSoup(html, "lxml")
    if not paragraphs:
        paragraphs = clean_paragraphs(_json_ld_article_body(soup))
    titles: list[str] = []
    for attrs in ({"property": "og:title"}, {"name": "twitter:title"}):
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            titles.append(tag["content"].strip())
    if soup.title and soup.title.string:
        titles.append(soup.title.string.strip())
    titles.extend(h.get_text(" ", strip=True) for h in soup.find_all("h1")[:3])
    desc = soup.find("meta", attrs={"property": "og:description"}) or soup.find(
        "meta", attrs={"name": "description"}
    )
    for tag in soup(["script", "style", "noscript", "template", "svg"]):
        tag.decompose()
    if not paragraphs:
        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        paragraphs = [p for p in paragraphs if len(p) > 40]
    full_text = soup.get_text(" ", strip=True)
    if desc is not None and desc.get("content"):
        full_text = f"{desc['content']} \n {full_text}"
    return FetchedPage(
        True,
        page_title=" | ".join(dict.fromkeys(t for t in titles if t)),
        article_text=" ".join(paragraphs),
        full_text=full_text,
    )


class ThrottledRefetcher:
    """Calls `fetch_page` at most once per `per_domain_s` per domain, `cap` total."""

    def __init__(
        self,
        fetch_page: Callable[[str], FetchedPage],
        *,
        per_domain_s: float = 3.0,
        cap: int = 400,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.fetch_page = fetch_page
        self.per_domain_s = per_domain_s
        self.cap = cap
        self.sleep = sleep
        self.clock = clock
        self.calls = 0
        self._next: dict[str, float] = {}

    def __call__(self, row: dict) -> FetchedPage:
        url = row.get("url") or ""
        if self.calls >= self.cap:
            return FetchedPage(False, f"refetch cap of {self.cap} reached")
        domain = urlparse(url).netloc.lower()
        wait = self._next.get(domain, 0.0) - self.clock()
        if wait > 0:
            self.sleep(wait)
        self.calls += 1
        try:
            return self.fetch_page(url)
        finally:
            self._next[domain] = self.clock() + self.per_domain_s


def domain_round_robin(rows: list[dict]) -> list[dict]:
    """Interleave rows by domain so one outlet never takes a long serial run."""
    buckets: "OrderedDict[str, list[dict]]" = OrderedDict()
    for r in rows:
        buckets.setdefault(urlparse(r.get("url") or "").netloc.lower(), []).append(r)
    out: list[dict] = []
    while buckets:
        for dom in list(buckets):
            out.append(buckets[dom].pop(0))
            if not buckets[dom]:
                del buckets[dom]
    return out


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

def pg_array_literal(values: Iterable[str]) -> str:
    """PostgREST array literal with every element quoted ('{"a","b c"}')."""
    items = []
    for v in values:
        s = str(v).replace("\\", "\\\\").replace('"', '\\"')
        items.append(f'"{s}"')
    return "{" + ",".join(items) + "}"


def label_variants(keys: Iterable[str], configured: Iterable[str] = ()) -> list[str]:
    keys = {k.lower() for k in keys}
    out: set[str] = set()
    for k in keys:
        out.update({k, k.upper(), k.capitalize(), k.title()})
    out.update(c for c in configured if c and c.lower() in keys)
    return sorted(out)


def fetch_tagged_rows(sink, variants: list[str], since_iso: str | None) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    literal = pg_array_literal(variants)
    while True:
        q = (
            sink.client.table(sink.table).select("*")
            .filter("matched_keywords", "ov", literal)
        )
        if since_iso:
            q = q.gte("found_at", since_iso)
        page = q.order("url").range(offset, offset + PAGE - 1).execute().data or []
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        offset += PAGE


def apply_decision(sink, d: RowDecision) -> str:
    """Mutate one row, guarded on the array we read. 'ok' | 'changed' | 'failed'."""
    snapshot = pg_array_literal(d.matched_keywords)
    table = sink.client.table(sink.table)
    try:
        if d.action in (DELETE, DELETE_NW):
            res = table.delete().eq("url", d.url).eq("matched_keywords", snapshot).execute()
        else:
            res = (
                table.update({"matched_keywords": d.new_keywords})
                .eq("url", d.url).eq("matched_keywords", snapshot).execute()
            )
    except Exception as e:  # noqa: BLE001
        log.warning("mutation failed on %s: %s", d.url, e)
        return "failed"
    return "ok" if (res.data or []) else "changed"


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def write_csv(path: str, decisions: list[RowDecision]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_COLUMNS)
        for d in decisions:
            w.writerow([d.url, d.domain, d.title, "|".join(d.matched_keywords), d.action, d.reason])


def _md(s: str, limit: int = 160) -> str:
    s = re.sub(r"\s+", " ", s or "").replace("|", "\\|")
    return s if len(s) <= limit else s[: limit - 3] + "..."


def render_summary(
    decisions: list[RowDecision],
    *,
    applied: bool,
    outcomes: dict[str, str] | None = None,
    scope: str = "",
    backup_path: str | None = None,
) -> str:
    counts = Counter(d.action for d in decisions)
    lines = [
        "## Keyword sense exclusion purge - " + ("APPLIED" if applied else "dry-run (nothing written)"),
        "",
    ]
    if scope:
        lines.append(f"Scope: {scope}  ")
    lines.append(f"Rows tagged with a keyword that has a sense rule: {len(decisions)}  ")
    if backup_path:
        lines.append(f"Backup of affected rows: `{backup_path}`  ")
    if outcomes:
        oc = Counter(outcomes.values())
        lines.append(
            "Mutations: " + ", ".join(f"{k}={v}" for k, v in sorted(oc.items())) + "  "
        )
    lines += ["", "| action | rows |", "|---|---:|"]
    for action, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| {action} | {n} |")
    lines += ["", "| action | url | title | reason |", "|---|---|---|---|"]
    ordered = sorted(decisions, key=lambda d: (not d.mutates, d.action, d.url))
    for d in ordered:
        action = d.action
        if outcomes and d.url in outcomes and outcomes[d.url] != "ok":
            action = f"{action} ({outcomes[d.url]})"
        lines.append(f"| {action} | {_md(d.url, 120)} | {_md(d.title, 100)} | {_md(d.reason)} |")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(
    rows: list[dict],
    *,
    keywords: list[str] | None,
    refetch: Callable[[dict], FetchedPage] | None,
    apply: bool,
    sink=None,
    backup_dir: str = "purge_output",
    csv_path: str | None = None,
    scope: str = "",
    now: datetime | None = None,
) -> tuple[list[RowDecision], dict[str, str], str | None]:
    decisions: list[RowDecision] = []
    # Stored-text decisions first; only the rows needing a refetch go to the
    # network, interleaved by domain.
    needs_page: list[dict] = []
    for row in rows:
        d = decide_row(row, keywords=keywords, refetch=None)
        if d is None:
            continue
        if refetch is not None and d.pending_refetch:
            needs_page.append(row)
            continue
        decisions.append(d)
    if needs_page:
        log.info("refetching %d rows with no whole-word occurrence in stored text", len(needs_page))
    for i, row in enumerate(domain_round_robin(needs_page), 1):
        d = decide_row(row, keywords=keywords, refetch=refetch)
        if d is not None:
            decisions.append(d)
        if i % 25 == 0:
            log.info("  refetched %d/%d", i, len(needs_page))

    counts = Counter(d.action for d in decisions)
    log.info("decisions: %s", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none")
    if csv_path:
        write_csv(csv_path, decisions)
        log.info("csv written: %s", csv_path)

    outcomes: dict[str, str] = {}
    backup_path: str | None = None
    affected = [d for d in decisions if d.mutates]
    if apply and affected:
        if sink is None or getattr(sink, "client", None) is None:
            raise RuntimeError("--apply needs a configured Supabase sink")
        by_url = {r.get("url"): r for r in rows}
        stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"purge_keyword_sense_exclusions_backup_{stamp}.json")
        with open(backup_path, "w", encoding="utf-8") as fh:
            json.dump(
                [
                    {"row": by_url[d.url], "action": d.action, "new_keywords": d.new_keywords}
                    for d in affected
                ],
                fh, ensure_ascii=False, indent=1, default=str,
            )
        log.info("backup of %d affected rows written: %s", len(affected), backup_path)
        for d in affected:
            outcomes[d.url] = apply_decision(sink, d)
        oc = Counter(outcomes.values())
        log.info("mutations: %s", ", ".join(f"{k}={v}" for k, v in sorted(oc.items())))
    return decisions, outcomes, backup_path


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    ap.add_argument("--days", type=int, default=3, help="rows with found_at in the last N days (default 3)")
    ap.add_argument("--all-history", action="store_true", help="ignore --days, scan every tagged row")
    ap.add_argument("--keyword", action="append", default=None,
                    help="restrict to this keyword (repeatable; default: every keyword with a rule)")
    ap.add_argument("--refetch-missing-evidence", action="store_true",
                    help="re-fetch rows whose stored text has no whole-word occurrence")
    ap.add_argument("--refetch-per-domain-seconds", type=float, default=3.0)
    ap.add_argument("--refetch-timeout", type=int, default=15)
    ap.add_argument("--refetch-cap", type=int, default=400)
    ap.add_argument("--input-json", default=None,
                    help="read rows from a JSON array instead of Supabase (dry-run only)")
    ap.add_argument("--output-dir", default="purge_output", help="backup + csv directory")
    ap.add_argument("--csv", default=None, help="csv report path (default: <output-dir>/decisions.csv)")
    args = ap.parse_args(argv)

    keys = [k.lower() for k in (args.keyword or list(KEYWORD_SENSE_EXCLUSIONS))]
    unknown = [k for k in keys if k not in KEYWORD_SENSE_EXCLUSIONS]
    if unknown:
        log.error("no sense rule for: %s", ", ".join(unknown))
        return 2
    csv_path = args.csv or os.path.join(args.output_dir, "decisions.csv")
    refetch = None
    if args.refetch_missing_evidence:
        refetch = ThrottledRefetcher(
            lambda url: fetch_article_page(url, timeout=args.refetch_timeout),
            per_domain_s=args.refetch_per_domain_seconds,
            cap=args.refetch_cap,
        )

    sink = None
    if args.input_json:
        if args.apply:
            log.error("--apply cannot be combined with --input-json")
            return 2
        with open(args.input_json, encoding="utf-8") as fh:
            rows = json.load(fh)
        variants = set(label_variants(keys))
        rows = [r for r in rows if variants & set(r.get("matched_keywords") or [])]
        scope = f"offline file {args.input_json}"
    else:
        if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
            log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing - aborting")
            return 2
        from news_hunter.supabase_sync import get_sink

        sink = get_sink()
        if sink.client is None:
            log.error("Supabase client unavailable - aborting")
            return 2
        since = None
        if not args.all_history:
            since = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
        rows = fetch_tagged_rows(sink, label_variants(keys), since)
        scope = "all history" if since is None else f"found_at >= {since}"
    log.info("rows tagged with %s: %d (%s)", ", ".join(keys), len(rows), scope)

    decisions, outcomes, backup_path = run(
        rows,
        keywords=keys,
        refetch=refetch,
        apply=args.apply,
        sink=sink,
        backup_dir=args.output_dir,
        csv_path=csv_path,
        scope=scope,
    )
    summary = render_summary(
        decisions, applied=args.apply, outcomes=outcomes, scope=scope, backup_path=backup_path
    )
    print(summary)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(summary)
    return 1 if any(v == "failed" for v in outcomes.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
