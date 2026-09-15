"""Retro-purge: remove sense-excluded keyword labels from stored news_articles rows.

WHY THIS EXISTS. news_hunter/keyword_senses.py stops NEW hits such as the Jeep
Compass or Vinci Compass from being tagged 'Compass'. Rows already stored keep
their label: the scanner only rewrites matched_keywords for articles it sees
again inside its scan window, and an article whose only keyword is now excluded
is not written at all. This job re-applies the SAME rule (imported, never
copied) to the stored rows.

WHAT IT DECIDES, per row tagged with a keyword that has a SenseExclusion:

  * Judge the keyword on the stored text (title, title_original, title_en,
    snippet, snippet_en) with WHOLE-WORD occurrences.
      - every occurrence off-topic      -> remove the label  (strip_label)
        ... and the array becomes empty -> delete the row    (delete)
      - company context found           -> keep_company
      - protected occurrence            -> keep_<sense>      (keep_nefte_compass)
      - an occurrence left unexplained  -> keep_no_evidence
  * No whole-word occurrence in the stored text (the hit came from the article
    body under substring matching: 'compasso', 'descompasso', 'compassivo'):
      - without --refetch-missing-evidence -> keep_no_evidence
      - with it, re-fetch the article with the scanner's own fetch_html +
        extractor (throttled per domain, capped, deadline) and strip the label
        only when the fetched page has no whole-word occurrence, or only
        excluded-sense ones (strip_label_no_wholeword / delete_no_wholeword). A
        failed fetch or parse, a page that is not the stored article, or a body
        too thin to prove absence -> keep_fetch_failed.

MODES (never more than one):

  (default)            dry-run: decisions + CSV + step summary, no files that
                       could be mistaken for a plan, no writes.
  --plan-out PATH      as the dry-run, plus a JSON BACKUP of every affected full
                       row and a PLAN (the mutations, each with the array it was
                       decided on). Still no database writes.
  --apply-plan PATH    read a plan no older than --plan-max-age-hours (6), check
                       its backup is present, intact and covers every planned
                       row, mutate. The workflow uploads plan + backup as an
                       artifact BEFORE this step runs, so the backup is durable
                       first.
  --apply              local one-shot: plan (backup written first), then mutate.

Every mutation is guarded on url AND the matched_keywords array the decision was
made on. When the guard matches nothing the row is re-read: 'already_applied' if
a previous run already did it (row gone for a planned delete, or already holding
the new array), 'changed' only when someone else really rewrote it. Every
attempt is counted, so a duplicate never hides an earlier result.
--apply and --apply-plan abort unless each target keyword is match_type 'exact'
in the live keyword config: under substring matching the next scan would re-tag
a 'compasso' article still inside the 24 h window.

WHY THE NEXT SCAN CANNOT PUT THE LABEL BACK. supabase_sync writes
matched_keywords straight from the fresh match on every upsert; there is no
write-once / union for that column. The fresh match runs through the same sense
rule, so an excluded label is never re-computed, and an article whose only hit
is excluded is not upserted at all.

Usage:
    python -m scripts.purge_keyword_sense_exclusions                       # dry-run, last 3 days
    python -m scripts.purge_keyword_sense_exclusions --all-history --refetch-missing-evidence \\
        --plan-out purge_output/plan.json
    python -m scripts.purge_keyword_sense_exclusions --apply-plan purge_output/plan.json
    python -m scripts.purge_keyword_sense_exclusions --input-json rows.json --csv out.csv   # offline
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import Counter, OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable
from urllib.parse import urlparse

from news_hunter.keyword_senses import (
    KEYWORD_SENSE_EXCLUSIONS,
    SenseVerdict,
    evaluate_keyword_sense,
    rule_for,
)

log = logging.getLogger("purge_keyword_sense_exclusions")

PAGE = 500
PLAN_VERSION = 1
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
# The workflow applies right after planning; an older plan is refused.
DEFAULT_PLAN_MAX_AGE_HOURS = 6.0


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


def _detail(verdict: SenseVerdict, label: str) -> str:
    """Human reason for a verdict, including partial evidence on a keep."""
    evidence = "; ".join(verdict.evidence)
    if verdict.status == "no_evidence":
        if evidence:
            return (
                f"{verdict.unexplained} of {verdict.occurrences} whole-word '{label}' "
                f"without sense evidence ({evidence})"
            )
        return f"whole-word '{label}', no sense evidence"
    return evidence


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
    where = "refetched body: "
    if verdict.status == "no_occurrence":
        # The whole word sits outside the extracted paragraphs: a list of cars,
        # an asset table, a market-wrap bullet, a related-links rail. Absence is
        # not proven, so judge the text AROUND those occurrences instead.
        near_doc = (
            f"{stored_document(row)} \n {page.page_title} \n "
            f"{_occurrence_windows(page.full_text, label)}"
        )
        verdict = evaluate_keyword_sense(near_doc, label, exact=True)
        where = "refetched page, outside the extracted paragraphs: "
    if verdict.excluded:
        return True, "", where + _detail(verdict, label)
    return False, _keep_action_for(verdict.status, verdict.senses), where + _detail(verdict, label)


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
            reasons.append("stored text: " + _detail(verdict, label))
            continue
        if verdict.status != "no_occurrence":
            keep_actions.append(_keep_action_for(verdict.status, verdict.senses))
            reasons.append("stored text: " + _detail(verdict, label))
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
    """Fetch and parse one article. Any fetch OR parse error -> ok=False."""
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
    try:
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
    except Exception as e:  # noqa: BLE001
        return FetchedPage(False, f"parse error: {type(e).__name__}: {str(e)[:120]}")
    return FetchedPage(
        True,
        page_title=" | ".join(dict.fromkeys(t for t in titles if t)),
        article_text=" ".join(paragraphs),
        full_text=full_text,
    )


class ThrottledRefetcher:
    """Calls `fetch_page` at most once per `per_domain_s` per domain.

    Bounded twice: `cap` fetches in total, and no fetch STARTS after
    `deadline_s` seconds from the first one (a slow outlet can hold a single
    fetch for timeout x 4 through the impersonation retries). Past either bound
    the row gets ok=False, i.e. keep_fetch_failed.
    """

    def __init__(
        self,
        fetch_page: Callable[[str], FetchedPage],
        *,
        per_domain_s: float = 3.0,
        cap: int = 400,
        deadline_s: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.fetch_page = fetch_page
        self.per_domain_s = per_domain_s
        self.cap = cap
        self.deadline_s = deadline_s
        self.sleep = sleep
        self.clock = clock
        self.calls = 0
        self._started: float | None = None
        self._next: dict[str, float] = {}

    def __call__(self, row: dict) -> FetchedPage:
        url = row.get("url") or ""
        if self.calls >= self.cap:
            return FetchedPage(False, f"refetch cap of {self.cap} reached")
        now = self.clock()
        if self._started is None:
            self._started = now
        if self.deadline_s is not None and now - self._started >= self.deadline_s:
            return FetchedPage(False, f"refetch deadline of {self.deadline_s / 60:.0f} min reached")
        domain = urlparse(url).netloc.lower()
        wait = self._next.get(domain, 0.0) - now
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
# Database access and guards
# ---------------------------------------------------------------------------

def pg_array_literal(values: Iterable[str]) -> str:
    """PostgREST array literal with every element quoted ('{"a","b c"}')."""
    items = []
    for v in values:
        s = str(v).replace("\\", "\\\\").replace('"', '\\"')
        items.append(f'"{s}"')
    return "{" + ",".join(items) + "}"


def label_variants(keys: Iterable[str]) -> list[str]:
    out: set[str] = set()
    for k in {k.lower() for k in keys}:
        out.update({k, k.upper(), k.capitalize(), k.title()})
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


def keywords_not_exact(keys: Iterable[str], get_config: Callable[[], dict] | None = None) -> list[str]:
    """Target keywords whose live effective match_type is not 'exact'."""
    if get_config is None:
        from news_hunter.store import get_config as _live_config

        get_config = _live_config
    exact = {k.lower() for k in (get_config().get("exact_keywords") or ()) if k}
    return sorted({k.lower() for k in keys} - exact)


#: Result of one guarded mutation attempt: (url, planned action, result).
#: result: 'ok' (written now), 'already_applied' (a previous run did it: the row
#: is gone for a planned delete, or already holds new_keywords), 'changed' (the
#: row was rewritten by someone else since the plan; nothing written), 'failed'.
Outcome = tuple[str, str, str]


def apply_decision(sink, d: RowDecision) -> str:
    """Mutate one row, guarded on url + the array the decision was made on."""
    snapshot = pg_array_literal(d.matched_keywords)
    is_delete = d.action in (DELETE, DELETE_NW)
    try:
        table = sink.client.table(sink.table)
        if is_delete:
            res = table.delete().eq("url", d.url).eq("matched_keywords", snapshot).execute()
        else:
            res = (
                table.update({"matched_keywords": d.new_keywords})
                .eq("url", d.url).eq("matched_keywords", snapshot).execute()
            )
    except Exception as e:  # noqa: BLE001
        log.warning("mutation failed on %s: %s", d.url, e)
        return "failed"
    if res.data:
        return "ok"
    # The guard matched nothing: tell a resumed / re-applied plan apart from a
    # row the scanner really rewrote.
    try:
        current = (
            sink.client.table(sink.table).select("url, matched_keywords")
            .eq("url", d.url).execute().data or []
        )
    except Exception as e:  # noqa: BLE001
        log.warning("re-select failed on %s: %s", d.url, e)
        return "failed"
    if not current:
        return "already_applied" if is_delete else "changed"
    if not is_delete and list(current[0].get("matched_keywords") or []) == list(d.new_keywords):
        return "already_applied"
    return "changed"


def apply_decisions(sink, decisions: Iterable[RowDecision]) -> list[Outcome]:
    """One Outcome per attempt, so a duplicate never overwrites an earlier result."""
    outcomes: list[Outcome] = [
        (d.url, d.action, apply_decision(sink, d)) for d in decisions if d.mutates
    ]
    oc = Counter(result for _u, _a, result in outcomes)
    log.info("mutations: %s", ", ".join(f"{k}={v}" for k, v in sorted(oc.items())) or "none")
    return outcomes


# ---------------------------------------------------------------------------
# Plan + backup files
# ---------------------------------------------------------------------------

def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def write_plan(
    plan_path: str,
    decisions: list[RowDecision],
    rows: list[dict],
    *,
    keywords: list[str],
    scope: str,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Write the backup (full affected rows) and then the plan. Returns both paths.

    Always writes both files, even with nothing to mutate, so an upload step
    configured with if-no-files-found: error proves the planning step ran.
    """
    affected = [d for d in decisions if d.mutates]
    by_url = {r.get("url"): r for r in rows}
    out_dir = os.path.dirname(os.path.abspath(plan_path))
    os.makedirs(out_dir, exist_ok=True)
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    backup_path = os.path.join(out_dir, f"purge_keyword_sense_exclusions_backup_{stamp}.json")
    with open(backup_path, "w", encoding="utf-8") as fh:
        json.dump(
            [{"row": by_url[d.url], "action": d.action, "new_keywords": d.new_keywords}
             for d in affected],
            fh, ensure_ascii=False, indent=1, default=str,
        )
        fh.flush()
        os.fsync(fh.fileno())
    plan = {
        "version": PLAN_VERSION,
        "created_at": (now or datetime.now(timezone.utc)).isoformat(),
        "scope": scope,
        "keywords": sorted({k.lower() for k in keywords}),
        "backup_file": os.path.basename(backup_path),
        "backup_sha256": _sha256(backup_path),
        "backup_rows": len(affected),
        "mutations": [asdict(d) for d in affected],
    }
    with open(plan_path, "w", encoding="utf-8") as fh:
        json.dump(plan, fh, ensure_ascii=False, indent=1)
    log.info("backup of %d affected rows: %s", len(affected), backup_path)
    log.info("plan written: %s", plan_path)
    return plan_path, backup_path


def load_plan(plan_path: str) -> tuple[dict, list[RowDecision]]:
    """Read a plan and refuse it unless its backup file is present and intact."""
    with open(plan_path, encoding="utf-8") as fh:
        plan = json.load(fh)
    if plan.get("version") != PLAN_VERSION:
        raise ValueError(f"unsupported plan version {plan.get('version')!r}")
    backup = os.path.join(os.path.dirname(os.path.abspath(plan_path)), plan["backup_file"])
    if not os.path.exists(backup):
        raise ValueError(f"backup file missing: {backup}")
    if _sha256(backup) != plan["backup_sha256"]:
        raise ValueError(f"backup file does not match the plan checksum: {backup}")
    mutations = [RowDecision(**m) for m in plan["mutations"]]
    with open(backup, encoding="utf-8") as fh:
        backed_up = {e["row"]["url"] for e in json.load(fh)}
    missing = [d.url for d in mutations if d.url not in backed_up]
    if missing:
        raise ValueError(f"plan lists {len(missing)} rows absent from its backup: {missing[:3]}")
    keeps = [f"{d.action} {d.url}" for d in mutations if not d.mutates]
    if keeps:
        raise ValueError(f"plan lists {len(keeps)} non-mutating actions: {keeps[:3]}")
    return plan, mutations


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
    outcomes: list[Outcome] | None = None,
    scope: str = "",
    backup_path: str | None = None,
    heading: str | None = None,
) -> str:
    counts = Counter(d.action for d in decisions)
    title = heading or ("APPLIED" if applied else "dry-run (nothing written)")
    lines = ["## Keyword sense exclusion purge - " + title, ""]
    if scope:
        lines.append(f"Scope: {scope}  ")
    lines.append(f"Rows in this summary: {len(decisions)}  ")
    if backup_path:
        lines.append(f"Backup of affected rows: `{backup_path}`  ")
    by_row: dict[tuple[str, str], list[str]] = {}
    if outcomes:
        oc = Counter(result for _u, _a, result in outcomes)
        lines.append(
            "Mutation attempts: " + ", ".join(f"{k}={v}" for k, v in sorted(oc.items())) + "  "
        )
        for url, action, result in outcomes:
            by_row.setdefault((url, action), []).append(result)
    lines += ["", "| action | rows |", "|---|---:|"]
    for action, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| {action} | {n} |")
    lines += ["", "| action | url | title | reason |", "|---|---|---|---|"]
    ordered = sorted(decisions, key=lambda d: (not d.mutates, d.action, d.url))
    for d in ordered:
        action = d.action
        results = by_row.get((d.url, d.action), [])
        if any(r != "ok" for r in results):
            action = f"{action} ({', '.join(results)})"
        lines.append(f"| {action} | {_md(d.url, 120)} | {_md(d.title, 100)} | {_md(d.reason)} |")
    return "\n".join(lines) + "\n"


def _emit(summary: str) -> None:
    print(summary)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(summary)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def decide_all(
    rows: list[dict],
    *,
    keywords: list[str] | None,
    refetch: Callable[[dict], FetchedPage] | None,
) -> list[RowDecision]:
    decisions: list[RowDecision] = []
    # A paginated read can return the same url twice if rows shift between
    # pages: decide each url once.
    seen: set[str] = set()
    unique: list[dict] = []
    for row in rows:
        url = row.get("url") or ""
        if url in seen:
            continue
        seen.add(url)
        unique.append(row)
    if len(unique) != len(rows):
        log.warning("ignored %d duplicate rows (same url)", len(rows) - len(unique))
    # Stored-text decisions first; only the rows needing a refetch go to the
    # network, interleaved by domain.
    needs_page: list[dict] = []
    for row in unique:
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
    return decisions


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
    plan_out: str | None = None,
) -> tuple[list[RowDecision], list[Outcome], str | None]:
    """Decide; with plan_out or apply write backup + plan; with apply, mutate.

    The backup and the plan are on disk (fsync'd) before the first mutation.
    """
    decisions = decide_all(rows, keywords=keywords, refetch=refetch)
    if csv_path:
        write_csv(csv_path, decisions)
        log.info("csv written: %s", csv_path)
    backup_path: str | None = None
    if plan_out or apply:
        if apply and (sink is None or getattr(sink, "client", None) is None):
            raise RuntimeError("--apply needs a configured Supabase sink")
        _p, backup_path = write_plan(
            plan_out or os.path.join(backup_dir, "plan.json"),
            decisions, rows,
            keywords=list(keywords or KEYWORD_SENSE_EXCLUSIONS), scope=scope, now=now,
        )
    outcomes: list[Outcome] = apply_decisions(sink, decisions) if apply else []
    return decisions, outcomes, backup_path


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="plan (backup first), then write changes")
    mode.add_argument("--plan-out", default=None, help="write backup + plan to this path; no DB writes")
    mode.add_argument("--apply-plan", default=None, help="mutate from a plan written by --plan-out")
    ap.add_argument("--days", type=int, default=3, help="rows with found_at in the last N days (default 3)")
    ap.add_argument("--all-history", action="store_true", help="ignore --days, scan every tagged row")
    ap.add_argument("--keyword", action="append", default=None,
                    help="restrict to this keyword (repeatable; default: every keyword with a rule)")
    ap.add_argument("--refetch-missing-evidence", action="store_true",
                    help="re-fetch rows whose stored text has no whole-word occurrence")
    ap.add_argument("--refetch-per-domain-seconds", type=float, default=3.0)
    ap.add_argument("--refetch-timeout", type=int, default=15)
    ap.add_argument("--refetch-cap", type=int, default=400)
    ap.add_argument("--refetch-deadline-minutes", type=float, default=60.0,
                    help="no refetch starts after this many minutes (default 60)")
    ap.add_argument("--input-json", default=None,
                    help="read rows from a JSON array instead of Supabase (no --apply)")
    ap.add_argument("--output-dir", default="purge_output", help="backup + csv directory")
    ap.add_argument("--csv", default=None, help="csv report path (default: <output-dir>/decisions.csv)")
    ap.add_argument("--plan-max-age-hours", type=float, default=DEFAULT_PLAN_MAX_AGE_HOURS,
                    help="--apply-plan refuses a plan older than this (default 6)")
    args = ap.parse_args(argv)

    if args.apply_plan:
        return _main_apply_plan(args.apply_plan, max_age_hours=args.plan_max_age_hours)

    keys = [k.lower() for k in (args.keyword or list(KEYWORD_SENSE_EXCLUSIONS))]
    unknown = [k for k in keys if k not in KEYWORD_SENSE_EXCLUSIONS]
    if unknown:
        log.error("no sense rule for: %s", ", ".join(unknown))
        return 2
    if args.apply and args.input_json:
        log.error("--apply cannot be combined with --input-json")
        return 2
    csv_path = args.csv or os.path.join(args.output_dir, "decisions.csv")
    refetch = None
    if args.refetch_missing_evidence:
        refetch = ThrottledRefetcher(
            lambda url: fetch_article_page(url, timeout=args.refetch_timeout),
            per_domain_s=args.refetch_per_domain_seconds,
            cap=args.refetch_cap,
            deadline_s=args.refetch_deadline_minutes * 60,
        )

    sink = None
    if args.input_json:
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
        if args.apply:
            not_exact = keywords_not_exact(keys)
            if not_exact:
                log.error("refusing to apply: match_type is not 'exact' for %s", ", ".join(not_exact))
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
        plan_out=args.plan_out,
    )
    heading = "plan written (nothing written to the database)" if args.plan_out else None
    _emit(render_summary(
        decisions, applied=args.apply, outcomes=outcomes, scope=scope,
        backup_path=backup_path, heading=heading,
    ))
    return 1 if any(result == "failed" for _u, _a, result in outcomes) else 0


def _main_apply_plan(
    plan_path: str,
    *,
    sink=None,
    get_config: Callable[[], dict] | None = None,
    max_age_hours: float = DEFAULT_PLAN_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> int:
    try:
        plan, mutations = load_plan(plan_path)
        created = datetime.fromisoformat(plan["created_at"])
        if created.tzinfo is None:
            raise ValueError("created_at has no timezone")
    except (OSError, ValueError, KeyError, TypeError) as e:
        log.error("refusing plan %s: %s", plan_path, e)
        return 2
    age_h = ((now or datetime.now(timezone.utc)) - created).total_seconds() / 3600
    if age_h > max_age_hours:
        log.error(
            "refusing plan %s: created %.1f h ago, older than --plan-max-age-hours %.1f "
            "(stored rows may have moved on; plan again)", plan_path, age_h, max_age_hours,
        )
        return 2
    if sink is None:
        if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
            log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing - aborting")
            return 2
        from news_hunter.supabase_sync import get_sink

        sink = get_sink()
    if getattr(sink, "client", None) is None:
        log.error("Supabase client unavailable - aborting")
        return 2
    not_exact = keywords_not_exact(plan["keywords"], get_config)
    if not_exact:
        log.error("refusing to apply: match_type is not 'exact' for %s", ", ".join(not_exact))
        return 2
    log.info("applying plan %s: %d mutations (backup %s)", plan_path, len(mutations), plan["backup_file"])
    outcomes = apply_decisions(sink, mutations)
    _emit(render_summary(
        mutations, applied=True, outcomes=outcomes, scope=plan.get("scope", ""),
        backup_path=plan["backup_file"],
    ))
    return 1 if any(result == "failed" for _u, _a, result in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
