"""Zero-yield guard: fail the scan LOUDLY when a whole stage produces nothing.

Why this exists (2026-10-19 incident): googlenewsdecoder 0.2.x renamed its
result flag `status` -> `success` on 2026-09-20. The resolver kept reading
`status`, every Google News wrapper stayed undecoded and was dropped, and the
scan logged `resolve=400(ok=0)` every five minutes for four weeks while the run
stayed GREEN. Every GNews-only source (Reuters, Bloomberg, the five foreign
languages, ~110 international domains) went silent and nobody was paged.

The guard compares, per scan, how much each stage got against how much it was
asked to do. A stage tripping is a whole-stage failure, not a dead source:
single dead feeds are already named by the fetcher's "feeds returning 0 items"
line and are not this module's business.

Checks (each needs a minimum sample, so a quiet night cannot trip them):
* gnews_resolve -- decoded wrappers / wrappers submitted to the decoder.
  Healthy is ~95%; 0% means the decoder is broken or blocked.
* registered_feeds -- RSS/sitemap/homepage tasks that returned >= 1 item /
  tasks submitted. Healthy is ~94% (7 of ~120 return 0 on 2026-10-19).
* gnews_queries -- Google News `site:` queries that returned >= 1 item /
  queries submitted. Healthy is ~95%; near 0 means Google blocks the runner.

The scan still persists whatever it got; the guard only decides the exit code
and writes one ERROR line per tripped stage (grep `ZERO-YIELD`).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageRule:
    name: str
    min_total: int      # below this many attempts the ratio is not judged
    min_ratio: float    # trip when ok/total falls below this


RULES: tuple[StageRule, ...] = (
    StageRule("gnews_resolve", min_total=20, min_ratio=0.5),
    StageRule("registered_feeds", min_total=20, min_ratio=0.5),
    StageRule("gnews_queries", min_total=20, min_ratio=0.25),
)


def new_stage_stats() -> dict[str, dict[str, int]]:
    """Empty counters, one {"ok", "total"} pair per guarded stage."""
    return {r.name: {"ok": 0, "total": 0} for r in RULES}


def violations(stage_stats: dict[str, dict[str, int]] | None) -> list[str]:
    """One human-readable message per tripped stage; empty list = healthy.

    A stage absent from `stage_stats`, or below its minimum sample, is not
    judged (e.g. a scan run with include_google_news=False).
    """
    out: list[str] = []
    stats = stage_stats or {}
    for rule in RULES:
        s = stats.get(rule.name)
        if not s:
            continue
        total = s.get("total", 0)
        ok = s.get("ok", 0)
        if total < rule.min_total:
            continue
        ratio = ok / total
        if ratio < rule.min_ratio:
            out.append(
                f"{rule.name} ok={ok}/{total} ({ratio:.0%}) below {rule.min_ratio:.0%}"
            )
    return out
