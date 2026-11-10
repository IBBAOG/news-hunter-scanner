"""Per-run source selection: SCAN_ONLY_SOURCES / SCAN_SKIP_SOURCES.

Why this exists (2026-11-10): Cloudflare serves a managed challenge (HTTP 403)
to GitHub-hosted runner IPs on every brasilenergia.com.br page, /login
included, so the cloud scan (scan.yml, ubuntu-latest) has collected zero Brasil
Energia items since 2026-11-03 while staying green. From a residential IP the
login passes. So Brasil Energia is scanned by a separate job on the SectorData
self-hosted runner, and the cloud scan skips it:

    cloud scan (scan.yml):      SCAN_SKIP_SOURCES=brasil_energia
    self-hosted BE-only scan:   SCAN_ONLY_SOURCES=brasil_energia

Both variables take a comma-separated list of tokens. A token is either an
ALIAS below (a named outlet that spans several hosts) or a host as registered
in sources.py (``www.`` optional). Matching is by host with ``www.`` stripped,
so ``brasilenergia.com.br`` and ``www.brasilenergia.com.br`` are one source.
Path-scoped Google News entries (``marketwatch.com/story``) match by their host.

Rules:
* Neither variable set -> every source runs (the historical behaviour).
* SCAN_ONLY_SOURCES set -> only those sources run; SCAN_SKIP_SOURCES is then
  applied on top (skip wins).
* A token that names no registered source is a configuration error
  (``SourceFilterError``): a typo in an allowlist would otherwise produce a
  scan of nothing that looks like a quiet night.

The filter is applied wherever a source domain is enumerated (registered
feeds, standard sitemaps, homepage scrapers, every Google News `site:` block),
so a skipped source costs no request at all.
"""
from __future__ import annotations

import os
from functools import lru_cache

ONLY_ENV = "SCAN_ONLY_SOURCES"
SKIP_ENV = "SCAN_SKIP_SOURCES"

#: Named outlets. Values are hosts; they are normalised like any token.
ALIASES: dict[str, tuple[str, ...]] = {
    "brasil_energia": ("brasilenergia.com.br",),
}


class SourceFilterError(ValueError):
    """SCAN_ONLY_SOURCES / SCAN_SKIP_SOURCES names something that is not a source."""


def normalize_host(domain: str) -> str:
    """Lowercased host of a registry entry, without a leading ``www.``."""
    host = (domain or "").strip().lower()
    for scheme in ("https://", "http://"):
        if host.startswith(scheme):
            host = host[len(scheme):]
    host = host.split("/", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def registered_hosts() -> set[str]:
    """Every source host the scanner knows about (normalised)."""
    from .sources import (
        HOMEPAGE_SCRAPERS,
        INTERNATIONAL_RSS_DOMAINS,
        LANGUAGES,
        NO_RSS_DOMAINS,
        RSS_FEEDS,
        STANDARD_SITEMAPS,
    )

    entries: list[str] = []
    entries += list(RSS_FEEDS)
    entries += list(STANDARD_SITEMAPS)
    entries += list(HOMEPAGE_SCRAPERS)
    entries += list(NO_RSS_DOMAINS)
    entries += list(INTERNATIONAL_RSS_DOMAINS)
    for cfg in LANGUAGES.values():
        entries += list(cfg.no_rss_domains or ())
    return {normalize_host(e) for e in entries if e}


def _parse(env_name: str) -> frozenset[str]:
    return _parse_raw(env_name, os.environ.get(env_name, "") or "")


@lru_cache(maxsize=32)
def _parse_raw(env_name: str, raw: str) -> frozenset[str]:
    tokens = [t.strip().lower() for t in raw.split(",") if t.strip()]
    if not tokens:
        return frozenset()
    known = registered_hosts()
    hosts: set[str] = set()
    for tok in tokens:
        targets = ALIASES.get(tok, (tok,))
        for t in targets:
            h = normalize_host(t)
            if h not in known:
                raise SourceFilterError(
                    f"{env_name}: {tok!r} is neither an alias "
                    f"({', '.join(sorted(ALIASES))}) nor a registered source host"
                )
            hosts.add(h)
    return frozenset(hosts)


def only_hosts() -> frozenset[str]:
    """Hosts of SCAN_ONLY_SOURCES (empty = no allowlist)."""
    return _parse(ONLY_ENV)


def skip_hosts() -> frozenset[str]:
    """Hosts of SCAN_SKIP_SOURCES (empty = nothing skipped)."""
    return _parse(SKIP_ENV)


def is_allowlist_run() -> bool:
    """True when SCAN_ONLY_SOURCES restricts this run to a subset of sources."""
    return bool(only_hosts())


def selected_hosts() -> frozenset[str]:
    """The allowlisted hosts minus the skipped ones (empty when no allowlist)."""
    return only_hosts() - skip_hosts()


def is_selected(domain: str) -> bool:
    """Should this run touch `domain` (a registry entry or a URL host)?"""
    host = normalize_host(domain)
    only = only_hosts()
    if only and host not in only:
        return False
    return host not in skip_hosts()


def filter_domains(domains) -> list[str]:
    """Keep the registry entries this run should touch, order preserved."""
    return [d for d in domains if is_selected(d)]


def describe() -> str:
    """One log line naming the active selection, or '' when none is active."""
    only, skip = only_hosts(), skip_hosts()
    parts = []
    if only:
        parts.append(f"only={','.join(sorted(only))}")
    if skip:
        parts.append(f"skip={','.join(sorted(skip))}")
    return " ".join(parts)
