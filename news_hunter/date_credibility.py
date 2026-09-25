"""Date credibility: never present an old article as new because a feed re-dated it.

INCIDENT (2026-09-25). Kpler's Webflow feed (www.kpler.com/blog/rss.xml) sets
<pubDate> to the item's LAST RE-PUBLISH time. From 10:41 UTC Kpler re-published
its back catalogue and 81 of 100 feed items were suddenly dated "today", while
their own pages said Mar 31 .. Sep 10 (JSON-LD `datePublished`). The scanner
believed the feed: enrich only read the page date when the feed had none, and
it skipped the page fetch altogether when the item already had title + date +
snippet. 75 old posts landed at the top of /news-hunter as new.

Three rules, all generic (no source is special-cased here):

R2  The earliest credible date wins. Whenever the article HTML is at hand, the
    page's own publication date is read -- `article:published_time`, JSON-LD
    `datePublished`, `itemprop="datePublished"`; never `dateModified` -- and it
    replaces the feed date when it is earlier by more than TOLERANCE. Stage 4's
    window then drops the old item. A date printed at the end of the feed title
    in the "<headline> | <Source> - Mon DD, YYYY" form is read the same way
    (it needs no fetch; measured: only kpler.com carries it today).

R3  Re-stamp evidence means verify or defer -- on BATCH evidence only. Legit
    news feeds re-date stored articles all the time (updates); a re-publication
    shows as a batch: RESTAMP_BATCH_MIN re-dated URLs we already store (feed
    date > min(stored published_at, created_at) + TOLERANCE), or as many items
    whose printed title date contradicts the feed date, within
    RESTAMP_BATCH_SPAN of one another. A flagged feed's dates are modification
    times for the scan: each never-seen item must pass R2 with its page
    fetched, inside a per-domain budget. A page that WAS fetched and hides its
    date (Kpler's empty datePublished), or an exhausted budget, DEFERS the item
    (not persisted, counted, retried next scan). A page we could NOT fetch
    (transport error, HTTP >= 400, a WAF challenge) admits the item with the
    feed date, counted as unverified: our own blocks must not become a zero.
    The pipeline owns that phase (pipeline._run_date_credibility); this module
    owns the pure pieces.

T   Clean titles. "<headline> | <Source>( - <date>)?" loses the suffix of the
    item's OWN source name, and a title that ends with the page's <h1> (joined
    by plain whitespace, as Kpler's "<SEO title> <headline>") becomes the h1.

Tolerance: 24 h. It absorbs the timezone noise of naive local timestamps. A
date-only value ("Apr 01, 2026") names a whole day in an unknown timezone, so it
is compared by the END of that day (DAY_SPAN): a post from late yesterday, local
time, is never read as old.

Rows we already store are NOT this module's concern: a database trigger keeps
news_articles.published_at monotone (an update can never move it later).
"""
from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable

from dateutil import parser as date_parser

#: How much EARLIER than the feed date a page (or title) date must be before it
#: overrides the feed date. Also the slack of the R3 re-stamp test.
TOLERANCE = timedelta(hours=24)

#: A date-only value is compared by the end of its day.
DAY_SPAN = timedelta(hours=24)

#: R3 flags a feed on a BATCH of re-dates, never on isolated ones: at least
#: RESTAMP_BATCH_MIN distinct urls whose feed dates fall within
#: RESTAMP_BATCH_SPAN of one another -- counted separately for stored urls the
#: feed re-dated and for items whose printed title date is older than the feed
#: date. Measured 2026-09-25 on news_articles_published_at_clamp_20260925_bak:
#:   * legit feeds re-date stored articles all the time (updates, not
#:     re-publications). Last 30 days, urls re-dated > 24 h after first seen:
#:     wsj 36 (22 of them > 72 h), estadao 14, theedgemalaysia 13, asharq 12,
#:     moneycontrol 10, argus 9, investing.com 8 (all > 72 h), dw 6. Neither
#:     jump size nor age separates them from Kpler; the batch does.
#:   * replayed one scan per 5 minutes over 2026-08-26 .. 09-25 (RSS feeds,
#:     the only ones R3 reads): "any one re-date" would have flagged estadao
#:     for 291 h in 10 episodes; "3 visible in one scan" still twice (24.6 h),
#:     from six unrelated updates in 2.2 h; "3 within 10 minutes" never -- its
#:     closest updates were 6 minutes apart, three of them within 27 minutes,
#:     so a 30-minute span would flag it and 10 does not.
#:   * Kpler on 2026-09-25 re-dated 6 stored posts within 3 min 19 s
#:     (10:41:49 .. 10:45:08) and showed 60 title-date contradictions in one
#:     scan: flagged. investing.com re-dates its four "live levels" pages
#:     together (within 13-33 s) every week: flagged twice in 30 days. Its pages
#:     fail from the runner, so those items are admitted unverified, not lost.
RESTAMP_BATCH_MIN = 3
RESTAMP_BATCH_SPAN = timedelta(minutes=10)

#: Anything outside these bounds is a CMS placeholder (epoch zero, 0001-01-01,
#: a year typo), never a publication date.
_MIN_YEAR = 1995
_MAX_FUTURE = timedelta(days=2)

#: Values longer than this are prose, not a date.
_MAX_DATE_CHARS = 64

#: A 4-digit year must be present: dateutil happily turns "2" into "the 2nd of
#: the current month", and that must never reach a comparison.
_YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")

#: "05/09/2026" is 5 September on a Brazilian page and 9 May to dateutil. A
#: misread month can make a fresh article look months old, so a numeric date
#: whose first two fields could both be the month is refused outright.
_AMBIGUOUS_DMY_RE = re.compile(r"^\s*(\d{1,2})[/.\-](\d{1,2})[/.\-]\d{4}\b")

# Two sentinel "defaults": a component the string does not carry is filled from
# the default, so the two parses disagree exactly on the missing components.
_DEFAULT_A = datetime(2000, 1, 1, 0, 0, 0)
_DEFAULT_B = datetime(2001, 2, 2, 13, 37, 42)


@dataclass(frozen=True)
class ParsedDate:
    """A publication date read from a page or a feed title."""

    value: datetime       # aware, UTC; midnight UTC for a date-only value
    date_only: bool       # True when the source string carried no time of day
    source: str = ""      # where it was read ("meta:article:published_time", ...)
    raw: str = ""

    @property
    def latest(self) -> datetime:
        """The latest instant this value can stand for (end of day if date-only)."""
        return self.value + DAY_SPAN if self.date_only else self.value


def parse_date_value(raw, *, source: str = "", now: datetime | None = None) -> ParsedDate | None:
    """Parse one publication-date string. None for empty, partial or garbage input.

    Accepts every format dateutil understands ("2026-04-01T14:02:58+00:00",
    "Apr 01, 2026", "Wed, 01 Apr 2026 14:02:58 GMT") but only when the string
    carries a full calendar date: year, month and day. A year alone, or a
    "Sep 25" without a year, is rejected rather than completed from today.
    """
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s or len(s) > _MAX_DATE_CHARS or not _YEAR_RE.search(s):
        return None
    amb = _AMBIGUOUS_DMY_RE.match(s)
    if amb and int(amb.group(1)) <= 12 and int(amb.group(2)) <= 12 and amb.group(1) != amb.group(2):
        return None
    try:
        a = date_parser.parse(s, default=_DEFAULT_A)
        b = date_parser.parse(s, default=_DEFAULT_B)
    except (ValueError, TypeError, OverflowError):
        return None
    if a.date() != b.date():
        return None  # year, month or day was missing and got filled in
    date_only = a.time() != b.time() and a.hour != b.hour
    dt = a
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    if date_only:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    ref = now or datetime.now(timezone.utc)
    if dt.year < _MIN_YEAR or dt > ref + _MAX_FUTURE:
        return None
    return ParsedDate(value=dt, date_only=date_only, source=source, raw=s)


def is_older(candidate: ParsedDate | None, feed_date: datetime | None,
             *, tolerance: timedelta = TOLERANCE) -> bool:
    """True when `candidate` proves the item is older than `feed_date` says."""
    if candidate is None or feed_date is None:
        return False
    return candidate.latest < feed_date - tolerance


def credible_published(feed_date: datetime | None, candidate: ParsedDate | None,
                       *, tolerance: timedelta = TOLERANCE) -> datetime | None:
    """R2: the earliest credible date wins.

    The candidate replaces the feed date only when it is earlier by more than
    `tolerance` (a later page date never pushes an item forward). With no feed
    date at all the caller's own fallback applies, so this returns None.
    """
    if feed_date is None:
        return None
    if is_older(candidate, feed_date, tolerance=tolerance):
        return candidate.value  # type: ignore[union-attr]
    return feed_date


# =============================================================================
# The page's own publication date
# =============================================================================

_PUBLISHED_META = (
    {"property": "article:published_time"},
    {"name": "article:published_time"},
)

# JSON-LD entities whose datePublished is the ARTICLE's. schema.org has ~20
# article subtypes (NewsArticle, BlogPosting, ReportageNewsArticle, ...); they
# all end in "Article" or "Posting", plus Report.
_ARTICLE_TYPE_RE = re.compile(r"(?:Article|Posting)$|^Report$")
# Second tier: the page itself. Deliberately NOT WebSite / Organization /
# CollectionPage: their datePublished is the site's or the section's, and
# trusting it would re-date every article of that site to its launch day.
_PAGE_TYPES = frozenset({"WebPage", "ItemPage"})
# JSON-LD keys that lead to the page's main entity. The walk never descends into
# anything else (itemListElement, hasPart, author, ...), where OTHER articles'
# dates live.
_MAIN_ENTITY_KEYS = ("@graph", "mainEntity", "mainEntityOfPage")
_JSONLD_DATE_RE = re.compile(r'"datePublished"\s*:\s*"([^"]{0,64})"')


def _types(node: dict) -> list[str]:
    t = node.get("@type")
    if isinstance(t, str):
        return [t]
    if isinstance(t, list):
        return [x for x in t if isinstance(x, str)]
    return []


def _jsonld_entities(data) -> list[dict]:
    """Page-level JSON-LD entities, in document order (top level, @graph, main entity)."""
    out: list[dict] = []
    stack = [data]
    seen = 0
    while stack and seen < 200:
        node = stack.pop(0)
        seen += 1
        if isinstance(node, list):
            stack[0:0] = node
            continue
        if not isinstance(node, dict):
            continue
        out.append(node)
        for key in _MAIN_ENTITY_KEYS:
            child = node.get(key)
            if isinstance(child, (dict, list)):
                stack.append(child)
    return out


def _jsonld_dates(soup) -> tuple[list[ParsedDate], list[ParsedDate]]:
    """(article-typed dates, page-typed dates) from every ld+json block."""
    articles: list[ParsedDate] = []
    pages: list[ParsedDate] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string if script.string is not None else script.get_text()
        if not text or "datePublished" not in text:
            continue
        try:
            data = json.loads(text, strict=False)
        except (ValueError, TypeError):
            # Broken JSON (trailing commas, stray control characters) is common.
            # The first datePublished of a block is its main entity's.
            m = _JSONLD_DATE_RE.search(text)
            if m:
                parsed = parse_date_value(m.group(1), source="jsonld:unparsed")
                if parsed is not None:
                    articles.append(parsed)
            continue
        for node in _jsonld_entities(data):
            types = _types(node)
            is_article = any(_ARTICLE_TYPE_RE.search(t) for t in types)
            is_page = any(t in _PAGE_TYPES for t in types)
            if not (is_article or is_page):
                continue
            label = "/".join(types) or "?"
            parsed = parse_date_value(node.get("datePublished"), source=f"jsonld:{label}")
            if parsed is None:
                continue
            (articles if is_article else pages).append(parsed)
    return articles, pages


def _itemprop_date(soup) -> ParsedDate | None:
    for tag in soup.find_all(attrs={"itemprop": "datePublished"}, limit=5):
        for attr in ("content", "datetime"):
            parsed = parse_date_value(tag.get(attr), source=f"itemprop:{tag.name}")
            if parsed is not None:
                return parsed
    return None


def page_published_date(soup) -> ParsedDate | None:
    """The page's own publication date, or None. Never dateModified.

    Order: <meta article:published_time>, an article-typed JSON-LD entity, the
    first itemprop="datePublished", a WebPage-typed JSON-LD entity. Empty and
    unparseable values are skipped (Kpler serves `"datePublished": ""` on some
    posts), so a page without a usable date returns None -- it never borrows
    the feed's.
    """
    if soup is None:
        return None
    for attrs in _PUBLISHED_META:
        tag = soup.find("meta", attrs=attrs)
        if tag is not None:
            key = attrs.get("property") or attrs.get("name")
            parsed = parse_date_value(tag.get("content"), source=f"meta:{key}")
            if parsed is not None:
                return parsed
    articles, pages = _jsonld_dates(soup)
    if articles:
        return articles[0]
    parsed = _itemprop_date(soup)
    if parsed is not None:
        return parsed
    if pages:
        return pages[0]
    return None


_WS_RE = re.compile(r"\s+")


def _norm(text: str) -> str:
    return _WS_RE.sub(" ", text or "").strip()


def page_headlines(soup, limit: int = 3) -> tuple[str, ...]:
    """Text of the page's first <h1> elements (whitespace-normalised)."""
    if soup is None:
        return ()
    out: list[str] = []
    for h in soup.find_all("h1", limit=limit * 2):
        text = _norm(h.get_text(" ", strip=True))
        if text and len(text) <= 300:
            out.append(text)
        if len(out) >= limit:
            break
    return tuple(out)


# =============================================================================
# Titles (T) and the date printed in them
# =============================================================================

def _fold(text: str) -> str:
    return _norm(text).casefold()


def split_source_suffix(title: str, source_name: str) -> tuple[str, ParsedDate | None]:
    """'<headline> | <source name>( - <date>)?' -> ('<headline>', date or None).

    Only the item's OWN source name counts as a suffix, and only after the LAST
    "|", so a section label ("Opinion | ...", "... | CNN Politics") is never
    touched. What follows the name must be nothing, a bare " -", or " - " plus
    a full date; anything else leaves the title exactly as it came.
    """
    if not title or not source_name or "|" not in title:
        return title, None
    head, _, tail = title.rpartition("|")
    head = head.rstrip()
    if not _norm(head):
        return title, None
    name = _fold(source_name)
    tail_f = _fold(tail)
    if tail_f == name:
        return head, None
    if not name or not tail_f.startswith(name):
        return title, None
    rest = tail_f[len(name):].strip()
    if not rest.startswith("-"):
        return title, None
    rest = rest[1:].strip()
    if not rest:
        return head, None
    parsed = parse_date_value(rest, source="title-suffix")
    if parsed is None:
        return title, None
    return head, parsed


_LABEL_SEPARATORS = "|:-–—·•»/"
_MIN_HEADLINE_WORDS = 3


def clean_title(title: str, source_name: str, headlines: Iterable[str] = ()) -> str:
    """Display title: suffix stripped, then the h1 if the title ends with it.

    The h1 replaces the title only when (a) the title ends with it, joined by
    plain whitespace -- a prefix ending in a label separator ("Opinion | ",
    "EXCLUSIVO: ") is a label and stays; (b) the h1 has >= 3 words. Without an
    h1, a title that is literally the same text twice ("X X", Kpler's shape
    when its SEO title equals the headline) collapses to one copy. Any other
    title is returned unchanged.
    """
    if not title:
        return title
    stripped, _ = split_source_suffix(title, source_name)
    norm = _norm(stripped)
    folded = norm.casefold()
    for h in headlines:
        hn = _norm(h)
        hf = hn.casefold()
        if len(hn.split()) < _MIN_HEADLINE_WORDS or len(hf) >= len(folded):
            continue
        if not folded.endswith(hf):
            continue
        boundary = len(folded) - len(hf)
        if folded[boundary - 1] != " ":
            continue
        prefix = folded[:boundary].rstrip()
        if not prefix or prefix[-1] in _LABEL_SEPARATORS:
            continue
        return hn
    n = len(norm)
    if n >= 3 and n % 2 == 1 and norm[n // 2] == " " and norm[: n // 2] == norm[n // 2 + 1:]:
        return norm[: n // 2]
    return stripped


# =============================================================================
# R3: re-stamp evidence
# =============================================================================

@dataclass(frozen=True)
class StoredDates:
    """What news_articles already holds for a url (both columns are NOT NULL)."""

    published_at: datetime | None
    created_at: datetime | None

    @property
    def reference(self) -> datetime | None:
        """min(published_at, created_at): created_at is the write-once first-seen
        time, so a published_at that an earlier re-stamp already pushed forward
        cannot hide the next one."""
        vals = [v for v in (self.published_at, self.created_at) if v is not None]
        return min(vals) if vals else None


def parse_stored_timestamp(raw) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        try:
            dt = date_parser.isoparse(str(raw))
        except (ValueError, TypeError, OverflowError):
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def restamps(url: str, feed_date: datetime | None, stored: StoredDates | None,
             *, tolerance: timedelta = TOLERANCE) -> bool:
    """True when the feed dates a url we already store later than we first had it."""
    if feed_date is None or stored is None:
        return False
    ref = stored.reference
    return ref is not None and feed_date > ref + tolerance


def largest_batch(dates: Iterable[datetime], span: timedelta = RESTAMP_BATCH_SPAN) -> int:
    """How many of `dates` fit, at most, inside one window of length `span`."""
    ts = sorted(d for d in dates if d is not None)
    best = lo = 0
    for hi, t in enumerate(ts):
        while t - ts[lo] > span:
            lo += 1
        best = max(best, hi - lo + 1)
    return best


def is_batch(dates: Iterable[datetime], *, minimum: int = RESTAMP_BATCH_MIN,
             span: timedelta = RESTAMP_BATCH_SPAN) -> bool:
    """R3: do these re-date times form a batch (a re-publication, not updates)?"""
    return largest_batch(dates, span) >= minimum


def rotate_budget(items: list, cap: int, *, bucket: int) -> tuple[list, list]:
    """(selected, over_budget): half the cap to the head of `items` (newest
    first), half to a window that rotates with `bucket` over the rest.

    The scanner is stateless: an item dropped as old in one scan comes back in
    the next. Without the rotating half the same newest items would take the
    whole budget every scan and a genuinely new post sitting further down a
    re-stamped feed would never get its turn.
    """
    if cap <= 0:
        return [], list(items)
    if len(items) <= cap:
        return list(items), []
    head_n = (cap + 1) // 2
    head, rest = items[:head_n], items[head_n:]
    window = cap - head_n
    start = (bucket * window) % len(rest) if window else 0
    picked_rest = [rest[(start + i) % len(rest)] for i in range(window)]
    picked = set(id(x) for x in picked_rest)
    selected = head + picked_rest
    over = [x for x in rest if id(x) not in picked]
    return selected, over


# =============================================================================
# Per-scan page evidence
# =============================================================================

@dataclass
class PageEvidence:
    """What one fetched article page said about itself.

    `challenge`: the answer was a bot-wall interstitial, not the article (only
    judged when the page carries no publication date). R3 treats it like a
    failed fetch -- the source did not hide its date, we never saw the page.
    """

    page_date: ParsedDate | None = None
    headlines: tuple[str, ...] = field(default_factory=tuple)
    challenge: bool = False


# A WAF / bot-wall interstitial answered with HTTP 200. Titles of the common
# ones (Cloudflare, Imperva / Incapsula / Distil, Akamai, DataDome, PerimeterX,
# DDoS-Guard, Amazon) and resource paths only their challenges load. Consulted
# only for a page WITHOUT a publication date, so a real article that merely
# embeds a widget from one of these vendors is never reclassified.
# Generic phrases count only as the WHOLE title, optionally followed by
# " | <vendor or site>" ("Attention Required! | Cloudflare"): a headline such
# as "Access denied: the week Iran closed Hormuz" is not an interstitial.
_CHALLENGE_TITLE_RE = re.compile(
    r"^\W*(?:just a moment|attention required|access denied|pardon our interruption"
    r"|are you a robot|robot check|security check|bot verification|ddos-guard"
    r"|403 forbidden|please wait|one moment,? please)[\s.!…]*(?:\|.*)?$",
    re.IGNORECASE,
)
# Distinctive enough to count anywhere in the title.
_CHALLENGE_TITLE_ANYWHERE_RE = re.compile(
    r"incapsula incident id|checking (?:your|the) browser before accessing"
    r"|verify(?:ing)? (?:that )?you are (?:a )?human",
    re.IGNORECASE,
)
_CHALLENGE_TITLE_MAX = 100  # interstitial titles are short
_CHALLENGE_MARKERS = (
    "/cdn-cgi/challenge-platform/", "_cf_chl_opt", "cf-browser-verification",
    "captcha-delivery.com", "_incapsula_resource", "px-captcha",
    "perimeterx", "ddos-guard.net", "/distil_r_captcha",
)


def looks_like_challenge(soup) -> bool:
    """True when the fetched page is a bot-wall interstitial, not an article."""
    if soup is None:
        return False
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if title and len(title) <= _CHALLENGE_TITLE_MAX and (
        _CHALLENGE_TITLE_RE.search(title) or _CHALLENGE_TITLE_ANYWHERE_RE.search(title)
    ):
        return True
    for tag in soup.find_all(["script", "iframe", "form", "link", "meta"], limit=300):
        blob = " ".join(
            str(v) for v in (tag.get("src"), tag.get("action"), tag.get("href"),
                             tag.get("content"), tag.string) if v
        ).lower()
        if blob and any(m in blob for m in _CHALLENGE_MARKERS):
            return True
    return False


_lock = threading.Lock()
_pages: dict[str, PageEvidence] = {}


def reset_scan() -> None:
    """Forget every page seen so far. The pipeline calls it at the start of a scan."""
    with _lock:
        _pages.clear()


def record_page(url: str, soup) -> PageEvidence:
    """Read and remember what a fetched page says about itself (thread-safe).

    Fail-soft: a page whose markup trips the readers yields empty evidence (no
    date, no h1) rather than an exception inside the enrich path.
    """
    try:
        page_date = page_published_date(soup)
        ev = PageEvidence(
            page_date=page_date,
            headlines=page_headlines(soup),
            challenge=page_date is None and looks_like_challenge(soup),
        )
    except Exception:  # noqa: BLE001
        ev = PageEvidence()
    if url:
        with _lock:
            _pages[url] = ev
    return ev


def page_seen(url: str) -> PageEvidence | None:
    with _lock:
        return _pages.get(url)
