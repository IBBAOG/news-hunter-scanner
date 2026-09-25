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
    page's own publication date is read -- the article's own first (an
    article-typed JSON-LD entity, the main article's itemprop="datePublished"),
    then `article:published_time`, `<meta name="date">`, a WebPage entity;
    never `dateModified`, never a bare `<time>`, and none at all when two of
    them disagree by more than TOLERANCE -- and it replaces the feed date when
    it is earlier by more than TOLERANCE; the window then drops the old item.
    A date printed at the end of the feed title in the
    "<headline> | <Source> - Mon DD, YYYY" form is read the same way (it needs
    no fetch; measured: only kpler.com carries it today). Every never-seen item
    of a domain's own feed gets a budgeted page check (the spot check), so R2
    runs even in fast mode, where no page used to be read. The pipeline decides
    R2 for the whole scan at once (pipeline._run_date_credibility, and the
    snippet backfill), so that a date several new items share -- a template
    constant, not a publication date -- is recognised (TEMPLATE_MIN) and
    ignored instead of dropping them all.

R3  Re-stamp evidence means verify or defer -- on BATCH evidence only. Legit
    news feeds re-date stored articles all the time (updates); a re-publication
    shows as a batch: RESTAMP_BATCH_MIN witnesses within RESTAMP_BATCH_SPAN of
    one another, counting together stored URLs the feed re-dates (feed date >
    min(stored published_at, created_at) + TOLERANCE), items whose printed
    title date contradicts the feed date, and never-seen items whose page the
    spot check proved older. A flagged feed's dates are modification times for
    the scan: each never-seen item must pass R2 with its page read, inside a
    per-domain budget. A page read without a date (Kpler's empty
    datePublished), or an exhausted budget, DEFERS the item (not persisted,
    counted, retried next scan; no age-out -- see the README). A page we could
    NOT read defers too, unless the site answered it with a 4xx or a bot-wall
    challenge and no other page of the site was read this scan: only a site
    that blocks us admits its items with the feed date, counted as unverified,
    so our own blocks do not become a zero. A check that did not complete (our
    deadline) is no answer at all: the item is deferred. The pipeline owns that
    phase (pipeline._run_date_credibility); this module owns the pure pieces.

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
from urllib.parse import urlparse

from dateutil import parser as date_parser

#: How much EARLIER than the feed date a page (or title) date must be before it
#: overrides the feed date. Also the slack of the R3 re-stamp test.
TOLERANCE = timedelta(hours=24)

#: A date-only value is compared by the end of its day.
DAY_SPAN = timedelta(hours=24)

#: R3 flags a feed on a BATCH of re-dates, never on isolated ones: at least
#: RESTAMP_BATCH_MIN distinct urls whose feed dates fall within
#: RESTAMP_BATCH_SPAN of one another -- stored urls the feed re-dated, items
#: whose printed title date is older than the feed date and never-seen items
#: whose page proved them older, counted together. Measured 2026-09-25 on
#: news_articles_published_at_clamp_20260925_bak:
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

#: Feed-intrinsic re-stamp signal, read off one fetched feed alone (no page, no
#: database): MOST of the feed carries a date from the last SPIKE_WINDOW, yet
#: the feed as a whole spans at least SPIKE_MIN_SPAN -- "everything is new right
#: now" in a feed that normally holds weeks. That is the bulk re-stamp
#: signature, and it catches a re-publish made only of posts we never stored
#: (no stored date to contradict, no title date, no page date: Kpler had 32 of
#: them on 2026-09-25). Kpler at 11:52 UTC that day: 81 of 100 items within
#: 2 h, span 26 days. A high-volume news feed is dense in the last hours too,
#: but spans a day or two; a quiet feed on a busy day does not reach half.
SPIKE_MIN_ITEMS = 10
SPIKE_FRESH_SHARE = 0.5
SPIKE_WINDOW = timedelta(hours=2)
SPIKE_MIN_SPAN = timedelta(days=7)

#: Template guard: when this many never-seen items of one feed carry the SAME
#: older page date in one scan, the date is a CMS constant, not theirs -- the
#: feed's page dates are ignored for the scan (the items count as dateless)
#: instead of dropping every new article it publishes.
TEMPLATE_MIN = 3

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
        if a.date() != b.date():
            return None  # year, month or day was missing and got filled in
        # An hour without minutes ("10h", "10:00" without seconds) is a time.
        date_only = a.time() != b.time() and a.hour != b.hour
        # The conversion belongs inside the try: dateutil happily builds an
        # offset of 24 h or more ("+99:00", "+2400", "UTC+30") that raises
        # only when first used -- here, not in the caller's scan.
        dt = a.replace(tzinfo=timezone.utc) if a.tzinfo is None else a.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None
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


# =============================================================================
# The page's own publication date
# =============================================================================

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


def _jsonld_dates(texts: Iterable[str]) -> tuple[list[ParsedDate], list[ParsedDate]]:
    """(article-typed dates, page-typed dates) from the page's ld+json blocks."""
    articles: list[ParsedDate] = []
    pages: list[ParsedDate] = []
    for text in texts:
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


_WS_RE = re.compile(r"\s+")


def _norm(text: str) -> str:
    return _WS_RE.sub(" ", text or "").strip()


@dataclass
class PageSignals:
    """Every publication-date signal of one page, read in ONE tree traversal.

    Not read, on purpose: a bare `<time datetime>` (sidebars, related-article
    cards and "updated" stamps all carry one), and an itemprop="datePublished"
    that does not belong to the page's main article (see _main_article_dates):
    a related card, a comment or a sidebar would re-date a new article to a
    neighbour's older date.
    """

    jsonld_article: list[ParsedDate] = field(default_factory=list)  # article-typed JSON-LD entities
    itemprop: list[ParsedDate] = field(default_factory=list)        # itemprop="datePublished" of the main article
    meta_published: ParsedDate | None = None   # <meta property|name="article:published_time">
    meta_date: ParsedDate | None = None        # <meta name="date">
    jsonld_page: list[ParsedDate] = field(default_factory=list)     # WebPage-typed JSON-LD entities
    headlines: tuple[str, ...] = ()            # first <h1> texts
    title: str = ""                            # <title>
    # src / action / href / content of script, iframe, form, link and meta
    # tags plus the head of inline scripts: what a bot-wall interstitial loads
    # (looks_like_challenge reads it; kept so no second traversal is needed).
    resources: list[str] = field(default_factory=list)

    def candidates(self) -> list[ParsedDate]:
        """Every publication date the page states, the article's own first."""
        out = [*self.jsonld_article, *self.itemprop]
        out += [d for d in (self.meta_published, self.meta_date) if d is not None]
        return out + self.jsonld_page

    @property
    def conflict(self) -> bool:
        """Two of the page's publication dates disagree by more than TOLERANCE."""
        return dates_disagree(self.candidates())

    @property
    def published(self) -> ParsedDate | None:
        """The page's own publication date, or None.

        The article's own date comes first: an article-typed JSON-LD entity,
        then the main article's itemprop, then <meta article:published_time>,
        <meta name="date"> and a WebPage entity. When two of them disagree by
        more than TOLERANCE the page trusts NONE of them: one is wrong, and a
        wrong older date would drop a genuinely new article.
        """
        cands = self.candidates()
        if not cands or dates_disagree(cands):
            return None
        return cands[0]


def dates_disagree(dates: Iterable[ParsedDate], *, tolerance: timedelta = TOLERANCE) -> bool:
    """True when two of `dates` are more than `tolerance` apart.

    A date-only value stands for its whole day, so the distance is measured
    between the spans [value, latest]: "Sep 24, 2026" and 2026-09-24T21:00Z
    agree.
    """
    ds = [d for d in dates if d is not None]
    if len(ds) < 2:
        return False
    return max(d.value for d in ds) - min(d.latest for d in ds) > tolerance


_SCAN_NAMES = frozenset({"title", "meta", "script", "h1", "time", "iframe", "form", "link", "article"})
_INLINE_SCRIPT_HEAD = 2000   # challenge markers sit at the top of an inline script
# Page furniture: an itemprop date inside one of these is never the article's.
_OFF_CONTENT = frozenset({"aside", "nav", "footer"})


def _itemprops(tag) -> list[str]:
    v = tag.attrs.get("itemprop")
    if not v:
        return []
    return v.split() if isinstance(v, str) else [str(x) for x in v]


def _scan_filter(tag) -> bool:
    return (tag.name in _SCAN_NAMES
            or "itemscope" in tag.attrs
            or "datePublished" in _itemprops(tag))


def _item_type(tag) -> str:
    """Last path segment of the microdata itemtype ("http://schema.org/NewsArticle" -> "NewsArticle")."""
    v = tag.attrs.get("itemtype")
    if not v:
        return ""
    first = (v.split() if isinstance(v, str) else [str(x) for x in v] or [""])[0]
    return first.rstrip("/").rsplit("/", 1)[-1]


def _is_article_scope(tag) -> bool:
    """An <article> element, or a microdata item of an article type."""
    if "itemscope" in tag.attrs:
        t = _item_type(tag)
        if t:
            return bool(_ARTICLE_TYPE_RE.search(t))
    return tag.name == "article"


def _owner(tag):
    """(the article scope `tag` belongs to or None, rejected?).

    Walks up from `tag`: the first <article> / article-typed item is the owner.
    Page furniture (aside, nav, footer) or a microdata item of another kind
    (Comment, Person, Review...) reached first rejects the tag: its date is
    not the article's. A WebPage item is the page itself, transparent.
    """
    for p in tag.parents:
        if p.name in _OFF_CONTENT:
            return None, True
        if "itemscope" in p.attrs:
            if _item_type(p) in _PAGE_TYPES:
                continue
            if _is_article_scope(p):
                return p, False
            return None, True
        if p.name == "article":
            return p, False
    return None, False


def _main_article_dates(itemprop_tags, h1_tags, scopes) -> list[ParsedDate]:
    """The itemprop="datePublished" values that belong to the page's MAIN article.

    The main article is the scope (<article> or article-typed microdata item)
    that holds the page's headline: the first <h1> inside such a scope. An
    itemprop counts when that scope owns it. On a page without any article
    scope, an itemprop outside page furniture counts. Anything else -- a
    related-article card, a comment, a sidebar, or article scopes none of
    which holds the headline -- is not read: many sites mark their teaser
    cards up as <article>, and a lone card must not pass for the page.
    """
    content = [s for s in scopes if not any(p.name in _OFF_CONTENT for p in s.parents)]
    main = None
    for h in h1_tags:
        owner, rejected = _owner(h)
        if owner is not None and not rejected:
            main = owner
            break
    out: list[ParsedDate] = []
    for tag in itemprop_tags:
        owner, rejected = _owner(tag)
        if rejected:
            continue
        if main is not None:
            if owner is not main:
                continue
        elif content or owner is not None:
            continue
        name = tag.name
        parsed = (parse_date_value(tag.get("content"), source=f"itemprop:{name}")
                  or parse_date_value(tag.get("datetime"), source=f"itemprop:{name}"))
        if parsed is not None:
            out.append(parsed)
    return out


def read_page_signals(soup) -> PageSignals:
    """One pass over the tree (the old reader walked it up to five times:
    9-26 ms per page on 210 KB Kpler pages, on top of the parse itself)."""
    sig = PageSignals()
    if soup is None:
        return sig
    ld_texts: list[str] = []
    h1s: list[str] = []
    h1_tags: list = []
    itemprop_tags: list = []
    scopes: list = []
    for tag in soup.find_all(_scan_filter):
        name = tag.name
        if name == "meta":
            prop = (tag.get("property") or "").strip().lower()
            meta_name = (tag.get("name") or "").strip().lower()
            if sig.meta_published is None and "article:published_time" in (prop, meta_name):
                sig.meta_published = parse_date_value(
                    tag.get("content"), source="meta:article:published_time")
            if sig.meta_date is None and meta_name == "date":
                sig.meta_date = parse_date_value(tag.get("content"), source="meta:date")
        elif name == "script":
            if (tag.get("type") or "").strip().lower() == "application/ld+json":
                text = tag.string if tag.string is not None else tag.get_text()
                if text:
                    ld_texts.append(text)
            elif tag.string:
                sig.resources.append(tag.string[:_INLINE_SCRIPT_HEAD])
        elif name == "h1":
            if len(h1_tags) < 5:
                h1_tags.append(tag)
            if len(h1s) < 3:
                text = _norm(tag.get_text(" ", strip=True))
                if text and len(text) <= 300:
                    h1s.append(text)
        elif name == "title" and not sig.title:
            sig.title = tag.get_text(" ", strip=True)
        for attr in ("src", "action", "href", "content"):
            value = tag.attrs.get(attr)
            if value and name in ("script", "iframe", "form", "link", "meta"):
                sig.resources.append(value if isinstance(value, str) else " ".join(value))
        if _is_article_scope(tag):
            scopes.append(tag)
        if "datePublished" in _itemprops(tag):
            itemprop_tags.append(tag)
    sig.jsonld_article, sig.jsonld_page = _jsonld_dates(ld_texts)
    if itemprop_tags:
        sig.itemprop = _main_article_dates(itemprop_tags, h1_tags, scopes)
    sig.headlines = tuple(h1s)
    return sig


def page_published_date(soup) -> ParsedDate | None:
    """The page's own publication date, or None. Never dateModified.

    See PageSignals.published: the article's own date first, and no date at
    all when two of the page's dates disagree. Empty and unparseable values
    are skipped (Kpler serves `"datePublished": ""` on some posts), so a page
    without a usable date returns None -- it never borrows the feed's.
    """
    return read_page_signals(soup).published


def page_headlines(soup) -> tuple[str, ...]:
    """Text of the page's first <h1> elements (whitespace-normalised)."""
    return read_page_signals(soup).headlines


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


def clean_display_title(title: str, source_name: str, headlines: Iterable[str] = ()) -> str:
    """Display title: suffix stripped, then the h1 if the title ends with it.

    Not _clipinator_shim.clean_title, which strips ANY registered outlet name
    after "|", "-" or an en dash (and would cut a "- IEA" attribution): this one
    touches only the item's own source after the last "|".

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


@dataclass
class StoredLookup:
    """What a news_articles lookup answered.

    `found`: the stored dates. `failed`: urls with no answer -- `bad` ones the
    query itself refused even alone (bisection), plus every url left when the
    database stopped answering (`unavailable` says why: the error, or the
    lookup's deadline). The pipeline treats a failed url as never seen.
    """

    found: dict[str, StoredDates] = field(default_factory=dict)
    failed: set[str] = field(default_factory=set)
    bad: set[str] = field(default_factory=set)
    unavailable: str = ""
    seconds: float = 0.0


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


def restamps(feed_date: datetime | None, stored: StoredDates | None,
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


@dataclass(frozen=True)
class FreshSpike:
    """How fresh one fetched feed looks as a whole (see SPIKE_* above)."""

    fresh: int            # items dated within SPIKE_WINDOW of `now` (or later)
    total: int            # every item of the fetch, dated or not
    span: timedelta       # newest minus oldest item date

    @property
    def tripped(self) -> bool:
        return (self.total >= SPIKE_MIN_ITEMS
                and self.fresh >= SPIKE_FRESH_SHARE * self.total
                and self.span >= SPIKE_MIN_SPAN)

    def label(self) -> str:
        return f"{self.fresh}/{self.total},{self.span.total_seconds() / 86400:.0f}d"


def fresh_spike(dates: Iterable[datetime | None], now: datetime) -> FreshSpike:
    """The feed-fresh-spike reading of one fetched feed's item dates."""
    ds = list(dates)
    dated = [d for d in ds if d is not None]
    fresh = sum(1 for d in dated if d >= now - SPIKE_WINDOW)
    span = (max(dated) - min(dated)) if dated else timedelta(0)
    return FreshSpike(fresh=fresh, total=len(ds), span=span)


def shared_page_date(dated: Iterable[tuple[str, ParsedDate]], *,
                     minimum: int = TEMPLATE_MIN) -> ParsedDate | None:
    """The page date at least `minimum` distinct urls share, or None.

    `dated` holds (url, page date) of one feed's never-seen items whose page
    date would re-date them. Real old posts republished together carry their
    own dates; the same value on several of them is a template constant.
    """
    urls_by_date: dict[tuple[datetime, bool], set[str]] = {}
    first: dict[tuple[datetime, bool], ParsedDate] = {}
    for url, d in dated:
        if d is None:
            continue
        key = (d.value, d.date_only)
        urls_by_date.setdefault(key, set()).add(url)
        first.setdefault(key, d)
    best = max(urls_by_date, key=lambda k: len(urls_by_date[k]), default=None)
    if best is None or len(urls_by_date[best]) < minimum:
        return None
    return first[best]


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

    `read`: the page was parsed and is not a bot-wall interstitial. A challenge
    page (`challenge`, judged only when there is no publication date), a page
    whose markup could not be read or a failed fetch (`status` / `error`)
    counts as NOT read: the source did not hide its date, we never saw the
    page. Only a completed check has a PageEvidence; one that never completed
    has none.
    `snippet`: filled by the date check's own fetch (enrich.fetch_page_evidence)
    so the snippet backfill never fetches the same page twice in a scan.
    """

    page_date: ParsedDate | None = None
    headlines: tuple[str, ...] = field(default_factory=tuple)
    challenge: bool = False
    read: bool = False
    snippet: str = ""
    # The page's publication dates disagreed, so page_date is None
    # (PageSignals.published); informational.
    conflict: bool = False
    # A fetch that failed: the HTTP status when the site answered (None for a
    # transport error or a timeout) and the error text.
    status: int | None = None
    error: str = ""

    @property
    def blocked(self) -> bool:
        """The site answered and refused us: a 4xx or a bot-wall challenge."""
        return self.challenge or (self.status is not None and 400 <= self.status < 500)


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


def _challenge_from(sig: PageSignals) -> bool:
    title = sig.title
    if title and len(title) <= _CHALLENGE_TITLE_MAX and (
        _CHALLENGE_TITLE_RE.search(title) or _CHALLENGE_TITLE_ANYWHERE_RE.search(title)
    ):
        return True
    return any(m in r.lower() for r in sig.resources for m in _CHALLENGE_MARKERS)


def looks_like_challenge(soup) -> bool:
    """True when the fetched page is a bot-wall interstitial, not an article."""
    if soup is None:
        return False
    return _challenge_from(read_page_signals(soup))


_lock = threading.Lock()
_pages: dict[str, PageEvidence] = {}
# Hosts (without "www.") of which at least one page was READ this scan: the R3
# test for "is this site blocking us, or only this page failing?".
_read_hosts: set[str] = set()


def _host(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def reset_scan() -> None:
    """Forget every page seen so far. The pipeline calls it at the start of a scan."""
    with _lock:
        _pages.clear()
        _read_hosts.clear()


def record_page(url: str, soup) -> PageEvidence:
    """Read and remember what a fetched page says about itself (thread-safe).

    Fail-soft: a page whose markup trips the reader yields evidence with
    `read=False` rather than an exception inside the enrich path.
    """
    try:
        sig = read_page_signals(soup)
        page_date = sig.published
        challenge = page_date is None and _challenge_from(sig)
        ev = PageEvidence(page_date=page_date, headlines=sig.headlines,
                          challenge=challenge, read=not challenge,
                          conflict=page_date is None and sig.conflict)
    except Exception:  # noqa: BLE001
        ev = PageEvidence()
    if url:
        with _lock:
            _pages[url] = ev
            if ev.read:
                _read_hosts.add(_host(url))
    return ev


def page_seen(url: str) -> PageEvidence | None:
    with _lock:
        return _pages.get(url)


def site_was_read(url: str) -> bool:
    """Was any page of `url`'s site read successfully this scan?"""
    with _lock:
        return _host(url) in _read_hosts
