# news-hunter-scanner

Cron-driven scanner that sweeps ~60 Brazilian oil & gas news sources every
5 minutes via GitHub Actions and pushes new articles to a shared Supabase
table (`news_articles`).

Reads the keyword search set from `news_hunter_keywords` (union of every
authenticated user's personal list). The SectorData dashboard at
`ibbaoilandgasdata.vercel.app/news-hunter` reads the same `news_articles`
table with row-level security applied per user.

## Architecture

```
News source (RSS / sitemap / Google News / homepage scrape)
        ↓ fetcher.py  (48 workers, 12s deadline)
Raw item list
        ↓ filter.py   (keyword + 24h window)
Candidates
        ↓ enrich.py   (24 workers, fetch_html → snippet + published_at)
Articles
        ↓ supabase_sync.py  (UPSERT on url PK, batch of ≤100)
Supabase news_articles
        ↓ anon + RLS
Dashboard /news-hunter
```

## Deploy

Runs entirely on GitHub Actions — no infra needed. The workflow at
`.github/workflows/scan.yml` triggers `python news_hunter_service.py --once`
every 5 minutes via cron.

Required repository secrets (set under **Settings → Secrets → Actions**):

- `SUPABASE_URL` — `https://<project>.supabase.co`
- `SUPABASE_SERVICE_KEY` — Supabase service role key

Manual run: **Actions → News Hunter scan → Run workflow**.

## Diagnosing a dead source

A source can stop producing while every run stays green. Each scan logs, at
INFO, the feeds that returned zero items, the ones that timed out, and the
per-feed HTTP errors — start there:

```bash
gh run view <run-id> --repo IBBAOG/news-hunter-scanner --log \
  | grep -E "feed summary|feeds returning|feed errors"
```

Note that a Google News `site:` query returning zero is routine (no keyword hit
in the window) and is counted separately from the named list; a **registered
feed** returning zero is always anomalous.

If a feed looks healthy from your machine but produces nothing in production,
the difference is the IP: several Brazilian sites sit behind WAFs that
challenge datacenter ranges only. `.github/workflows/diagnose_feed.yml` runs
`scripts/diagnose_feed.py` on the same runner image the scanner uses and prints
status, latency, headers and body for four probes (scanner-equivalent timeout,
generous timeout, `curl_cffi` browser impersonation, end-to-end `_fetch_one`):

```bash
gh workflow run diagnose_feed.yml --repo IBBAOG/news-hunter-scanner \
  -f url=https://example.com/feed/
```

`cf-mitigated: challenge` + HTTP 403 there but 200 locally means the source
needs the Google News `site:` route (`NO_RSS_DOMAINS` in `sources.py`) — see
the Monitor Mercantil entry for a worked example.

### When the feed is fine and the EXTRACTOR is what died

A third failure mode, quieter than both of the above: the feed answers 200,
returns fresh items, and the outlet still barely appears in the dashboard —
because `_clipinator_shim`'s extractor for that domain no longer matches
anything, so the near-miss **lede rescue** reads the meta description instead of
the article and the source silently degrades to title-only matching. Nothing
turns red; `no_body` does not even rise, because a body of sorts was returned.

Two shapes seen on 2026-08-20, both fixed in `_clipinator_shim.py`:

- **Page-builder migration.** Agência iNFRA moved to a Hello Elementor theme:
  `div.entry-content` stopped existing, and the `<article>` elements left on the
  page are related-post cards holding zero `<p>`. Selector is now the Elementor
  widget-TYPE class, never the `elementor-element-<hash>` sibling (that hash
  changes when the page is re-saved in the builder).
- **Body not in the DOM at all.** JOTA is a Next.js Pages-Router site whose
  article exists only inside the `__NEXT_DATA__` JSON island; the served page
  carries exactly one `<p>` and zero `<article>`. `ex_jota` lifts the body out
  of `props.pageProps.post.content` (see `_next_data_html`).

Cheap check on any suspect domain:

```bash
curl -sA "Mozilla/5.0" "<article-url>" -o /tmp/a.html
grep -c "<p[ >]" /tmp/a.html          # a real article page has dozens
grep -c "__NEXT_DATA__" /tmp/a.html   # 1 => the body is probably in there
python -c "from news_hunter._clipinator_shim import _extract;   print(len(_extract(open('/tmp/a.html',encoding='utf-8').read(), '<domain>')[1]))"
```

Zero paragraphs on a page that visibly has a body is the whole diagnosis. The
same fix has to land twice: `SectorData`'s `src/lib/clipping/sources.ts` carries
an independent copy of these extractors for the clipping generator.

### When the feed answers 200 and has simply stopped moving

The harder failure is a feed that keeps working and keeps returning the *same*
items. Every counter stays green — right status, right item count, real dates —
and the source quietly leaves the product. Poder360 spent six days like this in
2026-08: Cloudflare pinned its `/feed/` object at 2026-08-07 19:55 UTC while the
same URL answered fresh from a residential IP and, on demand, from a runner.

Two checks name it:

```bash
# 1. the run log: each scan compares every feed's newest item against a
#    per-domain budget (FEED_STALE_HOURS in sources.py, default 48h)
gh run view <run-id> --repo IBBAOG/news-hunter-scanner --log | grep "feeds stale"

# 2. the CDN: `age` far past the origin's own max-age means served-stale
curl -sSI https://www.poder360.com.br/feed/ | grep -Ei "^(age|cache-control|cf-cache-status)"
```

In `news_articles` the fingerprint is a row whose `found_at` sits at exactly
`published_at` + the scan window: the scanner kept re-seeing that item until the
window filter dropped it, which a feed with a ~1.5h span cannot do honestly.

A cache buster does not necessarily help — Poder360's zone ignores the query
string in its cache key, and a client `Cache-Control: no-cache` with it. What
works is a **second path**, since each path is its own cache object: that domain
registers `/feed/` and `/feed/atom/`, deduped by normalized URL.

## Why an article can reach the feed with no body

Measured 2026-08-20: **1,026 of the 1,383 rows published in the previous 24h
(74%) carried an empty `snippet`** — the dashboard was showing headlines and
little else. That single symptom had three unrelated causes, and each needed its
own fix:

1. **The feed had the body and we never read it.** WordPress publishes the whole
   article in `<content:encoded>` while `<description>` is a teaser — JOTA ships
   96 characters of description text against 5,264 of body. `_entry_to_item` now
   falls back to the body's **lede** (first 3 real paragraphs, the same bound the
   lede rescue uses) when the description is thin. Free: no HTTP at all.
   Measure a candidate feed with
   `grep -c "content:encoded" feed.xml` before assuming it has nothing to give.
2. **Nobody ever fetched the body of an item that matched on its TITLE.** The
   lede rescue is a *near-miss* mechanism — it only runs when title AND summary
   failed to match. An item that matched on the title was persisted as-is, so a
   source covered only by Google News (Brasil 247, Agência iNFRA, Reuters), which
   arrives with no description at all, was **guaranteed** to land bodyless
   forever. Stage 3d (`_run_snippet_backfill`) is the mirror: it takes the
   already-approved items that still have no snippet and fetches a capped subset.
   It never re-validates keywords — the item already passed, the snippet is
   display.

   The budget is allocated **round-robin across domains**, recency-first inside
   each. Pure global recency looks obvious and allocates terribly: whoever
   publishes most takes everything. On the day this shipped,
   `finance.sina.com.cn` held 367 of the ~994 bodyless rows in 24h and its
   articles were also the freshest, so Brasil 247 — three articles that day —
   queued behind a foreign firehose. Round-robin fixes the allocation with no
   priority list to maintain, and has a second payoff: a domain that can never
   answer burns **one** slot per scan instead of six. Reuters is the live case —
   `diagnose_snippet` on the runner returns 401 (DataDome) for every article of
   it, so it can never fill and would otherwise sit permanently at the front of
   the queue.
3. **The upsert erased snippets it had already stored.** The scanner is stateless
   and re-pushes the same row every scan (~355 rows / 5 min against ~7 genuinely
   new articles). A row whose body was fetched in scan N came back with an empty
   snippet in scan N+1 and overwrote the stored text — the work undid itself
   every five minutes. `_split_on_snippet` now sends those rows **without the
   `snippet` key at all**, so PostgREST leaves the column out of the
   `ON CONFLICT … DO UPDATE SET` list. Same write-once family as the fabricated
   `published_at` and the `title_en` overlay.

### The two passes, and why one is not enough

**Stage 3d (live, in every scan)** covers articles the current scan still sees.
That is a real ceiling, not a detail: a Google-News-only source is re-collected
only while Google keeps returning it. Measured 2026-08-20 — Brasil 247's newest
articles filled within minutes of Stage 3d shipping, while its day-old ones had
already dropped out of the `site:` result set and were unreachable to the live
pipeline forever. Nothing was wrong with the fetch: `diagnose_snippet` pulls 357
chars from that same domain on the runner without trouble. The collector simply
never offered those rows again.

**`backfill_snippets` (daily, `scripts/backfill_snippets.py`)** asks the
*database* instead of the collector: rows with an empty snippet, fetch the body,
write it back. It is the repair pass. It writes with an `UPDATE` keyed on `url`
and touches **only** `snippet` — never an upsert — so a row keeps its date, its
`matched_keywords` and its translation overlay, and the repair cannot regress any
write-once guarantee. Run it by hand for a targeted repair:

```bash
gh workflow run backfill_snippets.yml -f domain=estadao.com.br -f limit=300
gh workflow run backfill_snippets.yml -f limit=1200            # general drain
gh workflow run backfill_snippets.yml -f dry_run=true          # report only
```

Its per-domain tally is the fastest read on which sources are structurally
unreachable rather than merely queued. As of 2026-08-20, reliably refusing the
runner (HTTP 403 / 401 DataDome): `www.reuters.com`, `br.investing.com`,
`www.alarabiya.net`, `www.arabnews.com`, `asharqbusiness.com`,
`www.bloomberg.com`, `www.spglobal.com`, `neftegaz.ru`,
`www.offshore-technology.com`, `interfax.com`. Those land title-only by
necessity, and no amount of budget changes that — the ceiling is the WAF, and
lifting it would take a headless tier the scanner does not have.

Two invariants worth keeping in mind before touching any of this:

- **The free lede must stay the same size as the fetched lede.** Widening
  `_LEDE_PARAGRAPHS` would silently turn every full-text feed into a body-wide
  keyword match, which is a different (and much noisier) product decision than
  the one the lede rescue implements.
- **Stage 3d must ask the database what it already has.** `urls_with_snippet()`
  is what stops a stateless scanner from re-downloading the same articles every
  five minutes forever; without it the cap is spent on finished work and a
  genuinely new article never gets its turn. It deliberately selects only the
  `url` column — the stored text is already protected by `_split_on_snippet`, and
  pulling snippets back for ~1,400 rows every 5 minutes would eat the 5 GB/month
  Supabase egress budget the sink is written around.

### `url=in.(...)` lookups are limited by ENCODED length, not by item count

Every write-once protection here (dates, translations, and now snippets) works
by asking Supabase what it already stored, with a `url=in.(...)` filter. That
filter travels in the **query string**, so the ceiling is the percent-encoded
request length — and encoding is not uniform: a Latin URL costs ~150 characters,
an Arabic or Chinese one ~257, because every character becomes `%XX%XX`.

Fixed 100-item chunks therefore passed on Latin sources and failed the moment a
batch caught attaqa / alarabiya / yicai, with a message that names no limit at
all:

```
{'message': 'JSON could not be generated', 'code': 400}
```

Measured against the live project, 2026-08-20, with real Arabic URLs:

| urls | request chars | result |
|-----:|--------------:|--------|
|   25 |         6,480 | 200 |
|   50 |        12,880 | 200 |
|   75 |        19,280 | 200 |
|   95 |        24,400 | 200 |
|  100 |        25,680 | **400** |

`_chunk_urls_for_query` now cuts by encoded weight with half that wall as its
budget. The bug was worth hunting because of how quietly it degraded: the lookup
returned `None`, `_preserve_translations` took its defensive branch, and **133
rows per scan were silently deferred** — the write-once protection for
translations had been running blind, and every new lookup would have inherited
the same trap.

## Listing scrapers: never fabricate a publication date twice

Sources without a feed are covered by `HOMEPAGE_SCRAPERS`, which harvests
article links from a listing page. Those items arrive with no title, no body
and no date — `enrich_item` fetches each article page to fill them in.

When that fetch fails (paywalled 404 slug variant, timeout, dropped session)
the scanner used to stamp `published_at = now()` for the two
`RECENT_ONLY_SCRAPERS` and UPSERT it. On a row that **already existed** the
stamp was re-applied on every scan, and because the date is always "now" the
item never left the 24h window — so it was re-discovered and re-stamped
forever. That is how Brasil Energia articles from 28-29/07 sat at the top of
the feed on 04/08 labelled "13m ago" (481 rows had been poisoned this way since
2026-04-29). Three rules keep it dead:

1. **`_scrape_homepage` reads the date the listing prints next to each link**
   (`RawItem.published_hint`). The Brasil Energia "últimas notícias" page is an
   archive spanning ~7 days, not a last-24h list — without that date the
   scanner cannot tell that its tail is old. Stale items are now dropped before
   any article fetch. The hint deliberately stays out of `title` /
   `published_at`: filling those would make `enrich_item(need_snippet=False)`
   skip the article fetch and cost us the snippet and the exact time.
2. **An item we could not reach is not persisted.** No page title and no body
   means the only thing left to show is a URL slug; the item stays on the
   listing and the next scan (~5 min) retries it.
3. **A fabricated date is write-once** (`Article.published_is_approx`).
   `supabase_sync` looks up the existing row and keeps its stored date, so the
   stamp only ever lands on first discovery. A *real* date found later
   overwrites it only when it is EARLIER: since migration 20272100000000 the
   trigger `trg_news_articles_published_at_monotone` lets an UPDATE move
   `published_at` backwards only.

Regression tests: `tests/test_stale_listing_items.py`.

### A feed date can be a modification time (2026-09-25)

Kpler's Webflow feed sets `<pubDate>` to the item's last **re-publish**. When
Kpler re-published its back catalogue, 81 of 100 items were dated "today" while
each page kept its own `datePublished` (Mar 31 to Sep 10); 75 old posts landed
as new, because enrich read the page date only when the feed had none and never
fetched a page for an item that already had title, date and snippet. The rules
(`news_hunter/date_credibility.py`, Stage 4b in `pipeline.py`):

1. **The earliest credible date wins (R2).** Whenever the page is fetched, its
   own date is read (`article:published_time`, JSON-LD `datePublished`,
   `itemprop="datePublished"`, `<meta name="date">`; never `dateModified`, and
   never a bare `<time datetime>` -- sidebars and related-article cards carry
   those, and the first one would re-date a new article to an older neighbour)
   and replaces the feed date when earlier by more than 24 h, so the window
   drops the item. A date-only value ("Apr 01, 2026") counts as the end of its
   day. A date the feed prints at the end of its own title
   (`"… | Kpler - Jun 30, 2026"`) is read the same way, with no fetch.
2. **Every never-seen item gets a spot check.** Production runs in fast mode,
   where no page is read for a well-formed feed item -- which is how a small
   re-publish in a feed with no stored history, or a brand-new source, got in
   unchecked. Stage 4b now reads up to 4 pages per feed per scan of the items
   `news_articles` does not hold yet (32 per scan in all, shared round-robin):
   a page that proves the item older drops it and counts as re-stamp evidence;
   a dateless or unreadable page admits it with the feed date. A spot check
   never defers.
3. **Batch re-stamp evidence means verify or defer (R3).** News feeds re-date
   stored articles all the time (updates); a re-publication shows as a
   **batch**: at least 3 witnesses within 10 minutes of one another
   (`RESTAMP_BATCH_MIN` / `RESTAMP_BATCH_SPAN`), counting together stored urls
   the feed re-dates (feed date > `min(published_at, created_at) + 24h`),
   fresh items whose printed title date is older, and never-seen items whose
   page proved them older -- so a feed escalates inside the scan that shows the
   burst. Measured over 30 days of the clamp backup, only Kpler and
   investing.com's weekly "live levels" batch qualify; "any one re-date" would
   have flagged estadao for 291 h. A flagged feed's never-seen items are
   checked up to 8 pages per scan (half to the newest items, half rotating):
   - page read and dated: verified, or dropped as older;
   - page read and **dateless**: **deferred** (Kpler's empty `datePublished`);
   - over the budget: **deferred**, next scan;
   - page **not read** (transport error, HTTP >= 400, a timeout, our deadline,
     a WAF challenge): **deferred** while another page of the same site was
     read this scan -- a 403 on one old post must not publish it -- and
     **admitted with the feed date**, counted as `unverified_admitted`, only when
     the whole site blocks us (investing.com fails 8/8 from the runner): our own
     block must not become a zero.

   **Accepted cost (no age-out).** A dateless page stays deferred for as long
   as its feed is flagged, i.e. until the burst's evidence leaves the 24 h
   window. A genuinely new post that carries no date and appears during a
   re-stamp episode can therefore be lost; it is counted in the log. Admitting
   dateless items after N hours was rejected: it would let a burst's undated
   old posts (Kpler had 32) land together N hours later. Kpler publishes about
   one post a day. The daily-ops burst probe (P13) is the backstop for
   re-stamps on sites that block the runner.
4. **Clean titles.** `"<headline> | <own source name>( - <date>)"` loses the
   suffix, and a title ending with the page's `<h1>` after plain whitespace
   (Kpler's `"<SEO title> <headline>"`) becomes the `<h1>`.

Stored rows are not this rule's concern: the database keeps
`news_articles.published_at` monotone. The stored-date lookup fails per url,
never for the scan: a failed chunk is retried once, then bisected, so a url
whose query keeps failing (a Cloudflare 400, an over-long query) fails alone;
a failed candidate url defers only its own item, a failed evidence-only url
only loses that evidence (`lookup=PARTIAL`). Every scan logs one line, zero
included:

```
date credibility: page_older=13 [kpler.com=13] (title_date=13) restamp_domains=[www.kpler.com(db=4,title=60,page=2;batch=16)] isolated=[www.cbsnews.com(db=1;batch=1)] verified=0 spot=[ok=6 dateless=1] unverified_admitted=0 [] deferred=14 [www.kpler.com: no_page_date=8 over_budget=6] pages=15+2reused looked_up=431 lookup=ok in 3.2s
```

`db=4,title=60,page=2;batch=16` counts each kind of witness and the largest
10-minute batch across all of them; `isolated` lists feeds with evidence below
the batch threshold; `spot` sums the checks of feeds that are not flagged;
`pages=fetched+reused` are the pages the stage downloaded itself and those
enrich had already read.

Measure before touching a threshold: `diagnose_date_credibility.yml` samples
every registered feed's pages from the runner (R2 re-dates, template dates, R3
flags; `mode=feeds`), checks the title cleaner against every stored title
(`mode=titles`), times the whole date-credibility cost off/on in a full scan
without writes -- Stage 4b, the spot checks and the page-evidence parsing
(`mode=full-scan`) -- and `-f dry_run=<feed url>` runs the real pipeline over
one feed without writing. The stored-date lookup stays sequential: the same
chunks sent from four threads over the client's shared HTTP/2 connection failed
on the runner (`ConnectionTerminated`, Cloudflare 400). Regression tests:
`tests/test_date_credibility*.py`.

## Translation: a 200-shaped success carrying an error page

`deep-translator` scrapes `translate.google.com` and parses whatever HTML comes
back. When Google answers **HTTP 500** it still returns a body, so the parser
extracts the visible text of Google's error page and hands it back as an
ordinary non-empty string — no exception, nothing empty. `translate_to_en`'s
only acceptance test was "non-empty", so it accepted it and the sink stored it.
Measured on production `news_articles` on 2026-09-01: **2,670 rows (34% of every
row carrying a `title_en`)** whose English headline read, verbatim and
identically,

```
Error 500 (Server Error)!!1500.That's an error.There was an error. Please try again later.That's all we know.
```

across `ar`/`zh`/`ru`/`iw`/`es`, oldest 2026-08-19 — the day the multilingual
wave shipped. Same defect family as the fabricated date above: a value invented
on a failure path and written as if it were a fact. Two properties made it
permanent rather than transient: the sink's translation overlay is **write-once**
(a row with a non-empty `title_en` is never overwritten), and old articles are
never re-scanned.

**The guard** lives in the producer, `news_hunter.translate.looks_like_error_page`,
because that is the only place that sees the difference. It keys on the *shape of
Google's error template* — the literal `Error <3 digits> (<reason>)!!1` marker,
or both of the template's closing sentences together — never on a vocabulary
word, so a headline containing "error", "server" or a three-digit number still
passes. A rejection is logged at **WARNING** (before this, an outage of the
translate endpoint was invisible in the scan log) and falls through to the next
backend in the chain, so a transient 500 on the first attempt still gets its
`auto` retry. `title_original` always holds the native headline, so a rejected
translation costs display quality, never data.

Regression tests: `tests/test_multilingual_error_page_guard.py` — both
directions, since a guard that merely stops alarming is worse than no guard.

**The repair pass.** Guarding the producer fixes nothing already stored. After a
database sweep NULLs the poisoned values, `scripts/repair_foreign_titles.py`
re-translates the rows that need it — foreign `source_lang`, non-empty
`title_original`, NULL `title_en` — through the *same guarded* `translate_to_en`,
so a run launched during an outage writes nothing rather than re-poisoning the
rows it was dispatched to fix (and exits non-zero to say so). It writes with an
UPDATE keyed on url and filtered `title_en is null`: it only ever fills
something empty, never overwrites. Idempotent and safe to re-run — a repaired
row stops matching the predicate, so consecutive runs walk down the backlog.

```bash
gh workflow run repair_foreign_titles.yml --repo IBBAOG/news-hunter-scanner -f limit=500
gh workflow run repair_foreign_titles.yml --repo IBBAOG/news-hunter-scanner -f dry_run=true
gh workflow run repair_foreign_titles.yml --repo IBBAOG/news-hunter-scanner -f lang=ar -f limit=1000
```

Regression tests: `tests/test_repair_foreign_titles.py`.

## Brasil Energia cookie refresh

Brasil Energia's `be-auth` session cookie expires roughly every 14 days. The
SectorData clipping generator reads the cookie body from the shared
`public.clipping_cookies` table (in the SectorData Supabase project); a stale
cookie causes silent fallback to public/teaser article bodies.

Workflow `.github/workflows/refresh_brasil_energia_cookie.yml` logs in via
`news_hunter/brasilenergia_auth.py`, dumps the live cookie jar to the Netscape
format and UPDATEs the `brasilenergia.com.br` row twice a week (Mon + Thu
06:00 UTC) — ~4x safety margin over the 14-day TTL.

Manual refresh:

```bash
gh workflow run refresh_brasil_energia_cookie.yml --repo IBBAOG/news-hunter-scanner
```

Required secrets (in addition to those above): `BRASIL_ENERGIA_USER`,
`BRASIL_ENERGIA_PASS`.

## International coverage

Alongside the ~60 Brazilian sources, the scanner carries **161 English-language
international outlets** — six that predated the program, 42 added in four waves
(2026-08-18) and 113 added by the five Wave 5 branches and their post-merge
corrections (2026-09-14). Two surfaces are used, in this order of preference:

- **RSS** — a dated feed the scanner's feed path reaches from its datacenter
  runner (**58 registrations**). Matches land on the item title/summary with no
  body fetch, and the body is there when a keyword needs it.
- **GNews en-US** — a Google News `site:<domain> when:<window> (<keywords>)`
  query at `hl=en-US` (**103 outlets**), used when the outlet's own feed is
  WAF-blocked, paywalled, dateless or absent. Google supplies the date; bodies
  stay unreachable, so these land **title-only** (empty snippet).

Counts are from the registries themselves (`len(ENGLISH_NO_RSS_DOMAINS)` and the
`RSS_FEEDS` keys tagged in `INTERNATIONAL_RSS_DOMAINS`) on 2026-09-14 and move
with every wave — nothing asserts them, so re-count rather than trust them. The
EN GNews list is past the per-scan burst budget (`EN_GNEWS_QUERIES_PER_SCAN`, 34),
so it now rotates in **4 cohorts**: every domain is queried every ~20 minutes,
well inside each query's 24h `when:` window.

What makes English outlets yield at all is the keyword layer. Matching runs over
the **full live Supabase lexicon — 187 keywords (78 exact / 109 substring) on
2026-09-14** (180 global rows in `news_hunter_default_keywords` plus the
per-user rows the scanner unions in; `oil`, `gas`, `Brent`, `WTI`, `OPEC`, `LNG`,
`crude`, `diesel`, `refinery`, `Hormuz`, `ExxonMobil`, `Petrobras`, …) — the
number `measure_source` prints as `keywords: N total` at the top of every run,
which is the only count worth quoting. While *retrieval* on the GNews route is
narrowed to
the **12-term `ENGLISH_KEYWORD_PRIORITY`** subset (`sources.py`), the block Google
will not truncate. The two are different funnels — a matching gap is closed with a
DB keyword row, never by editing the tuple.

The tables below are a **directory, not a second source of truth**: the per-domain
comment in `sources.py` — next to each entry, and next to every *rejected*
candidate — is the authoritative record, carrying the full measured
`items/fresh/pass/near/rescued`, the resolution/keying rationale and the
false-positive caveats. `pass` here is the title/summary pass count over a
**7-day** window unless a shorter feed span is noted in parentheses.

### Global trade press & shipping

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| OilPrice | oilprice.com | RSS | 11 (10h) |
| Offshore Engineer | oedigital.com | RSS | 7 (24h) |
| gCaptain | gcaptain.com | RSS | 9 (21h) |
| Splash247 | splash247.com | RSS | 3 (6h) |
| Hellenic Shipping News | hellenicshippingnews.com | RSS | 7 (20h) |
| LNG Prime | lngprime.com | RSS | 10 (29h) |
| CNBC (Energy) | cnbc.com | RSS | 23 (244h) |
| Reuters | reuters.com | GNews en-US | — |
| Bloomberg | bloomberg.com | GNews en-US | 55 |
| S&P Global Commodity Insights | spglobal.com | GNews en-US | 44 |
| Energy Intelligence | energyintel.com | GNews en-US | 65 |
| Upstream | upstreamonline.com | GNews en-US | 60 |
| TradeWinds | tradewindsnews.com | GNews en-US | 44 |
| Argus Media | argusmedia.com | GNews en-US | 8 |
| Global Energy Network | globalenergynetwork.net | RSS (14s timeout override) | 19 (promoted from GNews 2026-09-14; GNews was 18) |
| CNN | cnn.com / edition.cnn.com | GNews en-US | ~16–29/30d |
| The Edge Singapore | theedgesingapore.com | GNews en-US | 19 (24h) |

### United States

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| Oil & Gas 360 | oilandgas360.com | RSS | 14 (99h) |
| Natural Gas Intelligence | naturalgasintel.com | RSS | 10 (6h) |
| Oil & Gas Journal (OGJ) | ogj.com | GNews en-US | 24 |
| World Oil | worldoil.com | GNews en-US | 22 |
| Rigzone | rigzone.com/news | GNews en-US | 31 |
| Hart Energy | hartenergy.com | GNews en-US | 23 |
| E&E News | eenews.net | GNews en-US | 15 |

### Europe

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| Offshore Energy | offshore-energy.biz | RSS | 10 (24h) |
| Offshore Technology | offshore-technology.com | GNews en-US | 21 |
| Montel News | montelnews.com | GNews en-US | 8 |
| Energy Voice | energyvoice.com | GNews en-US | 4 |

### Russia–CIS

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| The Moscow Times | themoscowtimes.com | RSS | 17 |
| bne IntelliNews | intellinews.com | RSS | 4 (8h; promoted from GNews 2026-09-14, GNews was 11; bimodal origin, ~50% of polls time out — see below) |
| Interfax | interfax.com | GNews en-US | 11 |
| TASS | tass.com | GNews en-US | 8 |

### Middle East

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| Arab News | arabnews.com | GNews en-US | 79 |
| Zawya | zawya.com | GNews en-US | 38 |
| Gulf News | gulfnews.com | GNews en-US | 24 |
| Al Jazeera | aljazeera.com | GNews en-US | 17 |
| MEES | mees.com | GNews en-US | 10 |
| Shana | shana.ir | GNews en-US | 7 |
| The National | thenationalnews.com | RSS | 6 (+2 rescued) |
| Iraq Oil Report | iraqoilreport.com | GNews en-US | 4 |

### China

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| Xinhua | english.news.cn | GNews en-US | 43 |
| South China Morning Post | scmp.com | GNews en-US | 13 |
| China Daily | chinadaily.com.cn | GNews en-US | 9 |
| Global Times | globaltimes.cn | GNews en-US | 7 |

### India

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| ET EnergyWorld | energy.economictimes.indiatimes.com | RSS | 14 (56h) |
| Moneycontrol | moneycontrol.com | GNews en-US | 57 |
| The Hindu BusinessLine | thehindubusinessline.com | RSS | 23 comm. / 6 econ. |
| Mint | livemint.com | RSS | 5 (4 on-beat) |

### Wave 5 anchors (2026-09-14)

The five blocks below are the directory rows for the five parallel waves of the
2026-09 international expansion. Each wave fills ONLY its own block, between its
own BEGIN/END comments, and leaves every fence line alone — that is what keeps
the five branches merging without conflicts. The matching anchors in
`sources.py` live inside `RSS_FEEDS`, `INTERNATIONAL_RSS_DOMAINS`,
`ENGLISH_NO_RSS_DOMAINS` and `FEED_TIMEOUT_OVERRIDES`. Empty is the correct
state until a wave lands.

<!-- --- Wave 5A (2026-09-14): Global wires & US mainstream --- BEGIN -->

### Global wires & US mainstream

24 candidates measured on the runner 2026-09-14 (window 7d, live 187-keyword
set), 24 registered, 0 rejected. Three kept RSS; the other 21 are GNews en-US,
because their own feed is WAF-blocked (AP, Barron's), frozen (WSJ/Dow Jones,
Telegraph, MarketWatch), absent or malformed (The Times, Forbes energy), a slow
stub (Washington Post: 9-10s for 3-11 items) or a general firehose yielding a
fraction of the keyword-scoped query (BBC 3 vs 23, NBC 7 vs 28, NPR 2 vs 19).
`marketwatch.com/story` is path-scoped on purpose: the bare domain measures
pass=75 but ~45 of those are perennial quote/chart/filing pages. ABC News is
registered as `abcnews.com`, not `abcnews.go.com` (that host returns 0 GNews
items and is not what the outlet's own feed links to).

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| Associated Press | apnews.com | GNews en-US | 33 |
| Financial Times | ft.com | RSS | 21 commodities / 19 companies-energy |
| The Wall Street Journal | wsj.com | GNews en-US | 34 |
| The New York Times | nytimes.com | GNews en-US | 22 |
| The Washington Post | washingtonpost.com | GNews en-US | 40 |
| The Economist | economist.com | GNews en-US | 19 (~12 are the daily World in Brief) |
| BBC News | bbc.com | GNews en-US | 23 |
| The Guardian | theguardian.com | RSS | 11 (business/oil tag, 15 fresh) |
| The Times | thetimes.com | GNews en-US | 7 |
| The Telegraph | telegraph.co.uk | GNews en-US | 21 |
| Sky News | news.sky.com | GNews en-US | 13 |
| POLITICO | politico.com | GNews en-US | 6 |
| POLITICO Europe | politico.eu | GNews en-US | 6 |
| Axios | axios.com | GNews en-US | 12 |
| Forbes | forbes.com | GNews en-US | 11 |
| Fortune | fortune.com | GNews en-US | 21 |
| Business Insider | businessinsider.com | GNews en-US | 36 |
| MarketWatch | marketwatch.com/story | GNews en-US | 54 |
| Barron's | barrons.com | GNews en-US | 20 |
| NPR | npr.org | GNews en-US | 19 |
| ABC News | abcnews.com | GNews en-US | 52 |
| NBC News | nbcnews.com | GNews en-US | 28 |
| CBS News | cbsnews.com | RSS | 7 (world feed; GNews 60 is half local pump copy) |
| Fox Business | foxbusiness.com | GNews en-US | 20 |

<!-- --- Wave 5A (2026-09-14): Global wires & US mainstream --- END -->

<!-- ---- merge fence: keep >=4 lines between wave blocks ---- -->


<!-- --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- BEGIN -->

### US regional, Europe, Canada, Oceania mainstream

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| Investing.com (EN edition) | investing.com | RSS | 6 (2h) |
| France 24 (EN edition) | france24.com | RSS | 6 (10h) |
| Le Monde in English | lemonde.fr | RSS | 3 (13h) |
| The Irish Times | irishtimes.com | RSS | 10 |
| City A.M. | cityam.com | RSS | 3 (13h) |
| Financial Post | financialpost.com | RSS | 1 (2h) site-wide + 2 energy |
| Calgary Herald | calgaryherald.com | RSS | 2 |
| ABC News Australia | abc.net.au | RSS | 2 (12h) |
| Australian Financial Review | afr.com | RSS | 7 (109h) markets + 3 (9h) front |
| Houston Chronicle | houstonchronicle.com | GNews en-US | 35 |
| Los Angeles Times | latimes.com | GNews en-US | 21 |
| USA Today | usatoday.com | GNews en-US | 36 |
| Deutsche Welle (EN edition) | dw.com | GNews en-US | 23 (titles verified English 20/20, 2026-09-14) |
| Euronews | euronews.com | GNews en-US | 23 (titles verified English 23/23, 2026-09-14) |
| Swissinfo | swissinfo.ch | GNews en-US | 12 (titles verified English 11/11, 2026-09-14) |
| The Globe and Mail | theglobeandmail.com | GNews en-US | 98 |
| CBC News | cbc.ca | GNews en-US | 39 |
| The Australian | theaustralian.com.au | GNews en-US | 46 |
| Energy News Bulletin (AU) | energynewsbulletin.net | GNews en-US | 8 |
| Norwegian Offshore Directorate | sodir.no | GNews en-US | 1 |

Surface pick for this wave, since almost every candidate had BOTH a feed and a
GNews surface: the higher measured 7-day yield, normalising a feed whose span is
shorter than the window (`pass x 168/span` -- valid because the scanner polls
every ~5 min and so sees every item of a 10-30 slot rolling feed), with a
tie-break to RSS whenever GNews was under 2x richer (an RSS item lands with a
summary, a GNews item lands title-only). Both numbers are recorded per entry in
`sources.py`.

Rejected, 4 of 24 (measurements in the `ENGLISH_NO_RSS_DOMAINS` Wave 5B block):
**Voice of America** (voanews.com -- GNews 0 items; RSS reachable but frozen,
newest pubDate March 2025 on three different sections), **Der Spiegel
International** (spiegel.de -- feed `fresh=0`, newest English item 11 days old;
GNews `items=2 fresh=0`), **Euractiv** (euractiv.com -- re-tested under the lower
bar and worse than in 2026-08-18: RSS 403 from the runner, GNews `pass=0`
against 16 fresh items), **The Local Norway** (thelocal.no -- RSS `fresh=10
pass=0`, GNews `items=1 pass=0`). **Houston Chronicle**, rejected in Wave 3 under
the old `>= 3 on-beat/7d` bar, was re-measured here at `pass=35/7d` and is
registered.

<!-- --- Wave 5B (2026-09-14): US regional, Europe, Canada, Oceania mainstream --- END -->

<!-- ---- merge fence: keep >=4 lines between wave blocks ---- -->


<!-- --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- BEGIN -->

### Africa, Middle East & North-East Asia mainstream

Measured on the runner 2026-09-14 (24 candidates, 24 registered, 0 rejected):
6 RSS + 18 GNews en-US. Eight of the GNews entries are outlets whose own feed
answers 200 from a residential IP and 403 from the runner; the per-domain
comment in `sources.py` carries each rejected surface with its numbers.

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| AllAfrica | allafrica.com | GNews en-US | 36 |
| BusinessDay Nigeria | businessday.ng | GNews en-US | 53 |
| Punch | punchng.com | GNews en-US | 54 |
| ThisDay | thisdaylive.com | GNews en-US | 19 |
| Business Day / BusinessLive (South Africa) | businesslive.co.za | RSS | 10 (297h) |
| The East African | theeastafrican.co.ke | GNews en-US | 3 |
| Africa Oil+Gas Report | africaoilgasreport.com | GNews en-US | 8 (of 8 items) |
| Energy Capital & Power | energycapitalpower.com | GNews en-US | 1 |
| Al Arabiya English | english.alarabiya.net | GNews en-US | 52 |
| Middle East Eye | middleeasteye.net | RSS | 14 (7h) |
| The Times of Israel | timesofisrael.com | GNews en-US | 40 |
| Haaretz | haaretz.com | GNews en-US | 19 |
| Anadolu Agency (EN) | aa.com.tr/en | GNews en-US | 60 |
| Daily Sabah | dailysabah.com | RSS | 7 (84h, business) + 5 (energy) |
| Ahram Online | english.ahram.org.eg | GNews en-US | 47 |
| Tehran Times | tehrantimes.com | RSS | 7 (26h) |
| Oil & Gas Middle East | oilandgasmiddleeast.com | GNews en-US | 37 (of 38 items) |
| AGBI | agbi.com | GNews en-US | 26 |
| Trade Arabia | tradearabia.com/news | GNews en-US | 13 |
| Pipeline Oil & Gas Magazine | pipelineoilandgasnews.com | RSS | 14 (121h) |
| Nikkei Asia | asia.nikkei.com | GNews en-US | 11 |
| The Japan Times | japantimes.co.jp | GNews en-US | 14 |
| NHK World | www3.nhk.or.jp/nhkworld | GNews en-US | 3 |
| The Korea Herald | koreaherald.com | RSS | 4 (8h) |

<!-- --- Wave 5C (2026-09-14): Africa, Middle East & North-East Asia mainstream --- END -->

<!-- ---- merge fence: keep >=4 lines between wave blocks ---- -->


<!-- --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- BEGIN -->

### South & South-East Asia, Latin America mainstream, downstream trade press I

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| The Straits Times | straitstimes.com | RSS | 2 (41h, +1 rescued) |
| CNA / Channel NewsAsia | channelnewsasia.com | RSS | 3 (5h general) / 2 (7h business) |
| Bangkok Post | bangkokpost.com | RSS | 2 (19h) |
| VnExpress International | e.vnexpress.net | RSS | 2 (178h, 1 on-beat) |
| The Times of India | timesofindia.indiatimes.com | RSS | 4 (25h) |
| The Economic Times | economictimes.indiatimes.com | RSS | 8 (26h) |
| Business Standard | business-standard.com | RSS | 6 (81h) |
| Dawn | dawn.com | RSS | 15 (88h) |
| The Astana Times | astanatimes.com | RSS | 2 (11h) |
| Trend News Agency | en.trend.az | RSS | 2 (6h, +1 rescued) |
| MercoPress | en.mercopress.com | GNews en-US | 5 (RSS moved out 2026-09-14: its only pass in 48h was an RAF refuelling "tanker") |
| Buenos Aires Times | batimes.com.ar | RSS | 6 |
| Mexico News Daily | mexiconewsdaily.com | RSS | 1 (28h) |
| The Jakarta Post | thejakartapost.com | GNews en-US | 12 |
| The Edge Malaysia | theedgemalaysia.com | GNews en-US | 69 |
| Kaieteur News | kaieteurnewsonline.com | GNews en-US | 19 (~14 net of tag-archive pages) |
| BNamericas | bnamericas.com | GNews en-US | 25 |
| Hydrocarbon Processing | hydrocarbonprocessing.com | GNews en-US | 61 (~40 real articles) |
| Hydrocarbon Engineering | hydrocarbonengineering.com | GNews en-US | 13 |

**Surface correction, 2026-09-14 (post-merge):** MercoPress was registered on
RSS and moved to GNews en-US. Its feed measured `pass=1` over 48h and that pass
was a false positive (an RAF air-to-air refuelling *tanker*), i.e. an on-beat
yield of zero, while `site:en.mercopress.com` measures `pass=5/7d` all on-beat.
The feed line stays in `sources.py` commented out, with both numbers.

Rejected by this wave, with the measurement recorded next to a commented-out
entry in `ENGLISH_NO_RSS_DOMAINS`: Caixin Global (re-test, GNews pass=0/7d),
OilNOW (feed 403 on every path, GNews 1 item), Stabroek News (feed 404 on every
path, GNews 0 items), Petroleum Economist (feed 404; GNews 7 items whose title
is always the string "Petroleum Economist"), Oilfield Technology (feed 403,
GNews 2 items / pass=0).

<!-- --- Wave 5D (2026-09-14): South & South-East Asia, Latin America mainstream, downstream trade press I --- END -->

<!-- ---- merge fence: keep >=4 lines between wave blocks ---- -->


<!-- --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- BEGIN -->

### O&G / refining / shipping trade press & institutions

| Outlet | Domain | Surface | pass |
|---|---|---|---|
| LNG Industry | lngindustry.com | RSS | 20 |
| World Pipelines | worldpipelines.com | RSS | 4 |
| Tanks and Terminals | tanksterminals.com | RSS | 5 |
| Drilling Contractor (IADC) | drillingcontractor.org | RSS | 10 |
| RBN Energy | rbnenergy.com | RSS | 11 |
| Fuels & Lubes (F+L Daily) | fuelsandlubes.com | RSS | 17 |
| Energy Monitor | energymonitor.ai | RSS | 4 (2 on-beat) |
| Ship & Bunker | shipandbunker.com | RSS | 10 |
| Seatrade Maritime News | seatrade-maritime.com | RSS | 19 |
| Kpler | kpler.com | RSS (Insights blog) | 13 |
| US EIA (Today in Energy) | eia.gov | RSS (14s timeout override) | 2 |
| Offshore Magazine | offshore-mag.com | GNews en-US | 20 |
| Gas Processing & LNG | gasprocessingnews.com | GNews en-US (feed dateless) | 19 |
| Quantum Commodity Intelligence | qcintel.com | GNews en-US | 84 |
| Lloyd's List | lloydslist.com | GNews en-US | 22 |
| Riviera Maritime Media | rivieramm.com | GNews en-US | 22 |
| Mobility Plaza (ex-PetrolPlaza) | mobilityplaza.com | GNews en-US | 41 |
| IEA | iea.org | GNews en-US | 5 |
| OPEC | opec.org | GNews en-US | 3 (1 distinct/wk) |
| Wood Mackenzie | woodmac.com | GNews en-US | 21 (~6 on-beat) |
| Rystad Energy | rystadenergy.com | GNews en-US | 2 |
| Vortexa | vortexa.com | GNews en-US | 2 |
| Oxford Institute for Energy Studies | oxfordenergy.org | GNews en-US | 1 |
| ICIS | icis.com | GNews en-US | 3 |
| JPT / SPE | jpt.spe.org | GNews en-US | 1 |

Rejected, with the measurement recorded in the wave's `REJECTED` comment block
in `sources.py`: **Natural Gas World** (feed frozen at Feb 2025, fresh=0; GNews
items=0), **OPIS** (blog feed publishes ~monthly, fresh=0 over 7d and 48h; GNews
items=0), **PetrolPlaza** (domain retired, 301 to mobilityplaza.com, which is
registered above). OPEC, ICIS and JPT/SPE were rejected on 2026-08-18 under the
old bar and are registered here after a re-measurement.

<!-- --- Wave 5E (2026-09-14): O&G / refining / shipping trade press & institutions --- END -->

<!-- ---- merge fence: keep >=4 lines between wave blocks ---- -->


## Measuring a candidate source

Registering a source is a **measurement, not a guess**, and it is made **on the
runner** — from a datacenter IP, with the Supabase secrets, against the LIVE
keyword set. `scripts/measure_source.py` refuses to print a yield table when the
keyword set it loaded is the ~25-term hardcoded fallback (what a dev machine
without the service key gets), because those numbers describe a funnel
production does not use. So: always through the workflow.

### (a) An RSS candidate

```bash
gh workflow run measure_source.yml \
  -f urls="https://site.com/feed/ https://site.com/energy/rss.xml" \
  -f hours=168 -f lede=true
gh run list --workflow=measure_source.yml --limit 1     # then: gh run view <id> --log
```

Measure **every plausible feed of the site in ONE run** — the summary table at
the bottom compares them, and the right pick is usually a section feed, not the
site-wide one. `hours=168` (7 days) is the standard window; `lede=true` also
fetches the bodies of near-misses, which is how you learn whether the source is
usable title-only or needs the body.

A feed that returns `ERROR: ... timed out` is not necessarily dead, it may just
be **slow** — re-measure with a longer budget before concluding anything:

```bash
gh workflow run measure_source.yml -f urls="https://site.com/feed/" -f feed_timeout=12 -f hours=168
```

If it yields at 12s, the host belongs in `sources.FEED_TIMEOUT_OVERRIDES` with
the measured `fetch=N.NNs` plus headroom (the default is 4s and the 2026-08-18
waves lost eia.gov, intellinews and globalenergynetwork.net to it). All three
were recovered on 2026-09-14 by re-measuring: eia.gov (10.14s) and
globalenergynetwork.net (10.65s, a 1000-entry archive feed) with an override,
intellinews with none. A one-off timeout is not a verdict.

**Neither is a one-off success — measure more than once.** A host can be
*bimodal* rather than slow, and then a single probe is a coin flip that will
confirm whatever you already believed. intellinews is the worked example: read
at `fetch=0.53s` on 2026-09-14 and promoted to RSS on that basis, it was
re-measured on 2026-09-15 three times minutes apart at `feed_timeout=15` and
gave `0.09s`, `ReadTimeout@15s`, `ReadTimeout@15s`; production at the 4s default
was 50/50 over four scans. For that shape an override is *worse* than useless —
a longer budget converts none of the hangs and burns the 22s `COLLECT_DEADLINE`
shared by ~65 feeds on every second scan — so the host keeps the default and
stays on RSS, because a 10-15 slot feed on a 7-8h span polled every 5 min loses
~nothing to a 50% answer rate. Check a row count before calling it an outage.

### (b) A Google News en-US candidate

For outlets whose feed is WAF-blocked, paywalled, dateless or absent, the
surface is `site:<domain> when:<window> (<keywords>)` at `hl=en-US`:

```bash
gh workflow run measure_source.yml -f gnews_en="site.com other-site.com" -f hours=168
```

**Never hand-build that URL.** `--gnews-en` assembles it through
`google_news_site_queries_en()` with the LIVE keywords, so the OR-block is the
12-term `ENGLISH_KEYWORD_PRIORITY` subset production actually sends and `when:`
sits where Google will not truncate it; a hand-made query silently measures a
different funnel, and pasting a raw `&` through a shell truncates the locale.
The workflow prints the exact URL it fetched. A path is allowed and is honoured
by Google (`-f gnews_en="rigzone.com/news"` — measured 2026-08-18: the bare
domain is ~59% job listings, the `/news` path is ~all articles).

### (c) Is it already registered — and did the registration land?

```bash
gh workflow run measure_source.yml -f gnews_en="site.com" -f persisted="site.com"
```

`--persisted` lists the newest `news_articles` rows for the domain. Run it
**before** a registration ("register X" is often really "X went silent while the
workflow stayed green") and **after** one — it is the only end-to-end proof that
articles actually landed.

### (d) Reading the table, and the acceptance rule

```
items=37 span=168h fresh=37 pass=22 near=15 rescued=0 no_body=15 fetch=1.83s
```

| column | meaning |
|---|---|
| `items` | raw entries the fetcher returned. `0` = unreachable, unindexed, or nothing published |
| `span` | oldest -> newest `published_at` in the payload. `-` = **dateless**, which the scanner cannot persist (see the World Oil autopsy in `sources.py`) |
| `fresh` | entries inside `--hours` |
| `pass` | entries that match on title/summary — **what lands without paying a body fetch**, i.e. the real yield for a paywalled or GNews source |
| `near` | fresh entries that missed and would become lede-rescue candidates |
| `rescued` | of those, how many match once the body is fetched (`lede=true` only) |
| `no_body` | near-misses whose body could not be fetched — a wall of these means bodies are unreachable, so treat `pass` as the ceiling |

**Acceptance rule for the 2026-09 international programme** (Eduardo,
2026-09-14 — deliberately LOWER than the 2026-08-18 bar of ">= 3 on-beat passes
per 7d"):

- **Register** every candidate with ANY viable surface: `pass >= 1` over 7 days,
  or `pass >= 3` over 48 hours. Try the other surface before giving up — a dead
  feed is not a dead domain (measure `--gnews-en`), and a thin site-wide feed is
  not a thin site (measure the section feeds).
- **Reject** only a candidate whose EVERY surface returns 0 items (unreachable,
  not indexed by Google News, or dateless), or whose passes are >= 80% off-beat
  false positives (offshore wind, crude steel, edible/palm/coconut oil).

Either way, **write the numbers down** next to the entry in `sources.py` — or,
for a rejection, in the wave's commented-out `REJECTED` block. That per-domain
comment is the authoritative record (the README tables are a directory); it is
what stops the next wave from silently re-testing a domain that was already
measured.

Where the entry goes:

| verdict | registry |
|---|---|
| usable RSS feed | `RSS_FEEDS` **and** `INTERNATIONAL_RSS_DOMAINS` (both apex and www forms — without it the outlet is classified as national) |
| GNews-only | `ENGLISH_NO_RSS_DOMAINS` |
| rich but slow feed | `RSS_FEEDS` + `INTERNATIONAL_RSS_DOMAINS` + `FEED_TIMEOUT_OVERRIDES` |
| rejected | a commented-out entry with the measured numbers |

Adding English GNews domains past `EN_GNEWS_QUERIES_PER_SCAN` (34) is expected
and safe: the scan then rotates the list over `ceil(n/34)` cohorts, one per
5-minute scan, and the run log says `gnews en cohort k/N (m domains)`. Every
domain is still queried well inside the 24h `when:` window. What must NOT grow
uncapped is the foreign-language block — it is GNews-only and is submitted first
precisely because a dropped query there is total data loss for that language.

## Local dev

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env         # fill in SUPABASE_URL / SUPABASE_SERVICE_KEY
python news_hunter_service.py --once    # one scan
python news_hunter_service.py           # daemon loop (SCAN_INTERVAL_SECONDS=30)
```

## Translation throttling, retry pass and canonical urls (2026-09-11)

From 2026-09-01 Google throttled the scraped `translate.google.com/m` endpoint
(verified from the runner AND a residential IP): it answers with its
`Error 500 (Server Error)!!1` page, which `looks_like_error_page` correctly
refuses — so rows were left with `title_en` NULL instead of poisoned, but there
was no other backend and no retry, and the newest foreign headlines on /home
stayed native.

- `news_hunter/translate.py`: backend chain `google_web` (deep-translator,
  mapped code then auto) -> `clients5` (Google's JSON dictionary endpoint) ->
  `mymemory`. Every reply goes through the same guard (Google error page,
  MyMemory quota warning, echoed input). A per-backend circuit breaker skips a
  backend for 600s after 3 consecutive failures; each scan logs
  `translate backends: ...` counters.
- Stage 3e (`pipeline._run_translation_retry` + `news_hunter/translation_retry.py`):
  after the upsert, what is left of `TRANSLATE_CAP` (max 20) goes to foreign rows
  from the last 7 days still missing `title_en`, newest and oldest interleaved.
  Fill-only UPDATE keyed on url and filtered `title_en IS NULL`.
- `store.normalize_url` now collapses Sina Finance tracking query strings on
  article pages, AMP mirrors (`/amp/<path>`, `<path>/amp`, `?amp=1`,
  `?outputType=amp`) and a few host-specific tracking keys, so one article is one
  row and one translation. `scripts/dedupe_canonical_urls.py`
  (+ `dedupe_canonical_urls.yml`, dry-run by default) folds rows already stored
  onto the canonical url, keeping every existing translation.

## Keyword sense exclusion (2026-09-15)

A keyword is a string; a reader follows a sense. `Compass` is meant for Compass
Gás e Energia (Cosan: Comgás, Edge, biometano, gas release), but of the 571 rows
tagged `Compass` on 2026-09-15 dozens were the Jeep Compass and many more were
homonymous companies (Vinci Compass, Compass Group, Compass Diversified, Compass
Point, Compass Pathways, Compass Minerals, Compass Therapeutics, Compass Inc.).
Whole-word matching (`match_type = 'exact'`, set in the database the same day)
removes `compasso` / `descompasso` / `compassivo`; it cannot tell a car or
another company from the Cosan business.

- `news_hunter/keyword_senses.py` holds `KEYWORD_SENSE_EXCLUSIONS`
  (keyword -> off-topic senses + on-topic context + protected occurrences). Only
  `compass` has an entry: senses `automotive` and `other_entity`, company context
  (Cosan, Comgás, PASS3, GNV, biometano, gas release, "empresa de gás", the state
  gas distributors, "da Edge"...) that always wins, and `Nefte Compass` (Energy
  Intelligence) protected.
- `filter.matches_keywords` applies it to every hit, so every stage (slug, title,
  summary, lede rescue, Stage 4 and its fast-mode fallback) honours it. Title and
  summary are judged together (`sense_context`); other keywords on the same
  article are untouched. A hit is dropped only when every occurrence is
  explained by an excluded sense:
  - an occurrence pattern (`Jeep Compass`, `Renegade, Compass e Commander`,
    `Vinci Compass`, `Compass Group`) explains only the occurrence it touches, so
    "Vinci Compass e Compass divulgam balanço hoje" keeps the label;
  - document vocabulary (`Jeep`, `SUV`, `km/l`; `psilocibina`, `mercado
    imobiliário`, `$0,02`) explains every occurrence in the text. That is the one
    spill-over: a bare `Compass` in a text that says `SUV` is read as the car.
  Vocabulary is kept narrow because Compass sells natural gas for vehicles:
  `montadora`, `Stellantis`, `veículos` + `rodovias` and a bare model year
  (`Compass 2026: guidance...`) are deliberately NOT evidence.
- `scripts/purge_keyword_sense_exclusions.py` + `purge_keyword_sense_exclusions.yml`
  apply the same rule to stored rows: strip the label, delete the row when no
  keyword is left, keep anything without positive evidence.
  `--refetch-missing-evidence` re-fetches rows whose stored text has no whole-word
  occurrence (per-domain throttle, cap, deadline) and strips only when the fetched
  article proves it; a failed fetch or parse, or a thin body, keeps the row. The
  workflow runs three steps: plan (`--plan-out`: JSON backup of every affected
  row + the plan, no writes), upload (artifact, 90 days, fails when files are
  missing), and only with `apply=true` apply (`--apply-plan`: backup checksum
  verified, each write guarded on url + the planned array). Applying refuses a
  keyword whose live `match_type` is not `exact`.
- The scan cannot re-add a purged label: `supabase_sync` writes
  `matched_keywords` straight from the fresh match (no write-once or union for
  that column). The one-off `dedupe_canonical_urls.py` does union labels across
  duplicate rows, so run the purge after any dedupe pass, not before.

Adding a keyword: add an entry, tune it against real rows (read-only SQL), and
extend `tests/test_keyword_sense_exclusions.py` with those rows.

### A source's own name (2026-09-25)

A different problem sits next to it: the text is not ambiguous, the source is.
Every kpler.com item says "Kpler" (the `| Kpler - <date>` title suffix, the
body, the byline), so the `Kpler` keyword -- meant for what OTHER outlets write
about the company -- matched Kpler's whole blog: 38 of 103 kpler.com rows on
2026-09-25 matched only `Kpler` (marketing posts such as "How to choose ship
tracking software for your business"). `SOURCE_OWN_NAME_KEYWORDS` in
`keyword_senses.py` lists, per domain, keywords that do not count there; every
stage (title, summary, slug, lede rescue, Stage 4 and its fast-mode fallback)
drops them, and an item left with no keyword is not persisted, counted in the
`own-name keywords:` line each scan logs. The only entry is `kpler.com ->
{kpler}`. It is explicit on purpose, never derived from the display name:
`Petrobras` on agencia.petrobras.com.br is exactly what we want.
