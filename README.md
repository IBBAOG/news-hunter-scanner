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
   stamp only ever lands on first discovery. A *real* date found later still
   overwrites it, so a row is always curable.

Regression tests: `tests/test_stale_listing_items.py`.

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

Alongside the ~60 Brazilian sources, the scanner carries **48 English-language
international outlets** across seven regions — six that predated the program plus
**42 added in four waves** (2026-08-18). Two surfaces are used, in this order of
preference:

- **RSS** — a dated feed the scanner's feed path reaches from its datacenter
  runner (15 outlets). Matches land on the item title/summary with no body fetch.
- **GNews en-US** — a Google News `site:<domain> when:<window> (<keywords>)`
  query at `hl=en-US` (33 outlets), used when the outlet's own feed is
  WAF-blocked, paywalled, dateless or absent. Google supplies the date; bodies
  stay unreachable, so these land **title-only** (empty snippet).

What makes English outlets yield at all is the keyword layer. Matching runs over
the **full 91-keyword Supabase lexicon** (41 exact / 50 substring — `oil`, `gas`,
`Brent`, `WTI`, `OPEC`, `LNG`, `crude`, `diesel`, `refinery`, `Hormuz`,
`ExxonMobil`, `Petrobras`, …), while *retrieval* on the GNews route is narrowed to
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
| Global Energy Network | globalenergynetwork.net | GNews en-US | 18 |
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
| bne IntelliNews | intellinews.com | GNews en-US | 11 |
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

## Local dev

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env         # fill in SUPABASE_URL / SUPABASE_SERVICE_KEY
python news_hunter_service.py --once    # one scan
python news_hunter_service.py           # daemon loop (SCAN_INTERVAL_SECONDS=30)
```
