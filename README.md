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
