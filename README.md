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

## Local dev

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env         # fill in SUPABASE_URL / SUPABASE_SERVICE_KEY
python news_hunter_service.py --once    # one scan
python news_hunter_service.py           # daemon loop (SCAN_INTERVAL_SECONDS=30)
```
