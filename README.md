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

## Local dev

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env         # fill in SUPABASE_URL / SUPABASE_SERVICE_KEY
python news_hunter_service.py --once    # one scan
python news_hunter_service.py           # daemon loop (SCAN_INTERVAL_SECONDS=30)
```
