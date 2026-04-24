# news-hunter-scanner

Cloud daemon that scans ~60 Brazilian oil & gas news sources every 30 seconds
and pushes new articles to a shared Supabase table (`news_articles`). Deployed
on Fly.io free tier in the `gru` (São Paulo) region.

Reads the keyword search set from the `news_hunter_keywords` table (union of
every authenticated user's personal list). The SectorData dashboard at
`ibbaoilandgasdata.vercel.app/news-hunter` reads the same `news_articles`
table with row-level security applied per user.

## Setup

```bash
# 1. Install flyctl
iwr https://fly.io/install.ps1 -useb | iex    # Windows PowerShell
# or: winget install Fly.Flyctl

# 2. Log in
flyctl auth login

# 3. Create app
flyctl apps create news-hunter-scanner

# 4. Set secrets
flyctl secrets set \
  SUPABASE_URL="https://<project>.supabase.co" \
  SUPABASE_SERVICE_KEY="<service-role-key>"

# 5. Deploy
flyctl deploy

# 6. Tail logs
flyctl logs
```

## Local dev

```bash
python -m venv venv
source venv/bin/activate     # or venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env         # fill in SUPABASE_URL / SUPABASE_SERVICE_KEY
python news_hunter_service.py
```

## Knobs

- `SCAN_INTERVAL_SECONDS` (default 30) — sweep cadence. Raise to 60/90 if
  sources start rate-limiting Fly.io's IPs.

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
