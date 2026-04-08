# Rental Scout

## Testing Requirements

**Before every deploy to Railway:**
1. Start the server locally: `source .venv/bin/activate && uvicorn app.main:app --port 9123`
2. Open http://127.0.0.1:9123 in a browser and click through the feature you changed
3. Check the Live Activity tab to verify logs appear and persist
4. Test API endpoints with curl if relevant
5. Only deploy after verifying the feature works end-to-end

Do NOT just check that imports pass. Actually use the UI.

## Local Dev

```bash
source .venv/bin/activate
cp .env.example .env  # fill in APIFY_API_TOKEN and ANTHROPIC_API_KEY
uvicorn app.main:app --port 9123 --reload
```

## Deploy

```bash
railway up --detach
```

## Architecture

- **Craigslist**: Direct HTTP scraper (not Apify) — fast, free, gets full descriptions
- **Other platforms**: Apify actors (Zillow, Realtor, Redfin, Apartments.com, Facebook, Rent.com)
- **AI Scoring**: Claude Haiku, batch mode (5 listings per API call)
- **Draft Messages**: Claude Sonnet, using Caity's template
- **Database**: Postgres on Railway, SQLite locally
- **Activity Logs**: Stored in DB (activity_logs table), not in-memory
