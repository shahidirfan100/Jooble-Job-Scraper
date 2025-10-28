# Jooble.org Job Scraper

This Apify actor scrapes job listings from Jooble.org using Crawlee's CheerioCrawler and gotScraping.

## Features

- Scrapes Jooble.org search results and job detail pages (no browser required).
- Prefers structured data (JSON-LD) where available, falls back to HTML parsing.
- Handles pagination until the requested number of results is reached.
- Extracts rich job metadata from both HTML and JSON-LD.
- Supports proxy rotation and rate limiting.
- Saves results to an Apify dataset using a consistent schema.

## Input

The actor accepts the following input fields (all optional unless noted):

- `startUrls` / `startUrl` — Specific Jooble.org search URL(s) to start from. If provided, these override searchQuery.
- `searchQuery` (string) — Keyword for job search, e.g., 'admin'. Used in URL as ukw parameter.
- `maxItems` (integer) — Maximum number of job listings to collect. Default: 100.
- `proxyConfiguration` — Proxy settings (use Apify Proxy for best results).

## Output

Each item saved to the dataset follows this structure:

```
{
	"title": "...",
	"company": "...",
	"location": "...",
	"date_posted": "...",
	"job_type": "...",
	"job_category": "...",
	"description_html": "<p>...</p>",
	"description_text": "Plain text version of description",
	"job_url": "...",
	"salary": "..."
}
```

## Notes

- The actor uses CheerioCrawler with gotScraping; no additional local packages are required beyond those in package.json.
- On Apify platform, provide `proxyConfiguration` and reasonable `maxItems` to avoid rate limits.
- If Jooble.org changes their markup, selectors in `src/main.js` may need small updates.