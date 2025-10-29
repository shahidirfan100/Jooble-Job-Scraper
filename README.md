# Jooble Job Scraper

This Apify Actor scrapes job listings from [Jooble.org](https://jooble.org), a global job search platform. It extracts detailed information about job opportunities including titles, companies, locations, salaries, job types, posting dates, and descriptions.

## Features

- **Keyword Search**: Search for jobs using specific keywords (e.g., "software engineer", "data analyst")
- **Location Filtering**: Filter jobs by location/region
- **Pagination Support**: Automatically handles multiple pages of search results
- **Detailed Extraction**: Extracts comprehensive job information including:
  - Job title and URL
  - Company name
  - Location
  - Salary information
  - Job type (full-time, part-time, etc.)
  - Posting date
  - Job description (text and HTML)
- **Duplicate Prevention**: Avoids duplicate job listings
- **Stealth Mode**: Uses randomized headers and user agents to avoid detection

## Input Parameters

- **startUrl** (optional): Direct Jooble search URL to scrape from. If provided, overrides keyword and region fields.
- **keyword**: Search keyword for jobs (required if startUrl not provided)
- **region**: Location/region filter (optional)
- **maxJobs**: Maximum number of jobs to collect (default: 100, 0 = unlimited)
- **max_pages**: Maximum number of search result pages to scrape (default: 5)
- **dateFilter**: Filter jobs by posting date (default: "all")
  - "all": All time
  - "1": Last 24 hours
  - "7": Last 7 days
  - "30": Last 30 days

## Output

The Actor outputs structured JSON data for each job listing to the default dataset. Each item contains:

```json
{
  "job_title": "Software Engineer",
  "company": "Tech Corp",
  "location": "New York, NY",
  "date_posted": "2 days ago",
  "job_type": "Full-time",
  "job_url": "https://jooble.org/job/software-engineer-tech-corp",
  "description_text": "We are looking for a skilled software engineer...",
  "description_html": "<div>We are looking for a skilled software engineer...</div>",
  "salary": "$80,000 - $120,000",
  "source_url": "https://jooble.org/SearchResult?ukw=software+engineer&rgns=New+York&p=1",
  "page_number": 1
}
```

## Quick Start

### Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the Actor locally:
   ```bash
   apify run
   ```

### Deploy to Apify

1. Login to Apify:
   ```bash
   apify login
   ```

2. Push the Actor:
   ```bash
   apify push
   ```

## Usage Example

To scrape software engineering jobs in New York:

```json
{
  "keyword": "software engineer",
  "region": "New York",
  "maxJobs": 50,
  "max_pages": 5,
  "dateFilter": "7"
}
```

To scrape from a specific Jooble URL:

```json
{
  "startUrl": "https://jooble.org/SearchResult?ukw=data+scientist&rgns=London",
  "maxJobs": 100,
  "dateFilter": "1"
}
```

## Dependencies

- apify
- beautifulsoup4
- requests-html
- lxml
- stealthkit (optional, for enhanced stealth)

## Project Structure

```
.actor/
├── actor.json           # Actor configuration
├── input_schema.json    # Input validation schema
├── output_schema.json   # Output schema definition
└── dataset_schema.json  # Dataset structure and views
src/
└── main.py             # Main scraper logic
requirements.txt        # Python dependencies
Dockerfile             # Container definition
README.md              # This file
```

## Resources

- [Apify Platform Documentation](https://docs.apify.com/platform)
- [Apify Python SDK](https://docs.apify.com/sdk/python/)
- [Jooble Job Search](https://jooble.org)