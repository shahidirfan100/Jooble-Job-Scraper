"""Apify Actor entry point: Jooble Job Scraper (Python, requests_html + BeautifulSoup).

This Actor fetches job listings from Jooble using query parameters for `ukw` (keyword)
and `rgns` (region), paginates via `p` parameter, and outputs structured dataset items.
"""

from __future__ import annotations

import asyncio
import random
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Set

from apify import Actor  # pyright: ignore[reportMissingImports]
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession

# Stealth headers / random user-agents
try:
    # Minimal, defensive import in case environment lacks stealthkit
    from stealthkit import user_agent as sk_user_agent
    from stealthkit.headers import build_headers as sk_build_headers
except Exception:  # pragma: no cover - fallback if stealthkit not available
    sk_user_agent = None
    sk_build_headers = None


def build_stealth_headers(referer: Optional[str] = None) -> Dict[str, str]:
    """Return randomized, browser-like headers using stealthkit if available.

    Falls back to a simple rotating User-Agent list if stealthkit is unavailable.
    """
    if sk_build_headers and sk_user_agent:
        ua = sk_user_agent.random()
        headers = sk_build_headers(user_agent=ua)
    else:
        fallback_uas = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
        ]
        ua = random.choice(fallback_uas)
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'DNT': '1',
            'Connection': 'keep-alive',
        }
    if referer:
        headers['Referer'] = referer
    return headers


def absolute_url(base: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return urllib.parse.urljoin(base, href)


def select_first_text(soup: BeautifulSoup, selectors: Iterable[str]) -> Optional[str]:
    for selector in selectors:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    return None


def select_first_html(soup: BeautifulSoup, selectors: Iterable[str]) -> Optional[str]:
    for selector in selectors:
        el = soup.select_one(selector)
        if el:
            return str(el)
    return None


def extract_job_blocks(page_soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Try multiple strategies to identify job listing blocks on Jooble search pages."""
    candidates = []
    # Common structures observed across locales; keep broad fallbacks
    strategies = [
        'article',
        'div[data-test="job"]',
        'div[class*="job" i]',
        'div[class*="result" i]',
        'li[class*="job" i]',
        'li[class*="result" i]',
        'article[class*="job" i]',
        'div[id^="job_" i]',
        'section[role="main"] article',
        'div[data-qa*="vacancy" i]',
    ]
    for css in strategies:
        found = page_soup.select(css)
        if found:
            candidates.extend(found)
    # De-duplicate while preserving order
    seen: Set[int] = set()
    unique_blocks: List[BeautifulSoup] = []
    for block in candidates:
        key = id(block)
        if key not in seen:
            seen.add(key)
            unique_blocks.append(block)
    return unique_blocks


def parse_job_block(block: BeautifulSoup, page_url: str) -> Optional[Dict[str, Any]]:
    """Parse a single job block and return a structured dict or None if incomplete."""
    # Title and URL
    title_el = block.select_one('div[class*="position" i], a[href*="/job/"]')
    if not title_el:
        # Sometimes title anchor is nested
        title_el = block.select_one('a[href*="/job/"]')
    if not title_el:
        # Additional Jooble variants
        title_el = block.select_one('a[data-qa*="vacancy-title" i], h2 a, h3 a')

    job_title = title_el.get_text(strip=True) if title_el else None
    href = title_el.get('href') if title_el else None
    job_url = absolute_url(page_url, href)

    # Company, location, date, salary
    company = select_first_text(block, [
        'span[class*="company-name" i]',
        'div[data-test="company_name"]',
        'div[class*="company" i]',
        'a[data-qa*="vacancy-company-name" i]',
        'span[data-qa*="company" i]',
    ])
    location = select_first_text(block, [
        'span[class*="location" i]',
        'div[class*="location" i]',
        'span[data-qa*="location" i]',
    ])
    date_posted = select_first_text(block, [
        'span[class*="date" i]',
        'time',
    ])
    salary = select_first_text(block, [
        'div[class*="salary" i]',
        'span[class*="salary" i]',
        'span[data-qa*="salary" i]',
    ])

    # Description text/html
    description_html = select_first_html(block, [
        'div[class*="description" i]',
        'div[class*="desc" i]',
        'div[data-qa*="vacancy-snippet" i]',
    ])
    description_text = None
    if description_html:
        # Parse again to clean text
        description_text = BeautifulSoup(description_html, 'lxml').get_text(strip=True)

    # Job type heuristic: look for tags inside description or chips
    job_type = select_first_text(block, [
        'div[class*="description" i] span[class*="type" i]',
        'span[class*="type" i]',
        'a[class*="tag" i]',
        'span[class*="tag" i]',
        'span[data-qa*="employment" i]',
        'span[data-qa*="schedule" i]',
    ])

    if not job_title and not job_url:
        return None

    return {
        'job_title': job_title,
        'company': company,
        'location': location,
        'date_posted': date_posted,
        'job_type': job_type,
        'job_url': job_url,
        'description_text': description_text,
        'description_html': description_html,
        'salary': salary,
    }


async def fetch_search_page(session: AsyncHTMLSession, url: str, referer: Optional[str]) -> Optional[str]:
    headers = build_stealth_headers(referer=referer)
    # Add encodings and pragma-like hints often present in real browsers
    headers.setdefault('Accept-Encoding', 'gzip, deflate, br, zstd')

    # Simple retry with exponential backoff and jitter
    max_attempts = 3
    backoff_base = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            # Randomized small delay before request to reduce burstiness
            await asyncio.sleep(random.uniform(0.4, 1.2))

            resp = await session.get(url, headers=headers, timeout=30)
            status = getattr(resp, 'status_code', None)

            html_text = resp.text
            # If blocked or too small, attempt a lightweight JS render once per attempt
            if not html_text or len(html_text) < 1000 or status in {403, 429}:
                try:
                    await resp.html.arender(timeout=25, sleep=random.uniform(0.8, 1.6))
                    html_text = resp.html.html
                except Exception as render_err:
                    Actor.log.debug(f'JS render failed on attempt {attempt} for {url}: {render_err}')

            # Basic success heuristic
            if html_text and ('job' in html_text.lower() or len(html_text) > 2000):
                return html_text

            # Prepare next retry
            if attempt < max_attempts:
                delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0.2, 0.8)
                Actor.log.info(f'Weak/blocked response (status={status}) for {url}, retrying in {delay:.1f}s...')
                await asyncio.sleep(delay)
        except Exception as e:
            if attempt < max_attempts:
                delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0.2, 0.8)
                Actor.log.info(f'Fetch error on attempt {attempt} for {url}: {e}. Retrying in {delay:.1f}s...')
                await asyncio.sleep(delay)
            else:
                Actor.log.warning(f'Failed to fetch {url}: {e}')

    return None


async def main() -> None:
    """Jooble job scraper main entry."""
    async with Actor:
        actor_input = await Actor.get_input() or {}

        keyword: str = (actor_input.get('keyword') or actor_input.get('ukw') or '').strip()
        # Region is optional for Jooble; empty means global or site default
        region: str = (actor_input.get('region') or actor_input.get('rgns') or '').strip()
        max_pages: int = int(actor_input.get('max_pages') or 1)
        start_url: Optional[str] = (actor_input.get('startUrl') or '').strip() or None
        max_jobs: int = int(actor_input.get('maxJobs') or 0)

        if not start_url and not keyword:
            Actor.log.info('Provide either "startUrl" or "keyword" (ukw). Exiting...')
            await Actor.exit()

        def page_url(page_number: int) -> str:
            # If startUrl provided, preserve its query params and just update p
            if start_url:
                parsed = urllib.parse.urlparse(start_url)
                q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                q['p'] = [str(page_number)]
                new_query = urllib.parse.urlencode({k: v[0] if isinstance(v, list) else v for k, v in q.items()})
                return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', new_query, ''))
            # Build URL with optional region. Jooble accepts empty rgns.
            params = {
                'ukw': keyword,
                'p': str(page_number),
            }
            if region:
                params['rgns'] = region
            else:
                # Keep param present but empty to mirror provided sample URL
                params['rgns'] = ''
            return 'https://jooble.org/SearchResult?' + urllib.parse.urlencode(params)

        seen_urls: Set[str] = set()
        total_pushed = 0
        referer_url: Optional[str] = None

        session = AsyncHTMLSession()

        try:
            for page in range(1, max_pages + 1):
                url = page_url(page)
                Actor.log.info(f'Scraping search page {page}: {url}')

                html = await fetch_search_page(session, url, referer=referer_url)
                referer_url = url
                if not html:
                    Actor.log.warning(f'Empty HTML for {url}, stopping pagination.')
                    break

                soup = BeautifulSoup(html, 'lxml')
                blocks = extract_job_blocks(soup)
                if not blocks:
                    # As a secondary check, attempt render again if not already
                    Actor.log.info('No job blocks found; attempting JS render fallback...')
                    html2 = await fetch_search_page(session, url, referer=referer_url)
                    if html2:
                        soup = BeautifulSoup(html2, 'lxml')
                        blocks = extract_job_blocks(soup)

                if not blocks:
                    Actor.log.info('No job blocks detected on page, ending.')
                    break

                new_items_on_page = 0
                for block in blocks:
                    item = parse_job_block(block, page_url=url)
                    if not item:
                        continue
                    job_url = item.get('job_url')
                    if job_url and job_url in seen_urls:
                        continue

                    # Enrich with metadata
                    item['source_url'] = url
                    item['page_number'] = page
                    await Actor.push_data(item)
                    total_pushed += 1
                    new_items_on_page += 1
                    if job_url:
                        seen_urls.add(job_url)

                Actor.log.info(f'Page {page}: pushed {new_items_on_page} new items (total {total_pushed}).')

                if max_jobs > 0 and total_pushed >= max_jobs:
                    Actor.log.info(f'Reached maxJobs limit ({max_jobs}). Stopping.')
                    break

                if new_items_on_page == 0:
                    Actor.log.info('No new items on page; stopping pagination early.')
                    break

        finally:
            try:
                await session.close()
            except Exception:
                pass

        Actor.log.info(f'Scrape complete. Total items: {total_pushed}.')