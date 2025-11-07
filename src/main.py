"""Apify Actor entry point: Jooble Job Scraper (Python, Playwright + BeautifulSoup).

This Actor fetches job listings from Jooble using query parameters for `ukw` (keyword)
and `rgns` (region), paginates via `p` parameter, and outputs structured dataset items.
"""

# --- Imports ---
import asyncio
import random
import urllib.parse
def get_random_user_agent():
    # TODO: Implement random user agent selection
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
import re
import json
from typing import List, Dict, Any, Set, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page
# Helper stubs and missing imports
def absolute_url(base: str, href: str) -> str:
    # TODO: Implement absolute URL resolution
    return href

def select_first_text(container, selectors):
    # TODO: Implement text selection from container
    return None

def select_first_html(container, selectors):
    # TODO: Implement HTML selection from container
    return None

# Dummy Actor stub for logging
class Actor:
    class log:
        @staticmethod
        def debug(msg):
            print(msg)
        @staticmethod
        def info(msg):
            print(msg)
        @staticmethod
        def warning(msg):
            print(msg)
        @staticmethod
        def error(msg):
            print(msg)
# If using Apify Actor, import Actor as needed

async def main():
    return []


def parse_job_block(block: BeautifulSoup, page_url: str):
    # TODO: Implement job block parsing logic
    return {}


def extract_jobs_from_links(soup: BeautifulSoup, page_url: str):
    # TODO: Implement anchor-based job extraction logic
    return []

def extract_jobs_from_ld_json(soup: BeautifulSoup) -> list:
    # TODO: Implement JSON-LD job extraction logic
    return []

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
        # broader anchors possibly wrapped in list containers
        'div[data-qa*="vacancy-item" i]',
        'div[class*="vacancy" i]',
        # Jooble-specific patterns from actual HTML structure
        'div[class*="card" i]',
        'div[class*="listing" i]',
        'div[class*="item" i]',
        # Look for divs containing job_card_link anchors
        'div:has(> a.job_card_link)',
        'div:has(> a[href*="redirect"])',
        # More generic container patterns
        'li',
        'article',
        'section > div',
    ]
    for css in strategies:
        try:
            found = page_soup.select(css)
            if found:
                candidates.extend(found)
                Actor.log.debug(f'Strategy "{css}" found {len(found)} elements')
        except Exception as e:
            # Some selectors like :has() might not work in older BeautifulSoup
            Actor.log.debug(f'Strategy "{css}" failed: {e}')
            continue
    
    # De-duplicate while preserving order
    seen: Set[int] = set()
    unique_blocks: List[BeautifulSoup] = []
    for block in candidates:
        key = id(block)
        if key not in seen:
            seen.add(key)
            unique_blocks.append(block)
    Actor.log.info(f'extract_job_blocks found {len(unique_blocks)} unique blocks')
    return unique_blocks


def parse_job_block(block: BeautifulSoup, page_url: str) -> Optional[Dict[str, Any]]:
    """Parse a single job block and return a structured dict or None if incomplete."""
    # Title and URL
    title_el = block.select_one('div[class*="position" i], a[href*="/job/"]')
    if not title_el:
        # Sometimes title anchor is nested
        title_el = block.select_one('a[href*="/job/"]')
    if not title_el:
        # Additional Jooble variants, including obfuscated multi-class link
        title_el = block.select_one(
            'a.job_card_link, '
            'a[class*="job_card_link" i], '
            'a._8w9Ce2.tUC4Fj._6i4Nb0.wtCvxI.job_card_link, '
            'a[data-qa*="vacancy-title" i], '
            'h2 a, h3 a, '
            'a[href*="/jdp/"], a[href*="/jd/"], a[href*="/j/"], a[href*="redirect?"]'
        )

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
        description_text = BeautifulSoup(description_html, 'html.parser').get_text(strip=True)

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


async def fetch_search_page(page: Page, url: str, referer: Optional[str]) -> Optional[str]:
    """Fetch the HTML content of a search page using Playwright with stealth measures."""
    
    # Set referer if provided
    if referer:
        await page.set_extra_http_headers({'Referer': referer})
    
    # Add random delay to mimic human behavior
    await asyncio.sleep(random.uniform(1.0, 3.0))
    
    try:
        # Navigate to the page
        response = await page.goto(url, wait_until='networkidle', timeout=30000)
        if response.status >= 400:
            Actor.log.warning(f'HTTP {response.status} for {url}')
            return None
        
        # Wait a bit for dynamic content
        await asyncio.sleep(random.uniform(2.0, 5.0))
        
        # Simulate human-like scrolling
        await page.evaluate("""
            window.scrollTo({
                top: Math.floor(Math.random() * 500) + 100,
                behavior: 'smooth'
            });
        """)
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        # Get the HTML content
        html = await page.content()
        return html
    
    except Exception as e:
        Actor.log.error(f'Error fetching {url}: {e}')
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
        
        # Get proxy configuration
        proxy_config = actor_input.get('proxyConfiguration')
        proxy_url = None
        effective_proxy: Optional[str] = None
        
        if proxy_config:
            use_apify_proxy = proxy_config.get('useApifyProxy', False)
            if use_apify_proxy:
                # Get Apify proxy URL from the Actor configuration
                proxy_url = Actor.create_proxy_configuration(actor_proxy_input=proxy_config)
                if proxy_url:
                    Actor.log.info('Using Apify Proxy to avoid IP blocking')
            elif proxy_config.get('proxyUrls'):
                # Use custom proxy if provided
                proxy_urls = proxy_config.get('proxyUrls', [])
                if proxy_urls:
                    proxy_url = random.choice(proxy_urls)
                    Actor.log.info(f'Using custom proxy')

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
            # Do NOT send empty rgns to avoid anti-bot heuristics; omit when not provided
            if region:
                params['rgns'] = region
            return 'https://jooble.org/SearchResult?' + urllib.parse.urlencode(params)

        seen_urls: Set[str] = set()
        total_pushed = 0
        referer_url: Optional[str] = None

        # Launch Playwright browser with stealth
        async with async_playwright() as p:
            # Configure browser launch options
            launch_options = {
                'headless': True,  # Run headless for server environment
                'args': [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                ]
            }
            
            # Add proxy if configured
            if proxy_url:
                if hasattr(proxy_url, 'new_url'):
                    proxy_str = await proxy_url.new_url()
                    launch_options['proxy'] = {'server': proxy_str}
                    Actor.log.info(f'Configured browser with Apify Proxy')
                elif isinstance(proxy_url, str):
                    launch_options['proxy'] = {'server': proxy_url}
                    Actor.log.info(f'Configured browser with custom proxy')
            
            browser = await p.chromium.launch(**launch_options)
            
            # Create browser context with stealth
            context = await browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
            )
            
            # Apply stealth plugin
            
            page = await context.new_page()
            
            # Set additional headers to mimic real browser
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
            })
            
            try:
                # Skip AJAX for now, as Playwright handles dynamic content
                # If needed, can add back later
                
                for page_num in range(1, max_pages + 1):
                    url = page_url(page_num)
                    Actor.log.info(f'Scraping search page {page_num}: {url}')

                    html = await fetch_search_page(page, url, referer=referer_url)
                    referer_url = url
                    if not html:
                        Actor.log.warning(f'Empty HTML for {url}, stopping pagination.')
                        break

                        # Debug: Log HTML stats
                    Actor.log.info(f'HTML length: {len(html)} chars')
                    html_lower = html.lower()
                    has_job = 'job' in html_lower
                    has_redirect = 'redirect' in html_lower
                    has_vacancy = 'vacancy' in html_lower
                    has_position = 'position' in html_lower
                    Actor.log.info(f'HTML contains: job={has_job}, redirect={has_redirect}, vacancy={has_vacancy}, position={has_position}')
                
                    # Save HTML sample for debugging (first page only)
                    if page == 1:
                        try:
                            await Actor.set_value('debug_html_sample', html[:50000], content_type='text/html')
                            Actor.log.info('Saved HTML sample to key-value store (debug_html_sample)')
                        except Exception as e:
                            Actor.log.debug(f'Failed to save HTML sample: {e}')

                    soup = BeautifulSoup(html, 'html.parser')
                
                    # Debug: Count all links
                    all_links = soup.find_all('a', href=True)
                    Actor.log.info(f'Found {len(all_links)} total links on page')
                    redirect_links = [a for a in all_links if 'redirect' in a.get('href', '').lower()]
                    job_links = [a for a in all_links if any(p in a.get('href', '').lower() for p in ['job', 'vacancy', 'position'])]
                    Actor.log.info(f'Found {len(redirect_links)} redirect links, {len(job_links)} job-related links')
                    blocks = extract_job_blocks(soup)
                    if not blocks:
                        # Log what we found for debugging
                        Actor.log.warning(f'No job blocks found. Total links: {len(all_links)}, Redirect links: {len(redirect_links)}, Job links: {len(job_links)}')

                    collected_any = False

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
                        collected_any = True

                    if not collected_any:
                        # Try JSON-LD fallback
                        ld_jobs = extract_jobs_from_ld_json(soup)
                        for item in ld_jobs:
                            job_url = item.get('job_url')
                            if job_url and job_url in seen_urls:
                                continue
                            item['source_url'] = url
                            item['page_number'] = page
                            await Actor.push_data(item)
                            total_pushed += 1
                            new_items_on_page += 1
                            if job_url:
                                seen_urls.add(job_url)
                            collected_any = True

                    if not collected_any:
                        # As a last resort, scan anchors directly for job links
                        link_jobs = extract_jobs_from_links(soup, page_url=url)
                        for item in link_jobs:
                            job_url = item.get('job_url')
                            if job_url and job_url in seen_urls:
                                continue
                            item['source_url'] = url
                            item['page_number'] = page
                            await Actor.push_data(item)
                            total_pushed += 1
                            new_items_on_page += 1
                            if job_url:
                                seen_urls.add(job_url)
                        if new_items_on_page == 0:
                            Actor.log.info('No job blocks, JSON-LD, or anchor-based jobs detected on page, ending.')
                            break

                    Actor.log.info(f'Page {page}: pushed {new_items_on_page} new items (total {total_pushed}).')

                    if max_jobs > 0 and total_pushed >= max_jobs:
                        Actor.log.info(f'Reached maxJobs limit ({max_jobs}). Stopping.')
                        break

                    if new_items_on_page == 0:
                        Actor.log.info('No new items on page; stopping pagination early.')
                        break

            finally:
                await browser.close()

        Actor.log.info(f'Scrape complete. Total items: {total_pushed}.')