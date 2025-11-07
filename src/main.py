"""Apify Actor entry point: Jooble Job Scraper (Python, Playwright + BeautifulSoup).

This Actor fetches job listings from Jooble using query parameters for `ukw` (keyword)
and `rgns` (region), paginates via `p` parameter, and outputs structured dataset items.
"""

# --- Imports ---
import asyncio
import random
import urllib.parse
import re
import json
from typing import List, Dict, Any, Set, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, Tag
from playwright.async_api import async_playwright, Page
from apify import Actor

# --- Helper Functions ---

def get_random_user_agent():
    """Return a random realistic user agent."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)


def absolute_url(base: str, href: str) -> str:
    """Convert relative URL to absolute."""
    if not href:
        return ''
    return urljoin(base, href)


def select_first_text(container: Tag, selectors: List[str]) -> Optional[str]:
    """Try multiple selectors and return first non-empty text."""
    if not container:
        return None
    for selector in selectors:
        try:
            elem = container.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        except Exception:
            continue
    return None


def select_first_html(container: Tag, selectors: List[str]) -> Optional[str]:
    """Try multiple selectors and return first non-empty HTML."""
    if not container:
        return None
    for selector in selectors:
        try:
            elem = container.select_one(selector)
            if elem:
                html = str(elem)
                if html:
                    return html
        except Exception:
            continue
    return None


# --- Job Extraction Functions ---

def extract_jobs_from_ld_json(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Extract jobs from JSON-LD structured data in script tags."""
    jobs = []
    
    # Find all script tags
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            
            # Handle single JobPosting
            if isinstance(data, dict) and data.get('@type') == 'JobPosting':
                job = parse_json_ld_job(data)
                if job:
                    jobs.append(job)
            
            # Handle array of JobPostings
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') == 'JobPosting':
                        job = parse_json_ld_job(item)
                        if job:
                            jobs.append(job)
        except (json.JSONDecodeError, Exception) as e:
            Actor.log.debug(f'Error parsing JSON-LD: {e}')
            continue
    
    return jobs


def parse_json_ld_job(data: dict) -> Optional[Dict[str, Any]]:
    """Parse a single JobPosting JSON-LD object."""
    try:
        # Extract location
        location = None
        if 'jobLocation' in data:
            loc = data['jobLocation']
            if isinstance(loc, dict):
                address = loc.get('address', {})
                if isinstance(address, dict):
                    location = address.get('addressLocality') or address.get('addressRegion')
        
        # Extract hiring organization
        company = None
        if 'hiringOrganization' in data:
            org = data['hiringOrganization']
            if isinstance(org, dict):
                company = org.get('name')
        
        # Extract salary
        salary = None
        if 'baseSalary' in data:
            sal = data['baseSalary']
            if isinstance(sal, dict):
                value = sal.get('value', {})
                if isinstance(value, dict):
                    min_val = value.get('minValue')
                    max_val = value.get('maxValue')
                    currency = sal.get('currency')
                    if min_val and max_val:
                        salary = f"{min_val}-{max_val} {currency}"
                    elif min_val:
                        salary = f"{min_val} {currency}"
        
        return {
            'job_title': data.get('title'),
            'company': company,
            'location': location,
            'date_posted': data.get('datePosted'),
            'job_type': data.get('employmentType'),
            'job_url': data.get('url'),
            'description_text': data.get('description'),
            'description_html': None,
            'salary': salary,
        }
    except Exception as e:
        Actor.log.debug(f'Error parsing JSON-LD job: {e}')
        return None


def extract_jobs_from_links(soup: BeautifulSoup, page_url: str) -> List[Dict[str, Any]]:
    """Extract jobs by finding job links directly (fallback method)."""
    jobs = []
    seen_urls = set()
    
    # Jooble uses specific link patterns
    link_selectors = [
        'a.job_card_link',
        'a[href*="/redirect?"]',
        'a[href*="/job/"]',
        'a[href*="/jdp/"]',
    ]
    
    for selector in link_selectors:
        for link in soup.select(selector):
            href = link.get('href')
            if not href:
                continue
            
            abs_url = absolute_url(page_url, href)
            if abs_url in seen_urls:
                continue
            seen_urls.add(abs_url)
            
            # Get title from link text
            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            
            # Try to find additional info from parent container
            parent = link.find_parent(['div', 'article', 'li'])
            company = None
            location = None
            salary = None
            
            if parent:
                company = select_first_text(parent, [
                    'span[class*="company" i]',
                    'div[class*="company" i]',
                ])
                location = select_first_text(parent, [
                    'span[class*="location" i]',
                    'div[class*="location" i]',
                ])
                salary = select_first_text(parent, [
                    'span[class*="salary" i]',
                    'div[class*="salary" i]',
                ])
            
            jobs.append({
                'job_title': title,
                'company': company,
                'location': location,
                'date_posted': None,
                'job_type': None,
                'job_url': abs_url,
                'description_text': None,
                'description_html': None,
                'salary': salary,
            })
    
    return jobs


def extract_job_blocks(page_soup: BeautifulSoup) -> List[Tag]:
    """Extract job listing blocks from Jooble search page."""
    blocks = []
    
    # Jooble-specific selectors based on actual site structure
    selectors = [
        'article',  # Most common container
        'div[class*="_75cac"]',  # Jooble class pattern
        'div[data-test-name="job-item"]',
        'div.job-item',
    ]
    
    for selector in selectors:
        try:
            found = page_soup.select(selector)
            if found:
                for elem in found:
                    # Verify it has a job link
                    link = elem.find('a', href=True)
                    if link:
                        href = link.get('href', '')
                        if any(pattern in href for pattern in ['/redirect', '/job/', '/jdp/']):
                            blocks.append(elem)
                
                if blocks:
                    Actor.log.debug(f'Selector "{selector}" found {len(blocks)} job blocks')
                    break
        except Exception as e:
            Actor.log.debug(f'Selector "{selector}" failed: {e}')
            continue
    
    return blocks


def parse_job_block(block: Tag, page_url: str) -> Optional[Dict[str, Any]]:
    """Parse a single job block and return structured job data."""
    try:
        # Find job link - Jooble uses specific patterns
        link = block.find('a', href=True)
        if not link:
            return None
        
        href = link.get('href')
        if not href or not any(p in href for p in ['/redirect', '/job/', '/jdp/']):
            return None
        
        job_url = absolute_url(page_url, href)
        job_title = link.get_text(strip=True)
        
        if not job_title:
            return None
        
        # Extract company - look for common patterns
        company = select_first_text(block, [
            'span[class*="company" i]',
            'div[class*="company" i]',
            'a[class*="company" i]',
            '[data-company]',
        ])
        
        # Extract location
        location = select_first_text(block, [
            'span[class*="location" i]',
            'div[class*="location" i]',
            '[class*="city" i]',
            '[data-location]',
        ])
        
        # Extract salary
        salary = select_first_text(block, [
            'span[class*="salary" i]',
            'div[class*="salary" i]',
            '[class*="wage" i]',
            '[data-salary]',
        ])
        
        # Extract description snippet
        description_text = select_first_text(block, [
            'div[class*="description" i]',
            'div[class*="snippet" i]',
            'p[class*="description" i]',
        ])
        
        # Extract date posted
        date_posted = select_first_text(block, [
            'span[class*="date" i]',
            'time',
            '[datetime]',
        ])
        
        # Extract job type
        job_type = select_first_text(block, [
            'span[class*="type" i]',
            'span[class*="schedule" i]',
            '[data-type]',
        ])
        
        return {
            'job_title': job_title,
            'company': company,
            'location': location,
            'date_posted': date_posted,
            'job_type': job_type,
            'job_url': job_url,
            'description_text': description_text,
            'description_html': None,
            'salary': salary,
        }
    except Exception as e:
        Actor.log.debug(f'Error parsing job block: {e}')
        return None


async def fetch_search_page(page: Page, url: str, referer: Optional[str] = None) -> Optional[str]:
    """Fetch the HTML content of a search page using Playwright with stealth measures."""
    
    # Set referer if provided
    if referer:
        await page.set_extra_http_headers({'Referer': referer})
    
    # Add random delay to mimic human behavior
    await asyncio.sleep(random.uniform(1.5, 3.5))
    
    try:
        # Navigate to the page with realistic timeout
        response = await page.goto(url, wait_until='domcontentloaded', timeout=45000)
        if not response:
            Actor.log.error(f'No response from {url}')
            return None
            
        if response.status >= 400:
            Actor.log.warning(f'HTTP {response.status} for {url}')
            return None
        
        # Wait for content to load - Jooble uses dynamic rendering
        try:
            # Wait for job listings to appear
            await page.wait_for_selector('article, a[href*="/redirect"], div[class*="job"]', timeout=15000)
        except Exception as e:
            Actor.log.debug(f'Timeout waiting for job listings: {e}')
        
        # Additional wait for dynamic content
        await asyncio.sleep(random.uniform(2.0, 4.0))
        
        # Simulate human-like scrolling behavior
        await page.evaluate("""
            async () => {
                // Smooth scroll down
                await new Promise(resolve => {
                    let totalHeight = 0;
                    const distance = Math.floor(Math.random() * 200) + 100;
                    const timer = setInterval(() => {
                        const scrollHeight = document.documentElement.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if(totalHeight >= scrollHeight / 2){
                            clearInterval(timer);
                            resolve();
                        }
                    }, 100);
                });
            }
        """)
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        # Get the HTML content
        html = await page.content()
        Actor.log.info(f'Successfully fetched page: {len(html)} chars')
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
                # Create Apify Proxy configuration object (async) and use it
                try:
                    proxy_cfg = await Actor.create_proxy_configuration(actor_proxy_input=proxy_config)
                    proxy_url = proxy_cfg
                    Actor.log.info('Using Apify Proxy to avoid IP blocking')
                except Exception as e:
                    Actor.log.error(f'Failed to create Apify proxy configuration: {e}')
                    proxy_url = None
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
            
            # Create browser context with stealth and realistic settings
            context = await browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
                # Add more realistic browser features
                java_script_enabled=True,
                accept_downloads=False,
                bypass_csp=True,
                ignore_https_errors=True,
            )
            
            # Inject scripts to avoid detection
            await context.add_init_script("""
                // Overwrite the `plugins` property to use a custom getter
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // Overwrite the `plugins` property
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                
                // Overwrite the `languages` property
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                
                // Mock chrome object
                window.chrome = {
                    runtime: {}
                };
                
                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)
            
            page = await context.new_page()

            # Network and console event handlers for debugging connectivity
            def _on_request_failed(request):
                try:
                    Actor.log.warning(f'Request failed: {request.url} - {getattr(request, "failure", None)}')
                except Exception:
                    pass

            def _on_console(msg):
                try:
                    Actor.log.debug(f'Console [{msg.type}]: {msg.text}')
                except Exception:
                    pass

            def _on_response(response):
                try:
                    Actor.log.debug(f'Response: {response.url} -> {response.status}')
                except Exception:
                    pass

            page.on('requestfailed', _on_request_failed)
            page.on('console', _on_console)
            page.on('response', _on_response)

            # Set additional headers to mimic real browser
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
            })

            # Quick connectivity health checks to help debug network blocking
            try:
                health = await page.goto('https://example.com', wait_until='domcontentloaded', timeout=20000)
                Actor.log.info(f'Health check example.com status: {health.status if health else "no-response"}')
            except Exception as e:
                Actor.log.warning(f'Health check example.com failed: {e}')

            try:
                jooble_health = await page.goto('https://jooble.org', wait_until='domcontentloaded', timeout=30000)
                Actor.log.info(f'Jooble root status: {jooble_health.status if jooble_health else "no-response"}')
                if jooble_health and jooble_health.status == 200:
                    try:
                        txt = await jooble_health.text()
                        await Actor.set_value('jooble_root_snippet', txt[:2000], content_type='text/plain')
                    except Exception:
                        pass
            except Exception as e:
                Actor.log.warning(f'Jooble health check failed: {e}')
            
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

                    soup = BeautifulSoup(html, 'lxml')

                
                    # Extract jobs using hybrid approach
                    blocks = extract_job_blocks(soup)
                    Actor.log.info(f'Found {len(blocks)} job blocks on page {page_num}')

                    new_items_on_page = 0
                    
                    # Try parsing job blocks first
                    for block in blocks:
                        item = parse_job_block(block, page_url=url)
                        if not item:
                            continue
                        job_url = item.get('job_url')
                        if job_url and job_url in seen_urls:
                            continue

                        # Enrich with metadata
                        item['source_url'] = url
                        item['page_number'] = page_num
                        await Actor.push_data(item)
                        total_pushed += 1
                        new_items_on_page += 1
                        if job_url:
                            seen_urls.add(job_url)

                    # Fallback: Try JSON-LD if no blocks found
                    if new_items_on_page == 0:
                        Actor.log.info('No jobs from blocks, trying JSON-LD extraction')
                        ld_jobs = extract_jobs_from_ld_json(soup)
                        for item in ld_jobs:
                            job_url = item.get('job_url')
                            if job_url and job_url in seen_urls:
                                continue
                            item['source_url'] = url
                            item['page_number'] = page_num
                            await Actor.push_data(item)
                            total_pushed += 1
                            new_items_on_page += 1
                            if job_url:
                                seen_urls.add(job_url)

                    # Last resort: Direct link extraction
                    if new_items_on_page == 0:
                        Actor.log.info('No jobs from JSON-LD, trying direct link extraction')
                        link_jobs = extract_jobs_from_links(soup, page_url=url)
                        for item in link_jobs:
                            job_url = item.get('job_url')
                            if job_url and job_url in seen_urls:
                                continue
                            item['source_url'] = url
                            item['page_number'] = page_num
                            await Actor.push_data(item)
                            total_pushed += 1
                            new_items_on_page += 1
                            if job_url:
                                seen_urls.add(job_url)

                    Actor.log.info(f'Page {page_num}: pushed {new_items_on_page} new items (total {total_pushed}).')

                    if new_items_on_page == 0:
                        Actor.log.info('No jobs found on this page, stopping pagination.')
                        break

                    if max_jobs > 0 and total_pushed >= max_jobs:
                        Actor.log.info(f'Reached maxJobs limit ({max_jobs}). Stopping.')
                        break

            finally:
                await browser.close()

        Actor.log.info(f'Scrape complete. Total items: {total_pushed}.')


if __name__ == '__main__':
    asyncio.run(main())
