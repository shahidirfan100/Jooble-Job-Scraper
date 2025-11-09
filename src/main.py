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
    """Return a random realistic user agent with latest Chrome versions."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)


def get_client_hints_for_ua(user_agent: str) -> Dict[str, str]:
    """Generate client hint headers that match the user agent."""
    # Extract platform and version from UA
    is_mobile = '"Mobile"' in user_agent
    is_windows = 'Windows NT' in user_agent
    is_mac = 'Macintosh' in user_agent
    is_linux = 'Linux' in user_agent

    # Extract Chrome version
    version_match = re.search(r'Chrome/(\d+)\.(\d+)\.(\d+)\.(\d+)', user_agent)
    if version_match:
        major, minor, build, patch = version_match.groups()
        chrome_version = f'"{major}.{minor}.{build}.{patch}"'
    else:
        chrome_version = '"131.0.0.0"'  # fallback

    headers = {
        'Sec-CH-UA': f'"Chromium";v={major}, "Google Chrome";v={major}, "Not:A-Brand";v="99"',
        'Sec-CH-UA-Mobile': '?1' if is_mobile else '?0',
        'Sec-CH-UA-Platform': f'"{ "Windows" if is_windows else "macOS" if is_mac else "Linux" }"',
    }

    return headers


def human_like_delay(min_seconds: float = 0.5, max_seconds: float = 3.0) -> float:
    """Generate human-like delay with realistic distribution."""
    # Use beta distribution for more realistic timing (peaks around 1-2 seconds)
    import math
    alpha, beta_param = 2.0, 2.0
    u = random.random()
    delay = min_seconds + (max_seconds - min_seconds) * (u ** (1/(alpha+beta_param-1)))
    return delay


def simulate_reading_time(text_length: int) -> float:
    """Simulate reading time based on text length (words per minute)."""
    if text_length <= 0:
        return 0
    words_per_minute = 200 + random.randint(-50, 50)  # 150-250 WPM
    estimated_words = text_length / 5  # rough estimate
    reading_time = (estimated_words / words_per_minute) * 60
    # Add some jitter and minimum time
    return max(0.5, reading_time * (0.8 + random.random() * 0.4))


def simulate_mouse_movement(page: Page) -> None:
    """Simulate realistic mouse movement patterns."""
    import asyncio
    # Random mouse movements
    viewport = page.viewport_size or {'width': 1920, 'height': 1080}
    x = random.randint(100, viewport['width'] - 100)
    y = random.randint(100, viewport['height'] - 100)
    # Move mouse with slight curve
    page.mouse.move(x, y)


def simulate_network_latency() -> float:
    """Simulate realistic network latency based on connection type."""
    # Simulate different connection types
    connection_types = [
        ('fast_4g', 20, 50),    # 20-50ms
        ('slow_4g', 50, 150),   # 50-150ms
        ('fast_3g', 100, 300),  # 100-300ms
        ('slow_3g', 200, 500),  # 200-500ms
    ]

    conn_type, min_lat, max_lat = random.choice(connection_types)
    # Add jitter and occasional spikes
    base_latency = random.uniform(min_lat, max_lat)
    jitter = random.uniform(-0.2, 0.2) * base_latency
    spike = random.random() < 0.1  # 10% chance of latency spike

    if spike:
        base_latency *= (2 + random.random() * 3)  # 2-5x spike

    return max(10, base_latency + jitter) / 1000  # Convert to seconds


def exponential_backoff_with_jitter(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0, jitter_factor: float = 0.1) -> float:
    """Calculate exponential backoff delay with jitter to avoid thundering herd."""
    # Exponential backoff: base_delay * (2 ^ (attempt - 1))
    delay = base_delay * (2 ** (attempt - 1))
    
    # Cap at max_delay
    delay = min(delay, max_delay)
    
    # Add jitter (±jitter_factor * delay)
    jitter = random.uniform(-jitter_factor, jitter_factor) * delay
    delay += jitter
    
    # Ensure minimum delay
    return max(base_delay, delay)


def generate_realistic_referer(current_url: str, page_num: int) -> Optional[str]:
    """Generate a realistic referer URL for the current request."""
    if page_num == 1:
        # First page - could come from search engine or direct
        referer_options = [
            None,  # Direct access
            'https://www.google.com/search?q=jobs',  # Google search
            'https://www.bing.com/search?q=job+search',  # Bing search
            'https://duckduckgo.com/?q=employment',  # DuckDuckGo
            'https://www.linkedin.com/jobs',  # LinkedIn jobs
            'https://indeed.com/',  # Indeed
        ]
        return random.choice(referer_options)
    else:
        # Subsequent pages - likely from previous page in pagination
        parsed = urllib.parse.urlparse(current_url)
        query = urllib.parse.parse_qs(parsed.query)
        
        # Modify page number to previous page
        prev_page = page_num - 1
        query['p'] = [str(prev_page)]
        
        prev_query = urllib.parse.urlencode({k: v[0] if isinstance(v, list) else v for k, v in query.items()})
        prev_url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', prev_query, ''))
        
        return prev_url


async def simulate_connection_warmup(page: Page) -> None:
    """Simulate connection warmup and DNS resolution."""
    # Simulate DNS lookup and connection establishment
    await asyncio.sleep(simulate_network_latency() * 0.5)

    # Simulate TCP handshake
    await asyncio.sleep(simulate_network_latency() * 0.3)

    # Simulate TLS handshake if HTTPS
    if random.random() < 0.9:  # 90% of sites are HTTPS
        await asyncio.sleep(simulate_network_latency() * 0.4)


async def create_stealth_context(browser):
    """Create a new browser context with stealth settings."""
    context = await browser.new_context(
        user_agent=get_random_user_agent(),
        viewport={'width': 1920 + random.randint(-100, 100), 'height': 1080 + random.randint(-50, 50)},
        locale=random.choice(['en-US', 'en-GB', 'en-CA']),
        timezone_id=random.choice(['America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo']),
        java_script_enabled=True,
        accept_downloads=False,
        bypass_csp=True,
        ignore_https_errors=True,
        # Randomize other properties
        device_scale_factor=random.choice([1, 1.25, 1.5]),
        is_mobile=random.random() < 0.1,  # 10% mobile
        has_touch=random.random() < 0.15,  # 15% touch
        # Enhanced stealth options
        permissions=[],  # Block all permissions
        geolocation=None,  # No geolocation
        color_scheme='light',  # Force light mode
        reduced_motion='no-preference',  # Normal motion
        forced_colors=None,  # No forced colors
    )

    # Enhanced stealth script injection
    await context.add_init_script("""
        // Remove webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        // Mock plugins array with realistic values
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
                { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
            ]
        });

        // Mock languages with slight variations
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en', 'es']
        });

        // Enhanced chrome object
        window.chrome = {
            runtime: {
                onConnect: undefined,
                onMessage: undefined,
                connect: function() { return {}; },
                sendMessage: function() { return {}; }
            },
            csi: function() { return {}; },
            loadTimes: function() { return {}; },
            app: {
                isInstalled: false
            }
        };

        // Mock permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Override screen properties to be more realistic
        Object.defineProperty(screen, 'availTop', { value: 0 });
        Object.defineProperty(screen, 'availLeft', { value: 0 });

        // Mock hardware concurrency with variation
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 4 + Math.floor(Math.random() * 12)  // Random between 4-16
        });

        // Mock device memory
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => [4, 8, 16][Math.floor(Math.random() * 3)]
        });

        // Remove automation indicators
        delete window.callPhantom;
        delete window._phantom;
        delete window.__nightmare;
        delete window._seleniumRunner;
        delete window.__webdriver_script_fn;
        delete window.__driver_evaluate;
        delete window.__webdriver_evaluate;
        delete window.__selenium_evaluate;
        delete window.__fxdriver_evaluate;
        delete window.__driver_unwrapped;
        delete window.__selenium_unwrapped;
        delete window.__fxdriver_unwrapped;

        // Mock battery API
        if (!navigator.getBattery) {
            navigator.getBattery = () => Promise.resolve({
                charging: Math.random() > 0.3,  // 70% chance charging
                chargingTime: Math.random() * 3600,
                dischargingTime: Math.random() * 7200,
                level: 0.1 + Math.random() * 0.9  // 10-100%
            });
        }

        // Mock connection API
        if (!navigator.connection) {
            navigator.connection = {
                effectiveType: ['4g', '3g', 'slow-2g'][Math.floor(Math.random() * 3)],
                rtt: 20 + Math.random() * 200,
                downlink: 0.5 + Math.random() * 50
            };
        }

        // Mock WebGL
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) { // UNMASKED_VENDOR_WEBGL
                return 'Intel Inc.';
            }
            if (parameter === 37446) { // UNMASKED_RENDERER_WEBGL
                return 'Intel(R) Iris(TM) Graphics 6100';
            }
            return getParameter.call(this, parameter);
        };

        // Mock canvas fingerprint randomization
        const toDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(...args) {
            const result = toDataURL.apply(this, args);
            // Add slight randomization to canvas fingerprint
            return result.replace(/.$/, String.fromCharCode(Math.floor(Math.random() * 26) + 97));
        };
    """)

    return context


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


async def fetch_search_page(page: Page, url: str, referer: Optional[str] = None, page_num: Optional[int] = None) -> Optional[str]:
    """Fetch the HTML content of a search page using Playwright with retries and diagnostics.

    This function will try multiple attempts with exponential backoff, rotate user-agents,
    and on final failure save a screenshot and HTML snippet to Key-Value store for debugging.
    """
    max_attempts = 3
    base_delay = 2.0

    for attempt in range(1, max_attempts + 1):
        ua = get_random_user_agent()
        client_hints = get_client_hints_for_ua(ua)
        # Set reasonable headers including User-Agent and Referer
        headers = {
            'User-Agent': ua,
            'Accept-Language': 'en-US,en;q=0.9',
            **client_hints  # Add client hint headers
        }
        if referer:
            headers['Referer'] = referer

        try:
            # Human-like delay before attempt with jitter
            delay = human_like_delay(1.0, 4.0) * attempt  # Increase delay with attempts
            await asyncio.sleep(delay)

            await page.set_extra_http_headers(headers)

            # Simulate connection warmup before navigation
            await simulate_connection_warmup(page)

            Actor.log.info(f'Attempt {attempt}/{max_attempts} fetching {url} with UA: {ua[:60]}')
            response = await page.goto(url, wait_until='domcontentloaded', timeout=30000)

            if not response:
                Actor.log.warning(f'No response object on attempt {attempt} for {url}')
                raise RuntimeError('No response')

            status = response.status
            Actor.log.debug(f'Navigation response status: {status} for {url}')

            if status == 403 or status == 429:
                Actor.log.warning(f'Blocked or rate-limited (status {status}) on attempt {attempt} for {url}')
                # If last attempt, capture diagnostics
                if attempt == max_attempts:
                    try:
                        key = f'fail_screenshot_page_{page_num or "na"}_attempt_{attempt}.png'
                        screenshot = await page.screenshot(full_page=False)
                        await Actor.set_value(key, screenshot, content_type='image/png')
                        html_snip = await page.content()
                        await Actor.set_value(f'fail_html_page_{page_num or "na"}_attempt_{attempt}.html', html_snip[:10000], content_type='text/html')
                    except Exception as e:
                        Actor.log.debug(f'Failed to save diagnostics: {e}')
                # Smart backoff based on status
                if status == 429:
                    # Rate limited - longer backoff
                    backoff_delay = exponential_backoff_with_jitter(attempt, base_delay * 2, max_delay=60)
                else:
                    # Blocked - standard backoff
                    backoff_delay = exponential_backoff_with_jitter(attempt, base_delay, max_delay=30)
                await asyncio.sleep(backoff_delay)
                continue

            if status >= 400:
                Actor.log.warning(f'HTTP error {status} for {url} on attempt {attempt}')
                if attempt == max_attempts:
                    # Save diagnostics
                    try:
                        await Actor.set_value(f'fail_status_page_{page_num or "na"}', str(status), content_type='text/plain')
                        html_snip = await page.content()
                        await Actor.set_value(f'fail_html_page_{page_num or "na"}_attempt_{attempt}.html', html_snip[:10000], content_type='text/html')
                    except Exception as e:
                        Actor.log.debug(f'Failed to save error snapshot: {e}')
                    return None
                # Backoff for other errors
                backoff_delay = exponential_backoff_with_jitter(attempt, base_delay, max_delay=20)
                await asyncio.sleep(backoff_delay)
                continue

            # Wait for job listing elements (best-effort)
            try:
                await page.wait_for_selector('article, a[href*="/redirect"], div[class*="job"]', timeout=12000)
            except Exception as e:
                Actor.log.debug(f'Wait for selector timed out: {e}')

            # Simulate human-like browsing behavior
            try:
                # Random mouse movement
                viewport = page.viewport_size or {'width': 1920, 'height': 1080}
                mouse_x = random.randint(200, viewport['width'] - 200)
                mouse_y = random.randint(200, viewport['height'] - 200)
                await page.mouse.move(mouse_x, mouse_y)

                # Simulate reading time before scrolling
                await asyncio.sleep(human_like_delay(2.0, 5.0))

                # Natural scrolling pattern
                scroll_steps = random.randint(2, 5)
                for i in range(scroll_steps):
                    scroll_amount = random.randint(200, 600)
                    await page.evaluate(f"window.scrollBy(0, {scroll_amount});")
                    await asyncio.sleep(human_like_delay(0.5, 2.0))

                # Simulate more reading time
                await asyncio.sleep(human_like_delay(1.0, 3.0))

                # Random additional interactions
                if random.random() < 0.3:  # 30% chance
                    # Simulate hovering over a job listing
                    try:
                        job_elements = await page.query_selector_all('article, div[class*="job"]')
                        if job_elements:
                            random_job = random.choice(job_elements[:5])  # First 5 jobs
                            box = await random_job.bounding_box()
                            if box:
                                await page.mouse.move(
                                    box['x'] + box['width'] / 2,
                                    box['y'] + box['height'] / 2
                                )
                                await asyncio.sleep(human_like_delay(0.5, 1.5))
                    except Exception:
                        pass
            except Exception as e:
                Actor.log.debug(f'Error during browsing simulation: {e}')

            # Final small delay to allow async content
            await asyncio.sleep(human_like_delay(1.0, 2.5))

            html = await page.content()
            Actor.log.info(f'Successfully fetched {url} (len={len(html)}) on attempt {attempt}')
            return html

        except Exception as e:
            Actor.log.warning(f'Attempt {attempt} failed for {url}: {e}')
            if attempt == max_attempts:
                # Save screenshot and HTML snippet for diagnosis
                try:
                    key = f'exception_screenshot_page_{page_num or "na"}_attempt_{attempt}.png'
                    screenshot = await page.screenshot(full_page=False)
                    await Actor.set_value(key, screenshot, content_type='image/png')
                    html_snip = await page.content()
                    await Actor.set_value(f'exception_html_page_{page_num or "na"}_attempt_{attempt}.html', html_snip[:15000], content_type='text/html')
                except Exception as e2:
                    Actor.log.debug(f'Failed to save final diagnostics: {e2}')
                return None
            # Exponential backoff before retrying with jitter
            backoff_delay = exponential_backoff_with_jitter(attempt, base_delay, max_delay=15)
            await asyncio.sleep(backoff_delay)
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
            # Configure browser launch options with enhanced stealth
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
                    '--disable-ipc-flooding-protection',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-field-trial-config',
                    '--disable-back-forward-cache',
                    '--disable-hang-monitor',
                    '--disable-prompt-on-repost',
                    '--force-color-profile=srgb',
                    '--metrics-recording-only',
                    '--no-crash-upload',
                    '--disable-logging',
                    '--disable-login-animations',
                    '--disable-notifications',
                    '--disable-permissions-api',
                    '--disable-session-crashed-bubble',
                    '--disable-infobars',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-component-extensions-with-background-pages',
                    '--disable-default-apps',
                    '--disable-extensions',
                    '--disable-extensions-except',
                    '--disable-extensions-file-access-check',
                    '--disable-extensions-http-throttling',
                    '--disable-features=TranslateUI',
                    '--disable-features=BlinkGenPropertyTrees',
                    '--no-default-browser-check',
                    '--no-first-run',
                    '--mute-audio',
                    '--disable-sync',
                    '--disable-translate',
                    '--hide-scrollbars',
                    '--metrics-recording-only',
                    '--no-crash-upload',
                    '--disable-component-update',
                    '--disable-domain-reliability',
                    '--disable-client-side-phishing-detection',
                    '--disable-background-networking',
                    '--disable-breakpad',
                    '--disable-component-extensions-with-background-pages',
                    '--disable-features=OptimizationHints',
                    '--disable-features=OptimizationGuideModelDownloading',
                    '--disable-features=OptimizationGuideModelStore',
                    '--disable-features=OptimizationGuidePersonalizedSuggestions',
                    '--disable-features=OptimizationGuideDebugLogs',
                    '--disable-features=HeavyAdPrivacyMitigations',
                    '--disable-features=HeavyAdIntervention',
                    '--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"',
                ]
            }
            
            # Add proxy if configured
            if proxy_url:
                try:
                    # Resolve proxy string (ProxyConfiguration has new_url())
                    proxy_str = None
                    if hasattr(proxy_url, 'new_url'):
                        proxy_str = await proxy_url.new_url()
                    elif isinstance(proxy_url, str):
                        proxy_str = proxy_url

                    if proxy_str:
                        # Parse credentials if present and pass them separately to Playwright
                        parsed = urllib.parse.urlparse(proxy_str)
                        scheme = parsed.scheme or 'http'
                        host = parsed.hostname
                        port = parsed.port
                        username = parsed.username
                        password = parsed.password
                        if host and port:
                            server = f"{scheme}://{host}:{port}"
                        else:
                            server = proxy_str

                        proxy_option = {'server': server}
                        if username:
                            proxy_option['username'] = urllib.parse.unquote(username)
                        if password:
                            proxy_option['password'] = urllib.parse.unquote(password)

                        launch_options['proxy'] = proxy_option
                        Actor.log.info(f'Configured browser with proxy server={server} user={bool(username)}')
                except Exception as e:
                    Actor.log.error(f'Failed to configure proxy for Playwright: {e}')
            
            browser = await p.chromium.launch(**launch_options)
            
            # Create initial browser context with stealth and realistic settings
            context = await create_stealth_context(browser)
            page = await context.new_page()
            
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

            # Set additional headers to mimic real browser (remove DNT as it's a bot signature)
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
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
                
                session_page_count = 0
                max_pages_per_session = random.randint(3, 8)  # Rotate session every 3-8 pages
                
                for page_num in range(1, max_pages + 1):
                    # Rotate session if needed
                    if session_page_count >= max_pages_per_session:
                        Actor.log.info(f'Rotating session after {session_page_count} pages')
                        await context.close()
                        context = await create_stealth_context(browser)
                        page = await context.new_page()
                        # Re-attach event handlers
                        page.on('requestfailed', _on_request_failed)
                        page.on('console', _on_console)
                        page.on('response', _on_response)
                        # Re-set headers
                        await page.set_extra_http_headers({
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Sec-Fetch-User': '?1',
                            'Cache-Control': 'max-age=0',
                        })
                        session_page_count = 0
                        max_pages_per_session = random.randint(3, 8)
                        # Clear cookies and local storage
                        await context.clear_cookies()
                        await page.evaluate("localStorage.clear(); sessionStorage.clear();")
                    
                    url = page_url(page_num)
                    Actor.log.info(f'Scraping search page {page_num}: {url}')

                    # Generate realistic referer
                    referer = generate_realistic_referer(url, page_num)

                    html = await fetch_search_page(page, url, referer=referer, page_num=page_num)
                    referer_url = url  # Update for next iteration
                    if not html:
                        Actor.log.warning(f'Empty HTML for {url}, stopping pagination.')
                        break

                    soup = BeautifulSoup(html, 'lxml')
                    session_page_count += 1

                
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

                    # Add pacing between pages to avoid rate limiting
                    if page_num < max_pages:
                        page_delay = human_like_delay(3.0, 8.0)  # 3-8 seconds between pages
                        Actor.log.debug(f'Pacing: waiting {page_delay:.1f}s before next page')
                        await asyncio.sleep(page_delay)

            finally:
                await browser.close()

        Actor.log.info(f'Scrape complete. Total items: {total_pushed}.')


if __name__ == '__main__':
    asyncio.run(main())
